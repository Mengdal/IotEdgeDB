package wal

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/rs/zerolog"
)

// RecoveryCallback is called for each batch of records during recovery (row format)
type RecoveryCallback func(ctx context.Context, records []map[string]interface{}) error

// ColumnarRecoveryCallback is called for columnar WAL entries during recovery
// database may be empty if the WAL entry predates the envelope format (defaults to "default")
type ColumnarRecoveryCallback func(ctx context.Context, database, measurement string, columns map[string][]interface{}) error

// RecoveryStats holds statistics about WAL recovery
type RecoveryStats struct {
	RecoveredFiles   int
	RecoveredBatches int
	RecoveredEntries int
	CorruptedEntries int
	StagingCleared   int // measurements whose staging was cleared by FLUSH_OK
	RecoveryDuration time.Duration
}

// DefaultMaxStagingRecords is the default per-measurement staging threshold.
// When staging for a single measurement exceeds this count, its data is
// replayed immediately to bound recovery memory usage.
// Set high (5M) so incremental replay fires only in extreme memory-pressure
// scenarios, not during normal cross-file FLUSH_OK operation. A too-low value
// risks duplicate replay if oversized data is replayed before its FLUSH_OK
// appears in a later WAL file.
const DefaultMaxStagingRecords = 5000000

// RecoveryOptions configures WAL recovery behavior
type RecoveryOptions struct {
	// BatchSize limits how many records are replayed per callback invocation
	// This provides backpressure during mass recovery after prolonged outages
	// 0 means no limit (all records in an entry replayed at once)
	BatchSize int

	// MaxStagingRecords limits per-measurement staging accumulation before
	// incremental replay. 0 means use DefaultMaxStagingRecords.
	// This prevents OOM when recovering many WAL files without FLUSH_OK markers.
	MaxStagingRecords int

	// ColumnarCallback handles columnar WAL entries from the zero-copy write path
	ColumnarCallback ColumnarRecoveryCallback
}

// Recovery manages WAL recovery operations
type Recovery struct {
	walDir string
	logger zerolog.Logger
}

// NewRecovery creates a new WAL recovery manager
func NewRecovery(walDir string, logger zerolog.Logger) *Recovery {
	return &Recovery{
		walDir: walDir,
		logger: logger.With().Str("component", "wal-recovery").Logger(),
	}
}

// measurementKey builds a stable key for staging lookups.
func measurementKey(database, measurement string) string {
	if database == "" {
		database = "default"
	}
	return database + "/" + measurement
}

// stagingEntry holds accumulated data for a single measurement during recovery.
// Records are row-format (from Append path), Columns are columnar (from AppendRaw path).
type stagingEntry struct {
	records     []map[string]interface{}
	columns     map[string][]interface{} // merged columnar data
	recordCount int                      // tracked to bound staging memory
}

// replayOversizedStaging replays staging entries whose record count exceeds the
// configured threshold. Called after each WAL file is processed to bound memory.
func replayOversizedStaging(
	ctx context.Context,
	staging map[string]*stagingEntry,
	opts *RecoveryOptions,
	callback RecoveryCallback,
	recoveredEntries *int,
	recoveredBatches *int,
	logger zerolog.Logger,
) {
	for key, se := range staging {
		if se.recordCount <= opts.MaxStagingRecords {
			continue
		}

		database, measurement := splitMeasurementKey(key)

		// Replay columnar data
		if len(se.columns) > 0 && opts.ColumnarCallback != nil {
			if err := opts.ColumnarCallback(ctx, database, measurement, se.columns); err != nil {
				logger.Error().Err(err).Str("measurement", key).Msg("Failed to replay oversized columnar staging")
				continue
			}
			*recoveredBatches++
		}

		// Replay row data
		if len(se.records) > 0 {
			replayed := replayRowStaging(ctx, callback, se.records, opts.BatchSize, logger, key)
			*recoveredEntries += replayed
			*recoveredBatches++
		}

		// Clear the replayed staging
		replayedCount := se.recordCount
		se.records = nil
		se.columns = nil
		se.recordCount = 0

		logger.Debug().
			Str("measurement", key).
			Int("records", replayedCount).
			Msg("Incrementally replayed oversized staging entry")
	}
}

// replayRowStaging replays a batch of row records through the callback.
// Returns the number of records successfully replayed.
func replayRowStaging(
	ctx context.Context,
	callback RecoveryCallback,
	records []map[string]interface{},
	batchSize int,
	logger zerolog.Logger,
	key string,
) int {
	recovered := 0
	if batchSize > 0 && len(records) > batchSize {
		for i := 0; i < len(records); i += batchSize {
			end := i + batchSize
			if end > len(records) {
				end = len(records)
			}
			if err := callback(ctx, records[i:end]); err != nil {
				logger.Error().Err(err).Str("measurement", key).Msg("Failed to replay row staging batch")
				break
			}
			recovered += end - i
		}
	} else {
		if err := callback(ctx, records); err != nil {
			logger.Error().Err(err).Str("measurement", key).Msg("Failed to replay row staging data")
			return 0
		}
		recovered = len(records)
	}
	return recovered
}

// Recover scans the WAL directory and replays all WAL files
func (r *Recovery) Recover(ctx context.Context, callback RecoveryCallback) (*RecoveryStats, error) {
	return r.RecoverWithOptions(ctx, callback, nil)
}

// RecoverWithOptions scans the WAL directory and replays WAL files using
// FLUSH_OK-aware staging recovery. All successfully confirmed (FLUSH_OK) data
// is skipped; only unconfirmed data is replayed.
func (r *Recovery) RecoverWithOptions(ctx context.Context, callback RecoveryCallback, opts *RecoveryOptions) (*RecoveryStats, error) {
	startTime := time.Now()
	stats := &RecoveryStats{}

	if opts == nil {
		opts = &RecoveryOptions{}
	}
	if opts.MaxStagingRecords <= 0 {
		opts.MaxStagingRecords = DefaultMaxStagingRecords
	}

	// Check if WAL directory exists
	if _, err := os.Stat(r.walDir); os.IsNotExist(err) {
		r.logger.Info().Msg("No WAL directory found, skipping recovery")
		return stats, nil
	}

	// Find all pending WAL files
	walFiles, err := r.findWALFiles()
	if err != nil {
		return nil, err
	}

	if len(walFiles) == 0 {
		r.logger.Info().Msg("No WAL files found, skipping recovery")
		return stats, nil
	}

	r.logger.Info().Int("files", len(walFiles)).Msg("WAL recovery started")

	// Per-measurement staging — accumulates data until a FLUSH_OK clears it
	staging := make(map[string]*stagingEntry)

	// Collect processed WAL file paths for deferred deletion
	var recoveredFiles []string

	// Process each WAL file — accumulate in staging
	for _, walFile := range walFiles {
		select {
		case <-ctx.Done():
			return stats, ctx.Err()
		default:
		}

		r.logger.Info().Str("file", filepath.Base(walFile)).Msg("Recovering WAL file")

		reader := NewReader(walFile, r.logger)
		entries, err := reader.ReadAll()
		if err != nil {
			r.logger.Error().Err(err).Str("file", walFile).Msg("Failed to read WAL file")
			continue
		}

		for _, entry := range entries {
			// Control records
			if entry.Control != nil {
				switch entry.Control.Type {
				case FlushOK:
					key := measurementKey(entry.Control.Database, entry.Control.Measurement)
					if _, exists := staging[key]; exists {
						delete(staging, key)
						stats.StagingCleared++
					}
				case FlushFail:
					// No-op: staging data remains, will be replayed at EOF
				}
				continue
			}

			// Data entries — accumulate in staging
			if entry.ColumnarData != nil {
				key := measurementKey(entry.ColumnarData.Database, entry.ColumnarData.Measurement)
				se, ok := staging[key]
				if !ok {
					se = &stagingEntry{
						columns: make(map[string][]interface{}),
					}
					staging[key] = se
				}
				// Merge columnar data into staging and count rows
				first := true
				for colName, values := range entry.ColumnarData.Columns {
					se.columns[colName] = append(se.columns[colName], values...)
					if first {
						se.recordCount += len(values)
						first = false
					}
				}
			} else if entry.Records != nil {
				// Row records carry _database and _measurement keys
				for _, rec := range entry.Records {
					db, _ := rec["_database"].(string)
					meas, _ := rec["_measurement"].(string)
					if meas == "" {
						meas, _ = rec["measurement"].(string)
					}
					if meas == "" {
						meas, _ = rec["m"].(string)
					}
					if meas == "" {
						continue
					}
					if db == "" {
						db, _ = rec["database"].(string)
					}
					if db == "" {
						db = "default"
					}
					key := measurementKey(db, meas)
					se, ok := staging[key]
					if !ok {
						se = &stagingEntry{}
						staging[key] = se
					}
					se.records = append(se.records, rec)
					se.recordCount++
				}
			}
		}

		stats.CorruptedEntries += int(reader.CorruptedEntries)

		// Incremental replay: flush oversized staging entries to bound memory.
		// Without FLUSH_OK markers (first boot, no flushes yet), staging can
		// accumulate O(total unconfirmed data). Replaying large measurements
		// after each file avoids OOM.
		replayOversizedStaging(ctx, staging, opts, callback, &stats.RecoveredEntries, &stats.RecoveredBatches, r.logger)

		// Track file for deferred deletion
		if len(entries) > 0 {
			recoveredFiles = append(recoveredFiles, walFile)
		} else {
			// Empty WAL file — delete immediately
			if err := os.Remove(walFile); err != nil {
				r.logger.Error().Err(err).Str("file", walFile).Msg("Failed to delete empty WAL file")
			} else {
				r.logger.Debug().Str("file", filepath.Base(walFile)).Msg("Deleted empty WAL file")
			}
		}
	}

	// Replay remaining staging data (no FLUSH_OK confirmation)
	recoveredEntries := 0
	recoveredBatches := 0
	replayFailed := false

	for key, se := range staging {
		database, measurement := splitMeasurementKey(key)

		// Replay columnar data
		if len(se.columns) > 0 {
			if opts.ColumnarCallback != nil {
				if err := opts.ColumnarCallback(ctx, database, measurement, se.columns); err != nil {
					r.logger.Error().Err(err).Str("measurement", key).Msg("Failed to replay columnar staging data")
					replayFailed = true
					continue
				}
				recoveredBatches++
				for _, col := range se.columns {
					recoveredEntries += len(col)
					break
				}
			}
		}

		// Replay row data
		if len(se.records) > 0 {
			replayed := replayRowStaging(ctx, callback, se.records, opts.BatchSize, r.logger, key)
			if replayed < len(se.records) {
				replayFailed = true
			}
			if replayed > 0 {
				recoveredBatches++
				recoveredEntries += replayed
			}
		}
	}

	// Delete recovered files ONLY after all staging data is successfully replayed.
	// If any replay failed, keep all files for retry on next restart.
	if !replayFailed {
		for _, walFile := range recoveredFiles {
			if err := os.Remove(walFile); err != nil {
				r.logger.Error().Err(err).Str("file", walFile).Msg("Failed to delete recovered WAL file")
			} else {
				stats.RecoveredFiles++
				r.logger.Info().
					Str("file", filepath.Base(walFile)).
					Msg("WAL file recovered and deleted")
			}
		}
	} else {
		r.logger.Warn().
			Int("staging_entries", len(staging)).
			Msg("WAL replay partially failed — keeping all files for retry on next restart")
	}

	stats.RecoveredBatches = recoveredBatches
	stats.RecoveredEntries = recoveredEntries
	stats.RecoveryDuration = time.Since(startTime)

	r.logger.Info().
		Int("files", stats.RecoveredFiles).
		Int("batches", stats.RecoveredBatches).
		Int("entries", stats.RecoveredEntries).
		Int("staging_cleared", stats.StagingCleared).
		Int("corrupted", stats.CorruptedEntries).
		Dur("duration", stats.RecoveryDuration).
		Msg("WAL recovery complete")

	return stats, nil
}

// splitMeasurementKey reverses measurementKey: "db/meas" → ("db", "meas")
func splitMeasurementKey(key string) (database, measurement string) {
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == '/' {
			return key[:i], key[i+1:]
		}
	}
	return key, key
}

// findWALFiles finds all WAL files in the directory, sorted by modification time
func (r *Recovery) findWALFiles() ([]string, error) {
	pattern := filepath.Join(r.walDir, "*.wal")
	walFiles, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	// Sort by modification time (oldest first)
	sort.Slice(walFiles, func(i, j int) bool {
		infoI, _ := os.Stat(walFiles[i])
		infoJ, _ := os.Stat(walFiles[j])
		if infoI == nil || infoJ == nil {
			return walFiles[i] < walFiles[j]
		}
		return infoI.ModTime().Before(infoJ.ModTime())
	})

	return walFiles, nil
}

// CleanupOldWALs removes legacy .recovered WAL files older than the specified age.
// Note: As of the current implementation, WAL files are deleted immediately after
// successful recovery, so this function is primarily for cleaning up legacy files
// from previous versions that renamed files to .recovered instead of deleting them.
func (r *Recovery) CleanupOldWALs(maxAge time.Duration) (int, int64, error) {
	pattern := filepath.Join(r.walDir, "*.wal.recovered")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return 0, 0, err
	}

	now := time.Now()
	deletedCount := 0
	freedBytes := int64(0)

	for _, file := range matches {
		info, err := os.Stat(file)
		if err != nil {
			continue
		}

		age := now.Sub(info.ModTime())
		if age > maxAge {
			size := info.Size()
			if err := os.Remove(file); err != nil {
				r.logger.Error().Err(err).Str("file", file).Msg("Failed to delete old WAL file")
				continue
			}
			deletedCount++
			freedBytes += size
			r.logger.Debug().Str("file", filepath.Base(file)).Msg("Deleted old WAL file")
		}
	}

	if deletedCount > 0 {
		r.logger.Info().
			Int("deleted", deletedCount).
			Int64("freed_bytes", freedBytes).
			Msg("Cleaned up old WAL files")
	}

	return deletedCount, freedBytes, nil
}

// ListWALFiles lists all WAL files in the directory.
// Returns active (pending) WAL files and legacy .recovered files.
// Note: As of the current implementation, WAL files are deleted immediately after
// successful recovery, so the recovered list will typically be empty or contain
// only legacy files from previous versions.
func (r *Recovery) ListWALFiles() (active []string, recovered []string, err error) {
	// Active WAL files (pending recovery)
	activePattern := filepath.Join(r.walDir, "*.wal")
	active, err = filepath.Glob(activePattern)
	if err != nil {
		return nil, nil, err
	}

	// Legacy recovered WAL files (from previous versions)
	recoveredPattern := filepath.Join(r.walDir, "*.wal.recovered")
	recovered, err = filepath.Glob(recoveredPattern)
	if err != nil {
		return nil, nil, err
	}

	return active, recovered, nil
}
