package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"iedb/internal/metrics"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/rs/zerolog"
)

// WAL file format constants
var (
	WALMagic   = []byte{'I', 'E', 'D', 'B'} // Magic bytes
	WALVersion = uint16(0x0001)             // Version 1
)

const (
	WALChecksumCRC32 = 0x01 // CRC32 checksum type

	// Entry format: [Length: 4 bytes] [Timestamp: 8 bytes] [Checksum: 4 bytes] [Payload: N bytes]
	WALEntryHeaderSize = 16
	WALFileHeaderSize  = 7 // Magic(4) + Version(2) + ChecksumType(1)

	// MaxWALPayloadSize is the maximum allowed payload size for a single WAL entry.
	// This limit prevents integer overflow during buffer allocation (CWE-190) and
	// aligns with the replication protocol limit (100MB).
	MaxWALPayloadSize = 100 * 1024 * 1024 // 100MB

	// WALEnvelopeMarker is the first byte of an enveloped WAL payload.
	// Enveloped format: [0x01][2-byte db name length][db name][original msgpack]
	// Since msgpack maps/arrays always start with bytes >= 0x80, 0x01 is unambiguous.
	WALEnvelopeMarker = 0x01

	// WALFlushOK is the payload marker for a FLUSH_OK control record.
	// Format: [0x00][db_len 2B BE][db_name][meas_len 2B BE][meas_name]
	WALFlushOK byte = 0x00

	// WALFlushFail is the payload marker for a FLUSH_FAIL control record.
	// Format: [0x02][db_len 2B BE][db_name][meas_len 2B BE][meas_name]
	WALFlushFail byte = 0x02
)

// ParseEnvelope extracts the database name and msgpack payload from a WAL entry.
// If the payload uses the envelope format [0x01][2-byte dbLen][dbName][msgpack],
// it returns the database name and the inner msgpack bytes. Otherwise, it returns
// defaultDB and the original payload unchanged.
func ParseEnvelope(payload []byte, defaultDB string) (database string, msgpackData []byte) {
	if len(payload) > 3 && payload[0] == WALEnvelopeMarker {
		dbLen := binary.BigEndian.Uint16(payload[1:3])
		if int(3+dbLen) <= len(payload) {
			return string(payload[3 : 3+dbLen]), payload[3+dbLen:]
		}
	}
	return defaultDB, payload
}

// SyncMode defines how WAL syncs to disk
type SyncMode string

const (
	SyncModeFsync     SyncMode = "fsync"     // Full sync: data + metadata (safest)
	SyncModeFdatasync SyncMode = "fdatasync" // Data sync only (balanced, default)
	SyncModeAsync     SyncMode = "async"     // No explicit sync (fastest, least safe)
)

// ErrPayloadTooLarge indicates the payload exceeds MaxWALPayloadSize.
var ErrPayloadTooLarge = errors.New("WAL payload exceeds maximum allowed size")

// ErrWALDropped is returned by Append/AppendRaw/AppendRawWithMeta when the
// async entry channel is full and the entry is dropped. Previous behavior
// returned nil and silently incremented DroppedEntries — callers logging
// "data preserved in WAL for recovery" downstream were reporting durability
// they did not actually have. Returning a sentinel lets callers (the
// ingestion buffer in particular) increment their own error counters and
// surface accurate operator-facing messages. Use errors.Is to detect.
var ErrWALDropped = errors.New("WAL entry dropped: async buffer full")

// ControlType identifies the type of a WAL control record.
type ControlType uint8

const (
	FlushOK   ControlType = 0x00 // measurement data successfully flushed to Parquet
	FlushFail ControlType = 0x02 // measurement flush failed (audit only, no-op on recovery)
)

// ControlRecord is a WAL entry that marks flush completion for a measurement.
type ControlRecord struct {
	Type        ControlType
	Database    string
	Measurement string
}

// walEntry is a pre-serialized WAL entry ready for writing
type walEntry struct {
	data []byte // Complete entry: header + payload
}

// WriterConfig holds configuration for WAL writer
type WriterConfig struct {
	WALDir       string        // Directory for WAL files
	SyncMode     SyncMode      // Sync mode: fsync, fdatasync, async
	MaxSizeBytes int64         // Rotate WAL when it reaches this size (default: 100MB)
	MaxAge       time.Duration // Rotate WAL after this duration (default: 45min = 3 × MaxBufferAgeSeconds)
	SyncInterval time.Duration // Sync at most this often (default: 1s, 0 = sync every write)
	SyncBytes    int64         // Sync after this many bytes written (default: 1MB, 0 = no byte threshold)
	BufferSize   int           // Size of async write buffer (default: 10000)
	Logger       zerolog.Logger
}

// ReplicationEntry represents a WAL entry for replication.
// This is passed to the replication hook for streaming to readers.
type ReplicationEntry struct {
	// Sequence is a monotonically increasing number for ordering
	Sequence uint64

	// TimestampUS is the entry timestamp in microseconds since epoch
	TimestampUS uint64

	// Payload is the raw msgpack data
	Payload []byte
}

// ReplicationHook is called for each WAL entry before it's written locally.
// This enables real-time streaming of entries to reader nodes.
type ReplicationHook func(entry *ReplicationEntry)

// Writer is a Write-Ahead Log writer with configurable durability
type Writer struct {
	config WriterConfig
	logger zerolog.Logger

	// Current WAL file
	currentFile *os.File
	currentPath string
	currentSize int64
	startTime   time.Time

	// Batched sync tracking
	lastSyncTime   time.Time // Last time we synced
	bytesSinceSync int64     // Bytes written since last sync

	// Async write buffer
	entryChan chan walEntry
	done      chan struct{}
	wg        sync.WaitGroup

	// Replication hook for streaming entries to readers
	replicationHook ReplicationHook
	sequence        uint64 // Monotonic sequence counter for replication

	// Metrics (atomic for lock-free reads)
	TotalEntries   int64
	TotalBytes     int64
	TotalSyncs     int64
	TotalRotations int64
	DroppedEntries int64 // Entries dropped due to full buffer
	WriteErrors    int64 // Entries lost due to disk write failure
	SyncErrors     int64 // fsync/fdatasync failures

	mu sync.Mutex
}

// NewWriter creates a new WAL writer
func NewWriter(cfg *WriterConfig) (*Writer, error) {
	// Set defaults
	if cfg.SyncMode == "" {
		cfg.SyncMode = SyncModeFdatasync
	}
	if cfg.MaxSizeBytes == 0 {
		cfg.MaxSizeBytes = 100 * 1024 * 1024 // 100MB
	}
	if cfg.MaxAge == 0 {
		// 45min = 3 × default MaxBufferAgeSeconds (15min). Hardcoded because
		// WAL WriterConfig and IngestConfig are independent — the WAL writer
		// does not know MaxBufferAgeSeconds. Operators who tune MaxBufferAgeSeconds
		// should also set max_age_seconds in the [wal] config section.
		cfg.MaxAge = 45 * time.Minute
	}
	// Default batched sync: every 1s OR every 1MB, whichever comes first.
	// 1s is a balanced default across storage media — NVMe/cloud SSD can go
	// lower (100-500ms); HDD/slow cloud disk may need 2-5s via config.
	if cfg.SyncInterval == 0 {
		cfg.SyncInterval = 1 * time.Second
	}
	if cfg.SyncBytes == 0 {
		cfg.SyncBytes = 1024 * 1024 // 1MB
	}
	if cfg.BufferSize < 1 {
		cfg.BufferSize = 10000 // Default buffer size
	} else if cfg.BufferSize > 1000000 {
		cfg.BufferSize = 1000000 // Cap to prevent excessive memory allocation
	}

	// Create WAL directory with owner-only permissions (WAL contains sensitive data)
	if err := os.MkdirAll(cfg.WALDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	w := &Writer{
		config:       *cfg,
		logger:       cfg.Logger.With().Str("component", "wal-writer").Logger(),
		lastSyncTime: time.Now(),
		entryChan:    make(chan walEntry, cfg.BufferSize),
		done:         make(chan struct{}),
	}

	// Initialize first WAL file
	if err := w.rotate(); err != nil {
		return nil, fmt.Errorf("failed to create initial WAL file: %w", err)
	}

	// Start async writer goroutine
	w.wg.Add(1)
	go w.writerLoop()

	w.logger.Info().
		Str("dir", cfg.WALDir).
		Str("sync_mode", string(cfg.SyncMode)).
		Int64("max_size_mb", cfg.MaxSizeBytes/1024/1024).
		Dur("max_age", cfg.MaxAge).
		Dur("sync_interval", cfg.SyncInterval).
		Int64("sync_bytes", cfg.SyncBytes).
		Int("buffer_size", cfg.BufferSize).
		Msg("WAL writer initialized (async mode)")

	return w, nil
}

// writerLoop is the background goroutine that writes entries to disk
func (w *Writer) writerLoop() {
	defer w.wg.Done()

	syncTicker := time.NewTicker(w.config.SyncInterval)
	defer syncTicker.Stop()

	for {
		select {
		case entry := <-w.entryChan:
			w.writeEntry(entry)

		case <-syncTicker.C:
			// Periodic sync
			w.mu.Lock()
			if w.bytesSinceSync > 0 {
				w.sync()
				w.lastSyncTime = time.Now()
				w.bytesSinceSync = 0
				atomic.AddInt64(&w.TotalSyncs, 1)
			}
			w.mu.Unlock()

		case <-w.done:
			// Drain remaining entries before shutdown
			for {
				select {
				case entry := <-w.entryChan:
					w.writeEntry(entry)
				default:
					// No more entries, final sync and exit
					w.mu.Lock()
					if w.bytesSinceSync > 0 {
						w.sync()
						atomic.AddInt64(&w.TotalSyncs, 1)
					}
					w.mu.Unlock()
					return
				}
			}
		}
	}
}

// writeEntry writes a single entry to the WAL file (called from writerLoop)
func (w *Writer) writeEntry(entry walEntry) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Write entry
	n, err := w.currentFile.Write(entry.data)
	if err != nil {
		atomic.AddInt64(&w.WriteErrors, 1)
		w.logger.Error().Err(err).
			Int64("total_write_errors", atomic.LoadInt64(&w.WriteErrors)).
			Msg("Failed to write WAL entry — data lost")
		return
	}

	bytesWritten := int64(n)
	w.currentSize += bytesWritten
	w.bytesSinceSync += bytesWritten

	// Update metrics
	atomic.AddInt64(&w.TotalEntries, 1)
	atomic.AddInt64(&w.TotalBytes, bytesWritten)

	// Sync if byte threshold exceeded
	if w.bytesSinceSync >= w.config.SyncBytes {
		w.sync()
		w.lastSyncTime = time.Now()
		w.bytesSinceSync = 0
		atomic.AddInt64(&w.TotalSyncs, 1)
	}

	// Check if rotation needed
	age := time.Since(w.startTime)
	if w.currentSize >= w.config.MaxSizeBytes || age >= w.config.MaxAge {
		if err := w.rotate(); err != nil {
			w.logger.Error().Err(err).Msg("Failed to rotate WAL")
		}
	}
}

// rotate creates a new WAL file. The old file is kept open until the new
// file is successfully created and its header written — if the new file
// cannot be opened, writes continue to the old file and rotation is
// retried on the next trigger. This prevents the nil-pointer crash that
// would occur if old file were closed before the new one is ready.
func (w *Writer) rotate() error {
	// Generate new filename
	timestamp := time.Now().UTC().Format("20060102_150405.000000000")
	filename := fmt.Sprintf("iedb-%s.wal", timestamp)
	newPath := filepath.Join(w.config.WALDir, filename)

	// Open new file with owner-only permissions (WAL contains sensitive data)
	newFile, err := os.OpenFile(newPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("failed to create WAL file: %w", err)
	}

	// Write WAL header (check byte count to guard against short writes)
	header := make([]byte, WALFileHeaderSize)
	copy(header[0:4], WALMagic)
	binary.BigEndian.PutUint16(header[4:6], WALVersion)
	header[6] = WALChecksumCRC32

	n, err := newFile.Write(header)
	if err != nil {
		newFile.Close()
		return fmt.Errorf("failed to write WAL header: %w", err)
	}
	if n != WALFileHeaderSize {
		newFile.Close()
		return fmt.Errorf("short write on WAL header: wrote %d of %d bytes", n, WALFileHeaderSize)
	}

	// New file is ready — now safely close the old one.
	oldPath := w.currentPath
	if w.currentFile != nil {
		if w.bytesSinceSync > 0 {
			w.sync()
			atomic.AddInt64(&w.TotalSyncs, 1)
		}
		if err := w.currentFile.Close(); err != nil {
			w.logger.Error().Err(err).
				Str("old_file", oldPath).
				Msg("Failed to close old WAL file after rotation")
		}
	}

	// Switch to the new file
	w.currentFile = newFile
	w.currentPath = newPath
	w.currentSize = int64(WALFileHeaderSize)
	w.startTime = time.Now()
	w.lastSyncTime = time.Now()
	w.bytesSinceSync = 0
	atomic.AddInt64(&w.TotalRotations, 1)

	w.logger.Info().
		Str("file", filename).
		Str("old_file", oldPath).
		Msg("WAL rotated")
	return nil
}

// Append writes records to the WAL asynchronously (non-blocking)
func (w *Writer) Append(records []map[string]interface{}) error {
	// Serialize records with MessagePack
	payload, err := msgpack.Marshal(records)
	if err != nil {
		return fmt.Errorf("failed to serialize records: %w", err)
	}

	return w.AppendRaw(payload)
}

// AppendRawWithMeta writes raw msgpack bytes with database metadata envelope.
// Format: [0x01 marker][2-byte db name length][db name][original msgpack]
// This preserves the database name for correct recovery routing.
//
// Unlike calling AppendRaw with a pre-built envelope, this method builds the
// WAL entry in a single allocation to avoid copying the payload twice.
func (w *Writer) AppendRawWithMeta(database string, payload []byte) error {
	dbBytes := []byte(database)
	envelopeHeaderLen := 1 + 2 + len(dbBytes) // marker + dbLen + dbName
	totalPayloadLen := envelopeHeaderLen + len(payload)

	if totalPayloadLen > MaxWALPayloadSize {
		return fmt.Errorf("%w: size %d exceeds limit %d", ErrPayloadTooLarge, totalPayloadLen, MaxWALPayloadSize)
	}

	// Compute CRC32 over the logical payload (envelope header + msgpack) without copying
	crc := crc32.NewIEEE()
	var envHeader [1 + 2 + 255]byte // envelope header (db name max 255 bytes)
	envHeader[0] = WALEnvelopeMarker
	binary.BigEndian.PutUint16(envHeader[1:3], uint16(len(dbBytes)))
	copy(envHeader[3:], dbBytes)
	crc.Write(envHeader[:envelopeHeaderLen])
	crc.Write(payload)
	checksum := crc.Sum32()

	timestampUS := uint64(time.Now().UnixMicro())

	// Replication hook
	if w.replicationHook != nil {
		w.mu.Lock()
		w.sequence++
		seq := w.sequence
		hook := w.replicationHook
		w.mu.Unlock()

		// Build envelope for replication (unavoidable copy for hook consumers)
		repPayload := make([]byte, totalPayloadLen)
		copy(repPayload, envHeader[:envelopeHeaderLen])
		copy(repPayload[envelopeHeaderLen:], payload)
		hook(&ReplicationEntry{
			Sequence:    seq,
			TimestampUS: timestampUS,
			Payload:     repPayload,
		})
	}

	// Build complete WAL entry in one allocation: header + envelope header + payload
	entryData := make([]byte, WALEntryHeaderSize+totalPayloadLen)
	binary.BigEndian.PutUint32(entryData[0:4], uint32(totalPayloadLen))
	binary.BigEndian.PutUint64(entryData[4:12], timestampUS)
	binary.BigEndian.PutUint32(entryData[12:16], checksum)
	copy(entryData[WALEntryHeaderSize:], envHeader[:envelopeHeaderLen])
	copy(entryData[WALEntryHeaderSize+envelopeHeaderLen:], payload)

	return w.tryEnqueue(entryData)
}

// tryEnqueue is the shared non-blocking send into entryChan used by
// every Append variant. On channel-full it bumps the dropped counter
// (both on the Writer and the global metrics package) and returns
// ErrWALDropped — callers use errors.Is to differentiate from real
// I/O errors. Centralized so the drop accounting is impossible to
// drift across the multiple append paths.
func (w *Writer) tryEnqueue(entryData []byte) error {
	select {
	case w.entryChan <- walEntry{data: entryData}:
		return nil
	default:
		atomic.AddInt64(&w.DroppedEntries, 1)
		metrics.Get().IncWALDroppedEntries()
		return ErrWALDropped
	}
}

// AppendRaw writes raw (already serialized) msgpack bytes to the WAL asynchronously
// This is a zero-copy optimization - use this when you already have msgpack bytes
func (w *Writer) AppendRaw(payload []byte) error {
	// Validate payload size to prevent integer overflow during allocation (CWE-190)
	if len(payload) > MaxWALPayloadSize {
		return fmt.Errorf("%w: size %d exceeds limit %d", ErrPayloadTooLarge, len(payload), MaxWALPayloadSize)
	}

	// Calculate checksum (CRC32)
	checksum := crc32.ChecksumIEEE(payload)

	// Get current timestamp (microseconds since epoch)
	timestampUS := uint64(time.Now().UnixMicro())

	// Call replication hook before local write (if set)
	// This enables real-time streaming to reader nodes
	if w.replicationHook != nil {
		w.mu.Lock()
		w.sequence++
		seq := w.sequence
		hook := w.replicationHook
		w.mu.Unlock()

		hook(&ReplicationEntry{
			Sequence:    seq,
			TimestampUS: timestampUS,
			Payload:     payload,
		})
	}

	// Build complete entry: header + payload
	entryData := make([]byte, WALEntryHeaderSize+len(payload))
	binary.BigEndian.PutUint32(entryData[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint64(entryData[4:12], timestampUS)
	binary.BigEndian.PutUint32(entryData[12:16], checksum)
	copy(entryData[WALEntryHeaderSize:], payload)

	return w.tryEnqueue(entryData)
}

// buildControlEntry constructs a complete WAL entry for a control record.
// Format: header(16B) + [marker 1B][db_len 2B BE][db_name][meas_len 2B BE][meas_name]
func (w *Writer) buildControlEntry(ctrlType ControlType, database, measurement string) []byte {
	dbBytes := []byte(database)
	measBytes := []byte(measurement)
	payloadLen := 1 + 2 + len(dbBytes) + 2 + len(measBytes)

	payload := make([]byte, payloadLen)
	payload[0] = byte(ctrlType)
	binary.BigEndian.PutUint16(payload[1:3], uint16(len(dbBytes)))
	copy(payload[3:], dbBytes)
	binary.BigEndian.PutUint16(payload[3+len(dbBytes):], uint16(len(measBytes)))
	copy(payload[3+len(dbBytes)+2:], measBytes)

	checksum := crc32.ChecksumIEEE(payload)
	timestampUS := uint64(time.Now().UnixMicro())

	entryData := make([]byte, WALEntryHeaderSize+payloadLen)
	binary.BigEndian.PutUint32(entryData[0:4], uint32(payloadLen))
	binary.BigEndian.PutUint64(entryData[4:12], timestampUS)
	binary.BigEndian.PutUint32(entryData[12:16], checksum)
	copy(entryData[WALEntryHeaderSize:], payload)

	return entryData
}

// AppendControl writes a control record (FLUSH_OK or FLUSH_FAIL) to the WAL.
// Control records MUST not be dropped — they are essential for correct recovery.
// The method first tries the async channel (with retry backoff), then falls back
// to a synchronous write under the writer lock. The synchronous write includes an
// immediate fsync to guarantee the control record is durable before the caller
// proceeds (e.g., before PurgeAll deletes the WAL file on clean shutdown).
func (w *Writer) AppendControl(ctrlType ControlType, database, measurement string) error {
	entryData := w.buildControlEntry(ctrlType, database, measurement)

	// Try async send with exponential backoff (1ms, 2ms, 4ms, 8ms, 16ms).
	// Check w.done on each attempt so we don't spin during shutdown.
	for attempt := 0; attempt < 5; attempt++ {
		select {
		case w.entryChan <- walEntry{data: entryData}:
			return nil
		case <-w.done:
			// Shutting down — exit retry loop to fall through to synchronous write.
			attempt = 5 // break out of for loop
		default:
			if attempt < 4 {
				time.Sleep(time.Millisecond << attempt)
			}
		}
	}

	// Fallback: synchronous write under lock, with immediate fsync.
	w.mu.Lock()
	defer w.mu.Unlock()

	w.logger.Warn().
		Str("database", database).
		Str("measurement", measurement).
		Uint8("ctrl_type", uint8(ctrlType)).
		Msg("Control record channel full, writing synchronously")

	if w.currentFile == nil {
		return fmt.Errorf("cannot write control record: no WAL file open")
	}

	n, err := w.currentFile.Write(entryData)
	if err != nil {
		return fmt.Errorf("failed to write control record synchronously: %w", err)
	}

	bytesWritten := int64(n)
	w.currentSize += bytesWritten
	w.bytesSinceSync += bytesWritten

	atomic.AddInt64(&w.TotalEntries, 1)
	atomic.AddInt64(&w.TotalBytes, bytesWritten)

	// Sync immediately: control records must be durable before caller
	// considers the operation complete (e.g., before WAL PurgeAll).
	w.sync()
	w.lastSyncTime = time.Now()
	w.bytesSinceSync = 0
	atomic.AddInt64(&w.TotalSyncs, 1)

	// Check rotation
	if w.currentSize >= w.config.MaxSizeBytes || time.Since(w.startTime) >= w.config.MaxAge {
		if err := w.rotate(); err != nil {
			w.logger.Error().Err(err).Msg("Failed to rotate WAL after control record")
		}
	}

	return nil
}

// parseControlPayload extracts database and measurement from a control record payload.
// Payload format: [marker 1B][db_len 2B BE][db_name][meas_len 2B BE][meas_name]
func parseControlPayload(payload []byte) (database, measurement string) {
	if len(payload) < 4 {
		return "", ""
	}
	dbLen := binary.BigEndian.Uint16(payload[1:3])
	if int(3+dbLen+2) > len(payload) {
		return "", ""
	}
	database = string(payload[3 : 3+dbLen])
	measOffset := 3 + int(dbLen)
	measLen := binary.BigEndian.Uint16(payload[measOffset : measOffset+2])
	if measOffset+2+int(measLen) > len(payload) {
		return database, ""
	}
	measurement = string(payload[measOffset+2 : measOffset+2+int(measLen)])
	return database, measurement
}

// sync syncs the WAL file to disk based on sync mode.
// Errors are logged and tracked via SyncErrors counter — callers do not
// receive the error because sync is advisory (the OS will eventually
// flush even if fsync fails).
func (w *Writer) sync() {
	if w.currentFile == nil {
		return
	}

	var err error
	switch w.config.SyncMode {
	case SyncModeFsync:
		err = w.currentFile.Sync()
	case SyncModeFdatasync:
		err = w.currentFile.Sync()
	case SyncModeAsync:
		return
	}
	if err != nil {
		atomic.AddInt64(&w.SyncErrors, 1)
		w.logger.Error().Err(err).
			Int64("total_sync_errors", atomic.LoadInt64(&w.SyncErrors)).
			Msg("WAL fsync failed — data may not be durable")
	}
}

// Close closes the WAL writer
func (w *Writer) Close() error {
	// Signal shutdown
	close(w.done)

	// Wait for writer goroutine to finish
	w.wg.Wait()

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.currentFile != nil {
		err := w.currentFile.Close()
		w.currentFile = nil
		w.logger.Info().
			Str("file", w.currentPath).
			Int64("dropped_entries", atomic.LoadInt64(&w.DroppedEntries)).
			Msg("WAL closed")
		return err
	}
	return nil
}

// purgeWALFiles deletes WAL files matching the given filter function.
// Returns the count of deleted files.
func (w *Writer) purgeWALFiles(shouldDelete func(path string) bool) (int, error) {
	pattern := filepath.Join(w.config.WALDir, "*.wal")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return 0, err
	}

	deleted := 0
	for _, f := range files {
		if shouldDelete(f) {
			if err := os.Remove(f); err != nil {
				w.logger.Error().Err(err).Str("file", f).Msg("Failed to purge WAL file")
			} else {
				deleted++
			}
		}
	}
	return deleted, nil
}

// PurgeAll deletes all WAL files in the directory.
// Call this after a clean shutdown where all data has been flushed to storage,
// so that recovery on next startup doesn't replay already-persisted data.
func (w *Writer) PurgeAll() (int, error) {
	deleted, err := w.purgeWALFiles(func(_ string) bool { return true })
	if deleted > 0 {
		w.logger.Info().Int("deleted", deleted).Msg("Purged WAL files after clean shutdown")
	}
	return deleted, err
}

// PurgeInactive deletes all WAL files except the currently active one.
// Use this during normal operation (unlike PurgeAll which is for shutdown)
// to clean up rotated WAL files after their data has been flushed to storage.
func (w *Writer) PurgeInactive() (int, error) {
	w.mu.Lock()
	activePath := w.currentPath
	w.mu.Unlock()

	deleted, err := w.purgeWALFiles(func(path string) bool {
		return path != activePath
	})
	if deleted > 0 {
		w.logger.Info().Int("deleted", deleted).Msg("Purged inactive WAL files")
	}
	return deleted, err
}

// PurgeOlderThan deletes inactive WAL files whose modification time is older
// than the given threshold. The active WAL file is never deleted.
// Use this during normal operation to safely purge rotated WAL files whose
// data has been flushed to parquet by the normal buffer flush cycle.
func (w *Writer) PurgeOlderThan(minAge time.Duration) (int, error) {
	w.mu.Lock()
	activePath := w.currentPath
	w.mu.Unlock()

	now := time.Now()
	deleted, err := w.purgeWALFiles(func(path string) bool {
		if path == activePath {
			return false
		}
		info, statErr := os.Stat(path)
		if statErr != nil {
			return false
		}
		return now.Sub(info.ModTime()) > minAge
	})
	if deleted > 0 {
		w.logger.Info().Int("deleted", deleted).Dur("min_age", minAge).Msg("Purged old WAL files")
	}
	return deleted, err
}

// Stats returns WAL statistics
func (w *Writer) Stats() map[string]interface{} {
	w.mu.Lock()
	defer w.mu.Unlock()

	age := time.Since(w.startTime)
	return map[string]interface{}{
		"current_file":        w.currentPath,
		"current_size_mb":     float64(w.currentSize) / 1024 / 1024,
		"current_age_seconds": age.Seconds(),
		"sync_mode":           string(w.config.SyncMode),
		"total_entries":       atomic.LoadInt64(&w.TotalEntries),
		"total_bytes":         atomic.LoadInt64(&w.TotalBytes),
		"total_syncs":         atomic.LoadInt64(&w.TotalSyncs),
		"total_rotations":     atomic.LoadInt64(&w.TotalRotations),
		"dropped_entries":     atomic.LoadInt64(&w.DroppedEntries),
		"write_errors":        atomic.LoadInt64(&w.WriteErrors),
		"sync_errors":         atomic.LoadInt64(&w.SyncErrors),
		"buffer_size":         w.config.BufferSize,
		"buffer_used":         len(w.entryChan),
	}
}

// CurrentFile returns the current WAL file path
func (w *Writer) CurrentFile() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.currentPath
}

// SetReplicationHook sets the hook function called for each WAL entry.
// This enables cluster replication by streaming entries to reader nodes.
// The hook is called synchronously before the entry is written locally.
func (w *Writer) SetReplicationHook(hook ReplicationHook) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.replicationHook = hook
	w.logger.Info().Msg("Replication hook set")
}

// CurrentSequence returns the current replication sequence number.
func (w *Writer) CurrentSequence() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sequence
}
