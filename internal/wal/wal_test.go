package wal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func newTestWriter(t *testing.T, syncMode SyncMode) (*Writer, string) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "wal-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	cfg := &WriterConfig{
		WALDir:       tmpDir,
		SyncMode:     syncMode,
		MaxSizeBytes: 10 * 1024 * 1024, // 10MB
		MaxAge:       time.Hour,
		SyncInterval: 100 * time.Millisecond,
		SyncBytes:    1024 * 1024,
		BufferSize:   1000,
		Logger:       zerolog.Nop(),
	}

	writer, err := NewWriter(cfg)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create writer: %v", err)
	}

	return writer, tmpDir
}

func TestNewWriter(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeFdatasync)
	defer os.RemoveAll(tmpDir)
	defer writer.Close()

	// Check that WAL directory was created
	if _, err := os.Stat(tmpDir); os.IsNotExist(err) {
		t.Error("WAL directory was not created")
	}

	// Check that a WAL file was created
	files, _ := filepath.Glob(filepath.Join(tmpDir, "*.wal"))
	if len(files) != 1 {
		t.Errorf("expected 1 WAL file, got %d", len(files))
	}

	// Check current file path is set
	if writer.CurrentFile() == "" {
		t.Error("current file path should not be empty")
	}
}

func TestWriter_Defaults(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "wal-test-defaults-*")
	defer os.RemoveAll(tmpDir)

	cfg := &WriterConfig{
		WALDir: tmpDir,
		Logger: zerolog.Nop(),
	}

	writer, err := NewWriter(cfg)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}
	defer writer.Close()

	// Check defaults were applied
	if writer.config.SyncMode != SyncModeFdatasync {
		t.Errorf("default sync mode: got %s, want %s", writer.config.SyncMode, SyncModeFdatasync)
	}
	if writer.config.MaxSizeBytes != 100*1024*1024 {
		t.Errorf("default max size: got %d, want %d", writer.config.MaxSizeBytes, 100*1024*1024)
	}
	if writer.config.MaxAge != 45*time.Minute {
		t.Errorf("default max age: got %v, want %v", writer.config.MaxAge, 45*time.Minute)
	}
}

func TestWriter_Append(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)

	records := []map[string]interface{}{
		{
			"measurement": "cpu",
			"time":        int64(1609459200000000),
			"host":        "server01",
			"usage":       90.5,
		},
		{
			"measurement": "memory",
			"time":        int64(1609459200000001),
			"host":        "server01",
			"free":        int64(1024),
		},
	}

	err := writer.Append(records)
	if err != nil {
		t.Errorf("Append failed: %v", err)
	}

	// Give async writer time to process
	time.Sleep(50 * time.Millisecond)

	// Close to ensure all data is written
	writer.Close()

	// Verify stats
	stats := writer.Stats()
	if stats["total_entries"].(int64) < 1 {
		t.Errorf("expected at least 1 entry, got %d", stats["total_entries"])
	}
}

func TestWriter_AppendRaw(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)

	// Pre-serialized msgpack data
	payload := []byte{0x91, 0x81, 0xa1, 0x6d, 0xa3, 0x63, 0x70, 0x75} // Simple msgpack

	err := writer.AppendRaw(payload)
	if err != nil {
		t.Errorf("AppendRaw failed: %v", err)
	}

	// Give async writer time to process
	time.Sleep(50 * time.Millisecond)

	writer.Close()

	if atomic.LoadInt64(&writer.TotalEntries) < 1 {
		t.Errorf("expected at least 1 entry, got %d", atomic.LoadInt64(&writer.TotalEntries))
	}
}

func TestWriter_SyncModes(t *testing.T) {
	modes := []SyncMode{SyncModeFsync, SyncModeFdatasync, SyncModeAsync}

	for _, mode := range modes {
		t.Run(string(mode), func(t *testing.T) {
			writer, tmpDir := newTestWriter(t, mode)
			defer os.RemoveAll(tmpDir)

			records := []map[string]interface{}{
				{"measurement": "test", "value": 1.0},
			}

			err := writer.Append(records)
			if err != nil {
				t.Errorf("Append failed with mode %s: %v", mode, err)
			}

			time.Sleep(50 * time.Millisecond)
			writer.Close()

			if atomic.LoadInt64(&writer.TotalEntries) < 1 {
				t.Errorf("mode %s: expected at least 1 entry", mode)
			}
		})
	}
}

func TestWriter_Stats(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)

	stats := writer.Stats()

	// Check required stat fields
	requiredFields := []string{
		"current_file",
		"current_size_mb",
		"current_age_seconds",
		"sync_mode",
		"total_entries",
		"total_bytes",
		"total_syncs",
		"total_rotations",
		"dropped_entries",
		"buffer_size",
		"buffer_used",
	}

	for _, field := range requiredFields {
		if _, ok := stats[field]; !ok {
			t.Errorf("missing stat field: %s", field)
		}
	}

	writer.Close()
}

func TestWriter_Close(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)

	// Write some data
	records := []map[string]interface{}{
		{"measurement": "test", "value": 1.0},
	}
	writer.Append(records)

	// Give async writer time to process
	time.Sleep(50 * time.Millisecond)

	// Close should not error
	err := writer.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Verify data was written
	if atomic.LoadInt64(&writer.TotalEntries) < 1 {
		t.Errorf("expected at least 1 entry after close, got %d", atomic.LoadInt64(&writer.TotalEntries))
	}
}

func TestReader_ReadAll(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)

	// Write test records
	records := []map[string]interface{}{
		{
			"measurement": "cpu",
			"time":        int64(1609459200000000),
			"host":        "server01",
			"usage":       90.5,
		},
	}

	writer.Append(records)
	time.Sleep(50 * time.Millisecond)

	walFile := writer.CurrentFile()
	writer.Close()

	// Read back
	reader := NewReader(walFile, zerolog.Nop())
	entries, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(entries) < 1 {
		t.Errorf("expected at least 1 entry, got %d", len(entries))
	}

	// Verify entry content
	if len(entries) > 0 && len(entries[0].Records) > 0 {
		rec := entries[0].Records[0]
		if rec["measurement"] != "cpu" {
			t.Errorf("measurement: got %v, want 'cpu'", rec["measurement"])
		}
	}
}

func TestReader_InvalidMagic(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "wal-test-invalid-*")
	defer os.RemoveAll(tmpDir)

	// Create file with invalid magic
	invalidFile := filepath.Join(tmpDir, "invalid.wal")
	os.WriteFile(invalidFile, []byte("BADM0000"), 0600)

	reader := NewReader(invalidFile, zerolog.Nop())
	_, err := reader.ReadAll()
	if err == nil {
		t.Error("expected error for invalid magic bytes")
	}
}

func TestReader_CorruptedChecksum(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)

	// Write valid data
	records := []map[string]interface{}{
		{"measurement": "cpu", "value": 90.5},
	}
	writer.Append(records)
	time.Sleep(50 * time.Millisecond)

	walFile := writer.CurrentFile()
	writer.Close()

	// Corrupt the file by modifying bytes after the header
	data, _ := os.ReadFile(walFile)
	if len(data) > 30 {
		data[30] ^= 0xFF // Flip some bits in the payload
		os.WriteFile(walFile, data, 0600)
	}

	// Read should handle corrupted entries gracefully
	reader := NewReader(walFile, zerolog.Nop())
	_, err := reader.ReadAll()
	// Error is acceptable, panic is not
	_ = err
}

func TestRecovery_Recover(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)

	// Write test records
	records := []map[string]interface{}{
		{
			"measurement": "cpu",
			"time":        int64(1609459200000000),
			"host":        "server01",
			"usage":       90.5,
		},
	}

	writer.Append(records)
	time.Sleep(50 * time.Millisecond)
	writer.Close()

	// Recover
	recovery := NewRecovery(tmpDir, zerolog.Nop())

	var recoveredRecords []map[string]interface{}
	callback := func(ctx context.Context, records []map[string]interface{}) error {
		recoveredRecords = append(recoveredRecords, records...)
		return nil
	}

	ctx := context.Background()
	stats, err := recovery.Recover(ctx, callback)
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if stats.RecoveredFiles < 1 {
		t.Errorf("expected at least 1 recovered file, got %d", stats.RecoveredFiles)
	}

	if len(recoveredRecords) < 1 {
		t.Errorf("expected at least 1 recovered record, got %d", len(recoveredRecords))
	}

	// Check that WAL file was deleted after successful recovery
	activeFiles, _, _ := recovery.ListWALFiles()
	if len(activeFiles) != 0 {
		t.Errorf("expected 0 active files after recovery, got %d", len(activeFiles))
	}
}

func TestRecovery_NoWALDirectory(t *testing.T) {
	recovery := NewRecovery("/nonexistent/path", zerolog.Nop())

	stats, err := recovery.Recover(context.Background(), func(ctx context.Context, records []map[string]interface{}) error {
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if stats.RecoveredFiles != 0 {
		t.Errorf("expected 0 recovered files for nonexistent dir, got %d", stats.RecoveredFiles)
	}
}

func TestRecovery_EmptyDirectory(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "wal-test-empty-*")
	defer os.RemoveAll(tmpDir)

	recovery := NewRecovery(tmpDir, zerolog.Nop())

	stats, err := recovery.Recover(context.Background(), func(ctx context.Context, records []map[string]interface{}) error {
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if stats.RecoveredFiles != 0 {
		t.Errorf("expected 0 recovered files for empty dir, got %d", stats.RecoveredFiles)
	}
}

func TestRecovery_ContextCancellation(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)

	// Write test records
	records := []map[string]interface{}{
		{"measurement": "cpu", "value": 90.5},
	}
	writer.Append(records)
	time.Sleep(50 * time.Millisecond)
	writer.Close()

	// Recover with cancelled context
	recovery := NewRecovery(tmpDir, zerolog.Nop())

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := recovery.Recover(ctx, func(ctx context.Context, records []map[string]interface{}) error {
		return nil
	})

	if err != context.Canceled {
		t.Errorf("expected context.Canceled error, got %v", err)
	}
}

func TestRecovery_CleanupOldWALs(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "wal-test-cleanup-*")
	defer os.RemoveAll(tmpDir)

	// Create some old recovered files
	oldFile := filepath.Join(tmpDir, "old.wal.recovered")
	os.WriteFile(oldFile, []byte("old data"), 0600)

	// Set file modification time to past
	oldTime := time.Now().Add(-48 * time.Hour)
	os.Chtimes(oldFile, oldTime, oldTime)

	recovery := NewRecovery(tmpDir, zerolog.Nop())

	// Cleanup files older than 24 hours
	deleted, freed, err := recovery.CleanupOldWALs(24 * time.Hour)
	if err != nil {
		t.Fatalf("CleanupOldWALs failed: %v", err)
	}

	if deleted != 1 {
		t.Errorf("expected 1 deleted file, got %d", deleted)
	}
	if freed == 0 {
		t.Error("expected non-zero freed bytes")
	}

	// Verify file was deleted
	if _, err := os.Stat(oldFile); !os.IsNotExist(err) {
		t.Error("old file should have been deleted")
	}
}

func TestRecovery_ListWALFiles(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "wal-test-list-*")
	defer os.RemoveAll(tmpDir)

	// Create mix of files
	os.WriteFile(filepath.Join(tmpDir, "active1.wal"), []byte("data"), 0600)
	os.WriteFile(filepath.Join(tmpDir, "active2.wal"), []byte("data"), 0600)
	os.WriteFile(filepath.Join(tmpDir, "old.wal.recovered"), []byte("data"), 0600)
	os.WriteFile(filepath.Join(tmpDir, "other.txt"), []byte("data"), 0600)

	recovery := NewRecovery(tmpDir, zerolog.Nop())

	active, recovered, err := recovery.ListWALFiles()
	if err != nil {
		t.Fatalf("ListWALFiles failed: %v", err)
	}

	if len(active) != 2 {
		t.Errorf("expected 2 active files, got %d", len(active))
	}
	if len(recovered) != 1 {
		t.Errorf("expected 1 recovered file, got %d", len(recovered))
	}
}

func TestWALFileFormat(t *testing.T) {
	// Test WAL constants
	if len(WALMagic) != 4 {
		t.Errorf("WALMagic should be 4 bytes, got %d", len(WALMagic))
	}
	if string(WALMagic) != "IEDB" {
		t.Errorf("WALMagic should be 'IEDB', got %q", string(WALMagic))
	}
	if WALVersion != 0x0001 {
		t.Errorf("WALVersion should be 0x0001, got 0x%04x", WALVersion)
	}
	if WALFileHeaderSize != 7 {
		t.Errorf("WALFileHeaderSize should be 7, got %d", WALFileHeaderSize)
	}
	if WALEntryHeaderSize != 16 {
		t.Errorf("WALEntryHeaderSize should be 16, got %d", WALEntryHeaderSize)
	}
}

// Benchmark tests
func BenchmarkWriter_Append(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "wal-bench-*")
	defer os.RemoveAll(tmpDir)

	cfg := &WriterConfig{
		WALDir:       tmpDir,
		SyncMode:     SyncModeAsync,
		MaxSizeBytes: 1024 * 1024 * 1024, // 1GB to prevent rotation
		BufferSize:   100000,
		Logger:       zerolog.Nop(),
	}

	writer, _ := NewWriter(cfg)
	defer writer.Close()

	records := []map[string]interface{}{
		{
			"measurement": "cpu",
			"time":        int64(1609459200000000),
			"host":        "server01",
			"usage":       90.5,
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writer.Append(records)
	}
}

func BenchmarkWriter_AppendRaw(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "wal-bench-raw-*")
	defer os.RemoveAll(tmpDir)

	cfg := &WriterConfig{
		WALDir:       tmpDir,
		SyncMode:     SyncModeAsync,
		MaxSizeBytes: 1024 * 1024 * 1024,
		BufferSize:   100000,
		Logger:       zerolog.Nop(),
	}

	writer, _ := NewWriter(cfg)
	defer writer.Close()

	payload := []byte{0x91, 0x84, 0xa1, 0x6d, 0xa3, 0x63, 0x70, 0x75, 0xa1, 0x74, 0xcf, 0x00, 0x05, 0xb7, 0x8d, 0x8d, 0x40, 0x00, 0x00, 0xa1, 0x68, 0xa8, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x30, 0x31, 0xa5, 0x75, 0x73, 0x61, 0x67, 0x65, 0xcb, 0x40, 0x56, 0xa0, 0x00, 0x00, 0x00, 0x00, 0x00}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writer.AppendRaw(payload)
	}
}

func TestWriter_AppendRaw_PayloadTooLarge(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)
	defer writer.Close()

	// Create payload exceeding the limit
	payload := make([]byte, MaxWALPayloadSize+1)

	err := writer.AppendRaw(payload)
	if err == nil {
		t.Fatal("AppendRaw should fail for oversized payload")
	}
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Errorf("Expected ErrPayloadTooLarge, got: %v", err)
	}
}

func TestWriter_AppendRaw_PayloadTooLarge_ErrorMessage(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)
	defer writer.Close()

	oversizeAmount := 1000
	payload := make([]byte, MaxWALPayloadSize+oversizeAmount)

	err := writer.AppendRaw(payload)
	if err == nil {
		t.Fatal("Expected error for oversized payload")
	}

	// Verify error message contains useful information
	errMsg := err.Error()
	if !strings.Contains(errMsg, "104858600") { // MaxWALPayloadSize + 1000
		t.Errorf("Error message should contain actual size, got: %s", errMsg)
	}
	if !strings.Contains(errMsg, "104857600") { // MaxWALPayloadSize
		t.Errorf("Error message should contain limit, got: %s", errMsg)
	}
}

func TestWriter_AppendRaw_PayloadAtLimit(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)
	defer writer.Close()

	// Create payload exactly at the limit - should succeed
	payload := make([]byte, MaxWALPayloadSize)

	err := writer.AppendRaw(payload)
	if err != nil {
		t.Errorf("AppendRaw should succeed for payload at limit: %v", err)
	}
}

// TestRecovery_FlushOKStaging verifies the new staging-based recovery:
// data → FLUSH_OK → data for same measurement: only unconfirmed data is replayed.
func TestRecovery_FlushOKStaging(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-test-staging-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	writer, err := NewWriter(&WriterConfig{
		WALDir:       tmpDir,
		SyncMode:     SyncModeAsync,
		MaxSizeBytes: 100 * 1024 * 1024,
		Logger:       zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	// Write data for cpu measurement
	writer.Append([]map[string]interface{}{
		{"_measurement": "cpu", "value": 100.0},
	})
	time.Sleep(50 * time.Millisecond)

	// Write FLUSH_OK for cpu — this confirms the data is in Parquet
	writer.AppendControl(FlushOK, "default", "cpu")
	time.Sleep(50 * time.Millisecond)

	// Write more data for cpu — this is NOT confirmed (no FLUSH_OK)
	writer.Append([]map[string]interface{}{
		{"_measurement": "cpu", "value": 200.0},
	})
	time.Sleep(50 * time.Millisecond)

	writer.Close()

	// Recover — only the unconfirmed data (value=200) should be replayed
	recovery := NewRecovery(tmpDir, zerolog.Nop())

	var recoveredValues []float64
	callback := func(ctx context.Context, records []map[string]interface{}) error {
		for _, r := range records {
			if v, ok := r["value"].(float64); ok {
				recoveredValues = append(recoveredValues, v)
			}
		}
		return nil
	}

	stats, err := recovery.RecoverWithOptions(context.Background(), callback, nil)
	if err != nil {
		t.Fatalf("RecoverWithOptions failed: %v", err)
	}

	// Only the unconfirmed entry should be replayed
	if len(recoveredValues) != 1 {
		t.Fatalf("expected 1 recovered value, got %d: %v", len(recoveredValues), recoveredValues)
	}
	if recoveredValues[0] != 200.0 {
		t.Errorf("expected recovered value 200.0, got %v", recoveredValues[0])
	}

	// Staging cleared stat
	if stats.StagingCleared != 1 {
		t.Errorf("expected 1 staging cleared, got %d", stats.StagingCleared)
	}

	// The single remaining staging entry should be replayed
	if stats.RecoveredEntries != 1 {
		t.Errorf("expected 1 recovered entry, got %d", stats.RecoveredEntries)
	}

	t.Logf("FLUSH_OK staging: %d entries replayed, %d staging cleared", stats.RecoveredEntries, stats.StagingCleared)
}

func TestRecovery_BatchSize(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)

	// Write a batch of 10 records
	records := make([]map[string]interface{}, 10)
	for i := 0; i < 10; i++ {
		records[i] = map[string]interface{}{
			"measurement": "cpu",
			"index":       i,
			"value":       float64(i * 10),
		}
	}
	writer.Append(records)
	time.Sleep(50 * time.Millisecond)
	writer.Close()

	// Recover with BatchSize = 3 (should split into 4 batches: 3+3+3+1)
	recovery := NewRecovery(tmpDir, zerolog.Nop())

	var batchCount int
	var totalRecords int
	callback := func(ctx context.Context, records []map[string]interface{}) error {
		batchCount++
		totalRecords += len(records)
		// Verify batch size limit
		if len(records) > 3 {
			t.Errorf("batch size exceeded limit: got %d, max 3", len(records))
		}
		return nil
	}

	stats, err := recovery.RecoverWithOptions(context.Background(), callback, &RecoveryOptions{
		BatchSize: 3,
	})
	if err != nil {
		t.Fatalf("RecoverWithOptions failed: %v", err)
	}

	// Should have split into 4 batches (3+3+3+1=10)
	if batchCount != 4 {
		t.Errorf("expected 4 batches, got %d", batchCount)
	}

	if totalRecords != 10 {
		t.Errorf("expected 10 total records, got %d", totalRecords)
	}

	if stats.RecoveredEntries != 10 {
		t.Errorf("expected 10 recovered entries in stats, got %d", stats.RecoveredEntries)
	}
}

func TestRecovery_PartialFailure_KeepsFileForRetry(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)

	// Write test records
	records := []map[string]interface{}{
		{"measurement": "cpu", "value": 1.0},
		{"measurement": "cpu", "value": 2.0},
		{"measurement": "cpu", "value": 3.0},
	}
	writer.Append(records)
	time.Sleep(50 * time.Millisecond)
	walFile := writer.CurrentFile()
	writer.Close()

	// Recover with a callback that fails on the first call
	recovery := NewRecovery(tmpDir, zerolog.Nop())

	callbackErr := errors.New("simulated S3 failure")
	callback := func(ctx context.Context, records []map[string]interface{}) error {
		return callbackErr
	}

	stats, err := recovery.Recover(context.Background(), callback)
	if err != nil {
		t.Fatalf("Recover should not return error for callback failure: %v", err)
	}

	// Should have 0 recovered files (recovery failed)
	if stats.RecoveredFiles != 0 {
		t.Errorf("expected 0 recovered files, got %d", stats.RecoveredFiles)
	}

	// WAL file should still exist for retry
	if _, err := os.Stat(walFile); os.IsNotExist(err) {
		t.Error("WAL file should be kept for retry after failure")
	}

	// Second recovery attempt should succeed
	var recoveredRecords int
	successCallback := func(ctx context.Context, records []map[string]interface{}) error {
		recoveredRecords += len(records)
		return nil
	}

	stats2, err := recovery.Recover(context.Background(), successCallback)
	if err != nil {
		t.Fatalf("Second recovery attempt failed: %v", err)
	}

	if stats2.RecoveredFiles != 1 {
		t.Errorf("expected 1 recovered file on retry, got %d", stats2.RecoveredFiles)
	}

	if recoveredRecords != 3 {
		t.Errorf("expected 3 recovered records on retry, got %d", recoveredRecords)
	}

	// WAL file should now be deleted
	if _, err := os.Stat(walFile); !os.IsNotExist(err) {
		t.Error("WAL file should be deleted after successful retry")
	}
}

func TestRecovery_BatchSize_PartialBatchFailure(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)

	// Write 10 records
	records := make([]map[string]interface{}, 10)
	for i := 0; i < 10; i++ {
		records[i] = map[string]interface{}{
			"measurement": "cpu",
			"index":       i,
		}
	}
	writer.Append(records)
	time.Sleep(50 * time.Millisecond)
	walFile := writer.CurrentFile()
	writer.Close()

	// Recover with BatchSize=3, fail on second batch
	recovery := NewRecovery(tmpDir, zerolog.Nop())

	batchNum := 0
	callback := func(ctx context.Context, records []map[string]interface{}) error {
		batchNum++
		if batchNum == 2 {
			return errors.New("simulated failure on second batch")
		}
		return nil
	}

	stats, err := recovery.RecoverWithOptions(context.Background(), callback, &RecoveryOptions{
		BatchSize: 3,
	})
	if err != nil {
		t.Fatalf("Recover should not return error for callback failure: %v", err)
	}

	// Recovery failed, so file should be kept
	if stats.RecoveredFiles != 0 {
		t.Errorf("expected 0 recovered files after partial failure, got %d", stats.RecoveredFiles)
	}

	// WAL file should still exist
	if _, err := os.Stat(walFile); os.IsNotExist(err) {
		t.Error("WAL file should be kept after partial batch failure")
	}
}

// ==========================================================================
// Control record tests
// ==========================================================================

// TestAppendControl_WritesAndReadsBack verifies that control records round-trip
// correctly through the WAL: written, read, and correctly identified by type.
func TestAppendControl_WritesAndReadsBack(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)

	// Write FLUSH_OK
	if err := writer.AppendControl(FlushOK, "testdb", "cpu_metrics"); err != nil {
		t.Fatalf("AppendControl FLUSH_OK failed: %v", err)
	}
	// Write FLUSH_FAIL
	if err := writer.AppendControl(FlushFail, "testdb", "memory_metrics"); err != nil {
		t.Fatalf("AppendControl FLUSH_FAIL failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	walFile := writer.CurrentFile()
	writer.Close()

	// Read back
	reader := NewReader(walFile, zerolog.Nop())
	entries, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("expected 2 control entries, got %d", len(entries))
	}

	// First entry: FLUSH_OK
	e1 := entries[0]
	if e1.Control == nil {
		t.Fatal("expected first entry to be a control record")
	}
	if e1.Control.Type != FlushOK {
		t.Errorf("expected FLUSH_OK (0x00), got 0x%02x", e1.Control.Type)
	}
	if e1.Control.Database != "testdb" {
		t.Errorf("expected db 'testdb', got '%s'", e1.Control.Database)
	}
	if e1.Control.Measurement != "cpu_metrics" {
		t.Errorf("expected meas 'cpu_metrics', got '%s'", e1.Control.Measurement)
	}

	// Second entry: FLUSH_FAIL
	e2 := entries[1]
	if e2.Control == nil {
		t.Fatal("expected second entry to be a control record")
	}
	if e2.Control.Type != FlushFail {
		t.Errorf("expected FLUSH_FAIL (0x02), got 0x%02x", e2.Control.Type)
	}
	if e2.Control.Measurement != "memory_metrics" {
		t.Errorf("expected meas 'memory_metrics', got '%s'", e2.Control.Measurement)
	}
}

// TestRecovery_FlushOKClearsOnlyOwnMeasurement verifies that a FLUSH_OK for
// measurement A does not clear staging for measurement B.
func TestRecovery_FlushOKClearsOnlyOwnMeasurement(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)

	// Write data for two measurements
	writer.Append([]map[string]interface{}{
		{"_measurement": "cpu", "value": 1.0},
		{"_measurement": "cpu", "value": 2.0},
	})
	time.Sleep(30 * time.Millisecond)

	writer.Append([]map[string]interface{}{
		{"_measurement": "memory", "value": 100.0},
	})
	time.Sleep(30 * time.Millisecond)

	// FLUSH_OK only for cpu — memory stays in staging
	writer.AppendControl(FlushOK, "default", "cpu")
	time.Sleep(30 * time.Millisecond)
	writer.Close()

	// Recover
	recovery := NewRecovery(tmpDir, zerolog.Nop())
	recoveredByMeasurement := make(map[string]int)
	callback := func(ctx context.Context, records []map[string]interface{}) error {
		for _, r := range records {
			m, _ := r["_measurement"].(string)
			recoveredByMeasurement[m]++
		}
		return nil
	}

	stats, err := recovery.RecoverWithOptions(context.Background(), callback, nil)
	if err != nil {
		t.Fatalf("RecoverWithOptions failed: %v", err)
	}

	// cpu should have 0 recovered entries (cleared by FLUSH_OK)
	if recoveredByMeasurement["cpu"] != 0 {
		t.Errorf("expected 0 recovered cpu entries, got %d", recoveredByMeasurement["cpu"])
	}
	// memory should have 1 recovered entry (no FLUSH_OK)
	if recoveredByMeasurement["memory"] != 1 {
		t.Errorf("expected 1 recovered memory entry, got %d", recoveredByMeasurement["memory"])
	}
	if stats.StagingCleared != 1 {
		t.Errorf("expected 1 staging cleared, got %d", stats.StagingCleared)
	}
}

// TestRecovery_NoFlushOK_ReplaysAll verifies that without any FLUSH_OK,
// all data is replayed at EOF.
func TestRecovery_NoFlushOK_ReplaysAll(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)

	records := make([]map[string]interface{}, 50)
	for i := 0; i < 50; i++ {
		records[i] = map[string]interface{}{
			"_measurement": "temp",
			"value":        float64(i),
		}
	}
	writer.Append(records)
	time.Sleep(50 * time.Millisecond)
	writer.Close()

	recovery := NewRecovery(tmpDir, zerolog.Nop())
	recoveredCount := 0
	callback := func(ctx context.Context, recs []map[string]interface{}) error {
		recoveredCount += len(recs)
		return nil
	}

	_, err := recovery.Recover(context.Background(), callback)
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if recoveredCount != 50 {
		t.Errorf("expected 50 recovered entries (no FLUSH_OK), got %d", recoveredCount)
	}
}

// TestRecovery_FlushFailDoesNotClearStaging verifies FLUSH_FAIL is a no-op
// during recovery — staging data is preserved.
func TestRecovery_FlushFailDoesNotClearStaging(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)

	// Write data
	writer.Append([]map[string]interface{}{
		{"_measurement": "sensor", "value": 42.0},
	})
	time.Sleep(30 * time.Millisecond)

	// Write FLUSH_FAIL (simulating flush failure)
	writer.AppendControl(FlushFail, "default", "sensor")
	time.Sleep(30 * time.Millisecond)
	writer.Close()

	// Recover
	recovery := NewRecovery(tmpDir, zerolog.Nop())
	recoveredCount := 0
	callback := func(ctx context.Context, recs []map[string]interface{}) error {
		recoveredCount += len(recs)
		return nil
	}

	_, err := recovery.Recover(context.Background(), callback)
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// FLUSH_FAIL is a no-op — data should still be replayed
	if recoveredCount != 1 {
		t.Errorf("expected 1 recovered entry (FLUSH_FAIL no-op), got %d", recoveredCount)
	}
}
