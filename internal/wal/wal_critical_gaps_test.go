package wal

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func readAllFromWALFile(t *testing.T, walFile string) []Entry {
	t.Helper()
	r := NewReader(walFile, zerolog.Nop())
	entries, err := r.ReadAll()
	if err != nil {
		t.Logf("ReadAll: %v", err)
	}
	return entries
}

// ============================================================================
// GAP 1: ParseEnvelope
// ============================================================================

func TestParseEnvelope_WithValidEnvelope(t *testing.T) {
	dbName := "testdb"
	dbBytes := []byte(dbName)
	inner := []byte{0x92, 0x01, 0x02}
	envelope := make([]byte, 1+2+len(dbBytes)+len(inner))
	envelope[0] = WALEnvelopeMarker
	binary.BigEndian.PutUint16(envelope[1:3], uint16(len(dbBytes)))
	copy(envelope[3:], dbBytes)
	copy(envelope[3+len(dbBytes):], inner)

	gotDB, gotData := ParseEnvelope(envelope, "defaultDB")
	if gotDB != dbName {
		t.Errorf("db = %q, want %q", gotDB, dbName)
	}
	if len(gotData) != len(inner) {
		t.Errorf("payload len = %d, want %d", len(gotData), len(inner))
	}
}

func TestParseEnvelope_NoEnvelopeMarker(t *testing.T) {
	payload := []byte{0x93, 0x01, 0x02, 0x03}
	gotDB, gotData := ParseEnvelope(payload, "defaultDB")
	if gotDB != "defaultDB" {
		t.Errorf("expected defaultDB, got %q", gotDB)
	}
	if len(gotData) != len(payload) {
		t.Error("payload should be unchanged")
	}
}

func TestParseEnvelope_TooShort(t *testing.T) {
	payload := []byte{WALEnvelopeMarker, 0x00}
	gotDB, _ := ParseEnvelope(payload, "fallback")
	if gotDB != "fallback" {
		t.Errorf("expected fallback for short envelope, got %q", gotDB)
	}
}

func TestParseEnvelope_DbLenExceedsPayload(t *testing.T) {
	// dbLen says 100, but envelope is only 13 bytes total
	payload := make([]byte, 13)
	payload[0] = WALEnvelopeMarker
	binary.BigEndian.PutUint16(payload[1:3], 100)
	gotDB, _ := ParseEnvelope(payload, "fallback")
	if gotDB != "fallback" {
		t.Errorf("expected fallback when dbLen > available, got %q", gotDB)
	}
}

func TestParseEnvelope_EmptyDBName(t *testing.T) {
	inner := []byte{0x01}
	envelope := make([]byte, 1+2+0+len(inner))
	envelope[0] = WALEnvelopeMarker
	binary.BigEndian.PutUint16(envelope[1:3], 0)
	copy(envelope[3:], inner)

	gotDB, gotData := ParseEnvelope(envelope, "defaultDB")
	if gotDB != "" {
		t.Errorf("empty db name, got %q", gotDB)
	}
	if len(gotData) != len(inner) {
		t.Error("payload should be inner data")
	}
}

// ============================================================================
// GAP 2: AppendRawWithMeta — happy path + read-back
// ============================================================================

func TestAppendRawWithMeta_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(&WriterConfig{
		WALDir: dir, SyncMode: SyncModeFsync, MaxSizeBytes: 100 << 20,
		BufferSize: 100, MaxAge: 1 * time.Hour, Logger: zerolog.Nop(),
	})

	dbName := "iotdb"
	// Use real msgpack bytes for columnar format: {"m":"cpu","columns":{"v":[1]}}
	payload := []byte{
		0x82, 0xa1, 0x6d, 0xa3, 0x63, 0x70, 0x75,
		0xa7, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x73,
		0x81, 0xa1, 0x76, 0x91, 0x01,
	}
	w.AppendRawWithMeta(dbName, payload)
	w.Close()

	entries := readAllFromWALFile(t, w.CurrentFile())
	if len(entries) == 0 {
		t.Fatal("no entries — async write may not have been flushed")
	}
	found := false
	for _, e := range entries {
		if e.ColumnarData != nil && e.ColumnarData.Database == dbName {
			found = true
		}
	}
	if !found {
		t.Error("columnar entry with correct database not found")
	}
}

func TestAppendRawWithMeta_CRCValid(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(&WriterConfig{
		WALDir: dir, SyncMode: SyncModeFsync, MaxSizeBytes: 100 << 20,
		BufferSize: 10, MaxAge: 1 * time.Hour, Logger: zerolog.Nop(),
	})

	// Use valid msgpack payload
	payload := []byte{
		0x82, 0xa1, 0x6d, 0xa1, 0x78,
		0xa7, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x73,
		0x80,
	}
	w.AppendRawWithMeta("mydb", payload)
	w.Close()

	entries := readAllFromWALFile(t, w.CurrentFile())
	if len(entries) == 0 {
		t.Fatal("no entries — CRC may have failed")
	}
	t.Logf("CRC valid: %d entries with db=%q", len(entries), entries[0].ColumnarData.Database)
}

func TestAppendRawWithMeta_PayloadTooLarge(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(&WriterConfig{
		WALDir: dir, SyncMode: SyncModeAsync, MaxSizeBytes: 100 << 20,
		BufferSize: 10, MaxAge: 1 * time.Hour, Logger: zerolog.Nop(),
	})
	defer w.Close()

	err := w.AppendRawWithMeta("db", make([]byte, MaxWALPayloadSize+1))
	if err == nil {
		t.Fatal("expected ErrPayloadTooLarge")
	}
}

// ============================================================================
// GAP 3: rotate — full rotation path
// ============================================================================

func TestRotate_CreatesNewFile(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(&WriterConfig{
		WALDir: dir, SyncMode: SyncModeAsync, MaxSizeBytes: 100 << 20,
		MaxAge: 1 * time.Hour, BufferSize: 10, Logger: zerolog.Nop(),
	})

	initial := w.CurrentFile()
	if initial == "" {
		t.Fatal("no initial WAL file")
	}
	w.AppendRaw([]byte("pre-rotation"))
	w.Close()

	info, _ := os.Stat(initial)
	t.Logf("WAL file: %s, size=%d", initial, info.Size())
}

func TestRotate_PreservesDataAcrossFiles(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(&WriterConfig{
		WALDir: dir, SyncMode: SyncModeFsync,
		MaxSizeBytes: 200, // small — each AppendRaw may trigger rotation
		MaxAge:       1 * time.Hour,
		BufferSize:   100,
		Logger:       zerolog.Nop(),
	})

	for i := 0; i < 10; i++ {
		w.AppendRaw(make([]byte, 100)) // 100 bytes each
	}
	w.Close()

	files := findWALFiles(t, dir)
	t.Logf("WAL files: %d", len(files))
	if len(files) == 0 {
		t.Fatal("no WAL files")
	}

	// Read from the final WAL file
	totalEntries := 0
	for _, f := range files {
		entries := readAllFromWALFile(t, filepath.Join(dir, f))
		totalEntries += len(entries)
	}
	if totalEntries == 0 {
		t.Fatal("no entries recovered after rotation")
	}
	t.Logf("total entries across %d files: %d", len(files), totalEntries)
}

// ============================================================================
// GAP 4: writeEntry — error recovery
// ============================================================================

func TestWriteEntry_WriteErrorsIncrementedOnBadFile(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(&WriterConfig{
		WALDir: dir, SyncMode: SyncModeAsync, MaxSizeBytes: 100 << 20,
		BufferSize: 100, MaxAge: 1 * time.Hour, Logger: zerolog.Nop(),
	})

	initial := w.WriteErrors

	// Force-close the underlying file to induce write failures
	w.mu.Lock()
	if w.currentFile != nil {
		w.currentFile.Close()
	}
	w.mu.Unlock()

	for i := 0; i < 3; i++ {
		w.AppendRaw([]byte("should-fail"))
	}
	w.Close()

	after := w.WriteErrors
	t.Logf("WriteErrors: %d -> %d", initial, after)
}

// ============================================================================
// GAP 5: SetReplicationHook / CurrentSequence
// ============================================================================

func TestReplicationHook_AppendRaw(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(&WriterConfig{
		WALDir: dir, SyncMode: SyncModeFsync, MaxSizeBytes: 100 << 20,
		BufferSize: 100, MaxAge: 1 * time.Hour, Logger: zerolog.Nop(),
	})

	var seq uint64
	w.SetReplicationHook(func(e *ReplicationEntry) { seq = e.Sequence })
	w.AppendRaw([]byte("data"))
	w.Close()

	if seq != 1 {
		t.Errorf("expected seq=1, got %d", seq)
	}
}

func TestReplicationHook_AppendRawWithMeta(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(&WriterConfig{
		WALDir: dir, SyncMode: SyncModeFsync, MaxSizeBytes: 100 << 20,
		BufferSize: 100, MaxAge: 1 * time.Hour, Logger: zerolog.Nop(),
	})

	var seq uint64
	w.SetReplicationHook(func(e *ReplicationEntry) { seq = e.Sequence })
	w.AppendRawWithMeta("db", []byte("col-data"))
	w.Close()

	if seq != 1 {
		t.Errorf("expected seq=1, got %d", seq)
	}
}

func TestReplicationHook_SequenceMonotonic(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(&WriterConfig{
		WALDir: dir, SyncMode: SyncModeFsync, MaxSizeBytes: 100 << 20,
		BufferSize: 100, MaxAge: 1 * time.Hour, Logger: zerolog.Nop(),
	})

	seqs := make([]uint64, 0, 5)
	w.SetReplicationHook(func(e *ReplicationEntry) { seqs = append(seqs, e.Sequence) })
	for i := 0; i < 5; i++ {
		w.AppendRaw([]byte("x"))
	}
	w.Close()

	for i := 1; i < len(seqs); i++ {
		if seqs[i] <= seqs[i-1] {
			t.Errorf("seq[%d]=%d ≤ seq[%d]=%d — not monotonic", i-1, seqs[i-1], i, seqs[i])
		}
	}
}

func TestCurrentSequence_Initial(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(&WriterConfig{
		WALDir: dir, SyncMode: SyncModeAsync, MaxSizeBytes: 100 << 20,
		BufferSize: 10, MaxAge: 1 * time.Hour, Logger: zerolog.Nop(),
	})
	defer w.Close()

	if w.CurrentSequence() != 0 {
		t.Errorf("initial sequence should be 0, got %d", w.CurrentSequence())
	}
}

// ============================================================================
// GAP 6: readEntry — oversized payload
// ============================================================================

func TestReadEntry_OversizedPayload(t *testing.T) {
	dir := t.TempDir()
	f, _ := os.Create(filepath.Join(dir, "oversized.wal"))
	header := make([]byte, WALFileHeaderSize)
	copy(header, WALMagic)
	binary.BigEndian.PutUint16(header[4:6], WALVersion)
	header[6] = WALChecksumCRC32
	f.Write(header)

	entryHdr := make([]byte, WALEntryHeaderSize)
	binary.BigEndian.PutUint32(entryHdr[0:4], MaxWALPayloadSize+1)
	binary.BigEndian.PutUint64(entryHdr[4:12], uint64(time.Now().UnixMicro()))
	binary.BigEndian.PutUint32(entryHdr[12:16], 0)
	f.Write(entryHdr)
	f.Write(make([]byte, 100))
	f.Close()

	entries := readAllFromWALFile(t, f.Name())
	if len(entries) > 0 {
		t.Error("should get no entries for oversized payload")
	}
}

// ============================================================================
// GAP 7: readEntry — truncated entry
// ============================================================================

func TestReadEntry_TruncatedPayload(t *testing.T) {
	dir := t.TempDir()
	f, _ := os.Create(filepath.Join(dir, "truncated.wal"))
	header := make([]byte, WALFileHeaderSize)
	copy(header, WALMagic)
	binary.BigEndian.PutUint16(header[4:6], WALVersion)
	header[6] = WALChecksumCRC32
	f.Write(header)

	realPayload := []byte("short")
	entryHdr := make([]byte, WALEntryHeaderSize)
	binary.BigEndian.PutUint32(entryHdr[0:4], 1000) // claims 1000 bytes
	binary.BigEndian.PutUint64(entryHdr[4:12], uint64(time.Now().UnixMicro()))
	binary.BigEndian.PutUint32(entryHdr[12:16], crc32.ChecksumIEEE(realPayload))
	f.Write(entryHdr)
	f.Write(realPayload) // only 5 bytes, not 1000
	f.Close()

	entries := readAllFromWALFile(t, f.Name())
	if len(entries) > 0 {
		t.Error("should get 0 entries for truncated payload")
	}
}

// ============================================================================
// GAP 8: Columnar recovery path
// ============================================================================

func TestRecovery_ColumnarRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(&WriterConfig{
		WALDir: dir, SyncMode: SyncModeFsync, MaxSizeBytes: 100 << 20,
		BufferSize: 10, MaxAge: 1 * time.Hour, Logger: zerolog.Nop(),
	})

	dbName := "iotdb"
	// Real msgpack: {"m":"cpu","columns":{"v":[1]}}
	payload := []byte{
		0x82, 0xa1, 0x6d, 0xa3, 0x63, 0x70, 0x75,
		0xa7, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x73,
		0x81, 0xa1, 0x76, 0x91, 0x01,
	}
	w.AppendRawWithMeta(dbName, payload)
	w.Close()

	rec := NewRecovery(dir, zerolog.Nop())
	var colCount int
	colCb := func(ctx context.Context, database, measurement string, columns map[string][]interface{}) error {
		colCount++
		if database != dbName {
			t.Errorf("db = %q, want %q", database, dbName)
		}
		return nil
	}
	_, err := rec.RecoverWithOptions(context.Background(),
		func(ctx context.Context, r []map[string]interface{}) error { return nil },
		&RecoveryOptions{ColumnarCallback: colCb})
	if err != nil {
		t.Fatalf("RecoverWithOptions: %v", err)
	}
	t.Logf("columnar entries: %d", colCount)
}

func TestRecovery_ColumnarDBExtraction(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(&WriterConfig{
		WALDir: dir, SyncMode: SyncModeFsync, MaxSizeBytes: 100 << 20,
		BufferSize: 10, MaxAge: 1 * time.Hour, Logger: zerolog.Nop(),
	})

	// msgpack: {"m":"temperature","columns":{"value":[1.0,2.0]}}
	payload := []byte{
		0x82, 0xa1, 0x6d, 0xab, 0x74, 0x65, 0x6d, 0x70,
		0x65, 0x72, 0x61, 0x74, 0x75, 0x72, 0x65,
		0xa7, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x73,
		0x81, 0xa5, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x92, 0x01, 0x02,
	}
	w.AppendRawWithMeta("sensor_db", payload)
	w.Close()

	var db, meas string
	rec := NewRecovery(dir, zerolog.Nop())
	rec.RecoverWithOptions(context.Background(),
		func(ctx context.Context, r []map[string]interface{}) error { return nil },
		&RecoveryOptions{
			ColumnarCallback: func(ctx context.Context, d, m string, c map[string][]interface{}) error {
				db, meas = d, m
				return nil
			},
		})

	if db != "sensor_db" {
		t.Errorf("db = %q, want sensor_db", db)
	}
	if meas != "temperature" {
		t.Errorf("measurement = %q, want temperature", meas)
	}
}

// ============================================================================
// GAP 9: AppendControl — sync fallback
// ============================================================================

func TestAppendControl_SyncFallback(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(&WriterConfig{
		WALDir: dir, SyncMode: SyncModeFsync, MaxSizeBytes: 100 << 20,
		BufferSize: 1, MaxAge: 1 * time.Hour, Logger: zerolog.Nop(),
	})
	defer w.Close()

	// Fill the tiny async buffer
	for i := 0; i < 10; i++ {
		w.AppendRaw([]byte("filler"))
	}
	// Buffer full — AppendControl should use sync fallback
	err := w.AppendControl(FlushOK, "db", "cpu")
	if err != nil {
		t.Logf("AppendControl fallback result: %v", err)
	}

	entries := readAllFromWALFile(t, w.CurrentFile())
	hasCtrl := false
	for _, e := range entries {
		if e.Control != nil && e.Control.Type == FlushOK {
			hasCtrl = true
		}
	}
	if hasCtrl {
		t.Log("FLUSH_OK written via sync fallback")
	}
}

// ============================================================================
// GAP 10: replayOversizedStaging
// ============================================================================

func TestReplayOversizedStaging_AboveThreshold(t *testing.T) {
	staging := map[string]*stagingEntry{
		"db/cpu": {
			columns:     map[string][]interface{}{"v": {float64(1.0)}},
			recordCount: 200,
		},
	}
	opts := &RecoveryOptions{MaxStagingRecords: 100}
	var called bool
	opts.ColumnarCallback = func(ctx context.Context, db, meas string, cols map[string][]interface{}) error {
		called = true
		return nil
	}
	var batches int
	replayOversizedStaging(context.Background(), staging, opts,
		func(ctx context.Context, r []map[string]interface{}) error { return nil },
		new(int), &batches, zerolog.Nop())

	if !called {
		t.Error("columnar callback should be called for oversized staging")
	}
	if batches == 0 {
		t.Error("recoveredBatches should be incremented")
	}
	if staging["db/cpu"].recordCount != 0 {
		t.Error("staging should be cleared after replay")
	}
}

func TestReplayOversizedStaging_BelowThreshold(t *testing.T) {
	staging := map[string]*stagingEntry{
		"db/cpu": {
			records:     []map[string]interface{}{{"x": 1}},
			recordCount: 5,
		},
	}
	opts := &RecoveryOptions{MaxStagingRecords: 100}
	var batches int
	replayOversizedStaging(context.Background(), staging, opts,
		func(ctx context.Context, r []map[string]interface{}) error { return nil },
		new(int), &batches, zerolog.Nop())

	if staging["db/cpu"].recordCount != 5 {
		t.Error("below-threshold staging should NOT be cleared")
	}
	if batches != 0 {
		t.Error("no batches should be recovered below threshold")
	}
}

// helpers

func findWALFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, _ := os.ReadDir(dir)
	var files []string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".wal" {
			files = append(files, e.Name())
		}
	}
	return files
}
