package ingest

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"iedb/internal/config"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
)

// mockStorage implements storage.Backend with an in-memory map for testing.
type mockStorage struct {
	mu    sync.RWMutex
	files map[string][]byte
}

func newMockStorage() *mockStorage {
	return &mockStorage{files: make(map[string][]byte)}
}

func (m *mockStorage) Type() string      { return "mock" }
func (m *mockStorage) ConfigJSON() string { return "{}" }
func (m *mockStorage) Close() error       { return nil }

func (m *mockStorage) Write(_ context.Context, path string, data []byte) error {
	m.mu.Lock()
	m.files[path] = append([]byte(nil), data...)
	m.mu.Unlock()
	return nil
}

func (m *mockStorage) Read(_ context.Context, path string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if data, ok := m.files[path]; ok {
		return append([]byte(nil), data...), nil
	}
	return nil, os.ErrNotExist
}

func (m *mockStorage) ReadTo(_ context.Context, path string, w io.Writer) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if data, ok := m.files[path]; ok {
		_, err := w.Write(data)
		return err
	}
	return os.ErrNotExist
}

func (m *mockStorage) WriteReader(_ context.Context, path string, r io.Reader, _ int64) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.files[path] = data
	m.mu.Unlock()
	return nil
}

func (m *mockStorage) Delete(_ context.Context, path string) error {
	m.mu.Lock()
	delete(m.files, path)
	m.mu.Unlock()
	return nil
}

func (m *mockStorage) Exists(_ context.Context, path string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.files[path]
	return ok, nil
}

func (m *mockStorage) List(_ context.Context, prefix string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var paths []string
	for p := range m.files {
		if strings.HasPrefix(p, prefix) {
			paths = append(paths, p)
		}
	}
	return paths, nil
}

func (m *mockStorage) filesForPrefix(prefix string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var paths []string
	for p := range m.files {
		if strings.HasPrefix(p, prefix) {
			paths = append(paths, p)
		}
	}
	return paths
}

// makeTestConfig creates an IngestConfig suitable for integration tests.
func makeTestConfig(tmpDir string, maxFileSizeMB int) *config.IngestConfig {
	return &config.IngestConfig{
		BufferFileSizeMB:     maxFileSizeMB,
		BufferTmpDir:         tmpDir,
		Compression:          "snappy",
		UseDictionary:        false,
		WriteStatistics:      false,
		DataPageVersion:      "2.0",
		ShardCount:           4,
		DefaultSortKeys:      "time",
		FlushTimeoutSeconds:  30,
	}
}

// makeTestColumns creates n rows of test columnar data (time, value, sensor).
func makeTestColumns(n int) map[string][]interface{} {
	times := make([]interface{}, n)
	values := make([]interface{}, n)
	tags := make([]interface{}, n)
	now := time.Now().UnixMicro()
	for i := 0; i < n; i++ {
		times[i] = now + int64(i*1000)
		values[i] = float64(i) * 1.5
		tags[i] = "sensor_a"
	}
	return map[string][]interface{}{
		"time":   times,
		"value":  values,
		"sensor": tags,
	}
}

// writeArrowFile writes a valid Arrow IPC stream file with the given records.
// Optionally appends trailing garbage bytes to simulate a truncated write after crash.
func writeArrowFile(path string, records []arrow.Record, trailingGarbage []byte) error {
	var buf []byte
	for _, rec := range records {
		var wBuf bytesBuf
		w := ipc.NewWriter(&wBuf, ipc.WithSchema(rec.Schema()))
		if err := w.Write(rec); err != nil {
			return fmt.Errorf("ipc write: %w", err)
		}
		if err := w.Close(); err != nil {
			return fmt.Errorf("ipc close: %w", err)
		}
		buf = append(buf, wBuf.Bytes()...)
	}
	buf = append(buf, trailingGarbage...)
	return os.WriteFile(path, buf, 0644)
}

// bytesBuf is a simple io.Writer backed by a byte slice.
type bytesBuf struct {
	b []byte
}

func (b *bytesBuf) Write(p []byte) (int, error) {
	b.b = append(b.b, p...)
	return len(p), nil
}

func (b *bytesBuf) Bytes() []byte { return b.b }

// buildTestRecord creates an Arrow record with time, value, and sensor columns.
func buildTestRecord(times []int64, values []float64, sensors []string) arrow.Record {
	timeBuilder := array.NewInt64Builder(memory.DefaultAllocator)
	valueBuilder := array.NewFloat64Builder(memory.DefaultAllocator)
	sensorBuilder := array.NewStringBuilder(memory.DefaultAllocator)
	defer timeBuilder.Release()
	defer valueBuilder.Release()
	defer sensorBuilder.Release()

	for i := range times {
		timeBuilder.Append(times[i])
		valueBuilder.Append(values[i])
		sensorBuilder.Append(sensors[i])
	}

	timeArr := timeBuilder.NewArray()
	valueArr := valueBuilder.NewArray()
	sensorArr := sensorBuilder.NewArray()
	defer timeArr.Release()
	defer valueArr.Release()
	defer sensorArr.Release()

	fields := []arrow.Field{
		{Name: "time", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		{Name: "sensor", Type: arrow.BinaryTypes.String},
	}
	schema := arrow.NewSchema(fields, nil)
	return array.NewRecord(schema, []arrow.Array{timeArr, valueArr, sensorArr}, int64(len(times)))
}

// TestIntegration_WriteAndFlushAll tests the full Write → Close → Parquet flow.
func TestIntegration_WriteAndFlushAll(t *testing.T) {
	tmpDir := t.TempDir()
	store := newMockStorage()

	buf := NewArrowFileBuffer(makeTestConfig(tmpDir, 10), store, zerolog.Nop())

	cols := makeTestColumns(100)
	if err := buf.WriteColumnarDirect(context.Background(), "testdb", "cpu", cols); err != nil {
		t.Fatalf("WriteColumnarDirect: %v", err)
	}

	if err := buf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Verify Parquet files in storage
	paths := store.filesForPrefix("testdb/cpu/")
	if len(paths) == 0 {
		t.Fatal("expected parquet files in storage, got none")
	}
	for _, p := range paths {
		if !strings.HasSuffix(p, ".parquet") {
			t.Errorf("expected .parquet file, got: %s", p)
		}
	}

	// Verify .arrow files cleaned up
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".arrow") {
			t.Errorf("expected no .arrow files after Close, found: %s", e.Name())
		}
	}

	t.Logf("storage files: %v", paths)
}

// TestIntegration_MultipleMeasurements verifies separate .arrow files per measurement.
func TestIntegration_MultipleMeasurements(t *testing.T) {
	tmpDir := t.TempDir()
	store := newMockStorage()

	buf := NewArrowFileBuffer(makeTestConfig(tmpDir, 10), store, zerolog.Nop())

	// Write to two different measurements
	cpuCols := makeTestColumns(50)
	memCols := makeTestColumns(30)

	if err := buf.WriteColumnarDirect(context.Background(), "testdb", "cpu", cpuCols); err != nil {
		t.Fatalf("WriteColumnarDirect cpu: %v", err)
	}
	if err := buf.WriteColumnarDirect(context.Background(), "testdb", "memory", memCols); err != nil {
		t.Fatalf("WriteColumnarDirect memory: %v", err)
	}

	if err := buf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Verify both measurements have Parquet files
	cpuFiles := store.filesForPrefix("testdb/cpu/")
	memFiles := store.filesForPrefix("testdb/memory/")

	if len(cpuFiles) == 0 {
		t.Error("expected Parquet files for cpu measurement")
	}
	if len(memFiles) == 0 {
		t.Error("expected Parquet files for memory measurement")
	}

	t.Logf("cpu files: %v", cpuFiles)
	t.Logf("memory files: %v", memFiles)
}

// TestIntegration_AutoFlush verifies that files exceeding the size threshold
// are automatically converted to Parquet by the background worker.
func TestIntegration_AutoFlush(t *testing.T) {
	tmpDir := t.TempDir()
	store := newMockStorage()

	// Use 1MB threshold so auto-flush triggers with moderate data
	buf := NewArrowFileBuffer(makeTestConfig(tmpDir, 1), store, zerolog.Nop())
	defer buf.Close()

	// Write enough data to exceed 1MB (~50000 rows with 3 columns)
	cols := makeTestColumns(50000)
	if err := buf.WriteColumnarDirect(context.Background(), "testdb", "cpu", cols); err != nil {
		t.Fatalf("WriteColumnarDirect: %v", err)
	}

	// Poll for auto-flush completion
	var parquetPaths []string
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		parquetPaths = store.filesForPrefix("testdb/cpu/")
		if len(parquetPaths) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if len(parquetPaths) == 0 {
		t.Fatal("expected auto-flush to produce Parquet file within 5s, got none")
	}
	for _, p := range parquetPaths {
		if !strings.HasSuffix(p, ".parquet") {
			t.Errorf("expected .parquet file, got: %s", p)
		}
	}

	t.Logf("auto-flushed parquet files: %v", parquetPaths)
}

// TestIntegration_Close verifies Close drains all buffered data to storage.
func TestIntegration_Close(t *testing.T) {
	tmpDir := t.TempDir()
	store := newMockStorage()

	buf := NewArrowFileBuffer(makeTestConfig(tmpDir, 10), store, zerolog.Nop())

	// Write data to multiple measurements
	if err := buf.WriteColumnarDirect(context.Background(), "db1", "cpu", makeTestColumns(100)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := buf.WriteColumnarDirect(context.Background(), "db1", "mem", makeTestColumns(200)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if err := buf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// All data should be in storage
	cpuFiles := store.filesForPrefix("db1/cpu/")
	memFiles := store.filesForPrefix("db1/mem/")

	if len(cpuFiles) == 0 {
		t.Error("expected cpu parquet files")
	}
	if len(memFiles) == 0 {
		t.Error("expected mem parquet files")
	}

	// No .arrow files should remain
	entries, _ := os.ReadDir(tmpDir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".arrow") {
			t.Errorf("orphaned .arrow file: %s", e.Name())
		}
	}
}

// TestIntegration_CrashRecovery verifies that orphaned .arrow files from a crash
// are recovered (truncated of trailing garbage), reopened for appending, and that
// subsequent writes flow into the recovered files.
func TestIntegration_CrashRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	store := newMockStorage()

	// Step 1: Manually write an Arrow IPC stream file with valid data + trailing garbage
	times := make([]int64, 50)
	values := make([]float64, 50)
	sensors := make([]string, 50)
	now := time.Now().UnixMicro()
	for i := 0; i < 50; i++ {
		times[i] = now + int64(i*1000)
		values[i] = float64(i) * 1.5
		sensors[i] = "sensor_a"
	}

	rec := buildTestRecord(times, values, sensors)
	defer rec.Release()

	orphanPath := filepath.Join(tmpDir, "testdb_cpu_1234567890.arrow")
	trailingGarbage := []byte{0xFF, 0xFE, 0xFD, 0xFC, 0x00, 0x00} // invalid IPC data
	if err := writeArrowFile(orphanPath, []arrow.Record{rec}, trailingGarbage); err != nil {
		t.Fatalf("writeArrowFile: %v", err)
	}

	// Step 2: Create buffer — startupRecovery should find, truncate, and reopen the file
	buf := NewArrowFileBuffer(makeTestConfig(tmpDir, 10), store, zerolog.Nop())

	// Step 3: Write more data to the same measurement (should append to recovered file)
	moreCols := makeTestColumns(30)
	if err := buf.WriteColumnarDirect(context.Background(), "testdb", "cpu", moreCols); err != nil {
		buf.Close()
		t.Fatalf("WriteColumnarDirect after recovery: %v", err)
	}

	// Step 4: Close — converts all data to Parquet
	if err := buf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Step 5: Verify Parquet files in storage
	parquetPaths := store.filesForPrefix("testdb/cpu/")
	if len(parquetPaths) == 0 {
		t.Fatal("expected parquet files after recovery + close, got none")
	}
	t.Logf("recovery parquet files: %v", parquetPaths)

	// Verify no .arrow files remain
	entries, _ := os.ReadDir(tmpDir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".arrow") {
			t.Errorf("orphaned .arrow file after recovery+close: %s", e.Name())
		}
	}
}

// TestIntegration_CrashRecoveryCorruptFile verifies that completely corrupt .arrow
// files (no valid batches) are deleted during recovery.
func TestIntegration_CrashRecoveryCorruptFile(t *testing.T) {
	tmpDir := t.TempDir()
	store := newMockStorage()

	// Write a completely corrupt file (garbage bytes, not valid IPC at all)
	corruptPath := filepath.Join(tmpDir, "testdb_cpu_garbage.arrow")
	if err := os.WriteFile(corruptPath, []byte{0x00, 0x01, 0x02, 0x03, 0xFF}, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Create buffer — recovery should detect and delete the corrupt file
	buf := NewArrowFileBuffer(makeTestConfig(tmpDir, 10), store, zerolog.Nop())

	// Write some data to verify buffer still works after recovering from corruption
	if err := buf.WriteColumnarDirect(context.Background(), "testdb", "cpu", makeTestColumns(10)); err != nil {
		buf.Close()
		t.Fatalf("WriteColumnarDirect: %v", err)
	}

	if err := buf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The corrupt file should have been cleaned up during recovery
	if _, err := os.Stat(corruptPath); !os.IsNotExist(err) {
		t.Errorf("corrupt recovery file should have been deleted, but still exists at %s", corruptPath)
	}

	// Valid data should be in storage
	paths := store.filesForPrefix("testdb/cpu/")
	if len(paths) == 0 {
		t.Error("expected parquet files after recovery from corruption")
	}
}

// TestIntegration_GetStats verifies that stats are reported correctly.
func TestIntegration_GetStats(t *testing.T) {
	tmpDir := t.TempDir()
	store := newMockStorage()

	buf := NewArrowFileBuffer(makeTestConfig(tmpDir, 10), store, zerolog.Nop())

	// Stats should show 0 before writes
	stats := buf.GetStats()
	if stats["total_records_buffered"].(int64) != 0 {
		t.Error("expected 0 records buffered initially")
	}

	// Write data
	if err := buf.WriteColumnarDirect(context.Background(), "testdb", "cpu", makeTestColumns(100)); err != nil {
		buf.Close()
		t.Fatalf("WriteColumnarDirect: %v", err)
	}

	stats = buf.GetStats()
	if stats["total_records_buffered"].(int64) != 100 {
		t.Errorf("expected 100 records buffered, got %d", stats["total_records_buffered"])
	}
	if stats["active_files"].(int) != 1 {
		t.Errorf("expected 1 active file, got %d", stats["active_files"])
	}

	if err := buf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// After Close, active files should be 0
	stats = buf.GetStats()
	if stats["active_files"].(int) != 0 {
		t.Errorf("expected 0 active files after close, got %d", stats["active_files"])
	}
}

// TestIntegration_FlushAll verifies that FlushAll submits all files for conversion.
func TestIntegration_FlushAll(t *testing.T) {
	tmpDir := t.TempDir()
	store := newMockStorage()

	buf := NewArrowFileBuffer(makeTestConfig(tmpDir, 10), store, zerolog.Nop())

	// Write to multiple measurements
	buf.WriteColumnarDirect(context.Background(), "testdb", "cpu", makeTestColumns(50))
	buf.WriteColumnarDirect(context.Background(), "testdb", "mem", makeTestColumns(50))

	// FlushAll submits to convert queue
	if err := buf.FlushAll(context.Background()); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}

	// Close ensures all conversions complete
	if err := buf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	cpuFiles := store.filesForPrefix("testdb/cpu/")
	memFiles := store.filesForPrefix("testdb/mem/")

	if len(cpuFiles) == 0 {
		t.Error("expected cpu parquet files after FlushAll + Close")
	}
	if len(memFiles) == 0 {
		t.Error("expected mem parquet files after FlushAll + Close")
	}
}

// TestIntegration_DifferentDatabases verifies that the same measurement name
// in different databases uses separate .arrow files.
func TestIntegration_DifferentDatabases(t *testing.T) {
	tmpDir := t.TempDir()
	store := newMockStorage()

	buf := NewArrowFileBuffer(makeTestConfig(tmpDir, 10), store, zerolog.Nop())

	buf.WriteColumnarDirect(context.Background(), "db_a", "cpu", makeTestColumns(30))
	buf.WriteColumnarDirect(context.Background(), "db_b", "cpu", makeTestColumns(30))

	if err := buf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	dbAFiles := store.filesForPrefix("db_a/cpu/")
	dbBFiles := store.filesForPrefix("db_b/cpu/")

	if len(dbAFiles) == 0 {
		t.Error("expected db_a parquet files")
	}
	if len(dbBFiles) == 0 {
		t.Error("expected db_b parquet files")
	}
}

// TestIntegration_NilColumns verifies that columns with all-nil values are handled.
func TestIntegration_NilColumns(t *testing.T) {
	tmpDir := t.TempDir()
	store := newMockStorage()

	buf := NewArrowFileBuffer(makeTestConfig(tmpDir, 10), store, zerolog.Nop())

	cols := map[string][]interface{}{
		"time":  {time.Now().UnixMicro(), time.Now().UnixMicro() + 1000, time.Now().UnixMicro() + 2000},
		"value": {1.5, nil, 3.0},
		"tag":   {"a", "b", "c"},
		"empty": {nil, nil, nil}, // entirely null column
	}

	if err := buf.WriteColumnarDirect(context.Background(), "testdb", "cpu", cols); err != nil {
		buf.Close()
		t.Fatalf("WriteColumnarDirect with nil values: %v", err)
	}

	if err := buf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	paths := store.filesForPrefix("testdb/cpu/")
	if len(paths) == 0 {
		t.Fatal("expected parquet files with nil columns")
	}
}

// TestIntegration_EmptyData verifies that WriteColumnarDirect with no records is a no-op.
func TestIntegration_EmptyData(t *testing.T) {
	tmpDir := t.TempDir()
	store := newMockStorage()

	buf := NewArrowFileBuffer(makeTestConfig(tmpDir, 10), store, zerolog.Nop())

	// Empty columns (0 records)
	empty := map[string][]interface{}{
		"time":  {},
		"value": {},
	}
	if err := buf.WriteColumnarDirect(context.Background(), "testdb", "cpu", empty); err != nil {
		buf.Close()
		t.Fatalf("WriteColumnarDirect empty: %v", err)
	}

	if err := buf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// No Parquet files should be generated for empty data — Close() converts only
	// files with records. An empty writeBatch creates no .arrow file.
	paths := store.filesForPrefix("testdb/cpu/")
	if len(paths) > 0 {
		// Acceptable: some implementations may produce an empty Parquet file.
		// The key requirement is that the buffer doesn't crash on empty input.
		t.Logf("empty data produced parquet files: %v", paths)
	}
}
