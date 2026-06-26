//go:build duckdb_arrow

package ingest

import (
	"context"
	"io"
	"strings"
	"testing"

	"iedb/internal/config"

	"github.com/rs/zerolog"
)

// testNoopStorage is a minimal backend for testing.
type testNoopStorage struct{}

func (s *testNoopStorage) Write(_ context.Context, _ string, _ []byte) error    { return nil }
func (s *testNoopStorage) WriteReader(_ context.Context, _ string, _ io.Reader, _ int64) error {
	return nil
}
func (s *testNoopStorage) Read(_ context.Context, _ string) ([]byte, error)    { return nil, nil }
func (s *testNoopStorage) ReadTo(_ context.Context, _ string, _ io.Writer) error { return nil }
func (s *testNoopStorage) ReadToAt(_ context.Context, _ string, _ io.Writer, _ int64) error {
	return nil
}
func (s *testNoopStorage) Delete(_ context.Context, _ string) error      { return nil }
func (s *testNoopStorage) Exists(_ context.Context, _ string) (bool, error) { return false, nil }
func (s *testNoopStorage) List(_ context.Context, _ string) ([]string, error) { return nil, nil }
func (s *testNoopStorage) StatFile(_ context.Context, _ string) (int64, error) { return -1, nil }
func (s *testNoopStorage) Close() error      { return nil }
func (s *testNoopStorage) Type() string      { return "noop" }
func (s *testNoopStorage) ConfigJSON() string { return "{}" }

func newTestBuffer(t *testing.T) *ArrowBuffer {
	t.Helper()
	logger := zerolog.New(zerolog.NewTestWriter(t)).Level(zerolog.Disabled)
	buf := NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize:           10000, // large enough to not trigger immediate flush
		MaxBufferMemoryMB:      128,
		ShardCount:             4,
		FlushWorkers:           2,
		FlushQueueSize:         16,
		MinBufferMemoryMB:      16,
		MaxBufferAgeMS:         60000,
		MaxBufferAgeSeconds:    900,
		FlushTimeoutSeconds:    30,
		Compression:            "none",
	}, &testNoopStorage{}, logger)
	return buf
}

// ============================================================================
// SECTION 1: Schema routing chain
// ============================================================================

func TestSchemaHash_Deterministic(t *testing.T) {
	h1 := schemaHash("host:S,usage:f")
	h2 := schemaHash("host:S,usage:f")
	if h1 != h2 {
		t.Errorf("schemaHash should be deterministic: %s vs %s", h1, h2)
	}
	if len(h1) != 8 {
		t.Errorf("schemaHash should be 8 hex chars, got %d: %s", len(h1), h1)
	}
}

func TestSchemaHash_DifferentSignatures(t *testing.T) {
	h1 := schemaHash("host:S,usage:f")
	h2 := schemaHash("host:S,usage:i")
	if h1 == h2 {
		t.Error("different signatures should produce different hashes")
	}
}

func TestSchemaKey_Construction(t *testing.T) {
	key := schemaKey("mydb", "cpu", "host:S,usage:f")
	if !strings.HasPrefix(key, "mydb/cpu__") {
		t.Errorf("schemaKey should start with db/meas__, got: %s", key)
	}
	parts := strings.SplitN(key, "__", 2)
	if len(parts) != 2 || len(parts[1]) != 8 {
		t.Errorf("schemaKey should have 8-char hex hash, got: %s", key)
	}
}

func TestSchemaKey_DifferentSchemasDifferentKeys(t *testing.T) {
	k1 := schemaKey("db", "cpu", "host:S")
	k2 := schemaKey("db", "cpu", "host:S,usage:f")
	if k1 == k2 {
		t.Error("different schemas should get different buffer keys")
	}
}

func TestIsSchemaHashHex(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"a1b2c3d4", true},
		{"00000000", true},
		{"ffffffff", true},
		{"a1b2c3d", false},         // 7 chars
		{"a1b2c3d4e", false},       // 9 chars
		{"a1b2c3dg", false},        // 'g' not hex
		{"A1B2C3D4", false},        // uppercase (schemaHash uses lowercase)
		{"__mysuffix", false},      // not hex at all
		{"", false},
	}
	for _, tt := range tests {
		if isSchemaHashHex(tt.input) != tt.expected {
			t.Errorf("isSchemaHashHex(%q) = %v, want %v", tt.input, !tt.expected, tt.expected)
		}
	}
}

func TestStripSchemaHash(t *testing.T) {
	tests := []struct {
		key         string
		wantBase    string
		wantHash    string
	}{
		{"mydb/cpu__a1b2c3d4", "mydb/cpu", "a1b2c3d4"},
		{"mydb/cpu", "mydb/cpu", ""},
		{"db/measurement__no_hash", "db/measurement__no_hash", ""}, // >8 chars after __
		{"db__8chars__12345678", "db__8chars", "12345678"},         // last __ pair is hash
		{"key__without_hash_here", "key__without_hash_here", ""},   // suffix > 8 chars
	}
	for _, tt := range tests {
		base, hash := StripSchemaHash(tt.key)
		if base != tt.wantBase || hash != tt.wantHash {
			t.Errorf("StripSchemaHash(%q) = (%q, %q), want (%q, %q)",
				tt.key, base, hash, tt.wantBase, tt.wantHash)
		}
	}
}

// ============================================================================
// SECTION 2: Type conflict detection
// ============================================================================

func TestParseSignature(t *testing.T) {
	m := parseSignature("host:S,usage:f,temp:f")
	if len(m) != 3 {
		t.Errorf("expected 3 fields, got %d: %v", len(m), m)
	}
	if m["host"] != "S" || m["usage"] != "f" {
		t.Errorf("wrong types: %v", m)
	}
}

func TestParseSignature_Empty(t *testing.T) {
	m := parseSignature("")
	if len(m) != 0 {
		t.Errorf("expected empty map, got: %v", m)
	}
}

func TestHasTypeConflict_SameSignature(t *testing.T) {
	if hasTypeConflict("host:S,usage:f", "host:S,usage:f") {
		t.Error("same signature should not conflict")
	}
}

func TestHasTypeConflict_TypeChange(t *testing.T) {
	if !hasTypeConflict("host:S,usage:f", "host:S,usage:i") {
		t.Error("usage:f → usage:i should be a conflict")
	}
}

func TestHasTypeConflict_FieldAdded(t *testing.T) {
	if hasTypeConflict("host:S", "host:S,usage:f") {
		t.Error("adding a field should NOT be a conflict")
	}
}

func TestHasTypeConflict_FieldRemoved(t *testing.T) {
	if hasTypeConflict("host:S,usage:f", "host:S") {
		t.Error("removing a field should NOT be a conflict")
	}
}

func TestHasTypeConflict_Empty(t *testing.T) {
	if hasTypeConflict("", "host:S") {
		t.Error("empty oldSig should not conflict")
	}
	if hasTypeConflict("host:S", "") {
		t.Error("empty newSig should not conflict")
	}
}

func TestComputeColumnSignature(t *testing.T) {
	columns := map[string][]interface{}{
		"host":  {"srv1", "srv2"},
		"usage": {45.5, 87.3},
	}
	sig := computeColumnSignature(columns)
	if sig == "" {
		t.Fatal("signature should not be empty")
	}
	if !strings.Contains(sig, "host") || !strings.Contains(sig, "usage") {
		t.Errorf("signature missing columns: %s", sig)
	}
}

// ============================================================================
// SECTION 3: Zero-copy helpers
// ============================================================================

func TestTryInt64ZeroCopy(t *testing.T) {
	buf := newTestBuffer(t)
	defer buf.Close()

	t.Run("homogeneous int64", func(t *testing.T) {
		arr, ok := buf.tryInt64ZeroCopy([]interface{}{int64(1), int64(2), int64(3)})
		if !ok || len(arr) != 3 || arr[0] != 1 || arr[2] != 3 {
			t.Errorf("should succeed: ok=%v arr=%v", ok, arr)
		}
	})
	t.Run("mixed types", func(t *testing.T) {
		_, ok := buf.tryInt64ZeroCopy([]interface{}{int64(1), float64(2.0), int64(3)})
		if ok {
			t.Error("should fail on mixed int64/float64")
		}
	})
	t.Run("nil element", func(t *testing.T) {
		_, ok := buf.tryInt64ZeroCopy([]interface{}{int64(1), nil, int64(3)})
		if ok {
			t.Error("should fail on nil element")
		}
	})
	t.Run("empty", func(t *testing.T) {
		arr, ok := buf.tryInt64ZeroCopy([]interface{}{})
		if !ok || len(arr) != 0 {
			t.Errorf("empty should succeed: ok=%v arr=%v", ok, arr)
		}
	})
}

func TestTryFloat64ZeroCopy(t *testing.T) {
	buf := newTestBuffer(t)
	defer buf.Close()

	arr, ok := buf.tryFloat64ZeroCopy([]interface{}{float64(1.1), float64(2.2)})
	if !ok || len(arr) != 2 {
		t.Errorf("should succeed: ok=%v arr=%v", ok, arr)
	}
	_, ok = buf.tryFloat64ZeroCopy([]interface{}{float64(1.0), "string"})
	if ok {
		t.Error("should fail on mixed types")
	}
}

func TestTryStringZeroCopy(t *testing.T) {
	buf := newTestBuffer(t)
	defer buf.Close()

	arr, ok := buf.tryStringZeroCopy([]interface{}{"a", "b", "c"})
	if !ok || len(arr) != 3 {
		t.Errorf("should succeed: ok=%v arr=%v", ok, arr)
	}
	_, ok = buf.tryStringZeroCopy([]interface{}{"ok", int64(42)})
	if ok {
		t.Error("should fail on mixed types")
	}
}

func TestTryBoolZeroCopy(t *testing.T) {
	buf := newTestBuffer(t)
	defer buf.Close()

	arr, ok := buf.tryBoolZeroCopy([]interface{}{true, false, true})
	if !ok || len(arr) != 3 {
		t.Errorf("should succeed: ok=%v arr=%v", ok, arr)
	}
}

// ============================================================================
// SECTION 4: SnapshotEntry / MeasurementBufferKeys (query-view dependency)
// ============================================================================

func TestSnapshotEntry_BasicRoundTrip(t *testing.T) {
	buf := newTestBuffer(t)
	defer buf.Close()

	err := buf.WriteColumnarDirect(context.Background(), "testdb", "cpu", map[string][]interface{}{
		"host":  {"srv1", "srv2"},
		"usage": {45.5, 87.3},
	})
	if err != nil {
		t.Fatalf("WriteColumnarDirect: %v", err)
	}

	keys := buf.MeasurementBufferKeys("testdb", "cpu")
	if len(keys) == 0 {
		t.Fatal("MeasurementBufferKeys returned empty — buffer has no data for testdb/cpu")
	}
	t.Logf("buffer keys: %v", keys)

	snap := buf.SnapshotEntry(keys[0])
	if snap == nil {
		t.Fatal("SnapshotEntry returned nil")
	}
	if snap.recordCount != 2 {
		t.Errorf("expected 2 records, got %d", snap.recordCount)
	}
	if _, ok := snap.columns["host"]; !ok {
		t.Error("snapshot missing 'host' column")
	}
}

func TestSnapshotEntry_NotFound(t *testing.T) {
	buf := newTestBuffer(t)
	defer buf.Close()

	snap := buf.SnapshotEntry("nonexistent/key")
	if snap != nil {
		t.Error("SnapshotEntry should return nil for missing key")
	}
}

func TestMeasurementBufferKeys_MultipleSchemaVariants(t *testing.T) {
	buf := newTestBuffer(t)
	defer buf.Close()

	// Write two batches — these have the same schema, so same key
	buf.WriteColumnarDirect(context.Background(), "mydb", "sensors", map[string][]interface{}{
		"temp": {22.5},
	})
	buf.WriteColumnarDirect(context.Background(), "mydb", "sensors", map[string][]interface{}{
		"temp": {23.0},
	})

	keys := buf.MeasurementBufferKeys("mydb", "sensors")
	if len(keys) == 0 {
		t.Fatal("no keys found")
	}
	// Schema is the same for both writes, so only one key
	t.Logf("keys for mydb/sensors: %v (count=%d)", keys, len(keys))
}

func TestMeasurementBufferKeys_WrongDatabase(t *testing.T) {
	buf := newTestBuffer(t)
	defer buf.Close()
	buf.WriteColumnarDirect(context.Background(), "db1", "cpu", map[string][]interface{}{"x": {1}})

	keys := buf.MeasurementBufferKeys("db2", "cpu")
	if len(keys) != 0 {
		t.Errorf("expected 0 keys for wrong database, got %v", keys)
	}
}

// ============================================================================
// SECTION 5: WriteColumnarDirect — the primary buffer entry point
// ============================================================================

func TestWriteColumnarDirect_AllTypes(t *testing.T) {
	buf := newTestBuffer(t)
	defer buf.Close()

	// Test all 4 supported types
	err := buf.WriteColumnarDirect(context.Background(), "db", "alltypes", map[string][]interface{}{
		"str_col":   {"hello", "world"},
		"int_col":   {int64(1), int64(2)},
		"float_col": {float64(1.5), float64(2.5)},
		"bool_col":  {true, false},
	})
	if err != nil {
		t.Fatalf("WriteColumnarDirect all types: %v", err)
	}

	keys := buf.MeasurementBufferKeys("db", "alltypes")
	if len(keys) == 0 {
		t.Fatal("no buffer keys found after write")
	}
	t.Logf("alltypes keys: %v", keys)
}

func TestWriteColumnarDirect_EmptyColumns(t *testing.T) {
	buf := newTestBuffer(t)
	defer buf.Close()

	err := buf.WriteColumnarDirect(context.Background(), "db", "empty", map[string][]interface{}{})
	if err == nil {
		// Empty columns may be accepted (no data to write) or rejected
		t.Log("empty columns accepted (no-op)")
	}
}

func TestWriteColumnarDirect_DifferentColumnLengths(t *testing.T) {
	buf := newTestBuffer(t)
	defer buf.Close()

	err := buf.WriteColumnarDirect(context.Background(), "db", "mismatch", map[string][]interface{}{
		"a": {1, 2, 3},
		"b": {1, 2}, // one row short
	})
	if err == nil {
		t.Log("mismatched column lengths accepted — may be rejected by convertAndAppendToEntry")
	}
}

// ============================================================================
// SECTION 6: convertAndAppendToEntry type dispatch
// ============================================================================

func TestConvertAndAppendToEntry_NilValues(t *testing.T) {
	buf := newTestBuffer(t)
	defer buf.Close()

	columns := map[string][]interface{}{
		"host":  {"srv1", nil, "srv3"},       // mixed nil
		"usage": {45.5, nil, 87.3},            // mixed nil
	}
	err := buf.WriteColumnarDirect(context.Background(), "db", "nil_test", columns)
	if err != nil {
		t.Fatalf("WriteColumnarDirect with nils: %v", err)
	}
	keys := buf.MeasurementBufferKeys("db", "nil_test")
	if len(keys) == 0 {
		t.Fatal("no keys — nils may have been rejected")
	}
	t.Logf("nil_test keys: %v", keys)
}

func TestConvertAndAppendToEntry_IntAsFloat64(t *testing.T) {
	buf := newTestBuffer(t)
	defer buf.Close()

	// Go literals: untyped integers are `int`, floats are `float64`
	// WriteColumnarDirect wraps []interface{}, so `1` becomes `int(1)`, `1.5` becomes `float64(1.5)`
	columns := map[string][]interface{}{
		"val": {1, 2, 3}, // Go `int` type
	}
	err := buf.WriteColumnarDirect(context.Background(), "db", "intTest", columns)
	if err != nil {
		t.Fatalf("int columns: %v", err)
	}
	keys := buf.MeasurementBufferKeys("db", "intTest")
	if len(keys) == 0 {
		t.Fatal("int columns rejected")
	}
	t.Logf("intTest keys: %v", keys)
}

func TestConvertAndAppendToEntry_TimeColumn(t *testing.T) {
	buf := newTestBuffer(t)
	defer buf.Close()

	columns := map[string][]interface{}{
		"time":  {"2026-06-26T00:00:00Z", "2026-06-26T01:00:00Z"},
		"value": {42.0, 43.0},
	}
	err := buf.WriteColumnarDirect(context.Background(), "db", "time_test", columns)
	if err != nil {
		t.Fatalf("time column write: %v", err)
	}
	keys := buf.MeasurementBufferKeys("db", "time_test")
	if len(keys) == 0 {
		t.Fatal("no keys for time_test")
	}
	t.Logf("time_test keys: %v", keys)
}

// ============================================================================
// SECTION 7: AllBufferKeys integration
// ============================================================================

func TestAllBufferKeys(t *testing.T) {
	buf := newTestBuffer(t)
	defer buf.Close()

	// Write to multiple databases/measurements
	buf.WriteColumnarDirect(context.Background(), "db1", "cpu", map[string][]interface{}{"x": {1}})
	buf.WriteColumnarDirect(context.Background(), "db1", "mem", map[string][]interface{}{"x": {2}})
	buf.WriteColumnarDirect(context.Background(), "db2", "cpu", map[string][]interface{}{"x": {3}})

	keys := buf.AllBufferKeys()
	if len(keys) < 2 {
		t.Errorf("expected at least 2 keys, got %d: %v", len(keys), keys)
	}
	t.Logf("All buffer keys (%d): %v", len(keys), keys)
}

// ============================================================================
// SECTION 8: WriteColumnarDirectNoWAL (WAL recovery path)
// ============================================================================

func TestWriteColumnarDirectNoWAL(t *testing.T) {
	buf := newTestBuffer(t)
	defer buf.Close()

	err := buf.WriteColumnarDirectNoWAL(context.Background(), "recovery_db", "cpu", map[string][]interface{}{
		"host":  {"recovered-srv"},
		"usage": {99.9},
	})
	if err != nil {
		t.Fatalf("WriteColumnarDirectNoWAL: %v", err)
	}

	keys := buf.MeasurementBufferKeys("recovery_db", "cpu")
	if len(keys) == 0 {
		t.Fatal("WriteColumnarDirectNoWAL should write to buffer")
	}
}
