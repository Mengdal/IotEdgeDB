package ingest

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"iedb/internal/config"
	"iedb/pkg/models"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/rs/zerolog"
)

// createTestArrowBuffer creates an ArrowBuffer for testing
func createTestArrowBuffer(t *testing.T) *ArrowBuffer {
	t.Helper()
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	cfg := &config.IngestConfig{
		MaxBufferSize:  1000,
		MaxBufferAgeMS: 60000,
		Compression:    "snappy",
	}
	mockStorage := &mockStorageBackend{}
	return &ArrowBuffer{
		config:       cfg,
		storage:      mockStorage,
		writer:       NewArrowWriter(cfg, logger),
		shardCount:   32,
		flushQueue:   make(chan flushTask, 100),
		flushWorkers: 1,
		logger:       logger,
	}
}

type mockStorageBackend struct{}

func (m *mockStorageBackend) Write(ctx context.Context, path string, data []byte) error { return nil }
func (m *mockStorageBackend) WriteReader(ctx context.Context, path string, reader io.Reader, size int64) error {
	return nil
}
func (m *mockStorageBackend) Read(ctx context.Context, path string) ([]byte, error) { return nil, nil }
func (m *mockStorageBackend) ReadTo(ctx context.Context, path string, writer io.Writer) error {
	return nil
}
func (m *mockStorageBackend) Delete(ctx context.Context, path string) error { return nil }
func (m *mockStorageBackend) Exists(ctx context.Context, path string) (bool, error) {
	return false, nil
}
func (m *mockStorageBackend) List(ctx context.Context, prefix string) ([]string, error) {
	return nil, nil
}
func (m *mockStorageBackend) Close() error       { return nil }
func (m *mockStorageBackend) Type() string       { return "mock" }
func (m *mockStorageBackend) ConfigJSON() string { return "{}" }
func (m *mockStorageBackend) ReadToAt(_ context.Context, _ string, _ io.Writer, _ int64) error {
	return nil
}
func (m *mockStorageBackend) StatFile(_ context.Context, _ string) (int64, error) { return -1, nil }
func (m *mockStorageBackend) AppendReader(_ context.Context, _ string, _ io.Reader, _ int64) error {
	return nil
}

func TestRowsToColumnar_SingleRecord(t *testing.T) {
	buffer := createTestArrowBuffer(t)
	now := time.Now()
	rows := []*models.Record{
		{
			Measurement: "cpu",
			Time:        now,
			Timestamp:   now.UnixMicro(),
			Fields:      map[string]interface{}{"usage": 75.5, "system": 10.2},
			Tags:        map[string]string{"host": "server01", "region": "us-east"},
		},
	}
	result := buffer.rowsToColumnar("cpu", rows)
	if result.Measurement != "cpu" {
		t.Errorf("Expected measurement 'cpu', got %q", result.Measurement)
	}
	if !result.Columnar {
		t.Error("Expected Columnar to be true")
	}
	timeCol, ok := result.Columns["time"]
	if !ok || len(timeCol) != 1 || timeCol[0].(int64) != now.UnixMicro() {
		t.Errorf("time column: ok=%v len=%d", ok, len(timeCol))
	}
	usageCol := result.Columns["usage"]
	if !ok || usageCol[0].(float64) != 75.5 {
		t.Errorf("usage: %v", usageCol[0])
	}
	hostCol := result.Columns["host"]
	if hostCol[0].(string) != "server01" {
		t.Errorf("host: %v", hostCol[0])
	}
}

func TestRowsToColumnar_MultipleRecords(t *testing.T) {
	buffer := createTestArrowBuffer(t)
	now := time.Now()
	rows := []*models.Record{
		{Measurement: "cpu", Timestamp: now.UnixMicro(), Fields: map[string]interface{}{"usage": 75.5}, Tags: map[string]string{"host": "server01"}},
		{Measurement: "cpu", Timestamp: now.Add(time.Second).UnixMicro(), Fields: map[string]interface{}{"usage": 80.2}, Tags: map[string]string{"host": "server02"}},
		{Measurement: "cpu", Timestamp: now.Add(2 * time.Second).UnixMicro(), Fields: map[string]interface{}{"usage": 65.0}, Tags: map[string]string{"host": "server03"}},
	}
	result := buffer.rowsToColumnar("cpu", rows)
	if len(result.Columns["time"]) != 3 {
		t.Errorf("Expected 3 time values, got %d", len(result.Columns["time"]))
	}
	if len(result.Columns["usage"]) != 3 {
		t.Errorf("Expected 3 usage values, got %d", len(result.Columns["usage"]))
	}
}

func TestRowsToColumnar_SchemaVariation(t *testing.T) {
	buffer := createTestArrowBuffer(t)
	now := time.Now()
	rows := []*models.Record{
		{Measurement: "cpu", Timestamp: now.UnixMicro(), Fields: map[string]interface{}{"usage": 75.5}, Tags: map[string]string{"host": "server01"}},
		{Measurement: "cpu", Timestamp: now.Add(time.Second).UnixMicro(), Fields: map[string]interface{}{"usage": 80.2, "system": 15.0}, Tags: map[string]string{"host": "server02", "region": "us-east"}},
		{Measurement: "cpu", Timestamp: now.Add(2 * time.Second).UnixMicro(), Fields: map[string]interface{}{"system": 20.0}, Tags: map[string]string{"region": "us-west"}},
	}
	result := buffer.rowsToColumnar("cpu", rows)
	if _, ok := result.Columns["usage"]; !ok {
		t.Error("Missing 'usage' column")
	}
	if _, ok := result.Columns["system"]; !ok {
		t.Error("Missing 'system' column")
	}
	usageCol := result.Columns["usage"]
	if usageCol[2] != nil {
		t.Errorf("Row 2: expected nil usage, got %v", usageCol[2])
	}
	systemCol := result.Columns["system"]
	if systemCol[0] != nil {
		t.Errorf("Row 0: expected nil system, got %v", systemCol[0])
	}
}

func TestRowsToColumnar_EmptyRecords(t *testing.T) {
	buffer := createTestArrowBuffer(t)
	result := buffer.rowsToColumnar("cpu", []*models.Record{})
	if result.Measurement != "cpu" || !result.Columnar || len(result.Columns) != 0 {
		t.Errorf("Unexpected empty result")
	}
}

func TestRowsToColumnar_TimestampFallback(t *testing.T) {
	buffer := createTestArrowBuffer(t)
	now := time.Now()
	rows := []*models.Record{
		{Measurement: "cpu", Timestamp: 12345678, Time: now, Fields: map[string]interface{}{"value": 1.0}},
		{Measurement: "cpu", Time: now, Fields: map[string]interface{}{"value": 2.0}},
		{Measurement: "cpu", Fields: map[string]interface{}{"value": 3.0}},
	}
	result := buffer.rowsToColumnar("cpu", rows)
	timeCol := result.Columns["time"]
	if timeCol[0].(int64) != 12345678 {
		t.Errorf("Row 0: expected 12345678, got %v", timeCol[0])
	}
	if timeCol[1].(int64) != now.UnixMicro() {
		t.Errorf("Row 1: expected %d, got %v", now.UnixMicro(), timeCol[1])
	}
	if timeCol[2].(int64) == 0 {
		t.Error("Row 2: expected non-zero timestamp")
	}
}

func TestRowsToColumnar_DifferentFieldTypes(t *testing.T) {
	buffer := createTestArrowBuffer(t)
	now := time.Now()
	rows := []*models.Record{
		{Measurement: "metrics", Timestamp: now.UnixMicro(), Fields: map[string]interface{}{
			"float_val": 3.14159, "int_val": int64(42), "string_val": "hello", "bool_val": true,
		}},
	}
	result := buffer.rowsToColumnar("metrics", rows)
	if result.Columns["float_val"][0].(float64) != 3.14159 {
		t.Errorf("float mismatch")
	}
	if result.Columns["int_val"][0].(int64) != 42 {
		t.Errorf("int mismatch")
	}
	if result.Columns["string_val"][0].(string) != "hello" {
		t.Errorf("string mismatch")
	}
	if result.Columns["bool_val"][0].(bool) != true {
		t.Errorf("bool mismatch")
	}
}

func TestDecimal128_ConvertColumnsToTyped(t *testing.T) {
	buffer := createTestArrowBuffer(t)
	buffer.decimalConfig = map[string]map[string]config.DecimalSpec{
		"trades": {
			"price":  {Precision: 18, Scale: 8},
			"amount": {Precision: 18, Scale: 8},
		},
	}
	columns := map[string][]interface{}{
		"time":   {int64(1000000), int64(2000000), int64(3000000)},
		"price":  {float64(123.456), float64(789.012), float64(0.00000001)},
		"amount": {float64(100.0), float64(200.5), nil},
		"symbol": {"AAPL", "GOOG", "MSFT"},
	}
	entry := &bufferEntry{columns: make(map[string]ColumnData)}
	numRecords, err := buffer.convertAndAppendToEntry(entry, "trades", columns)
	if err != nil {
		t.Fatalf("convertAndAppendToEntry failed: %v", err)
	}
	if numRecords != 3 {
		t.Fatalf("expected 3 records, got %d", numRecords)
	}
	// Verify price is []decimal128.Num
	priceCD, ok := entry.columns["price"]
	if !ok {
		t.Fatal("missing 'price' column")
	}
	prices, ok := priceCD.Data.([]decimal128.Num)
	if !ok {
		t.Fatalf("expected []decimal128.Num for price, got %T", priceCD.Data)
	}
	if len(prices) != 3 {
		t.Fatalf("expected 3 prices, got %d", len(prices))
	}
	// Price has nil validity (no nulls — ColumnData doesn't cross-column backfill)
	if priceCD.Validity != nil {
		t.Errorf("expected nil validity for price, got %v", priceCD.Validity)
	}
	// Verify amount has validity (has nil)
	amountCD, ok := entry.columns["amount"]
	if !ok {
		t.Fatal("missing 'amount' column")
	}
	amounts, ok := amountCD.Data.([]decimal128.Num)
	if !ok {
		t.Fatalf("expected []decimal128.Num for amount, got %T", amountCD.Data)
	}
	if len(amounts) != 3 {
		t.Fatalf("expected 3 amounts, got %d", len(amounts))
	}
	if amountCD.Validity == nil {
		t.Fatal("expected validity bitmap for amount (has nil)")
	}
	if amountCD.Validity[0] != true || amountCD.Validity[1] != true || amountCD.Validity[2] != false {
		t.Errorf("unexpected validity: %v", amountCD.Validity)
	}
	// Verify non-decimal columns
	timeCD, ok := entry.columns["time"]
	if !ok || len(timeCD.Data.([]int64)) != 3 {
		t.Fatalf("time column: ok=%v len=%d", ok, len(timeCD.Data.([]int64)))
	}
	symbolCD, ok := entry.columns["symbol"]
	if !ok || symbolCD.Data.([]string)[0] != "AAPL" {
		t.Fatalf("symbol column: ok=%v", ok)
	}
}

func TestDecimal128_WriteParquetRoundTrip(t *testing.T) {
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	writer := NewArrowWriter(&config.IngestConfig{Compression: "snappy", UseDictionary: true}, logger)
	price1, _ := decimal128.FromFloat64(123.45678901, 18, 8)
	price2, _ := decimal128.FromFloat64(789.01234567, 18, 8)
	price3, _ := decimal128.FromFloat64(0.00000001, 18, 8)
	entry := &bufferEntry{
		columns: map[string]ColumnData{
			"time":   {Data: []int64{1000000, 2000000, 3000000}},
			"price":  {Data: []decimal128.Num{price1, price2, price3}},
			"symbol": {Data: []string{"AAPL", "GOOG", "MSFT"}},
		},
	}
	decimalCols := map[string]config.DecimalSpec{"price": {Precision: 18, Scale: 8}}
	data, err := writer.WriteParquetColumnar(context.Background(), "trades", entry, decimalCols, nil)
	if err != nil {
		t.Fatalf("WriteParquetColumnar failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty Parquet data")
	}
	t.Logf("Written %d bytes of Parquet data with Decimal128 columns", len(data))
}

func TestDecimal128_StringConversion(t *testing.T) {
	buffer := createTestArrowBuffer(t)
	buffer.decimalConfig = map[string]map[string]config.DecimalSpec{
		"trades": {"price": {Precision: 38, Scale: 18}},
	}
	columns := map[string][]interface{}{
		"time":  {int64(1000000)},
		"price": {"123.456789012345678901"},
	}
	entry := &bufferEntry{columns: make(map[string]ColumnData)}
	_, err := buffer.convertAndAppendToEntry(entry, "trades", columns)
	if err != nil {
		t.Fatalf("convertAndAppendToEntry failed: %v", err)
	}
	priceCD, ok := entry.columns["price"]
	if !ok {
		t.Fatal("missing 'price'")
	}
	prices, ok := priceCD.Data.([]decimal128.Num)
	if !ok || len(prices) != 1 || prices[0] == (decimal128.Num{}) {
		t.Fatal("expected non-zero decimal128 value")
	}
}

func TestDecimal128_NoConfigNoImpact(t *testing.T) {
	buffer := createTestArrowBuffer(t)
	columns := map[string][]interface{}{
		"time":  {int64(1000000)},
		"price": {float64(123.45)},
	}
	entry := &bufferEntry{columns: make(map[string]ColumnData)}
	_, err := buffer.convertAndAppendToEntry(entry, "trades", columns)
	if err != nil {
		t.Fatalf("convertAndAppendToEntry failed: %v", err)
	}
	priceCD, ok := entry.columns["price"]
	if !ok {
		t.Fatal("missing 'price'")
	}
	if _, ok := priceCD.Data.([]float64); !ok {
		t.Fatalf("expected []float64, got %T", priceCD.Data)
	}
}

func TestDecimal128_SchemaMetadata(t *testing.T) {
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	writer := NewArrowWriter(&config.IngestConfig{Compression: "snappy"}, logger)
	price1, _ := decimal128.FromFloat64(100.0, 18, 8)
	columns := map[string]ColumnData{
		"time":  {Data: []int64{1000000}},
		"price": {Data: []decimal128.Num{price1}},
	}
	schema, err := writer.inferSchema(columns, nil, map[string]config.DecimalSpec{"price": {Precision: 18, Scale: 8}})
	if err != nil {
		t.Fatalf("inferSchema failed: %v", err)
	}
	md := schema.Metadata()
	idx := md.FindKey("iedb:decimals")
	if idx < 0 {
		t.Fatal("expected iedb:decimals metadata")
	}
	if md.Values()[idx] != "price:18,8" {
		t.Errorf("expected 'price:18,8', got %q", md.Values()[idx])
	}
}

func BenchmarkGetColumnSignature(b *testing.B) {
	columns := map[string]ColumnData{
		"time": {}, "host": {}, "region": {}, "datacenter": {},
		"usage_idle": {}, "usage_user": {}, "usage_system": {}, "usage_iowait": {},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getColumnSignature(columns)
	}
}

func TestGetColumnSignature(t *testing.T) {
	tests := []struct {
		name     string
		columns  map[string]ColumnData
		expected string
	}{
		{name: "empty", columns: map[string]ColumnData{}, expected: ""},
		{name: "single", columns: map[string]ColumnData{"value": {Data: []float64{1.0}}}, expected: "value:f64"},
		{name: "sorted",
			columns: map[string]ColumnData{
				"zebra": {Data: []string{"a"}}, "apple": {Data: []int64{1}}, "mango": {Data: []float64{1.0}},
			},
			expected: "apple:i64,mango:f64,zebra:str",
		},
		{name: "skip internal",
			columns: map[string]ColumnData{
				"value": {Data: []float64{1.0}}, "time": {Data: []int64{1}}, "_hidden": {Data: []string{"x"}},
			},
			expected: "time:i64,value:f64",
		},
		{name: "type change",
			columns:  map[string]ColumnData{"cpu": {Data: []float64{1.0}}},
			expected: "cpu:f64",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getColumnSignature(tt.columns); got != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, got)
			}
		})
	}
	t.Run("type change produces different signature", func(t *testing.T) {
		sig1 := getColumnSignature(map[string]ColumnData{"cpu": {Data: []int64{1}}})
		sig2 := getColumnSignature(map[string]ColumnData{"cpu": {Data: []float64{1.0}}})
		if sig1 == sig2 {
			t.Errorf("Expected different sigs, both got %q", sig1)
		}
	})
}

func BenchmarkRowsToColumnar(b *testing.B) {
	buffer := &ArrowBuffer{logger: zerolog.New(os.Stderr).Level(zerolog.Disabled)}
	now := time.Now()
	rows := make([]*models.Record, 1000)
	for i := 0; i < 1000; i++ {
		rows[i] = &models.Record{
			Measurement: "cpu", Timestamp: now.Add(time.Duration(i)*time.Second).UnixMicro(),
			Fields: map[string]interface{}{"usage": float64(i) / 10.0, "system": float64(i) / 20.0},
			Tags:   map[string]string{"host": "server01", "region": "us-east"},
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer.rowsToColumnar("cpu", rows)
	}
}

func BenchmarkRowsToColumnar_SchemaVariation(b *testing.B) {
	buffer := &ArrowBuffer{logger: zerolog.New(os.Stderr).Level(zerolog.Disabled)}
	now := time.Now()
	rows := make([]*models.Record, 1000)
	for i := 0; i < 1000; i++ {
		fields := map[string]interface{}{"usage": float64(i) / 10.0}
		if i%2 == 0 {
			fields["system"] = float64(i) / 20.0
		}
		if i%3 == 0 {
			fields["idle"] = float64(100-i) / 10.0
		}
		tags := map[string]string{"host": "server01"}
		if i%4 == 0 {
			tags["region"] = "us-east"
		}
		rows[i] = &models.Record{
			Measurement: "cpu", Timestamp: now.Add(time.Duration(i)*time.Second).UnixMicro(),
			Fields: fields, Tags: tags,
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer.rowsToColumnar("cpu", rows)
	}
}

func TestSliceColumnsByIndices_BoundsCheck(t *testing.T) {
	columns := map[string]ColumnData{
		"time":        {Data: []int64{1, 2, 3, 4, 5, 6, 7, 8}},
		"cpu":         {Data: []float64{10.0, 20.0, 30.0, 40.0, 50.0, 60.0}},
		"temperature": {Data: []float64{70.0, 80.0}},
		"host":        {Data: []string{"a", "b", "c", "d"}},
		"active":      {Data: []bool{true, false, true}},
	}
	indices := []int{0, 2, 4, 6, 7}
	// colSlice operates on single ColumnData — test per-column
	timeCol := colSlice(columns["time"], indices).Data.([]int64)
	expected := []int64{1, 3, 5, 7, 8}
	for i, v := range expected {
		if timeCol[i] != v {
			t.Errorf("time[%d]=%d want %d", i, timeCol[i], v)
		}
	}
	cpuCol := colSlice(columns["cpu"], indices).Data.([]float64)
	expCPU := []float64{10.0, 30.0, 50.0, 0.0, 0.0}
	for i, v := range expCPU {
		if cpuCol[i] != v {
			t.Errorf("cpu[%d]=%f want %f", i, cpuCol[i], v)
		}
	}
	hostCol := colSlice(columns["host"], indices).Data.([]string)
	expHost := []string{"a", "c", "", "", ""}
	for i, v := range expHost {
		if hostCol[i] != v {
			t.Errorf("host[%d]=%q want %q", i, hostCol[i], v)
		}
	}
	activeCol := colSlice(columns["active"], indices).Data.([]bool)
	expActive := []bool{true, true, false, false, false}
	for i, v := range expActive {
		if activeCol[i] != v {
			t.Errorf("active[%d]=%v want %v", i, activeCol[i], v)
		}
	}
}

func TestSortEntryByKeys_NilValidityEntry(t *testing.T) {
	entry := &bufferEntry{
		columns: map[string]ColumnData{
			"time": {Data: []int64{3, 1, 2}},
			"val":  {Data: []float64{30.0, 10.0, 20.0}, Validity: nil},
		},
		tagColumns: []string{},
		schema:     "time:i64,val:f64",
	}
	sorted := sortEntryByKeys(entry, []string{"time"})
	sortedTime := sorted.columns["time"].Data.([]int64)
	exp := []int64{1, 2, 3}
	for i, v := range exp {
		if sortedTime[i] != v {
			t.Errorf("sorted[%d]=%d want %d", i, sortedTime[i], v)
		}
	}
	if valCD, ok := sorted.columns["val"]; !ok {
		t.Error("column 'val' missing")
	} else if valCD.Validity != nil {
		t.Errorf("validity=%v want nil", valCD.Validity)
	}
	if sorted.schema != entry.schema {
		t.Errorf("schema changed")
	}
}

func TestSliceEntryByIndices_NilValidityEntry(t *testing.T) {
	entry := &bufferEntry{
		columns: map[string]ColumnData{
			"time": {Data: []int64{1, 2, 3, 4}},
			"val":  {Data: []float64{10.0, 20.0, 30.0, 40.0}, Validity: nil},
		},
		tagColumns: []string{},
		schema:     "time:i64,val:f64",
	}
	sliced := sliceEntryByIndices(entry, []int{0, 2})
	slicedTime := sliced.columns["time"].Data.([]int64)
	if len(slicedTime) != 2 || slicedTime[0] != 1 || slicedTime[1] != 3 {
		t.Errorf("time=%v want [1 3]", slicedTime)
	}
	if valCD, ok := sliced.columns["val"]; !ok {
		t.Error("column 'val' missing")
	} else if valCD.Validity != nil {
		t.Errorf("validity=%v want nil", valCD.Validity)
	}
}
