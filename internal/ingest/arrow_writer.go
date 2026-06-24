package ingest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"iedb/internal/config"
	"iedb/internal/metrics"
	"iedb/internal/storage"
	"iedb/internal/tiering"
	"iedb/internal/wal"
	"iedb/pkg/models"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/rs/zerolog"
)

const (
	flushTypeAsync = "async"
	flushTypeSync  = "sync"
)

// sharedArrowAllocator is a package-level shared allocator for Arrow operations.
// memory.GoAllocator is documented as thread-safe for concurrent use.
// Using a shared instance avoids allocator overhead per-write operation.
var sharedArrowAllocator = memory.NewGoAllocator()

// sortIndicesPool reuses permutation index slices across sort operations.
// Sorting allocates []int of size N for permutation indices; reusing them
// avoids repeated allocation when flush batches are consistently sized.
var sortIndicesPool = sync.Pool{
	New: func() any { return make([]int, 0, 10000) },
}

func getSortIndices(n int) []int {
	if v := sortIndicesPool.Get(); v != nil {
		s := v.([]int)
		if cap(s) >= n {
			return s[:n]
		}
	}
	return make([]int, n)
}

func putSortIndices(s []int) {
	if s != nil && cap(s) > 0 {
		sortIndicesPool.Put(s)
	}
}

// int64SliceToTimestamps reinterprets a []int64 as []arrow.Timestamp without copying.
// Safe because arrow.Timestamp is defined as `type Timestamp int64` (identical layout).
// LIFETIME: the caller must ensure src is not GC'd or reallocated while the returned
// slice or any Arrow array/builder built from it is still alive. Use only within the
// same stack frame as src.
func int64SliceToTimestamps(src []int64) []arrow.Timestamp {
	return *(*[]arrow.Timestamp)(unsafe.Pointer(&src))
}

// buildValidityBitmap converts a []bool validity slice to an Arrow bitmap buffer.
// Returns (nil, 0) when all values are valid (no nulls), which is the Arrow convention
// for "all valid" and avoids an allocation entirely on the common path.
func buildValidityBitmap(valid []bool) (*memory.Buffer, int) {
	if len(valid) == 0 {
		return nil, 0
	}
	// Fast scan: count nulls. Most time-series data has zero nulls.
	nullCount := 0
	for _, v := range valid {
		if !v {
			nullCount++
		}
	}
	if nullCount == 0 {
		return nil, 0
	}
	// Build Arrow bitmap: 1 bit per value, LSB-first within each byte.
	bitmapSize := (len(valid) + 7) / 8
	bitmap := make([]byte, bitmapSize)
	for i, v := range valid {
		if v {
			bitmap[i/8] |= 1 << (i % 8)
		}
	}
	return memory.NewBufferBytes(bitmap), nullCount
}

// getFlushMessageType returns the human-readable flush type message for logging
func getFlushMessageType(flushType string) string {
	switch flushType {
	case flushTypeAsync:
		return "Async flush"
	case flushTypeSync:
		return "Periodic flush"
	default:
		return flushType + " flush"
	}
}

// ArrowWriter handles Arrow schema inference and Parquet writing
type ArrowWriter struct {
	compression     compress.Compression
	useDictionary   bool
	writeStatistics bool
	dataPageVersion string

	// Pre-built Parquet writer properties (immutable after construction)
	writerProps *parquet.WriterProperties
	arrowProps  pqarrow.ArrowWriterProperties
	logger      zerolog.Logger
}

// NewArrowWriter creates a new Arrow writer
func NewArrowWriter(cfg *config.IngestConfig, logger zerolog.Logger) *ArrowWriter {
	// Parse compression
	var comp compress.Compression
	switch cfg.Compression {
	case "gzip":
		comp = compress.Codecs.Gzip
	case "zstd":
		comp = compress.Codecs.Zstd
	case "snappy":
		comp = compress.Codecs.Snappy
	default:
		comp = compress.Codecs.Snappy
	}

	// Pre-build Parquet writer properties once — they are immutable config objects
	// that do not change after startup. Rebuilding them on every flush wastes CPU.
	writerOpts := []parquet.WriterProperty{
		parquet.WithCompression(comp),
		parquet.WithDictionaryDefault(cfg.UseDictionary),
		parquet.WithStats(cfg.WriteStatistics),
	}
	if cfg.DataPageVersion == "2.0" {
		writerOpts = append(writerOpts, parquet.WithDataPageVersion(parquet.DataPageV2))
	}
	return &ArrowWriter{
		compression:     comp,
		useDictionary:   cfg.UseDictionary,
		writeStatistics: cfg.WriteStatistics,
		dataPageVersion: cfg.DataPageVersion,
		writerProps:     parquet.NewWriterProperties(writerOpts...),
		arrowProps:      pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()),
		logger:          logger.With().Str("component", "arrow-writer").Logger(),
	}
}

// =============================================================================
// Type Conversion Helpers - Consolidated from duplicate implementations
// =============================================================================

// toInt64 converts any numeric type to int64
// Returns (value, ok) where ok is false if conversion failed
func toInt64(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int8:
		return int64(val), true
	case int16:
		return int64(val), true
	case int32:
		return int64(val), true
	case int64:
		return val, true
	case uint:
		// On 64-bit systems, uint can exceed MaxInt64
		if uint64(val) > math.MaxInt64 {
			return 0, false
		}
		return int64(val), true
	case uint8:
		return int64(val), true
	case uint16:
		return int64(val), true
	case uint32:
		return int64(val), true
	case uint64:
		if val > math.MaxInt64 {
			return 0, false
		}
		return int64(val), true
	case float32:
		// Bounds check required before conversion to int64
		if val > float32(math.MaxInt64) || val < float32(math.MinInt64) {
			return 0, false
		}
		return int64(val), true //nolint:gosec // Bounds checked above
	case float64:
		// Bounds check required before conversion to int64
		if val > float64(math.MaxInt64) || val < float64(math.MinInt64) {
			return 0, false
		}
		return int64(val), true //nolint:gosec // Bounds checked above
	default:
		return 0, false
	}
}

// toFloat64 converts any numeric type to float64
// Returns (value, ok) where ok is false if conversion failed
func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float32:
		return float64(val), true
	case float64:
		return val, true
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint8:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	default:
		return 0, false
	}
}

// firstNonNil returns the first non-nil value from a slice
// Returns nil if the slice is empty or all values are nil
func firstNonNil(col []interface{}) interface{} {
	for _, v := range col {
		if v != nil {
			return v
		}
	}
	return nil
}

// inferArrowType determines the Arrow data type from a Go value
// Special handling for "time" column which uses Timestamp type
func inferArrowType(colName string, firstVal interface{}) (arrow.DataType, error) {
	if colName == "time" {
		return arrow.FixedWidthTypes.Timestamp_us, nil
	}

	switch firstVal.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return arrow.PrimitiveTypes.Int64, nil
	case float32, float64:
		return arrow.PrimitiveTypes.Float64, nil
	case string:
		return arrow.BinaryTypes.String, nil
	case bool:
		return arrow.FixedWidthTypes.Boolean, nil
	default:
		return nil, fmt.Errorf("unsupported type: %T", firstVal)
	}
}

// sortColumnsTimeFirst sorts column names with "time" first, then alphabetical
func sortColumnsTimeFirst(colNames []string) {
	sort.Slice(colNames, func(i, j int) bool {
		if colNames[i] == "time" {
			return true
		}
		if colNames[j] == "time" {
			return false
		}
		return colNames[i] < colNames[j]
	})
}

// =============================================================================
// Schema Inference
// =============================================================================

// inferSchema infers Arrow schema from columnar data.
// tagColumns optionally lists which columns are tags (stored as schema metadata for compaction dedup).
// decimalCols optionally maps column names to DecimalSpec for Decimal128 columns.
func (w *ArrowWriter) inferSchema(columns map[string]ColumnData, tagColumns []string, decimalCols map[string]config.DecimalSpec) (*arrow.Schema, error) {
	// Collect sorted column names for deterministic schema field order.
	// Go map iteration is non-deterministic; without sorting, Arrow VIEW
	// schemas would have random column order, causing query result mismatches.
	names := make([]string, 0, len(columns))
	for name := range columns {
		if len(name) > 0 && name[0] == '_' {
			continue // skip internal metadata columns
		}
		names = append(names, name)
	}
	sort.Strings(names)

	var fields []arrow.Field
	for _, name := range names {
		col := columns[name]

		var arrowType arrow.DataType

		switch arr := col.Data.(type) {
		case []int64:
			// Special case: time column uses timestamp type
			if name == "time" {
				arrowType = arrow.FixedWidthTypes.Timestamp_us
			} else {
				arrowType = arrow.PrimitiveTypes.Int64
			}
		case []float64:
			arrowType = arrow.PrimitiveTypes.Float64
		case []string:
			arrowType = arrow.BinaryTypes.String
		case []bool:
			arrowType = arrow.FixedWidthTypes.Boolean
		case []decimal128.Num:
			if spec, ok := decimalCols[name]; ok {
				arrowType = &arrow.Decimal128Type{Precision: spec.Precision, Scale: spec.Scale}
			} else {
				// Fallback: use max precision if no config (shouldn't happen in normal flow)
				arrowType = &arrow.Decimal128Type{Precision: 38, Scale: 18}
			}
		default:
			return nil, fmt.Errorf("unsupported column type for column %s: %T", name, arr)
		}

		fields = append(fields, arrow.Field{Name: name, Type: arrowType, Nullable: true})
	}

	// Build schema metadata keys/values
	var metaKeys, metaValues []string

	// Store tag column names for compaction auto-dedup
	if len(tagColumns) > 0 {
		sorted := make([]string, len(tagColumns))
		copy(sorted, tagColumns)
		sort.Strings(sorted)
		metaKeys = append(metaKeys, "iedb:tags")
		metaValues = append(metaValues, strings.Join(sorted, ","))
	}

	// Store decimal column specs for self-describing Parquet files
	if len(decimalCols) > 0 {
		var parts []string
		for col, spec := range decimalCols {
			parts = append(parts, fmt.Sprintf("%s:%d,%d", col, spec.Precision, spec.Scale))
		}
		sort.Strings(parts)
		metaKeys = append(metaKeys, "iedb:decimals")
		metaValues = append(metaValues, strings.Join(parts, ";"))
	}

	var metadata *arrow.Metadata
	if len(metaKeys) > 0 {
		md := arrow.NewMetadata(metaKeys, metaValues)
		metadata = &md
	}

	return arrow.NewSchema(fields, metadata), nil
}

// WriteParquetColumnar writes columnar data directly to Parquet (zero-copy path).
// validity is an optional map of column name → []bool where false means null.
// Columns without a validity entry (or when validity is nil) are treated as fully valid.
// tagColumns optionally lists which columns are tags (stored as Parquet metadata for compaction dedup).
// decimalCols optionally maps column names to DecimalSpec for Decimal128 type inference.
func (w *ArrowWriter) WriteParquetColumnar(ctx context.Context, measurement string, entry *bufferEntry, decimalCols map[string]config.DecimalSpec, schema *arrow.Schema) ([]byte, error) {
	// Use provided schema, or infer as fallback (defense-in-depth)
	if schema == nil {
		var err error
		schema, err = w.inferSchema(entry.columns, entry.tagColumns, decimalCols)
		if err != nil {
			return nil, fmt.Errorf("failed to infer schema: %w", err)
		}
	}

	// Create Arrow arrays from columns
	// MEMORY FIX: Use shared allocator instead of creating new one per write
	mem := sharedArrowAllocator
	builders := make([]array.Builder, len(schema.Fields()))
	arrays := make([]arrow.Array, len(schema.Fields()))

	// CRITICAL: Release both builders and arrays to prevent memory leak
	defer func() {
		for _, builder := range builders {
			if builder != nil {
				builder.Release()
			}
		}
		for _, arr := range arrays {
			if arr != nil {
				arr.Release()
			}
		}
	}()

	// Build arrays — numeric types bypass the builder to avoid an extra
	// data copy inside AppendValues.  Zero-copy buffers (NewBufferBytes)
	// reference the sorted column slices directly; lifetime is safe because
	// entry.columns outlives every array and record created in this function.
	for i, field := range schema.Fields() {
		cd, ok := entry.columns[field.Name]
		if !ok {
			return nil, fmt.Errorf("column %s not found in data", field.Name)
		}
		col := cd.Data
		colValidity := cd.Validity

		switch field.Type.ID() {
		case arrow.INT64:
			intCol, ok := col.([]int64)
			if !ok {
				return nil, fmt.Errorf("column %s: expected []int64, got %T", field.Name, col)
			}
			if len(intCol) == 0 {
				builder := array.NewInt64Builder(mem)
				builders[i] = builder
				arrays[i] = builder.NewArray()
				continue
			}
			validBuf, nulls := buildValidityBitmap(colValidity)
			dataBuf := memory.NewBufferBytes(
				unsafe.Slice((*byte)(unsafe.Pointer(&intCol[0])), len(intCol)*8))
			arrData := array.NewData(arrow.PrimitiveTypes.Int64, len(intCol),
				[]*memory.Buffer{validBuf, dataBuf}, nil, nulls, 0)
			arrays[i] = array.NewInt64Data(arrData)
			arrData.Release()

		case arrow.TIMESTAMP:
			intCol, ok := col.([]int64)
			if !ok {
				return nil, fmt.Errorf("column %s: expected []int64 for timestamp, got %T", field.Name, col)
			}
			if len(intCol) == 0 {
				builder := array.NewTimestampBuilder(mem, arrow.FixedWidthTypes.Timestamp_us.(*arrow.TimestampType))
				builders[i] = builder
				arrays[i] = builder.NewArray()
				continue
			}
			tsValues := int64SliceToTimestamps(intCol)
			validBuf, nulls := buildValidityBitmap(colValidity)
			dataBuf := memory.NewBufferBytes(
				unsafe.Slice((*byte)(unsafe.Pointer(&tsValues[0])), len(tsValues)*8))
			arrData := array.NewData(arrow.FixedWidthTypes.Timestamp_us, len(tsValues),
				[]*memory.Buffer{validBuf, dataBuf}, nil, nulls, 0)
			arrays[i] = array.NewTimestampData(arrData)
			arrData.Release()

		case arrow.FLOAT64:
			floatCol, ok := col.([]float64)
			if !ok {
				return nil, fmt.Errorf("column %s: expected []float64, got %T", field.Name, col)
			}
			if len(floatCol) == 0 {
				builder := array.NewFloat64Builder(mem)
				builders[i] = builder
				arrays[i] = builder.NewArray()
				continue
			}
			validBuf, nulls := buildValidityBitmap(colValidity)
			dataBuf := memory.NewBufferBytes(
				unsafe.Slice((*byte)(unsafe.Pointer(&floatCol[0])), len(floatCol)*8))
			arrData := array.NewData(arrow.PrimitiveTypes.Float64, len(floatCol),
				[]*memory.Buffer{validBuf, dataBuf}, nil, nulls, 0)
			arrays[i] = array.NewFloat64Data(arrData)
			arrData.Release()

		case arrow.STRING:
			builder := array.NewStringBuilder(mem)
			builders[i] = builder
			if strCol, ok := col.([]string); ok {
				builder.AppendValues(strCol, colValidity)
			} else {
				return nil, fmt.Errorf("column %s: expected []string, got %T", field.Name, col)
			}
			arrays[i] = builder.NewArray()

		case arrow.BOOL:
			builder := array.NewBooleanBuilder(mem)
			builders[i] = builder
			if boolCol, ok := col.([]bool); ok {
				builder.AppendValues(boolCol, colValidity)
			} else {
				return nil, fmt.Errorf("column %s: expected []bool, got %T", field.Name, col)
			}
			arrays[i] = builder.NewArray()

		case arrow.DECIMAL128:
			decCol, ok := col.([]decimal128.Num)
			if !ok {
				return nil, fmt.Errorf("column %s: expected []decimal128.Num, got %T", field.Name, col)
			}
			if len(decCol) == 0 {
				dt := field.Type.(*arrow.Decimal128Type)
				builder := array.NewDecimal128Builder(mem, dt)
				builders[i] = builder
				arrays[i] = builder.NewArray()
				continue
			}
			dt := field.Type.(*arrow.Decimal128Type)
			validBuf, nulls := buildValidityBitmap(colValidity)
			dataBuf := memory.NewBufferBytes(
				unsafe.Slice((*byte)(unsafe.Pointer(&decCol[0])), len(decCol)*16))
			arrData := array.NewData(dt, len(decCol),
				[]*memory.Buffer{validBuf, dataBuf}, nil, nulls, 0)
			arrays[i] = array.NewDecimal128Data(arrData)
			arrData.Release()

		default:
			return nil, fmt.Errorf("unsupported Arrow type for column %s: %s", field.Name, field.Type.Name())
		}
	}

	return w.writeRecordToParquet(schema, arrays)
}

// writeRecordToParquet writes Arrow arrays to Parquet bytes
func (w *ArrowWriter) writeRecordToParquet(schema *arrow.Schema, arrays []arrow.Array) ([]byte, error) {
	// Create record batch
	record := array.NewRecord(schema, arrays, -1)
	defer record.Release()

	// Write to Parquet with pre-allocated buffer.
	// Estimate compressed output size to avoid reallocation growth.
	// Conservative heuristic: numRows × numCols × 12 bytes per compressed value.
	var buf bytes.Buffer
	buf.Grow(int(record.NumRows()) * int(record.NumCols()) * 12)

	// Use pre-built writer properties (constructed once at startup, immutable)
	writer, err := pqarrow.NewFileWriter(
		schema,
		&buf,
		w.writerProps,
		w.arrowProps,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Parquet writer: %w", err)
	}

	// Write record batch
	if err := writer.Write(record); err != nil {
		writer.Close()
		return nil, fmt.Errorf("failed to write record batch: %w", err)
	}

	// Close writer
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close Parquet writer: %w", err)
	}

	w.logger.Debug().
		Int("columns", len(schema.Fields())).
		Int("rows", int(record.NumRows())).
		Int("size", buf.Len()).
		Msg("Wrote Parquet file")

	return buf.Bytes(), nil
}

// bufferShard represents a single shard of the buffer map with its own lock

// bufferEntry holds all buffered data and metadata for a single measurement key.
type bufferEntry struct {
	columns        map[string]ColumnData // growing typed column arrays with null bitmaps
	tagColumns     []string              // tag column names (stable for this schema, set on first write)
	startTime      time.Time             // first record arrival time
	recordCount    int                   // total record count
	estimatedBytes uint64                // estimated memory usage
	schema         string                // column signature for schema evolution
	arrowSchema    *arrow.Schema         // inferred Arrow schema (nil until first flush preparation)
}

// Entry is the exported alias for bufferEntry. External consumers
// reference this name; internal code uses bufferEntry for consistency.
type Entry = bufferEntry

// isEmpty returns true when the entry has no buffered columnar data.
// Such entries are "empty shells" kept to preserve the cached arrowSchema
// across flush cycles — they are eligible for GC after extended idleness.
func (e *bufferEntry) isEmpty() bool {
	return len(e.columns) == 0
}

// GetColumns returns the columns map for external consumers (database package).
func (e *bufferEntry) GetColumns() map[string]ColumnData { return e.columns }

// GetSchema returns the column signature for external consumers (database package).
func (e *bufferEntry) GetSchema() string { return e.schema }

// GetTagColumns returns the tag column names for external consumers (database package).
func (e *bufferEntry) GetTagColumns() []string { return e.tagColumns }

// GetArrowSchema returns the cached Arrow schema, or nil if not yet inferred.
func (e *bufferEntry) GetArrowSchema() *arrow.Schema { return e.arrowSchema }

// appendEntryToEntry appends data from src bufferEntry into dst bufferEntry.

// This is the core operation for the columnar in-place accumulation approach.
func appendEntryToEntry(dst, src *bufferEntry) {
	if dst.columns == nil {
		dst.columns = make(map[string]ColumnData)
	}
	for name, col := range src.columns {
		if existing, ok := dst.columns[name]; ok {
			dst.columns[name] = colAppend(existing, col)
		} else {
			dst.columns[name] = col
		}
	}
	if len(dst.tagColumns) == 0 && len(src.tagColumns) > 0 {
		dst.tagColumns = src.tagColumns
	}
	dst.recordCount += src.recordCount
	dst.estimatedBytes += uint64(src.recordCount) * estimateBytesPerRow(src)
}

// =============================================================================
// Column Dispatch Functions -- centralized type switch for all column operations.
// Every function that operates on typed column slices routes through these
// instead of writing inline type switches, so adding a 6th type only requires
// adding one case to each of these functions.
// =============================================================================

// ColumnData bundles a typed column slice with its null bitmap.
// Data is the underlying typed slice ([]int64, []float64, etc.).
// Validity is nil when all values are valid; when non-nil, len(Validity) == len(slice).
type ColumnData struct {
	Data     any
	Validity []bool
}

// colLen returns the number of rows in a typed column slice.
func colLen(c ColumnData) int {
	switch v := c.Data.(type) {
	case []int64:
		return len(v)
	case []float64:
		return len(v)
	case []string:
		return len(v)
	case []bool:
		return len(v)
	case []decimal128.Num:
		return len(v)
	default:
		return 0
	}
}

// colAppend concatenates two ColumnData values. dst and src must have the same element type.
// Validity bitmaps are merged: src.Validity is appended to dst.Validity.
// When only one side has Validity, the other side is treated as all-valid (true).
func colAppend(dst, src ColumnData) ColumnData {
	if dst.Data == nil {
		return src
	}
	switch v := src.Data.(type) {
	case []int64:
		{
			dstData := dst.Data.([]int64)
			need := len(dstData) + len(v)
			if cap(dstData) < need {
				grown := make([]int64, len(dstData), need+need/4)
				copy(grown, dstData)
				dstData = grown
			}
			var mergedValidity []bool
			if dst.Validity != nil || src.Validity != nil {
				mergedValidity = mergeValidity(dst.Validity, src.Validity, len(dstData), len(v))
			}
			return ColumnData{Data: append(dstData, v...), Validity: mergedValidity}
		}
	case []float64:
		{
			dstData := dst.Data.([]float64)
			need := len(dstData) + len(v)
			if cap(dstData) < need {
				grown := make([]float64, len(dstData), need+need/4)
				copy(grown, dstData)
				dstData = grown
			}
			var mergedValidity []bool
			if dst.Validity != nil || src.Validity != nil {
				mergedValidity = mergeValidity(dst.Validity, src.Validity, len(dstData), len(v))
			}
			return ColumnData{Data: append(dstData, v...), Validity: mergedValidity}
		}
	case []string:
		{
			dstData := dst.Data.([]string)
			need := len(dstData) + len(v)
			if cap(dstData) < need {
				grown := make([]string, len(dstData), need+need/4)
				copy(grown, dstData)
				dstData = grown
			}
			var mergedValidity []bool
			if dst.Validity != nil || src.Validity != nil {
				mergedValidity = mergeValidity(dst.Validity, src.Validity, len(dstData), len(v))
			}
			return ColumnData{Data: append(dstData, v...), Validity: mergedValidity}
		}
	case []bool:
		{
			dstData := dst.Data.([]bool)
			need := len(dstData) + len(v)
			if cap(dstData) < need {
				grown := make([]bool, len(dstData), need+need/4)
				copy(grown, dstData)
				dstData = grown
			}
			var mergedValidity []bool
			if dst.Validity != nil || src.Validity != nil {
				mergedValidity = mergeValidity(dst.Validity, src.Validity, len(dstData), len(v))
			}
			return ColumnData{Data: append(dstData, v...), Validity: mergedValidity}
		}
	case []decimal128.Num:
		{
			dstData := dst.Data.([]decimal128.Num)
			need := len(dstData) + len(v)
			if cap(dstData) < need {
				grown := make([]decimal128.Num, len(dstData), need+need/4)
				copy(grown, dstData)
				dstData = grown
			}
			var mergedValidity []bool
			if dst.Validity != nil || src.Validity != nil {
				mergedValidity = mergeValidity(dst.Validity, src.Validity, len(dstData), len(v))
			}
			return ColumnData{Data: append(dstData, v...), Validity: mergedValidity}
		}
	default:
		return dst
	}
}

// mergeValidity merges two validity bitmaps when appending columns.
// dstLen is the number of pre-existing rows that dst covers.
// srcLen is the number of rows in the source batch.
func mergeValidity(dst, src []bool, dstLen, srcLen int) []bool {
	if dst == nil && src == nil {
		return nil
	}
	if dst == nil {
		dst = make([]bool, dstLen)
		for i := range dst {
			dst[i] = true
		}
	}
	if src == nil {
		pad := make([]bool, srcLen)
		for i := range pad {
			pad[i] = true
		}
		return append(dst, pad...)
	}
	return append(dst, src...)
}

// colSlice extracts rows by index list. Returns a new slice; source is unmodified.
func colSlice(c ColumnData, indices []int) ColumnData {
	n := len(indices)
	switch v := c.Data.(type) {
	case []int64:
		out := make([]int64, n)
		vl := len(v)
		for i, idx := range indices {
			if idx < vl {
				out[i] = v[idx]
			}
		}
		return ColumnData{Data: out, Validity: sliceValidity(c.Validity, indices)}
	case []float64:
		out := make([]float64, n)
		vl := len(v)
		for i, idx := range indices {
			if idx < vl {
				out[i] = v[idx]
			}
		}
		return ColumnData{Data: out, Validity: sliceValidity(c.Validity, indices)}
	case []string:
		out := make([]string, n)
		vl := len(v)
		for i, idx := range indices {
			if idx < vl {
				out[i] = v[idx]
			}
		}
		return ColumnData{Data: out, Validity: sliceValidity(c.Validity, indices)}
	case []bool:
		out := make([]bool, n)
		vl := len(v)
		for i, idx := range indices {
			if idx < vl {
				out[i] = v[idx]
			}
		}
		return ColumnData{Data: out, Validity: sliceValidity(c.Validity, indices)}
	case []decimal128.Num:
		out := make([]decimal128.Num, n)
		vl := len(v)
		for i, idx := range indices {
			if idx < vl {
				out[i] = v[idx]
			}
		}
		return ColumnData{Data: out, Validity: sliceValidity(c.Validity, indices)}
	default:
		return c
	}
}

// sliceValidity creates a new validity bitmap by indexing an existing bitmap with indices.
func sliceValidity(v []bool, indices []int) []bool {
	if v == nil {
		return nil
	}
	out := make([]bool, len(indices))
	vl := len(v)
	for i, idx := range indices {
		if idx < vl {
			out[i] = v[idx]
		}
	}
	return out
}

// colSliceFrom returns the sub-slice of col from start index to end.
func colSliceFrom(c ColumnData, start int) ColumnData {
	switch v := c.Data.(type) {
	case []int64:
		return ColumnData{Data: v[start:], Validity: sliceValidityFrom(c.Validity, start)}
	case []float64:
		return ColumnData{Data: v[start:], Validity: sliceValidityFrom(c.Validity, start)}
	case []string:
		return ColumnData{Data: v[start:], Validity: sliceValidityFrom(c.Validity, start)}
	case []bool:
		return ColumnData{Data: v[start:], Validity: sliceValidityFrom(c.Validity, start)}
	case []decimal128.Num:
		return ColumnData{Data: v[start:], Validity: sliceValidityFrom(c.Validity, start)}
	default:
		return c
	}
}

// sliceValidityFrom slices a validity bitmap from start to end.
func sliceValidityFrom(v []bool, start int) []bool {
	if v == nil {
		return nil
	}
	return v[start:]
}

// colPermute reorders a column according to permutation indices. Allocates a new slice.
func colPermute(c ColumnData, indices []int) ColumnData {
	n := len(indices)
	switch v := c.Data.(type) {
	case []int64:
		out := make([]int64, n)
		for i, idx := range indices {
			out[i] = v[idx]
		}
		return ColumnData{Data: out, Validity: permuteValidity(c.Validity, indices)}
	case []float64:
		out := make([]float64, n)
		for i, idx := range indices {
			out[i] = v[idx]
		}
		return ColumnData{Data: out, Validity: permuteValidity(c.Validity, indices)}
	case []string:
		out := make([]string, n)
		for i, idx := range indices {
			out[i] = v[idx]
		}
		return ColumnData{Data: out, Validity: permuteValidity(c.Validity, indices)}
	case []bool:
		out := make([]bool, n)
		for i, idx := range indices {
			out[i] = v[idx]
		}
		return ColumnData{Data: out, Validity: permuteValidity(c.Validity, indices)}
	case []decimal128.Num:
		out := make([]decimal128.Num, n)
		for i, idx := range indices {
			out[i] = v[idx]
		}
		return ColumnData{Data: out, Validity: permuteValidity(c.Validity, indices)}
	default:
		return c
	}
}

// permuteValidity creates a new validity bitmap by reindexing with permutation indices.
func permuteValidity(v []bool, indices []int) []bool {
	if v == nil {
		return nil
	}
	out := make([]bool, len(indices))
	for i, idx := range indices {
		out[i] = v[idx]
	}
	return out
}

// colLess compares two rows within a single column. false < true for bool columns.
func colLess(c ColumnData, i, j int) bool {
	switch v := c.Data.(type) {
	case []int64:
		return v[i] < v[j]
	case []float64:
		return v[i] < v[j]
	case []string:
		return v[i] < v[j]
	case []bool:
		return !v[i] && v[j]
	case []decimal128.Num:
		return v[i].Less(v[j])
	default:
		return false
	}
}

// colEstBytesPerRow estimates bytes per row for a single column.
func colEstBytesPerRow(c ColumnData) uint64 {
	switch v := c.Data.(type) {
	case []int64:
		return 8
	case []float64:
		return 8
	case []bool:
		return 1
	case []string:
		n := len(v)
		if n == 0 {
			return 32
		}
		if n > 100 {
			n = 100
		}
		var sumLen int
		for i := 0; i < n; i++ {
			sumLen += len(v[i])
		}
		return uint64(sumLen / n)
	default:
		return 64
	}
}

// colIthVal returns the i-th value, or nil if the value is null.
func colIthVal(c ColumnData, i int) any {
	if c.Validity != nil && i < len(c.Validity) && !c.Validity[i] {
		return nil
	}
	switch v := c.Data.(type) {
	case []int64:
		if i < len(v) {
			return v[i]
		}
	case []float64:
		if i < len(v) {
			return v[i]
		}
	case []string:
		if i < len(v) {
			return v[i]
		}
	case []bool:
		if i < len(v) {
			return v[i]
		}
	case []decimal128.Num:
		if i < len(v) {
			return v[i]
		}
	}
	return nil
}

// colTypeTag returns a short type tag string for a typed column slice.
// Used by getColumnSignature. Returns "" for unknown types.
func colTypeTag(c ColumnData) string {
	switch c.Data.(type) {
	case []int64:
		return "i64"
	case []float64:
		return "f64"
	case []string:
		return "str"
	case []bool:
		return "bool"
	case []decimal128.Num:
		return "dec"
	default:
		return "unk"
	}
}

type bufferShard struct {
	mu      sync.RWMutex
	buffers map[string]*bufferEntry // bufferKey → merged buffer state
}

// estimateBytesPerRow estimates per-row memory usage for a bufferEntry.
func estimateBytesPerRow(entry *bufferEntry) uint64 {
	if entry == nil || len(entry.columns) == 0 {
		return 256
	}
	var totalBytes uint64
	for _, col := range entry.columns {
		totalBytes += colEstBytesPerRow(col)
	}
	totalBytes += uint64(len(entry.columns))
	return totalBytes
}

// estimateBytesFromData estimates total memory usage from a columnar data map.
// numRows is the row count (from any typed column's len). Used by prepend paths
// where data arrives without a TypedColumnBatch wrapper.
func estimateBytesFromData(columns map[string]ColumnData, numRows int) uint64 {
	if len(columns) == 0 {
		return 0
	}
	var perRow uint64
	for _, col := range columns {
		perRow += colEstBytesPerRow(col)
	}
	perRow += uint64(len(columns))
	return perRow * uint64(numRows)
}

// flushTask represents a flush operation to be executed by workers
type flushTask struct {
	ctx         context.Context
	cancel      context.CancelFunc // must be called when task completes to release resources
	bufferKey   string
	database    string
	measurement string
	entry       *bufferEntry // replaces data, validity, tagColumns, recordCount, arrowSchema
	trigger     string       // "size", "age", "hard_limit", or "manual"
}

// WALWriter interface for Write-Ahead Log support
type WALWriter interface {
	Append(records []map[string]interface{}) error
	AppendRaw(payload []byte) error                          // Zero-copy: write raw msgpack bytes directly
	AppendRawWithMeta(database string, payload []byte) error // Zero-copy with database metadata envelope
	AppendControl(ctrlType wal.ControlType, database, measurement string) error
	Stats() map[string]interface{}
	Close() error
}

// FileRegistrar announces a newly written Parquet file to the cluster-wide
// manifest. Implementations should be non-blocking — file registration is
// fire-and-forget from the flush path's perspective. Wired by the cluster
// coordinator when peer replication is enabled (Enterprise feature).
//
// sha256 is a hex-encoded content checksum of the Parquet file bytes.
// Peers use this to verify the integrity of data pulled from the origin.
type FileRegistrar interface {
	RegisterFile(database, measurement, path string, partitionTime time.Time, sizeBytes int64, sha256 string)
}

// ArrowBuffer manages buffering and periodic flushing of Arrow data
// Uses lock sharding to reduce contention across concurrent writes
type ArrowBuffer struct {
	config  *config.IngestConfig
	storage storage.Backend
	writer  *ArrowWriter

	// Optional WAL for durability
	wal WALWriter

	// Optional tiering manager for registering files in tier metadata
	tieringManager *tiering.Manager

	// Optional file registrar for cluster-wide file manifest (Enterprise peer replication)
	// Set by cmd/iedb/main.go when clustering + peer replication is enabled.
	// Called asynchronously after each flush — never blocks the flush path.
	fileRegistrar FileRegistrar

	// Adaptive flush engine (always active in production).
	adaptiveFlush atomic.Pointer[AdaptiveFlushEngine]

	// Buffer overflow protection: hard limit in bytes.
	// When total buffer estimated bytes exceeds hardLimit, oldest entries
	// are evicted. If still over, writeColumnarInternal returns 503.
	// Set via SetBufferHardLimit; 0 means no limit (backward compatible).
	// atomic: written from SIGHUP goroutine (SetBufferHardLimit), read from write path (ensureBufferSpace).
	hardLimit atomic.Uint64

	// OPTIMIZATION: Shard buffers to reduce lock contention
	// Configurable via ingest.shard_count (default 32)
	// Each shard handles ~1/N of measurements where N = shard count
	// This allows N concurrent writes to different measurements
	shards     []*bufferShard
	shardCount uint32

	// Background flush
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// OPTIMIZATION: Worker pool for bounded flush concurrency
	// Prevents goroutine explosion under sustained load
	flushQueue   chan flushTask
	flushWorkers int

	// closing is the shutdown short-circuit checked by tryEnqueueFlush.
	// See Close() for the full ordering rationale; senders see this
	// flag set before the channel could be closed (the channel is
	// never closed; workers exit on b.ctx.Done()).
	closing atomic.Bool
	// Sort key configuration (for multi-column sorting)
	sortKeysConfig  map[string][]string // measurement -> sort keys
	defaultSortKeys []string            // default sort keys

	// Decimal column configuration (for Decimal128 precision support)
	decimalConfig        map[string]map[string]config.DecimalSpec // measurement -> column -> spec
	defaultDecimalConfig map[string]config.DecimalSpec            // default decimal columns

	// Flush timeout for storage writes (prevents workers from blocking forever on S3 hangs)
	flushTimeout time.Duration
	maxBufferAge time.Duration // pre-calculated; prefers MaxBufferAgeSeconds, falls back to MaxBufferAgeMS

	// Metrics (using atomic operations to avoid lock contention)
	totalRecordsBuffered atomic.Int64
	totalRecordsWritten  atomic.Int64
	totalFlushes         atomic.Int64
	totalErrors          atomic.Int64
	totalWALErrors       atomic.Int64 // WAL write failures (real I/O / serialization errors)
	totalWALDropped      atomic.Int64 // WAL backpressure drops (entry queued but channel full)
	queueDepth           atomic.Int64 // Current flush queue depth

	// walDropLogSampler debounces the WAL-dropped Warn so a sustained
	// burst of backpressure produces ~one log line per second instead
	// of one per dropped record. Operators get the rate via the
	// totalWALDropped counter and the underlying metrics.IncWALDroppedEntries
	// counter; the log line is for human-readable signal that the
	// degraded state is in effect.
	walDropLastLogNano atomic.Int64

	logger zerolog.Logger
}

// getColumnSignature returns a sorted string of "name:type" pairs for schema comparison.
// Encodes both column names and their Go slice types so that a type change (e.g.
// int64→float64 on the same column) is detected as schema evolution and triggers a
// flush before the new-schema data is appended.
func getColumnSignature(columns map[string]ColumnData) string {
	type colEntry struct{ name, typ string }
	entries := make([]colEntry, 0, len(columns))
	size := -1 // will add 1 per comma; starts at -1 so the first entry adds 0 commas
	for name, val := range columns {
		if len(name) == 0 || name[0] == '_' {
			continue // skip empty and internal columns
		}
		typ := colTypeTag(val)
		entries = append(entries, colEntry{name, typ})
		size += 1 + len(name) + 1 + len(typ) // comma + name + colon + typ
	}
	if len(entries) == 0 {
		return ""
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })
	var sb strings.Builder
	sb.Grow(size)
	for i, e := range entries {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(e.name)
		sb.WriteByte(':')
		sb.WriteString(e.typ)
	}
	return sb.String()
}

// schemaHash returns the first 8 hex chars of the SHA-256 of the column signature.
func schemaHash(signature string) string {
	h := sha256.Sum256([]byte(signature))
	return hex.EncodeToString(h[:4]) // 8 hex chars
}

// schemaKey constructs a buffer key that includes the schema hash, so different
// schemas for the same (database, measurement) naturally get separate buffer entries.
func schemaKey(database, measurement, signature string) string {
	return database + "/" + measurement + "__" + schemaHash(signature)
}

// StripSchemaHash removes the "__hash" suffix from a buffer key if present.
// Returns the original key and an empty string if no hash suffix exists.
// Only treats a suffix as a hash when it is exactly 8 lowercase hex characters
// (matching schemaHash), so measurement names containing "__" are not
// incorrectly truncated.
func StripSchemaHash(bufferKey string) (baseKey string, hash string) {
	if idx := strings.LastIndex(bufferKey, "__"); idx >= 0 {
		suffix := bufferKey[idx+2:]
		if isSchemaHashHex(suffix) {
			return bufferKey[:idx], suffix
		}
	}
	return bufferKey, ""
}

// isSchemaHashHex returns true when s is exactly 8 lowercase hex characters.
func isSchemaHashHex(s string) bool {
	if len(s) != 8 {
		return false
	}
	for i := 0; i < 8; i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

// hasTypeConflict returns true if oldSig and newSig share any field name
// with a different type. Field additions/removals are NOT conflicts — only
// the same field changing type (e.g. float64→int64) qualifies.
func hasTypeConflict(oldSig, newSig string) bool {
	if oldSig == "" || newSig == "" || oldSig == newSig {
		return false
	}
	oldFields := parseSignature(oldSig)
	newFields := parseSignature(newSig)
	for name, oldType := range oldFields {
		if newType, exists := newFields[name]; exists && newType != oldType {
			return true
		}
	}
	return false
}

// parseSignature parses "name1:type1,name2:type2,..." into a map.
func parseSignature(sig string) map[string]string {
	result := make(map[string]string)
	for _, pair := range strings.Split(sig, ",") {
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) == 2 {
			result[parts[0]] = parts[1]
		}
	}
	return result
}

// flushTypeConflicts scans all shards for existing buffer entries under the
// same measurement (base key) whose schema has a type conflict with newSig.
// Conflicting entries are flushed before the new-schema data is written.
func (b *ArrowBuffer) flushTypeConflicts(baseKey, newSig string) {
	prefix := baseKey + "__"
	for i := uint32(0); i < b.shardCount; i++ {
		shard := b.shards[i]

		// Collect conflict keys under the lock, then release before
		// calling flushBufferLocked (which releases and re-acquires the
		// lock during I/O).  Iterating the map while the lock is released
		// causes a fatal "concurrent map iteration and map write" panic
		// when a concurrent ingest inserts into the same shard.
		shard.mu.Lock()
		var conflictKeys []string
		for key, entry := range shard.buffers {
			if entry.isEmpty() {
				continue
			}
			if strings.HasPrefix(key, prefix) && hasTypeConflict(entry.schema, newSig) {
				conflictKeys = append(conflictKeys, key)
			}
		}
		shard.mu.Unlock()

		for _, key := range conflictKeys {
			// Re-check under lock: the entry may have been flushed
			// or deleted between collection and now.
			shard.mu.Lock()
			entry, exists := shard.buffers[key]
			if !exists || entry.isEmpty() || !hasTypeConflict(entry.schema, newSig) {
				shard.mu.Unlock()
				continue
			}
			parts := splitBufferKey(key)
			if len(parts) != 2 {
				shard.mu.Unlock()
				continue
			}

			b.logger.Debug().
				Str("buffer_key", key).
				Str("old_schema", entry.schema).
				Str("new_schema", newSig).
				Msg("Type conflict detected, flushing old buffer")

			// flushBufferLocked releases and re-acquires the lock.
			// We are holding shard.mu — flushBufferLocked will
			// Unlock for I/O and Lock before returning.
			flushCtx, flushCancel := context.WithTimeout(b.ctx, b.flushTimeout)
			if err := b.flushBufferLocked(flushCtx, shard, key, parts[0], parts[1], "schema_conflict"); err != nil {
				b.logger.Warn().Err(err).
					Str("buffer_key", key).
					Str("old_schema", entry.schema).
					Str("new_schema", newSig).
					Msg("Type conflict flush failed — data restored to buffer or WAL")
			}
			flushCancel()
			// flushBufferLocked returns with the lock re-acquired;
			// release it so the next iteration can lock cleanly.
			shard.mu.Unlock()
		}
	}
}

// getShard returns the shard for a given buffer key using FNV-1a hash
func (b *ArrowBuffer) getShard(bufferKey string) *bufferShard {
	// FNV-1a hash (fast, good distribution)
	hash := uint32(2166136261)
	for i := 0; i < len(bufferKey); i++ {
		hash ^= uint32(bufferKey[i])
		hash *= 16777619
	}
	return b.shards[hash%b.shardCount]
}

// getSortKeys returns sort keys for a measurement.
// Users configure ADDITIONAL sort columns - "time" is always appended automatically.
// This ensures data is always sorted by time within each partition.
func (b *ArrowBuffer) getSortKeys(measurement string) []string {
	var keys []string

	// Check measurement-specific config
	if measurementKeys, exists := b.sortKeysConfig[measurement]; exists {
		keys = measurementKeys
	} else {
		// Use default
		keys = b.defaultSortKeys
	}

	// Always ensure "time" is the last sort key
	// Skip adding if already present (backwards compatibility with legacy configs)
	for _, k := range keys {
		if k == "time" {
			return keys
		}
	}

	// Append "time" - users configure ADDITIONAL sort keys only
	return append(keys, "time")
}

// getDecimalColumns returns the decimal column config for a measurement.
// Falls back to default config if no measurement-specific config exists.
func (b *ArrowBuffer) getDecimalColumns(measurement string) map[string]config.DecimalSpec {
	if specs, exists := b.decimalConfig[measurement]; exists {
		return specs
	}
	return b.defaultDecimalConfig
}

// bufferMaxAgeFromConfig derives the max buffer age from config,
// preferring the new MaxBufferAgeSeconds field over the deprecated
// MaxBufferAgeMS. 0 for both means "use the adaptive engine's default."
func bufferMaxAgeFromConfig(cfg *config.IngestConfig) time.Duration {
	if cfg.MaxBufferAgeSeconds > 0 {
		return time.Duration(cfg.MaxBufferAgeSeconds) * time.Second
	}
	return time.Duration(cfg.MaxBufferAgeMS) * time.Millisecond
}

// NewArrowBuffer creates a new Arrow buffer with automatic flushing
func NewArrowBuffer(cfg *config.IngestConfig, storage storage.Backend, logger zerolog.Logger) *ArrowBuffer {
	ctx, cancel := context.WithCancel(context.Background())

	// Use configured values with sensible fallbacks
	flushWorkers := cfg.FlushWorkers
	if flushWorkers <= 0 {
		flushWorkers = 16 // Fallback if not configured
	}

	queueSize := cfg.FlushQueueSize
	if queueSize <= 0 {
		queueSize = 100 // Fallback if not configured
	}

	shardCount := cfg.ShardCount
	if shardCount <= 0 {
		shardCount = 32 // Fallback if not configured
	}

	// Parse sort keys config using shared function
	sortKeysConfig, defaultSortKeys, err := config.ParseSortKeys(*cfg)
	if err != nil {
		logger.Warn().Err(err).Msg("Invalid sort keys config, using defaults")
		sortKeysConfig = make(map[string][]string)
		defaultSortKeys = []string{"time"}
	}

	// Parse decimal column config
	decimalConfig, defaultDecimalConfig, err := config.ParseDecimalColumns(*cfg)
	if err != nil {
		logger.Warn().Err(err).Msg("Invalid decimal columns config, decimal support disabled")
		decimalConfig = make(map[string]map[string]config.DecimalSpec)
		defaultDecimalConfig = nil
	}

	// Parse flush timeout (default 30s)
	flushTimeout := time.Duration(cfg.FlushTimeoutSeconds) * time.Second
	if cfg.FlushTimeoutSeconds <= 0 {
		flushTimeout = 30 * time.Second
	}

	buffer := &ArrowBuffer{
		config:               cfg,
		storage:              storage,
		writer:               NewArrowWriter(cfg, logger),
		shards:               make([]*bufferShard, shardCount),
		shardCount:           uint32(shardCount),
		ctx:                  ctx,
		cancel:               cancel,
		flushQueue:           make(chan flushTask, queueSize),
		flushWorkers:         flushWorkers,
		flushTimeout:         flushTimeout,
		maxBufferAge:         bufferMaxAgeFromConfig(cfg),
		sortKeysConfig:       sortKeysConfig,
		defaultSortKeys:      defaultSortKeys,
		decimalConfig:        decimalConfig,
		defaultDecimalConfig: defaultDecimalConfig,
		logger:               logger.With().Str("component", "arrow-buffer").Logger(),
	}

	// Initialize shards
	for i := 0; i < shardCount; i++ {
		buffer.shards[i] = &bufferShard{
			buffers: make(map[string]*bufferEntry),
		}
	}

	// Start flush workers
	for i := 0; i < flushWorkers; i++ {
		buffer.wg.Add(1)
		go buffer.flushWorker(i)
	}

	buffer.logger.Info().
		Str("compression", cfg.Compression).
		Int("shards", shardCount).
		Int("flush_workers", flushWorkers).
		Int("queue_size", queueSize).
		Dur("flush_timeout", flushTimeout).
		Msg("ArrowBuffer initialized with lock sharding and worker pool")

	return buffer
}

// SetWAL sets the WAL writer for durability
// When set, records are written to WAL before being buffered
func (b *ArrowBuffer) SetWAL(wal WALWriter) {
	b.wal = wal
	b.logger.Info().Msg("WAL enabled for ArrowBuffer")
}

// SetTieringManager sets the tiering manager for automatic file registration.
// When set, newly written parquet files are automatically registered in tiering metadata.
func (b *ArrowBuffer) SetTieringManager(tm *tiering.Manager) {
	b.tieringManager = tm
	b.logger.Info().Msg("Tiering manager enabled for ArrowBuffer - files will be auto-registered")
}

// SetFileRegistrar sets the cluster-wide file manifest registrar.
// When set, newly written Parquet files are announced to the cluster manifest
// asynchronously (non-blocking) — used by peer replication to discover files
// that need to be pulled from other nodes.
func (b *ArrowBuffer) SetFileRegistrar(fr FileRegistrar) {
	b.fileRegistrar = fr
	b.logger.Info().Msg("File registrar enabled for ArrowBuffer - files will be announced to cluster manifest")
}

// registerFileInTiering registers a newly written parquet file in the tiering metadata
// and (if enabled) announces it to the cluster-wide file manifest for peer replication.
// This allows the tiering system to track the file for future migration and query routing,
// and enables Enterprise peer replication to replicate the file to other cluster nodes.
//
// sha256Hex is a hex-encoded SHA-256 of the Parquet bytes, computed by the caller on the
// in-memory buffer immediately before the backend write. Peers validate downloaded bytes
// against this checksum.
func (b *ArrowBuffer) registerFileInTiering(ctx context.Context, database, measurement, storagePath string, partitionTime time.Time, sizeBytes int64, sha256Hex string) {
	// Register in local tiering metadata (hot/cold tracking)
	if b.tieringManager != nil {
		metadata := b.tieringManager.GetMetadata()
		if metadata != nil {

			file := &tiering.FileMetadata{
				Path:          storagePath,
				Database:      database,
				Measurement:   measurement,
				PartitionTime: partitionTime,
				Tier:          tiering.TierHot,
				SizeBytes:     sizeBytes,
				CreatedAt:     time.Now().UTC(),
			}

			if err := metadata.RecordFile(ctx, file); err != nil {
				b.logger.Warn().Err(err).
					Str("path", storagePath).
					Str("database", database).
					Str("measurement", measurement).
					Msg("Failed to register file in tiering metadata")
			}
		}
	}

	// Announce to cluster-wide file manifest (peer replication).
	// The registrar implementation MUST be non-blocking — it's called on
	// the hot flush path.
	if b.fileRegistrar != nil {
		b.fileRegistrar.RegisterFile(database, measurement, storagePath, partitionTime, sizeBytes, sha256Hex)
	}
}

// columnarToWALRecords converts columnar data to row-based records for WAL storage
// Each record includes database, measurement, and all column values
func (b *ArrowBuffer) columnarToWALRecords(database string, record *models.ColumnarRecord) []map[string]interface{} {
	if len(record.Columns) == 0 {
		return nil
	}

	// Find the number of rows from the first column
	var numRows int
	for _, col := range record.Columns {
		numRows = len(col)
		break
	}

	if numRows == 0 {
		return nil
	}

	// Convert columnar to row format
	records := make([]map[string]interface{}, numRows)
	for i := 0; i < numRows; i++ {
		row := map[string]interface{}{
			"_database":    database,
			"_measurement": record.Measurement,
		}
		for colName, colData := range record.Columns {
			if i < len(colData) {
				row[colName] = colData[i]
			}
		}
		records[i] = row
	}

	return records
}

// rowsToColumnar converts a slice of row-format Records into a ColumnarRecord.
// This enables the MessagePack handler to accept row-format data and convert it
// to the columnar format expected by the Arrow writer.
//
// The conversion:
// - time column: populated from Record.Timestamp (microseconds) or Record.Time
// - Tag columns: stored directly by tag name (matches Line Protocol behavior)
// - Field columns: stored directly by field name (conflicts get "_value" suffix)
func (b *ArrowBuffer) rowsToColumnar(measurement string, rows []*models.Record) *models.ColumnarRecord {
	if len(rows) == 0 {
		return &models.ColumnarRecord{
			Measurement: measurement,
			Columnar:    true,
			Columns:     make(map[string][]interface{}),
		}
	}

	// Pre-allocate columns map - estimate based on first record
	firstRow := rows[0]
	estimatedCols := 1 + len(firstRow.Tags) + len(firstRow.Fields) // time + tags + fields
	columns := make(map[string][]interface{}, estimatedCols)

	// Initialize time column
	columns["time"] = make([]interface{}, 0, len(rows))

	// First pass: collect all unique column names across all rows
	// This handles schema variations where different rows may have different fields/tags
	allTags := make(map[string]struct{})
	allFields := make(map[string]struct{})
	for _, row := range rows {
		for tag := range row.Tags {
			allTags[tag] = struct{}{}
		}
		for field := range row.Fields {
			allFields[field] = struct{}{}
		}
	}

	// Initialize columns for all tags and fields
	// Tags are stored directly by name (matching Line Protocol behavior)
	// Fields that conflict with tags get "_value" suffix
	for tag := range allTags {
		columns[tag] = make([]interface{}, 0, len(rows))
	}
	for field := range allFields {
		if _, hasTag := allTags[field]; hasTag {
			columns[field+"_value"] = make([]interface{}, 0, len(rows))
		} else {
			columns[field] = make([]interface{}, 0, len(rows))
		}
	}

	// Second pass: populate columns with values
	for _, row := range rows {
		// Handle timestamp: prefer Timestamp (microseconds) if set, otherwise convert Time
		var timestamp int64
		if row.Timestamp != 0 {
			timestamp = row.Timestamp
		} else if !row.Time.IsZero() {
			timestamp = row.Time.UnixMicro()
		} else {
			// Use current time if no timestamp provided
			timestamp = time.Now().UnixMicro()
		}
		columns["time"] = append(columns["time"], timestamp)

		// Add tag values (nil for missing tags to maintain column alignment)
		for tag := range allTags {
			if val, ok := row.Tags[tag]; ok {
				columns[tag] = append(columns[tag], val)
			} else {
				columns[tag] = append(columns[tag], nil)
			}
		}

		// Add field values (nil for missing fields to maintain column alignment)
		// Fields that conflict with tags get "_value" suffix
		for field := range allFields {
			colName := field
			if _, hasTag := allTags[field]; hasTag {
				colName = field + "_value"
			}
			if val, ok := row.Fields[field]; ok {
				columns[colName] = append(columns[colName], val)
			} else {
				columns[colName] = append(columns[colName], nil)
			}
		}
	}

	// Collect tag column names for Parquet metadata (enables auto-dedup in compaction)
	tagColumns := make([]string, 0, len(allTags))
	for tag := range allTags {
		tagColumns = append(tagColumns, tag)
	}

	return &models.ColumnarRecord{
		Measurement: measurement,
		Columnar:    true,
		Columns:     columns,
		TagColumns:  tagColumns,
	}
}

// Write adds records to the buffer (for MessagePack handler)
func (b *ArrowBuffer) Write(ctx context.Context, database string, records interface{}) error {
	// Handle batch of records (from MessagePack decoder)
	recordList, ok := records.([]interface{})
	if !ok {
		return fmt.Errorf("expected []interface{}, got %T", records)
	}

	// OPTIMIZATION: Lazy initialization - avoid map allocation for pure columnar writes (common path)
	var rowRecordsByMeasurement map[string][]*models.Record

	for _, record := range recordList {
		switch r := record.(type) {
		case *models.ColumnarRecord:
			if err := b.writeColumnar(ctx, database, r); err != nil {
				b.logger.Error().Err(err).Str("measurement", r.Measurement).Msg("Failed to write columnar record")
				b.totalErrors.Add(1)
				return err
			}
		case *models.Record:
			// Lazy init: only allocate map when we actually have row records
			if rowRecordsByMeasurement == nil {
				rowRecordsByMeasurement = make(map[string][]*models.Record)
			}
			// Group row records by measurement for batch conversion
			rowRecordsByMeasurement[r.Measurement] = append(rowRecordsByMeasurement[r.Measurement], r)
		default:
			b.logger.Warn().Interface("type", fmt.Sprintf("%T", record)).Msg("Unknown record type")
		}
	}

	// Convert grouped row records to columnar format and write
	for measurement, rowRecords := range rowRecordsByMeasurement {
		columnar := b.rowsToColumnar(measurement, rowRecords)
		if err := b.writeColumnar(ctx, database, columnar); err != nil {
			b.logger.Error().Err(err).Str("measurement", measurement).Msg("Failed to write converted row records")
			b.totalErrors.Add(1)
			return err
		}
	}

	return nil
}

// WriteColumnarDirect writes columnar data directly to the buffer
// This is the preferred method for Line Protocol which already has columnar data
func (b *ArrowBuffer) WriteColumnarDirect(ctx context.Context, database, measurement string, columns map[string][]interface{}) error {
	record := &models.ColumnarRecord{
		Measurement: measurement,
		Columns:     columns,
		Columnar:    true,
	}
	return b.writeColumnar(ctx, database, record)
}

// WriteColumnarRecord writes a pre-built ColumnarRecord to the buffer.
// Preserves TagColumns metadata for Parquet schema (enables auto-dedup in compaction).
func (b *ArrowBuffer) WriteColumnarRecord(ctx context.Context, database string, record *models.ColumnarRecord) error {
	return b.writeColumnar(ctx, database, record)
}

// WriteColumnarDirectNoWAL writes columnar data without writing to WAL.
// Used during WAL recovery to avoid re-writing recovered data back to WAL.
func (b *ArrowBuffer) WriteColumnarDirectNoWAL(ctx context.Context, database, measurement string, columns map[string][]interface{}) error {
	record := &models.ColumnarRecord{
		Measurement: measurement,
		Columns:     columns,
		Columnar:    true,
	}
	return b.writeColumnarInternal(ctx, database, record, true)
}

// WriteTypedColumnarDirect writes a pre-typed bufferEntry to the buffer,
// bypassing the []interface{} → typed conversion in convertColumnsToTyped.
// Used by format-specific parsers (e.g., TLE) that know column types at compile time.
func (b *ArrowBuffer) WriteTypedColumnarDirect(ctx context.Context, database, measurement string, entry *bufferEntry) error {
	return b.writeTypedColumnarInternal(ctx, database, measurement, entry, false)
}

// WriteArrowRecord writes an arrow.Record directly to the ingest buffer.
// This is the Flight DoPut fast path: zero-copy where possible (numeric columns
// reference the arrow array backing store), copy where necessary (string/bool).
// The record is converted to a bufferEntry and routed through the same WAL,
// flush-trigger, and VIEW-refresh machinery as all other write paths.
func (b *ArrowBuffer) WriteArrowRecord(ctx context.Context, database, measurement string, record arrow.Record) error {
	entry := arrowRecordToEntry(record)
	return b.writeTypedColumnarInternal(ctx, database, measurement, entry, false)
}

// arrowRecordToEntry converts an arrow.Record into a bufferEntry suitable
// for writeTypedColumnarInternal. Numeric columns (int64, float64) are
// extracted by reference to avoid copying the Arrow backing store.
func arrowRecordToEntry(record arrow.Record) *bufferEntry {
	entry := &bufferEntry{
		columns:     make(map[string]ColumnData, int(record.NumCols())),
		recordCount: int(record.NumRows()),
	}

	schema := record.Schema()
	for i := 0; i < int(record.NumCols()); i++ {
		col := record.Column(i)
		colName := schema.Field(i).Name
		cd := arrowArrayToColumnData(col)
		if cd.Data != nil || cd.Validity != nil {
			entry.columns[colName] = cd
		}
	}

	// Compute column signature for buffer routing
	if len(entry.columns) > 0 {
		entry.schema = getColumnSignature(entry.columns)
	}

	return entry
}

// arrowArrayToColumnData converts a single arrow.Array to ColumnData.
// Numeric types (int64, float64) reference the arrow backing store directly
// (zero-copy). String and boolean types are copied.
//
// IMPORTANT (zero-copy contract): Int64/Float64 ColumnData.Data holds a reference
// to the Arrow array's backing memory. The caller MUST ensure the source
// arrow.Record/arrow.Array is NOT released until the buffer entry has been
// flushed to storage (i.e., the ColumnData is no longer referenced).
// For Flight DoPut, this is guaranteed because the flush happens synchronously
// within WriteArrowRecord's call to writeTypedColumnarInternal.
func arrowArrayToColumnData(arr arrow.Array) ColumnData {
	switch arr := arr.(type) {
	case *array.Int64:
		vals := arr.Int64Values()
		// arr.Int64Values() returns the backing []int64 — zero-copy
		if len(vals) == 0 {
			return ColumnData{}
		}
		cd := ColumnData{Data: vals}
		if arr.NullN() > 0 {
			cd.Validity = extractValidityArr(arr)
		}
		return cd

	case *array.Float64:
		vals := arr.Float64Values()
		if len(vals) == 0 {
			return ColumnData{}
		}
		cd := ColumnData{Data: vals}
		if arr.NullN() > 0 {
			cd.Validity = extractValidityArr(arr)
		}
		return cd

	case *array.String:
		n := arr.Len()
		if n == 0 {
			return ColumnData{}
		}
		vals := make([]string, n)
		for j := 0; j < n; j++ {
			if arr.IsNull(j) {
				continue
			}
			vals[j] = arr.Value(j)
		}
		cd := ColumnData{Data: vals}
		if arr.NullN() > 0 {
			cd.Validity = extractValidityArr(arr)
		}
		return cd

	case *array.Boolean:
		n := arr.Len()
		if n == 0 {
			return ColumnData{}
		}
		vals := make([]bool, n)
		for j := 0; j < n; j++ {
			if arr.IsNull(j) {
				continue
			}
			vals[j] = arr.Value(j)
		}
		cd := ColumnData{Data: vals}
		if arr.NullN() > 0 {
			cd.Validity = extractValidityArr(arr)
		}
		return cd

	case *array.Timestamp:
		n := arr.Len()
		if n == 0 {
			return ColumnData{}
		}
		vals := make([]int64, n)
		for j := 0; j < n; j++ {
			if arr.IsNull(j) {
				continue
			}
			vals[j] = int64(arr.Value(j))
		}
		cd := ColumnData{Data: vals}
		if arr.NullN() > 0 {
			cd.Validity = extractValidityArr(arr)
		}
		return cd

	case *array.Int32:
		n := arr.Len()
		if n == 0 {
			return ColumnData{}
		}
		vals := make([]int64, n)
		for j := 0; j < n; j++ {
			if arr.IsNull(j) {
				continue
			}
			vals[j] = int64(arr.Value(j))
		}
		cd := ColumnData{Data: vals}
		if arr.NullN() > 0 {
			cd.Validity = extractValidityArr(arr)
		}
		return cd

	case *array.Float32:
		n := arr.Len()
		if n == 0 {
			return ColumnData{}
		}
		vals := make([]float64, n)
		for j := 0; j < n; j++ {
			if arr.IsNull(j) {
				continue
			}
			vals[j] = float64(arr.Value(j))
		}
		cd := ColumnData{Data: vals}
		if arr.NullN() > 0 {
			cd.Validity = extractValidityArr(arr)
		}
		return cd

	default:
		// Unknown type — skip
		return ColumnData{}
	}
}

// extractValidityArr builds a []bool null bitmap from an arrow.Array's nulls.
func extractValidityArr(arr arrow.Array) []bool {
	n := arr.Len()
	valid := make([]bool, n)
	for i := 0; i < n; i++ {
		valid[i] = arr.IsValid(i)
	}
	return valid
}

// walDropLogIntervalNano is the minimum interval between successive
// WAL-dropped Warn log emissions. Backpressure on a busy node can
// produce hundreds of dropped entries per second; one Warn per drop
// is log-spam. Operators get the rate from the totalWALDropped
// counter; the log line just signals "the degraded state is in
// effect, look at the counter."
const walDropLogIntervalNano = int64(time.Second)

// recordWALError classifies the error from a WAL append: a backpressure
// drop (wal.ErrWALDropped) is operationally distinct from a real I/O
// failure. Backpressure increments the dedicated totalWALDropped
// counter and emits a sampled Warn; other errors hit totalWALErrors
// and an unsampled Error log. The caller must NOT propagate the
// error — both paths leave the data buffered and the caller continues.
//
// fields is a small closure that adds context (db/measurement/size)
// to whichever logger ends up firing — keeps the call sites clean.
func (b *ArrowBuffer) recordWALError(err error, fields func(*zerolog.Event)) {
	if errors.Is(err, wal.ErrWALDropped) {
		b.totalWALDropped.Add(1)
		// Sampled Warn — at most one log line per walDropLogIntervalNano.
		now := time.Now().UnixNano()
		last := b.walDropLastLogNano.Load()
		if now-last >= walDropLogIntervalNano && b.walDropLastLogNano.CompareAndSwap(last, now) {
			ev := b.logger.Warn().Err(err).
				Int64("total_dropped", b.totalWALDropped.Load())
			if fields != nil {
				fields(ev)
			}
			ev.Msg("WAL backpressure: entries dropped on full async channel; data buffered in memory and will flush, but follower-side durability is degraded until backpressure clears")
		}
		return
	}
	b.totalWALErrors.Add(1)
	ev := b.logger.Error().Err(err)
	if fields != nil {
		fields(ev)
	}
	ev.Msg("WAL write failed - data may be lost on crash")
}

// flushSendOutcome tells callers whether tryEnqueueFlush actually
// queued the task. Callers don't need to do anything different on
// queued vs dropped today (data is in WAL either way), but having a
// distinct outcome makes the audit trail and metrics readable.
type flushSendOutcome int

const (
	flushQueued      flushSendOutcome = iota // task accepted on flushQueue
	flushSkipClosing                         // buffer is closing — short-circuit
	flushCtxCanceled                         // ctx fired during the select (defense-in-depth vs the closing flag)
	flushQueueFull                           // queue at capacity, drop relying on WAL replay
)

// tryEnqueueFlush is the shared non-blocking send into b.flushQueue
// used by both writeColumnarInternal and writeTypedColumnarInternal.
// It encapsulates:
//  1. The closing-flag short-circuit (Close() set the flag; data
//     stays in WAL, no panic from a closed channel).
//  2. The ctx.Done() defense-in-depth select arm (covers the narrow
//     window between flag-load and select-eval where Close()'s
//     cancel could fire).
//  3. The queue-full default arm (queue at capacity; data stays in
//     WAL for recovery).
//
// The caller MUST already have built `task` and called flushCancel
// to register the timeout context — tryEnqueueFlush does not own
// that lifecycle. flushCancel is invoked here on every non-queued
// outcome so the ctx is cleaned up promptly.
//
// Returns the outcome so the caller can pass it to metrics/logging
// uniformly. All non-queued outcomes increment the same
// IncWALRecordsPreserved counter to keep the operator-visible
// "records that fell back to WAL" rate authoritative.
func (b *ArrowBuffer) tryEnqueueFlush(
	task flushTask,
	flushCancel context.CancelFunc,
	bufferKey string,
	totalBuffered int,
) flushSendOutcome {
	if b.closing.Load() {
		flushCancel()
		b.logger.Warn().
			Str("buffer_key", bufferKey).
			Int("records", totalBuffered).
			Msg("Flush queue send skipped: buffer is closing (data preserved in WAL)")
		metrics.Get().IncWALRecordsPreserved(int64(totalBuffered))
		return flushSkipClosing
	}
	select {
	case b.flushQueue <- task:
		b.queueDepth.Add(1)
		b.logger.Info().
			Str("buffer_key", bufferKey).
			Int("total_records", totalBuffered).
			Int64("queue_depth", b.queueDepth.Load()).
			Msg("Buffer size exceeded, queued flush to worker pool")
		return flushQueued
	case <-b.ctx.Done():
		flushCancel()
		b.logger.Warn().
			Str("buffer_key", bufferKey).
			Int("records", totalBuffered).
			Msg("Flush queue send aborted: ArrowBuffer ctx canceled (data preserved in WAL)")
		metrics.Get().IncWALRecordsPreserved(int64(totalBuffered))
		return flushCtxCanceled
	default:
		flushCancel()
		b.logger.Warn().
			Str("buffer_key", bufferKey).
			Int("records", totalBuffered).
			Int64("queue_depth", b.queueDepth.Load()).
			Msg("Flush queue full - data preserved in WAL for recovery")
		b.totalErrors.Add(1)
		metrics.Get().IncWALRecordsPreserved(int64(totalBuffered))
		return flushQueueFull
	}
}

// writeColumnar writes a columnar record to the buffer
func (b *ArrowBuffer) writeColumnar(ctx context.Context, database string, record *models.ColumnarRecord) error {
	return b.writeColumnarInternal(ctx, database, record, false)
}

// computeColumnSignature infers the column signature for a raw map[string][]interface{}
// by checking the Go type of the first non-nil value in each column. This avoids
// allocating typed arrays just to compute the buffer key. The result matches
// getColumnSignature called on already-typed data.
func computeColumnSignature(columns map[string][]interface{}) string {
	type colEntry struct{ name, typ string }
	entries := make([]colEntry, 0, len(columns))
	size := -1
	for name, col := range columns {
		if len(name) == 0 || name[0] == '_' {
			continue
		}
		if len(col) == 0 {
			continue
		}
		firstVal := firstNonNil(col)
		if firstVal == nil {
			continue
		}
		var typ string
		switch firstVal.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			typ = "i64"
		case float32, float64:
			typ = "f64"
		case string:
			typ = "str"
		case bool:
			typ = "bool"
		default:
			typ = "unk"
		}
		entries = append(entries, colEntry{name, typ})
		size += 1 + len(name) + 1 + len(typ)
	}
	if len(entries) == 0 {
		return ""
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })
	var sb strings.Builder
	sb.Grow(size)
	for i, e := range entries {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(e.name)
		sb.WriteByte(':')
		sb.WriteString(e.typ)
	}
	return sb.String()
}

// convertAndAppendToEntry converts []interface{} columns to typed slices and
// appends them directly into entry's growing data/validity maps. Single-pass:
// conversion and append happen in one loop, eliminating the intermediate
// TypedColumnBatch allocation that existed in convertColumnsToTyped+appendEntryToEntry.
func (b *ArrowBuffer) convertAndAppendToEntry(entry *bufferEntry, measurement string, columns map[string][]interface{}) (int, error) {
	if entry.columns == nil {
		entry.columns = make(map[string]ColumnData)
	}
	decimalCols := b.getDecimalColumns(measurement)
	var numRecords int

	for name, col := range columns {
		if len(col) == 0 {
			continue
		}
		if numRecords == 0 {
			numRecords = len(col)
		}

		// Decimal column — special path
		if decimalCols != nil {
			if spec, isDecimal := decimalCols[name]; isDecimal {
				arr, valid, err := convertToDecimal128Slice(col, spec.Precision, spec.Scale)
				if err != nil {
					return 0, fmt.Errorf("decimal conversion error in column '%s': %w", name, err)
				}
				entry.columns[name] = colAppend(entry.columns[name], ColumnData{Data: arr, Validity: valid})
				continue
			}
		}

		firstVal := firstNonNil(col)
		if firstVal == nil {
			continue
		}

		switch firstVal.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			if arr, ok := b.tryInt64ZeroCopy(col); ok {
				entry.columns[name] = colAppend(entry.columns[name], ColumnData{Data: arr})
				continue
			}
			arr := make([]int64, len(col))
			valid := make([]bool, len(col))
			colHasNils := false
			for i, v := range col {
				if v == nil {
					colHasNils = true
					continue
				}
				valid[i] = true
				val, ok := toInt64(v)
				if !ok {
					return 0, fmt.Errorf("cannot convert %T to int64 in column '%s'", v, name)
				}
				arr[i] = val
			}
			if colHasNils {
				entry.columns[name] = colAppend(entry.columns[name], ColumnData{Data: arr, Validity: valid})
			} else {
				entry.columns[name] = colAppend(entry.columns[name], ColumnData{Data: arr})
			}

		case float32, float64:
			if arr, ok := b.tryFloat64ZeroCopy(col); ok {
				entry.columns[name] = colAppend(entry.columns[name], ColumnData{Data: arr})
				continue
			}
			arr := make([]float64, len(col))
			valid := make([]bool, len(col))
			colHasNils := false
			for i, v := range col {
				if v == nil {
					colHasNils = true
					continue
				}
				valid[i] = true
				val, ok := toFloat64(v)
				if !ok {
					return 0, fmt.Errorf("cannot convert %T to float64 in column '%s'", v, name)
				}
				arr[i] = val
			}
			if colHasNils {
				entry.columns[name] = colAppend(entry.columns[name], ColumnData{Data: arr, Validity: valid})
			} else {
				entry.columns[name] = colAppend(entry.columns[name], ColumnData{Data: arr})
			}

		case string:
			if arr, ok := b.tryStringZeroCopy(col); ok {
				entry.columns[name] = colAppend(entry.columns[name], ColumnData{Data: arr})
				continue
			}
			arr := make([]string, len(col))
			valid := make([]bool, len(col))
			colHasNils := false
			for i, v := range col {
				if v == nil {
					colHasNils = true
					continue
				}
				valid[i] = true
				str, ok := v.(string)
				if !ok {
					return 0, fmt.Errorf("unexpected type in string column '%s': %T", name, v)
				}
				arr[i] = str
			}
			if colHasNils {
				entry.columns[name] = colAppend(entry.columns[name], ColumnData{Data: arr, Validity: valid})
			} else {
				entry.columns[name] = colAppend(entry.columns[name], ColumnData{Data: arr})
			}

		case bool:
			if arr, ok := b.tryBoolZeroCopy(col); ok {
				entry.columns[name] = colAppend(entry.columns[name], ColumnData{Data: arr})
				continue
			}
			arr := make([]bool, len(col))
			valid := make([]bool, len(col))
			colHasNils := false
			for i, v := range col {
				if v == nil {
					colHasNils = true
					continue
				}
				valid[i] = true
				bval, ok := v.(bool)
				if !ok {
					return 0, fmt.Errorf("unexpected type in bool column '%s': %T", name, v)
				}
				arr[i] = bval
			}
			if colHasNils {
				entry.columns[name] = colAppend(entry.columns[name], ColumnData{Data: arr, Validity: valid})
			} else {
				entry.columns[name] = colAppend(entry.columns[name], ColumnData{Data: arr})
			}

		default:
			return 0, fmt.Errorf("unsupported column type for '%s': %T", name, firstVal)
		}
	}

	entry.recordCount += numRecords
	entry.estimatedBytes += estimateBytesFromData(entry.columns, numRecords)
	return numRecords, nil
}
func (b *ArrowBuffer) writeColumnarInternal(ctx context.Context, database string, record *models.ColumnarRecord, skipWAL bool) error {
	// WAL: Write to WAL before buffering (if enabled)
	// Skip WAL during recovery to avoid re-writing recovered data
	if b.wal != nil && !skipWAL {
		// ZERO-COPY PATH: Use raw msgpack bytes if available (avoids re-serialization)
		if len(record.RawPayload) > 0 {
			if err := b.wal.AppendRawWithMeta(database, record.RawPayload); err != nil {
				b.recordWALError(err, func(ev *zerolog.Event) {
					ev.Str("database", database).
						Str("measurement", record.Measurement).
						Int("payload_size", len(record.RawPayload))
				})
			}
		} else {
			walRecords := b.columnarToWALRecords(database, record)
			if len(walRecords) > 0 {
				if err := b.wal.Append(walRecords); err != nil {
					b.recordWALError(err, func(ev *zerolog.Event) {
						ev.Str("database", database).
							Str("measurement", record.Measurement).
							Int("records", len(walRecords))
					})
				}
			}
		}
	}

	// Lock-outside: compute column signature for buffer key
	newSignature := computeColumnSignature(record.Columns)

	// Construct schema-specific buffer key
	bufferKey := schemaKey(database, record.Measurement, newSignature)

	// Flush type conflicts
	baseKey := database + "/" + record.Measurement
	b.flushTypeConflicts(baseKey, newSignature)

	shard := b.getShard(bufferKey)

	var columnsForFlush map[string]ColumnData
	var tagColumnsForFlush []string
	var shouldFlush bool

	shard.mu.Lock()

	entry, exists := shard.buffers[bufferKey]
	if !exists {
		entry = &bufferEntry{
			columns:   make(map[string]ColumnData),
			startTime: time.Now().UTC(),
			schema:    newSignature,
		}
		shard.buffers[bufferKey] = entry
	} else if entry.isEmpty() {
		entry.startTime = time.Now().UTC()
		entry.recordCount = 0
		entry.estimatedBytes = 0
	}

	// Single-pass: convert []interface{} -> typed slices + append to entry
	numRecords, err := b.convertAndAppendToEntry(entry, record.Measurement, record.Columns)
	if err != nil {
		shard.mu.Unlock()
		return fmt.Errorf("failed to convert and append columns: %w", err)
	}

	// Propagate tag column names for Parquet metadata (enables auto-dedup in compaction)
	if len(entry.tagColumns) == 0 && len(record.TagColumns) > 0 {
		entry.tagColumns = record.TagColumns
	}

	// Infer Arrow schema on first data arrival
	if entry.arrowSchema == nil && len(entry.columns) > 0 {
		tagCols := record.TagColumns
		decCols := b.getDecimalColumns(record.Measurement)
		schema, err := b.writer.inferSchema(entry.columns, tagCols, decCols)
		if err != nil {
			shard.mu.Unlock()
			return fmt.Errorf("failed to infer Arrow schema: %w", err)
		}
		entry.arrowSchema = schema
	}

	totalBuffered := entry.recordCount

	// Flush gate
	if b.adaptiveFlush.Load() == nil && totalBuffered >= b.config.MaxBufferSize {
		columnsForFlush = entry.columns
		tagColumnsForFlush = entry.tagColumns
		entry.columns = make(map[string]ColumnData)
		entry.tagColumns = nil
		entry.recordCount = 0
		entry.estimatedBytes = 0

		flushCtx, flushCancel := context.WithTimeout(b.ctx, b.flushTimeout)
		task := flushTask{
			ctx:         flushCtx,
			cancel:      flushCancel,
			bufferKey:   bufferKey,
			database:    database,
			measurement: record.Measurement,
			entry: &bufferEntry{
				columns:     columnsForFlush,
				tagColumns:  tagColumnsForFlush,
				recordCount: totalBuffered,
				arrowSchema: entry.arrowSchema,
			},
		}
		outcome := b.tryEnqueueFlush(task, flushCancel, bufferKey, totalBuffered)

		if outcome == flushQueued {
			shouldFlush = true
		} else {
			prependFlushDataToEntry(entry, columnsForFlush, tagColumnsForFlush, totalBuffered)
		}
	}

	shard.mu.Unlock()

	b.totalRecordsBuffered.Add(int64(numRecords))

	// Hard-limit enforcement
	if err := b.ensureBufferSpace(0); err != nil {
		return err
	}

	b.logger.Debug().
		Str("buffer_key", bufferKey).
		Int("num_records", numRecords).
		Int("total_buffered", totalBuffered).
		Bool("flushing", shouldFlush).
		Msg("Added columnar data to buffer")

	return nil
}

// writeTypedColumnarInternal writes a pre-typed bufferEntry to the buffer.
// Mirrors writeColumnarInternal but skips convertColumnsToTyped since the entry
// is already typed ([]int64, []float64, []string). Used by format-specific parsers
// that know column types at compile time.
func (b *ArrowBuffer) writeTypedColumnarInternal(ctx context.Context, database, measurement string, src *bufferEntry, skipWAL bool) error {
	// WAL: Convert typed entry to row format for WAL storage
	if b.wal != nil && !skipWAL {
		walRecords := entryToWALRecords(database, measurement, src, b.getDecimalColumns(measurement))
		if len(walRecords) > 0 {
			if err := b.wal.Append(walRecords); err != nil {
				b.recordWALError(err, func(ev *zerolog.Event) {
					ev.Str("database", database).
						Str("measurement", measurement).
						Int("records", len(walRecords))
				})
			}
		}
	}

	newSignature := src.schema
	if newSignature == "" && len(src.columns) > 0 {
		newSignature = getColumnSignature(src.columns)
	}

	bufferKey := schemaKey(database, measurement, newSignature)

	baseKey := database + "/" + measurement
	b.flushTypeConflicts(baseKey, newSignature)

	shard := b.getShard(bufferKey)

	var columnsForFlush map[string]ColumnData
	var tagColumnsForFlush []string
	var shouldFlush bool

	shard.mu.Lock()

	entry, exists := shard.buffers[bufferKey]
	if !exists {
		entry = &bufferEntry{
			columns:   make(map[string]ColumnData),
			startTime: time.Now().UTC(),
			schema:    newSignature,
		}
		shard.buffers[bufferKey] = entry
	} else if entry.isEmpty() {
		entry.startTime = time.Now().UTC()
		entry.recordCount = 0
		entry.estimatedBytes = 0
	}
	if entry.arrowSchema == nil && len(src.columns) > 0 {
		tagCols := src.tagColumns
		decCols := b.getDecimalColumns(measurement)
		schema, err := b.writer.inferSchema(src.columns, tagCols, decCols)
		if err != nil {
			shard.mu.Unlock()
			return fmt.Errorf("failed to infer Arrow schema: %w", err)
		}
		entry.arrowSchema = schema
	}

	appendEntryToEntry(entry, src)

	totalBuffered := entry.recordCount
	numRecords := src.recordCount

	if b.adaptiveFlush.Load() == nil && totalBuffered >= b.config.MaxBufferSize {
		columnsForFlush = entry.columns
		tagColumnsForFlush = entry.tagColumns
		entry.columns = make(map[string]ColumnData)
		entry.tagColumns = nil
		entry.recordCount = 0
		entry.estimatedBytes = 0

		flushCtx, flushCancel := context.WithTimeout(b.ctx, b.flushTimeout)
		task := flushTask{
			ctx:         flushCtx,
			cancel:      flushCancel,
			bufferKey:   bufferKey,
			database:    database,
			measurement: measurement,
			entry: &bufferEntry{
				columns:     columnsForFlush,
				tagColumns:  tagColumnsForFlush,
				recordCount: totalBuffered,
				arrowSchema: entry.arrowSchema,
			},
		}
		outcome := b.tryEnqueueFlush(task, flushCancel, bufferKey, totalBuffered)

		if outcome == flushQueued {
			shouldFlush = true
		} else {
			prependFlushDataToEntry(entry, columnsForFlush, tagColumnsForFlush, totalBuffered)
		}
	}

	shard.mu.Unlock()

	b.totalRecordsBuffered.Add(int64(numRecords))

	if err := b.ensureBufferSpace(0); err != nil {
		return err
	}

	b.logger.Debug().
		Str("buffer_key", bufferKey).
		Int("num_records", numRecords).
		Int("total_buffered", totalBuffered).
		Bool("flushing", shouldFlush).
		Msg("Added typed columnar data to buffer")

	return nil
}

// entryToWALRecords converts a bufferEntry to row-format records for WAL storage.
// This is the WAL fallback path for typed entries (e.g., TLE) that don't have raw msgpack bytes.
func entryToWALRecords(database, measurement string, entry *bufferEntry, decimalCols map[string]config.DecimalSpec) []map[string]interface{} {
	if entry.recordCount == 0 {
		return nil
	}

	records := make([]map[string]interface{}, entry.recordCount)
	for i := 0; i < entry.recordCount; i++ {
		row := map[string]interface{}{
			"_database":    database,
			"_measurement": measurement,
		}
		for colName, colData := range entry.columns {
			val := colIthVal(colData, i)
			// WAL stores decimals as float64 (lossy but WAL is recovery-only)
			if dec, ok := val.(decimal128.Num); ok {
				s := int32(0)
				if decimalCols != nil {
					if spec, ok2 := decimalCols[colName]; ok2 {
						s = spec.Scale
					}
				}
				f := dec.ToBigFloat(s)
				val, _ = f.Float64()
			}
			row[colName] = val
		}
		records[i] = row
	}

	return records
}

// convertToDecimal128Slice converts a []interface{} column to []decimal128.Num.
// Accepts float64, float32, int64, int*, uint*, and string values.
// Returns the typed array, optional validity bitmap (nil if no nulls), and error.
func convertToDecimal128Slice(col []interface{}, precision, scale int32) ([]decimal128.Num, []bool, error) {
	arr := make([]decimal128.Num, len(col))
	var valid []bool

	for i, v := range col {
		if v == nil {
			if valid == nil {
				valid = make([]bool, len(col))
				for j := 0; j < i; j++ {
					valid[j] = true
				}
			}
			continue
		}
		if valid != nil {
			valid[i] = true
		}

		var num decimal128.Num
		var err error

		switch val := v.(type) {
		case float64:
			num, err = decimal128.FromFloat64(val, precision, scale)
		case float32:
			num, err = decimal128.FromFloat64(float64(val), precision, scale)
		case int64:
			num, err = decimal128.FromString(strconv.FormatInt(val, 10), precision, scale)
		case int:
			num, err = decimal128.FromString(strconv.FormatInt(int64(val), 10), precision, scale)
		case int32:
			num, err = decimal128.FromString(strconv.FormatInt(int64(val), 10), precision, scale)
		case int16:
			num, err = decimal128.FromString(strconv.FormatInt(int64(val), 10), precision, scale)
		case int8:
			num, err = decimal128.FromString(strconv.FormatInt(int64(val), 10), precision, scale)
		case uint64:
			num, err = decimal128.FromString(strconv.FormatUint(val, 10), precision, scale)
		case uint:
			num, err = decimal128.FromString(strconv.FormatUint(uint64(val), 10), precision, scale)
		case uint32:
			num, err = decimal128.FromString(strconv.FormatUint(uint64(val), 10), precision, scale)
		case uint16:
			num, err = decimal128.FromString(strconv.FormatUint(uint64(val), 10), precision, scale)
		case uint8:
			num, err = decimal128.FromString(strconv.FormatUint(uint64(val), 10), precision, scale)
		case string:
			num, err = decimal128.FromString(val, precision, scale)
		default:
			return nil, nil, fmt.Errorf("cannot convert %T to decimal128 at row %d", v, i)
		}

		if err != nil {
			return nil, nil, fmt.Errorf("row %d: %w", i, err)
		}
		arr[i] = num
	}

	return arr, valid, nil
}

// ZERO-COPY HELPERS: Try bulk type assertion for homogeneous arrays

// tryInt64ZeroCopy attempts zero-copy conversion for homogeneous int64 arrays.
// Single-pass: allocates and fills in one scan. Returns nil on first nil/type-mismatch,
// paying only the GC cost of discarding the partial allocation — which is rare in practice.
func (b *ArrowBuffer) tryInt64ZeroCopy(col []interface{}) ([]int64, bool) {
	arr := make([]int64, len(col))
	for i, v := range col {
		if v == nil {
			return nil, false
		}
		val, ok := v.(int64)
		if !ok {
			return nil, false
		}
		arr[i] = val
	}
	return arr, true
}

// tryFloat64ZeroCopy attempts zero-copy conversion for homogeneous float64 arrays.
// Single-pass: allocates and fills in one scan.
func (b *ArrowBuffer) tryFloat64ZeroCopy(col []interface{}) ([]float64, bool) {
	arr := make([]float64, len(col))
	for i, v := range col {
		if v == nil {
			return nil, false
		}
		val, ok := v.(float64)
		if !ok {
			return nil, false
		}
		arr[i] = val
	}
	return arr, true
}

// tryStringZeroCopy attempts zero-copy conversion for homogeneous string arrays.
// Single-pass: allocates and fills in one scan.
func (b *ArrowBuffer) tryStringZeroCopy(col []interface{}) ([]string, bool) {
	arr := make([]string, len(col))
	for i, v := range col {
		if v == nil {
			return nil, false
		}
		val, ok := v.(string)
		if !ok {
			return nil, false
		}
		arr[i] = val
	}
	return arr, true
}

// tryBoolZeroCopy attempts zero-copy conversion for homogeneous bool arrays.
// Single-pass: allocates and fills in one scan.
func (b *ArrowBuffer) tryBoolZeroCopy(col []interface{}) ([]bool, bool) {
	arr := make([]bool, len(col))
	for i, v := range col {
		if v == nil {
			return nil, false
		}
		val, ok := v.(bool)
		if !ok {
			return nil, false
		}
		arr[i] = val
	}
	return arr, true
}

// gcEmptyEntries removes buffer entries that have been empty (no batches)
// for longer than 2x maxBufferAge. These are schema-cache shells whose
// measurement hasn't been written to recently. The factor of 2 provides
// a safety margin so entries aren't collected between frequent flushes.
func (b *ArrowBuffer) gcEmptyEntries() {
	// Read maxAge from the adaptive flush engine when available so
	// SIGHUP hot reload changes take effect. Fall back to the static
	// maxBufferAge for compatibility with test setups that skip the engine.
	maxAge := b.maxBufferAge
	if e := b.adaptiveFlush.Load(); e != nil {
		maxAge = time.Duration(e.MaxAge())
	}
	threshold := maxAge * 2
	now := time.Now().UTC()
	var cleaned int

	for i := uint32(0); i < b.shardCount; i++ {
		shard := b.shards[i]
		shard.mu.Lock()
		for key, entry := range shard.buffers {
			if entry.isEmpty() && now.Sub(entry.startTime) > threshold {
				delete(shard.buffers, key)
				cleaned++
			}
		}
		shard.mu.Unlock()
	}

	if cleaned > 0 {
		b.logger.Debug().
			Int("cleaned", cleaned).
			Msg("GC cleaned empty buffer entries")
	}
}

// splitBufferKey splits "database/measurement" into [database, measurement]
func splitBufferKey(key string) []string {
	// Strip schema hash suffix if present (e.g. "db/cpu__abc123" -> "db/cpu")
	cleanKey, _ := StripSchemaHash(key)
	// Find first slash to split database/measurement
	for i, c := range cleanKey {
		if c == '/' {
			return []string{cleanKey[:i], cleanKey[i+1:]}
		}
	}
	return []string{cleanKey}
}

// flushRecordsAsync performs fire-and-forget flush in background goroutine
// OPTIMIZATION: This is launched as a goroutine and doesn't block the write path
// flushWorker processes flush tasks from the queue
// OPTIMIZATION: Bounded worker pool prevents goroutine explosion
func (b *ArrowBuffer) flushWorker(workerID int) {
	defer b.wg.Done()

	b.logger.Info().Int("worker_id", workerID).Msg("Flush worker started")

	for {
		select {
		case <-b.ctx.Done():
			b.logger.Info().Int("worker_id", workerID).Msg("Flush worker stopping")
			return
		case task, ok := <-b.flushQueue:
			if !ok {
				return
			}
			b.queueDepth.Add(-1)

			b.logger.Debug().
				Int("worker_id", workerID).
				Str("buffer_key", task.bufferKey).
				Int("records", task.entry.recordCount).
				Int64("queue_depth", b.queueDepth.Load()).
				Msg("Worker processing flush task")

			startTime := time.Now()
			database := task.database
			measurement := task.measurement

			if err := b.flushPartitionedData(task.ctx, task.bufferKey, database, measurement, task.entry, "async", startTime); err != nil {
				b.prependFlushData(task.bufferKey, task.entry.columns, task.entry.tagColumns, task.entry.recordCount, task.entry.arrowSchema)
				b.logger.Error().
					Err(err).
					Str("buffer_key", task.bufferKey).
					Int("records", task.entry.recordCount).
					Msg("Flush failed — data restored to buffer, will retry")

				if b.wal != nil {
					if cerr := b.wal.AppendControl(wal.FlushFail, database, measurement); cerr != nil {
						b.logger.Error().Err(cerr).Msg("Failed to write FLUSH_FAIL control record")
					}
				}
				task.cancel()
				continue
			}

			if b.wal != nil {
				if cerr := b.wal.AppendControl(wal.FlushOK, database, measurement); cerr != nil {
					b.logger.Error().Err(cerr).Msg("Failed to write FLUSH_OK control record")
				}
			}

			trigger := task.trigger
			if trigger == "" {
				trigger = "size"
			}
			metrics.Get().RecordBufferFlushRecords(trigger, task.entry.recordCount)

			task.cancel()
		}
	}
}

// SetBufferHardLimit sets the buffer overflow hard limit.
// When total estimated buffer bytes exceeds this limit, oldest entries are evicted
// (Layer 1), and if still over, new writes are rejected with 503 (Layer 2).
// A value of 0 disables overflow protection.
func (b *ArrowBuffer) SetBufferHardLimit(limitBytes uint64) {
	b.hardLimit.Store(limitBytes)
}

// estimateTotalBufferBytes scans all shards and returns the total estimated bytes.
func (b *ArrowBuffer) estimateTotalBufferBytes() uint64 {
	var total uint64
	for i := uint32(0); i < b.shardCount; i++ {
		shard := b.shards[i]
		shard.mu.RLock()
		for _, entry := range shard.buffers {
			total += entry.estimatedBytes
		}
		shard.mu.RUnlock()
	}
	return total
}

// AllBufferKeys returns all buffer keys across all shards, excluding empty entries.
// Used by diagnostic endpoints and query-time VIEW construction to discover which
// measurements have buffered in-memory data not yet flushed to Parquet storage.
func (b *ArrowBuffer) AllBufferKeys() []string {
	var keys []string
	for i := uint32(0); i < b.shardCount; i++ {
		shard := b.shards[i]
		shard.mu.RLock()
		for key, entry := range shard.buffers {
			if entry.isEmpty() {
				continue
			}
			keys = append(keys, key)
		}
		shard.mu.RUnlock()
	}
	return keys
}

// SnapshotEntry creates a zero-copy snapshot of a buffer entry for the given bufferKey.
// Only copies slice headers (Data and Validity are shallow copies of the underlying
// Go slices), not the underlying arrays. Go's append semantics guarantee that existing
// elements are never modified, making this safe for concurrent use.
//
// Returns nil if the entry is not found or is empty.
//
// The snapshot is extracted under a shard read lock, so it is consistent at the
// moment of acquisition. The caller should use the snapshot promptly; the data it
// references remains valid because Go's GC never moves slice backing arrays and
// colAppend never modifies existing elements.
func (b *ArrowBuffer) SnapshotEntry(bufferKey string) *Entry {
	shard := b.getShard(bufferKey)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	entry, ok := shard.buffers[bufferKey]
	if !ok || entry.isEmpty() {
		return nil
	}

	// Shallow copy of the columns map — each ColumnData retains its slice header
	// (Data any, Validity []bool) pointing to the same backing arrays.
	cols := make(map[string]ColumnData, len(entry.columns))
	for name, col := range entry.columns {
		cols[name] = col
	}

	// Copy tagColumns slice header (same backing array, which is never modified after creation)
	tagCols := entry.tagColumns

	return &Entry{
		columns:     cols,
		tagColumns:  tagCols,
		schema:      entry.schema,
		recordCount: entry.recordCount,
		arrowSchema: entry.arrowSchema,
	}
}

// MeasurementBufferKeys returns all buffer keys whose base key (after stripping
// the schema hash) matches "database/measurement". This enables query-time lookup
// of all schema variants for a measurement.
func (b *ArrowBuffer) MeasurementBufferKeys(database, measurement string) []string {
	baseKey := database + "/" + measurement
	var keys []string
	for i := uint32(0); i < b.shardCount; i++ {
		shard := b.shards[i]
		shard.mu.RLock()
		for key := range shard.buffers {
			if stripped, _ := StripSchemaHash(key); stripped == baseKey {
				entry := shard.buffers[key]
				if !entry.isEmpty() {
					keys = append(keys, key)
				}
			}
		}
		shard.mu.RUnlock()
	}
	return keys
}

// evictOldestEntries evicts the oldest buffer entries until total estimated bytes
// is below targetBytes. Returns true if any entries were evicted.
// When inlineFlushCount is below maxInlineFlushes, evicted entries are flushed
// inline to Parquet instead of being silently deleted. Beyond the cap, entries
// are deleted without flush (data preserved only in WAL).
func (b *ArrowBuffer) evictOldestEntries(targetBytes uint64, inlineFlushCount *int) bool {
	const maxInlineFlushes = 5
	evicted := false
	now := time.Now()

	// Two-pass: first find the globally oldest entry across all shards,
	// then evict it. Repeat until under targetBytes or nothing left.
	for {
		// Pass 1: scan all shards for the globally oldest entry.
		var oldestShardIdx uint32
		var oldestKey string
		var oldestTime time.Time
		for i := uint32(0); i < b.shardCount; i++ {
			shard := b.shards[i]
			shard.mu.RLock()
			for key, entry := range shard.buffers {
				if entry.isEmpty() {
					continue
				}
				if oldestKey == "" || entry.startTime.Before(oldestTime) {
					oldestShardIdx = i
					oldestKey = key
					oldestTime = entry.startTime
				}
			}
			shard.mu.RUnlock()
		}
		if oldestKey == "" {
			return evicted // nothing left to evict
		}

		// Pass 2: lock only the shard containing the oldest entry and evict it.
		shard := b.shards[oldestShardIdx]
		shard.mu.Lock()
		entry, exists := shard.buffers[oldestKey]
		if !exists {
			// Entry was concurrently flushed/evicted — restart from pass 1.
			shard.mu.Unlock()
			if b.estimateTotalBufferBytes() <= targetBytes {
				return evicted
			}
			continue
		}
		columns := entry.columns
		tagCols := entry.tagColumns
		recordCount := entry.recordCount
		arrowSchema := entry.arrowSchema
		delete(shard.buffers, oldestKey)
		parts := splitBufferKey(oldestKey)
		shard.mu.Unlock()

		evicted = true
		if *inlineFlushCount < maxInlineFlushes && len(parts) == 2 {
			*inlineFlushCount++
			b.logger.Warn().
				Str("buffer_key", oldestKey).
				Dur("age", now.Sub(oldestTime)).
				Int("inline_flush", *inlineFlushCount).
				Msg("Buffer overflow: flushing oldest entry inline")

			merged := &bufferEntry{
				columns:     columns,
				tagColumns:  tagCols,
				schema:      entry.schema,
				recordCount: recordCount,
				arrowSchema: arrowSchema,
			}
			startTime := time.Now()
			if err := b.flushPartitionedData(context.Background(), oldestKey, parts[0], parts[1], merged, "hard_limit", startTime); err != nil {
				b.logger.Error().
					Str("buffer_key", oldestKey).
					Int("records", merged.recordCount).
					Msg("Buffer overflow: inline flush failed — restoring data to buffer")
				// Restore data to buffer for retry. prependFlushData recreates
				// the entry with fresh startTime so it won't be re-selected
				// as oldest immediately on the next eviction iteration.
				b.prependFlushData(oldestKey, columns, tagCols, recordCount, arrowSchema)
			}
		} else {
			if *inlineFlushCount >= maxInlineFlushes {
				b.logger.Error().
					Str("buffer_key", oldestKey).
					Int("max_inline_flushes", maxInlineFlushes).
					Msg("Buffer overflow: inline flush cap reached, evicting without flush — data in WAL only")
			}
			b.logger.Warn().
				Str("buffer_key", oldestKey).
				Dur("age", now.Sub(oldestTime)).
				Msg("Buffer overflow: evicted oldest entry (data preserved in WAL)")
			metrics.Get().IncBufferFlushFailures() // tracks overflow evictions
		}

		if b.estimateTotalBufferBytes() <= targetBytes {
			return true
		}
	}
}

// ensureBufferSpace checks the buffer hard limit and applies overflow protection.
// Returns nil if there is space (possibly after eviction), or error if the hard
// limit is exceeded even after eviction.
func (b *ArrowBuffer) ensureBufferSpace(newEntryBytes uint64) error {
	hardLimit := b.hardLimit.Load()
	if hardLimit == 0 {
		return nil // no limit configured
	}

	total := b.estimateTotalBufferBytes()
	if total+newEntryBytes <= hardLimit {
		return nil
	}

	// Layer 1: evict oldest entries until below 80% hard limit.
	// inlineFlushCount caps the number of inline flushes to bound latency;
	// beyond the cap, entries are evicted without flush (WAL-only).
	inlineFlushCount := 0
	target := hardLimit * 80 / 100
	for i := 0; i < 100 && b.estimateTotalBufferBytes() > target; i++ {
		if !b.evictOldestEntries(target, &inlineFlushCount) {
			break // nothing left to evict
		}
	}

	// Layer 2: if still over hard limit, reject with backpressure
	hardLimit = b.hardLimit.Load()
	total = b.estimateTotalBufferBytes()
	if total+newEntryBytes > hardLimit {
		return fmt.Errorf("buffer hard limit exceeded: %d bytes (current %d + new %d), limit %d",
			total+newEntryBytes, total, newEntryBytes, hardLimit)
	}

	return nil
}

// restoreBufferEntry rebuilds a bufferEntry from batch references when mergeBatches fails.
// The entry was deleted by flushCandidate before the task was created; this puts it back.

// flushPartitionedData is the shared core logic for partitioning and writing data by hour boundaries
// Called by both async (flushWithDataTimePartitioning) and sync (flushBufferLockedDataTime) paths
// Uses hash-based grouping to partition by hour, then sorts each hour independently
func (b *ArrowBuffer) flushPartitionedData(ctx context.Context, bufferKey, database, measurement string, merged *bufferEntry, flushType string, startTime time.Time) error {
	// Get sort keys for this measurement (guaranteed to include "time")
	sortKeys := b.getSortKeys(measurement)

	// Get decimal column config for this measurement (nil if none configured)
	decimalCols := b.getDecimalColumns(measurement)

	// Extract time column (doesn't need to be sorted yet)
	times, ok := merged.columns["time"].Data.([]int64)
	if !ok || len(times) == 0 {
		return fmt.Errorf("no time data in batch")
	}

	// OPTIMIZATION: Group by hour in a single O(n) pass
	// This gives us: hour buckets, global min/max, per-hour min/max
	hourBuckets, globalMin, globalMax, err := groupByHour(times)
	if err != nil {
		return fmt.Errorf("failed to group by hour: %w", err)
	}

	minTime := time.UnixMicro(globalMin).UTC()
	maxTime := time.UnixMicro(globalMax).UTC()

	// Log warning if data is significantly old or in the future
	now := time.Now().UTC()
	if minTime.Before(now.AddDate(0, 0, -7)) {
		b.logger.Warn().
			Time("data_time", minTime).
			Str("buffer_key", bufferKey).
			Msg("Data timestamp is >7 days old - possible backfill or clock skew")
	} else if minTime.After(now.Add(time.Hour)) {
		b.logger.Warn().
			Time("data_time", minTime).
			Str("buffer_key", bufferKey).
			Msg("Data timestamp is >1 hour in future - possible clock skew")
	}

	// OPTIMIZATION: If batch fits within single hour, skip splitting
	// Check if min and max fall within the same hour
	minHour := minTime.Truncate(time.Hour)
	maxHour := maxTime.Truncate(time.Hour)
	if minHour.Equal(maxHour) {
		// Single hour - sort once and write one file
		sorted := sortEntryByKeys(merged, sortKeys)

		parquetData, err := b.writer.WriteParquetColumnar(ctx, measurement, sorted, decimalCols, merged.arrowSchema)
		if err != nil {
			return fmt.Errorf("failed to write Parquet: %w", err)
		}

		storagePath := b.generateStoragePath(database, measurement, minTime)

		// Compute SHA-256 of the Parquet buffer before the backend write so the
		// hash lands in the cluster manifest with the same commit that announces
		// the file. Peers use this to verify bytes pulled from the origin.
		// The buffer is already in memory; this is an O(n) scan of ~MB of data.
		parquetSum := sha256.Sum256(parquetData)
		parquetSumHex := hex.EncodeToString(parquetSum[:])
		if err := b.storage.Write(ctx, storagePath, parquetData); err != nil {
			return fmt.Errorf("failed to write to storage: %w", err)
		}

		// Register file in tiering metadata for query routing
		b.registerFileInTiering(ctx, database, measurement, storagePath, minTime, int64(len(parquetData)), parquetSumHex)

		b.totalRecordsWritten.Add(int64(merged.recordCount))
		b.totalFlushes.Add(1)

		flushDuration := time.Since(startTime)
		msgType := getFlushMessageType(flushType)

		b.logger.Info().
			Str("buffer_key", bufferKey).
			Str("storage_path", storagePath).
			Int("records", merged.recordCount).
			Int("size_bytes", len(parquetData)).
			Dur("flush_duration", flushDuration).
			Strs("sort_keys", sortKeys).
			Msgf("%s completed (single hour, data_time)", msgType)

		return nil
	}

	// Multiple hours - process each hour bucket independently
	b.logger.Info().
		Str("buffer_key", bufferKey).
		Int("num_hours", len(hourBuckets)).
		Int("total_records", merged.recordCount).
		Msg("Splitting batch across multiple hour partitions")

	// Collect registration entries after all storage writes succeed.
	// registerFileInTiering is called only after every hour's storage.Write succeeds
	// to avoid leaving stale manifest entries when a later hour fails — WAL replay
	// would otherwise create a duplicate file for the already-registered hour.
	type tieringEntry struct {
		storagePath string
		bucketTime  time.Time
		sizeBytes   int64
		sha256Hex   string
		records     int
	}
	written := make([]tieringEntry, 0, len(hourBuckets))
	for hourID, bucket := range hourBuckets {
		// Save count before clearing indices
		splitRecordCount := len(bucket.indices)

		// Extract rows for this hour using the index list
		hourBatch := sliceEntryByIndices(merged, bucket.indices)

		// Sort this hour's data by configured sort keys
		sorted := sortEntryByKeys(hourBatch, sortKeys)

		// Write Parquet file for this hour
		parquetData, err := b.writer.WriteParquetColumnar(ctx, measurement, sorted, decimalCols, merged.arrowSchema)
		if err != nil {
			return fmt.Errorf("failed to write Parquet for hour %d: %w", hourID, err)
		}

		// Use bucket's minTime for path generation (convert hourID to time only here)
		bucketTime := hourIDToTime(hourID)
		storagePath := b.generateStoragePath(database, measurement, bucketTime)

		// Compute SHA-256 of the Parquet buffer for peer replication checksum.
		// See the single-hour branch above for rationale.
		parquetSum := sha256.Sum256(parquetData)
		parquetSumHex := hex.EncodeToString(parquetSum[:])
		if err := b.storage.Write(ctx, storagePath, parquetData); err != nil {
			return fmt.Errorf("failed to write to storage for hour %d: %w", hourID, err)
		}

		written = append(written, tieringEntry{
			storagePath: storagePath,
			bucketTime:  bucketTime,
			sizeBytes:   int64(len(parquetData)),
			sha256Hex:   parquetSumHex,
			records:     splitRecordCount,
		})

		b.logger.Info().
			Str("buffer_key", bufferKey).
			Str("storage_path", storagePath).
			Int64("hour_id", hourID).
			Int("records", splitRecordCount).
			Int("size_bytes", len(parquetData)).
			Msg("Wrote hour partition")
	}

	// All hours written successfully — now register in tiering and cluster manifest.
	totalWritten := 0
	for _, e := range written {
		b.registerFileInTiering(ctx, database, measurement, e.storagePath, e.bucketTime, e.sizeBytes, e.sha256Hex)
		totalWritten += e.records
	}
	b.totalRecordsWritten.Add(int64(totalWritten))
	b.totalFlushes.Add(int64(len(hourBuckets)))

	flushDuration := time.Since(startTime)
	msgType := getFlushMessageType(flushType)

	b.logger.Info().
		Str("buffer_key", bufferKey).
		Int("num_files", len(hourBuckets)).
		Int("total_records", totalWritten).
		Dur("flush_duration", flushDuration).
		Msgf("%s completed (multi-hour split, data_time)", msgType)

	return nil
}

// flushBufferLocked writes buffered data to Parquet and storage (synchronous version for periodic flush)
// Note: Caller must hold shard.mu lock
func (b *ArrowBuffer) flushBufferLocked(ctx context.Context, shard *bufferShard, bufferKey, database, measurement, trigger string) error {
	entry, exists := shard.buffers[bufferKey]
	if !exists || entry.isEmpty() {
		return nil
	}

	recordCount := entry.recordCount

	// Move data out of entry
	columns := entry.columns
	tagCols := entry.tagColumns
	entry.columns = make(map[string]ColumnData)
	entry.tagColumns = nil
	entry.recordCount = 0
	entry.estimatedBytes = 0

	// Release lock before expensive operations
	shard.mu.Unlock()

	merged := &bufferEntry{
		columns:     columns,
		tagColumns:  tagCols,
		schema:      entry.schema,
		recordCount: recordCount,
	}

	startTime := time.Now().UTC()
	if err := b.flushPartitionedData(ctx, bufferKey, database, measurement, merged, flushTypeSync, startTime); err != nil {
		b.prependFlushData(bufferKey, columns, tagCols, recordCount, entry.arrowSchema)
		b.logger.Warn().
			Err(err).
			Str("buffer_key", bufferKey).
			Int("records", merged.recordCount).
			Msg("Flush failed — data restored to buffer, will retry")

		if b.wal != nil {
			if cerr := b.wal.AppendControl(wal.FlushFail, database, measurement); cerr != nil {
				b.logger.Error().Err(cerr).Msg("Failed to write FLUSH_FAIL control record")
			}
		}
		shard.mu.Lock()
		return err
	}

	if b.wal != nil {
		if cerr := b.wal.AppendControl(wal.FlushOK, database, measurement); cerr != nil {
			b.logger.Error().Err(cerr).Msg("Failed to write FLUSH_OK control record")
		}
	}

	metrics.Get().RecordBufferFlushRecords(trigger, recordCount)

	shard.mu.Lock()
	return nil
}

// prependFlushData prepends flushed data back into the buffer entry after a flush failure.
// Old data (the failed flush payload) is placed BEFORE new data that may have arrived
// concurrently during the flush attempt, preserving arrival-time order.
func (b *ArrowBuffer) prependFlushData(bufferKey string, columns map[string]ColumnData, tagColumns []string, flushedRows int, arrowSchema *arrow.Schema) {
	shard := b.getShard(bufferKey)
	shard.mu.Lock()
	entry, exists := shard.buffers[bufferKey]
	if !exists {
		// Entry was deleted by eviction — recreate it.
		// Compute schema from data so type-conflict detection works.
		schema := getColumnSignature(columns)
		entry = &bufferEntry{
			columns:        columns,
			tagColumns:     tagColumns,
			schema:         schema,
			startTime:      time.Now().UTC(),
			recordCount:    flushedRows,
			estimatedBytes: estimateBytesFromData(columns, flushedRows),
			arrowSchema:    arrowSchema,
		}
		shard.buffers[bufferKey] = entry
		shard.mu.Unlock()
		return
	}
	prependFlushDataToEntry(entry, columns, tagColumns, flushedRows)
	shard.mu.Unlock()
}

// prependFlushDataToEntry prepends flush data into an existing entry.
// Caller must hold shard.mu.
func prependFlushDataToEntry(entry *bufferEntry, columns map[string]ColumnData, tagColumns []string, flushedRows int) {
	// Prepend column data: old data first, new data second
	for name, col := range columns {
		if existing, ok := entry.columns[name]; ok {
			entry.columns[name] = colAppend(col, existing)
		} else {
			entry.columns[name] = col
		}
	}

	// Preserve tag columns
	if len(entry.tagColumns) == 0 && len(tagColumns) > 0 {
		entry.tagColumns = make([]string, len(tagColumns))
		copy(entry.tagColumns, tagColumns)
	}

	entry.recordCount += flushedRows
	entry.estimatedBytes += estimateBytesFromData(columns, flushedRows)
}

// sortColumnsByTime sorts all columns by the time column in-place
// Returns the sorted columns and any error encountered
func sortColumnsByTime(columns map[string]ColumnData) (map[string]ColumnData, error) {
	// Delegate to multi-key sort with just "time" key
	return sortColumnsByKeys(columns, []string{"time"})
}

// sortColumnsByKeys sorts columns by multiple keys (e.g., sensor_id, then time)
// Returns the sorted columns and any error encountered
func sortColumnsByKeys(columns map[string]ColumnData, sortKeys []string) (map[string]ColumnData, error) {
	sorted, indices, err := sortColumnsByKeysWithPermutation(columns, sortKeys)
	putSortIndices(indices)
	return sorted, err
}

// sortColumnsByKeysWithPermutation sorts columns and returns the permutation indices used.
// The permutation can be applied to validity bitmaps or other parallel arrays by the caller,
// avoiding a second sort pass.
func sortColumnsByKeysWithPermutation(columns map[string]ColumnData, sortKeys []string) (map[string]ColumnData, []int, error) {
	if len(sortKeys) == 0 {
		return nil, nil, fmt.Errorf("no sort keys provided")
	}

	// FAST PATH: Time-only sort (most common case) - avoid multi-key overhead
	if len(sortKeys) == 1 && sortKeys[0] == "time" {
		sorted, indices, err := sortColumnsByTimeOnlyWithPermutation(columns)
		return sorted, indices, err
	}

	// Validate all sort keys exist and cache column pointers
	cachedCols := make([]ColumnData, len(sortKeys))
	for i, key := range sortKeys {
		col, exists := columns[key]
		if !exists {
			return nil, nil, fmt.Errorf("sort key column not found: %s", key)
		}
		cachedCols[i] = col
	}

	// Get first column to determine row count
	var n int
	for _, col := range columns {
		if n = colLen(col); n > 0 {
			break
		}
	}

	if n == 0 {
		return columns, nil, nil
	}

	// Create permutation indices [0, 1, 2, ..., n-1]
	indices := getSortIndices(n)
	for i := range indices {
		indices[i] = i
	}

	// Multi-key sort with cached columns (no map lookups in comparator)
	sort.Slice(indices, func(i, j int) bool {
		return compareMultiKeyCached(cachedCols, indices[i], indices[j])
	})

	// Apply permutation to all columns
	result := make(map[string]ColumnData, len(columns))
	for colName, colData := range columns {
		result[colName] = colPermute(colData, indices)
	}

	return result, indices, nil
}

// sortColumnsByTimeOnly is an optimized path for time-only sorting.
// Avoids the multi-key comparator overhead for the common case.
func sortColumnsByTimeOnly(columns map[string]ColumnData) (map[string]ColumnData, error) {
	sorted, _, err := sortColumnsByTimeOnlyWithPermutation(columns)
	return sorted, err
}

// sortColumnsByTimeOnlyWithPermutation sorts by time and returns the permutation used.
// Returns nil indices when data is already sorted (no permutation needed).
func sortColumnsByTimeOnlyWithPermutation(columns map[string]ColumnData) (map[string]ColumnData, []int, error) {
	timeCol, exists := columns["time"]
	if !exists {
		return nil, nil, fmt.Errorf("time column not found")
	}

	times, ok := timeCol.Data.([]int64)
	if !ok {
		return nil, nil, fmt.Errorf("time column is not []int64")
	}

	n := len(times)
	if n == 0 {
		return columns, nil, nil
	}

	// FAST PATH: Check if already sorted (common case for time-series producers)
	// This is O(n) but much cheaper than sorting + permutation when data is in order
	alreadySorted := true
	for i := 1; i < n; i++ {
		if times[i] < times[i-1] {
			alreadySorted = false
			break
		}
	}
	if alreadySorted {
		return columns, nil, nil // nil indices signals identity permutation
	}

	// Create permutation indices
	indices := getSortIndices(n)
	for i := range indices {
		indices[i] = i
	}

	// Sort by time directly (no function call overhead per comparison)
	sort.Slice(indices, func(i, j int) bool {
		return times[indices[i]] < times[indices[j]]
	})

	// Apply permutation to all columns
	result := make(map[string]ColumnData, len(columns))
	for colName, colData := range columns {
		result[colName] = colPermute(colData, indices)
	}

	return result, indices, nil
}

// compareMultiKeyCached compares two rows by multiple sort keys using cached column pointers
// This avoids map lookups on every comparison (called O(n log n) times)
func compareMultiKeyCached(cachedCols []ColumnData, i, j int) bool {
	for _, col := range cachedCols {
		if colLess(col, i, j) {
			return true
		}
		if colLess(col, j, i) {
			return false
		}
		// Equal, continue to next key
	}

	// All keys equal
	return false
}

// sortEntryByKeys sorts a bufferEntry by the given keys,
// keeping validity bitmaps aligned with the reordered data.
// Uses the permutation returned by sortColumnsByKeysWithPermutation to avoid
// a second sort pass when validity bitmaps need reordering.
func sortEntryByKeys(batch *bufferEntry, sortKeys []string) *bufferEntry {
	sorted, indices, err := sortColumnsByKeysWithPermutation(batch.columns, sortKeys)
	putSortIndices(indices)
	if err != nil {
		return batch
	}
	// nil indices means already sorted
	result := &bufferEntry{
		columns:     sorted,
		tagColumns:  batch.tagColumns,
		schema:      batch.schema,
		recordCount: batch.recordCount,
	}
	return result
}

// sliceEntryByIndices extracts rows from a bufferEntry by index list,
// keeping validity bitmaps aligned.
func sliceEntryByIndices(batch *bufferEntry, indices []int) *bufferEntry {
	sliced := make(map[string]ColumnData, len(batch.columns))
	for name, col := range batch.columns {
		sliced[name] = colSlice(col, indices)
	}
	return &bufferEntry{
		columns:     sliced,
		tagColumns:  batch.tagColumns,
		schema:      batch.schema,
		recordCount: len(indices),
	}
}

// microPerHour is the number of microseconds in one hour (3600 * 1,000,000)
const microPerHour = int64(3600_000_000)

// hourBucket represents a collection of row indices belonging to a specific hour
// Used for hash-based grouping that doesn't require globally sorted data
type hourBucket struct {
	hourID  int64 // Hour identifier (microseconds / microPerHour)
	indices []int // Row indices belonging to this hour
	minTime int64 // Minimum timestamp in this hour (microseconds)
	maxTime int64 // Maximum timestamp in this hour (microseconds)
}

// hourIDToTime converts an hourID back to a time.Time for path generation
func hourIDToTime(hourID int64) time.Time {
	return time.UnixMicro(hourID * microPerHour).UTC()
}

// groupByHour groups row indices by hour and tracks min/max times
// Works correctly regardless of whether data is globally sorted by time
// Returns: map of hourID -> bucket, global min time, global max time
// Uses integer division for fast hour extraction (no time.Time allocations)
func groupByHour(times []int64) (map[int64]*hourBucket, int64, int64, error) {
	if len(times) == 0 {
		return nil, 0, 0, fmt.Errorf("empty time column")
	}

	buckets := make(map[int64]*hourBucket)
	globalMin := times[0]
	globalMax := times[0]

	// Single pass: group by hour and track min/max
	for i, t := range times {
		// Update global min/max
		if t < globalMin {
			globalMin = t
		}
		if t > globalMax {
			globalMax = t
		}

		// Fast hour extraction using integer division (no time.Time allocation)
		hourID := t / microPerHour

		// Get or create bucket
		bucket, exists := buckets[hourID]
		if !exists {
			bucket = &hourBucket{
				hourID:  hourID,
				indices: make([]int, 0, 100), // Pre-allocate some capacity
				minTime: t,
				maxTime: t,
			}
			buckets[hourID] = bucket
		} else {
			// Update bucket min/max
			if t < bucket.minTime {
				bucket.minTime = t
			}
			if t > bucket.maxTime {
				bucket.maxTime = t
			}
		}

		// Add row index to bucket
		bucket.indices = append(bucket.indices, i)
	}

	return buckets, globalMin, globalMax, nil
}

// generateStoragePath creates a hierarchical storage path for partition pruning
// Format: {database}/{measurement}/{YYYY}/{MM}/{DD}/{HH}/{measurement}_{timestamp}_{nanos}.parquet
//
// This hierarchical structure enables DuckDB to skip entire directories when querying time ranges:
// - Query all of November: read_parquet('s3://bucket/db/cpu/2025/11/*/*/*.parquet')
// - Query specific day: read_parquet('s3://bucket/db/cpu/2025/11/25/*/*.parquet')
// - Query specific hour: read_parquet('s3://bucket/db/cpu/2025/11/25/16/*.parquet')
func (b *ArrowBuffer) generateStoragePath(database, measurement string, partitionTime time.Time) string {
	// Hierarchical partitioning: year/month/day/hour
	year := partitionTime.Format("2006")
	month := partitionTime.Format("01")
	day := partitionTime.Format("02")
	hour := partitionTime.Format("15")

	// Filename includes measurement, timestamp, and nanos for uniqueness
	// Use current time for filename to avoid collisions
	now := time.Now().UTC()
	timestamp := now.Format("20060102_150405")
	nanos := now.UnixNano() % 1_000_000_000

	return fmt.Sprintf("%s/%s/%s/%s/%s/%s/%s_%s_%09d.parquet",
		database, measurement, year, month, day, hour, measurement, timestamp, nanos)
}

// FlushAll flushes all buffered data to storage
func (b *ArrowBuffer) FlushAll(ctx context.Context) error {
	b.logger.Info().Msg("Flushing all buffers...")

	var lastErr error

	// Flush all buffers in all shards
	for shardIdx := range b.shards {
		shard := b.shards[shardIdx]

		shard.mu.Lock()

		// Copy keys to avoid modifying map while iterating
		keys := make([]string, 0, len(shard.buffers))
		for key, entry := range shard.buffers {
			if entry.isEmpty() {
				continue
			}
			keys = append(keys, key)
		}

		for _, key := range keys {
			parts := splitBufferKey(key)
			if len(parts) != 2 {
				b.logger.Error().Str("buffer_key", key).Msg("Invalid buffer key format during flush")
				continue
			}

			if err := b.flushBufferLocked(ctx, shard, key, parts[0], parts[1], "manual"); err != nil {
				b.logger.Error().Err(err).Str("buffer_key", key).Msg("Failed to flush buffer")
				lastErr = err
			}
			// flushBufferLocked returns with the lock held (re-acquires after I/O)
		}

		shard.mu.Unlock()
	}

	b.logger.Info().Msg("All buffers flushed")
	return lastErr
}

// Close stops the buffer and flushes remaining data
//
// Shutdown ordering matters here:
//  1. Set b.closing so writer goroutines short-circuit before reaching
//     the channel send. Writers past shard.mu.Unlock() but not yet at
//     the select would otherwise race a closed channel.
//  2. Cancel b.ctx so flush workers exit via the <-b.ctx.Done() arm of
//     their select. Data already enqueued is dropped in favor of WAL
//     replay — that's the correct trade-off given a graceful shutdown
//     should be quick.
//  3. We deliberately do NOT close(b.flushQueue). Workers exit on ctx
//     cancellation; closing the channel would re-introduce the
//     send-on-closed-channel race the closing flag was added to fix.
//  4. Wait for workers to drain. Then take shard locks to flush any
//     in-memory buffers synchronously.

// SetAdaptiveFlushEngine 设置自适应刷盘引擎。由 main.go 在启动时注入。
func (b *ArrowBuffer) SetAdaptiveFlushEngine(engine *AdaptiveFlushEngine) {
	b.adaptiveFlush.Store(engine)
}

// StartAdaptiveFlush 启动自适应刷盘引擎的后台循环。
func (b *ArrowBuffer) StartAdaptiveFlush() {
	if e := b.adaptiveFlush.Load(); e != nil {
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			e.Run(b.ctx)
		}()
	}
}

func (b *ArrowBuffer) Close() error {
	b.logger.Info().Msg("Closing ArrowBuffer...")

	// Mark closing BEFORE cancelling so any writer past the shard
	// unlock observes either the flag (skips send) or the cancelled
	// ctx (Done arm fires). Either path avoids the panic.
	b.closing.Store(true)
	b.cancel()

	// Wait for all workers to finish (they exit via b.ctx.Done())
	b.wg.Wait()

	b.logger.Info().Msg("All flush workers stopped, flushing remaining buffers")

	// Flush all remaining buffers in all shards
	for shardIdx := range b.shards {
		shard := b.shards[shardIdx]

		shard.mu.Lock()

		// Copy keys to avoid modifying map while iterating
		// (flushBufferLocked releases and re-acquires the lock during I/O)
		keys := make([]string, 0, len(shard.buffers))
		for key, entry := range shard.buffers {
			if entry.isEmpty() {
				continue
			}
			keys = append(keys, key)
		}

		for _, key := range keys {
			parts := splitBufferKey(key)
			if len(parts) != 2 {
				b.logger.Error().Str("buffer_key", key).Msg("Invalid buffer key format during close")
				continue
			}

			flushCtx, flushCancel := context.WithTimeout(context.Background(), b.flushTimeout)
			if err := b.flushBufferLocked(flushCtx, shard, key, parts[0], parts[1], "shutdown"); err != nil {
				b.logger.Error().Err(err).Str("buffer_key", key).Msg("Failed to flush buffer during close")
			}
			flushCancel()
			// flushBufferLocked returns with the lock held (re-acquires after I/O)
		}

		shard.mu.Unlock()
	}

	b.logger.Info().
		Int64("total_records_written", b.totalRecordsWritten.Load()).
		Int64("total_flushes", b.totalFlushes.Load()).
		Msg("ArrowBuffer closed")

	return nil
}

// GetStats returns buffer statistics
func (b *ArrowBuffer) GetStats() map[string]interface{} {
	// Count active buffers across all shards
	activeBuffers := 0
	for shardIdx := range b.shards {
		shard := b.shards[shardIdx]
		shard.mu.RLock()
		activeBuffers += len(shard.buffers)
		shard.mu.RUnlock()
	}

	// Read atomic values (lock-free!)
	return map[string]interface{}{
		"total_records_buffered": b.totalRecordsBuffered.Load(),
		"total_records_written":  b.totalRecordsWritten.Load(),
		"total_flushes":          b.totalFlushes.Load(),
		"total_errors":           b.totalErrors.Load(),
		"total_wal_errors":       b.totalWALErrors.Load(),
		"total_wal_dropped":      b.totalWALDropped.Load(),
		"active_buffers":         activeBuffers,
		"flush_queue_depth":      b.queueDepth.Load(),
		"flush_workers":          b.flushWorkers,
	}
}

// TotalBufferedRecords 返回指定 bufferKey 的总缓冲记录数。
func (b *ArrowBuffer) TotalBufferedRecords(bufferKey string) int {
	shard := b.getShard(bufferKey)
	shard.mu.RLock()
	if entry, ok := shard.buffers[bufferKey]; ok {
		count := entry.recordCount
		shard.mu.RUnlock()
		return count
	}
	shard.mu.RUnlock()
	return 0
}

// TotalBufferedBytes 返回所有 bufferKey 的估算内存总占用。
func (b *ArrowBuffer) TotalBufferedBytes() uint64 {
	var total uint64
	for i := uint32(0); i < b.shardCount; i++ {
		shard := b.shards[i]
		shard.mu.RLock()
		for _, entry := range shard.buffers {
			total += entry.estimatedBytes
		}
		shard.mu.RUnlock()
	}
	return total
}
