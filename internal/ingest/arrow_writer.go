package ingest

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"unsafe"

	"iedb/internal/config"

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

// int64SliceToTimestamps converts []int64 to []arrow.Timestamp without allocation.
// This is safe because arrow.Timestamp is defined as `type Timestamp int64`.
// The conversion is a simple reinterpretation of the slice header.
func int64SliceToTimestamps(src []int64) []arrow.Timestamp {
	return *(*[]arrow.Timestamp)(unsafe.Pointer(&src))
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

// schemaCacheEntry holds a cached schema with LRU tracking
type schemaCacheEntry struct {
	schema     *arrow.Schema
	key        string
	prev, next *schemaCacheEntry
}

// schemaLRUCache is a thread-safe LRU cache for Arrow schemas
type schemaLRUCache struct {
	capacity int
	cache    map[string]*schemaCacheEntry
	head     *schemaCacheEntry // Most recently used
	tail     *schemaCacheEntry // Least recently used
	mu       sync.RWMutex
	hits     int64
	misses   int64
}

// newSchemaLRUCache creates a new LRU cache with given capacity
func newSchemaLRUCache(capacity int) *schemaLRUCache {
	return &schemaLRUCache{
		capacity: capacity,
		cache:    make(map[string]*schemaCacheEntry),
	}
}

// get retrieves a schema from cache, returns nil if not found
func (c *schemaLRUCache) get(key string) *arrow.Schema {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.cache[key]
	if !ok {
		c.misses++
		return nil
	}

	// Move to front (most recently used)
	c.moveToFront(entry)
	c.hits++
	return entry.schema
}

// set adds or updates a schema in cache
func (c *schemaLRUCache) set(key string, schema *arrow.Schema) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if already exists
	if entry, ok := c.cache[key]; ok {
		entry.schema = schema
		c.moveToFront(entry)
		return
	}

	// Create new entry
	entry := &schemaCacheEntry{
		schema: schema,
		key:    key,
	}

	// Add to cache
	c.cache[key] = entry
	c.addToFront(entry)

	// Evict if over capacity
	if len(c.cache) > c.capacity {
		c.evictLRU()
	}
}

// moveToFront moves an entry to the front of the list
func (c *schemaLRUCache) moveToFront(entry *schemaCacheEntry) {
	if entry == c.head {
		return // Already at front
	}

	// Remove from current position
	c.removeEntry(entry)

	// Add to front
	c.addToFront(entry)
}

// addToFront adds an entry to the front of the list
func (c *schemaLRUCache) addToFront(entry *schemaCacheEntry) {
	entry.prev = nil
	entry.next = c.head

	if c.head != nil {
		c.head.prev = entry
	}
	c.head = entry

	if c.tail == nil {
		c.tail = entry
	}
}

// removeEntry removes an entry from the list
func (c *schemaLRUCache) removeEntry(entry *schemaCacheEntry) {
	if entry.prev != nil {
		entry.prev.next = entry.next
	} else {
		c.head = entry.next
	}

	if entry.next != nil {
		entry.next.prev = entry.prev
	} else {
		c.tail = entry.prev
	}
}

// evictLRU removes the least recently used entry
func (c *schemaLRUCache) evictLRU() {
	if c.tail == nil {
		return
	}

	// Remove from cache map
	delete(c.cache, c.tail.key)

	// Remove from list
	c.removeEntry(c.tail)
}

// ArrowWriter handles Arrow schema inference and Parquet writing
type ArrowWriter struct {
	compression     compress.Compression
	useDictionary   bool
	writeStatistics bool
	dataPageVersion string

	// LRU Schema cache (measurement -> schema) with bounded size
	schemaCache *schemaLRUCache

	logger zerolog.Logger
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

	// Schema cache capacity - 1000 schemas is ~100-200KB memory
	// Most deployments have <100 unique measurement/schema combinations
	const schemaCacheCapacity = 1000

	return &ArrowWriter{
		compression:     comp,
		useDictionary:   cfg.UseDictionary,
		writeStatistics: cfg.WriteStatistics,
		dataPageVersion: cfg.DataPageVersion,
		schemaCache:     newSchemaLRUCache(schemaCacheCapacity),
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

// getSchema gets or infers Arrow schema for columnar data (LRU cached per measurement)
func (w *ArrowWriter) getSchema(measurement string, columns map[string]interface{}, tagColumns []string, decimalCols map[string]config.DecimalSpec) (*arrow.Schema, error) {
	// Create cache key from column names and types
	var colNames []string
	var typeNames []string

	for name := range columns {
		if name[0] == '_' {
			continue // Skip internal columns
		}
		colNames = append(colNames, name)
	}

	// Get type signatures
	for _, name := range colNames {
		col := columns[name]
		switch col.(type) {
		case []int64:
			if name == "time" {
				typeNames = append(typeNames, "timestamp")
			} else {
				typeNames = append(typeNames, "int64")
			}
		case []float64:
			typeNames = append(typeNames, "float64")
		case []string:
			typeNames = append(typeNames, "string")
		case []bool:
			typeNames = append(typeNames, "bool")
		case []decimal128.Num:
			typeNames = append(typeNames, "decimal128")
		default:
			typeNames = append(typeNames, "unknown")
		}
	}

	// Create cache key (includes tag columns to ensure metadata correctness)
	cacheKey := fmt.Sprintf("%s:%v:%v:%v", measurement, colNames, typeNames, tagColumns)

	// Check LRU cache
	if schema := w.schemaCache.get(cacheKey); schema != nil {
		return schema, nil
	}

	// Cache miss - infer schema
	schema, err := w.inferSchema(columns, tagColumns, decimalCols)
	if err != nil {
		return nil, err
	}

	// Store in LRU cache
	w.schemaCache.set(cacheKey, schema)

	w.logger.Debug().
		Str("measurement", measurement).
		Str("cache_key", cacheKey).
		Msg("Schema cache miss, inferred and cached")

	return schema, nil
}

// inferSchema infers Arrow schema from columnar data.
// tagColumns optionally lists which columns are tags (stored as schema metadata for compaction dedup).
// decimalCols optionally maps column names to DecimalSpec for Decimal128 columns.
func (w *ArrowWriter) inferSchema(columns map[string]interface{}, tagColumns []string, decimalCols map[string]config.DecimalSpec) (*arrow.Schema, error) {
	var fields []arrow.Field

	for name, col := range columns {
		// Skip internal metadata columns
		if name[0] == '_' {
			continue
		}

		var arrowType arrow.DataType

		switch arr := col.(type) {
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
// validity is an optional map of column name to []bool where false means null.
// Columns without a validity entry (or when validity is nil) are treated as fully valid.
// tagColumns optionally lists which columns are tags (stored as Parquet metadata for compaction dedup).
// decimalCols optionally maps column names to DecimalSpec for Decimal128 type inference.
func (w *ArrowWriter) WriteParquetColumnar(ctx context.Context, measurement string, columns map[string]interface{}, validity map[string][]bool, tagColumns []string, decimalCols map[string]config.DecimalSpec) ([]byte, error) {
	// Get or infer schema (with caching)
	schema, err := w.getSchema(measurement, columns, tagColumns, decimalCols)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
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

	// Build arrays
	for i, field := range schema.Fields() {
		col, ok := columns[field.Name]
		if !ok {
			return nil, fmt.Errorf("column %s not found in data", field.Name)
		}

		// Get validity bitmap for this column (nil means all valid)
		var colValidity []bool
		if validity != nil {
			colValidity = validity[field.Name]
		}

		switch field.Type.ID() {
		case arrow.INT64:
			builder := array.NewInt64Builder(mem)
			builders[i] = builder
			if intCol, ok := col.([]int64); ok {
				builder.AppendValues(intCol, colValidity)
			} else {
				return nil, fmt.Errorf("column %s: expected []int64, got %T", field.Name, col)
			}
			arrays[i] = builder.NewArray()

		case arrow.TIMESTAMP:
			builder := array.NewTimestampBuilder(mem, arrow.FixedWidthTypes.Timestamp_us.(*arrow.TimestampType))
			builders[i] = builder
			if intCol, ok := col.([]int64); ok {
				// MEMORY FIX: Zero-copy conversion from []int64 to []arrow.Timestamp
				// This avoids allocating a temporary slice on every write
				tsValues := int64SliceToTimestamps(intCol)
				builder.AppendValues(tsValues, colValidity)
			} else {
				return nil, fmt.Errorf("column %s: expected []int64 for timestamp, got %T", field.Name, col)
			}
			arrays[i] = builder.NewArray()

		case arrow.FLOAT64:
			builder := array.NewFloat64Builder(mem)
			builders[i] = builder
			if floatCol, ok := col.([]float64); ok {
				builder.AppendValues(floatCol, colValidity)
			} else {
				return nil, fmt.Errorf("column %s: expected []float64, got %T", field.Name, col)
			}
			arrays[i] = builder.NewArray()

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
			dt := field.Type.(*arrow.Decimal128Type)
			builder := array.NewDecimal128Builder(mem, dt)
			builders[i] = builder
			if decCol, ok := col.([]decimal128.Num); ok {
				builder.AppendValues(decCol, colValidity)
			} else {
				return nil, fmt.Errorf("column %s: expected []decimal128.Num, got %T", field.Name, col)
			}
			arrays[i] = builder.NewArray()

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

	// Write to Parquet
	var buf bytes.Buffer

	// Configure Parquet writer properties (built once with all options)
	writerOpts := []parquet.WriterProperty{
		parquet.WithCompression(w.compression),
		parquet.WithDictionaryDefault(w.useDictionary),
		parquet.WithStats(w.writeStatistics),
	}
	if w.dataPageVersion == "2.0" {
		writerOpts = append(writerOpts, parquet.WithDataPageVersion(parquet.DataPageV2))
	}
	writerProps := parquet.NewWriterProperties(writerOpts...)

	// Create Arrow writer properties
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())

	// Create Parquet writer
	writer, err := pqarrow.NewFileWriter(
		schema,
		&buf,
		writerProps,
		arrowProps,
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

// TypedColumnBatch holds typed column arrays with optional validity bitmaps.
// Validity tracks which values are null (false=null, true=valid).
// Columns without a validity entry are fully valid (no nulls).
type TypedColumnBatch struct {
	Data       map[string]interface{} // typed arrays ([]int64, []float64, []string, []bool)
	Validity   map[string][]bool      // per-column null bitmap; nil entry = all valid
	TagColumns []string               // tag column names (for Parquet metadata, enables auto-dedup)
}
