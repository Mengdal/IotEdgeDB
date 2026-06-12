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

// int64SliceToTimestamps reinterprets a []int64 as []arrow.Timestamp without copying.
// Safe because arrow.Timestamp is defined as `type Timestamp int64` (identical layout).
// LIFETIME: the caller must ensure src is not GC'd or reallocated while the returned
// slice or any Arrow array/builder built from it is still alive. Use only within the
// same stack frame as src.
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

// ctxKeyRestoreFallback is a context value key used to break the recursion
// cycle: restoreBufferEntry → flushRecordsAsync → [fail] → restoreBufferEntry.
// When present in the context, flushRecordsAsync skips restore calls and logs
// a critical error instead.
type ctxKeyRestoreFallback struct{}

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

	// Pre-built Parquet writer properties (immutable after construction)
	writerProps *parquet.WriterProperties
	arrowProps  pqarrow.ArrowWriterProperties
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
// validity is an optional map of column name → []bool where false means null.
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
// TypedColumnBatch holds typed column arrays with optional validity bitmaps.
// Validity tracks which values are null (false=null, true=valid).
// Columns without a validity entry are fully valid (no nulls).
type TypedColumnBatch struct {
	Data       map[string]interface{} // typed arrays ([]int64, []float64, []string, []bool)
	Validity   map[string][]bool      // per-column null bitmap; nil entry = all valid
	TagColumns []string               // tag column names (for Parquet metadata, enables auto-dedup)
	Signature  string                 // sorted column-name string; cached to avoid per-write recomputation
}

// bufferEntry holds all buffered data and metadata for a single measurement key.
// Replaces 6 separate maps (buffers, bufferStartTimes, bufferRecordCounts,
// bufferSchemas, bufferEstimatedBytes, bufferRefreshIndex) with a single struct,
// reducing 5+ hash lookups to 1 on the write path.
type bufferEntry struct {
	batches        []*TypedColumnBatch // buffered data batches (immutable)
	startTime      time.Time           // first record arrival time
	recordCount    int                 // total record count
	estimatedBytes uint64              // estimated memory usage
	schema         string              // column signature for schema evolution
	refreshIndex   int                 // Arrow VIEW incremental refresh cursor
}

type bufferShard struct {
	mu      sync.RWMutex
	buffers map[string]*bufferEntry // bufferKey → merged buffer state
}

// estimateBytesPerRow 估算 TypedColumnBatch 中每行的内存占用。
func estimateBytesPerRow(batch *TypedColumnBatch) uint64 {
	if batch == nil || len(batch.Data) == 0 {
		return 256
	}
	var totalBytes uint64
	for _, col := range batch.Data {
		switch v := col.(type) {
		case []int64:
			totalBytes += 8
		case []float64:
			totalBytes += 8
		case []bool:
			totalBytes += 1
		case []string:
			n := len(v)
			if n > 100 {
				n = 100
			}
			var sumLen int
			for i := 0; i < n; i++ {
				sumLen += len(v[i])
			}
			totalBytes += uint64(sumLen / n)
		default:
			totalBytes += 64
		}
	}
	totalBytes += uint64(len(batch.Data))
	return totalBytes
}

// flushTask represents a flush operation to be executed by workers
type flushTask struct {
	ctx         context.Context
	cancel      context.CancelFunc // must be called when task completes to release resources
	bufferKey   string
	database    string
	measurement string
	records     []interface{}
	recordCount int
	trigger     string // "size", "age", "hard_limit", or "manual"
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

	// Optional buffer change notifier (injected by database package)
	notifier BufferChangeNotifier

	// Adaptive flush engine (replaces periodicFlush timer when set).
	// atomic.Pointer for race-free read in periodicFlush / write paths.
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
	ctx           context.Context
	cancel        context.CancelFunc
	flushTimer    *time.Timer   // self-adjusting: fires when the oldest buffer is due to expire
	flushDeadline time.Time     // absolute time when flushTimer will fire; updated whenever the timer is (re)set
	newBufferCh   chan struct{} // signals periodicFlush that a new buffer was created (used for idle→active wake-up)
	wg            sync.WaitGroup

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
	maxBufferAge time.Duration // pre-calculated from cfg.MaxBufferAgeMS

	// Metrics (using atomic operations to avoid lock contention)
	totalRecordsBuffered atomic.Int64
	totalRecordsWritten  atomic.Int64
	totalFlushes         atomic.Int64
	totalErrors          atomic.Int64
	totalWALErrors       atomic.Int64 // WAL write failures (real I/O / serialization errors)
	totalWALDropped      atomic.Int64 // WAL backpressure drops (entry queued but channel full)
	// totalSchemaChurnExceeded counts requests rejected because the
	// schema-evolution flush loop hit schemaEvolutionMaxIters under
	// sustained concurrent schema rotation against the same
	// (database, measurement) buffer. Pathological signal — operators
	// alert on a non-zero rate.
	totalSchemaChurnExceeded atomic.Int64
	queueDepth               atomic.Int64 // Current flush queue depth

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
func getColumnSignature(columns map[string]interface{}) string {
	type colEntry struct{ name, typ string }
	entries := make([]colEntry, 0, len(columns))
	size := -1 // will add 1 per comma; starts at -1 so the first entry adds 0 commas
	for name, val := range columns {
		if len(name) == 0 || name[0] == '_' {
			continue // skip empty and internal columns
		}
		var typ string
		switch val.(type) {
		case []int64:
			typ = "i64"
		case []float64:
			typ = "f64"
		case []string:
			typ = "str"
		case []bool:
			typ = "bool"
		case []decimal128.Num:
			typ = "dec"
		default:
			typ = "unk"
		}
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
func StripSchemaHash(bufferKey string) (baseKey string, hash string) {
	if idx := strings.LastIndex(bufferKey, "__"); idx >= 0 {
		return bufferKey[:idx], bufferKey[idx+1:]
	}
	return bufferKey, ""
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
			if !exists || !hasTypeConflict(entry.schema, newSig) {
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
			if err := b.flushBufferLocked(flushCtx, shard, key, parts[0], parts[1]); err != nil {
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
		flushTimer:           time.NewTimer(time.Duration(cfg.MaxBufferAgeMS) * time.Millisecond),
		flushDeadline:        time.Now().UTC().Add(time.Duration(cfg.MaxBufferAgeMS) * time.Millisecond),
		newBufferCh:          make(chan struct{}, 1),
		flushQueue:           make(chan flushTask, queueSize),
		flushWorkers:         flushWorkers,
		flushTimeout:         flushTimeout,
		maxBufferAge:         time.Duration(cfg.MaxBufferAgeMS) * time.Millisecond,
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

	// Start background flush
	buffer.wg.Add(1)
	go buffer.periodicFlush()

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

// WriteTypedColumnarDirect writes a pre-typed column batch to the buffer,
// bypassing the []interface{} → typed conversion in convertColumnsToTyped.
// Used by format-specific parsers (e.g., TLE) that know column types at compile time.
func (b *ArrowBuffer) WriteTypedColumnarDirect(ctx context.Context, database, measurement string, batch *TypedColumnBatch, numRecords int) error {
	return b.writeTypedColumnarInternal(ctx, database, measurement, batch, numRecords, false)
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

// schemaEvolutionMaxIters bounds the schema-evolution flush loop.
// Steady state: 1 iteration (no schema change). Adversarial state:
// rotating-schema writers could in principle trigger an unbounded
// loop because flushBufferLocked releases shard.mu for I/O and a
// concurrent writer can install a fresh schema in that window.
//
// Hitting the cap means at least 8 distinct schemas raced through
// the same (database, measurement) buffer in the time window of one
// flush — a sustained-churn signal, not transient. Surfacing this as
// ErrSchemaChurnExceeded lets the caller fail the request with a 503
// rather than committing a wide schema-mixed buffer to disk that
// query-side schema-on-read would then have to reconcile.
const schemaEvolutionMaxIters = 8

// ErrSchemaChurnExceeded is returned by flushOnSchemaChangeLocked when
// schemaEvolutionMaxIters is reached. Treat as a transient backpressure
// signal: the caller should reject the write with a retryable status
// (503) so upstream senders back off; the in-buffer rows for the older
// schemas have already been flushed to durable Parquet by the loop's
// per-iteration flushes, so there is no data loss — only a per-request
// failure under sustained schema-rotation churn.
var ErrSchemaChurnExceeded = errors.New("schema-evolution loop exceeded max iterations: sustained concurrent schema churn against the same (database, measurement)")

// writeColumnar writes a columnar record to the buffer
func (b *ArrowBuffer) writeColumnar(ctx context.Context, database string, record *models.ColumnarRecord) error {
	return b.writeColumnarInternal(ctx, database, record, false)
}

func (b *ArrowBuffer) writeColumnarInternal(ctx context.Context, database string, record *models.ColumnarRecord, skipWAL bool) error {
	// WAL: Write to WAL before buffering (if enabled)
	// Skip WAL during recovery to avoid re-writing recovered data
	if b.wal != nil && !skipWAL {
		// ZERO-COPY PATH: Use raw msgpack bytes if available (avoids re-serialization)
		if len(record.RawPayload) > 0 {
			if err := b.wal.AppendRawWithMeta(database, record.RawPayload); err != nil {
				// Don't fail the write — WAL is for durability, not
				// correctness. recordWALError differentiates backpressure
				// drops (sampled Warn) from real I/O failures (unsampled
				// Error) so operators can alert on the right signal.
				b.recordWALError(err, func(ev *zerolog.Event) {
					ev.Str("database", database).
						Str("measurement", record.Measurement).
						Int("payload_size", len(record.RawPayload))
				})
			}
		} else {
			// FALLBACK: Convert columnar to row format for WAL storage
			// This path is used for LineProtocol or when raw bytes aren't available
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

	// Convert []interface{} columns to typed arrays (optimized with zero-copy fast paths)
	typedColumns, numRecords, err := b.convertColumnsToTyped(record.Measurement, record.Columns)
	if err != nil {
		return fmt.Errorf("failed to convert columns: %w", err)
	}

	// Propagate tag column names for Parquet metadata (enables auto-dedup in compaction)
	typedColumns.TagColumns = record.TagColumns

	// Column signature for schema evolution detection (pre-computed in convertColumnsToTyped)
	newSignature := typedColumns.Signature
	if newSignature == "" && len(typedColumns.Data) > 0 {
		newSignature = getColumnSignature(typedColumns.Data)
	}

	// Construct schema-specific buffer key so different schemas for the
	// same measurement naturally get separate buffer entries.
	bufferKey := schemaKey(database, record.Measurement, newSignature)

	// Flush any existing buffer for this measurement whose schema has a
	// type conflict with the new one (same field, different type).
	baseKey := database + "/" + record.Measurement
	b.flushTypeConflicts(baseKey, newSignature)

	// OPTIMIZATION: Get shard for this buffer key (lock sharding)
	shard := b.getShard(bufferKey)

	// OPTIMIZATION: Extract-then-flush pattern
	// Hold lock ONLY to extract records, flush outside lock
	var recordsToFlush []interface{}
	var shouldFlush bool

	shard.mu.Lock()

	// Initialize buffer and record count if needed
	entry, exists := shard.buffers[bufferKey]
	if !exists {
		entry = &bufferEntry{
			startTime: time.Now().UTC(),
			schema:    newSignature,
		}
		shard.buffers[bufferKey] = entry
		// Tell periodicFlush to recompute its wakeup time for this new buffer.
		select {
		case b.newBufferCh <- struct{}{}:
		default:
		}
	}

	// Add typed columns to buffer (already converted via zero-copy fast paths)
	entry.batches = append(entry.batches, typedColumns)

	// CRITICAL FIX: Track count incrementally instead of O(n) loop
	entry.recordCount += numRecords
	entry.estimatedBytes += uint64(numRecords) * estimateBytesPerRow(typedColumns)
	totalBuffered := entry.recordCount

	// Check if buffer needs flush (size-based).
	// When adaptive flush engine is active, it owns all flush decisions and
	// this fixed-size gate is skipped to allow memory-pressure-driven buffering.
	if b.adaptiveFlush.Load() == nil && totalBuffered >= b.config.MaxBufferSize {
		// Extract records to flush (hold lock for microseconds only)
		recordsToFlush = make([]interface{}, len(entry.batches))
		for i, batch := range entry.batches {
			recordsToFlush[i] = batch
		}

		// Try to enqueue BEFORE deleting the entry. tryEnqueueFlush is
		// non-blocking (select with default) and does not acquire any
		// shard locks — safe to call while holding shard.mu.
		flushCtx, flushCancel := context.WithTimeout(b.ctx, b.flushTimeout)
		task := flushTask{
			ctx:         flushCtx,
			cancel:      flushCancel,
			bufferKey:   bufferKey,
			database:    database,
			measurement: record.Measurement,
			records:     recordsToFlush,
			recordCount: totalBuffered,
		}
		outcome := b.tryEnqueueFlush(task, flushCancel, bufferKey, totalBuffered)

		if outcome == flushQueued {
			// Only delete after confirmed enqueue — avoids data loss
			// when flushQueue is full or buffer is closing.
			delete(shard.buffers, bufferKey)
			shouldFlush = true
		}
		// On any non-queued outcome: keep entry in buffer for retry.
		// flushCancel has already been called by tryEnqueueFlush.
	}

	// Release lock IMMEDIATELY (lock held for <1ms)
	shard.mu.Unlock()

	// OPTIMIZATION: Update metrics with atomic operations (lock-free!)
	b.totalRecordsBuffered.Add(int64(numRecords))

	b.logger.Debug().
		Str("buffer_key", bufferKey).
		Int("num_records", numRecords).
		Int("total_buffered", totalBuffered).
		Bool("flushing", shouldFlush).
		Msg("Added columnar data to buffer")

	// Notify VIEW manager of new data
	if b.notifier != nil {
		b.notifier.OnNewData(bufferKey)
	}
	return nil
}

// writeTypedColumnarInternal writes a pre-typed column batch to the buffer.
// Mirrors writeColumnarInternal but skips convertColumnsToTyped since the batch
// is already typed ([]int64, []float64, []string). Used by format-specific parsers
// that know column types at compile time.
func (b *ArrowBuffer) writeTypedColumnarInternal(ctx context.Context, database, measurement string, typedColumns *TypedColumnBatch, numRecords int, skipWAL bool) error {
	// WAL: Convert typed batch to row format for WAL storage
	if b.wal != nil && !skipWAL {
		walRecords := typedBatchToWALRecords(database, measurement, typedColumns, numRecords, b.getDecimalColumns(measurement))
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

	// Column signature for schema evolution detection (pre-computed in convertColumnsToTyped)
	newSignature := typedColumns.Signature
	if newSignature == "" && len(typedColumns.Data) > 0 {
		newSignature = getColumnSignature(typedColumns.Data)
	}

	// Construct schema-specific buffer key.
	bufferKey := schemaKey(database, measurement, newSignature)

	// Flush any existing buffer with type conflicts.
	baseKey := database + "/" + measurement
	b.flushTypeConflicts(baseKey, newSignature)

	// Get shard for this buffer key (lock sharding)
	shard := b.getShard(bufferKey)

	var recordsToFlush []interface{}
	var shouldFlush bool

	shard.mu.Lock()

	// Initialize buffer and record count if needed
	entry, exists := shard.buffers[bufferKey]
	if !exists {
		entry = &bufferEntry{
			startTime: time.Now().UTC(),
			schema:    newSignature,
		}
		shard.buffers[bufferKey] = entry
		// Tell periodicFlush to recompute its wakeup time for this new buffer.
		select {
		case b.newBufferCh <- struct{}{}:
		default:
		}
	}

	// Add typed columns to buffer directly (no conversion needed)
	entry.batches = append(entry.batches, typedColumns)

	entry.recordCount += numRecords
	entry.estimatedBytes += uint64(numRecords) * estimateBytesPerRow(typedColumns)
	totalBuffered := entry.recordCount

	// Check if buffer needs flush (size-based).
	// When adaptive flush engine is active, it owns all flush decisions and
	// this fixed-size gate is skipped to allow memory-pressure-driven buffering.
	if b.adaptiveFlush.Load() == nil && totalBuffered >= b.config.MaxBufferSize {
		recordsToFlush = make([]interface{}, len(entry.batches))
		for i, batch := range entry.batches {
			recordsToFlush[i] = batch
		}

		// Try to enqueue BEFORE deleting — safe inside lock (non-blocking).
		flushCtx, flushCancel := context.WithTimeout(b.ctx, b.flushTimeout)
		task := flushTask{
			ctx:         flushCtx,
			cancel:      flushCancel,
			bufferKey:   bufferKey,
			database:    database,
			measurement: measurement,
			records:     recordsToFlush,
			recordCount: totalBuffered,
		}
		outcome := b.tryEnqueueFlush(task, flushCancel, bufferKey, totalBuffered)

		if outcome == flushQueued {
			delete(shard.buffers, bufferKey)
			shouldFlush = true
		}
	}

	shard.mu.Unlock()

	b.totalRecordsBuffered.Add(int64(numRecords))

	b.logger.Debug().
		Str("buffer_key", bufferKey).
		Int("num_records", numRecords).
		Int("total_buffered", totalBuffered).
		Bool("flushing", shouldFlush).
		Msg("Added typed columnar data to buffer")

	// Notify VIEW manager of new data (TLE path)
	if b.notifier != nil {
		b.notifier.OnNewData(bufferKey)
	}

	return nil
}

// typedBatchToWALRecords converts a TypedColumnBatch to row-format records for WAL storage.
// This is the WAL fallback path for typed batches (e.g., TLE) that don't have raw msgpack bytes.
func typedBatchToWALRecords(database, measurement string, batch *TypedColumnBatch, numRecords int, decimalCols map[string]config.DecimalSpec) []map[string]interface{} {
	if numRecords == 0 {
		return nil
	}

	records := make([]map[string]interface{}, numRecords)
	for i := 0; i < numRecords; i++ {
		row := map[string]interface{}{
			"_database":    database,
			"_measurement": measurement,
		}
		for colName, colData := range batch.Data {
			switch arr := colData.(type) {
			case []int64:
				if i < len(arr) {
					row[colName] = arr[i]
				}
			case []float64:
				if i < len(arr) {
					row[colName] = arr[i]
				}
			case []string:
				if i < len(arr) {
					row[colName] = arr[i]
				}
			case []bool:
				if i < len(arr) {
					row[colName] = arr[i]
				}
			case []decimal128.Num:
				// WAL stores decimals as float64 (lossy but WAL is recovery-only)
				if i < len(arr) {
					s := int32(0)
					if decimalCols != nil {
						if spec, ok := decimalCols[colName]; ok {
							s = spec.Scale
						}
					}
					f := arr[i].ToBigFloat(s)
					row[colName], _ = f.Float64()
				}
			}
		}
		records[i] = row
	}

	return records
}

// convertColumnsToTyped converts []interface{} columns to typed arrays with null tracking.
// Returns a TypedColumnBatch where Validity maps track which values are null (false=null).
// Columns with no nil values have no entry in Validity (all valid).
// ZERO-COPY OPTIMIZATION: Try bulk type assertion first before element-by-element conversion
func (b *ArrowBuffer) convertColumnsToTyped(measurement string, columns map[string][]interface{}) (*TypedColumnBatch, int, error) {
	typed := make(map[string]interface{})
	validity := make(map[string][]bool)
	var numRecords int

	// Look up decimal column config for this measurement (nil if none configured)
	decimalCols := b.getDecimalColumns(measurement)

	for name, col := range columns {
		if len(col) == 0 {
			continue
		}

		// Set record count from first column
		if numRecords == 0 {
			numRecords = len(col)
		}

		// Check if this column is declared as decimal — override normal type inference
		if decimalCols != nil {
			if spec, isDecimal := decimalCols[name]; isDecimal {
				arr, valid, err := convertToDecimal128Slice(col, spec.Precision, spec.Scale)
				if err != nil {
					return nil, 0, fmt.Errorf("decimal conversion error in column '%s': %w", name, err)
				}
				typed[name] = arr
				if valid != nil {
					validity[name] = valid
				}
				continue
			}
		}

		// Infer type from first non-nil value
		firstVal := firstNonNil(col)
		if firstVal == nil {
			continue // Skip all-nil columns
		}

		// FAST PATH: Try zero-copy bulk conversion first (fails fast on nils or mixed types).
		// If zero-copy succeeds, no nils exist and no validity bitmap is needed.
		switch firstVal.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			if arr, ok := b.tryInt64ZeroCopy(col); ok {
				typed[name] = arr
				continue
			}
			// Zero-copy failed (has nils or mixed types) — single-pass conversion with validity
			arr := make([]int64, len(col))
			valid := make([]bool, len(col))
			hasNils := false
			for i, v := range col {
				if v == nil {
					hasNils = true
					continue
				}
				valid[i] = true
				val, ok := toInt64(v)
				if !ok {
					return nil, 0, fmt.Errorf("cannot convert %T to int64 in column '%s'", v, name)
				}
				arr[i] = val
			}
			typed[name] = arr
			if hasNils {
				validity[name] = valid
			}

		case float32, float64:
			if arr, ok := b.tryFloat64ZeroCopy(col); ok {
				typed[name] = arr
				continue
			}
			arr := make([]float64, len(col))
			valid := make([]bool, len(col))
			hasNils := false
			for i, v := range col {
				if v == nil {
					hasNils = true
					continue
				}
				valid[i] = true
				val, ok := toFloat64(v)
				if !ok {
					return nil, 0, fmt.Errorf("cannot convert %T to float64 in column '%s'", v, name)
				}
				arr[i] = val
			}
			typed[name] = arr
			if hasNils {
				validity[name] = valid
			}

		case string:
			if arr, ok := b.tryStringZeroCopy(col); ok {
				typed[name] = arr
				continue
			}
			arr := make([]string, len(col))
			valid := make([]bool, len(col))
			hasNils := false
			for i, v := range col {
				if v == nil {
					hasNils = true
					continue
				}
				valid[i] = true
				str, ok := v.(string)
				if !ok {
					return nil, 0, fmt.Errorf("unexpected type in string column '%s': %T", name, v)
				}
				arr[i] = str
			}
			typed[name] = arr
			if hasNils {
				validity[name] = valid
			}

		case bool:
			if arr, ok := b.tryBoolZeroCopy(col); ok {
				typed[name] = arr
				continue
			}
			arr := make([]bool, len(col))
			valid := make([]bool, len(col))
			hasNils := false
			for i, v := range col {
				if v == nil {
					hasNils = true
					continue
				}
				valid[i] = true
				bval, ok := v.(bool)
				if !ok {
					return nil, 0, fmt.Errorf("unexpected type in bool column '%s': %T", name, v)
				}
				arr[i] = bval
			}
			typed[name] = arr
			if hasNils {
				validity[name] = valid
			}

		default:
			return nil, 0, fmt.Errorf("unsupported column type for '%s': %T", name, firstVal)
		}
	}

	batch := &TypedColumnBatch{Data: typed, Validity: validity, Signature: getColumnSignature(typed)}
	return batch, numRecords, nil
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

// periodicFlush runs in the background and flushes old buffers.
// It uses a self-adjusting timer that fires exactly when the oldest buffer is due
// to expire, eliminating the phase-misalignment lag of a fixed-period ticker.
func (b *ArrowBuffer) periodicFlush() {
	defer b.wg.Done()

	for {
		// If adaptive flush engine is active, it owns all age-based flushing.
		// Sleep until shutdown to avoid duplicate work and lock contention.
		if b.adaptiveFlush.Load() != nil {
			select {
			case <-b.ctx.Done():
				return
			}
		}

		select {
		case <-b.ctx.Done():
			return

		case <-b.newBufferCh:
			// A new buffer was created. Only reset the timer if the oldest
			// buffer's expiry is earlier than the currently scheduled deadline.
			// In practice this only triggers the idle→active transition: once
			// the timer is armed, a new buffer (expiry = now+maxAge) is always
			// later than any existing buffer's expiry, so the condition is false
			// and we skip the expensive shard scan entirely.
			nextDeadline := b.computeNextFlushDeadline()
			if nextDeadline.Before(b.flushDeadline) {
				if !b.flushTimer.Stop() {
					select {
					case <-b.flushTimer.C:
					default:
					}
				}
				b.flushDeadline = nextDeadline
				b.flushTimer.Reset(time.Until(nextDeadline))
			}

		case <-b.flushTimer.C:
			b.flushAgedBuffers()
			// Rearm the timer for the next oldest buffer expiry.
			b.flushDeadline = b.computeNextFlushDeadline()
			b.flushTimer.Reset(time.Until(b.flushDeadline))
		}
	}
}

// computeNextFlushDeadline returns the absolute time when the oldest buffered
// key is due to be flushed. If no buffers exist it returns now+maxAge so the
// goroutine sleeps until a new buffer signals it via newBufferCh.
// Returning an absolute time avoids drift from a second time.Now() call at
// the call site.
func (b *ArrowBuffer) computeNextFlushDeadline() time.Time {
	now := time.Now().UTC()
	maxAge := b.maxBufferAge
	earliest := now.Add(maxAge) // default when no buffers exist

	for _, shard := range b.shards {
		shard.mu.RLock()
		for _, entry := range shard.buffers {
			if expiry := entry.startTime.Add(maxAge); expiry.Before(earliest) {
				earliest = expiry
			}
		}
		shard.mu.RUnlock()
	}

	// Clamp to at least 1ms in the future so Reset never gets zero/negative.
	if earliest.Before(now.Add(time.Millisecond)) {
		return now.Add(time.Millisecond)
	}
	return earliest
}

// flushAgedBuffers flushes buffers that have exceeded max age
func (b *ArrowBuffer) flushAgedBuffers() {
	now := time.Now().UTC()
	maxAge := b.maxBufferAge

	threshold := maxAge

	// Iterate over all shards
	for shardIdx := range b.shards {
		shard := b.shards[shardIdx]

		// Collect aged keys under the lock, then release before
		// calling flushBufferLocked.  See flushTypeConflicts for
		// the full rationale — iterating while the lock is released
		// causes a fatal concurrent-map panic.
		shard.mu.Lock()
		var agedKeys []string
		for key, entry := range shard.buffers {
			age := now.Sub(entry.startTime)
			if age >= threshold {
				agedKeys = append(agedKeys, key)
			}
		}
		shard.mu.Unlock()

		for _, key := range agedKeys {
			shard.mu.Lock()
			entry, exists := shard.buffers[key]
			if !exists {
				shard.mu.Unlock()
				continue
			}
			if now.Sub(entry.startTime) < threshold {
				shard.mu.Unlock()
				continue
			}

			b.logger.Info().
				Str("buffer_key", key).
				Dur("age", now.Sub(entry.startTime)).
				Dur("threshold", threshold).
				Int("shard", shardIdx).
				Msg("Flushing aged buffer")

			// Parse buffer key to get database and measurement
			parts := splitBufferKey(key)
			if len(parts) != 2 {
				b.logger.Error().Str("buffer_key", key).Msg("Invalid buffer key format")
				shard.mu.Unlock()
				continue
			}

			flushCtx, flushCancel := context.WithTimeout(b.ctx, b.flushTimeout)
			if err := b.flushBufferLocked(flushCtx, shard, key, parts[0], parts[1]); err != nil {
				b.logger.Error().Err(err).Str("buffer_key", key).Msg("Failed to flush aged buffer")
			}
			flushCancel()
			// flushBufferLocked returns with the lock re-acquired.
			shard.mu.Unlock()
		}
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
				// Channel closed during shutdown
				return
			}
			b.queueDepth.Add(-1)

			b.logger.Debug().
				Int("worker_id", workerID).
				Str("buffer_key", task.bufferKey).
				Int("records", task.recordCount).
				Int64("queue_depth", b.queueDepth.Load()).
				Msg("Worker processing flush task")

			// Execute flush and gate post-flush cleanup on success.
			// On failure, data was restored to buffer — skip OnFlushComplete
			// (old VIEW is stale; new VIEW is created by the restore path)
			// and skip RecordBufferFlushRecords (inflating metrics).
			// Execute flush and gate post-flush cleanup on success.
			// On failure, data was restored to buffer — skip OnFlushComplete
			// (old VIEW is stale; new VIEW is created by the restore path)
			// and skip RecordBufferFlushRecords (inflating metrics).
			if b.flushRecordsAsync(task.ctx, task.bufferKey, task.database, task.measurement, task.records, task.recordCount) {
				// Notify VIEW manager that flush is complete
				if b.notifier != nil {
					b.notifier.OnFlushComplete(task.bufferKey)
				}
				// Record flush record count distribution
				trigger := task.trigger
				if trigger == "" {
					trigger = "size"
				}
				metrics.Get().RecordBufferFlushRecords(trigger, task.recordCount)
			}
			// Release timeout context resources
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

// AllBufferKeys returns all buffer keys across all shards.
// Used by ArrowViewManager for full-scan recovery when notifyCh overflows.
func (b *ArrowBuffer) AllBufferKeys() []string {
	var keys []string
	for i := uint32(0); i < b.shardCount; i++ {
		shard := b.shards[i]
		shard.mu.RLock()
		for key := range shard.buffers {
			keys = append(keys, key)
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
	for i := uint32(0); i < b.shardCount; i++ {
		shard := b.shards[i]
		shard.mu.Lock()
		var oldestKey string
		var oldestTime time.Time
		for key, entry := range shard.buffers {
			if oldestKey == "" || entry.startTime.Before(oldestTime) {
				oldestKey = key
				oldestTime = entry.startTime
			}
		}
		if oldestKey != "" && now.Sub(oldestTime) > 0 {
			// Extract batch references before deleting
			entry := shard.buffers[oldestKey]
			records := make([]interface{}, len(entry.batches))
			for j, batch := range entry.batches {
				records[j] = batch
			}
			recordCount := entry.recordCount
			delete(shard.buffers, oldestKey)
			parts := splitBufferKey(oldestKey)
			shard.mu.Unlock()

			if *inlineFlushCount < maxInlineFlushes && len(parts) == 2 {
				*inlineFlushCount++
				b.logger.Warn().
					Str("buffer_key", oldestKey).
					Dur("age", now.Sub(oldestTime)).
					Int("inline_flush", *inlineFlushCount).
					Msg("Buffer overflow: flushing oldest entry inline")
				b.flushRecordsAsync(context.Background(), oldestKey, parts[0], parts[1], records, recordCount)
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
			evicted = true
		} else {
			shard.mu.Unlock()
		}
		if evicted && b.estimateTotalBufferBytes() <= targetBytes {
			return true
		}
	}
	return evicted
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
func (b *ArrowBuffer) restoreBufferEntry(bufferKey string, batches []interface{}) {
	if len(batches) == 0 {
		return
	}

	shard := b.getShard(bufferKey)

	typedBatches := make([]*TypedColumnBatch, 0, len(batches))
	recordCount := 0
	estimatedBytes := uint64(0)

	for _, r := range batches {
		if batch, ok := r.(*TypedColumnBatch); ok {
			typedBatches = append(typedBatches, batch)
			batchRows := 0
			if len(batch.Data) > 0 {
				for _, col := range batch.Data {
					switch v := col.(type) {
					case []int64:
						batchRows = len(v)
					case []float64:
						batchRows = len(v)
					case []string:
						batchRows = len(v)
					case []bool:
						batchRows = len(v)
					}
					break
				}
			}
			recordCount += batchRows
			estimatedBytes += estimateBytesPerRow(batch) * uint64(batchRows)
		}
	}

	// Check buffer overflow BEFORE acquiring shard lock.
	// ensureBufferSpace scans all shards (RLock); must not be called
	// while holding a shard write lock or it will deadlock.
	if err := b.ensureBufferSpace(estimatedBytes); err != nil {
		// Hard limit exceeded even after eviction — flush inline as last resort.
		// Use a context value to prevent recursive restore if inline flush fails.
		b.logger.Warn().Err(err).
			Str("buffer_key", bufferKey).
			Int("records", recordCount).
			Msg("Buffer full, flushing restored data inline to avoid data loss")

		parts := splitBufferKey(bufferKey)
		if len(parts) == 2 {
			ctx := context.WithValue(context.Background(), ctxKeyRestoreFallback{}, true)
			b.flushRecordsAsync(ctx, bufferKey, parts[0], parts[1], batches, recordCount)
		}
		return
	}

	// Extract schema from the first batch so that type-conflict detection
	// (hasTypeConflict) works correctly for restored entries. Without this,
	// schema="" would short-circuit every check and never trigger a flush.
	var schema string
	if len(typedBatches) > 0 {
		if typedBatches[0].Signature != "" {
			schema = typedBatches[0].Signature
		} else if typedBatches[0].Data != nil {
			schema = getColumnSignature(typedBatches[0].Data)
		}
	}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Only restore if the bufferKey doesn't already have new data
	if _, exists := shard.buffers[bufferKey]; !exists {
		shard.buffers[bufferKey] = &bufferEntry{
			batches:        typedBatches,
			startTime:      time.Now(), // reset timer: retry on next adaptive cycle
			recordCount:    recordCount,
			estimatedBytes: estimatedBytes,
			schema:         schema,
		}
	}
}

// typedBatchToColumns converts a TypedColumnBatch's typed arrays back to
// map[string][]interface{} for re-ingestion via WriteColumnarDirectNoWAL.
func typedBatchToColumns(merged *TypedColumnBatch) map[string][]interface{} {
	columns := make(map[string][]interface{}, len(merged.Data))
	for colName, typedCol := range merged.Data {
		switch v := typedCol.(type) {
		case []int64:
			out := make([]interface{}, len(v))
			for i, val := range v {
				out[i] = val
			}
			columns[colName] = out
		case []float64:
			out := make([]interface{}, len(v))
			for i, val := range v {
				out[i] = val
			}
			columns[colName] = out
		case []string:
			out := make([]interface{}, len(v))
			for i, val := range v {
				out[i] = val
			}
			columns[colName] = out
		case []bool:
			out := make([]interface{}, len(v))
			for i, val := range v {
				out[i] = val
			}
			columns[colName] = out
		}
	}
	return columns
}

// writeBackMergedData re-ingests merged (TypedColumnBatch) data into the buffer
// after a flushPartitionedData failure. Uses WriteColumnarDirectNoWAL to skip
// double-WAL-writing — the data is already in WAL from the original ingest.
func (b *ArrowBuffer) writeBackMergedData(ctx context.Context, database, measurement string, merged *TypedColumnBatch) {
	// Estimate memory for the merged batch
	var estBytes uint64
	for _, col := range merged.Data {
		switch v := col.(type) {
		case []int64:
			estBytes += 8 * uint64(len(v))
		case []float64:
			estBytes += 8 * uint64(len(v))
		case []string:
			for _, s := range v {
				estBytes += uint64(len(s))
			}
		case []bool:
			estBytes += uint64(len(v))
		}
	}

	// Check buffer overflow before restoring
	if err := b.ensureBufferSpace(estBytes); err != nil {
		// Hard limit exceeded even after eviction — flush inline as last resort.
		// Use the recursion-guard context to prevent unbounded retry loops.
		b.logger.Warn().Err(err).
			Str("database", database).
			Str("measurement", measurement).
			Msg("Buffer full, flushing merged data inline to avoid data loss")

		// Build bufferKey and extract recordCount from the merged batch.
		sig := getColumnSignature(merged.Data)
		bufferKey := schemaKey(database, measurement, sig)
		recordCount := 0
		if times, ok := merged.Data["time"].([]int64); ok {
			recordCount = len(times)
		}
		records := []interface{}{merged}
		ctx := context.WithValue(context.Background(), ctxKeyRestoreFallback{}, true)
		b.flushRecordsAsync(ctx, bufferKey, database, measurement, records, recordCount)
		return
	}

	columns := typedBatchToColumns(merged)
	if err := b.WriteColumnarDirectNoWAL(ctx, database, measurement, columns); err != nil {
		b.logger.Error().Err(err).
			Str("database", database).
			Str("measurement", measurement).
			Msg("Failed to restore merged data after flush failure — data remains in WAL")
	}
}

func (b *ArrowBuffer) flushRecordsAsync(ctx context.Context, bufferKey, database, measurement string, records []interface{}, recordCount int) (flushed bool) {
	startTime := time.Now()

	// Merge typed column batches
	merged, err := b.mergeBatches(records)

	if err != nil {
		// When called from the restoreBufferEntry inline-flush fallback, skip
		// restore to prevent unbounded recursion. Data remains in WAL.
		if ctx.Value(ctxKeyRestoreFallback{}) != nil {
			b.logger.Error().Err(err).
				Str("buffer_key", bufferKey).
				Msg("CRITICAL: inline restore flush merge failed — data may be lost")
			return false
		}
		// mergeBatches failed — restore the original batch references back to buffer.
		// records has not been nilled yet, batch refs are still valid.
		b.restoreBufferEntry(bufferKey, records)
		b.logger.Error().
			Err(err).
			Str("buffer_key", bufferKey).
			Msg("Failed to merge batches — buffer entry restored, will retry")
		return false
	}

	// Only clear batch references AFTER confirming merge succeeded.
	// This keeps batch refs alive for restoreBufferEntry on merge failure.
	for i := range records {
		records[i] = nil
	}

	// Flush with data timestamp partitioning
	if err := b.flushWithDataTimePartitioning(ctx, bufferKey, database, measurement, merged, recordCount, startTime); err != nil {
		// When called from the restoreBufferEntry inline-flush fallback, skip
		// restore to prevent unbounded recursion. Data remains in WAL.
		if ctx.Value(ctxKeyRestoreFallback{}) != nil {
			b.logger.Error().Err(err).
				Str("buffer_key", bufferKey).
				Int("records", recordCount).
				Msg("CRITICAL: inline restore flush write failed — data may be lost")
			// Still write FLUSH_FAIL for audit trail
			if b.wal != nil {
				if cerr := b.wal.AppendControl(wal.FlushFail, database, measurement); cerr != nil {
					b.logger.Error().Err(cerr).Msg("Failed to write FLUSH_FAIL control record")
				}
			}
			return
		}
		// flushPartitionedData failed — put merged data back into buffer
		b.writeBackMergedData(ctx, database, measurement, merged)
		b.logger.Error().
			Err(err).
			Str("buffer_key", bufferKey).
			Int("records", recordCount).
			Msg("Failed to flush — merged data restored to buffer, will retry")

		// Write FLUSH_FAIL control record for audit trail
		if b.wal != nil {
			if cerr := b.wal.AppendControl(wal.FlushFail, database, measurement); cerr != nil {
				b.logger.Error().Err(cerr).Msg("Failed to write FLUSH_FAIL control record")
			}
		}
		return
	}

	// Flush succeeded — write FLUSH_OK to mark data as confirmed in Parquet
	if b.wal != nil {
		if cerr := b.wal.AppendControl(wal.FlushOK, database, measurement); cerr != nil {
			b.logger.Error().Err(cerr).Msg("Failed to write FLUSH_OK control record")
		}
	}
		return true
}

// flushWithDataTimePartitioning partitions data by data timestamps (async path)
func (b *ArrowBuffer) flushWithDataTimePartitioning(ctx context.Context, bufferKey, database, measurement string, merged *TypedColumnBatch, recordCount int, startTime time.Time) error {
	return b.flushPartitionedData(ctx, bufferKey, database, measurement, merged, recordCount, flushTypeAsync, startTime)
}

// flushPartitionedData is the shared core logic for partitioning and writing data by hour boundaries
// Called by both async (flushWithDataTimePartitioning) and sync (flushBufferLockedDataTime) paths
// Uses hash-based grouping to partition by hour, then sorts each hour independently
func (b *ArrowBuffer) flushPartitionedData(ctx context.Context, bufferKey, database, measurement string, merged *TypedColumnBatch, recordCount int, flushType string, startTime time.Time) error {
	// Get sort keys for this measurement (guaranteed to include "time")
	sortKeys := b.getSortKeys(measurement)

	// Get decimal column config for this measurement (nil if none configured)
	decimalCols := b.getDecimalColumns(measurement)

	// Extract time column (doesn't need to be sorted yet)
	times, ok := merged.Data["time"].([]int64)
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
		sorted := sortTypedColumnBatchByKeys(merged, sortKeys)

		parquetData, err := b.writer.WriteParquetColumnar(ctx, measurement, sorted.Data, sorted.Validity, sorted.TagColumns, decimalCols)
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

		b.totalRecordsWritten.Add(int64(recordCount))
		b.totalFlushes.Add(1)

		flushDuration := time.Since(startTime)
		msgType := getFlushMessageType(flushType)

		b.logger.Info().
			Str("buffer_key", bufferKey).
			Str("storage_path", storagePath).
			Int("records", recordCount).
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
		Int("total_records", recordCount).
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
		hourBatch := sliceTypedColumnBatchByIndices(merged, bucket.indices)

		// Sort this hour's data by configured sort keys
		sorted := sortTypedColumnBatchByKeys(hourBatch, sortKeys)

		// Write Parquet file for this hour
		parquetData, err := b.writer.WriteParquetColumnar(ctx, measurement, sorted.Data, sorted.Validity, sorted.TagColumns, decimalCols)
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
func (b *ArrowBuffer) flushBufferLocked(ctx context.Context, shard *bufferShard, bufferKey, database, measurement string) error {
	entry, exists := shard.buffers[bufferKey]
	if !exists || len(entry.batches) == 0 {
		// Clean up stale tracking entry even if buffer is empty
		delete(shard.buffers, bufferKey)
		// Notify VIEW manager after synchronous flush
		if b.notifier != nil {
			b.notifier.OnFlushComplete(bufferKey)
		}
		return nil
	}

	// Get record count before clearing buffer
	recordCount := entry.recordCount

	// Extract records to flush (hold lock for minimal time)
	recordsToFlush := make([]interface{}, len(entry.batches))
	for i, batch := range entry.batches {
		recordsToFlush[i] = batch
	}

	// Clear buffer immediately
	delete(shard.buffers, bufferKey)

	// Release lock before expensive operations
	shard.mu.Unlock()

	// Merge typed column batches
	merged, err := b.mergeBatches(recordsToFlush)
	if err != nil {
		// Restore original batch references back to buffer
		b.restoreBufferEntry(bufferKey, recordsToFlush)
		shard.mu.Lock() // Re-acquire lock for caller
		return fmt.Errorf("failed to merge batches: %w", err)
	}

	// Flush with data timestamp partitioning
	startTime := time.Now().UTC()
	if err := b.flushBufferLockedDataTime(ctx, bufferKey, database, measurement, merged, recordCount, startTime); err != nil {
		// Put merged data back into buffer
		b.writeBackMergedData(ctx, database, measurement, merged)
		b.logger.Warn().
			Err(err).
			Str("buffer_key", bufferKey).
			Int("records", recordCount).
			Msg("Flush failed — merged data restored to buffer, will retry")

		// Write FLUSH_FAIL control record for audit trail
		if b.wal != nil {
			if cerr := b.wal.AppendControl(wal.FlushFail, database, measurement); cerr != nil {
				b.logger.Error().Err(cerr).Msg("Failed to write FLUSH_FAIL control record")
			}
		}
		shard.mu.Lock() // Re-acquire lock for caller
		return err
	}

	// Flush succeeded — write FLUSH_OK to mark data as confirmed in Parquet
	if b.wal != nil {
		if cerr := b.wal.AppendControl(wal.FlushOK, database, measurement); cerr != nil {
			b.logger.Error().Err(cerr).Msg("Failed to write FLUSH_OK control record")
		}
	}

	// Notify VIEW manager after synchronous flush
	if b.notifier != nil {
		b.notifier.OnFlushComplete(bufferKey)
	}

	// Record flush record count distribution
	metrics.Get().RecordBufferFlushRecords("age", recordCount)

	// Re-acquire lock for caller
	shard.mu.Lock()
	return nil
}

// flushBufferLockedDataTime flushes with data_time partitioning (sync path)
func (b *ArrowBuffer) flushBufferLockedDataTime(ctx context.Context, bufferKey, database, measurement string, merged *TypedColumnBatch, recordCount int, startTime time.Time) error {
	return b.flushPartitionedData(ctx, bufferKey, database, measurement, merged, recordCount, flushTypeSync, startTime)
}

// mergeBatches merges multiple column batches into a single TypedColumnBatch.
// OPTIMIZATION: Pre-allocate merged arrays to avoid O(n²) append reallocations
// Handles sparse columns (schema evolution) by marking missing positions as null via validity bitmaps.
func (b *ArrowBuffer) mergeBatches(batches []interface{}) (*TypedColumnBatch, error) {
	if len(batches) == 0 {
		return nil, fmt.Errorf("no batches to merge")
	}

	// If only one batch, return it directly
	if len(batches) == 1 {
		if tcb, ok := batches[0].(*TypedColumnBatch); ok {
			return tcb, nil
		}
		// Legacy: bare map without validity (e.g. from WAL replay)
		if cols, ok := batches[0].(map[string]interface{}); ok {
			return &TypedColumnBatch{Data: cols, Validity: nil}, nil
		}
		return nil, fmt.Errorf("invalid batch type: %T", batches[0])
	}

	// PHASE 1: Calculate total rows from time column and collect column type info
	type colInfo struct {
		colType string // "int64", "float64", "string", "bool", "decimal128"
	}
	colTypes := make(map[string]colInfo)
	totalRows := 0

	// Track which columns have validity bitmaps and which batches have which columns
	hasAnyValidity := false

	// Union of tag columns across all batches (for Parquet metadata)
	tagColumnSet := make(map[string]struct{})

	// First pass: count total rows using time column
	for _, batch := range batches {
		var cols map[string]interface{}
		switch b := batch.(type) {
		case *TypedColumnBatch:
			cols = b.Data
			if len(b.Validity) > 0 {
				hasAnyValidity = true
			}
			for _, tag := range b.TagColumns {
				tagColumnSet[tag] = struct{}{}
			}
		case map[string]interface{}:
			cols = b
		default:
			return nil, fmt.Errorf("invalid batch type: %T", batch)
		}

		// Count rows from time column (always present)
		if timeCol, ok := cols["time"].([]int64); ok {
			totalRows += len(timeCol)
		}

		// Collect column types
		for name, col := range cols {
			if _, seen := colTypes[name]; !seen {
				var ct string
				switch col.(type) {
				case []int64:
					ct = "int64"
				case []float64:
					ct = "float64"
				case []string:
					ct = "string"
				case []bool:
					ct = "bool"
				case []decimal128.Num:
					ct = "decimal128"
				default:
					return nil, fmt.Errorf("unsupported column type: %T", col)
				}
				colTypes[name] = colInfo{colType: ct}
			}
		}
	}

	// Determine if we need validity tracking.
	// Needed if: any batch already has validity, OR columns are sparse across batches.
	// Check sparsity: if any batch doesn't have all columns, those positions are null.
	hasSparseColumns := false
	for _, batch := range batches {
		var cols map[string]interface{}
		switch b := batch.(type) {
		case *TypedColumnBatch:
			cols = b.Data
		case map[string]interface{}:
			cols = b
		}
		if len(cols) < len(colTypes) {
			hasSparseColumns = true
			break
		}
	}
	needsValidity := hasAnyValidity || hasSparseColumns

	// PHASE 2: Pre-allocate ALL columns to totalRows (handles sparse columns)
	merged := make(map[string]interface{}, len(colTypes))
	var mergedValidity map[string][]bool
	if needsValidity {
		mergedValidity = make(map[string][]bool, len(colTypes))
	}

	for name, info := range colTypes {
		switch info.colType {
		case "int64":
			merged[name] = make([]int64, totalRows)
		case "float64":
			merged[name] = make([]float64, totalRows)
		case "string":
			merged[name] = make([]string, totalRows)
		case "bool":
			merged[name] = make([]bool, totalRows)
		case "decimal128":
			merged[name] = make([]decimal128.Num, totalRows)
		}
		if needsValidity {
			// Initialize all positions as invalid (null). Positions with data get set to true below.
			mergedValidity[name] = make([]bool, totalRows)
		}
	}

	// PHASE 3: Copy data at correct row offsets (not per-column offsets)
	rowOffset := 0
	for _, batch := range batches {
		var cols map[string]interface{}
		var batchValidity map[string][]bool
		switch b := batch.(type) {
		case *TypedColumnBatch:
			cols = b.Data
			batchValidity = b.Validity
		case map[string]interface{}:
			cols = b
		}

		// Determine batch size from time column
		batchRows := 0
		if timeCol, ok := cols["time"].([]int64); ok {
			batchRows = len(timeCol)
		}

		// Copy each column's data at the current row offset
		for name, col := range cols {
			switch v := col.(type) {
			case []int64:
				copy(merged[name].([]int64)[rowOffset:], v)
			case []float64:
				copy(merged[name].([]float64)[rowOffset:], v)
			case []string:
				copy(merged[name].([]string)[rowOffset:], v)
			case []bool:
				copy(merged[name].([]bool)[rowOffset:], v)
			case []decimal128.Num:
				copy(merged[name].([]decimal128.Num)[rowOffset:], v)
			}

			// Copy validity bitmap for this column
			if needsValidity {
				dest := mergedValidity[name][rowOffset : rowOffset+batchRows]
				if batchValidity != nil {
					srcValid, ok := batchValidity[name]
					if ok && srcValid != nil {
						// Batch has explicit validity for this column — copy it
						copy(dest, srcValid)
					} else {
						// Either the column has no validity entry, or entry is nil
						// (contract: nil entry = all valid). Either way: all valid.
						for i := range dest {
							dest[i] = true
						}
					}
				} else {
					// Batch has no validity tracking at all → all valid
					for i := range dest {
						dest[i] = true
					}
				}
			}
		}
		// Sparse columns that don't exist in this batch keep validity=false (null)
		// at positions [rowOffset : rowOffset+batchRows] — no action needed.

		rowOffset += batchRows
	}

	// Optimization: strip validity entries that are all-true (no nulls)
	if mergedValidity != nil {
		for name, valid := range mergedValidity {
			allValid := true
			for _, v := range valid {
				if !v {
					allValid = false
					break
				}
			}
			if allValid {
				delete(mergedValidity, name)
			}
		}
		if len(mergedValidity) == 0 {
			mergedValidity = nil
		}
	}

	// Collect merged tag columns
	var mergedTagColumns []string
	if len(tagColumnSet) > 0 {
		mergedTagColumns = make([]string, 0, len(tagColumnSet))
		for tag := range tagColumnSet {
			mergedTagColumns = append(mergedTagColumns, tag)
		}
	}

	return &TypedColumnBatch{Data: merged, Validity: mergedValidity, TagColumns: mergedTagColumns}, nil
}

// sortColumnsByTime sorts all columns by the time column in-place
// Returns the sorted columns and any error encountered
func sortColumnsByTime(columns map[string]interface{}) (map[string]interface{}, error) {
	// Delegate to multi-key sort with just "time" key
	return sortColumnsByKeys(columns, []string{"time"})
}

// sortColumnsByKeys sorts columns by multiple keys (e.g., sensor_id, then time)
// Returns the sorted columns and any error encountered
func sortColumnsByKeys(columns map[string]interface{}, sortKeys []string) (map[string]interface{}, error) {
	sorted, _, err := sortColumnsByKeysWithPermutation(columns, sortKeys)
	return sorted, err
}

// sortColumnsByKeysWithPermutation sorts columns and returns the permutation indices used.
// The permutation can be applied to validity bitmaps or other parallel arrays by the caller,
// avoiding a second sort pass.
func sortColumnsByKeysWithPermutation(columns map[string]interface{}, sortKeys []string) (map[string]interface{}, []int, error) {
	if len(sortKeys) == 0 {
		return nil, nil, fmt.Errorf("no sort keys provided")
	}

	// FAST PATH: Time-only sort (most common case) - avoid multi-key overhead
	if len(sortKeys) == 1 && sortKeys[0] == "time" {
		sorted, indices, err := sortColumnsByTimeOnlyWithPermutation(columns)
		return sorted, indices, err
	}

	// Validate all sort keys exist and cache column pointers
	cachedCols := make([]interface{}, len(sortKeys))
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
		switch c := col.(type) {
		case []int64:
			n = len(c)
		case []float64:
			n = len(c)
		case []string:
			n = len(c)
		case []bool:
			n = len(c)
		case []decimal128.Num:
			n = len(c)
		}
		if n > 0 {
			break
		}
	}

	if n == 0 {
		return columns, nil, nil
	}

	// Create permutation indices [0, 1, 2, ..., n-1]
	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}

	// Multi-key sort with cached columns (no map lookups in comparator)
	sort.Slice(indices, func(i, j int) bool {
		return compareMultiKeyCached(cachedCols, indices[i], indices[j])
	})

	// Apply permutation to all columns
	result := make(map[string]interface{}, len(columns))
	for colName, colData := range columns {
		result[colName] = applyPermutation(colData, indices)
	}

	return result, indices, nil
}

// sortColumnsByTimeOnly is an optimized path for time-only sorting.
// Avoids the multi-key comparator overhead for the common case.
func sortColumnsByTimeOnly(columns map[string]interface{}) (map[string]interface{}, error) {
	sorted, _, err := sortColumnsByTimeOnlyWithPermutation(columns)
	return sorted, err
}

// sortColumnsByTimeOnlyWithPermutation sorts by time and returns the permutation used.
// Returns nil indices when data is already sorted (no permutation needed).
func sortColumnsByTimeOnlyWithPermutation(columns map[string]interface{}) (map[string]interface{}, []int, error) {
	timeCol, exists := columns["time"]
	if !exists {
		return nil, nil, fmt.Errorf("time column not found")
	}

	times, ok := timeCol.([]int64)
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
	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}

	// Sort by time directly (no function call overhead per comparison)
	sort.Slice(indices, func(i, j int) bool {
		return times[indices[i]] < times[indices[j]]
	})

	// Apply permutation to all columns
	result := make(map[string]interface{}, len(columns))
	for colName, colData := range columns {
		result[colName] = applyPermutation(colData, indices)
	}

	return result, indices, nil
}

// compareMultiKeyCached compares two rows by multiple sort keys using cached column pointers
// This avoids map lookups on every comparison (called O(n log n) times)
func compareMultiKeyCached(cachedCols []interface{}, i, j int) bool {
	for _, col := range cachedCols {
		switch c := col.(type) {
		case []int64:
			if c[i] < c[j] {
				return true
			}
			if c[i] > c[j] {
				return false
			}
			// Equal, continue to next key

		case []float64:
			if c[i] < c[j] {
				return true
			}
			if c[i] > c[j] {
				return false
			}

		case []string:
			if c[i] < c[j] {
				return true
			}
			if c[i] > c[j] {
				return false
			}

		case []bool:
			if !c[i] && c[j] { // false < true
				return true
			}
			if c[i] && !c[j] {
				return false
			}

		case []decimal128.Num:
			if c[i].Less(c[j]) {
				return true
			}
			if c[i].Greater(c[j]) {
				return false
			}
		}
	}

	// All keys equal
	return false
}

// applyPermutation reorders a column according to permutation indices
func applyPermutation(colData interface{}, indices []int) interface{} {
	switch col := colData.(type) {
	case []int64:
		result := make([]int64, len(indices))
		for i, idx := range indices {
			result[i] = col[idx]
		}
		return result

	case []float64:
		result := make([]float64, len(indices))
		for i, idx := range indices {
			result[i] = col[idx]
		}
		return result

	case []string:
		result := make([]string, len(indices))
		for i, idx := range indices {
			result[i] = col[idx]
		}
		return result

	case []bool:
		result := make([]bool, len(indices))
		for i, idx := range indices {
			result[i] = col[idx]
		}
		return result

	case []decimal128.Num:
		result := make([]decimal128.Num, len(indices))
		for i, idx := range indices {
			result[i] = col[idx]
		}
		return result

	default:
		return colData // Unknown type, return as-is
	}
}

// sortTypedColumnBatchByKeys sorts a TypedColumnBatch by the given keys,
// keeping validity bitmaps aligned with the reordered data.
// Uses the permutation returned by sortColumnsByKeysWithPermutation to avoid
// a second sort pass when validity bitmaps need reordering.
func sortTypedColumnBatchByKeys(batch *TypedColumnBatch, sortKeys []string) *TypedColumnBatch {
	sorted, indices, err := sortColumnsByKeysWithPermutation(batch.Data, sortKeys)
	if err != nil {
		return batch
	}

	// nil indices means data was already sorted — no permutation needed
	if indices == nil || batch.Validity == nil {
		return &TypedColumnBatch{Data: sorted, Validity: batch.Validity, TagColumns: batch.TagColumns, Signature: batch.Signature}
	}

	// Apply the same permutation to validity bitmaps (no second sort)
	sortedValidity := make(map[string][]bool, len(batch.Validity))
	for name, valid := range batch.Validity {
		// Per TypedColumnBatch contract, a nil entry means "all valid" — preserve as nil.
		if valid == nil {
			sortedValidity[name] = nil
			continue
		}
		newValid := make([]bool, len(indices))
		for i, idx := range indices {
			newValid[i] = valid[idx]
		}
		sortedValidity[name] = newValid
	}

	return &TypedColumnBatch{Data: sorted, Validity: sortedValidity, TagColumns: batch.TagColumns, Signature: batch.Signature}
}

// sliceTypedColumnBatchByIndices extracts rows from a TypedColumnBatch by index list,
// keeping validity bitmaps aligned.
func sliceTypedColumnBatchByIndices(batch *TypedColumnBatch, indices []int) *TypedColumnBatch {
	slicedData := sliceColumnsByIndices(batch.Data, indices)

	if batch.Validity == nil {
		return &TypedColumnBatch{Data: slicedData, Validity: nil, TagColumns: batch.TagColumns, Signature: batch.Signature}
	}

	slicedValidity := make(map[string][]bool, len(batch.Validity))
	for name, valid := range batch.Validity {
		// Per TypedColumnBatch contract, a nil entry means "all valid" — preserve as nil.
		if valid == nil {
			slicedValidity[name] = nil
			continue
		}
		newValid := make([]bool, len(indices))
		validLen := len(valid)
		for i, idx := range indices {
			if idx < validLen {
				newValid[i] = valid[idx]
			}
			// else: out-of-bounds stays false (null)
		}
		slicedValidity[name] = newValid
	}

	return &TypedColumnBatch{Data: slicedData, Validity: slicedValidity, TagColumns: batch.TagColumns, Signature: batch.Signature}
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

// sliceColumnsByIndices extracts rows from all columns based on a list of indices
// Returns a new column map with only the selected rows
// Handles sparse columns (columns shorter than indices) by using zero values for out-of-bounds access
func sliceColumnsByIndices(columns map[string]interface{}, indices []int) map[string]interface{} {
	result := make(map[string]interface{}, len(columns))

	for colName, colData := range columns {
		switch col := colData.(type) {
		case []int64:
			newCol := make([]int64, len(indices))
			colLen := len(col)
			for i, idx := range indices {
				if idx < colLen {
					newCol[i] = col[idx]
				}
				// else: leave as zero value (sparse column handling)
			}
			result[colName] = newCol

		case []float64:
			newCol := make([]float64, len(indices))
			colLen := len(col)
			for i, idx := range indices {
				if idx < colLen {
					newCol[i] = col[idx]
				}
				// else: leave as zero value (sparse column handling)
			}
			result[colName] = newCol

		case []string:
			newCol := make([]string, len(indices))
			colLen := len(col)
			for i, idx := range indices {
				if idx < colLen {
					newCol[i] = col[idx]
				}
				// else: leave as empty string (sparse column handling)
			}
			result[colName] = newCol

		case []bool:
			newCol := make([]bool, len(indices))
			colLen := len(col)
			for i, idx := range indices {
				if idx < colLen {
					newCol[i] = col[idx]
				}
				// else: leave as false (sparse column handling)
			}
			result[colName] = newCol

		case []decimal128.Num:
			newCol := make([]decimal128.Num, len(indices))
			colLen := len(col)
			for i, idx := range indices {
				if idx < colLen {
					newCol[i] = col[idx]
				}
			}
			result[colName] = newCol

		default:
			// Unknown type, copy as-is
			result[colName] = colData
		}
	}

	return result
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
		for key := range shard.buffers {
			keys = append(keys, key)
		}

		for _, key := range keys {
			parts := splitBufferKey(key)
			if len(parts) != 2 {
				b.logger.Error().Str("buffer_key", key).Msg("Invalid buffer key format during flush")
				continue
			}

			if err := b.flushBufferLocked(ctx, shard, key, parts[0], parts[1]); err != nil {
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
	// Stop periodic flush
	b.cancel()
	b.flushTimer.Stop()

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
		for key := range shard.buffers {
			keys = append(keys, key)
		}

		for _, key := range keys {
			parts := splitBufferKey(key)
			if len(parts) != 2 {
				b.logger.Error().Str("buffer_key", key).Msg("Invalid buffer key format during close")
				continue
			}

			flushCtx, flushCancel := context.WithTimeout(context.Background(), b.flushTimeout)
			if err := b.flushBufferLocked(flushCtx, shard, key, parts[0], parts[1]); err != nil {
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
		"total_records_buffered":      b.totalRecordsBuffered.Load(),
		"total_records_written":       b.totalRecordsWritten.Load(),
		"total_flushes":               b.totalFlushes.Load(),
		"total_errors":                b.totalErrors.Load(),
		"total_wal_errors":            b.totalWALErrors.Load(),
		"total_wal_dropped":           b.totalWALDropped.Load(),
		"total_schema_churn_exceeded": b.totalSchemaChurnExceeded.Load(),
		"active_buffers":              activeBuffers,
		"flush_queue_depth":           b.queueDepth.Load(),
		"flush_workers":               b.flushWorkers,
	}
}

// SetNotifier 设置缓冲变更通知回调。
// 由 database 包在启动时注入 ArrowViewManager。
func (b *ArrowBuffer) SetNotifier(n BufferChangeNotifier) {
	b.notifier = n
}

// SinceRefresh 返回指定 bufferKey 自上次 VIEW 刷新以来新增的 batch。
func (b *ArrowBuffer) SinceRefresh(bufferKey string) ([]*TypedColumnBatch, error) {
	var result []*TypedColumnBatch
	for i := uint32(0); i < b.shardCount; i++ {
		shard := b.shards[i]
		shard.mu.RLock()
		entry, ok := shard.buffers[bufferKey]
		if ok && entry.refreshIndex < len(entry.batches) {
			for _, b := range entry.batches[entry.refreshIndex:] {
				result = append(result, b)
			}
		}
		shard.mu.RUnlock()
	}
	return result, nil
}

// MarkRefreshed 更新指定 bufferKey 的刷新游标。
func (b *ArrowBuffer) MarkRefreshed(bufferKey string) {
	for i := uint32(0); i < b.shardCount; i++ {
		shard := b.shards[i]
		shard.mu.Lock()
		if entry, ok := shard.buffers[bufferKey]; ok {
			entry.refreshIndex = len(entry.batches)
		}
		shard.mu.Unlock()
	}
}

// TotalBufferedRecords 返回指定 bufferKey 的总缓冲记录数。
func (b *ArrowBuffer) TotalBufferedRecords(bufferKey string) int {
	total := 0
	for i := uint32(0); i < b.shardCount; i++ {
		shard := b.shards[i]
		shard.mu.RLock()
		if entry, ok := shard.buffers[bufferKey]; ok {
			total += entry.recordCount
		}
		shard.mu.RUnlock()
	}
	return total
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
