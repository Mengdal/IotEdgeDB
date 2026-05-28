package ingest

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"iedb/internal/config"
	"iedb/internal/storage"
	"iedb/internal/tiering"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/rs/zerolog"
)

// fileEntry holds the state for one measurement's active .arrow buffer file.
type fileEntry struct {
	file        *os.File      // open file handle, nil if not yet opened
	path        string        // full path to .arrow file in tmpDir
	size        int64         // bytes written so far
	schema      *arrow.Schema // union schema — grows to include all seen columns
	database    string
	measurement string
}

// fileShard holds a map of active file entries with its own lock.
type fileShard struct {
	entries map[string]*fileEntry // key: "database/measurement"
	mu      sync.RWMutex
}

// convertTask describes a file ready for Arrow -> Parquet conversion.
type convertTask struct {
	database    string
	measurement string
	path        string
}

// ArrowFileBuffer manages per-measurement Arrow IPC stream files as ingest buffers.
// Replaces the old heap-memory ArrowBuffer + WAL architecture.
type ArrowFileBuffer struct {
	config  *config.IngestConfig
	storage storage.Backend
	writer  *ArrowWriter

	shards     []*fileShard
	shardCount uint32

	maxFileSize  int64
	tmpDir       string
	convertQueue chan convertTask

	sortKeysConfig       map[string][]string
	defaultSortKeys      []string
	decimalConfig        map[string]map[string]config.DecimalSpec
	defaultDecimalConfig map[string]config.DecimalSpec

	tieringManager *tiering.Manager

	totalRecordsBuffered atomic.Int64
	totalRecordsWritten  atomic.Int64
	totalFlushes         atomic.Int64
	totalErrors          atomic.Int64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	logger zerolog.Logger
}

// NewArrowFileBuffer creates a new Arrow File Buffer.
func NewArrowFileBuffer(cfg *config.IngestConfig, storageBackend storage.Backend, logger zerolog.Logger) *ArrowFileBuffer {
	ctx, cancel := context.WithCancel(context.Background())

	shardCount := cfg.ShardCount
	if shardCount <= 0 {
		shardCount = 32
	}

	maxFileSize := int64(cfg.BufferFileSizeMB) * 1024 * 1024
	if maxFileSize <= 0 {
		maxFileSize = 10 * 1024 * 1024
	}

	tmpDir := cfg.BufferTmpDir
	if tmpDir == "" {
		tmpDir = "/tmp/iedb_buf"
	}

	sortKeysConfig, defaultSortKeys, err := config.ParseSortKeys(*cfg)
	if err != nil {
		logger.Warn().Err(err).Msg("Invalid sort keys config, using defaults")
		sortKeysConfig = make(map[string][]string)
		defaultSortKeys = []string{"time"}
	}

	decimalConfig, defaultDecimalConfig, err := config.ParseDecimalColumns(*cfg)
	if err != nil {
		logger.Warn().Err(err).Msg("Invalid decimal columns config, decimal support disabled")
		decimalConfig = make(map[string]map[string]config.DecimalSpec)
		defaultDecimalConfig = nil
	}

	buf := &ArrowFileBuffer{
		config:               cfg,
		storage:              storageBackend,
		writer:               NewArrowWriter(cfg, logger),
		shards:               make([]*fileShard, shardCount),
		shardCount:           uint32(shardCount),
		maxFileSize:          maxFileSize,
		tmpDir:               tmpDir,
		convertQueue:         make(chan convertTask, 16),
		sortKeysConfig:       sortKeysConfig,
		defaultSortKeys:      defaultSortKeys,
		decimalConfig:        decimalConfig,
		defaultDecimalConfig: defaultDecimalConfig,
		ctx:                  ctx,
		cancel:               cancel,
		logger:               logger.With().Str("component", "arrow-file-buffer").Logger(),
	}

	for i := 0; i < shardCount; i++ {
		buf.shards[i] = &fileShard{
			entries: make(map[string]*fileEntry),
		}
	}

	// Create tmpdir
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		logger.Fatal().Err(err).Str("tmp_dir", tmpDir).Msg("Failed to create buffer tmp dir")
	}

	// Run crash recovery before starting normal operations
	buf.startupRecovery()

	// Start background convert worker
	buf.wg.Add(1)
	go buf.convertWorker()

	buf.logger.Info().
		Int64("max_file_size_bytes", maxFileSize).
		Str("tmp_dir", tmpDir).
		Int("shards", shardCount).
		Msg("ArrowFileBuffer initialized")

	return buf
}

// getShard returns the shard for a given buffer key using FNV-1a hash.
func (b *ArrowFileBuffer) getShard(bufferKey string) *fileShard {
	hash := uint32(2166136261)
	for i := 0; i < len(bufferKey); i++ {
		hash ^= uint32(bufferKey[i])
		hash *= 16777619
	}
	return b.shards[hash%b.shardCount]
}

// SetTieringManager sets the tiering manager for automatic file registration.
func (b *ArrowFileBuffer) SetTieringManager(tm *tiering.Manager) {
	b.tieringManager = tm
	b.logger.Info().Msg("Tiering manager enabled for ArrowFileBuffer")
}

// startupRecovery scans tmpDir for orphaned .arrow files and converts them.
// Stub — full implementation in Task 7.
func (b *ArrowFileBuffer) startupRecovery() {
	// TODO: Implement in Task 7
}

// convertWorker is the background goroutine for Arrow -> Parquet conversion.
// Stub — full implementation in Task 5.
func (b *ArrowFileBuffer) convertWorker() {
	defer b.wg.Done()
	b.logger.Info().Msg("Convert worker stub started")
	<-b.ctx.Done()
}

// encodeToArrowIPC encodes a TypedColumnBatch as Arrow IPC stream format bytes.
// Each call produces one record batch in the stream. The stream has no footer
// so O(1) append — each batch is self-describing with its own schema.
// New columns not in the current union schema cause the schema to be extended.
func encodeToArrowIPC(batch *TypedColumnBatch, unionSchema **arrow.Schema) ([]byte, error) {
	// Build column arrays and fields from typed batch data
	colNames := make([]string, 0, len(batch.Data))
	for name := range batch.Data {
		colNames = append(colNames, name)
	}
	sort.Strings(colNames)

	var fields []arrow.Field
	var arrays []arrow.Array

	for _, colName := range colNames {
		colData := batch.Data[colName]
		validity := batch.Validity[colName]

		field, arr, err := buildSingleArrowArrayStandalone(colName, colData, validity)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", colName, err)
		}
		fields = append(fields, field)
		arrays = append(arrays, arr)
	}

	// Determine num rows from the first array
	numRows := 0
	if len(arrays) > 0 {
		numRows = arrays[0].Len()
	}

	schema := arrow.NewSchema(fields, nil)
	rec := array.NewRecord(schema, arrays, int64(numRows))
	defer rec.Release()

	// Release arrays after record takes ownership
	for _, arr := range arrays {
		arr.Release()
	}

	// Encode as Arrow IPC stream
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := writer.Write(rec); err != nil {
		return nil, fmt.Errorf("ipc write: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("ipc close: %w", err)
	}

	// Extend union schema with any new columns
	if unionSchema != nil && *unionSchema == nil {
		*unionSchema = schema
	}

	return buf.Bytes(), nil
}

// getDecimalColumns returns the decimal column config for a measurement.
// Falls back to default config if no measurement-specific config exists.
func (b *ArrowFileBuffer) getDecimalColumns(measurement string) map[string]config.DecimalSpec {
	if b.decimalConfig != nil {
		if cfg, ok := b.decimalConfig[measurement]; ok {
			return cfg
		}
	}
	return b.defaultDecimalConfig
}

// tryInt64ZeroCopy attempts zero-copy conversion for int64 arrays.
func (b *ArrowFileBuffer) tryInt64ZeroCopy(col []interface{}) ([]int64, bool) {
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

// tryFloat64ZeroCopy attempts zero-copy conversion for float64 arrays.
func (b *ArrowFileBuffer) tryFloat64ZeroCopy(col []interface{}) ([]float64, bool) {
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

// tryStringZeroCopy attempts zero-copy conversion for string arrays.
func (b *ArrowFileBuffer) tryStringZeroCopy(col []interface{}) ([]string, bool) {
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

// tryBoolZeroCopy attempts zero-copy conversion for bool arrays.
func (b *ArrowFileBuffer) tryBoolZeroCopy(col []interface{}) ([]bool, bool) {
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

// convertColumnsToTyped converts []interface{} columns to typed arrays with null tracking.
// Returns a TypedColumnBatch where Validity maps track which values are null (false=null).
// Columns with no nil values have no entry in Validity (all valid).
// ZERO-COPY OPTIMIZATION: Try bulk type assertion first before element-by-element conversion.
func (b *ArrowFileBuffer) convertColumnsToTyped(measurement string, columns map[string][]interface{}) (*TypedColumnBatch, int, error) {
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

	batch := &TypedColumnBatch{Data: typed, Validity: validity}
	return batch, numRecords, nil
}

// sanitizePathSegment replaces characters unsafe for filenames.
func sanitizePathSegment(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '/' || r == '\\' || r == ':' || r == '*' || r == '?' || r == '"' || r == '<' || r == '>' || r == '|' {
			return '_'
		}
		return r
	}, s)
}

// writeBatch encodes a TypedColumnBatch as Arrow IPC stream bytes, appends to the
// measurement's .arrow file, and checks if the file exceeds the size threshold.
// If the file is over maxFileSize, it closes the file and submits a convert task.
func (b *ArrowFileBuffer) writeBatch(ctx context.Context, database, measurement string, typedColumns *TypedColumnBatch, numRecords int) error {
	bufferKey := database + "/" + measurement

	shard := b.getShard(bufferKey)
	shard.mu.Lock()

	entry, exists := shard.entries[bufferKey]
	if !exists {
		path := filepath.Join(b.tmpDir, fmt.Sprintf("%s_%s_%d.arrow",
			sanitizePathSegment(database), sanitizePathSegment(measurement), time.Now().UnixNano()))
		f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			shard.mu.Unlock()
			return fmt.Errorf("open arrow file: %w", err)
		}
		entry = &fileEntry{
			file:        f,
			path:        path,
			size:        0,
			schema:      nil,
			database:    database,
			measurement: measurement,
		}
		shard.entries[bufferKey] = entry
	}

	// Encode to Arrow IPC stream bytes
	batchBytes, err := encodeToArrowIPC(typedColumns, &entry.schema)
	if err != nil {
		shard.mu.Unlock()
		return fmt.Errorf("encode: %w", err)
	}

	// Write to file
	n, err := entry.file.Write(batchBytes)
	if err != nil {
		shard.mu.Unlock()
		return fmt.Errorf("file write: %w", err)
	}
	entry.size += int64(n)

	var shouldFlush bool
	var flushPath string
	var flushDB, flushMeas string

	if entry.size >= b.maxFileSize {
		flushPath = entry.path
		flushDB = entry.database
		flushMeas = entry.measurement
		shouldFlush = true
		delete(shard.entries, bufferKey)
		entry.file.Close()
	}

	shard.mu.Unlock()

	b.totalRecordsBuffered.Add(int64(numRecords))

	if shouldFlush {
		b.submitConvert(flushDB, flushMeas, flushPath)
	}

	return nil
}

// WriteTypedColumnarDirect writes a pre-typed column batch to the file buffer.
func (b *ArrowFileBuffer) WriteTypedColumnarDirect(ctx context.Context, database, measurement string, batch *TypedColumnBatch, numRecords int) error {
	return b.writeBatch(ctx, database, measurement, batch, numRecords)
}

// WriteColumnarDirect writes columnar data through the file buffer.
func (b *ArrowFileBuffer) WriteColumnarDirect(ctx context.Context, database, measurement string, columns map[string][]interface{}) error {
	typedColumns, numRecords, err := b.convertColumnsToTyped(measurement, columns)
	if err != nil {
		return fmt.Errorf("convert columns: %w", err)
	}
	return b.writeBatch(ctx, database, measurement, typedColumns, numRecords)
}

// submitConvert sends a convert task to the background worker.
// Non-blocking — if the queue is full, logs warning.
func (b *ArrowFileBuffer) submitConvert(database, measurement, path string) {
	task := convertTask{
		database:    database,
		measurement: measurement,
		path:        path,
	}
	select {
	case b.convertQueue <- task:
	default:
		b.logger.Warn().
			Str("database", database).
			Str("measurement", measurement).
			Str("path", path).
			Msg("Convert queue full, dropping task")
	}
}
