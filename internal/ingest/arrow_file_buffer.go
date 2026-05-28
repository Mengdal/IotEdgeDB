package ingest

import (
	"context"
	"os"
	"sync"
	"sync/atomic"

	"iedb/internal/config"
	"iedb/internal/storage"
	"iedb/internal/tiering"

	"github.com/apache/arrow-go/v18/arrow"
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
