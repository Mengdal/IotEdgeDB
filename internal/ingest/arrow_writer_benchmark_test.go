package ingest

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"iedb/internal/config"

	"github.com/rs/zerolog"
)

// benchBufferHardLimit is a safety net preventing unbounded buffer growth
// during benchmarks.  It is set high enough that a normal short-duration
// benchmark run never hits it (normal peak per-benchmark is ~15 MB),
// and low enough to stay well below the OOM threshold even on 256 MB systems.
// On an ARM system with 500 MB this leaves ~370 MB for the OS, Go runtime,
// and DuckDB — comfortable headroom.
const benchBufferHardLimit = 128 << 20 // 128 MB

// ---------------------------------------------------------------------------
// Benchmark: bufferEntry access vs separate maps (the structural win)
// ---------------------------------------------------------------------------

func BenchmarkBufferEntry_SingleLookup(b *testing.B) {
	// Simulate the key operation: read count, bytes, age from a bufferEntry
	shards := make([]*bufferShard, 1)
	shards[0] = &bufferShard{buffers: make(map[string]*bufferEntry)}
	shards[0].buffers["db/test"] = &bufferEntry{
		columns:        map[string]ColumnData{"time": {Data: make([]int64, 5000)}},
		startTime:      time.Now().Add(-30 * time.Second),
		recordCount:    5000,
		estimatedBytes: 500000,
		schema:         "time:i64,value:f64",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry, ok := shards[0].buffers["db/test"]
		if ok {
			_ = entry.recordCount
			_ = entry.estimatedBytes
			_ = time.Since(entry.startTime)
		}
	}
}

func BenchmarkBufferEntry_WritePath(b *testing.B) {
	// Simulate the write-path operation with realistic entry lifecycle:
	// writes accumulate until threshold, then entry is reset (simulating flush).
	const batchSize = 100
	const flushThreshold = 1000 // reset entry every 10 batches

	batch := &bufferEntry{
		columns: map[string]ColumnData{
			"time":  {Data: make([]int64, batchSize)},
			"value": {Data: make([]float64, batchSize)},
			"host":  {Data: make([]string, batchSize)},
		},
	}

	entry := &bufferEntry{columns: make(map[string]ColumnData), startTime: time.Now()}
	itersSinceReset := 0

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if itersSinceReset >= flushThreshold {
			// Simulate flush: extract data, create wrapper (zero-copy)
			_ = &bufferEntry{columns: entry.columns, tagColumns: entry.tagColumns}
			entry = &bufferEntry{columns: make(map[string]ColumnData), startTime: time.Now()}
			itersSinceReset = 0
		}
		appendEntryToEntry(entry, batch)
		itersSinceReset++
	}
}

// BenchmarkBufferEntry_WriteFlushCycle measures the full write-to-flush cycle
// including data extraction and TypedColumnBatch wrapper creation (the new
// equivalent of mergeBatches).
func BenchmarkBufferEntry_WriteFlushCycle(b *testing.B) {
	const batchSize = 1000
	const batchesPerCycle = 10
	const totalRowsPerCycle = batchSize * batchesPerCycle

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Create batches (outside timing — same cost for both approaches)
		batches := make([]*bufferEntry, batchesPerCycle)
		for j := 0; j < batchesPerCycle; j++ {
			batches[j] = &bufferEntry{
				columns: map[string]ColumnData{
					"time":  {Data: make([]int64, batchSize)},
					"value": {Data: make([]float64, batchSize)},
					"host":  {Data: make([]string, batchSize)},
				},
			}
		}
		entry := &bufferEntry{columns: make(map[string]ColumnData), startTime: time.Now()}

		b.StartTimer()
		// Write phase
		for _, batch := range batches {
			appendEntryToEntry(entry, batch)
		}
		// Flush phase: extract + wrap (replace mergeBatches)
		_ = &bufferEntry{
			columns:    entry.columns,
			tagColumns: entry.tagColumns,
		}
		b.StopTimer()
	}
}

// BenchmarkBufferEntry_WriteZeroCopy measures first-write path where entry is
// empty — this exercises the zero-copy fast path in appendTypedBatchToEntry.
func BenchmarkBufferEntry_WriteZeroCopy(b *testing.B) {
	const batchSize = 1000

	batch := &bufferEntry{
		columns: map[string]ColumnData{
			"time":  {Data: make([]int64, batchSize)},
			"value": {Data: make([]float64, batchSize)},
			"host":  {Data: make([]string, batchSize)},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry := &bufferEntry{columns: make(map[string]ColumnData), startTime: time.Now()}
		appendEntryToEntry(entry, batch)
	}
}

// BenchmarkBufferEntry_SingleRow measures single-row-write overhead.
// Each call appends 1 row (3 columns) to a growing entry without reset,
// simulating a measurement that receives point-by-point ingestion.
func BenchmarkBufferEntry_SingleRow(b *testing.B) {
	const numBatches = 1000
	const batchSize = 1

	batches := make([]*bufferEntry, numBatches)
	for j := 0; j < numBatches; j++ {
		batches[j] = &bufferEntry{
			columns: map[string]ColumnData{
				"time":  {Data: []int64{int64(j)}},
				"value": {Data: []float64{float64(j) * 0.5}},
				"host":  {Data: []string{"srv"}},
			},
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry := &bufferEntry{columns: make(map[string]ColumnData), startTime: time.Now()}
		for _, batch := range batches {
			appendEntryToEntry(entry, batch)
		}
		// Flush: extract + wrap
		_ = &bufferEntry{columns: entry.columns, tagColumns: entry.tagColumns}
	}
}

// BenchmarkBufferEntry_SameTotalRows_BatchVsSingle compares total cost for
// same 1000 rows delivered in different batch sizes.
func BenchmarkBufferEntry_SameTotalRows(b *testing.B) {
	const totalRows = 1000

	makeBatches := func(batchSize int) []*bufferEntry {
		n := totalRows / batchSize
		batches := make([]*bufferEntry, n)
		for j := 0; j < n; j++ {
			times := make([]int64, batchSize)
			vals := make([]float64, batchSize)
			hosts := make([]string, batchSize)
			for k := 0; k < batchSize; k++ {
				times[k] = int64(j*batchSize + k)
				vals[k] = float64(j*batchSize+k) * 0.5
				hosts[k] = "srv"
			}
			batches[j] = &bufferEntry{
				columns: map[string]ColumnData{
					"time": {Data: times}, "value": {Data: vals}, "host": {Data: hosts},
				},
			}
		}
		return batches
	}

	b.Run("batchSize=1", func(b *testing.B) {
		batches := makeBatches(1)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			entry := &bufferEntry{columns: make(map[string]ColumnData), startTime: time.Now()}
			for _, batch := range batches {
				appendEntryToEntry(entry, batch)
			}
			_ = &bufferEntry{columns: entry.columns, tagColumns: entry.tagColumns}
		}
	})
	b.Run("batchSize=100", func(b *testing.B) {
		batches := makeBatches(100)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			entry := &bufferEntry{columns: make(map[string]ColumnData), startTime: time.Now()}
			for _, batch := range batches {
				appendEntryToEntry(entry, batch)
			}
			_ = &bufferEntry{columns: entry.columns, tagColumns: entry.tagColumns}
		}
	})
}

// ---------------------------------------------------------------------------
// Benchmark: write throughput through ArrowBuffer with mock storage
// ---------------------------------------------------------------------------

type benchStorage struct{}

func (s *benchStorage) Write(ctx context.Context, path string, data []byte) error { return nil }
func (s *benchStorage) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	return nil
}
func (s *benchStorage) Read(ctx context.Context, path string) ([]byte, error)      { return nil, nil }
func (s *benchStorage) ReadTo(ctx context.Context, path string, w io.Writer) error { return nil }
func (s *benchStorage) Delete(ctx context.Context, path string) error              { return nil }
func (s *benchStorage) Exists(ctx context.Context, path string) (bool, error)      { return false, nil }
func (s *benchStorage) List(ctx context.Context, prefix string) ([]string, error)  { return nil, nil }
func (s *benchStorage) Close() error                                               { return nil }
func (s *benchStorage) Type() string                                               { return "bench" }
func (s *benchStorage) ConfigJSON() string                                         { return "{}" }
func (s *benchStorage) ReadToAt(ctx context.Context, path string, w io.Writer, offset int64) error {
	return nil
}
func (s *benchStorage) StatFile(ctx context.Context, path string) (int64, error) { return -1, nil }
func (s *benchStorage) AppendReader(ctx context.Context, path string, r io.Reader, size int64) error {
	return nil
}

func makeBenchRecords(count int) map[string][]interface{} {
	now := time.Now().UTC().UnixMicro()
	times := make([]interface{}, count)
	values := make([]interface{}, count)
	hosts := make([]interface{}, count)
	for i := 0; i < count; i++ {
		times[i] = now + int64(i)
		values[i] = float64(i%100) * 0.5
		hosts[i] = "server01"
	}
	return map[string][]interface{}{"time": times, "value": values, "host": hosts}
}

func BenchmarkIngest_WriteBuffer(b *testing.B) {
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	storage := &benchStorage{}

	b.Run("batch=100", func(b *testing.B) {
		buf := NewArrowBuffer(&config.IngestConfig{
			MaxBufferSize:  100000, // high enough to avoid size-based flush during bench
			MaxBufferAgeMS: 60000,
			Compression:    "snappy",
			ShardCount:     32,
			FlushWorkers:   4,
			FlushQueueSize: 256,
		}, storage, logger)
		buf.SetBufferHardLimit(benchBufferHardLimit)
		defer buf.Close()

		ctx := context.Background()
		columns := makeBenchRecords(100)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.WriteColumnarDirect(ctx, "db", "cpu", columns)
		}
		b.ReportMetric(float64(b.N*100)/b.Elapsed().Seconds(), "rec/s")
	})

	b.Run("batch=10000", func(b *testing.B) {
		buf := NewArrowBuffer(&config.IngestConfig{
			MaxBufferSize:  100000,
			MaxBufferAgeMS: 60000,
			Compression:    "snappy",
			ShardCount:     32,
			FlushWorkers:   4,
			FlushQueueSize: 256,
		}, storage, logger)
		buf.SetBufferHardLimit(benchBufferHardLimit)
		defer buf.Close()

		ctx := context.Background()
		columns := makeBenchRecords(10000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.WriteColumnarDirect(ctx, "db", "cpu", columns)
		}
		b.ReportMetric(float64(b.N*10000)/b.Elapsed().Seconds(), "rec/s")
	})
}

// ---------------------------------------------------------------------------
// Benchmark: concurrent writes with varying shard sizes
// ---------------------------------------------------------------------------

func BenchmarkIngest_ConcurrentWrites(b *testing.B) {
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	storage := &benchStorage{}

	shardCounts := []int{1, 4, 16, 32, 64}

	for _, shards := range shardCounts {
		b.Run(formatShardName(shards), func(b *testing.B) {
			buf := NewArrowBuffer(&config.IngestConfig{
				MaxBufferSize:  100000,
				MaxBufferAgeMS: 60000,
				Compression:    "snappy",
				ShardCount:     shards,
				FlushWorkers:   shards / 2,
				FlushQueueSize: 256,
			}, storage, logger)
			buf.SetBufferHardLimit(benchBufferHardLimit)
			defer buf.Close()

			ctx := context.Background()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					measurement := "cpu"
					if i%3 == 0 {
						measurement = "memory"
					} else if i%3 == 1 {
						measurement = "disk"
					}
					columns := makeBenchRecords(50)
					buf.WriteColumnarDirect(ctx, "db", measurement, columns)
					i++
				}
			})
		})
	}
}

func formatShardName(n int) string {
	switch n {
	case 1:
		return "1_shard"
	case 4:
		return "4_shards"
	case 16:
		return "16_shards"
	case 32:
		return "32_shards"
	case 64:
		return "64_shards"
	default:
		return "n_shards"
	}
}

// ---------------------------------------------------------------------------
// Benchmark: lock contention between write and read (SnapshotEntry)
// ---------------------------------------------------------------------------

func BenchmarkIngest_ConcurrentWriteAndRead(b *testing.B) {
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	storage := &benchStorage{}

	buf := NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize:  100000,
		MaxBufferAgeMS: 60000,
		Compression:    "snappy",
		ShardCount:     16,
		FlushWorkers:   4,
		FlushQueueSize: 256,
	}, storage, logger)
	buf.SetBufferHardLimit(benchBufferHardLimit)
	defer buf.Close()

	ctx := context.Background()
	// Pre-populate
	for i := 0; i < 100; i++ {
		buf.WriteColumnarDirect(ctx, "db", "cpu", makeBenchRecords(100))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Even goroutines write, odd goroutines read
		id := 0
		for pb.Next() {
			if id%2 == 0 {
				buf.WriteColumnarDirect(ctx, "db", "cpu", makeBenchRecords(50))
			} else {
				buf.SnapshotEntry("db/cpu")
				buf.TotalBufferedBytes()
			}
			id++
		}
	})
}

// ---------------------------------------------------------------------------
// Benchmark: AdaptiveFlushEngine candidate collection at scale
// ---------------------------------------------------------------------------

func BenchmarkAdaptiveFlush_CollectCandidates(b *testing.B) {
	buf := fakeArrowBufferForBench(b, 32, 1000)
	monitor := &MemoryMonitor{}
	engine := NewAdaptiveFlushEngine(buf, monitor, 15*time.Minute, zerolog.Nop())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.collectCandidates()
	}
}

func BenchmarkAdaptiveFlush_SortAndSelect(b *testing.B) {
	buf := fakeArrowBufferForBench(b, 32, 1000)
	monitor := &MemoryMonitor{}
	engine := NewAdaptiveFlushEngine(buf, monitor, 15*time.Minute, zerolog.Nop())

	candidates := engine.collectCandidates()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Copy candidates to avoid modifying the original (sort is in-place)
		cp := make([]flushCandidate, len(candidates))
		copy(cp, candidates)
		engine.flushLargestUntil(cp, func() bool { return true })
	}
}

func fakeArrowBufferForBench(b *testing.B, shardCount int, entriesPerShard int) *ArrowBuffer {
	b.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)

	buf := &ArrowBuffer{
		ctx:          ctx,
		cancel:       cancel,
		shards:       make([]*bufferShard, shardCount),
		shardCount:   uint32(shardCount),
		flushQueue:   make(chan flushTask, 100),
		flushTimeout: 30 * time.Second,
		logger:       zerolog.Nop(),
	}
	for i := range buf.shards {
		buf.shards[i] = &bufferShard{
			buffers: make(map[string]*bufferEntry),
		}
		for j := 0; j < entriesPerShard; j++ {
			key := "db/meas_" + string(rune('a'+j%26))
			buf.shards[i].buffers[key] = &bufferEntry{
				columns:        map[string]ColumnData{"time": {Data: make([]int64, j*100)}},
				startTime:      time.Now().Add(-time.Duration(j) * time.Minute),
				recordCount:    j * 100,
				estimatedBytes: uint64(j * 10000),
			}
		}
	}
	return buf
}

// ---------------------------------------------------------------------------
// Benchmark: MemoryMonitor check (used in hot path)
// ---------------------------------------------------------------------------

func BenchmarkMemoryMonitor_Check(b *testing.B) {
	m := &MemoryMonitor{
		cfg:         MemoryMonitorConfig{GreenPct: 50, RedPct: 20},
		totalMemory: 16 * 1024 * 1024 * 1024,
		getAvailableMem: func() uint64 {
			return 8 * 1024 * 1024 * 1024 // 50%
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.check()
	}
}

// ---------------------------------------------------------------------------
// Benchmark: Buffer read path (TotalBufferedBytes used by adaptive engine)
// ---------------------------------------------------------------------------

func BenchmarkArrowBuffer_TotalBufferedBytes(b *testing.B) {
	buf := fakeArrowBufferForBench(b, 32, 500)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.TotalBufferedBytes()
	}
}
