package ingest

import (
	"context"
	"sort"
	"strings"
	"time"

	"iedb/internal/metrics"

	"github.com/rs/zerolog"
)

// flushCandidate 是自适应决策引擎收集的缓冲快照。
type flushCandidate struct {
	shardIdx  int
	bufferKey string
	entry     *bufferEntry // direct reference, avoids copying fields
}

// AdaptiveFlushEngine 是内存压力驱动的自适应刷盘决策引擎。
type AdaptiveFlushEngine struct {
	buffer         *ArrowBuffer
	monitor        *MemoryMonitor
	maxBufferBytes uint64
	minBufferBytes uint64
	maxAge         time.Duration
	candidatesBuf  []flushCandidate
	logger         zerolog.Logger
}

// NewAdaptiveFlushEngine 创建决策引擎。
func NewAdaptiveFlushEngine(
	buffer *ArrowBuffer,
	monitor *MemoryMonitor,
	maxAge time.Duration,
	logger zerolog.Logger,
) *AdaptiveFlushEngine {
	return &AdaptiveFlushEngine{
		buffer:         buffer,
		monitor:        monitor,
		maxBufferBytes: monitor.BufferLimit(),
		minBufferBytes: monitor.MinBufferBytes(),
		maxAge:         maxAge,
		logger:         logger,
	}
}

// Run 主循环，每秒评估一次。
func (e *AdaptiveFlushEngine) Run(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.evaluate()
		}
	}
}

// evaluate 执行一次完整的评估和决策。
func (e *AdaptiveFlushEngine) evaluate() {
	candidates := e.collectCandidates()

	// 硬上限检查
	var totalBytes uint64
	for _, c := range candidates {
		totalBytes += c.entry.estimatedBytes
	}

	// Update buffer estimated bytes metric
	metrics.Get().SetBufferEstimatedBytes(int64(totalBytes))

	if totalBytes > e.maxBufferBytes {
		e.logger.Debug().
			Uint64("total_bytes", totalBytes).
			Uint64("limit_bytes", e.maxBufferBytes).
			Msg("Buffer hard limit exceeded, flushing largest")
		metrics.Get().IncHardLimitFlush()
		e.flushUntilBelow(candidates, totalBytes, e.maxBufferBytes)
		return
	}

	// 15 分钟兜底
	expired := e.filterExpired(candidates)
	for _, c := range expired {
		metrics.Get().IncAgeExpiredFlush()
		e.flushCandidate(c)
	}

	if len(expired) > 0 {
		candidates = e.collectCandidates()
	}

	// 按压力等级决策
	pressure := e.monitor.PressureLevel()
	metrics.Get().SetMemoryPressureLevel(int64(pressure))
	switch pressure {
	case PressureGreen:
	case PressureYellow:
		e.flushLargestUntil(candidates, func() bool {
			return e.monitor.PressureLevel() == PressureGreen
		})
	case PressureRed:
		e.flushLargestUntil(candidates, func() bool {
			return e.monitor.PressureLevel() != PressureRed
		})
	}
}

// collectCandidates 遍历所有 shard，收集 bufferEntry 的引用。
func (e *AdaptiveFlushEngine) collectCandidates() []flushCandidate {
	e.candidatesBuf = e.candidatesBuf[:0]
	for i := uint32(0); i < e.buffer.shardCount; i++ {
		shard := e.buffer.shards[i]
		shard.mu.RLock()
		for key, entry := range shard.buffers {
			e.candidatesBuf = append(e.candidatesBuf, flushCandidate{
				shardIdx:  int(i),
				bufferKey: key,
				entry:     entry,
			})
		}
		shard.mu.RUnlock()
	}
	return e.candidatesBuf
}

// filterExpired 筛选出超过 maxAge 的缓冲。
func (e *AdaptiveFlushEngine) filterExpired(candidates []flushCandidate) []flushCandidate {
	var expired []flushCandidate
	for _, c := range candidates {
		if time.Since(c.entry.startTime) >= e.maxAge {
			expired = append(expired, c)
		}
	}
	return expired
}

// flushLargestUntil 按 recordCount 降序刷盘，直到 shouldStop 条件满足。
func (e *AdaptiveFlushEngine) flushLargestUntil(
	candidates []flushCandidate,
	shouldStop func() bool,
) {
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].entry.recordCount > candidates[j].entry.recordCount
	})

	minPerMeasurement := e.minBufferBytes / uint64(max(len(candidates), 1))

	for _, c := range candidates {
		if shouldStop() {
			break
		}
		if c.entry.estimatedBytes < minPerMeasurement {
			continue
		}
		e.flushCandidate(c)
	}
}

// flushUntilBelow 按 recordCount 降序刷盘直到低于 targetBytes。
func (e *AdaptiveFlushEngine) flushUntilBelow(
	candidates []flushCandidate,
	currentTotal uint64,
	targetBytes uint64,
) {
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].entry.recordCount > candidates[j].entry.recordCount
	})

	remaining := currentTotal
	for _, c := range candidates {
		if remaining <= targetBytes {
			break
		}
		remaining -= c.entry.estimatedBytes
		e.flushCandidate(c)
	}
}

// flushCandidate 触发单个 bufferKey 的刷盘。
func (e *AdaptiveFlushEngine) flushCandidate(c flushCandidate) {
	shard := e.buffer.shards[c.shardIdx]
	shard.mu.Lock()
	entry, exists := shard.buffers[c.bufferKey]
	if !exists || len(entry.batches) == 0 {
		shard.mu.Unlock()
		return
	}
	recordCount := entry.recordCount
	database, measurement := splitKeyToDBAndMeas(c.bufferKey)
	// Extract batch references before deleting entry
	records := make([]interface{}, len(entry.batches))
	for i, batch := range entry.batches {
		records[i] = batch
	}
	delete(shard.buffers, c.bufferKey)
	shard.mu.Unlock()

	flushCtx, flushCancel := context.WithTimeout(context.Background(), e.buffer.flushTimeout)
	task := flushTask{
		ctx:         flushCtx,
		cancel:      flushCancel,
		bufferKey:   c.bufferKey,
		database:    database,
		measurement: measurement,
		records:     records,
		recordCount: recordCount,
	}
	e.buffer.tryEnqueueFlush(task, flushCancel, c.bufferKey, recordCount)
}

// splitKeyToDBAndMeas 将 "database/measurement" 拆分为 database 和 measurement。
func splitKeyToDBAndMeas(key string) (database, measurement string) {
	idx := strings.LastIndex(key, "/")
	if idx < 0 {
		return key, key
	}
	return key[:idx], key[idx+1:]
}
