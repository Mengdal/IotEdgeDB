package ingest

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"iedb/internal/config"
	"iedb/internal/metrics"

	"github.com/rs/zerolog"
)

// flushCandidate 是自适应决策引擎收集的缓冲快照。
// Fields are copied by value under shard RLock to avoid data races
// with concurrent write-path mutations of the same bufferEntry fields.
type flushCandidate struct {
	shardIdx       int
	bufferKey      string
	estimatedBytes uint64    // copied from entry.estimatedBytes under RLock
	recordCount    int       // copied from entry.recordCount under RLock
	startTime      time.Time // copied from entry.startTime under RLock
	trigger        string    // "age", "hard_limit", or "pressure"
}

// AdaptiveFlushEngine 是内存压力驱动的自适应刷盘决策引擎。
type AdaptiveFlushEngine struct {
	buffer         *ArrowBuffer
	monitor        *MemoryMonitor
	maxBufferBytes atomic.Uint64
	minBufferBytes atomic.Uint64
	maxAge         atomic.Int64 // 纳秒
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
	e := &AdaptiveFlushEngine{
		buffer:  buffer,
		monitor: monitor,
		logger:  logger,
	}
	e.maxBufferBytes.Store(monitor.BufferLimit())
	e.minBufferBytes.Store(monitor.MinBufferBytes())
	e.maxAge.Store(int64(maxAge))
	return e
}

// Run 主循环，每秒评估一次。
func (e *AdaptiveFlushEngine) Run(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// GC timer re-reads maxAge on each cycle for hot-reload support.
	gcTimer := time.NewTimer(time.Duration(e.maxAge.Load()) * 2)
	defer gcTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.evaluate()
		case <-gcTimer.C:
			e.buffer.gcEmptyEntries()
			nextGC := time.Duration(e.maxAge.Load()) * 2
			if nextGC < 5*time.Minute {
				nextGC = 5 * time.Minute
			}
			gcTimer.Reset(nextGC)
		}
	}
}

// evaluate 执行一次完整的评估和决策。
func (e *AdaptiveFlushEngine) evaluate() {
	candidates := e.collectCandidates()

	// 硬上限检查
	var totalBytes uint64
	for _, c := range candidates {
		totalBytes += c.estimatedBytes
	}

	// Update buffer estimated bytes metric
	metrics.Get().SetBufferEstimatedBytes(int64(totalBytes))

	if totalBytes > e.maxBufferBytes.Load() {
		e.logger.Debug().
			Uint64("total_bytes", totalBytes).
			Uint64("limit_bytes", e.maxBufferBytes.Load()).
			Msg("Buffer hard limit exceeded, flushing largest")
		metrics.Get().IncHardLimitFlush()
		e.flushUntilBelow(candidates, totalBytes, e.maxBufferBytes.Load())
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
		metrics.Get().IncAdaptiveFlush()
		e.flushLargestUntil(candidates, func() bool {
			return e.monitor.PressureLevel() == PressureGreen
		})
	case PressureRed:
		metrics.Get().IncAdaptiveFlush()
		e.flushLargestUntil(candidates, func() bool {
			return e.monitor.PressureLevel() != PressureRed
		})
	}
}

// collectCandidates 遍历所有 shard，收集 bufferEntry 的快照（按值复制）。
func (e *AdaptiveFlushEngine) collectCandidates() []flushCandidate {
	e.candidatesBuf = e.candidatesBuf[:0]
	for i := uint32(0); i < e.buffer.shardCount; i++ {
		shard := e.buffer.shards[i]
		shard.mu.RLock()
		for key, entry := range shard.buffers {
			if entry.isEmpty() {
				continue
			}
			e.candidatesBuf = append(e.candidatesBuf, flushCandidate{
				shardIdx:       int(i),
				bufferKey:      key,
				estimatedBytes: entry.estimatedBytes,
				recordCount:    entry.recordCount,
				startTime:      entry.startTime,
			})
		}
		shard.mu.RUnlock()
	}
	return e.candidatesBuf
}

// filterExpired 筛选出超过 maxAge 的缓冲。
func (e *AdaptiveFlushEngine) filterExpired(candidates []flushCandidate) []flushCandidate {
	var expired []flushCandidate
	maxAge := time.Duration(e.maxAge.Load())
	for _, c := range candidates {
		if time.Since(c.startTime) >= maxAge {
			c.trigger = "age"
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
		return candidates[i].recordCount > candidates[j].recordCount
	})

	minPerMeasurement := e.minBufferBytes.Load() / uint64(max(len(candidates), 1))

	for i := range candidates {
		if shouldStop() {
			break
		}
		if candidates[i].estimatedBytes < minPerMeasurement {
			continue
		}
		if candidates[i].trigger == "" {
			candidates[i].trigger = "pressure"
		}
		e.flushCandidate(candidates[i])
	}
}

// flushUntilBelow 按 recordCount 降序刷盘直到低于 targetBytes。
func (e *AdaptiveFlushEngine) flushUntilBelow(
	candidates []flushCandidate,
	currentTotal uint64,
	targetBytes uint64,
) {
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].recordCount > candidates[j].recordCount
	})

	remaining := currentTotal
	for i := range candidates {
		if remaining <= targetBytes {
			break
		}
		remaining -= candidates[i].estimatedBytes
		if candidates[i].trigger == "" {
			candidates[i].trigger = "hard_limit"
		}
		e.flushCandidate(candidates[i])
	}
}

// flushCandidate 触发单个 bufferKey 的刷盘。
func (e *AdaptiveFlushEngine) flushCandidate(c flushCandidate) {
	shard := e.buffer.shards[c.shardIdx]
	shard.mu.Lock()
	entry, exists := shard.buffers[c.bufferKey]
	if !exists || entry.isEmpty() {
		shard.mu.Unlock()
		return
	}
	recordCount := entry.recordCount
	database, measurement := splitKeyToDBAndMeas(c.bufferKey)

	// Move data out of entry
	columns := entry.columns
	tagCols := entry.tagColumns
	entry.columns = make(map[string]ColumnData)
	entry.tagColumns = nil
	entry.recordCount = 0
	entry.estimatedBytes = 0

	trigger := c.trigger
	if trigger == "" {
		trigger = "hard_limit"
	}

	flushCtx, flushCancel := context.WithTimeout(e.buffer.ctx, e.buffer.flushTimeout)
	task := flushTask{
		ctx:         flushCtx,
		cancel:      flushCancel,
		bufferKey:   c.bufferKey,
		database:    database,
		measurement: measurement,
		entry: &bufferEntry{
			columns:     columns,
			tagColumns:  tagCols,
			recordCount: recordCount,
			arrowSchema: entry.arrowSchema,
		},
		trigger:     trigger,
	}
	outcome := e.buffer.tryEnqueueFlush(task, flushCancel, c.bufferKey, recordCount)

	if outcome == flushQueued {
		// Data moved to task; entry is empty shell
	} else {
		prependFlushDataToEntry(entry, columns, tagCols, recordCount)
	}
	shard.mu.Unlock()
}

// splitKeyToDBAndMeas 将 bufferKey 拆分为 database 和 measurement，
// 同时剥离 schema hash 后缀（例如 "db/cpu__abc123" → ("db", "cpu")）。
func splitKeyToDBAndMeas(key string) (database, measurement string) {
	cleanKey, _ := StripSchemaHash(key)
	idx := strings.LastIndex(cleanKey, "/")
	if idx < 0 {
		return cleanKey, cleanKey
	}
	return cleanKey[:idx], cleanKey[idx+1:]
}

// ValidateConfig 实现 config.Reloadable 接口。
func (e *AdaptiveFlushEngine) ValidateConfig(payload *config.ReloadPayload) error {
	if payload == nil || payload.Ingest == nil {
		return nil
	}
	ic := payload.Ingest
	if ic.MaxBufferAgeSeconds < 10 {
		return fmt.Errorf("max_buffer_age_seconds must be >= 10, got %d", ic.MaxBufferAgeSeconds)
	}
	if ic.MaxBufferAgeSeconds > 1_000_000_000 {
		return fmt.Errorf("max_buffer_age_seconds must be <= 1000000000, got %d", ic.MaxBufferAgeSeconds)
	}
	return nil
}

// ReloadConfig 实现 config.Reloadable 接口。
// IMPORTANT: MemoryMonitor.ReloadConfig must be called before this method,
// because BufferLimit() and MinBufferBytes() are read from the monitor.
// ReloadCoordinator guarantees this via registration order.
func (e *AdaptiveFlushEngine) ReloadConfig(payload *config.ReloadPayload) error {
	if payload == nil || payload.Ingest == nil {
		return nil
	}
	ic := payload.Ingest
	oldMaxAge := time.Duration(e.maxAge.Load())
	newMaxAge := time.Duration(ic.MaxBufferAgeSeconds) * time.Second
	newLimit := e.monitor.BufferLimit()
	newMin := e.monitor.MinBufferBytes()
	e.maxAge.Store(int64(newMaxAge))
	e.maxBufferBytes.Store(newLimit)
	e.minBufferBytes.Store(newMin)
	// Keep ArrowBuffer overflow protection in sync with the reloaded limit.
	if e.buffer != nil {
		e.buffer.SetBufferHardLimit(newLimit * 2)
	}
	e.logger.Info().
		Dur("old_max_age", oldMaxAge).
		Dur("new_max_age", newMaxAge).
		Uint64("buffer_limit_mb", newLimit/(1024*1024)).
		Msg("自适应刷盘引擎配置已热加载")
	return nil
}

// MaxAge returns the current max buffer age as nanoseconds (for use with time.Duration).
// Reads the atomic value directly, safe for concurrent access including SIGHUP hot reload.
func (e *AdaptiveFlushEngine) MaxAge() int64 {
	return e.maxAge.Load()
}
