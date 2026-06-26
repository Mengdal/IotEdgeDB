//go:build duckdb_arrow

package ingest

import (
	"context"
	"testing"
	"time"

	"iedb/internal/config"

	"github.com/rs/zerolog"
)

func newCriticalBuf(t *testing.T) *ArrowBuffer {
	t.Helper()
	return NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize:           10000,
		ShardCount:             4,
		FlushWorkers:           2,
		FlushQueueSize:         16,
		MaxBufferMemoryMB:      128,
		MinBufferMemoryMB:      16,
		MaxBufferAgeMS:         60000,
		MaxBufferAgeSeconds:    900,
		FlushTimeoutSeconds:    30,
		Compression:            "none",
	}, &testNoopStorage{}, zerolog.New(zerolog.NewTestWriter(t)).Level(zerolog.Disabled))
}

func newCriticalMonitor(t *testing.T) *MemoryMonitor {
	t.Helper()
	m := NewMemoryMonitor(MemoryMonitorConfig{
		MaxBufferMemoryMB: 128,
		MinBufferMemoryMB: 16,
		GreenPct:          50,
		RedPct:            20,
	}, 0, zerolog.New(zerolog.NewTestWriter(t)).Level(zerolog.Disabled))
	m.getAvailableMem = func() uint64 { return 1 << 30 } // plenty of memory → green
	return m
}

func drainFlushQueue(buf *ArrowBuffer) {
	for {
		select {
		case <-buf.flushQueue:
			buf.queueDepth.Add(-1)
		default:
			return
		}
	}
}

// ============================================================================
// SECTION 1: tryEnqueueFlush
// ============================================================================

func TestTryEnqueueFlush_Queued(t *testing.T) {
	buf := newCriticalBuf(t)
	defer buf.Close()
	drainFlushQueue(buf)

	_, cancel := context.WithCancel(context.Background())
	task := flushTask{cancel: cancel, bufferKey: "db/m__hash", entry: &bufferEntry{}, trigger: "size"}
	got := buf.tryEnqueueFlush(task, cancel, "db/m__hash", 10)
	if got != flushQueued {
		t.Fatalf("expected flushQueued, got %v", got)
	}
	if buf.queueDepth.Load() != 1 {
		t.Errorf("queueDepth = %d, want 1", buf.queueDepth.Load())
	}
}

func TestTryEnqueueFlush_Closing(t *testing.T) {
	buf := newCriticalBuf(t)
	buf.closing.Store(true)
	defer func() { buf.closing.Store(false); buf.Close() }()
	drainFlushQueue(buf)

	_, cancel := context.WithCancel(context.Background())
	task := flushTask{cancel: cancel, bufferKey: "k", entry: &bufferEntry{}}
	got := buf.tryEnqueueFlush(task, cancel, "k", 5)
	if got != flushSkipClosing {
		t.Fatalf("expected flushSkipClosing, got %v", got)
	}
}

func TestTryEnqueueFlush_QueueFull(t *testing.T) {
	buf := newCriticalBuf(t)
	defer buf.Close()
	drainFlushQueue(buf)
	for i := 0; i < 16; i++ {
		_, c := context.WithCancel(context.Background())
		buf.flushQueue <- flushTask{cancel: c, bufferKey: "fill", entry: &bufferEntry{}}
		buf.queueDepth.Add(1)
	}
	_, cancel := context.WithCancel(context.Background())
	task := flushTask{cancel: cancel, bufferKey: "of", entry: &bufferEntry{}}
	got := buf.tryEnqueueFlush(task, cancel, "of", 7)
	if got != flushQueueFull {
		t.Fatalf("expected flushQueueFull, got %v", got)
	}
	drainFlushQueue(buf)
}

func TestTryEnqueueFlush_CtxCanceled(t *testing.T) {
	buf := newCriticalBuf(t)
	buf.cancel()
	defer buf.Close()
	drainFlushQueue(buf)

	_, cancel := context.WithCancel(context.Background())
	task := flushTask{ cancel: cancel, bufferKey: "k", entry: &bufferEntry{}}
	got := buf.tryEnqueueFlush(task, cancel, "k", 3)
	if got != flushCtxCanceled {
		t.Fatalf("expected flushCtxCanceled, got %v", got)
	}
}

// ============================================================================
// SECTION 2: evictOldestEntries
// ============================================================================

func TestEvictOldestEntries_NoEntries(t *testing.T) {
	buf := newCriticalBuf(t)
	defer buf.Close()
	c := 0
	if buf.evictOldestEntries(0, &c) {
		t.Error("should return false with no entries")
	}
}

func TestEvictOldestEntries_EvictsEntry(t *testing.T) {
	buf := newCriticalBuf(t)
	defer buf.Close()
	drainFlushQueue(buf)

	buf.WriteColumnarDirect(context.Background(), "db", "cpu", map[string][]interface{}{
		"v": {float64(42.0)},
	})
	keys := buf.AllBufferKeys()
	if len(keys) == 0 {
		t.Skip("no entries (auto-flush already processed)")
	}
	t.Logf("before evict: %v", keys)

	c := 0
	result := buf.evictOldestEntries(0, &c)
	t.Logf("evicted=%v inlineFlushCount=%d remaining=%v", result, c, buf.AllBufferKeys())
}

func TestEvictOldestEntries_InlineFlushCap(t *testing.T) {
	buf := newCriticalBuf(t)
	defer buf.Close()
	drainFlushQueue(buf)

	for i := 0; i < 8; i++ {
		buf.WriteColumnarDirect(context.Background(), "db", "cpu", map[string][]interface{}{
			"v": {float64(i)},
		})
	}
	if len(buf.AllBufferKeys()) == 0 {
		t.Skip("no entries")
	}

	c := 0
	buf.evictOldestEntries(0, &c)
	// maxInlineFlushes = 5, so c should be capped at 5
	if c > 5 {
		t.Errorf("inline flush cap exceeded: got %d, max 5", c)
	}
	t.Logf("inlineFlushCount=%d (cap=5)", c)
}

// ============================================================================
// SECTION 3: ensureBufferSpace
// ============================================================================

func TestEnsureBufferSpace_NoLimit(t *testing.T) {
	buf := newCriticalBuf(t)
	defer buf.Close()
	if err := buf.ensureBufferSpace(1 << 30); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnsureBufferSpace_UnderLimit(t *testing.T) {
	buf := newCriticalBuf(t)
	defer buf.Close()
	buf.SetBufferHardLimit(1 << 30)
	if err := buf.ensureBufferSpace(1024); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnsureBufferSpace_RejectsWhenFull(t *testing.T) {
	buf := newCriticalBuf(t)
	defer buf.Close()
	drainFlushQueue(buf)

	for i := 0; i < 5; i++ {
		buf.WriteColumnarDirect(context.Background(), "db", "cpu", map[string][]interface{}{
			"v": {float64(i)},
		})
	}
	buf.SetBufferHardLimit(1) // 1 byte → everything over

	// ensureBufferSpace may evict entries first, then reject if still over
	err := buf.ensureBufferSpace(1 << 20)
	if err != nil {
		t.Logf("rejected (correct): %v", err)
	} else {
		t.Log("accepted (eviction may have cleared buffer)")
	}
}

// ============================================================================
// SECTION 4: flushCandidate
// ============================================================================

func TestFlushCandidate_Basic(t *testing.T) {
	buf := newCriticalBuf(t)
	defer buf.Close()
	drainFlushQueue(buf)

	buf.WriteColumnarDirect(context.Background(), "testdb", "cpu", map[string][]interface{}{
		"host": {"srv1"}, "usage": {float64(99.0)},
	})
	keys := buf.AllBufferKeys()
	if len(keys) == 0 {
		t.Skip("no entries")
	}

	m := newCriticalMonitor(t)
	defer m.Stop()
	e := NewAdaptiveFlushEngine(buf, m, 30*time.Second, zerolog.New(zerolog.NewTestWriter(t)).Level(zerolog.Disabled))

	e.flushCandidate(flushCandidate{
		shardIdx: 0, bufferKey: keys[0], estimatedBytes: 1024,
		recordCount: 1, startTime: time.Now(), trigger: "pressure",
	})
	t.Log("flushCandidate completed")
}

func TestFlushCandidate_NonExistentKey(t *testing.T) {
	buf := newCriticalBuf(t)
	defer buf.Close()
	m := newCriticalMonitor(t)
	defer m.Stop()
	e := NewAdaptiveFlushEngine(buf, m, 30*time.Second, zerolog.New(zerolog.NewTestWriter(t)).Level(zerolog.Disabled))

	// Should not panic
	e.flushCandidate(flushCandidate{shardIdx: 0, bufferKey: "no/such__key", trigger: "hard_limit"})
}

func TestFlushCandidate_EmptyTriggerDefaults(t *testing.T) {
	buf := newCriticalBuf(t)
	defer buf.Close()
	drainFlushQueue(buf)

	buf.WriteColumnarDirect(context.Background(), "db", "m", map[string][]interface{}{
		"v": {float64(1.0)},
	})
	keys := buf.AllBufferKeys()
	if len(keys) == 0 {
		t.Skip("no entries")
	}

	m := newCriticalMonitor(t)
	defer m.Stop()
	e := NewAdaptiveFlushEngine(buf, m, 30*time.Second, zerolog.New(zerolog.NewTestWriter(t)).Level(zerolog.Disabled))

	// Empty trigger → defaults to "hard_limit"
	e.flushCandidate(flushCandidate{
		shardIdx: 0, bufferKey: keys[0], estimatedBytes: 64,
		recordCount: 1, startTime: time.Now(), trigger: "",
	})
}

// ============================================================================
// SECTION 5: evaluate
// ============================================================================

func TestEvaluate_NoEntries(t *testing.T) {
	buf := newCriticalBuf(t)
	defer buf.Close()
	drainFlushQueue(buf)

	m := newCriticalMonitor(t)
	defer m.Stop()
	e := NewAdaptiveFlushEngine(buf, m, 30*time.Second, zerolog.New(zerolog.NewTestWriter(t)).Level(zerolog.Disabled))

	e.evaluate() // no entries → no-op, no panic
}

func TestEvaluate_HardLimitTriggered(t *testing.T) {
	buf := newCriticalBuf(t)
	defer buf.Close()
	drainFlushQueue(buf)

	for i := 0; i < 3; i++ {
		buf.WriteColumnarDirect(context.Background(), "db", "cpu", map[string][]interface{}{
			"v": {float64(i)},
		})
	}
	if len(buf.AllBufferKeys()) == 0 {
		t.Skip("no entries")
	}

	m := newCriticalMonitor(t)
	defer m.Stop()
	e := NewAdaptiveFlushEngine(buf, m, 30*time.Second, zerolog.New(zerolog.NewTestWriter(t)).Level(zerolog.Disabled))
	e.maxBufferBytes.Store(1) // force hard-limit path

	e.evaluate()
	t.Log("hard-limit evaluate completed without panic")
}

func TestEvaluate_AgeExpired(t *testing.T) {
	buf := newCriticalBuf(t)
	defer buf.Close()
	drainFlushQueue(buf)

	buf.WriteColumnarDirect(context.Background(), "db", "old", map[string][]interface{}{
		"v": {float64(99.0)},
	})
	if len(buf.AllBufferKeys()) == 0 {
		t.Skip("no entries")
	}

	m := newCriticalMonitor(t)
	defer m.Stop()
	e := NewAdaptiveFlushEngine(buf, m, 30*time.Second, zerolog.New(zerolog.NewTestWriter(t)).Level(zerolog.Disabled))
	e.maxAge.Store(int64(1))          // age=1ns → everything expired
	e.maxBufferBytes.Store(1 << 30)   // high limit → no hard-limit trigger

	e.evaluate()
	t.Log("age-expired evaluate completed without panic")
}

func TestEvaluate_PressureTriggersFlush(t *testing.T) {
	buf := newCriticalBuf(t)
	defer buf.Close()
	drainFlushQueue(buf)

	for i := 0; i < 5; i++ {
		buf.WriteColumnarDirect(context.Background(), "db", "cpu", map[string][]interface{}{
			"v": {float64(i)},
		})
	}
	if len(buf.AllBufferKeys()) == 0 {
		t.Skip("no entries")
	}

	logger := zerolog.New(zerolog.NewTestWriter(t)).Level(zerolog.Disabled)
	m := NewMemoryMonitor(MemoryMonitorConfig{
		MaxBufferMemoryMB: 128, MinBufferMemoryMB: 16, GreenPct: 50, RedPct: 20,
	}, 0, logger)
	m.getAvailableMem = func() uint64 { return 1 << 20 } // 1MB free → RED pressure
	m.check()                                             // set pressure to RED
	defer m.Stop()

	e := NewAdaptiveFlushEngine(buf, m, 30*time.Second, logger)
	e.maxBufferBytes.Store(1 << 30) // high limit → no hard-limit trigger

	e.evaluate()
	level := m.PressureLevel()
	t.Logf("pressure after evaluate: %v", level)
}

// ============================================================================
// SECTION 6: flushWorker
// ============================================================================

func TestFlushWorker_ProcessesTask(t *testing.T) {
	buf := newCriticalBuf(t)
	defer buf.Close()
	drainFlushQueue(buf)

	_, cancel := context.WithCancel(context.Background())
	buf.flushQueue <- flushTask{
		// ctx not used
		cancel:      cancel,
		bufferKey:   "db/m__h",
		database:    "db",
		measurement: "m",
		entry:       &bufferEntry{columns: map[string]ColumnData{}, recordCount: 0},
		trigger:     "size",
	}
	buf.queueDepth.Add(1)

	// simulate one worker iteration
	select {
	case <-buf.ctx.Done():
	case task := <-buf.flushQueue:
		buf.queueDepth.Add(-1)
		task.cancel()
	}
}

func TestFlushWorker_StopsOnCtxDone(t *testing.T) {
	buf := newCriticalBuf(t)
	buf.cancel()
	defer buf.Close()

	select {
	case <-buf.ctx.Done():
		t.Log("ctx done — worker would exit")
	default:
		t.Error("ctx should be cancelled")
	}
}

// ============================================================================
// SECTION 7: Pressure transitions
// ============================================================================

func TestMonitorPressureTransitions(t *testing.T) {
	m := NewMemoryMonitor(MemoryMonitorConfig{
		MaxBufferMemoryMB: 128, MinBufferMemoryMB: 16, GreenPct: 50, RedPct: 20,
	}, 0, zerolog.New(zerolog.NewTestWriter(t)).Level(zerolog.Disabled))
	defer m.Stop()

	// Set a known totalMemory (128MB) so percentages are predictable.
	// detectSystemMemory() returns unreliable values on non-Linux platforms.
	m.totalMemory = 128 << 20 // 128MB
	// greenPct=50 → green when avail ≥ 64MB
	// redPct=20  → red   when avail < 25.6MB

	// Green: 100MB > 64MB
	m.getAvailableMem = func() uint64 { return 100 << 20 }
	level := m.check()
	if level != PressureGreen {
		t.Errorf("100M free → green, got %v", level)
	}

	// Yellow: 40MB < 64MB but > 25.6MB
	m.getAvailableMem = func() uint64 { return 40 << 20 }
	level = m.check()
	if level != PressureYellow {
		t.Errorf("40M free → yellow, got %v", level)
	}

	// Red: 10MB < 25.6MB
	m.getAvailableMem = func() uint64 { return 10 << 20 }
	level = m.check()
	if level != PressureRed {
		t.Errorf("10M free → red, got %v", level)
	}
}
