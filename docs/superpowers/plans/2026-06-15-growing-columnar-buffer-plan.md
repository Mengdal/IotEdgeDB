# Growing Columnar Buffer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `bufferEntry.batches []*TypedColumnBatch` with directly growing columnar arrays (`data map[string]interface{}` + `validity map[string][]bool`), eliminating `mergeBatches` and simplifying flush failure recovery.

**Architecture:** Data enters via `convertColumnsToTyped` (unchanged) and is appended column-by-column into `entry.data`/`entry.validity`. Flush extracts the data map reference (zero-copy), wraps it as `TypedColumnBatch`, and passes directly to `flushPartitionedData`. On failure, data is prepended back to the entry.

**Tech Stack:** Go, Apache Arrow v18, DuckDB (unchanged from current)

---

### Task 1: Modify bufferEntry and flushTask structs

**Files:**
- Modify: `internal/ingest/arrow_writer.go:506-514` (bufferEntry)
- Modify: `internal/ingest/arrow_writer.go:519-521` (isEmpty)
- Modify: `internal/ingest/arrow_writer.go:561-571` (flushTask)

- [ ] **Step 1: Change bufferEntry struct**

Replace `batches []*TypedColumnBatch` with growing columnar fields:

```go
// bufferEntry holds all buffered data and metadata for a single measurement key.
// Replaces 6 separate maps (buffers, bufferStartTimes, bufferRecordCounts,
// bufferSchemas, bufferEstimatedBytes, bufferRefreshIndex) with a single struct,
// reducing 5+ hash lookups to 1 on the write path.
type bufferEntry struct {
	data          map[string]interface{} // growing typed column arrays ([]int64, []float64, []string, []bool, []decimal128.Num)
	validity      map[string][]bool      // growing null bitmaps; nil entry = all valid; nil map = no nulls at all
	tagColumns    []string               // tag column names (stable for this schema, set on first write)
	startTime     time.Time              // first record arrival time
	recordCount   int                    // total record count
	estimatedBytes uint64                // estimated memory usage
	schema        string                 // column signature for schema evolution
	arrowSchema   *arrow.Schema          // inferred Arrow schema (nil until first flush preparation)
	refreshIndex  int                    // Arrow VIEW incremental refresh cursor
}
```

- [ ] **Step 2: Change isEmpty**

```go
// isEmpty returns true when the entry has no buffered data.
// Such entries are "empty shells" kept to preserve the cached arrowSchema
// across flush cycles — they are eligible for GC after extended idleness.
func (e *bufferEntry) isEmpty() bool {
	return len(e.data) == 0
}
```

- [ ] **Step 3: Change flushTask struct**

Replace `records []interface{}` with typed fields:

```go
// flushTask represents a flush operation to be executed by workers
type flushTask struct {
	ctx         context.Context
	cancel      context.CancelFunc // must be called when task completes to release resources
	bufferKey   string
	database    string
	measurement string
	data        map[string]interface{} // typed columnar data (map["time"]→[]int64, etc.)
	validity    map[string][]bool      // null bitmaps; nil = no nulls
	tagColumns  []string               // tag column names for Parquet metadata
	recordCount int
	arrowSchema *arrow.Schema // cached schema from bufferEntry
	trigger     string        // "size", "age", "hard_limit", or "manual"
}
```

- [ ] **Step 4: Build to verify compilation breaks as expected**

Run: `go build -tags=duckdb_arrow ./internal/ingest/...`
Expected: compilation errors at all sites referencing `entry.batches`, `task.records`, etc.

- [ ] **Step 5: Commit**

```bash
git add internal/ingest/arrow_writer.go
git commit -m "refactor: change bufferEntry.batches and flushTask.records to growing columnar fields"
```

---

### Task 2: Add columnar append helper and validity merge logic

**Files:**
- Create: `internal/ingest/arrow_writer_columnar_append_test.go`
- Modify: `internal/ingest/arrow_writer.go` (add new functions after bufferEntry)

- [ ] **Step 1: Write failing test for appendTypedBatchToEntry**

```go
package ingest

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

func TestAppendTypedBatchToEntry_EmptyEntry(t *testing.T) {
	entry := &bufferEntry{
		data:     make(map[string]interface{}),
		validity: nil,
	}

	batch := &TypedColumnBatch{
		Data: map[string]interface{}{
			"time": []int64{1, 2, 3},
			"temp": []float64{23.5, 24.0, 25.1},
		},
		Validity:   nil, // no nulls
		TagColumns: []string{"sensor"},
		Signature:  "temp:f64,time:i64",
	}

	appendTypedBatchToEntry(entry, batch, 3)

	if entry.recordCount != 3 {
		t.Fatalf("recordCount = %d, want 3", entry.recordCount)
	}
	if len(entry.data["time"].([]int64)) != 3 {
		t.Fatalf("time len = %d, want 3", len(entry.data["time"].([]int64)))
	}
	if entry.data["time"].([]int64)[0] != 1 {
		t.Errorf("time[0] = %d, want 1", entry.data["time"].([]int64)[0])
	}
	if entry.validity != nil {
		t.Error("validity should be nil when no nulls")
	}
	if len(entry.tagColumns) != 1 || entry.tagColumns[0] != "sensor" {
		t.Errorf("tagColumns = %v, want [sensor]", entry.tagColumns)
	}
}

func TestAppendTypedBatchToEntry_WithNulls(t *testing.T) {
	entry := &bufferEntry{
		data:     make(map[string]interface{}),
		validity: nil,
	}

	batch1 := &TypedColumnBatch{
		Data: map[string]interface{}{
			"time": []int64{1, 2, 3},
			"val":  []float64{10.0, 20.0, 30.0},
		},
		Validity: map[string][]bool{
			"val": {true, false, true}, // row 1 is null
		},
		TagColumns: nil,
		Signature:  "time:i64,val:f64",
	}

	appendTypedBatchToEntry(entry, batch1, 3)

	if entry.validity == nil {
		t.Fatal("validity should be non-nil when batch has nulls")
	}
	valValid := entry.validity["val"]
	if len(valValid) != 3 {
		t.Fatalf("val validity len = %d, want 3", len(valValid))
	}
	if valValid[0] != true || valValid[1] != false || valValid[2] != true {
		t.Errorf("val validity = %v, want [true, false, true]", valValid)
	}
}

func TestAppendTypedBatchToEntry_MultipleAppends(t *testing.T) {
	entry := &bufferEntry{
		data:     make(map[string]interface{}),
		validity: nil,
	}

	batch1 := &TypedColumnBatch{
		Data: map[string]interface{}{
			"time": []int64{1, 2},
			"val":  []float64{10.0, 20.0},
		},
		Validity:   nil,
		TagColumns: nil,
		Signature:  "time:i64,val:f64",
	}

	batch2 := &TypedColumnBatch{
		Data: map[string]interface{}{
			"time": []int64{3, 4, 5},
			"val":  []float64{30.0, 40.0, 50.0},
		},
		Validity:   nil,
		TagColumns: nil,
		Signature:  "time:i64,val:f64",
	}

	appendTypedBatchToEntry(entry, batch1, 2)
	appendTypedBatchToEntry(entry, batch2, 3)

	if entry.recordCount != 5 {
		t.Fatalf("recordCount = %d, want 5", entry.recordCount)
	}

	timeCol := entry.data["time"].([]int64)
	for i, want := range []int64{1, 2, 3, 4, 5} {
		if timeCol[i] != want {
			t.Errorf("time[%d] = %d, want %d", i, timeCol[i], want)
		}
	}

	valCol := entry.data["val"].([]float64)
	for i, want := range []float64{10.0, 20.0, 30.0, 40.0, 50.0} {
		if valCol[i] != want {
			t.Errorf("val[%d] = %f, want %f", i, valCol[i], want)
		}
	}
}

func TestAppendTypedBatchToEntry_MixedNulls(t *testing.T) {
	// First batch has nulls, second doesn't — validity must be padded with true
	entry := &bufferEntry{
		data:     make(map[string]interface{}),
		validity: nil,
	}

	batch1 := &TypedColumnBatch{
		Data: map[string]interface{}{
			"time": []int64{1, 2},
			"val":  []float64{10.0, 20.0},
		},
		Validity: map[string][]bool{
			"val": {true, false}, // row 1 is null
		},
		TagColumns: nil,
		Signature:  "time:i64,val:f64",
	}

	batch2 := &TypedColumnBatch{
		Data: map[string]interface{}{
			"time": []int64{3, 4},
			"val":  []float64{30.0, 40.0},
		},
		Validity:   nil, // all valid
		TagColumns: nil,
		Signature:  "time:i64,val:f64",
	}

	appendTypedBatchToEntry(entry, batch1, 2)
	appendTypedBatchToEntry(entry, batch2, 2)

	valValid := entry.validity["val"]
	if len(valValid) != 4 {
		t.Fatalf("val validity len = %d, want 4", len(valValid))
	}
	// batch1: true, false; batch2 (all valid → padded): true, true
	want := []bool{true, false, true, true}
	for i, w := range want {
		if valValid[i] != w {
			t.Errorf("val validity[%d] = %v, want %v", i, valValid[i], w)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags=duckdb_arrow -run TestAppendTypedBatchToEntry ./internal/ingest/ -v`
Expected: FAIL with "undefined: appendTypedBatchToEntry"

- [ ] **Step 3: Implement appendTypedBatchToEntry**

```go
// appendTypedBatchToEntry appends a TypedColumnBatch's data and validity into
// a bufferEntry's growing columnar arrays. batchRows must equal the number of
// rows in the batch.
func appendTypedBatchToEntry(entry *bufferEntry, batch *TypedColumnBatch, batchRows int) {
	// Append data columns
	for name, col := range batch.Data {
		switch v := col.(type) {
		case []int64:
			if existing, ok := entry.data[name]; ok {
				entry.data[name] = append(existing.([]int64), v...)
			} else {
				entry.data[name] = v
			}
		case []float64:
			if existing, ok := entry.data[name]; ok {
				entry.data[name] = append(existing.([]float64), v...)
			} else {
				entry.data[name] = v
			}
		case []string:
			if existing, ok := entry.data[name]; ok {
				entry.data[name] = append(existing.([]string), v...)
			} else {
				entry.data[name] = v
			}
		case []bool:
			if existing, ok := entry.data[name]; ok {
				entry.data[name] = append(existing.([]bool), v...)
			} else {
				entry.data[name] = v
			}
		case []decimal128.Num:
			if existing, ok := entry.data[name]; ok {
				entry.data[name] = append(existing.([]decimal128.Num), v...)
			} else {
				entry.data[name] = v
			}
		}
	}

	// Merge validity bitmaps
	if batch.Validity != nil {
		if entry.validity == nil {
			entry.validity = make(map[string][]bool)
		}
		for name, batchValid := range batch.Validity {
			if batchValid != nil {
				entry.validity[name] = append(entry.validity[name], batchValid...)
			}
			// nil entry = all valid for this column; handled below with padding
		}
	}

	// Pad validity for columns that already have validity tracking but this
	// batch had no explicit entry (or batch.Validity was nil entirely).
	// These positions are valid (true) — the column exists, just no nulls.
	if entry.validity != nil {
		padded := false
		for name := range entry.validity {
			var hasExplicit bool
			if batch.Validity != nil {
				_, hasExplicit = batch.Validity[name]
			}
			if !hasExplicit {
				// Extend with all-true for these batchRows
				existing := entry.validity[name]
				for i := 0; i < batchRows; i++ {
					existing = append(existing, true)
				}
				entry.validity[name] = existing
				padded = true
			}
		}
		_ = padded
	}

	// Set tag columns on first write (stable per schema)
	if len(entry.tagColumns) == 0 && len(batch.TagColumns) > 0 {
		entry.tagColumns = make([]string, len(batch.TagColumns))
		copy(entry.tagColumns, batch.TagColumns)
	}

	entry.recordCount += batchRows
	entry.estimatedBytes += uint64(batchRows) * estimateBytesPerRow(batch)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -tags=duckdb_arrow -run TestAppendTypedBatchToEntry ./internal/ingest/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/ingest/arrow_writer.go internal/ingest/arrow_writer_columnar_append_test.go
git commit -m "feat: add appendTypedBatchToEntry with validity merge"
```

---

### Task 3: Modify writeColumnarInternal

**Files:**
- Modify: `internal/ingest/arrow_writer.go:1380-1546` (writeColumnarInternal)

- [ ] **Step 1: Rewrite the buffer append + flush trigger section (lines 1440-1522)**

Replace batch-array manipulation with growing columnar logic. The WAL section (lines 1381-1411) and convertColumnsToTyped section (lines 1413-1435) remain unchanged.

```go
	// OPTIMIZATION: Get shard for this buffer key (lock sharding)
	shard := b.getShard(bufferKey)

	// OPTIMIZATION: Extract-then-flush pattern
	// Hold lock ONLY to extract records, flush outside lock
	var dataForFlush map[string]interface{}
	var validityForFlush map[string][]bool
	var tagColumnsForFlush []string
	var shouldFlush bool

	shard.mu.Lock()

	// Initialize buffer and record count if needed
	entry, exists := shard.buffers[bufferKey]
	if !exists {
		entry = &bufferEntry{
			data:      make(map[string]interface{}),
			startTime: time.Now().UTC(),
			schema:    newSignature,
		}
		shard.buffers[bufferKey] = entry
	} else if entry.isEmpty() {
		entry.startTime = time.Now().UTC()
		entry.recordCount = 0
		entry.estimatedBytes = 0
		entry.refreshIndex = 0
	}
	// Infer Arrow schema eagerly on first entry creation.
	if entry.arrowSchema == nil && len(typedColumns.Data) > 0 {
		tagCols := record.TagColumns
		decCols := b.getDecimalColumns(record.Measurement)
		schema, err := b.writer.inferSchema(typedColumns.Data, tagCols, decCols)
		if err != nil {
			shard.mu.Unlock()
			return fmt.Errorf("failed to infer Arrow schema: %w", err)
		}
		entry.arrowSchema = schema
	}

	// Append data to growing columnar arrays
	appendTypedBatchToEntry(entry, typedColumns, numRecords)

	totalBuffered := entry.recordCount

	// Check if buffer needs flush (size-based).
	// When adaptive flush engine is active, it owns all flush decisions and
	// this fixed-size gate is skipped to allow memory-pressure-driven buffering.
	if b.adaptiveFlush.Load() == nil && totalBuffered >= b.config.MaxBufferSize {
		// Move data out of entry for flush
		dataForFlush = entry.data
		validityForFlush = entry.validity
		tagColumnsForFlush = entry.tagColumns
		entry.data = make(map[string]interface{})
		entry.validity = nil
		entry.tagColumns = nil
		entry.recordCount = 0
		entry.estimatedBytes = 0

		// Try to enqueue BEFORE clearing data. tryEnqueueFlush is
		// non-blocking (select with default) and does not acquire any
		// shard locks — safe to call while holding shard.mu.
		flushCtx, flushCancel := context.WithTimeout(b.ctx, b.flushTimeout)
		task := flushTask{
			ctx:         flushCtx,
			cancel:      flushCancel,
			bufferKey:   bufferKey,
			database:    database,
			measurement: record.Measurement,
			data:        dataForFlush,
			validity:    validityForFlush,
			tagColumns:  tagColumnsForFlush,
			recordCount: totalBuffered,
			arrowSchema: entry.arrowSchema,
		}
		outcome := b.tryEnqueueFlush(task, flushCancel, bufferKey, totalBuffered)

		if outcome == flushQueued {
			// Data successfully moved to task; entry is empty shell
			shouldFlush = true
		} else {
			// Enqueue failed — prepend data back into entry.
			// Lock is still held, so no concurrent writes have occurred.
			prependFlushDataToEntry(entry, dataForFlush, validityForFlush, tagColumnsForFlush, totalBuffered)
		}
		// On any non-queued outcome: flushCancel already called by tryEnqueueFlush.
	}

	// Release lock IMMEDIATELY (lock held for <1ms)
	shard.mu.Unlock()
```

- [ ] **Step 2: Check for existing compilation errors and fix references**

Search for and update all references to `entry.batches`, `task.records` in `writeColumnarInternal`:
```
grep -n "entry\.batches\|task\.records\|\.batches\b" internal/ingest/arrow_writer.go
```
Ensure no stale references remain.

- [ ] **Step 3: Run relevant tests**

Run: `go test -tags=duckdb_arrow -run "TestRowsToColumnar|TestDecimal128|TestGetColumnSignature" ./internal/ingest/ -v`
Expected: PASS (these don't touch buffer internals)

- [ ] **Step 4: Commit**

```bash
git add internal/ingest/arrow_writer.go
git commit -m "refactor: rewrite writeColumnarInternal for growing columnar buffer"
```

---

### Task 4: Modify writeTypedColumnarInternal

**Files:**
- Modify: `internal/ingest/arrow_writer.go:1552-1674` (writeTypedColumnarInternal)

- [ ] **Step 1: Apply the same pattern as writeColumnarInternal**

Replace lines 1580-1651 (shard lock section) with the same growing-buffer pattern used in Task 3. The logic is identical except there's no `convertColumnsToTyped` call — the typed batch is already provided.

```go
	// Get shard for this buffer key (lock sharding)
	shard := b.getShard(bufferKey)

	var dataForFlush map[string]interface{}
	var validityForFlush map[string][]bool
	var tagColumnsForFlush []string
	var shouldFlush bool

	shard.mu.Lock()

	// Initialize buffer and record count if needed
	entry, exists := shard.buffers[bufferKey]
	if !exists {
		entry = &bufferEntry{
			data:      make(map[string]interface{}),
			startTime: time.Now().UTC(),
			schema:    newSignature,
		}
		shard.buffers[bufferKey] = entry
	} else if entry.isEmpty() {
		entry.startTime = time.Now().UTC()
		entry.recordCount = 0
		entry.estimatedBytes = 0
		entry.refreshIndex = 0
	}
	if entry.arrowSchema == nil && len(typedColumns.Data) > 0 {
		tagCols := typedColumns.TagColumns
		decCols := b.getDecimalColumns(measurement)
		schema, err := b.writer.inferSchema(typedColumns.Data, tagCols, decCols)
		if err != nil {
			shard.mu.Unlock()
			return fmt.Errorf("failed to infer Arrow schema: %w", err)
		}
		entry.arrowSchema = schema
	}

	// Append data to growing columnar arrays
	appendTypedBatchToEntry(entry, typedColumns, numRecords)

	totalBuffered := entry.recordCount

	// Check if buffer needs flush (size-based).
	if b.adaptiveFlush.Load() == nil && totalBuffered >= b.config.MaxBufferSize {
		dataForFlush = entry.data
		validityForFlush = entry.validity
		tagColumnsForFlush = entry.tagColumns
		entry.data = make(map[string]interface{})
		entry.validity = nil
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
			data:        dataForFlush,
			validity:    validityForFlush,
			tagColumns:  tagColumnsForFlush,
			recordCount: totalBuffered,
			arrowSchema: entry.arrowSchema,
		}
		outcome := b.tryEnqueueFlush(task, flushCancel, bufferKey, totalBuffered)

		if outcome == flushQueued {
			shouldFlush = true
		} else {
			prependFlushDataToEntry(entry, dataForFlush, validityForFlush, tagColumnsForFlush, totalBuffered)
		}
	}

	shard.mu.Unlock()
```

- [ ] **Step 2: Commit**

```bash
git add internal/ingest/arrow_writer.go
git commit -m "refactor: rewrite writeTypedColumnarInternal for growing columnar buffer"
```

---

### Task 5: Implement flush worker adapter and modify flushBufferLocked

**Files:**
- Modify: `internal/ingest/arrow_writer.go:2063-2107` (flushWorker)
- Modify: `internal/ingest/arrow_writer.go:2710-2786` (flushBufferLocked)

- [ ] **Step 1: Rewrite flushWorker to use task.data directly**

Replace the `flushRecordsAsync` call with direct `flushPartitionedData`:

```go
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
				Int("records", task.recordCount).
				Int64("queue_depth", b.queueDepth.Load()).
				Msg("Worker processing flush task")

			// Wrap growing data as TypedColumnBatch for flushPartitionedData
			merged := &TypedColumnBatch{
				Data:       task.data,
				Validity:   task.validity,
				TagColumns: task.tagColumns,
			}

			startTime := time.Now()
			database := task.database
			measurement := task.measurement

			// Execute flush with data_time partitioning
			if err := b.flushPartitionedData(task.ctx, task.bufferKey, database, measurement, merged, task.recordCount, "async", startTime, task.arrowSchema); err != nil {
				// Flush failed — prepend data back into buffer
				b.prependFlushData(task.bufferKey, task.data, task.validity, task.tagColumns, task.recordCount)
				b.logger.Error().
					Err(err).
					Str("buffer_key", task.bufferKey).
					Int("records", task.recordCount).
					Msg("Flush failed — data restored to buffer, will retry")

				// Write FLUSH_FAIL control record for audit trail
				if b.wal != nil {
					if cerr := b.wal.AppendControl(wal.FlushFail, database, measurement); cerr != nil {
						b.logger.Error().Err(cerr).Msg("Failed to write FLUSH_FAIL control record")
					}
				}
				task.cancel()
				continue
			}

			// Flush succeeded — write FLUSH_OK
			if b.wal != nil {
				if cerr := b.wal.AppendControl(wal.FlushOK, database, measurement); cerr != nil {
					b.logger.Error().Err(cerr).Msg("Failed to write FLUSH_OK control record")
				}
			}

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

			// Release timeout context resources
			task.cancel()
		}
	}
}
```

- [ ] **Step 2: Rewrite flushBufferLocked**

Replace the extraction/merge/restore pattern:

```go
// flushBufferLocked writes buffered data to Parquet and storage (synchronous version for periodic flush)
// Note: Caller must hold shard.mu lock
func (b *ArrowBuffer) flushBufferLocked(ctx context.Context, shard *bufferShard, bufferKey, database, measurement, trigger string) error {
	entry, exists := shard.buffers[bufferKey]
	if !exists || entry.isEmpty() {
		// Keep empty shell for schema caching; GC will clean if idle too long.
		if b.notifier != nil {
			b.notifier.OnFlushComplete(bufferKey)
		}
		return nil
	}

	recordCount := entry.recordCount

	// Move data out of entry
	data := entry.data
	validity := entry.validity
	tagCols := entry.tagColumns
	entry.data = make(map[string]interface{})
	entry.validity = nil
	entry.tagColumns = nil
	entry.recordCount = 0
	entry.estimatedBytes = 0

	// Release lock before expensive operations
	shard.mu.Unlock()

	// Wrap as TypedColumnBatch
	merged := &TypedColumnBatch{
		Data:       data,
		Validity:   validity,
		TagColumns: tagCols,
	}

	// Flush with data timestamp partitioning
	startTime := time.Now().UTC()
	if err := b.flushBufferLockedDataTime(ctx, bufferKey, database, measurement, merged, recordCount, startTime, entry.arrowSchema); err != nil {
		// Put data back into buffer
		b.prependFlushData(bufferKey, data, validity, tagCols, recordCount)
		b.logger.Warn().
			Err(err).
			Str("buffer_key", bufferKey).
			Int("records", recordCount).
			Msg("Flush failed — data restored to buffer, will retry")

		if b.wal != nil {
			if cerr := b.wal.AppendControl(wal.FlushFail, database, measurement); cerr != nil {
				b.logger.Error().Err(cerr).Msg("Failed to write FLUSH_FAIL control record")
			}
		}
		shard.mu.Lock() // Re-acquire lock for caller
		return err
	}

	// Flush succeeded
	if b.wal != nil {
		if cerr := b.wal.AppendControl(wal.FlushOK, database, measurement); cerr != nil {
			b.logger.Error().Err(cerr).Msg("Failed to write FLUSH_OK control record")
		}
	}

	if b.notifier != nil {
		b.notifier.OnFlushComplete(bufferKey)
	}

	metrics.Get().RecordBufferFlushRecords(trigger, recordCount)

	shard.mu.Lock()
	return nil
}
```

- [ ] **Step 3: Commit**

```bash
git add internal/ingest/arrow_writer.go
git commit -m "refactor: rewrite flushWorker and flushBufferLocked for growing buffer"
```

---

### Task 6: Add prependFlushData functions

**Files:**
- Modify: `internal/ingest/arrow_writer.go` (add new functions after flushBufferLockedDataTime)

- [ ] **Step 1: Implement prependFlushData and prependFlushDataToEntry**

```go
// prependFlushData prepends flushed data back into the buffer entry after a flush failure.
// Old data (the failed flush payload) is placed BEFORE new data that may have arrived
// concurrently during the flush attempt, preserving arrival-time order.
func (b *ArrowBuffer) prependFlushData(bufferKey string, data map[string]interface{}, validity map[string][]bool, tagColumns []string, flushedRows int) {
	shard := b.getShard(bufferKey)
	shard.mu.Lock()
	entry, exists := shard.buffers[bufferKey]
	if !exists {
		// Entry was deleted by eviction — recreate it
		entry = &bufferEntry{
			data:       data,
			validity:   validity,
			tagColumns: tagColumns,
			startTime:  time.Now().UTC(),
			recordCount: flushedRows,
		}
		shard.buffers[bufferKey] = entry
		shard.mu.Unlock()
		return
	}
	prependFlushDataToEntry(entry, data, validity, tagColumns, flushedRows)
	shard.mu.Unlock()
}

// prependFlushDataToEntry prepends flush data into an existing entry.
// Caller must hold shard.mu.
func prependFlushDataToEntry(entry *bufferEntry, data map[string]interface{}, validity map[string][]bool, tagColumns []string, flushedRows int) {
	// Prepend column data: old data first, new data second
	for name, col := range data {
		switch v := col.(type) {
		case []int64:
			entry.data[name] = append(v, entry.data[name].([]int64)...)
		case []float64:
			entry.data[name] = append(v, entry.data[name].([]float64)...)
		case []string:
			entry.data[name] = append(v, entry.data[name].([]string)...)
		case []bool:
			entry.data[name] = append(v, entry.data[name].([]bool)...)
		case []decimal128.Num:
			entry.data[name] = append(v, entry.data[name].([]decimal128.Num)...)
		}
	}

	// Prepend validity bitmaps
	if validity != nil {
		if entry.validity == nil {
			entry.validity = make(map[string][]bool)
		}
		for name, v := range validity {
			entry.validity[name] = append(v, entry.validity[name]...)
		}
		// Pad columns that are in entry.validity but not in validity
		for name := range entry.validity {
			if _, has := validity[name]; !has {
				padding := make([]bool, flushedRows)
				for i := range padding {
					padding[i] = true
				}
				entry.validity[name] = append(padding, entry.validity[name]...)
			}
		}
	}

	// Preserve tag columns
	if len(entry.tagColumns) == 0 && len(tagColumns) > 0 {
		entry.tagColumns = make([]string, len(tagColumns))
		copy(entry.tagColumns, tagColumns)
	}

	entry.recordCount += flushedRows
}
```

- [ ] **Step 2: Run existing tests to verify no breakage**

Run: `go test -tags=duckdb_arrow ./internal/ingest/ -v -count=1`
Expected: compilation succeeds; existing tests pass or fail only where they reference old batch API

- [ ] **Step 3: Commit**

```bash
git add internal/ingest/arrow_writer.go
git commit -m "feat: add prependFlushData for flush failure recovery"
```

---

### Task 7: Modify adaptive_flush.go flushCandidate

**Files:**
- Modify: `internal/ingest/adaptive_flush.go:221-268` (flushCandidate)

- [ ] **Step 1: Rewrite flushCandidate with growing buffer pattern**

```go
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
	data := entry.data
	validity := entry.validity
	tagCols := entry.tagColumns
	entry.data = make(map[string]interface{})
	entry.validity = nil
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
		data:        data,
		validity:    validity,
		tagColumns:  tagCols,
		recordCount: recordCount,
		trigger:     trigger,
		arrowSchema: entry.arrowSchema,
	}
	outcome := e.buffer.tryEnqueueFlush(task, flushCancel, c.bufferKey, recordCount)

	if outcome == flushQueued {
		// Data moved to task; entry is empty shell
	} else {
		// Enqueue failed — prepend data back
		prependFlushDataToEntry(entry, data, validity, tagCols, recordCount)
	}
	shard.mu.Unlock()
}
```

- [ ] **Step 2: Commit**

```bash
git add internal/ingest/adaptive_flush.go
git commit -m "refactor: rewrite flushCandidate for growing columnar buffer"
```

---

### Task 8: Modify evictOldestEntries

**Files:**
- Modify: `internal/ingest/arrow_writer.go:2154-2234` (evictOldestEntries)

- [ ] **Step 1: Rewrite the extraction + flush call**

Replace lines 2197-2215 (batch extraction and flushRecordsAsync call):

```go
		data := entry.data
		validity := entry.validity
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

			merged := &TypedColumnBatch{
				Data:       data,
				Validity:   validity,
				TagColumns: tagCols,
			}
			startTime := time.Now()
			if err := b.flushPartitionedData(context.Background(), oldestKey, parts[0], parts[1], merged, recordCount, "hard_limit", startTime, arrowSchema); err != nil {
				b.logger.Error().Err(err).
					Str("buffer_key", oldestKey).
					Msg("Buffer overflow: inline flush failed — data lost from memory, recoverable from WAL")
			}
		} else {
```

- [ ] **Step 2: Commit**

```bash
git add internal/ingest/arrow_writer.go
git commit -m "refactor: rewrite evictOldestEntries for growing buffer"
```

---

### Task 9: Modify SinceRefresh, MarkRefreshed, gcEmptyEntries

**Files:**
- Modify: `internal/ingest/arrow_writer.go:3636-3662` (SinceRefresh, MarkRefreshed)
- Modify: `internal/ingest/arrow_writer.go:2022-2044` (gcEmptyEntries)

- [ ] **Step 1: Rewrite SinceRefresh to return sub-slice wrapped as TypedColumnBatch**

```go
// SinceRefresh 返回指定 bufferKey 自上次 VIEW 刷新以来新增的数据。
func (b *ArrowBuffer) SinceRefresh(bufferKey string) ([]*TypedColumnBatch, error) {
	for i := uint32(0); i < b.shardCount; i++ {
		shard := b.shards[i]
		shard.mu.RLock()
		entry, ok := shard.buffers[bufferKey]
		if ok && !entry.isEmpty() && entry.refreshIndex < entry.recordCount {
			// Extract sub-slice of new rows for each column
			subData := make(map[string]interface{}, len(entry.data))
			for name, col := range entry.data {
				switch v := col.(type) {
				case []int64:
					subData[name] = v[entry.refreshIndex:]
				case []float64:
					subData[name] = v[entry.refreshIndex:]
				case []string:
					subData[name] = v[entry.refreshIndex:]
				case []bool:
					subData[name] = v[entry.refreshIndex:]
				case []decimal128.Num:
					subData[name] = v[entry.refreshIndex:]
				}
			}
			// Sub-slice validity if present
			var subValidity map[string][]bool
			if entry.validity != nil {
				subValidity = make(map[string][]bool, len(entry.validity))
				for name, v := range entry.validity {
					subValidity[name] = v[entry.refreshIndex:]
				}
			}
			shard.mu.RUnlock()
			return []*TypedColumnBatch{{
				Data:       subData,
				Validity:   subValidity,
				TagColumns: entry.tagColumns,
			}}, nil
		}
		shard.mu.RUnlock()
	}
	return nil, nil
}
```

- [ ] **Step 2: Rewrite MarkRefreshed with row-count semantics**

```go
// MarkRefreshed 更新指定 bufferKey 的刷新游标。
func (b *ArrowBuffer) MarkRefreshed(bufferKey string) {
	for i := uint32(0); i < b.shardCount; i++ {
		shard := b.shards[i]
		shard.mu.Lock()
		if entry, ok := shard.buffers[bufferKey]; ok {
			entry.refreshIndex = entry.recordCount
		}
		shard.mu.Unlock()
	}
}
```

- [ ] **Step 3: Update gcEmptyEntries isEmpty check**

Replace line 2031:
```go
// Old:
if len(entry.batches) == 0 && now.Sub(entry.startTime) > threshold {
// New:
if entry.isEmpty() && now.Sub(entry.startTime) > threshold {
```

- [ ] **Step 4: Commit**

```bash
git add internal/ingest/arrow_writer.go
git commit -m "refactor: update SinceRefresh/MarkRefreshed/gcEmptyEntries for growing buffer"
```

---

### Task 10: Delete obsolete functions

**Files:**
- Modify: `internal/ingest/arrow_writer.go` (delete functions)
- Modify: `internal/ingest/adaptive_flush.go` (clean up imports if needed)

- [ ] **Step 1: Delete obsolete functions and types**

Remove these functions:
- `mergeBatches` (lines ~2796-3009)
- `flushRecordsAsync` (lines ~2457-2529)
- `restoreBufferEntry` (lines ~2272-2368)
- `writeBackMergedData` (lines ~2405-2455)
- `typedBatchToColumns` (lines ~2370-2403)
- `flushWithDataTimePartitioning` (lines ~2531-2534) — wrapper only called from deleted `flushRecordsAsync`

Remove the recursion-guard type (only referenced in deleted functions):
- `ctxKeyRestoreFallback` struct (line ~71)

- [ ] **Step 2: Verify flushBufferLockedDataTime is kept**

`flushBufferLockedDataTime` is still called from `flushBufferLocked` — do NOT delete:
```go
func (b *ArrowBuffer) flushBufferLockedDataTime(ctx context.Context, bufferKey, database, measurement string, merged *TypedColumnBatch, recordCount int, startTime time.Time, arrowSchema *arrow.Schema) error {
    return b.flushPartitionedData(ctx, bufferKey, database, measurement, merged, recordCount, flushTypeSync, startTime, arrowSchema)
}
```

- [ ] **Step 3: Build to verify no compilation errors**

Run: `go build -tags=duckdb_arrow ./internal/ingest/...`
Expected: compilation succeeds

- [ ] **Step 4: Run tests to verify no breakage**

Run: `go test -tags=duckdb_arrow ./internal/ingest/ -v -count=1`
Expected: tests pass (tests that directly called deleted functions like `TestMergeBatches_SparseColumns` will fail — see Step 5)

- [ ] **Step 5: Remove tests for deleted functions**

Search for tests that call deleted functions:
```bash
grep -rn "mergeBatches\|flushRecordsAsync\|restoreBufferEntry\|writeBackMergedData\|typedBatchToColumns\|flushWithDataTimePartitioning\|ctxKeyRestoreFallback" internal/ingest/*_test.go
```
Remove or update any test functions that directly call the deleted functions. `TestMergeBatches_SparseColumns` should be removed (its behavior is now covered by `TestAppendTypedBatchToEntry_MixedNulls` and the VIEW integration tests).

- [ ] **Step 6: Run remaining tests**

Run: `go test -tags=duckdb_arrow ./internal/ingest/ -v -count=1`
Expected: all remaining tests PASS

- [ ] **Step 7: Commit**

```bash
git add internal/ingest/arrow_writer.go internal/ingest/arrow_writer_test.go
git commit -m "refactor: delete 7 obsolete functions and types (replaced by growing buffer + prepend)"
```

---

### Task 11: Integration test — end-to-end write + flush

**Files:**
- Create: `internal/ingest/arrow_writer_growing_buffer_test.go`

- [ ] **Step 1: Write integration test**

```go
package ingest

import (
	"context"
	"os"
	"testing"
	"time"

	"iedb/internal/config"

	"github.com/rs/zerolog"
)

func TestGrowingBuffer_WriteAndFlush(t *testing.T) {
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	cfg := &config.IngestConfig{
		MaxBufferSize:  10,
		MaxBufferAgeMS: 60000,
		Compression:    "snappy",
		FlushWorkers:   1,
		FlushQueueSize: 10,
		ShardCount:     4,
	}

	buf := NewArrowBuffer(cfg, &mockStorageBackend{}, logger)
	defer buf.Close()

	ctx := context.Background()

	// Write 5 rows — below threshold
	columns1 := map[string][]interface{}{
		"time": {int64(1), int64(2), int64(3), int64(4), int64(5)},
		"val":  {10.0, 20.0, 30.0, 40.0, 50.0},
	}
	err := buf.WriteColumnarDirect(ctx, "testdb", "cpu", columns1)
	if err != nil {
		t.Fatalf("Write 1 failed: %v", err)
	}

	// Verify no flush triggered yet (below threshold)
	if buf.totalFlushes.Load() != 0 {
		t.Error("flush should not have been triggered yet")
	}

	// Write 6 more rows — crosses threshold (total 11 > 10)
	columns2 := map[string][]interface{}{
		"time": {int64(6), int64(7), int64(8), int64(9), int64(10), int64(11)},
		"val":  {60.0, 70.0, 80.0, 90.0, 100.0, 110.0},
	}
	err = buf.WriteColumnarDirect(ctx, "testdb", "cpu", columns2)
	if err != nil {
		t.Fatalf("Write 2 failed: %v", err)
	}

	// Wait for async flush to complete
	time.Sleep(200 * time.Millisecond)

	if buf.totalFlushes.Load() == 0 {
		t.Error("flush should have been triggered after exceeding MaxBufferSize")
	}
}

func TestGrowingBuffer_AppendMultipleTypedBatches(t *testing.T) {
	// Test the core data accumulation pattern without full buffer infrastructure
	entry := &bufferEntry{
		data: make(map[string]interface{}),
	}

	batch1 := &TypedColumnBatch{
		Data: map[string]interface{}{
			"time":  []int64{100, 200, 300},
			"value": []float64{1.1, 2.2, 3.3},
			"host":  []string{"a", "b", "c"},
		},
		Validity:   nil,
		TagColumns: []string{"host"},
		Signature:  "host:str,time:i64,value:f64",
	}

	batch2 := &TypedColumnBatch{
		Data: map[string]interface{}{
			"time":  []int64{400, 500},
			"value": []float64{4.4, 5.5},
			"host":  []string{"d", "e"},
		},
		Validity:   nil,
		TagColumns: nil,
		Signature:  "host:str,time:i64,value:f64",
	}

	appendTypedBatchToEntry(entry, batch1, 3)
	appendTypedBatchToEntry(entry, batch2, 2)

	if entry.recordCount != 5 {
		t.Fatalf("recordCount = %d, want 5", entry.recordCount)
	}

	timeCol := entry.data["time"].([]int64)
	wantTime := []int64{100, 200, 300, 400, 500}
	for i, w := range wantTime {
		if timeCol[i] != w {
			t.Errorf("time[%d] = %d, want %d", i, timeCol[i], w)
		}
	}

	if len(entry.tagColumns) != 1 || entry.tagColumns[0] != "host" {
		t.Errorf("tagColumns = %v, want [host]", entry.tagColumns)
	}

	// Verify entry can be wrapped as TypedColumnBatch for flush
	merged := &TypedColumnBatch{
		Data:       entry.data,
		Validity:   entry.validity,
		TagColumns: entry.tagColumns,
	}
	if len(merged.Data) != 3 {
		t.Errorf("merged Data has %d columns, want 3", len(merged.Data))
	}
}

func TestGrowingBuffer_PrependFlushData(t *testing.T) {
	entry := &bufferEntry{
		data:       make(map[string]interface{}),
		validity:   nil,
		tagColumns: nil,
	}

	// Simulate: flush extracted data, new data arrived, flush failed, prepend
	extractedData := map[string]interface{}{
		"time": []int64{1, 2, 3},
		"val":  []float64{10.0, 20.0, 30.0},
	}
	extractedValidity := map[string][]bool{
		"val": {true, false, true},
	}

	// Simulate new data arriving while flush was in progress
	entry.data["time"] = append(entry.data["time"].([]int64), int64(4), int64(5))
	entry.data["val"] = append(entry.data["val"].([]float64), 40.0, 50.0)
	entry.recordCount = 2

	// Prepend extracted data
	prependFlushDataToEntry(entry, extractedData, extractedValidity, nil, 3)

	if entry.recordCount != 5 {
		t.Fatalf("recordCount = %d, want 5", entry.recordCount)
	}

	timeCol := entry.data["time"].([]int64)
	wantTime := []int64{1, 2, 3, 4, 5}
	for i, w := range wantTime {
		if timeCol[i] != w {
			t.Errorf("time[%d] = %d, want %d (old data must come first)", i, timeCol[i], w)
		}
	}

	valCol := entry.data["val"].([]float64)
	wantVal := []float64{10.0, 20.0, 30.0, 40.0, 50.0}
	for i, w := range wantVal {
		if valCol[i] != w {
			t.Errorf("val[%d] = %f, want %f", i, valCol[i], w)
		}
	}

	// Verify validity: old data had [true, false, true], new data was all valid
	valValid := entry.validity["val"]
	wantValid := []bool{true, false, true, true, true}
	for i, w := range wantValid {
		if valValid[i] != w {
			t.Errorf("val validity[%d] = %v, want %v", i, valValid[i], w)
		}
	}
}
```

- [ ] **Step 2: Run integration tests**

Run: `go test -tags=duckdb_arrow -run TestGrowingBuffer ./internal/ingest/ -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add internal/ingest/arrow_writer_growing_buffer_test.go
git commit -m "test: add integration tests for growing columnar buffer"
```

---

### Task 12: Run full test suite and fix issues

**Files:**
- All modified files

- [ ] **Step 1: Run all ingest tests**

Run: `go test -tags=duckdb_arrow -race ./internal/ingest/ -v -count=1`
Expected: all tests PASS

- [ ] **Step 2: Run VIEW integration tests**

Run: `go test -tags=duckdb_arrow -race ./internal/database/ -run ArrowView -v -count=1`
Expected: PASS

- [ ] **Step 3: Run full project test suite**

Run: `make test`
Expected: all tests PASS

- [ ] **Step 4: Fix any failures**

If tests fail, diagnose and fix. Common issues:
- Tests that access `entry.batches` directly — update to use `entry.data`
- Tests that call deleted functions — remove or rewrite
- Race detector flags — check lock discipline in prepend paths

- [ ] **Step 5: Final commit**

```bash
git add -A
git commit -m "test: fix remaining test failures after growing buffer refactor"
```

---

### Task 13: Remove unused imports and run linter

**Files:**
- Modify: `internal/ingest/arrow_writer.go` (imports)
- Modify: `internal/ingest/adaptive_flush.go` (imports)

- [ ] **Step 1: Remove unused imports**

After deleting mergeBatches etc., some imports may become unused:
```bash
goimports -w internal/ingest/arrow_writer.go internal/ingest/adaptive_flush.go
```

- [ ] **Step 2: Run linter**

Run: `make lint`
Expected: no new issues

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "chore: remove unused imports after growing buffer refactor"
```
