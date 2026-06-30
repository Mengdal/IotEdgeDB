# Eliminate schemaLRUCache — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove standalone `schemaLRUCache` (~120 lines), store `*arrow.Schema` in `bufferEntry` instead, keep empty-shell entries after flush to serve as schema cache.

**Architecture:** Schema inference moves from deferred (inside `WriteParquetColumnar` via `getSchema`) to eager (at `bufferEntry` creation time). Flush no longer deletes entries — it clears data fields while preserving `arrowSchema` and `schema` strings. An infrequent GC pass (every 30 periodicFlush cycles) removes entries idle > 2× maxBufferAge. Seven sites that iterate or restore entries gain `len(batches)==0` awareness.

**Tech Stack:** Go, Apache Arrow v18 (`arrow-go`), file: `internal/ingest/arrow_writer.go`

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `internal/ingest/arrow_writer.go` | Modify | All changes — delete, add, modify ~200 lines net -100 |

No new files. No other files touched.

---

### Task 1: Delete schemaLRUCache and related code

**Files:**
- Modify: `internal/ingest/arrow_writer.go:73-97` (delete structs + constructor)
- Modify: `internal/ingest/arrow_writer.go:100-198` (delete 7 methods)
- Modify: `internal/ingest/arrow_writer.go:211` (remove field from ArrowWriter)
- Modify: `internal/ingest/arrow_writer.go:231-233` (remove const)
- Modify: `internal/ingest/arrow_writer.go:252` (remove from NewArrowWriter)
- Modify: `internal/ingest/arrow_writer.go:391-450` (delete getSchema)

- [ ] **Step 1: Delete schemaCacheEntry and schemaLRUCache type definitions**

Delete lines 73–89:
```go
// DELETE these lines (73-89):
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
```

- [ ] **Step 2: Delete newSchemaLRUCache and 7 methods**

Delete lines 91–198. Entire block from `// newSchemaLRUCache creates...` through the closing `}` of `evictLRU()`.

- [ ] **Step 3: Remove schemaCache field from ArrowWriter struct**

At line 210–211, delete:
```go
	// LRU Schema cache (measurement -> schema) with bounded size
	schemaCache *schemaLRUCache
```

- [ ] **Step 4: Remove schemaCache from NewArrowWriter**

Delete lines 231–233:
```go
	// Schema cache capacity - 1000 schemas is ~100-200KB memory
	// Most deployments have <100 unique measurement/schema combinations
	const schemaCacheCapacity = 1000
```

At line 252, delete:
```go
	schemaCache:     newSchemaLRUCache(schemaCacheCapacity),
```

- [ ] **Step 5: Delete getSchema method**

Delete entire function (lines 391–450): `func (w *ArrowWriter) getSchema(...)` through its closing `}`.

- [ ] **Step 6: Verify compilation fails (expected)**

Run: `go build -tags=duckdb_arrow ./internal/ingest/`
Expected: compilation errors (WriteParquetColumnar still calls `w.getSchema`)

- [ ] **Step 7: Commit**

```bash
git add internal/ingest/arrow_writer.go
git commit -m "refactor: delete schemaLRUCache and getSchema, pending call-site fixes"
```

---

### Task 2: Add arrowSchema to bufferEntry + isEmpty helper

**Files:**
- Modify: `internal/ingest/arrow_writer.go:699-706` (bufferEntry struct)
- Create: `internal/ingest/arrow_writer.go` (isEmpty method, after bufferEntry)

- [ ] **Step 1: Add arrowSchema field to bufferEntry**

At line 699, modify the struct to add `arrowSchema` after `schema`:
```go
type bufferEntry struct {
	batches        []*TypedColumnBatch // buffered data batches (immutable)
	startTime      time.Time           // first record arrival time
	recordCount    int                 // total record count
	estimatedBytes uint64              // estimated memory usage
	schema         string              // column signature for schema evolution
	arrowSchema    *arrow.Schema       // inferred Arrow schema (nil until first flush preparation)
	refreshIndex   int                 // Arrow VIEW incremental refresh cursor
}
```

- [ ] **Step 2: Add isEmpty method**

After the `bufferEntry` struct definition (after line 706), add:
```go
// isEmpty returns true when the entry has no buffered data batches.
// Such entries are "empty shells" kept to preserve the cached arrowSchema
// across flush cycles — they are eligible for GC after extended idleness.
func (e *bufferEntry) isEmpty() bool {
	return len(e.batches) == 0
}
```

- [ ] **Step 3: Verify compilation**

Run: `go build -tags=duckdb_arrow ./internal/ingest/`
Expected: same errors as before (no regressions from this change)

- [ ] **Step 4: Commit**

```bash
git add internal/ingest/arrow_writer.go
git commit -m "feat: add arrowSchema field to bufferEntry and isEmpty helper"
```

---

### Task 3: Move schema inference to entry creation time

**Files:**
- Modify: `internal/ingest/arrow_writer.go:531-534` (WriteParquetColumnar signature)
- Modify: `internal/ingest/arrow_writer.go:1641-1652` (writeColumnarInternal entry creation)
- Modify: `internal/ingest/arrow_writer.go:1764-1777` (writeTypedColumnarInternal entry creation)

- [ ] **Step 1: Modify WriteParquetColumnar to accept optional schema parameter**

Change the function signature (line 531) and body (lines 532-536):
```go
func (w *ArrowWriter) WriteParquetColumnar(ctx context.Context, measurement string, columns map[string]interface{}, validity map[string][]bool, tagColumns []string, decimalCols map[string]config.DecimalSpec, schema *arrow.Schema) ([]byte, error) {
	// Use provided schema, or infer as fallback (defense-in-depth)
	if schema == nil {
		var err error
		schema, err = w.inferSchema(columns, tagColumns, decimalCols)
		if err != nil {
			return nil, fmt.Errorf("failed to infer schema: %w", err)
		}
	}
	// ... rest of function unchanged ...
```

- [ ] **Step 2: Fix all call sites of WriteParquetColumnar**

There are exactly 2 call sites, both in `flushPartitionedData`:

Line 2839 (single-hour path):
```go
// Before:
parquetData, err := b.writer.WriteParquetColumnar(ctx, measurement, sorted.Data, sorted.Validity, sorted.TagColumns, decimalCols)
// After:
parquetData, err := b.writer.WriteParquetColumnar(ctx, measurement, sorted.Data, sorted.Validity, sorted.TagColumns, decimalCols, nil)
```

Line 2907 (multi-hour path):
```go
// Before:
parquetData, err := b.writer.WriteParquetColumnar(ctx, measurement, sorted.Data, sorted.Validity, sorted.TagColumns, decimalCols)
// After:
parquetData, err := b.writer.WriteParquetColumnar(ctx, measurement, sorted.Data, sorted.Validity, sorted.TagColumns, decimalCols, nil)
```

Note: `nil` is passed because in the next step we'll wire the real schema from `bufferEntry` through to the flush path. This keeps compilation working incrementally.

- [ ] **Step 3: Add schema inference at entry creation in writeColumnarInternal**

At lines 1641–1652, after entry creation, add schema inference:

```go
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

	// NEW: Infer Arrow schema eagerly on first entry creation.
	// The typedColumns.Data map is already populated with typed slices
	// from convertColumnsToTyped — we can infer directly from it.
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
```

- [ ] **Step 4: Same schema inference in writeTypedColumnarInternal**

At lines 1764–1777, after entry creation, add the same block:
```go
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

	// NEW: Infer Arrow schema eagerly on first entry creation.
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
```

- [ ] **Step 5: Verify compilation**

Run: `go build -tags=duckdb_arrow ./internal/ingest/`
Expected: clean build

- [ ] **Step 6: Commit**

```bash
git add internal/ingest/arrow_writer.go
git commit -m "feat: move schema inference to bufferEntry creation time"
```

---

### Task 4: Change delete→clear in flush paths, wire schema through flush

**Files:**
- Modify: `internal/ingest/arrow_writer.go:1687-1694` (writeColumnarInternal delete→clear)
- Modify: `internal/ingest/arrow_writer.go:1806-1812` (writeTypedColumnarInternal delete→clear)
- Modify: `internal/ingest/arrow_writer.go:2986-2987` (flushBufferLocked delete→clear)
- Modify: `internal/ingest/arrow_writer.go:2787-2788` (wire schema to flushPartitionedData)
- Modify: `internal/ingest/arrow_writer.go:2794` (flushPartitionedData accepts schema)
- Modify: `internal/ingest/arrow_writer.go:2839,2907` (pass schema to WriteParquetColumnar)
- Modify: `internal/ingest/arrow_writer.go:2963-2965` (flushBufferLocked wires schema)

- [ ] **Step 1: Change delete→clear in writeColumnarInternal**

Lines 1687–1694:
```go
// Before:
if outcome == flushQueued {
	// Only delete after confirmed enqueue — avoids data loss
	// when flushQueue is full or buffer is closing.
	delete(shard.buffers, bufferKey)
	shouldFlush = true
}

// After:
if outcome == flushQueued {
	// Clear data fields but keep entry as schema cache shell.
	// The arrowSchema and schema fields are preserved for reuse.
	entry.batches = nil
	entry.recordCount = 0
	entry.estimatedBytes = 0
	shouldFlush = true
}
```

- [ ] **Step 2: Change delete→clear in writeTypedColumnarInternal**

Lines 1806–1812 — same pattern:
```go
// Before:
if outcome == flushQueued {
	delete(shard.buffers, bufferKey)
	shouldFlush = true
}

// After:
if outcome == flushQueued {
	// Clear data fields but keep entry as schema cache shell.
	entry.batches = nil
	entry.recordCount = 0
	entry.estimatedBytes = 0
	shouldFlush = true
}
```

- [ ] **Step 3: Change delete→clear in flushBufferLocked**

Lines 2986–2987:
```go
// Before:
// Clear buffer immediately
delete(shard.buffers, bufferKey)

// After:
// Clear data fields but keep entry as schema cache shell.
entry.batches = nil
entry.recordCount = 0
entry.estimatedBytes = 0
```

- [ ] **Step 4: Wire schema from entry into flushPartitionedData**

Modify `flushRecordsAsync` (around line 2744) to extract `arrowSchema` before calling flush — we need to find the entry to get the schema. Since `flushRecordsAsync` is called from the worker path where the entry may have been cleared, the schema needs to be captured before the entry is cleared. 

The cleanest approach: capture `arrowSchema` in the `flushTask` struct. Add to `flushTask` (line 746–755):
```go
type flushTask struct {
	ctx         context.Context
	cancel      context.CancelFunc
	bufferKey   string
	database    string
	measurement string
	records     []interface{}
	recordCount int
	trigger     string // "size", "age", "hard_limit", or "manual"
	arrowSchema *arrow.Schema // NEW: cached schema from bufferEntry
}
```

At both task creation sites (writeColumnarInternal and writeTypedColumnarInternal), add:
```go
task := flushTask{
	// ... existing fields ...
	arrowSchema: entry.arrowSchema,
}
```

In `flushWorker` (line 2368), pass schema to `flushRecordsAsync`:
```go
if b.flushRecordsAsync(task.ctx, task.bufferKey, task.database, task.measurement, task.records, task.recordCount, task.arrowSchema) {
```

Modify `flushRecordsAsync` signature (line 2712):
```go
func (b *ArrowBuffer) flushRecordsAsync(ctx context.Context, bufferKey, database, measurement string, records []interface{}, recordCount int, arrowSchema *arrow.Schema) (flushed bool) {
```

And pass it through to `flushWithDataTimePartitioning` → `flushPartitionedData`:
```go
// In flushRecordsAsync, line 2744:
if err := b.flushWithDataTimePartitioning(ctx, bufferKey, database, measurement, merged, recordCount, startTime, arrowSchema); err != nil {
```

Modify `flushWithDataTimePartitioning` (line 2787):
```go
func (b *ArrowBuffer) flushWithDataTimePartitioning(ctx context.Context, bufferKey, database, measurement string, merged *TypedColumnBatch, recordCount int, startTime time.Time, arrowSchema *arrow.Schema) error {
	return b.flushPartitionedData(ctx, bufferKey, database, measurement, merged, recordCount, flushTypeAsync, startTime, arrowSchema)
}
```

Modify `flushPartitionedData` signature (line 2794):
```go
func (b *ArrowBuffer) flushPartitionedData(ctx context.Context, bufferKey, database, measurement string, merged *TypedColumnBatch, recordCount int, flushType string, startTime time.Time, arrowSchema *arrow.Schema) error {
```

Replace `nil` at both WriteParquetColumnar call sites (lines 2839, 2907) with `arrowSchema`:
```go
// Before:
parquetData, err := b.writer.WriteParquetColumnar(ctx, measurement, sorted.Data, sorted.Validity, sorted.TagColumns, decimalCols, nil)
// After:
parquetData, err := b.writer.WriteParquetColumnar(ctx, measurement, sorted.Data, sorted.Validity, sorted.TagColumns, decimalCols, arrowSchema)
```

- [ ] **Step 5: Wire schema through sync flush path (flushBufferLocked)**

Modify `flushBufferLockedDataTime` (line 3042–3045) — need schema from entry:
```go
// Before:
func (b *ArrowBuffer) flushBufferLockedDataTime(ctx context.Context, bufferKey, database, measurement string, merged *TypedColumnBatch, recordCount int, startTime time.Time) error {
	return b.flushPartitionedData(ctx, bufferKey, database, measurement, merged, recordCount, flushTypeSync, startTime)
}

// After:
func (b *ArrowBuffer) flushBufferLockedDataTime(ctx context.Context, bufferKey, database, measurement string, merged *TypedColumnBatch, recordCount int, startTime time.Time, arrowSchema *arrow.Schema) error {
	return b.flushPartitionedData(ctx, bufferKey, database, measurement, merged, recordCount, flushTypeSync, startTime, arrowSchema)
}
```

In `flushBufferLocked`, pass `entry.arrowSchema` (line 3003):
```go
// Before:
if err := b.flushBufferLockedDataTime(ctx, bufferKey, database, measurement, merged, recordCount, startTime); err != nil {
// After:
if err := b.flushBufferLockedDataTime(ctx, bufferKey, database, measurement, merged, recordCount, startTime, entry.arrowSchema); err != nil {
```

- [ ] **Step 6: Wire schema through conflict and aged flush paths**

`flushTypeConflicts` calls `flushBufferLocked` (line 1011) — entry already has `arrowSchema`, no change needed.

`flushAgedBuffers` calls `flushBufferLocked` (line 2309) — entry already has `arrowSchema`, no change needed.

`Close` calls `flushBufferLocked` (line 3836) — entry already has `arrowSchema`, no change needed.

`FlushAll` calls `flushBufferLocked` (line 3753) — entry already has `arrowSchema`, no change needed.

- [ ] **Step 7: Fix restoreBufferEntry and writeBackMergedData callers**

In `restoreBufferEntry`, when called from `flushRecordsAsync` (line 2729), `arrowSchema` is now an explicit parameter. Update the call:
```go
b.restoreBufferEntry(bufferKey, records, arrowSchema)
```

Modify `restoreBufferEntry` signature to accept and preserve arrowSchema:
```go
func (b *ArrowBuffer) restoreBufferEntry(bufferKey string, batches []interface{}, arrowSchema *arrow.Schema) {
	// ... existing logic ...
	shard.buffers[bufferKey] = &bufferEntry{
		batches:        typedBatches,
		startTime:      time.Now(),
		recordCount:    recordCount,
		estimatedBytes: estimatedBytes,
		schema:         schema,
		arrowSchema:    arrowSchema,   // preserve
	}
}
```

In `writeBackMergedData` (called from flushRecordsAsync line 2761), update the merge-batch → re-ingest path to skip WAL (already done) and preserve schema. This path calls `WriteColumnarDirectNoWAL` which eventually calls `writeColumnarInternal` — the entry will be found (empty shell) and `arrowSchema` is already set on the empty shell, so no additional change needed.

- [ ] **Step 8: Verify compilation**

Run: `go build -tags=duckdb_arrow ./internal/ingest/`
Expected: clean build

- [ ] **Step 9: Quick smoke test**

Run: `go test -tags=duckdb_arrow -run TestArrowBuffer_WriteAndFlush -v ./internal/ingest/ -count=1`
Expected: PASS (if this test exists, otherwise skip to next step)

- [ ] **Step 10: Commit**

```bash
git add internal/ingest/arrow_writer.go
git commit -m "feat: clear entries instead of delete, wire arrowSchema through flush chain"
```

---

### Task 5: Update all skip/restore conditions (7 sites)

**Files:**
- Modify: `internal/ingest/arrow_writer.go:967-1023` (flushTypeConflicts)
- Modify: `internal/ingest/arrow_writer.go:2257-2317` (flushAgedBuffers)
- Modify: `internal/ingest/arrow_writer.go:2234-2254` (computeNextFlushDeadline)
- Modify: `internal/ingest/arrow_writer.go:2542-2623` (restoreBufferEntry condition)
- Modify: `internal/ingest/arrow_writer.go:3799-3852` (Close)
- Modify: `internal/ingest/arrow_writer.go:3729-3765` (FlushAll)
- Modify: `internal/ingest/arrow_writer.go:2410-2421` (AllBufferKeys)

- [ ] **Step 1: flushTypeConflicts — skip empty entries**

At line 979, in the conflict-collection loop, add isEmpty check:
```go
for key, entry := range shard.buffers {
	if entry.isEmpty() {   // NEW: skip empty shells
		continue
	}
	if strings.HasPrefix(key, prefix) && hasTypeConflict(entry.schema, newSig) {
		conflictKeys = append(conflictKeys, key)
	}
}
```

- [ ] **Step 2: flushAgedBuffers — skip empty entries**

At line 2273, in the aged-key collection loop:
```go
for key, entry := range shard.buffers {
	if entry.isEmpty() {   // NEW: skip empty shells
		continue
	}
	age := now.Sub(entry.startTime)
	if age >= threshold {
		agedKeys = append(agedKeys, key)
	}
}
```

- [ ] **Step 3: computeNextFlushDeadline — skip empty entries**

At line 2241:
```go
for _, entry := range shard.buffers {
	if entry.isEmpty() {   // NEW: skip empty shells
		continue
	}
	if expiry := entry.startTime.Add(maxAge); expiry.Before(earliest) {
		earliest = expiry
	}
}
```

- [ ] **Step 4: restoreBufferEntry — accept empty shell as restorable**

At line 2614:
```go
// Before:
if _, exists := shard.buffers[bufferKey]; !exists {
	shard.buffers[bufferKey] = &bufferEntry{
		// ...
	}
}

// After:
entry, exists := shard.buffers[bufferKey]
if !exists || entry.isEmpty() {
	var retainedArrowSchema *arrow.Schema
	var retainedSchemaStr string
	if entry != nil {
		retainedArrowSchema = entry.arrowSchema
		retainedSchemaStr = entry.schema
	}
	if retainedArrowSchema == nil {
		retainedArrowSchema = arrowSchema // from parameter added in Task 4
	}
	shard.buffers[bufferKey] = &bufferEntry{
		batches:        typedBatches,
		startTime:      time.Now(),
		recordCount:    recordCount,
		estimatedBytes: estimatedBytes,
		schema:         retainedSchemaStr,
		arrowSchema:    retainedArrowSchema,
	}
}
```

- [ ] **Step 5: Close — skip empty entries in final drain**

At line 3823:
```go
for key, entry := range shard.buffers {
	if entry.isEmpty() {   // NEW: skip empty shells
		continue
	}
	keys = append(keys, key)
}
```

- [ ] **Step 6: FlushAll — skip empty entries**

At line 3741:
```go
for key, entry := range shard.buffers {
	if entry.isEmpty() {   // NEW: skip empty shells
		continue
	}
	keys = append(keys, key)
}
```

- [ ] **Step 7: AllBufferKeys — filter out empty entries**

At line 2415:
```go
for key, entry := range shard.buffers {
	if entry.isEmpty() {   // NEW: skip empty shells
		continue
	}
	keys = append(keys, key)
}
```

Note: `AllBufferKeys` now requires read access to the entry (not just the key). The lock is already `RLock`, so this is safe.

- [ ] **Step 8: Handle empty-shell reuse in write paths**

In `writeColumnarInternal`, when finding an existing entry that's an empty shell (around line 1640):
```go
entry, exists := shard.buffers[bufferKey]
if !exists {
	entry = &bufferEntry{
		startTime: time.Now().UTC(),
		schema:    newSignature,
	}
	shard.buffers[bufferKey] = entry
	// ... newBufferCh signal ...
} else if entry.isEmpty() {
	// Reuse empty shell: preserve arrowSchema/schema, reset data fields
	entry.startTime = time.Now().UTC()
	entry.recordCount = 0
	// batches already nil, estimatedBytes already 0
	// arrowSchema and schema preserved
}
```

Same pattern in `writeTypedColumnarInternal` (around line 1762).

- [ ] **Step 9: Verify compilation**

Run: `go build -tags=duckdb_arrow ./internal/ingest/`
Expected: clean build

- [ ] **Step 10: Commit**

```bash
git add internal/ingest/arrow_writer.go
git commit -m "feat: add empty-shell awareness to all entry iteration sites"
```

---

### Task 6: Add GC and integrate into periodicFlush

**Files:**
- Modify: `internal/ingest/arrow_writer.go:2181-2227` (periodicFlush)

- [ ] **Step 1: Add gcEmptyEntries method**

Add after `computeNextFlushDeadline` (after line 2254):
```go
// gcEmptyEntries removes buffer entries that have been empty (no batches)
// for longer than 2× maxBufferAge. These are schema-cache shells whose
// measurement hasn't been written to recently. The factor of 2 provides
// a safety margin so entries aren't collected between frequent flushes.
func (b *ArrowBuffer) gcEmptyEntries() {
	threshold := b.maxBufferAge * 2
	now := time.Now().UTC()
	var cleaned int

	for i := uint32(0); i < b.shardCount; i++ {
		shard := b.shards[i]
		shard.mu.Lock()
		for key, entry := range shard.buffers {
			if len(entry.batches) == 0 && now.Sub(entry.startTime) > threshold {
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
```

- [ ] **Step 2: Integrate into periodicFlush**

Add cycle counter and GC trigger. Modify `periodicFlush` (lines 2184–2227):
```go
func (b *ArrowBuffer) periodicFlush() {
	defer b.wg.Done()

	var cycleCount int64  // NEW

	for {
		// ... existing adaptive flush check unchanged ...

		select {
		case <-b.ctx.Done():
			return

		case <-b.newBufferCh:
			// ... existing logic unchanged ...

		case <-b.flushTimer.C:
			b.flushAgedBuffers()
			// Rearm the timer for the next oldest buffer expiry.
			b.flushDeadline = b.computeNextFlushDeadline()
			b.flushTimer.Reset(time.Until(b.flushDeadline))

			// NEW: Periodic empty-shell GC (every 30 cycles)
			cycleCount++
			if cycleCount%30 == 0 {
				b.gcEmptyEntries()
			}
		}
	}
}
```

- [ ] **Step 3: Verify compilation**

Run: `go build -tags=duckdb_arrow ./internal/ingest/`
Expected: clean build

- [ ] **Step 4: Commit**

```bash
git add internal/ingest/arrow_writer.go
git commit -m "feat: add empty-entry GC every 30 periodicFlush cycles"
```

---

### Task 7: Run full test suite and fix issues

- [ ] **Step 1: Run all arrow_writer tests**

```bash
go test -tags=duckdb_arrow -race -v -run 'TestArrow' ./internal/ingest/ -count=1
```

Expected: PASS on all tests. If any fail, diagnose and fix before proceeding.

- [ ] **Step 2: Run full ingest package tests with race detector**

```bash
go test -tags=duckdb_arrow -race -v ./internal/ingest/ -count=1
```

Expected: All tests pass with no race warnings.

- [ ] **Step 3: Run benchmarks to verify no performance regression**

```bash
go test -tags=duckdb_arrow -bench=. -benchmem ./internal/ingest/ -benchtime=5s
```

Compare: schema inference now happens at write time instead of flush time. The write path should show slightly higher CPU (microseconds for schema inference), but flush path should be unchanged (Parquet I/O dominates). Acceptable regression: <5% on write benchmark.

- [ ] **Step 4: Verify no unused imports**

Run: `go build -tags=duckdb_arrow ./internal/ingest/`
Check: no compilation errors about unused imports (the `sync` import was used by schemaLRUCache lock — if no other code uses `sync.RWMutex`, it may need removal).

If `sync` is now unused, remove it from imports and verify build.

- [ ] **Step 5: Commit final fixes**

```bash
git add internal/ingest/arrow_writer.go
git commit -m "fix: test fixes and unused import cleanup from schema cache removal"
```

---

### Task 8: Run fmt and lint

- [ ] **Step 1: Format code**

```bash
gofmt -w internal/ingest/arrow_writer.go
```

- [ ] **Step 2: Lint**

```bash
golangci-lint run internal/ingest/arrow_writer.go
```

Expected: no new warnings. Fix any that appear.

- [ ] **Step 3: Final commit**

```bash
git add internal/ingest/arrow_writer.go
git commit -m "style: gofmt arrow_writer.go after schema cache removal"
```

---

## Summary

| Task | Description | Net Lines |
|------|-------------|-----------|
| 1 | Delete schemaLRUCache + getSchema | -175 |
| 2 | Add arrowSchema + isEmpty | +12 |
| 3 | Move schema inference to entry creation | +35 |
| 4 | delete→clear + wire schema through flush | +20 (struct + params) |
| 5 | 7 skip/restore condition updates | +25 |
| 6 | GC + periodicFlush integration | +30 |
| 7 | Tests and fixes | ~0 |
| 8 | Fmt and lint | ~0 |
| **Total** | | **~ -53 net** |
