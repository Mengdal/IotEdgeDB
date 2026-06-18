# ColumnData Merge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Merge bufferEntry's dual-map (data + validity) into single `map[string]ColumnData`, upgrade 10 dispatch functions to ColumnData-aware signatures, eliminate 27-line validity backfill logic.

**Architecture:** (1) Add exported `ColumnData` struct; (2) Upgrade dispatch functions to take/return `ColumnData` with built-in validity handling; (3) Replace `bufferEntry.data` + `bufferEntry.validity` with `columns map[string]ColumnData`; (4) Simplify all callers — validity operations become transparent.

**Tech Stack:** Go 1.22+, duckdb_arrow build tag, Apache Arrow v18, CGO

**Spec:** `docs/superpowers/specs/2026-06-19-columndata-merge-design.md`

## Global Constraints

- Build tag `duckdb_arrow` required for all `go test` / `go build` commands
- Race detector enabled: `-race` for all test runs
- Code must match existing patterns in arrow_writer.go (comment density, naming, idiom)
- All existing tests must pass after adaptation
- ColumnData must be exported (used by database package)
- Pre-existing benchmarks must not regress

---

### Task 1: Add ColumnData type + upgrade dispatch functions

**Files:**
- Modify: `internal/ingest/arrow_writer.go`

**Interfaces:**
- Produces:
  - `type ColumnData struct { Data any; Validity []bool }`
  - `colLen(c ColumnData) int`
  - `colMake(firstVal any, n int) ColumnData`
  - `colAppend(dst, src ColumnData) ColumnData`
  - `colSlice(c ColumnData, indices []int) ColumnData`
  - `colPermute(c ColumnData, indices []int) ColumnData`
  - `colSliceFrom(c ColumnData, start int) ColumnData`
  - `colLess(c ColumnData, i, j int) bool`
  - `colEstBytesPerRow(c ColumnData) uint64`
  - `colIthVal(c ColumnData, i int) any`
  - `colTypeTag(c ColumnData) string`

- [ ] **Step 1: Add ColumnData type before dispatch functions**

Insert before `colLen` (L590):
```go
// ColumnData bundles a typed column slice with its null bitmap.
// Data is the underlying typed slice ([]int64, []float64, etc.).
// Validity is nil when all values are valid; when non-nil, len(Validity) == len(slice).
type ColumnData struct {
    Data     any
    Validity []bool
}
```

- [ ] **Step 2: Upgrade colLen**

```go
func colLen(c ColumnData) int {
    switch v := c.Data.(type) {
    case []int64:
        return len(v)
    case []float64:
        return len(v)
    case []string:
        return len(v)
    case []bool:
        return len(v)
    case []decimal128.Num:
        return len(v)
    default:
        return 0
    }
}
```

- [ ] **Step 3: Upgrade colMake**

```go
func colMake(firstVal any, n int) ColumnData {
    switch firstVal.(type) {
    case int64:
        return ColumnData{Data: make([]int64, n)}
    case float64:
        return ColumnData{Data: make([]float64, n)}
    case string:
        return ColumnData{Data: make([]string, n)}
    case bool:
        return ColumnData{Data: make([]bool, n)}
    case decimal128.Num:
        return ColumnData{Data: make([]decimal128.Num, n)}
    default:
        return ColumnData{}
    }
}
```

- [ ] **Step 4: Upgrade colAppend — now merges validity**

```go
// colAppend concatenates two ColumnData values. dst and src must have the same element type.
// Validity bitmaps are merged: src.Validity is appended to dst.Validity.
// When only one side has Validity, the other side is treated as all-valid (true).
func colAppend(dst, src ColumnData) ColumnData {
    switch v := src.Data.(type) {
    case []int64:
        if dst.Data == nil {
            return src
        }
        return ColumnData{
            Data:     append(dst.Data.([]int64), v...),
            Validity: mergeValidity(dst.Validity, src.Validity, colLenRaw(dst.Data), len(v)),
        }
    case []float64:
        if dst.Data == nil {
            return src
        }
        return ColumnData{
            Data:     append(dst.Data.([]float64), v...),
            Validity: mergeValidity(dst.Validity, src.Validity, colLenRaw(dst.Data), len(v)),
        }
    case []string:
        if dst.Data == nil {
            return src
        }
        return ColumnData{
            Data:     append(dst.Data.([]string), v...),
            Validity: mergeValidity(dst.Validity, src.Validity, colLenRaw(dst.Data), len(v)),
        }
    case []bool:
        if dst.Data == nil {
            return src
        }
        return ColumnData{
            Data:     append(dst.Data.([]bool), v...),
            Validity: mergeValidity(dst.Validity, src.Validity, colLenRaw(dst.Data), len(v)),
        }
    case []decimal128.Num:
        if dst.Data == nil {
            return src
        }
        return ColumnData{
            Data:     append(dst.Data.([]decimal128.Num), v...),
            Validity: mergeValidity(dst.Validity, src.Validity, colLenRaw(dst.Data), len(v)),
        }
    default:
        return dst
    }
}

// colLenRaw returns len of a typed slice any, for internal use without ColumnData wrapper.
func colLenRaw(col any) int {
    switch v := col.(type) {
    case []int64:
        return len(v)
    case []float64:
        return len(v)
    case []string:
        return len(v)
    case []bool:
        return len(v)
    case []decimal128.Num:
        return len(v)
    default:
        return 0
    }
}

// mergeValidity merges two validity bitmaps when appending columns.
// dstLen is the number of pre-existing rows that dst covers.
// srcLen is the number of rows in the source batch.
func mergeValidity(dst, src []bool, dstLen, srcLen int) []bool {
    if dst == nil && src == nil {
        return nil
    }
    if dst == nil {
        dst = make([]bool, dstLen)
        for i := range dst {
            dst[i] = true
        }
    }
    if src == nil {
        pad := make([]bool, srcLen)
        for i := range pad {
            pad[i] = true
        }
        return append(dst, pad...)
    }
    return append(dst, src...)
}
```

- [ ] **Step 5: Upgrade colSlice — slices validity alongside data**

```go
func colSlice(c ColumnData, indices []int) ColumnData {
    n := len(indices)
    switch v := c.Data.(type) {
    case []int64:
        out := make([]int64, n)
        vl := len(v)
        for i, idx := range indices {
            if idx < vl {
                out[i] = v[idx]
            }
        }
        return ColumnData{Data: out, Validity: sliceValidity(c.Validity, indices)}
    case []float64:
        out := make([]float64, n)
        vl := len(v)
        for i, idx := range indices {
            if idx < vl {
                out[i] = v[idx]
            }
        }
        return ColumnData{Data: out, Validity: sliceValidity(c.Validity, indices)}
    case []string:
        out := make([]string, n)
        vl := len(v)
        for i, idx := range indices {
            if idx < vl {
                out[i] = v[idx]
            }
        }
        return ColumnData{Data: out, Validity: sliceValidity(c.Validity, indices)}
    case []bool:
        out := make([]bool, n)
        vl := len(v)
        for i, idx := range indices {
            if idx < vl {
                out[i] = v[idx]
            }
        }
        return ColumnData{Data: out, Validity: sliceValidity(c.Validity, indices)}
    case []decimal128.Num:
        out := make([]decimal128.Num, n)
        vl := len(v)
        for i, idx := range indices {
            if idx < vl {
                out[i] = v[idx]
            }
        }
        return ColumnData{Data: out, Validity: sliceValidity(c.Validity, indices)}
    default:
        return c
    }
}

func sliceValidity(v []bool, indices []int) []bool {
    if v == nil {
        return nil
    }
    out := make([]bool, len(indices))
    vl := len(v)
    for i, idx := range indices {
        if idx < vl {
            out[i] = v[idx]
        }
    }
    return out
}
```

- [ ] **Step 6: Upgrade colSliceFrom**

```go
func colSliceFrom(c ColumnData, start int) ColumnData {
    switch v := c.Data.(type) {
    case []int64:
        return ColumnData{Data: v[start:], Validity: sliceValidityFrom(c.Validity, start)}
    case []float64:
        return ColumnData{Data: v[start:], Validity: sliceValidityFrom(c.Validity, start)}
    case []string:
        return ColumnData{Data: v[start:], Validity: sliceValidityFrom(c.Validity, start)}
    case []bool:
        return ColumnData{Data: v[start:], Validity: sliceValidityFrom(c.Validity, start)}
    case []decimal128.Num:
        return ColumnData{Data: v[start:], Validity: sliceValidityFrom(c.Validity, start)}
    default:
        return c
    }
}

func sliceValidityFrom(v []bool, start int) []bool {
    if v == nil {
        return nil
    }
    return v[start:]
}
```

- [ ] **Step 7: Upgrade colPermute — permutes validity alongside data**

```go
func colPermute(c ColumnData, indices []int) ColumnData {
    n := len(indices)
    switch v := c.Data.(type) {
    case []int64:
        out := make([]int64, n)
        for i, idx := range indices {
            out[i] = v[idx]
        }
        return ColumnData{Data: out, Validity: permuteValidity(c.Validity, indices)}
    case []float64:
        out := make([]float64, n)
        for i, idx := range indices {
            out[i] = v[idx]
        }
        return ColumnData{Data: out, Validity: permuteValidity(c.Validity, indices)}
    case []string:
        out := make([]string, n)
        for i, idx := range indices {
            out[i] = v[idx]
        }
        return ColumnData{Data: out, Validity: permuteValidity(c.Validity, indices)}
    case []bool:
        out := make([]bool, n)
        for i, idx := range indices {
            out[i] = v[idx]
        }
        return ColumnData{Data: out, Validity: permuteValidity(c.Validity, indices)}
    case []decimal128.Num:
        out := make([]decimal128.Num, n)
        for i, idx := range indices {
            out[i] = v[idx]
        }
        return ColumnData{Data: out, Validity: permuteValidity(c.Validity, indices)}
    default:
        return c
    }
}

func permuteValidity(v []bool, indices []int) []bool {
    if v == nil {
        return nil
    }
    out := make([]bool, len(indices))
    for i, idx := range indices {
        out[i] = v[idx]
    }
    return out
}
```

- [ ] **Step 8: Upgrade colLess, colEstBytesPerRow, colIthVal, colTypeTag**

```go
func colLess(c ColumnData, i, j int) bool {
    switch v := c.Data.(type) {
    case []int64:
        return v[i] < v[j]
    case []float64:
        return v[i] < v[j]
    case []string:
        return v[i] < v[j]
    case []bool:
        return !v[i] && v[j]
    case []decimal128.Num:
        return v[i].Less(v[j])
    default:
        return false
    }
}

func colEstBytesPerRow(c ColumnData) uint64 {
    switch v := c.Data.(type) {
    case []int64:
        return 8
    case []float64:
        return 8
    case []bool:
        return 1
    case []string:
        n := len(v)
        if n == 0 {
            return 32
        }
        if n > 100 {
            n = 100
        }
        var sumLen int
        for i := 0; i < n; i++ {
            sumLen += len(v[i])
        }
        return uint64(sumLen / n)
    default:
        return 64
    }
}

// colIthVal returns the i-th value, or nil if the value is null.
func colIthVal(c ColumnData, i int) any {
    if c.Validity != nil && i < len(c.Validity) && !c.Validity[i] {
        return nil
    }
    switch v := c.Data.(type) {
    case []int64:
        if i < len(v) {
            return v[i]
        }
    case []float64:
        if i < len(v) {
            return v[i]
        }
    case []string:
        if i < len(v) {
            return v[i]
        }
    case []bool:
        if i < len(v) {
            return v[i]
        }
    case []decimal128.Num:
        if i < len(v) {
            return v[i]
        }
    }
    return nil
}

func colTypeTag(c ColumnData) string {
    switch c.Data.(type) {
    case []int64:
        return "i64"
    case []float64:
        return "f64"
    case []string:
        return "str"
    case []bool:
        return "bool"
    case []decimal128.Num:
        return "dec"
    default:
        return "unk"
    }
}
```

- [ ] **Step 9: Build to verify dispatch functions compile**

```bash
go build -v -tags=duckdb_arrow ./internal/ingest/
```
Expected: compilation errors in callers (they still pass `any` instead of `ColumnData`). The dispatch functions themselves compile. This is expected — callers fixed in Task 2.

- [ ] **Step 10: Commit**

```bash
git add internal/ingest/arrow_writer.go
git commit -m "feat: add ColumnData type, upgrade dispatch functions to ColumnData-aware signatures

Add ColumnData{Data, Validity} struct. Upgrade 10 dispatch functions:
colLen, colMake, colAppend, colSlice, colSliceFrom, colPermute,
colLess, colEstBytesPerRow, colIthVal, colTypeTag — all take/return
ColumnData. Add mergeValidity, sliceValidity, permuteValidity helpers.

colAppend now atomically merges validity bitmaps.
colIthVal now internally checks validity and returns nil for nulls.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 2: bufferEntry columns merge + update all callers

**Files:**
- Modify: `internal/ingest/arrow_writer.go`
- Modify: `internal/ingest/adaptive_flush.go`

**Goal:** Replace `data` + `validity` fields with `columns map[string]ColumnData`, update every caller.

- [ ] **Step 1: Replace bufferEntry.data + validity with columns**

```go
// Old:
type bufferEntry struct {
    data           map[string]interface{}
    validity       map[string][]bool
    ...
}

// New:
type bufferEntry struct {
    columns       map[string]ColumnData
    ...
}
```

Delete: `GetData()`, `GetValidity()` accessors. Add:
```go
func (e *bufferEntry) GetColumns() map[string]ColumnData { return e.columns }
```

- [ ] **Step 2: Update appendEntryToEntry — 27-line validity logic becomes 0**

```go
func appendEntryToEntry(dst, src *bufferEntry) {
    if dst.columns == nil {
        dst.columns = make(map[string]ColumnData)
    }
    for name, col := range src.columns {
        if existing, ok := dst.columns[name]; ok {
            dst.columns[name] = colAppend(existing, col)
        } else {
            dst.columns[name] = col
        }
    }
    if len(dst.tagColumns) == 0 && len(src.tagColumns) > 0 {
        dst.tagColumns = src.tagColumns
    }
    dst.recordCount += src.recordCount
    dst.estimatedBytes += src.estimatedBytes
}
```

- [ ] **Step 3: Update convertAndAppendToEntry — validity management simplified**

Replace all `entry.data[name] = colAppend(entry.data[name], arr)` with:
```go
entry.columns[name] = colAppend(entry.columns[name], ColumnData{Data: arr, Validity: valid})
```

Remove all manual `entry.validity` initialization, backfill, and padding logic. colAppend handles it.

Replace `entry.data[name] = arr` (first write, zero-copy) with:
```go
entry.columns[name] = ColumnData{Data: arr, Validity: valid}
```

At the bottom of convertAndAppendToEntry, remove the entire `if hasNils && entry.validity != nil { ... backfill ... }` block — colAppend already merged validity per-column.

- [ ] **Step 4: Update sortEntryByKeys — validity automatically handled by colPermute**

```go
func sortEntryByKeys(entry *bufferEntry, sortKeys []string) *bufferEntry {
    sorted, indices, err := sortColumnsByKeysWithPermutation(entry.columns, sortKeys)
    if err != nil {
        return entry
    }
    // nil indices means already sorted
    result := &bufferEntry{
        columns:    sorted,
        tagColumns: entry.tagColumns,
        schema:     entry.schema,
        recordCount: entry.recordCount,
    }
    return result
}
```

**Delete `sortColumnsByKeysWithPermutation`'s validity handling entirely** — it now just passes `map[string]ColumnData` through, and colPermute handles validity.

- [ ] **Step 5: Update sliceEntryByIndices — validity handled by colSlice**

```go
func sliceEntryByIndices(entry *bufferEntry, indices []int) *bufferEntry {
    sliced := make(map[string]ColumnData, len(entry.columns))
    for name, col := range entry.columns {
        sliced[name] = colSlice(col, indices)
    }
    return &bufferEntry{
        columns:    sliced,
        tagColumns: entry.tagColumns,
        schema:     entry.schema,
        recordCount: len(indices),
    }
}
```

**Delete all validity slicing logic** — colSlice handles it.

- [ ] **Step 6: Update SinceRefresh**

```go
func (b *ArrowBuffer) SinceRefresh(bufferKey string) ([]*Entry, error) {
    shard := b.getShard(bufferKey)
    shard.mu.RLock()
    entry, ok := shard.buffers[bufferKey]
    if ok && !entry.isEmpty() && entry.refreshIndex < entry.recordCount {
        subColumns := make(map[string]ColumnData, len(entry.columns))
        for name, col := range entry.columns {
            subColumns[name] = colSliceFrom(col, entry.refreshIndex)
        }
        shard.mu.RUnlock()
        return []*Entry{{
            columns:     subColumns,
            tagColumns:  entry.tagColumns,
            schema:      entry.schema,
            recordCount: entry.recordCount - entry.refreshIndex,
        }}, nil
    }
    shard.mu.RUnlock()
    return nil, nil
}
```

- [ ] **Step 7: Update WriteParquetColumnar**

The function currently takes `*bufferEntry`. Change internal references:
```go
// Old:
col, ok := columns[field.Name]          // columns map[string]interface{}
colValidity = validity[field.Name]      // validity map[string][]bool

// New:
cd, ok := entry.columns[field.Name]     // entry.columns map[string]ColumnData
colValidity = cd.Validity

// Old: switch field.Type.ID() { case arrow.INT64: builder.AppendValues(intCol, colValidity)
// New: same switch, but col := cd.Data
```

- [ ] **Step 8: Update estimateBytesPerRow + estimateBytesFromData**

```go
func estimateBytesPerRow(entry *bufferEntry) uint64 {
    if entry == nil || len(entry.columns) == 0 {
        return 256
    }
    var totalBytes uint64
    for _, col := range entry.columns {
        totalBytes += colEstBytesPerRow(col)
    }
    totalBytes += uint64(len(entry.columns))
    return totalBytes
}

func estimateBytesFromData(columns map[string]ColumnData, numRows int) uint64 {
    if len(columns) == 0 {
        return 0
    }
    var perRow uint64
    for _, col := range columns {
        perRow += colEstBytesPerRow(col)
    }
    perRow += uint64(len(columns))
    return perRow * uint64(numRows)
}
```

- [ ] **Step 9: Update getColumnSignature + inferSchema**

```go
func getColumnSignature(columns map[string]ColumnData) string {
    // ... same structure, iterate columns, call colTypeTag(cd)
}

func (w *ArrowWriter) inferSchema(columns map[string]ColumnData, ...) (*arrow.Schema, error) {
    // ... switch cd.Data.(type) instead of col.(type)
}
```

- [ ] **Step 10: Update prependFlushData + prependFlushDataToEntry**

```go
func (b *ArrowBuffer) prependFlushData(bufferKey string, columns map[string]ColumnData, tagColumns []string, flushedRows int, arrowSchema *arrow.Schema) {
    // ... create entry with columns map, call appendEntryToEntry
}

func prependFlushDataToEntry(entry *bufferEntry, columns map[string]ColumnData, tagColumns []string, flushedRows int) {
    src := &bufferEntry{
        columns:     columns,
        tagColumns:  tagColumns,
        recordCount: flushedRows,
    }
    // Prepend: src first, then entry
    appendEntryToEntry(src, entry)
    entry.columns = src.columns
    entry.tagColumns = src.tagColumns
    entry.recordCount = src.recordCount
    entry.estimatedBytes = src.estimatedBytes
}
```

- [ ] **Step 11: Update flushTask, flushPartitionedData, flushWorker, evictOldestEntries, flushBufferLocked, flushCandidate**

All these functions pass entry data around as `*bufferEntry`. They're already using `entry.data` / `entry.validity` — change to `entry.columns`:

- `flushTask.entry` is already `*bufferEntry` — no change needed
- `flushPartitionedData`: `merged.data["time"]` → `merged.columns["time"].Data`
- All callers that construct `&bufferEntry{data: ..., validity: ...}` → `&bufferEntry{columns: ...}`

- [ ] **Step 12: Update convertAndAppendToEntry — tagColumns propagation entry**

```go
// Old:
if len(entry.tagColumns) == 0 && len(record.TagColumns) > 0 {
    entry.tagColumns = record.TagColumns
}
```
This already exists in writeColumnarInternal, no change needed.

- [ ] **Step 13: Build both packages**

```bash
go build -v -tags=duckdb_arrow ./internal/ingest/ ./internal/database/
```
Fix remaining `.data` / `.validity` references.

- [ ] **Step 14: Commit**

```bash
git add internal/ingest/arrow_writer.go internal/ingest/adaptive_flush.go
git commit -m "refactor: merge data+validity into columns map[string]ColumnData

Replace bufferEntry.data + bufferEntry.validity with single
columns map. Eliminate 27-line validity backfill in appendEntryToEntry.
colAppend/colSlice/colPermute/colSliceFrom now handle validity
atomically. Simplify sortEntryByKeys, sliceEntryByIndices,
SinceRefresh, WriteParquetColumnar, all flush paths.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 3: Update arrow_view.go

**Files:**
- Modify: `internal/database/arrow_view.go`

- [ ] **Step 1: Replace GetData() + GetValidity() with GetColumns()**

```go
// Old:
for name := range entry.GetData() { ... }
for _, col := range entry.GetData() { rowCount = colLen(col) }
values[i] = columnValue(col, validity, row)

// New:
for name := range entry.GetColumns() { ... }
for _, cd := range entry.GetColumns() { rowCount = colLen(cd) }
values[i] = colIthVal(cd, row)  // colIthVal checks cd.Validity internally
```

- [ ] **Step 2: Delete columnValue helper**

No longer needed — `colIthVal` handles null checking.

- [ ] **Step 3: Build both packages**

```bash
go build -v -tags=duckdb_arrow ./internal/ingest/ ./internal/database/
```
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add internal/database/arrow_view.go
git commit -m "refactor: update arrow_view.go for ColumnData merge

Replace GetData()+GetValidity() with GetColumns(). Use colIthVal
for null-aware row value access. Remove columnValue helper.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 4: Update tests

**Files:**
- Modify: `internal/ingest/arrow_writer_test.go`
- Modify: `internal/ingest/arrow_writer_columnar_append_test.go`
- Modify: `internal/ingest/arrow_writer_benchmark_test.go`
- Modify: `internal/database/arrow_view_integration_test.go`

- [ ] **Step 1: Mechanical replacements across all test files**

Search-and-replace patterns:
```
entry.data[name]                    → entry.columns[name].Data
entry.validity[name]                → entry.columns[name].Validity
entry.data                          → entry.columns (when iterating)
make(map[string]interface{})        → make(map[string]ColumnData)
&bufferEntry{data: ..., validity: ...} → &bufferEntry{columns: ...}
convertAndAppendToEntry — now expects entry.columns to be ColumnData map
```

- [ ] **Step 2: Run tests**

```bash
go test -v -tags=duckdb_arrow -race -count=1 ./internal/ingest/ 2>&1 | tail -10
go test -v -tags=duckdb_arrow -race -count=1 ./internal/database/ 2>&1 | tail -5
```
Expected: all PASS.

- [ ] **Step 3: Commit**

```bash
git add internal/ingest/arrow_writer_test.go \
        internal/ingest/arrow_writer_columnar_append_test.go \
        internal/ingest/arrow_writer_benchmark_test.go \
        internal/database/arrow_view_integration_test.go
git commit -m "test: update tests for ColumnData merge

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 5: Verification

- [ ] **Step 1: Run benchmarks**

```bash
go test -v -tags=duckdb_arrow -bench="BenchmarkIngest_WriteBuffer|BenchmarkBufferEntry" -benchmem -benchtime=2s -timeout 300s ./internal/ingest/ 2>&1 | grep -E "^Benchmark|ns/op|allocs/op|rec/s"
```
Expected: WriteBuffer/batch=100 ≤ current 3450 ns/op baseline, allocs may decrease further.

- [ ] **Step 2: Full test suite + race**

```bash
go test -v -tags=duckdb_arrow -race -count=1 ./internal/ingest/ 2>&1 | tail -5
go test -v -tags=duckdb_arrow -race -count=1 ./internal/database/ 2>&1 | tail -5
```
Expected: all PASS.

- [ ] **Step 3: Linter**

```bash
make lint
```
Fix any warnings.

- [ ] **Step 4: Verify no leftover .data or .validity field references on bufferEntry**

```bash
grep -n "\.data\[" internal/ingest/arrow_writer.go | grep -v "ColumnData\|\.Data"
```
Expected: zero results (all should be `.columns[name].Data` now).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "perf: ColumnData merge — atomic data+validity, eliminate backfill

Merge bufferEntry.data + bufferEntry.validity into
columns map[string]ColumnData. 10 dispatch functions upgraded to
ColumnData-aware signatures. colAppend/colSlice/colPermute handle
validity atomically — 27-line backfill logic deleted from
appendEntryToEntry. All sort/slice/flush/VIEW paths simplified.

Co-Authored-By: Claude <noreply@anthropic.com>"
```
