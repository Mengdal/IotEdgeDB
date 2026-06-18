# bufferEntry 统一 + 类型派发收拢 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Delete TypedColumnBatch, unify on bufferEntry, consolidate 50+ scattered type switch cases into 8 centralized dispatch functions, and merge convert+append into a single-pass hot path.

**Architecture:** Three-layer refactoring: (1) 8 stateless dispatch functions on `any`-typed columns, (2) TypedColumnBatch → bufferEntry type merge across all consumers, (3) hot-path optimization replacing convertColumnsToTyped + appendTypedBatchToEntry with a single convertAndAppendToEntry call.

**Tech Stack:** Go 1.22+, duckdb_arrow build tag, Apache Arrow v18, CGO

**Spec:** `docs/superpowers/specs/2026-06-18-buffer-entry-unification-design.md`

## Global Constraints

- Build tag `duckdb_arrow` required for all `go test` / `go build` commands
- Race detector enabled: `-race` for all test runs
- Code must match existing patterns in arrow_writer.go (comment density, naming, idiom)
- Benchmark target: WriteBuffer/batch=100 ≤ 3400 ns/op, ≤ 17 allocs/op
- All existing tests must pass with zero behavioral changes

---

### Task 1: Add 8 centralized column dispatch functions

**Files:**
- Modify: `internal/ingest/arrow_writer.go` (insert new block after `appendTypedBatchToEntry`, before `bufferShard`)

**Interfaces:**
- Produces:
  - `colLen(col any) int`
  - `colMake(firstVal any, n int) any`
  - `colAppend(dst, src any) any`
  - `colSlice(col any, indices []int) any`
  - `colPermute(col any, indices []int) any`
  - `colLess(col any, i, j int) bool`
  - `colEstBytesPerRow(col any) uint64`
  - `colIthVal(col any, i int) any`

- [ ] **Step 1: Insert dispatch functions after appendTypedBatchToEntry (after L604)**

```go
// =============================================================================
// Column Dispatch Functions — centralized type switch for all column operations.
// Every function that operates on typed column slices routes through these
// instead of writing inline type switches, so adding a 6th type only requires
// adding one case to each of these functions.
// =============================================================================

// colLen returns the number of rows in a typed column slice.
func colLen(col any) int {
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

// colMake allocates a new typed column slice of length n, matching the type of firstVal.
func colMake(firstVal any, n int) any {
	switch firstVal.(type) {
	case int64:
		return make([]int64, n)
	case float64:
		return make([]float64, n)
	case string:
		return make([]string, n)
	case bool:
		return make([]bool, n)
	case decimal128.Num:
		return make([]decimal128.Num, n)
	default:
		return nil
	}
}

// colAppend concatenates two typed column slices. dst and src must have the same element type.
// Returns a new slice; the original slices are not modified.
func colAppend(dst, src any) any {
	switch v := src.(type) {
	case []int64:
		return append(dst.([]int64), v...)
	case []float64:
		return append(dst.([]float64), v...)
	case []string:
		return append(dst.([]string), v...)
	case []bool:
		return append(dst.([]bool), v...)
	case []decimal128.Num:
		return append(dst.([]decimal128.Num), v...)
	default:
		return dst
	}
}

// colSlice extracts rows by index list. Returns a new slice; source is unmodified.
func colSlice(col any, indices []int) any {
	n := len(indices)
	switch v := col.(type) {
	case []int64:
		out := make([]int64, n)
		vl := len(v)
		for i, idx := range indices {
			if idx < vl {
				out[i] = v[idx]
			}
		}
		return out
	case []float64:
		out := make([]float64, n)
		vl := len(v)
		for i, idx := range indices {
			if idx < vl {
				out[i] = v[idx]
			}
		}
		return out
	case []string:
		out := make([]string, n)
		vl := len(v)
		for i, idx := range indices {
			if idx < vl {
				out[i] = v[idx]
			}
		}
		return out
	case []bool:
		out := make([]bool, n)
		vl := len(v)
		for i, idx := range indices {
			if idx < vl {
				out[i] = v[idx]
			}
		}
		return out
	case []decimal128.Num:
		out := make([]decimal128.Num, n)
		vl := len(v)
		for i, idx := range indices {
			if idx < vl {
				out[i] = v[idx]
			}
		}
		return out
	default:
		return col
	}
}

// colPermute reorders a column according to permutation indices. Allocates a new slice.
func colPermute(col any, indices []int) any {
	n := len(indices)
	switch v := col.(type) {
	case []int64:
		out := make([]int64, n)
		for i, idx := range indices {
			out[i] = v[idx]
		}
		return out
	case []float64:
		out := make([]float64, n)
		for i, idx := range indices {
			out[i] = v[idx]
		}
		return out
	case []string:
		out := make([]string, n)
		for i, idx := range indices {
			out[i] = v[idx]
		}
		return out
	case []bool:
		out := make([]bool, n)
		for i, idx := range indices {
			out[i] = v[idx]
		}
		return out
	case []decimal128.Num:
		out := make([]decimal128.Num, n)
		for i, idx := range indices {
			out[i] = v[idx]
		}
		return out
	default:
		return col
	}
}

// colLess compares two rows within a single column. false < true for bool columns.
func colLess(col any, i, j int) bool {
	switch v := col.(type) {
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

// colEstBytesPerRow estimates bytes per row for a single column.
func colEstBytesPerRow(col any) uint64 {
	switch v := col.(type) {
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

// colIthVal returns the i-th value from a typed column slice as an interface{}.
// Used by WAL conversion to extract individual row values.
func colIthVal(col any, i int) any {
	switch v := col.(type) {
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

// colTypeTag returns a short type tag string for a typed column slice.
// Used by getColumnSignature. Returns "" for unknown types.
func colTypeTag(col any) string {
	switch col.(type) {
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

- [ ] **Step 2: Build to verify compilation**

```bash
go build -v -tags=duckdb_arrow ./internal/ingest/
```
Expected: PASS (no errors, functions defined but not yet called)

- [ ] **Step 3: Run existing tests to verify no regressions**

```bash
go test -v -tags=duckdb_arrow -race -count=1 ./internal/ingest/ 2>&1 | tail -5
```
Expected: `ok  iedb/internal/ingest  ...s`

- [ ] **Step 4: Commit**

```bash
git add internal/ingest/arrow_writer.go
git commit -m "feat: add 8 centralized column dispatch functions

Add colLen, colMake, colAppend, colSlice, colPermute, colLess,
colEstBytesPerRow, colIthVal, and colTypeTag — each containing one
type switch for the 5 supported column types. These replace scattered
inline type switches in append, sort, slice, WAL, and VIEW paths.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 2: Wire dispatch functions into existing callers

**Files:**
- Modify: `internal/ingest/arrow_writer.go`

**Goal:** Replace all inline type switches in existing functions with calls to the centralized dispatch functions. No type changes — TypedColumnBatch and bufferEntry remain as-is. Behavior is byte-for-byte identical.

- [ ] **Step 1: Replace appendTypedBatchToEntry column append logic (L524-L551)**

Current code around L531-L551:
```go
// Old: inline type switch
switch newVals := v.(type) {
case []int64:
    entry.data[name] = append(existing.([]int64), newVals...)
case []float64:
    entry.data[name] = append(existing.([]float64), newVals...)
case []string:
    entry.data[name] = append(existing.([]string), newVals...)
case []bool:
    entry.data[name] = append(existing.([]bool), newVals...)
case []decimal128.Num:
    entry.data[name] = append(existing.([]decimal128.Num), newVals...)
}
```
Replace with:
```go
entry.data[name] = colAppend(existing, v)
```

- [ ] **Step 2: Replace prependFlushDataToEntry column prepend logic (L2759-L2791)**

All 5 inline type switch blocks (one per type: []int64, []float64, []string, []bool, []decimal128.Num), each with the pattern:
```go
// Old:
if existing, ok := entry.data[name]; ok {
    entry.data[name] = append(v, existing.([]int64)...)
} else {
    entry.data[name] = v
}
```
Replace all 5 with:
```go
if existing, ok := entry.data[name]; ok {
    entry.data[name] = colAppend(v, existing)
} else {
    entry.data[name] = v
}
```

- [ ] **Step 3: Replace estimateBytesPerRow (L616-L644)**

Replace the entire inline switch over column types (L617-L642) with a loop over `batch.Data`:
```go
func estimateBytesPerRow(batch *TypedColumnBatch) uint64 {
    if batch == nil || len(batch.Data) == 0 {
        return 256
    }
    var totalBytes uint64
    for _, col := range batch.Data {
        totalBytes += colEstBytesPerRow(col)
    }
    totalBytes += uint64(len(batch.Data))
    return totalBytes
}
```

- [ ] **Step 4: Replace estimateBytesFromData (L650-L683)**

Same pattern — replace inline switch:
```go
func estimateBytesFromData(data map[string]interface{}, numRows int) uint64 {
    if len(data) == 0 {
        return 0
    }
    var perRow uint64
    for _, col := range data {
        perRow += colEstBytesPerRow(col)
    }
    perRow += uint64(len(data))
    return perRow * uint64(numRows)
}
```

- [ ] **Step 5: Replace sliceColumnsByIndices (L3212-L3278)**

Replace all 5 type-specific blocks with a single dispatch:
```go
func sliceColumnsByIndices(columns map[string]interface{}, indices []int) map[string]interface{} {
    result := make(map[string]interface{}, len(columns))
    for colName, colData := range columns {
        result[colName] = colSlice(colData, indices)
    }
    return result
}
```

- [ ] **Step 6: Replace applyPermutation (L3034-L3074)**

Replace all 5 type-specific blocks:
```go
func applyPermutation(colData interface{}, indices []int) interface{} {
    return colPermute(colData, indices)
}
```

- [ ] **Step 7: Replace compareMultiKeyCached (L2983-L3031)**

Replace the 5-case switch with:
```go
func compareMultiKeyCached(cachedCols []interface{}, i, j int) bool {
    for _, col := range cachedCols {
        if colLess(col, i, j) {
            return true
        }
        if colLess(col, j, i) {
            return false
        }
        // Equal, continue to next key
    }
    return false
}
```

- [ ] **Step 8: Replace sortColumnsByKeysWithPermutation row-count detection (L2880-L2897)**

Replace the 5-case switch:
```go
var n int
for _, col := range columns {
    if n = colLen(col); n > 0 {
        break
    }
}
```

- [ ] **Step 9: Replace typedBatchToWALRecords inline switch (L1849-L1879)**

Replace the inner switch over column types:
```go
// Old: for colName, colData := range batch.Data { switch arr := colData.(type) { case []int64: ...
// New:
for colName, colData := range batch.Data {
    row[colName] = colIthVal(colData, i)
}
```

Also replace the decimal128 special-casing (L1867-L1878) by using:
```go
for colName, colData := range batch.Data {
    val := colIthVal(colData, i)
    // WAL stores decimals as float64 (lossy but WAL is recovery-only)
    if dec, ok := val.(decimal128.Num); ok {
        s := int32(0)
        if decimalCols != nil {
            if spec, ok2 := decimalCols[colName]; ok2 {
                s = spec.Scale
            }
        }
        f := dec.ToBigFloat(s)
        val, _ = f.Float64()
    }
    row[colName] = val
}
```

- [ ] **Step 10: Replace SinceRefresh inline switch (L3472-L3483)**

```go
// Old: switch v := col.(type) { case []int64: subData[name] = v[entry.refreshIndex:] ...
// New:
for name, col := range entry.data {
    subData[name] = colSliceFrom(col, entry.refreshIndex)
}
```

Add helper:
```go
// colSliceFrom returns the sub-slice of col from start index to end.
func colSliceFrom(col any, start int) any {
    switch v := col.(type) {
    case []int64:
        return v[start:]
    case []float64:
        return v[start:]
    case []string:
        return v[start:]
    case []bool:
        return v[start:]
    case []decimal128.Num:
        return v[start:]
    default:
        return col
    }
}
```

- [ ] **Step 11: Replace getColumnSignature inline switch (L810-L838)**

```go
func getColumnSignature(columns map[string]interface{}) string {
    type colEntry struct{ name, typ string }
    entries := make([]colEntry, 0, len(columns))
    size := -1
    for name, val := range columns {
        if len(name) == 0 || name[0] == '_' {
            continue
        }
        typ := colTypeTag(val)
        entries = append(entries, colEntry{name, typ})
        size += 1 + len(name) + 1 + len(typ)
    }
    // ... rest unchanged (sort, build string)
}
```

- [ ] **Step 12: Replace inferSchema inline switch (L264-L287)**

```go
// Old: switch arr := col.(type) { case []int64: ...
// New: use colTypeTag or a colKind helper
for name, col := range columns {
    // ... skip logic unchanged
    
    switch col.(type) {
    case []int64:
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
        // decimal handling unchanged
    default:
        return nil, fmt.Errorf("unsupported column type for column %s: %T", name, arr)
    }
}
```
Note: `inferSchema` already has Arrow type mapping that doesn't benefit from the dispatch functions (it maps to `arrow.DataType`, not column operations). Leave as-is except ensure it compiles after TypedColumnBatch removal in Task 3.

- [ ] **Step 13: Run tests to verify behavioral identity**

```bash
go test -v -tags=duckdb_arrow -race -count=1 ./internal/ingest/ 2>&1 | tail -10
```
Expected: `ok  iedb/internal/ingest` — all tests pass

- [ ] **Step 14: Commit**

```bash
git add internal/ingest/arrow_writer.go
git commit -m "refactor: wire centralized dispatch functions into all callers

Replace inline type switches in appendTypedBatchToEntry,
prependFlushDataToEntry, estimateBytesPerRow, estimateBytesFromData,
sliceColumnsByIndices, applyPermutation, compareMultiKeyCached,
sortColumnsByKeysWithPermutation, typedBatchToWALRecords, SinceRefresh,
and getColumnSignature with calls to the centralized col* dispatch
functions. Behavior is byte-for-byte identical.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 3: Merge TypedColumnBatch into bufferEntry

**Files:**
- Modify: `internal/ingest/arrow_writer.go`

**Goal:** Delete the `TypedColumnBatch` type. All former `*TypedColumnBatch` references become `*bufferEntry`. The `flushTask` struct is simplified to carry a single `*bufferEntry` field instead of 5 separate fields.

- [ ] **Step 1: Delete TypedColumnBatch, add Entry export**

Remove (L485-L494):
```go
type TypedColumnBatch struct {
    Data       map[string]interface{}
    Validity   map[string][]bool
    TagColumns []string
    Signature  string
}
```

Add immediately after bufferEntry type (L510):
```go
// Entry is the exported alias for bufferEntry. External consumers
// (database.ArrowViewManager) reference this name; internal code uses
// bufferEntry for consistency.
type Entry = bufferEntry
```

- [ ] **Step 2: Update bufferEntry — rename fields for clarity**

The fields are already named `data`/`validity`/`tagColumns`/`schema` in bufferEntry. No structural change needed — just verify the comment is updated:

```go
// bufferEntry holds all buffered data and metadata for a single measurement key.
// It replaces the former TypedColumnBatch — all operations that previously
// created or consumed TypedColumnBatch now use bufferEntry directly.
// In snapshot scenarios (sort intermediates, VIEW returns), startTime,
// estimatedBytes, and refreshIndex are zero-value dead fields.
type bufferEntry struct {
    data           map[string]interface{} // typed column arrays
    validity       map[string][]bool      // null bitmaps
    tagColumns     []string               // tag column names
    schema         string                 // column signature (=former TypedColumnBatch.Signature)
    startTime      time.Time
    recordCount    int
    estimatedBytes uint64
    arrowSchema    *arrow.Schema
    refreshIndex   int
}
```

- [ ] **Step 3: Simplify flushTask (L685-L698)**

Replace 5 fields with one:
```go
type flushTask struct {
    ctx       context.Context
    cancel    context.CancelFunc
    bufferKey string
    database  string
    measurement string
    entry     *bufferEntry          // replaces data, validity, tagColumns, recordCount, arrowSchema
    trigger   string
}
```

- [ ] **Step 4: Update estimateBytesPerRow signature**

```go
// Old: func estimateBytesPerRow(batch *TypedColumnBatch) uint64
// New:
func estimateBytesPerRow(entry *bufferEntry) uint64 {
    if entry == nil || len(entry.data) == 0 {
        return 256
    }
    var totalBytes uint64
    for _, col := range entry.data {
        totalBytes += colEstBytesPerRow(col)
    }
    totalBytes += uint64(len(entry.data))
    return totalBytes
}
```

- [ ] **Step 5: Update appendTypedBatchToEntry → appendEntryToEntry**

```go
// Old: func appendTypedBatchToEntry(entry *bufferEntry, batch *TypedColumnBatch, batchRows int)
// New:
func appendEntryToEntry(dst, src *bufferEntry) {
    if dst.data == nil {
        dst.data = make(map[string]interface{})
    }

    for name, col := range src.data {
        if existing, ok := dst.data[name]; ok {
            dst.data[name] = colAppend(existing, col)
        } else {
            dst.data[name] = col
        }
    }

    // Validity merge (identical logic to before, just src.validity instead of batch.Validity)
    if src.validity != nil || dst.validity != nil {
        wasNil := dst.validity == nil
        if wasNil {
            dst.validity = make(map[string][]bool)
        }
        if wasNil && dst.recordCount > 0 {
            for name := range dst.data {
                pad := make([]bool, dst.recordCount)
                for i := range pad {
                    pad[i] = true
                }
                dst.validity[name] = pad
            }
        }
        for name, vals := range src.validity {
            dst.validity[name] = append(dst.validity[name], vals...)
        }
        for name := range dst.validity {
            if _, inSrc := src.validity[name]; !inSrc {
                pad := make([]bool, src.recordCount)
                for i := range pad {
                    pad[i] = true
                }
                dst.validity[name] = append(dst.validity[name], pad...)
            }
        }
    }

    if len(dst.tagColumns) == 0 && len(src.tagColumns) > 0 {
        dst.tagColumns = src.tagColumns
    }

    dst.recordCount += src.recordCount
    dst.estimatedBytes += uint64(src.recordCount) * estimateBytesPerRow(src)
}
```

- [ ] **Step 6: Update all callers of appendTypedBatchToEntry**

Search-and-replace `appendTypedBatchToEntry(` → `appendEntryToEntry(` throughout arrow_writer.go. Remove the `batchRows` parameter from each call (it's now read from `src.recordCount`).

Calls to update:
- `writeColumnarInternal` (L1634)
- `writeTypedColumnarInternal` (L1775)
- `prependFlushDataToEntry` (L2757) — the internal logic is replaced by appendEntryToEntry

- [ ] **Step 7: Update prependFlushDataToEntry (L2757-L2839)**

Replace the entire function body — all prepend logic is now handled by `appendEntryToEntry`:
```go
func prependFlushDataToEntry(entry *bufferEntry, data map[string]interface{}, validity map[string][]bool, tagColumns []string, flushedRows int) {
    // Wrap the old flush data as a bufferEntry and prepend it.
    oldEntry := &bufferEntry{
        data:        data,
        validity:    validity,
        tagColumns:  tagColumns,
        recordCount: flushedRows,
    }
    appendEntryToEntry(oldEntry, entry)
    // Copy back to entry
    entry.data = oldEntry.data
    entry.validity = oldEntry.validity
    entry.tagColumns = oldEntry.tagColumns
    entry.recordCount = oldEntry.recordCount
    entry.estimatedBytes = oldEntry.estimatedBytes
}
```

- [ ] **Step 8: Update prependFlushData (L2729-L2753)**

```go
func (b *ArrowBuffer) prependFlushData(bufferKey string, data map[string]interface{}, validity map[string][]bool, tagColumns []string, flushedRows int, arrowSchema *arrow.Schema) {
    shard := b.getShard(bufferKey)
    shard.mu.Lock()
    entry, exists := shard.buffers[bufferKey]
    if !exists {
        schema := getColumnSignature(data)
        entry = &bufferEntry{
            data:           data,
            validity:       validity,
            tagColumns:     tagColumns,
            schema:         schema,
            startTime:      time.Now().UTC(),
            recordCount:    flushedRows,
            estimatedBytes: estimateBytesFromData(data, flushedRows),
            arrowSchema:    arrowSchema,
        }
        shard.buffers[bufferKey] = entry
        shard.mu.Unlock()
        return
    }
    prependFlushDataToEntry(entry, data, validity, tagColumns, flushedRows)
    shard.mu.Unlock()
}
```

- [ ] **Step 9: Update sortTypedColumnBatchByKeys → sortEntryByKeys**

```go
// Old: func sortTypedColumnBatchByKeys(batch *TypedColumnBatch, sortKeys []string) *TypedColumnBatch
// New:
func sortEntryByKeys(entry *bufferEntry, sortKeys []string) *bufferEntry {
    sorted, indices, err := sortColumnsByKeysWithPermutation(entry.data, sortKeys)
    if err != nil {
        return entry
    }

    sortedValidity := entry.validity
    if indices != nil && entry.validity != nil {
        sortedValidity = make(map[string][]bool, len(entry.validity))
        for name, valid := range entry.validity {
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
    }

    return &bufferEntry{
        data:       sorted,
        validity:   sortedValidity,
        tagColumns: entry.tagColumns,
        schema:     entry.schema,
        recordCount: entry.recordCount,
    }
}
```

- [ ] **Step 10: Update sliceTypedColumnBatchByIndices → sliceEntryByIndices**

```go
// Old: func sliceTypedColumnBatchByIndices(batch *TypedColumnBatch, indices []int) *TypedColumnBatch
// New:
func sliceEntryByIndices(entry *bufferEntry, indices []int) *bufferEntry {
    slicedData := sliceColumnsByIndices(entry.data, indices)

    var slicedValidity map[string][]bool
    if entry.validity != nil {
        slicedValidity = make(map[string][]bool, len(entry.validity))
        for name, valid := range entry.validity {
            if valid == nil {
                slicedValidity[name] = nil
                continue
            }
            newValid := make([]bool, len(indices))
            vl := len(valid)
            for i, idx := range indices {
                if idx < vl {
                    newValid[i] = valid[idx]
                }
            }
            slicedValidity[name] = newValid
        }
    }

    return &bufferEntry{
        data:       slicedData,
        validity:   slicedValidity,
        tagColumns: entry.tagColumns,
        schema:     entry.schema,
        recordCount: len(indices),
    }
}
```

- [ ] **Step 11: Update flushPartitionedData signature and body**

```go
// Old: func (b *ArrowBuffer) flushPartitionedData(..., merged *TypedColumnBatch, ...)
// New:
func (b *ArrowBuffer) flushPartitionedData(ctx context.Context, bufferKey, database, measurement string, merged *bufferEntry, flushType string, startTime time.Time) error {
```

Replace all `merged.Data` references with `merged.data`:
- `merged.Data["time"]` → `merged.data["time"]`
- `merged.Validity` → `merged.validity`
- `merged.TagColumns` → `merged.tagColumns`

Replace `sortTypedColumnBatchByKeys(merged, sortKeys)` → `sortEntryByKeys(merged, sortKeys)`
Replace `sliceTypedColumnBatchByIndices(merged, bucket.indices)` → `sliceEntryByIndices(merged, bucket.indices)`
Replace `sortTypedColumnBatchByKeys(hourBatch, sortKeys)` → `sortEntryByKeys(hourBatch, sortKeys)`

- [ ] **Step 12: Update flushBufferLocked (L2657-L2719)**

Remove the inline `&TypedColumnBatch{...}` wrapper:
```go
// Old:
merged := &TypedColumnBatch{
    Data:       data,
    Validity:   validity,
    TagColumns: tagCols,
}

// New:
merged := &bufferEntry{
    data:       data,
    validity:   validity,
    tagColumns: tagCols,
    schema:     entry.schema,
    recordCount: recordCount,
}
```

Update call from `b.flushBufferLockedDataTime(ctx, bufferKey, database, measurement, merged, recordCount, startTime, entry.arrowSchema)` to `b.flushPartitionedData(ctx, bufferKey, database, measurement, merged, flushTypeSync, startTime)`.

- [ ] **Step 13: Delete flushBufferLockedDataTime (L2721-L2724)**

This thin wrapper is no longer needed — `flushBufferLocked` now calls `flushPartitionedData` directly.

- [ ] **Step 14: Update flushWorker (L2230-L2300)**

Remove the `&TypedColumnBatch{...}` wrapper:
```go
// Old:
merged := &TypedColumnBatch{
    Data:       task.data,
    Validity:   task.validity,
    TagColumns: task.tagColumns,
}
if err := b.flushPartitionedData(task.ctx, task.bufferKey, database, measurement, merged, task.recordCount, "async", startTime, task.arrowSchema); err != nil {
    b.prependFlushData(task.bufferKey, task.data, task.validity, task.tagColumns, task.recordCount, task.arrowSchema)
```

```go
// New:
if err := b.flushPartitionedData(task.ctx, task.bufferKey, database, measurement, task.entry, "async", startTime); err != nil {
    b.prependFlushData(task.bufferKey, task.entry.data, task.entry.validity, task.entry.tagColumns, task.entry.recordCount, task.entry.arrowSchema)
```

- [ ] **Step 15: Update evictOldestEntries (L2408-L2422)**

Same removal of `&TypedColumnBatch{...}` wrapper:
```go
// Old:
merged := &TypedColumnBatch{
    Data:       data,
    Validity:   validity,
    TagColumns: tagCols,
}
if err := b.flushPartitionedData(context.Background(), oldestKey, parts[0], parts[1], merged, recordCount, "hard_limit", startTime, arrowSchema); err != nil {

// New:
merged := &bufferEntry{
    data:       data,
    validity:   validity,
    tagColumns: tagCols,
    schema:     entry.schema,
    recordCount: recordCount,
}
if err := b.flushPartitionedData(context.Background(), oldestKey, parts[0], parts[1], merged, "hard_limit", startTime); err != nil {
```

- [ ] **Step 16: Update flushCandidate in adaptive_flush.go (L221-L269)**

```go
// Old:
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

// New:
task := flushTask{
    ctx:         flushCtx,
    cancel:      flushCancel,
    bufferKey:   c.bufferKey,
    database:    database,
    measurement: measurement,
    entry: &bufferEntry{
        data:        data,
        validity:    validity,
        tagColumns:  tagCols,
        recordCount: recordCount,
        arrowSchema: entry.arrowSchema,
    },
    trigger:     trigger,
}
```

Also update the `prependFlushDataToEntry` call:
```go
// Old:
prependFlushDataToEntry(entry, data, validity, tagCols, recordCount)

// New: same — prependFlushDataToEntry already takes raw maps
```

- [ ] **Step 17: Update writeColumnarInternal flush enqueue (L1655-L1667)**

```go
// Old:
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

// New:
task := flushTask{
    ctx:         flushCtx,
    cancel:      flushCancel,
    bufferKey:   bufferKey,
    database:    database,
    measurement: record.Measurement,
    entry: &bufferEntry{
        data:        dataForFlush,
        validity:    validityForFlush,
        tagColumns:  tagColumnsForFlush,
        recordCount: totalBuffered,
        arrowSchema: entry.arrowSchema,
    },
}
```

- [ ] **Step 18: Update writeTypedColumnarInternal flush enqueue (L1790-L1801)**

Same pattern as Step 17.

- [ ] **Step 19: Update WriteTypedColumnarDirect (L1409-L1410)**

```go
// Old: func (b *ArrowBuffer) WriteTypedColumnarDirect(..., batch *TypedColumnBatch, ...)
// New:
func (b *ArrowBuffer) WriteTypedColumnarDirect(ctx context.Context, database, measurement string, entry *bufferEntry) error {
    return b.writeTypedColumnarInternal(ctx, database, measurement, entry, false)
}
```

- [ ] **Step 20: Update writeTypedColumnarInternal (L1712-L1834)**

```go
// Old: func (b *ArrowBuffer) writeTypedColumnarInternal(..., typedColumns *TypedColumnBatch, ...)
// New:
func (b *ArrowBuffer) writeTypedColumnarInternal(ctx context.Context, database, measurement string, src *bufferEntry, skipWAL bool) error {
```

Replace all `typedColumns.Data` → `src.data`, `typedColumns.Validity` → `src.validity`, `typedColumns.TagColumns` → `src.tagColumns`, `typedColumns.Signature` → `src.schema`.

Replace `appendTypedBatchToEntry(entry, typedColumns, numRecords)` → `appendEntryToEntry(entry, src)`.

- [ ] **Step 21: Update typedBatchToWALRecords (L1838-L1885)**

```go
// Old: func typedBatchToWALRecords(..., batch *TypedColumnBatch, numRecords int, ...)
// New:
func entryToWALRecords(database, measurement string, entry *bufferEntry, decimalCols map[string]config.DecimalSpec) []map[string]interface{} {
    if entry.recordCount == 0 {
        return nil
    }
    records := make([]map[string]interface{}, entry.recordCount)
    for i := 0; i < entry.recordCount; i++ {
        row := map[string]interface{}{
            "_database":    database,
            "_measurement": measurement,
        }
        for colName, colData := range entry.data {
            val := colIthVal(colData, i)
            if dec, ok := val.(decimal128.Num); ok {
                s := int32(0)
                if decimalCols != nil {
                    if spec, ok2 := decimalCols[colName]; ok2 {
                        s = spec.Scale
                    }
                }
                f := dec.ToBigFloat(s)
                val, _ = f.Float64()
            }
            row[colName] = val
        }
        records[i] = row
    }
    return records
}
```

Update call in writeTypedColumnarInternal: `typedBatchToWALRecords(...)` → `entryToWALRecords(database, measurement, src, b.getDecimalColumns(measurement))`.

- [ ] **Step 22: Update SinceRefresh (L3465-L3502)**

```go
// Old: func (b *ArrowBuffer) SinceRefresh(bufferKey string) ([]*TypedColumnBatch, error)
// New: (Entry = bufferEntry export added in Step 1)
func (b *ArrowBuffer) SinceRefresh(bufferKey string) ([]*Entry, error) {
    shard := b.getShard(bufferKey)
    shard.mu.RLock()
    entry, ok := shard.buffers[bufferKey]
    if ok && !entry.isEmpty() && entry.refreshIndex < entry.recordCount {
        subData := make(map[string]interface{}, len(entry.data))
        for name, col := range entry.data {
            subData[name] = colSliceFrom(col, entry.refreshIndex)
        }
        var subValidity map[string][]bool
        if entry.validity != nil {
            subValidity = make(map[string][]bool, len(entry.validity))
            for name, v := range entry.validity {
                subValidity[name] = v[entry.refreshIndex:]
            }
        }
        shard.mu.RUnlock()
        return []*Entry{{
            data:       subData,
            validity:   subValidity,
            tagColumns: entry.tagColumns,
            schema:     entry.schema,
            recordCount: entry.recordCount - entry.refreshIndex,
        }}, nil
    }
    shard.mu.RUnlock()
    return nil, nil
}
```

- [ ] **Step 23: Update WriteParquetColumnar (L329-L443)**

```go
// Old: func (w *ArrowWriter) WriteParquetColumnar(ctx, measurement, columns map[string]interface{}, validity map[string][]bool, tagColumns []string, decimalCols map[string]config.DecimalSpec, schema *arrow.Schema)
// New:
func (w *ArrowWriter) WriteParquetColumnar(ctx context.Context, measurement string, entry *bufferEntry, decimalCols map[string]config.DecimalSpec, schema *arrow.Schema) ([]byte, error) {
```

Replace `columns` → `entry.data`, `validity` → `entry.validity`, `tagColumns` → `entry.tagColumns` throughout.

Update call in `flushPartitionedData`: `b.writer.WriteParquetColumnar(ctx, measurement, sorted.Data, sorted.Validity, sorted.TagColumns, decimalCols, arrowSchema)` → `b.writer.WriteParquetColumnar(ctx, measurement, sorted, decimalCols, arrowSchema)`.

- [ ] **Step 24: Build and fix compilation errors**

```bash
go build -v -tags=duckdb_arrow ./internal/ingest/
```
Fix any remaining `TypedColumnBatch` references or type mismatches.

- [ ] **Step 25: Run tests**

```bash
go test -v -tags=duckdb_arrow -race -count=1 ./internal/ingest/ 2>&1 | tail -20
```
Expected: compilation errors or test failures in test files that still reference TypedColumnBatch. This is expected — tests will be fixed in Task 6.

- [ ] **Step 26: Build both packages to verify no cross-package type breaks**

```bash
go build -v -tags=duckdb_arrow ./internal/ingest/ ./internal/database/
```
The database package will fail at this point (arrow_view.go still uses `ingest.TypedColumnBatch`). This is expected — fix in next steps.

- [ ] **Step 27: Update arrow_view.go — type references**

In `internal/database/arrow_view.go`:

Search-and-replace all `[]*ingest.TypedColumnBatch` → `[]*ingest.Entry`.
Replace all `.Data` → `.data`, `.Validity` → `.validity`, `.Signature` → `.schema`, `.TagColumns` → `.tagColumns`.

Update `createOrReplaceTable` (L230):
```go
// Old: func (m *ArrowViewManager) createOrReplaceTable(bufferKey string, batches []*ingest.TypedColumnBatch)
// New:
func (m *ArrowViewManager) createOrReplaceTable(bufferKey string, entries []*ingest.Entry) {
```

Update `appendToTable` (L273):
```go
// Old: func (m *ArrowViewManager) appendToTable(bufferKey string, batches []*ingest.TypedColumnBatch) error
// New:
func (m *ArrowViewManager) appendToTable(bufferKey string, entries []*ingest.Entry) error {
```

Update `appendBatchToTable` → `appendEntryToTable` (L299):
```go
// Old: func (m *ArrowViewManager) appendBatchToTable(conn *sql.Conn, viewName string, batch *ingest.TypedColumnBatch) (int, error)
// New:
func (m *ArrowViewManager) appendEntryToTable(conn *sql.Conn, viewName string, entry *ingest.Entry) (int, error) {
```

```go
// Old: for _, col := range batch.Data { switch v := col.(type) { case []int64: rowCount = len(v) ...
// New:
rowCount := 0
for _, col := range entry.data {
    if rowCount = colLen(col); rowCount > 0 {
        break
    }
}
```

Replace `columnValue(batch.Data[name], batch.Validity[name], row)` with centralized helper:
```go
func columnValue(col any, validity []bool, row int) driver.Value {
    if validity != nil && row < len(validity) && !validity[row] {
        return nil
    }
    return colIthVal(col, row)
}
```

Update `buildCreateTableSQL` (L367):
```go
// Old: func (m *ArrowViewManager) buildCreateTableSQL(viewName string, batch *ingest.TypedColumnBatch) string
// New:
func (m *ArrowViewManager) buildCreateTableSQL(viewName string, entry *ingest.Entry) string {
    colNames := make([]string, 0, len(entry.data))
    for name := range entry.data {
        colNames = append(colNames, name)
    }
    // ... rest unchanged
}
```

Update `refreshView` (L201-L227):
```go
newEntries, err := m.buffer.SinceRefresh(bufferKey)
if err != nil || len(newEntries) == 0 {
    return
}
schemaChanged := false
if exists && len(newEntries) > 0 {
    schemaChanged = (newEntries[0].schema != "" && newEntries[0].schema != state.schema)
}
```

- [ ] **Step 28: Build both packages**

```bash
go build -v -tags=duckdb_arrow ./internal/ingest/ ./internal/database/
```
Expected: PASS (arrow_view.go now uses `ingest.Entry`)

- [ ] **Step 29: Run ingest package tests**

```bash
go test -v -tags=duckdb_arrow -race -count=1 ./internal/ingest/ 2>&1 | tail -20
```
Expected: compilation errors in test files that reference `TypedColumnBatch`. Tests fixed in Task 5.

- [ ] **Step 30: Commit**

```bash
git add internal/ingest/arrow_writer.go internal/ingest/adaptive_flush.go
git commit -m "refactor: merge TypedColumnBatch into bufferEntry

Delete TypedColumnBatch type. All references become *bufferEntry.
flushTask simplified from 5 fields (data, validity, tagColumns,
recordCount, arrowSchema) to 1 (entry *bufferEntry).
Renamed: appendTypedBatchToEntry→appendEntryToEntry,
sortTypedColumnBatchByKeys→sortEntryByKeys,
sliceTypedColumnBatchByIndices→sliceEntryByIndices,
typedBatchToWALRecords→entryToWALRecords.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 4: Hot path optimization — convertAndAppendToEntry

**Files:**
- Modify: `internal/ingest/arrow_writer.go`

**Goal:** Merge `convertColumnsToTyped` + `appendEntryToEntry` into a single-pass `convertAndAppendToEntry`. Add `computeColumnSignature` for lightweight lock-outside schema inference.

- [ ] **Step 1: Add computeColumnSignature (before writeColumnarInternal)**

```go
// computeColumnSignature infers the column signature for a raw map[string][]interface{}
// by checking the Go type of the first non-nil value in each column. This avoids
// allocating typed arrays just to compute the buffer key. The result matches
// getColumnSignature called on already-typed data.
func computeColumnSignature(columns map[string][]interface{}) string {
    type colEntry struct{ name, typ string }
    entries := make([]colEntry, 0, len(columns))
    size := -1
    for name, col := range columns {
        if len(name) == 0 || name[0] == '_' {
            continue
        }
        if len(col) == 0 {
            continue
        }
        firstVal := firstNonNil(col)
        if firstVal == nil {
            continue
        }
        var typ string
        switch firstVal.(type) {
        case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
            typ = "i64"
        case float32, float64:
            typ = "f64"
        case string:
            typ = "str"
        case bool:
            typ = "bool"
        default:
            typ = "unk"
        }
        entries = append(entries, colEntry{name, typ})
        size += 1 + len(name) + 1 + len(typ)
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
```

- [ ] **Step 2: Add convertAndAppendToEntry (after computeColumnSignature)**

```go
// convertAndAppendToEntry converts []interface{} columns to typed slices and
// appends them directly into entry's growing data/validity maps. Single-pass:
// conversion and append happen in one loop, eliminating the intermediate
// TypedColumnBatch allocation that existed in convertColumnsToTyped+appendEntryToEntry.
func (b *ArrowBuffer) convertAndAppendToEntry(entry *bufferEntry, measurement string, columns map[string][]interface{}) (int, error) {
    decimalCols := b.getDecimalColumns(measurement)
    var numRecords int

    hasNils := false
    for name, col := range columns {
        if len(col) == 0 {
            continue
        }
        if numRecords == 0 {
            numRecords = len(col)
        }

        // Decimal column — special path
        if decimalCols != nil {
            if spec, isDecimal := decimalCols[name]; isDecimal {
                arr, valid, err := convertToDecimal128Slice(col, spec.Precision, spec.Scale)
                if err != nil {
                    return 0, fmt.Errorf("decimal conversion error in column '%s': %w", name, err)
                }
                entry.data[name] = colAppend(entry.data[name], arr)
                if valid != nil {
                    hasNils = true
                    entry.validity[name] = append(entry.validity[name], valid...)
                } else if entry.validity != nil {
                    // Pad: no nulls in this column but validity tracking is active
                    pad := make([]bool, numRecords)
                    for i := range pad {
                        pad[i] = true
                    }
                    entry.validity[name] = append(entry.validity[name], pad...)
                }
                continue
            }
        }

        firstVal := firstNonNil(col)
        if firstVal == nil {
            continue
        }

        switch firstVal.(type) {
        case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
            if arr, ok := b.tryInt64ZeroCopy(col); ok {
                entry.data[name] = colAppend(entry.data[name], arr)
                continue
            }
            arr := make([]int64, len(col))
            valid := make([]bool, len(col))
            colHasNils := false
            for i, v := range col {
                if v == nil {
                    colHasNils = true
                    continue
                }
                valid[i] = true
                val, ok := toInt64(v)
                if !ok {
                    return 0, fmt.Errorf("cannot convert %T to int64 in column '%s'", v, name)
                }
                arr[i] = val
            }
            entry.data[name] = colAppend(entry.data[name], arr)
            if colHasNils {
                hasNils = true
                entry.validity[name] = append(entry.validity[name], valid...)
            }

        case float32, float64:
            if arr, ok := b.tryFloat64ZeroCopy(col); ok {
                entry.data[name] = colAppend(entry.data[name], arr)
                continue
            }
            arr := make([]float64, len(col))
            valid := make([]bool, len(col))
            colHasNils := false
            for i, v := range col {
                if v == nil {
                    colHasNils = true
                    continue
                }
                valid[i] = true
                val, ok := toFloat64(v)
                if !ok {
                    return 0, fmt.Errorf("cannot convert %T to float64 in column '%s'", v, name)
                }
                arr[i] = val
            }
            entry.data[name] = colAppend(entry.data[name], arr)
            if colHasNils {
                hasNils = true
                entry.validity[name] = append(entry.validity[name], valid...)
            }

        case string:
            if arr, ok := b.tryStringZeroCopy(col); ok {
                entry.data[name] = colAppend(entry.data[name], arr)
                continue
            }
            arr := make([]string, len(col))
            valid := make([]bool, len(col))
            colHasNils := false
            for i, v := range col {
                if v == nil {
                    colHasNils = true
                    continue
                }
                valid[i] = true
                str, ok := v.(string)
                if !ok {
                    return 0, fmt.Errorf("unexpected type in string column '%s': %T", name, v)
                }
                arr[i] = str
            }
            entry.data[name] = colAppend(entry.data[name], arr)
            if colHasNils {
                hasNils = true
                entry.validity[name] = append(entry.validity[name], valid...)
            }

        case bool:
            if arr, ok := b.tryBoolZeroCopy(col); ok {
                entry.data[name] = colAppend(entry.data[name], arr)
                continue
            }
            arr := make([]bool, len(col))
            valid := make([]bool, len(col))
            colHasNils := false
            for i, v := range col {
                if v == nil {
                    colHasNils = true
                    continue
                }
                valid[i] = true
                bval, ok := v.(bool)
                if !ok {
                    return 0, fmt.Errorf("unexpected type in bool column '%s': %T", name, v)
                }
                arr[i] = bval
            }
            entry.data[name] = colAppend(entry.data[name], arr)
            if colHasNils {
                hasNils = true
                entry.validity[name] = append(entry.validity[name], valid...)
            }

        default:
            return 0, fmt.Errorf("unsupported column type for '%s': %T", name, firstVal)
        }
    }

    // Backfill validity for columns without nulls in this batch
    if hasNils && entry.validity != nil {
        for name := range entry.data {
            if _, has := entry.validity[name]; has {
                continue
            }
            // This column had no nulls in this batch but other columns did
            pad := make([]bool, numRecords)
            for i := range pad {
                pad[i] = true
            }
            entry.validity[name] = append(entry.validity[name], pad...)
        }
    }

    entry.recordCount += numRecords
    entry.estimatedBytes += estimateBytesFromData(entry.data, numRecords)
    return numRecords, nil
}
```

- [ ] **Step 3: Rewrite writeColumnarInternal to use new functions**

Replace lines L1570-L1679 (from `// Convert []interface{} columns` through `shard.mu.Unlock()`):

```go
    // Lock-outside: compute column signature for buffer key
    newSignature := computeColumnSignature(record.Columns)

    // Construct schema-specific buffer key
    bufferKey := schemaKey(database, record.Measurement, newSignature)

    // Flush type conflicts
    baseKey := database + "/" + record.Measurement
    b.flushTypeConflicts(baseKey, newSignature)

    shard := b.getShard(bufferKey)

    var dataForFlush map[string]interface{}
    var validityForFlush map[string][]bool
    var tagColumnsForFlush []string
    var shouldFlush bool

    shard.mu.Lock()

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

    // Infer Arrow schema eagerly on first entry creation
    if entry.arrowSchema == nil && len(record.Columns) > 0 {
        // Need typed data for inferSchema — do a quick first-pass conversion
        // of just the first non-nil values for type inference.
        // We'll infer after the first append since convertAndAppendToEntry
        // produces typed data.
    }

    // Single-pass: convert []interface{} → typed slices + append to entry
    numRecords, err := b.convertAndAppendToEntry(entry, record.Measurement, record.Columns)
    if err != nil {
        shard.mu.Unlock()
        return fmt.Errorf("failed to convert and append columns: %w", err)
    }

    // Infer Arrow schema on first data arrival
    if entry.arrowSchema == nil && len(entry.data) > 0 {
        tagCols := record.TagColumns
        decCols := b.getDecimalColumns(record.Measurement)
        schema, err := b.writer.inferSchema(entry.data, tagCols, decCols)
        if err != nil {
            shard.mu.Unlock()
            return fmt.Errorf("failed to infer Arrow schema: %w", err)
        }
        entry.arrowSchema = schema
    }

    totalBuffered := entry.recordCount

    // Flush gate
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
            measurement: record.Measurement,
            entry: &bufferEntry{
                data:        dataForFlush,
                validity:    validityForFlush,
                tagColumns:  tagColumnsForFlush,
                recordCount: totalBuffered,
                arrowSchema: entry.arrowSchema,
            },
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

- [ ] **Step 4: Remove numRecords from convertAndAppendToEntry return**

The `numRecords` is now embedded in `entry.recordCount`. Update `b.totalRecordsBuffered.Add(int64(numRecords))` to use `entry.recordCount` diff or the returned value.

- [ ] **Step 5: Delete convertColumnsToTyped (L1891-L2041)**

Remove the entire function. Also remove the 4 zero-copy helpers if they're only called from convertColumnsToTyped. Check: `tryInt64ZeroCopy`, `tryFloat64ZeroCopy`, `tryStringZeroCopy`, `tryBoolZeroCopy` — these are now called from `convertAndAppendToEntry`, so keep them.

- [ ] **Step 6: Build**

```bash
go build -v -tags=duckdb_arrow ./internal/ingest/
```
Fix any compilation errors.

- [ ] **Step 7: Commit**

```bash
git add internal/ingest/arrow_writer.go
git commit -m "perf: single-pass convert+append on write hot path

Replace convertColumnsToTyped+appendEntryToEntry with convertAndAppendToEntry:
- lock-outside signature computation (computeColumnSignature, 0 alloc)
- lock-inside single pass: type conversion + colAppend directly to
  entry.data, eliminating intermediate map+struct allocation
- delete convertColumnsToTyped

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 5: Update tests

**Files:**
- Modify: `internal/ingest/arrow_writer_test.go`
- Modify: `internal/ingest/arrow_writer_columnar_append_test.go`
- Modify: `internal/ingest/arrow_writer_benchmark_test.go`
- Modify: `internal/database/arrow_view_integration_test.go`

- [ ] **Step 1: Update arrow_writer_columnar_append_test.go**

Search-and-replace all `&TypedColumnBatch{` → `&bufferEntry{`.
Replace `Data:` → `data:`, `Validity:` → `validity:`, `TagColumns:` → `tagColumns:`.
Replace `appendTypedBatchToEntry(entry, batch, N)` → `appendEntryToEntry(entry, batch)`.

- [ ] **Step 2: Update arrow_writer_test.go**

Replace `convertColumnsToTyped` calls with `computeColumnSignature` + manual validation, or call `convertAndAppendToEntry` on a fresh entry.

For test at L387:
```go
// Old:
batch, numRecords, err := buffer.convertColumnsToTyped("trades", columns)

// New:
entry := &bufferEntry{data: make(map[string]interface{})}
numRecords, err := buffer.convertAndAppendToEntry(entry, "trades", columns)
```

For `TestSortTypedColumnBatchByKeys_NilValidityEntry` → `TestSortEntryByKeys_NilValidityEntry`:
```go
// Old:
batch := &TypedColumnBatch{
    Data: map[string]interface{}{...},
    Validity: map[string][]bool{...},
}

// New:
entry := &bufferEntry{
    data: map[string]interface{}{...},
    validity: map[string][]bool{...},
}
sorted := sortEntryByKeys(entry, []string{"time"})
```

For `TestSliceTypedColumnBatchByIndices_NilValidityEntry` → `TestSliceEntryByIndices_NilValidityEntry`:
```go
// Old:
sliced := sliceTypedColumnBatchByIndices(batch, []int{0, 2})

// New:
sliced := sliceEntryByIndices(entry, []int{0, 2})
```

- [ ] **Step 3: Update arrow_writer_benchmark_test.go**

Replace all `&TypedColumnBatch{` → `&bufferEntry{`:
```go
// Old:
batch := &TypedColumnBatch{
    Data: map[string]interface{}{...},
}

// New:
entry := &bufferEntry{
    data: map[string]interface{}{...},
    recordCount: batchSize,
}
```

Replace `appendTypedBatchToEntry(entry, batch, N)` → `appendEntryToEntry(entry, src)`.

Replace `_ = &TypedColumnBatch{Data: entry.data, ...}` → `_ = &bufferEntry{data: entry.data, ...}`.

Update `SinceRefresh` type assertions: `[]*TypedColumnBatch` → `[]*Entry`.

- [ ] **Step 4: Run all tests**

```bash
go test -v -tags=duckdb_arrow -race -count=1 ./internal/ingest/ 2>&1 | tail -20
go test -v -tags=duckdb_arrow -race -count=1 ./internal/database/ 2>&1 | tail -20
```
Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/ingest/arrow_writer_test.go \
        internal/ingest/arrow_writer_columnar_append_test.go \
        internal/ingest/arrow_writer_benchmark_test.go
git commit -m "test: update tests for bufferEntry unification

Replace all TypedColumnBatch references with bufferEntry in tests.
Update convertColumnsToTyped calls to convertAndAppendToEntry.
Rename sortTypedColumnBatchByKeys→sortEntryByKeys etc.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 6: Verification — benchmarks & final checks

- [ ] **Step 1: Run benchmarks to measure improvement**

```bash
go test -v -tags=duckdb_arrow -bench="BenchmarkIngest_WriteBuffer|BenchmarkBufferEntry|BenchmarkIngest_ConcurrentWrites" -benchmem -benchtime=2s -timeout 300s ./internal/ingest/ 2>&1 | grep -E "^Benchmark|ns/op|allocs/op|rec/s"
```

Expected targets:
- `WriteBuffer/batch=100`: ≤ 3400 ns/op, ≤ 17 allocs/op (was 3935 ns/op, 20 allocs/op)
- `WriteBuffer/batch=10000`: ≤ 180000 ns/op, ≤ 69 allocs/op (was 185620 ns/op, 72 allocs/op)

- [ ] **Step 2: Run full test suite**

```bash
make test
```
Expected: all tests PASS, no race conditions.

- [ ] **Step 3: Run linter**

```bash
make lint
```
Fix any lint warnings introduced.

- [ ] **Step 4: Final review — verify no leftover TypedColumnBatch references**

```bash
grep -rn "TypedColumnBatch" internal/ --include="*.go" | grep -v "_test.go" | grep -v "\.git"
```
Expected: zero results (type fully removed from non-test code).

- [ ] **Step 5: Run impact analysis before final commit**

```bash
# Use GitNexus to verify blast radius
```
Run `detect_changes` to confirm affected symbols match expected scope (bufferEntry, Entry, flushTask, etc.)

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "perf: unified bufferEntry, centralized type dispatch, single-pass hot path

Eliminate TypedColumnBatch type — unified on bufferEntry with exported
Entry alias. Consolidate 50+ scattered type switch cases into 8
centralized dispatch functions (colLen, colAppend, colSlice, etc.).

Hot path: replace convertColumnsToTyped+appendTypedBatchToEntry with
convertAndAppendToEntry — single pass from []interface{} to typed
slices in entry.data. Lock-outside computeColumnSignature for buffer
key (0 alloc).

Benchmark targets: WriteBuffer/batch=100 ≤3400 ns/op, ≤17 allocs/op.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---
