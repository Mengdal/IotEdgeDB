## Why

The current Arrow VIEW system pushes buffered data to DuckDB on every write cycle (100ms batched refresh loop), requiring a background goroutine, async notification channel, dirty-set tracking, and an error-prone incremental Appender path with row-by-row type-switch overhead. This complexity exists solely to pre-materialize VIEWs eagerly — but time-series query patterns are read-infrequent relative to writes. Moving VIEW creation to query time eliminates the entire async subsystem while delivering fresher results and simpler code.

## What Changes

- **Remove** the entire `ArrowViewManager` push-based refresh system: `refreshLoop()` goroutine, `notifyCh` channel, `dirty` map, `needsFullScan` flag, `refreshView()`, `createOrReplaceTable()`, `appendToTable()`, `appendEntryToTable()`, `buildCreateTableSQL()`, `duckTypeFor()`, `columnValue()`, and all VIEW state tracking (`views` map, `measurementViews` index, `arrowViewState`)
- **Remove** the `BufferChangeNotifier` interface and `ArrowBuffer.SetNotifier()` injection point
- **Remove** the `OnNewData` and `OnFlushComplete` notification calls from the write/flush paths
- **Add** query-time VIEW construction in the query handler: zero-copy snapshot of buffer entries, vectorized Arrow array building via `AppendValues`, `RegisterView` registration on the query connection, and deferred release
- **Replace** `HasMeasurementData`/`MeasurementViewNames` with a lightweight synchronous buffer query method
- Query rewrite logic (`wrapWithBufferView`) remains structurally similar but calls the new synchronous path instead of checking pre-registered VIEWs
- Net code change: approximately –340 lines, with ~100 new lines in the query path

## Capabilities

### New Capabilities

- `lazy-buffer-view`: Query-time construction of DuckDB Arrow VIEWs from buffered data. On each query that references a measurement with in-memory buffered data, the system snapshots the buffer entries (zero-copy slice header references), builds Arrow record batches via vectorized builders, registers them as temporary DuckDB VIEWs on the query connection, executes the query with UNION ALL rewrites, and releases all Arrow resources when the query completes.

### Modified Capabilities

None — existing buffer query visibility behavior is preserved (query sees the same data), only the timing of VIEW materialization changes.

## Impact

- `internal/database/arrow_view.go` — deleted (~440 lines), replaced by ~100 lines in a new file or integrated into the query handler
- `internal/ingest/arrow_writer.go` — remove `SetNotifier`, `BufferChangeNotifier` interface, notification calls at lines ~2270, ~2389, ~2669, ~3043, ~3094 (~30 lines removed)
- `internal/api/query.go` — `wrapWithBufferView` refactored to call synchronous buffer snapshot + RegisterView; query execution acquires a DuckDB connection for the lifetime of the query (already done for Arrow queries)
- `cmd/iedb/main.go` — remove `SetArrowViewManager` wiring call
- No API contract changes — query results are identical
- No config changes
- No storage format changes
