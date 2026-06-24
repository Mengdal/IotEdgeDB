## 1. Query-time VIEW construction

- [ ] 1.1 Create `buildArrowRecordBatch` function: converts bufferEntry columns (Go typed slices) to an Arrow RecordBatch via vectorized `Builder.AppendValues`, reusing the type-switch logic from `WriteParquetColumnar` but without the Parquet serialization step
- [ ] 1.2 Create `registerBufferView` function: takes an Arrow RecordBatch, wraps it in a `RecordReader`, registers it as a DuckDB VIEW on the query connection via `duckdb.NewArrowFromConn` + `RegisterView`, and returns the release function
- [ ] 1.3 Add `SnapshotEntry` method to `ArrowBuffer`: zero-copy snapshot of a buffer entry (copy slice headers only, not underlying arrays), returning a shallow-copied `*Entry`

## 2. Query path integration

- [ ] 2.1 Refactor `wrapWithBufferView` to use synchronous snapshot + VIEW construction instead of checking `HasMeasurementData`/`MeasurementViewNames`
- [ ] 2.2 Modify query execution to acquire a DuckDB connection before VIEW construction, register VIEWs on that connection, execute the rewritten query, and defer VIEW release
- [ ] 2.3 Ensure the query path works correctly when no buffered data exists (no-op, no VIEW registered)

## 3. Remove push-based VIEW system

- [ ] 3.1 Delete `ArrowViewManager` struct and all methods: `NewArrowViewManager`, `OnNewData`, `OnFlushComplete`, `HasData`, `HasMeasurementData`, `MeasurementViewNames`, `registerInMeasurementIndexLocked`, `removeFromMeasurementIndexLocked`, `refreshLoop`, `refreshView`, `createOrReplaceTable`, `appendToTable`, `appendEntryToTable`, `buildCreateTableSQL`, `Close`
- [ ] 3.2 Delete helper functions: `duckTypeFor`, `columnValue`, `arrowViewState`
- [ ] 3.3 Remove `BufferChangeNotifier` interface from `internal/ingest/arrow_writer.go`
- [ ] 3.4 Remove `SetNotifier` method and `notifier` field from `ArrowBuffer`
- [ ] 3.5 Remove all `b.notifier.OnNewData(...)` calls from write paths (`writeColumnarInternal`, `writeTypedColumnarInternal`)
- [ ] 3.6 Remove all `b.notifier.OnFlushComplete(...)` calls from flush paths (`flushWorker`, `flushBufferLocked`, prepend-flush recovery)

## 4. Wiring cleanup

- [ ] 4.1 Remove `SetArrowViewManager` call from `cmd/iedb/main.go`
- [ ] 4.2 Remove `viewMgr` field from `QueryHandler` if no longer needed for other purposes (replace with direct buffer reference for snapshot access)
- [ ] 4.3 Update `QueryHandler` constructor to accept `*ArrowBuffer` directly instead of `*ArrowViewManager`

## 5. Tests

- [ ] 5.1 Update `arrow_view_integration_test.go` to test the new query-time VIEW construction path (remove dependency on `ArrowViewManager`)
- [ ] 5.2 Add test: query with concurrent writes ensures correct row count (snapshot isolation)
- [ ] 5.3 Add test: VIEW resources are released after query (no leak)
- [ ] 5.4 Add test: query on measurement with no buffered data returns only Parquet results
- [ ] 5.5 Verify existing tests pass: `make test` with `-tags=duckdb_arrow`
