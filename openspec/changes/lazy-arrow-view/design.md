## Context

The current `ArrowViewManager` in `internal/database/arrow_view.go` uses a push model: every buffer write triggers an async notification (`OnNewData`), a background goroutine (`refreshLoop`) batches dirty keys every 100ms, and for each dirty key it either creates or incrementally appends to a DuckDB temporary table via the row-by-row `Appender` API. This system is ~440 lines of code with multiple concurrency concerns.

In time-series workloads, writes vastly outnumber queries. The push model eagerly materializes VIEWs that may never be queried, and its 100ms tick introduces a visibility delay. The query-time pull model eliminates the async subsystem entirely, trading a ~50µs VIEW construction cost at query time for zero background overhead and real-time data visibility.

Relevant code:
- `internal/database/arrow_view.go` — ArrowViewManager, refreshLoop, all VIEW management
- `internal/ingest/arrow_writer.go` — bufferEntry, colSliceFrom, colAppend, BufferChangeNotifier
- `internal/api/query.go` — QueryHandler, wrapWithBufferView, buildReadParquetExprForMeasurement
- `internal/database/duckdb_arrow.go` — ArrowQueryContext, NewArrowFromConn (existing Arrow API usage)

## Goals / Non-Goals

**Goals:**
- Remove the entire push-based Arrow VIEW refresh system (~440 lines)
- Build Arrow VIEWs synchronously at query time on the query connection
- Zero-copy snapshot of buffer entries (slice header references, not deep copy)
- Vectorized Arrow array construction via `Builder.AppendValues` (no row-by-row loops)
- Register VIEWs via DuckDB's `Arrow.RegisterView` (C Arrow Stream interface, 1 CGO call)
- Maintain identical query semantics — queries see the same data they would have under the push model
- Net code reduction of ~340 lines

**Non-Goals:**
- Changing the bufferEntry data format (stays as Go typed slices)
- Changing the flush-to-Parquet path
- Changing the query rewrite logic (UNION ALL structure)
- Adding query result caching — every query builds its VIEW fresh
- Optimizing for high-frequency same-measurement queries (rare in time-series workloads)

## Decisions

### Decision 1: Zero-copy snapshot via Go slice semantics

**Choice**: Snapshot buffer entry columns by copying only the slice headers (`Data: any`, `Validity: []bool`), not the underlying arrays.

**Rationale**: Go's `append` guarantees it never modifies existing elements in the backing array. When `colAppend` extends a slice within capacity, it writes to a new position in the backing array; when it exceeds capacity, it allocates a new array and copies. In both cases, the old slice's [0:len) range remains unchanged. A snapshot taken before an append sees the data as it was at snapshot time.

**Alternatives considered**:
- Deep copy: O(n) extra allocation, doubles memory bandwidth before Arrow Builder copy. Rejected.
- Read-write lock per column: unnecessary complexity. Go's append semantics provide the guarantee.

### Decision 2: Build Arrow arrays inside the read lock

**Choice**: Hold `shard.mu.RLock()` while building Arrow arrays from the snapshot'ed slices.

**Rationale**: The Arrow Builder's `AppendValues` performs a vectorized memory copy (Go slice → Arrow buffer). For typical buffer sizes (1,000–100,000 rows × 5 columns × 8 bytes = 40KB–4MB), this completes in microseconds. Holding the read lock for this duration is acceptable and prevents the buffer from being modified (flushed, schema-changed) mid-build.

**Alternatives considered**:
- Snapshot then release lock then build: adds a second data copy (snapshot deep-copy + Arrow Builder copy). Rejected.
- Copy-on-write buffer: overengineered for this use case.

### Decision 3: Register VIEW on the query connection

**Choice**: Build and register the Arrow VIEW on the same DuckDB connection that executes the query, using `duckdb.NewArrowFromConn` + `RegisterView`, then `defer release()` after query completion.

**Rationale**: DuckDB's `arrow_scan`-registered views may be connection-scoped. Performing registration and query execution on the same connection guarantees visibility and avoids cross-connection lifecycle issues. The query handler already acquires a dedicated connection for Arrow queries (`ArrowQueryContext`).

**Alternatives considered**:
- Register on a global connection: uncertain visibility semantics across DuckDB connections. Risk of VIEW name collisions.
- Keep the Appender-based approach: retains all the complexity we're trying to remove.

### Decision 4: Single RecordReader per measurement, all entries merged

**Choice**: For each measurement with buffered data, merge all schema-variant entries into one Arrow RecordBatch (when schemas are identical across entries for the same key) or create separate RecordBatches (one per variant, matching the existing UNION ALL structure).

**Rationale**: Schema variants already result in separate buffer keys (via `schemaKey()` hashing) and the query already uses UNION ALL across variants. Each variant becomes its own VIEW. Within a variant, multiple entries from `SinceRefresh`-equivalent whole-buffer snapshot are just the full entry (there's only one entry per bufferKey since `appendEntryToEntry` merges them).

### Decision 5: Remove BufferChangeNotifier entirely

**Choice**: Delete the `BufferChangeNotifier` interface from `arrow_writer.go` and all its implementations.

**Rationale**: With no async VIEW refresh, there is no consumer of write/flush notifications. The notifier is dead code. Removal simplifies the write path (no channel push, no overflow handling).

## Risks / Trade-offs

- **[Risk] High-frequency queries on the same measurement**: Each query rebuilds the Arrow VIEW from scratch. With 100,000 buffered rows, Arrow construction is ~200µs — acceptable for typical dashboard refresh rates (5–30s). Not suitable for sub-second polling of the same measurement, but this is not a production pattern for time-series databases.
  → **Mitigation**: Document the trade-off. If profiling shows this as a bottleneck in the future, add a trivial TTL cache (e.g., "reuse VIEW if < 50ms old and buffer hasn't changed").

- **[Risk] Lock contention between writers and query readers**: The query path holds `shard.mu.RLock()` during Arrow construction. For very large buffers (1M+ rows), this could block writers for ~2ms.
  → **Mitigation**: Adaptive flush engine keeps buffer sizes bounded. If this becomes an issue, split the lock: snapshot under RLock, release, then build Arrow (accepting the second data copy).

- **[Risk] RegisterView connection visibility**: DuckDB's `arrow_scan` may have connection-scoped visibility semantics that require the VIEW to be registered on the exact same connection used for the query.
  → **Mitigation**: Already accounted for — we register and query on the same connection.

- **[Trade-off] No incremental VIEW refresh**: With the push model, a VIEW with 1M rows could receive 1,000 new rows and only append those 1,000. With the pull model, we rebuild the full Arrow array from all 1,001,000 rows.
  → **Mitigation**: In practice, buffer sizes are bounded by adaptive flush (typically < 100,000 rows before flush triggers). The vectorized rebuild is fast enough that this is not a regression.

## Migration Plan

1. Add the synchronous VIEW construction function to the query handler (or a new file `arrow_view_sync.go`)
2. Refactor `wrapWithBufferView` to use the new synchronous path
3. Verify with existing integration tests (`arrow_view_integration_test.go`)
4. Remove `ArrowViewManager`, `BufferChangeNotifier`, and all notification calls
5. Remove `SetArrowViewManager` wiring from `main.go`
6. Run full test suite with `-tags=duckdb_arrow`

Rollback: revert commit. No data migration, no config changes, no API changes.

## Open Questions

None — all design decisions are resolved. The implementation is straightforward deletion + replacement.
