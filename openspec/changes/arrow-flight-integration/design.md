## Context

IotEdgeDB is a Go time-series database (275 source files, ~109K LOC) built on DuckDB + Parquet. It currently exposes three transport layers on separate ports: HTTP API (:8080, Fiber), Coordinator TCP (:9100, custom binary frames), and Raft TCP (:9200, hashicorp/raft). Arrow RecordBatches already flow through the system (DuckDB → Arrow IPC streams for queries, Arrow buffer → Parquet for ingestion), but every client-facing and inter-node path crosses a serialization boundary: JSON encoding for HTTP queries, MsgPack decoding for ingestion, HTTP+JSON for cluster scatter-gather.

`arrow-go` v18.5.2 is already a direct dependency. Its `arrow/flight` package is unused, and `google.golang.org/grpc` v1.79.3 is already an indirect dependency (pulled by ClickHouse client). Adding Flight introduces zero new external dependencies.

## Goals / Non-Goals

**Goals:**
- Add a Flight gRPC server on an independent port, coexisting with the existing HTTP server
- Expose queries via Flight `DoGet` with Flight SQL metadata (`GetTables`, `GetSchemas`, `GetCatalogs`)
- Accept Arrow RecordBatch ingestion via Flight `DoPut`, bypassing MsgPack deserialization
- Replace HTTP-based cluster scatter-gather with parallel Flight `DoGet` calls for zero-copy shard merging
- Reuse existing `database.Manager`, `ingest.ArrowBuffer`, and `auth.Manager` interfaces unchanged
- Keep the HTTP API fully functional throughout — Flight is additive

**Non-Goals:**
- Replacing the Coordinator TCP protocol (heartbeat, WAL replication, file fetch, leader forwarding)
- Replacing the Raft transport
- Removing the HTTP API or MsgPack ingestion path
- Implementing Flight SQL `CreatePreparedStatement` / `ClosePreparedStatement` in v1
- TLS for inter-node Flight connections in v1 (insecure, cluster-internal)

## Decisions

### 1. Same process, independent port (B)

Chose to run the Flight gRPC server in the same OS process as the HTTP server, on a separate port (default :9090).

**Alternatives considered:**
- **Same port via cmux**: Cleaner single-port deployment, but couples HTTP and gRPC lifecycle, complicates debugging and metrics. Rejected because independence is more valuable than port unification.
- **Separate sidecar process**: Strongest isolation but adds operational complexity (two binaries, two process lifecycles, IPC overhead). Rejected as overengineered for v1.

**Rationale:** Separate port gives clean operational isolation (independent start/stop, independent metrics, independent TLS config) while keeping code simple — Flight methods call the same Go interfaces that HTTP handlers call.

### 2. Bearer token passthrough (A)

Chose to extract the `authorization` Bearer token from gRPC metadata and validate via the existing `auth.Manager.ValidateToken()`. Every Flight RPC method authenticates at entry.

**Alternatives considered:**
- **Flight Handshake**: More secure (token not sent per-request) but requires custom auth middleware and session management. Rejected — adds complexity without corresponding benefit for an internal-first protocol.
- **No auth in v1**: Would limit deployment to trusted networks only. Rejected — we want Flight usable externally from day one.

**Rationale:** Bearer passthrough reuses the existing token validation path with zero new code in `auth.Manager`. gRPC metadata is the standard mechanism for per-call credentials.

### 3. Full Flight SQL (C)

Chose to implement the complete `FlightSqlProducer` interface except prepared statements.

**Rationale:** Flight SQL is the gateway for BI tool connectivity. Without it, only custom clients can query — defeating a major motivation for introducing Flight. `GetTables`/`GetSchemas`/`GetCatalogs` are straightforward to implement given the existing `database.Manager.ListMeasurements()` method. Prepared statements are deferred to v2 to limit scope.

### 4. Schema mapping: catalog="iedb" / schema=<database> / table=<measurement> (B)

Chose a 3-level SQL namespace with fixed catalog `"iedb"`.

**Alternatives considered:**
- **Flat mapping (database → schema, measurement → table)**: Simpler but missing the catalog level that BI tools expect for multi-tenant or multi-cluster scenarios.
- **Prefix-based auto-grouping**: Fragile and unpredictable.

**Rationale:** The fixed `iedb` catalog is consistent with PostgreSQL's `db.schema.table` convention, aligns with InfluxDB 3.0's approach, and leaves room for multi-catalog expansion (multi-cluster, multi-tenant).

### 5. DoExchange scope: query forwarding + scatter-gather only (A)

Flight replaces only the HTTP-based query forwarding and scatter-gather paths. Coordinator TCP protocol is excluded.

**Rationale:** Query forwarding currently goes through HTTP → JSON encode/decode. That's pure waste — the data is already in Arrow format. The Coordinator TCP protocol (heartbeat, WAL replication, file fetch) uses a different transport stack and has different requirements (ordering, durability). Mixing concerns would couple Flight's availability to cluster health infrastructure. The cost/benefit of replacing Coordinator TCP with Flight is unclear and can be re-evaluated after DoGet/DoPut prove Flight in production.

### 6. arrow.Record → ArrowBuffer zero-copy path

For `DoPut`, when a ColumnData entry is empty, `appendArrowArray` directly references the `arrow.Array` backing slice (e.g., `arr.Int64Values()`) rather than copying. When ColumnData already has data, a typed batch append is used.

**Rationale:** The RecordBatch lifecycle is scoped to the `DoPut` handler; the buffer flush happens synchronously within the same lock, so the backing array remains valid. For string and boolean types, a copy is required due to variable-length backing and bit-packing respectively. This hybrid approach maximizes throughput for the common numeric-column case while maintaining correctness.

## Risks / Trade-offs

- **gRPC operational complexity** → Mitigation: Start insecure (no TLS), add later. HTTP API runs in parallel as escape hatch.
- **arrow-go Flight package maturity** → Mitigation: Apache official release v18.5.2, Flight protocol stable since 2019. Integration test suite validates correctness.
- **Performance regression vs. HTTP Arrow IPC** → Mitigation: gRPC has HTTP/2 multiplexing advantage; benchmarks before merging each step.
- **Team gRPC unfamiliarity** → Mitigation: 2-week prototype spike before production code. gRPC learning curve is lower than custom TCP protocol design.
- **Tight coupling of Coordinator TCP protocol makes partial replacement risky** → Mitigation: Explicitly exclude Coordinator TCP from scope. DoExchange only touches HTTP query forwarding, which is already a separate code path.
