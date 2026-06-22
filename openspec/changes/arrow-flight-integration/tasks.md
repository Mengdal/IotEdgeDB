## 1. Flight Server Foundation

- [x] 1.1 Add `[flight]` config section to `internal/config/` with fields: enabled, addr, tls, max_recv_msg_size, max_send_msg_size
- [x] 1.2 Create `internal/flight/server.go` — Flight server struct, constructor accepting `*database.Manager`, `*auth.Manager`, `*ingest.ArrowBuffer`, `zerolog.Logger`; embed `flight.BaseFlightServer`; gRPC server init with 64MB message limits and keepalive
- [x] 1.3 Create `internal/flight/server.go` — `Start(addr)` with `net.Listen` + `grpc.Serve`, `Stop()` with `GracefulStop`
- [x] 1.4 Register Flight server startup/shutdown in `cmd/iedb/main.go` behind `cfg.Flight.Enabled` flag
- [x] 1.5 Create `internal/flight/ticket.go` — `QueryTicket` and `FlightDescriptor` structs with JSON serialization

## 2. Authentication

- [x] 2.1 Create `internal/flight/auth.go` — extract Bearer token from gRPC metadata, strip prefix, call `auth.Manager.ValidateToken(ctx, token)`
- [x] 2.2 Implement RBAC authorization helper calling `auth.Manager.CheckPermission(db, measurement, action)`
- [x] 2.3 Write unit tests: valid token → pass, missing token → Unauthenticated, insufficient permissions → PermissionDenied

## 3. DoGet Query Service

- [x] 3.1 Create `internal/flight/do_get.go` — `GetFlightInfo` implementation: parse FlightDescriptor, `LIMIT 0` query for schema, serialize Ticket, return FlightInfo
- [x] 3.2 Create `internal/flight/do_get.go` — `DoGet` implementation: deserialize Ticket, auth, RBAC, `db.ArrowQueryContext()`, `flight.NewRecordWriter(stream)`, loop over batches
- [x] 3.3 Integrate Decimal normalization into DoGet and GetFlightInfo (decimal.go)
- [x] 3.4 Create `internal/flight/do_get.go` — `ListFlights` implementation: `db.ListMeasurements()`, build FlightInfo per measurement, stream results
- [x] 3.5 Write integration test `server_test.go`: DoGet round-trip, output matches HTTP Arrow IPC for same SQL
- [x] 3.6 Write benchmarks: DoGet (simple, 1K, 100K), WriteArrowRecord (1K, 10K), MergedReader (4x1K)

## 4. Flight SQL

- [x] 4.1 Create `internal/flight/flight_sql.go` — implement `GetCatalogs` (fixed `["iedb"]`), `GetSchemas` (list databases), `GetTableTypes` (fixed `["TABLE"]`)
- [x] 4.2 Implement `GetTables` with schema mapping catalog="iedb" / schema=<database> / table=<measurement>, include column schema per table
- [x] 4.3 Implement `GetSqlInfo` with standard SQL info (identifier quoting, supported functions, string functions, numeric functions)
- [x] 4.4 Implement Flight SQL `DoGet` handler accepting `StatementQuery` ticket (same execution logic as base DoGet)
- [x] 4.5 Return `UNIMPLEMENTED` for `CreatePreparedStatement` / `ClosePreparedStatement`
- [x] 4.6 Verify Flight SQL external client: GetCatalogs→"iedb", GetTables→1 table, SQL query→val=1

## 5. Flight Client

- [x] 5.1 Create `internal/flight/client.go` — `Client` struct with `grpc.ClientConn` and `flight.Client`
- [x] 5.2 Implement `NewClient(addr, opts)` with `grpc.Dial` (insecure, 64MB recv limit)
- [x] 5.3 Implement `Query(ctx, sql) → array.RecordReader` wrapping GetFlightInfo → DoGet flow
- [x] 5.4 Implement `ListMeasurements(ctx) → []MeasurementInfo` via ListFlights
- [x] 5.5 Implement `Close()` releasing gRPC connection
- [x] 5.6 Write `client_test.go`: connect to test Flight server, query → verify RecordBatch content

## 6. DoPut Ingestion

- [x] 6.1 Create `internal/flight/do_put.go` — DoPut implementation: parse FlightDescriptor, auth, RBAC, `flight.NewRecordReader(stream)`, loop writes
- [x] 6.2 Implement `ArrowBuffer.WriteArrowRecord(ctx, database, measurement, record)` in `internal/ingest/arrow_writer.go`
- [x] 6.3 Implement `arrowArrayToColumnData` / `arrowRecordToEntry` with typed dispatch: Int64/Float64 direct reference, String/Bool/Timestamp copy
- [x] 6.4 Integrate flush threshold check, WAL write, and VIEW refresh (reuse existing writeTypedColumnarInternal machinery)
- [x] 6.5 Write test: WriteArrowRecord → no crash, all types, empty batch, invalid descriptor, missing database/measurement
- [x] 6.6 Write test: Flight + Columnar paths write compatible Parquet (real temp dir, FlushAll, read_parquet query)
- [x] 6.7 Write benchmarks: WriteArrowRecord 1K/10K, MergedReader 4x1K

## 7. DoExchange — Cluster Scatter-Gather

- [x] 7.1 Create `internal/flight/client_pool.go` — `ClientPool` with double-checked locking, lazy connection init
- [x] 7.2 Create `internal/cluster/sharding/scatter_gather_flight.go` — `ExecuteFlight(ctx, ticket) → array.RecordReader`
- [x] 7.3 Implement parallel shard query: goroutine per shard using `ClientPool.Get(shard.Addr).Query()`, error collection, reader array
- [x] 7.4 Implement `flight.NewMergedReader(readers)` — concatenate same-schema RecordReaders into single stream
- [x] 7.5 Add ShardQueryExecutor interface + SetShardExecutor; DoGet delegates to it, falls back to local
- [x] 7.6 HTTP fallback: scatter-gather error logged, DoGet falls through to local DuckDB query
- [x] 7.7 Write integration test: MergedReader concatenation, empty/single-reader cases
- [x] 7.8 Simulate multi-node cluster: BenchmarkScatterGather_2Nodes (3.2ms), _4Nodes (1.4ms)

## 8. Polish & Docs

- [x] 8.1 Add comprehensive logging (SQL, duration, row count) to DoGet and GetFlightInfo RPCs
- [x] 8.2 Add Prometheus metrics: flight_do_get/do_put counters, rows sent, latency
- [x] 8.3 Write example Python client script demonstrating connect → query → receive Arrow table
- [x] 8.4 Update CLAUDE.md with `internal/flight/` package description and Flight server startup flow
