## 1. Flight Server Foundation

- [ ] 1.1 Add `[flight]` config section to `internal/config/` with fields: enabled, addr, tls, max_recv_msg_size, max_send_msg_size
- [ ] 1.2 Create `internal/flight/server.go` — Flight server struct, constructor accepting `*database.Manager`, `*auth.Manager`, `*ingest.ArrowBuffer`, `zerolog.Logger`; embed `flight.BaseFlightServer`; gRPC server init with 64MB message limits and keepalive
- [ ] 1.3 Create `internal/flight/server.go` — `Start(addr)` with `net.Listen` + `grpc.Serve`, `Stop()` with `GracefulStop`
- [ ] 1.4 Register Flight server startup/shutdown in `cmd/iedb/main.go` behind `cfg.Flight.Enabled` flag
- [ ] 1.5 Create `internal/flight/ticket.go` — `QueryTicket` and `FlightDescriptor` structs with JSON serialization

## 2. Authentication

- [ ] 2.1 Create `internal/flight/auth.go` — extract Bearer token from gRPC metadata, strip prefix, call `auth.Manager.ValidateToken(ctx, token)`
- [ ] 2.2 Implement RBAC authorization helper calling `auth.Manager.CheckPermission(db, measurement, action)`
- [ ] 2.3 Write unit tests: valid token → pass, missing token → Unauthenticated, insufficient permissions → PermissionDenied

## 3. DoGet Query Service

- [ ] 3.1 Create `internal/flight/do_get.go` — `GetFlightInfo` implementation: parse FlightDescriptor, `LIMIT 0` query for schema, serialize Ticket, return FlightInfo
- [ ] 3.2 Create `internal/flight/do_get.go` — `DoGet` implementation: deserialize Ticket, auth, RBAC, `db.ArrowQueryContext()`, `flight.NewRecordWriter(stream)`, loop over batches
- [ ] 3.3 Integrate `normalizeDecimalSchema` / `castDecimalBatch` from `internal/api/query_arrow.go` (extract to shared location or call directly)
- [ ] 3.4 Create `internal/flight/do_get.go` — `ListFlights` implementation: `db.ListMeasurements()`, build FlightInfo per measurement, stream results
- [ ] 3.5 Write integration test `server_test.go`: DoGet round-trip, output matches HTTP Arrow IPC for same SQL
- [ ] 3.6 Write benchmark `BenchmarkFlightVsHTTP`: compare DoGet vs HTTP Arrow IPC for result sets of 1K / 100K / 1M rows

## 4. Flight SQL

- [ ] 4.1 Create `internal/flight/flight_sql.go` — implement `GetCatalogs` (fixed `["iedb"]`), `GetSchemas` (list databases), `GetTableTypes` (fixed `["TABLE"]`)
- [ ] 4.2 Implement `GetTables` with schema mapping catalog="iedb" / schema=<database> / table=<measurement>, include column schema per table
- [ ] 4.3 Implement `GetSqlInfo` with standard SQL info (identifier quoting, supported functions, string functions, numeric functions)
- [ ] 4.4 Implement Flight SQL `DoGet` handler accepting `StatementQuery` ticket (same execution logic as base DoGet)
- [ ] 4.5 Return `UNIMPLEMENTED` for `CreatePreparedStatement` / `ClosePreparedStatement`
- [ ] 4.6 Verify with Python `pyarrow.flight.connect()`: browse catalogs/schemas/tables, execute SQL, receive results

## 5. Flight Client

- [ ] 5.1 Create `internal/flight/client.go` — `Client` struct with `grpc.ClientConn` and `flight.Client`
- [ ] 5.2 Implement `NewClient(addr, opts)` with `grpc.Dial` (insecure, 64MB recv limit)
- [ ] 5.3 Implement `Query(ctx, sql) → array.RecordReader` wrapping GetFlightInfo → DoGet flow
- [ ] 5.4 Implement `ListMeasurements(ctx) → []MeasurementInfo` via ListFlights
- [ ] 5.5 Implement `Close()` releasing gRPC connection
- [ ] 5.6 Write `client_test.go`: connect to test Flight server, query → verify RecordBatch content

## 6. DoPut Ingestion

- [ ] 6.1 Create `internal/flight/do_put.go` — DoPut implementation: parse FlightDescriptor, auth, RBAC, `flight.NewRecordReader(stream)`, loop writes
- [ ] 6.2 Implement `ArrowBuffer.WriteArrowRecord(ctx, database, measurement, record)` in `internal/ingest/arrow_writer.go`
- [ ] 6.3 Implement `bufferEntry.appendArrowArray(colName, arr)` with typed dispatch: Int64/Float64 direct reference, String copy, Boolean bitmap copy
- [ ] 6.4 Integrate flush threshold check, WAL write, and VIEW refresh (reuse existing logic from MsgPack path)
- [ ] 6.5 Write test: DoPut write → HTTP query → verify data matches input
- [ ] 6.6 Write test: identical data via DoPut vs MsgPack → query results identical
- [ ] 6.7 Write benchmark `BenchmarkPutVsMsgPack`: compare throughput at 100K / 1M / 10M records

## 7. DoExchange — Cluster Scatter-Gather

- [ ] 7.1 Create `internal/flight/client_pool.go` — `ClientPool` with double-checked locking, lazy connection init
- [ ] 7.2 Create `internal/cluster/sharding/scatter_gather_flight.go` — `ExecuteFlight(ctx, ticket) → array.RecordReader`
- [ ] 7.3 Implement parallel shard query: goroutine per shard using `ClientPool.Get(shard.Addr).Query()`, error collection, reader array
- [ ] 7.4 Implement `flight.NewMergedReader(readers)` — concatenate same-schema RecordReaders into single stream
- [ ] 7.5 Modify `internal/flight/do_get.go` to detect sharding and call `ScatterGather.ExecuteFlight()` instead of local query
- [ ] 7.6 Keep HTTP forwarding as fallback: when shard node lacks Flight, fall back to existing HTTP scatter-gather
- [ ] 7.7 Write integration test: 2-node cluster, Flight scatter-gather vs HTTP scatter-gather result parity
- [ ] 7.8 Write benchmark: Flight vs HTTP scatter-gather at 2 / 4 / 8 shards

## 8. Polish & Docs

- [ ] 8.1 Add comprehensive logging (request ID, SQL, duration, row count) to each Flight RPC
- [ ] 8.2 Add Prometheus metrics: flight_requests_total, flight_request_duration_seconds, flight_bytes_sent_total
- [ ] 8.3 Write example Python client script demonstrating connect → query → receive Arrow table
- [ ] 8.4 Update CLAUDE.md with `internal/flight/` package description and Flight server startup flow
