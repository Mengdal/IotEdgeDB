# Arrow Flight Integration Design

**Date:** 2026-06-19
**Status:** Draft
**Branch:** (to be created from `main`)

## 1. Motivation

IotEdgeDB currently uses three separate transport layers for data movement:

| Layer | Protocol | Role |
|-------|----------|------|
| Client API (:8080) | HTTP (Fiber) | Query (JSON / Arrow IPC stream), ingestion (MsgPack / LineProtocol) |
| Coordinator (:9100) | Custom TCP frames | Heartbeat, join, WAL replication, file fetch, leader forwarding |
| Raft (:9200) | hashicorp/raft TCP | Consensus log replication |

This design introduces **Apache Arrow Flight** as a fourth, unified transport for the high-throughput data path — query results, ingestion, and cluster-internal query forwarding. Flight transmits Arrow RecordBatches over gRPC with near-zero serialization cost, replacing JSON serialization in the hottest paths.

**Key principle:** Flight runs in parallel with existing transports. Nothing is removed until Flight proves itself in production.

## 2. Architecture

### 2.1 Target transport topology

```
                 ┌──────────────────────────────────────┐
                 │            IotEdgeDB 进程            │
                 │                                      │
Flight Client ──→│ :9090  gRPC Flight Server  ★新增    │
                 │   ├─ DoGet (query)                   │
                 │   ├─ DoPut (ingestion)               │
                 │   ├─ DoExchange (intra-cluster query)│
                 │   ├─ GetFlightInfo / ListFlights     │
                 │   └─ Flight SQL (GetTables, etc.)    │
                 │                                      │
HTTP Client ────→│ :8080  Fiber HTTP Server  (unchanged)│
                 │   ├─ /api/v1/query → JSON            │
                 │   ├─ /api/v1/query/arrow → Arrow IPC │
                 │   ├─ /api/v1/write/msgpack ← MsgPack │
                 │   └─ /write ← LineProtocol           │
                 │                                      │
Peer Node ──────→│ :8080  HTTP query forwarding (kept)  │
Peer Node ──────→│ :9090  Flight DoExchange ★replaces   │
                 │                                      │
Peer Node ──────→│ :9100  Coordinator TCP (unchanged)   │
Peer Node ──────→│ :9200  Raft TCP        (unchanged)   │
                 └──────────────────────────────────────┘
```

### 2.2 Design principles

1. **Parallel coexistence** — Flight and HTTP server run in the same process, independent ports. Flight is an additive path; zero changes to existing code paths.
2. **Reuse core, don't rewrite** — `database.Manager.ArrowQueryContext()`, `ingest.ArrowBuffer`, `auth.Manager` are called by Flight unmodified.
3. **Incremental delivery** — Each step (DoGet → DoPut → DoExchange) ships independently and is valuable on its own.
4. **Unified client** — `internal/flight/client.go` wraps gRPC connection management. Cluster code uses the same Client for both local and remote queries.

## 3. Step 1.1 — Flight DoGet (Query Service)

**Goal:** Stand up a Flight Server exposing `DoGet`, `GetFlightInfo`, `ListFlights`, and Flight SQL. Clients query IotEdgeDB via Flight and receive Arrow RecordBatch streams.

**Timeline:** 4–6 weeks

### 3.1 New files

```
internal/flight/
├── server.go           # Flight Server lifecycle, gRPC initialization
├── do_get.go           # ListFlights / GetFlightInfo / DoGet
├── do_put.go           # Placeholder (Step 1.2)
├── do_exchange.go      # Placeholder (Step 1.3)
├── flight_sql.go       # Flight SQL producer interface
├── auth.go             # Bearer token extraction + validation
├── ticket.go           # QueryTicket, IngestDescriptor custom types
├── client.go           # Flight client wrapper
├── server_test.go      # Integration tests
└── client_test.go      # Client tests
```

### 3.2 Core types (`ticket.go`)

```go
type QueryTicket struct {
    SQL         string `json:"sql"`
    Database    string `json:"database,omitempty"`
    Measurement string `json:"measurement,omitempty"`
}

type FlightDescriptor struct {
    SQL         string `json:"sql"`
    Database    string `json:"database,omitempty"`
    Measurement string `json:"measurement,omitempty"`
}
```

### 3.3 Server (`server.go`)

Embeds `flight.BaseFlightServer`. Constructor accepts `*database.Manager`, `*auth.Manager`, `zerolog.Logger`. Configures gRPC with:
- 64 MB max message size (send and receive)
- Optional TLS (off by default for internal clusters)
- Keepalive enforcement parameters

### 3.4 Auth (`auth.go`)

Extracts `authorization` key from gRPC metadata, strips `Bearer ` prefix, calls `auth.Manager.ValidateToken(ctx, token)`. Returns `codes.Unauthenticated` on failure. Called at the top of every Flight RPC method.

### 3.5 DoGet implementation (`do_get.go`)

| Method | Behavior |
|--------|----------|
| `ListFlights` | Calls `db.ListMeasurements()`, builds `FlightInfo` with schema for each, streams results |
| `GetFlightInfo` | Parses SQL from FlightDescriptor → creates DuckDB Arrow context → `LIMIT 0` query for schema → serializes QueryTicket |
| `DoGet` | Deserializes QueryTicket → authenticates → authorizes (RBAC) → `db.ArrowQueryContext()` → `flight.NewRecordWriter(stream)` → loops over batches, applies `normalizeDecimalSchema` / `castDecimalBatch` (reused from `query_arrow.go`) |

### 3.6 Flight SQL (`flight_sql.go`)

Implements `flight.FlightSqlProducer`. Schema mapping: `catalog="iedb"` (fixed) / `schema=<database>` / `table=<measurement>`.

| Method | Behavior |
|--------|----------|
| `GetCatalogs` | Returns `["iedb"]` only |
| `GetSchemas` | Returns all database names |
| `GetTables` | Returns all measurements grouped by database |
| `GetTableTypes` | Returns `["TABLE"]` |
| `GetSqlInfo` | Standard SQL info (identifier quoting, supported functions) |
| `DoGet` (StatementQuery ticket) | Same logic as base DoGet; ticket type is `StatementQuery` |
| `CreatePreparedStatement` / `ClosePreparedStatement` | Return `UNIMPLEMENTED` in v1 |

### 3.7 Client (`client.go`)

```go
type Client struct { ... }

func NewClient(addr string, opts ...ClientOption) (*Client, error)
func (c *Client) Query(ctx context.Context, sql string) (array.RecordReader, error)
func (c *Client) ListMeasurements(ctx context.Context) ([]MeasurementInfo, error)
func (c *Client) Close() error
```

`Query()` performs the full GetFlightInfo → DoGet flow internally. Callers receive `array.RecordReader` — they do not know or care whether the data came from a local or remote node.

### 3.8 Startup registration (`cmd/iedb/main.go`)

```go
if cfg.Flight.Enabled {
    flightServer := flight.New(dbManager, authManager, ingestBuffer, logger)
    go flightServer.Start(cfg.Flight.Addr)
    shutdownCoordinator.Register("flight", flightServer.Stop)
}
```

### 3.9 Configuration (`iedb.toml`)

```toml
[flight]
enabled = false   # off by default; explicitly enable
addr = ":9090"
tls = false
max_recv_msg_size = 67108864   # 64 MB
max_send_msg_size = 67108864   # 64 MB
```

### 3.10 Acceptance criteria

| Check | Verification |
|-------|-------------|
| Flight query returns same data as HTTP Arrow IPC | Integration test: query via both paths, compare RecordBatch contents byte-for-byte |
| Auth rejects invalid tokens | Test: DoGet with missing / wrong token → `Unauthenticated` |
| RBAC restricts database/measurement access | Test: user with limited permissions → denied |
| Performance ≥ HTTP Arrow IPC | Benchmark: `BenchmarkFlightVsHTTP` across varied result sizes |
| Python client can connect | `pyarrow.flight.connect("localhost:9090").do_get(...)` |

## 4. Step 1.2 — Flight DoPut (Ingestion Service)

**Goal:** Accept Arrow RecordBatches via Flight DoPut and write them directly into ArrowBuffer, skipping MsgPack deserialization and ColumnData type conversion.

**Timeline:** 4–6 weeks

### 4.1 Motivation — eliminated steps

```
MsgPack path (current):
  HTTP body → decompress → MsgPack decode → map[string][]interface{}
    → ColumnData type conversion → Arrow array build → Parquet

Flight path (new):
  gRPC body → RecordBatch → appendArrowArray() → Parquet
       ↑ already in Arrow columnar format; first three steps eliminated
```

Expected throughput improvement: **2–5×** (eliminates MsgPack decoding + ColumnData conversion CPU and allocations).

### 4.2 DoPut implementation (`do_put.go`)

```go
func (s *FlightServer) DoPut(stream flight.FlightService_DoPutServer) error {
    // 1. First message carries FlightDescriptor (database + measurement)
    fd, _ := stream.Recv()
    var desc IngestDescriptor
    json.Unmarshal(fd.FlightDescriptor.Cmd, &desc)

    // 2. Authenticate + authorize
    s.authenticate(stream.Context())
    s.authorize(stream.Context(), desc.Database, desc.Measurement, "write")

    // 3. Create RecordReader from the stream
    reader, _ := flight.NewRecordReader(stream)
    defer reader.Release()

    // 4. Write each RecordBatch directly into ArrowBuffer
    for reader.Next() {
        s.ingest.WriteArrowRecord(ctx, desc.Database, desc.Measurement, reader.Record())
        stream.Send(&flight.PutResult{AppMetadata: ackOK})
    }
    return reader.Err()
}
```

### 4.3 ArrowBuffer changes (`internal/ingest/arrow_writer.go`)

New method on `ArrowBuffer`:

```go
// WriteArrowRecord appends an arrow.Record directly into the buffer.
// Uses the same lock, flush threshold, and WAL logic as the MsgPack path.
func (b *ArrowBuffer) WriteArrowRecord(
    ctx context.Context,
    database, measurement string,
    record arrow.Record,
) error
```

New method on `bufferEntry`:

```go
// appendArrowArray appends an Arrow Array column directly to ColumnData.
// When ColumnData is empty: holds a reference to the Arrow backing slice (zero-copy).
// When ColumnData already has data: batch-converts and appends.
func (e *bufferEntry) appendArrowArray(colName string, arr arrow.Array)
```

Type dispatch: `*array.Int64` → direct `Int64Values()` reference; `*array.Float64` → direct `Float64Values()` reference; `*array.String` → copy required (variable-length backing); `*array.Boolean` → bitmap copy.

### 4.4 DuckDB VIEW integration

Flight `DoPut` writes must mark the buffer entry for VIEW refresh, so unflushed data is immediately queryable through DuckDB's `_iedb_buffer_view`:

```go
entry.refreshIndex++  // reuse existing incremental VIEW refresh mechanism
```

### 4.5 Acceptance criteria

| Check | Verification |
|-------|-------------|
| Flight write → queryable | DoPut → HTTP query returns the written data |
| Data parity with MsgPack path | Write same data via both paths → queries return identical results |
| Throughput benchmark | Bench: Flight DoPut vs HTTP MsgPack at 100K / 1M / 10M records |
| WAL compatibility | Flight writes trigger WAL records when WAL is enabled |
| Compression | gRPC-layer compression (gzip/zstd) compresses comparably to HTTP body compression |

## 5. Step 1.3 — Flight DoExchange (Intra-Cluster Query)

**Goal:** Replace HTTP-based query forwarding and scatter-gather with Flight, allowing zero-copy RecordBatch merging across shards.

**Scope:** Query forwarding + scatter-gather only. Coordinator TCP protocol (heartbeat, WAL replication, file fetch, leader forwarding) is explicitly excluded.

**Timeline:** 4–6 weeks

### 5.1 Current vs. target

```
Current:
  Coordinator receives SQL
    → HTTP POST to each shard :8080/api/v1/query
    → each shard returns JSON stream
    → coordinator merges JSON

Target:
  Coordinator receives Flight DoGet
    → Parallel FlightClient.Query() to each shard :9090
    → each shard returns RecordBatch stream
    → coordinator concatenates RecordReaders (zero-copy merge)
```

### 5.2 New files

```
internal/flight/
├── client_pool.go      # Connection pool for peer Flight clients

internal/cluster/sharding/
├── scatter_gather_flight.go  # Flight-based scatter-gather
```

### 5.3 ScatterGather Flight (`scatter_gather_flight.go`)

```go
func (sg *ScatterGather) ExecuteFlight(
    ctx context.Context,
    ticket flight.QueryTicket,
) (array.RecordReader, error) {
    shards := sg.router.GetShards(ticket.SQL)
    if len(shards) == 0 {
        return sg.localQuery(ctx, ticket)  // no remote shards
    }

    // Parallel Flight DoGet to all shards
    readers := make([]array.RecordReader, len(shards))
    // ... goroutine per shard using ClientPool.Get(shard.Addr).Query(ctx, ticket.SQL) ...

    // Merge: all shards return same schema, concatenate batches
    return flight.NewMergedReader(readers), nil
}
```

### 5.4 Connection pool (`client_pool.go`)

```go
type ClientPool struct {
    mu      sync.RWMutex
    clients map[string]*Client   // addr → Client
    opts    []ClientOption
}

func (p *ClientPool) Get(addr string) (*Client, error)
```

Lazy-initializes gRPC connections to peer nodes. Thread-safe double-checked locking.

### 5.5 Files modified

| File | Change |
|------|--------|
| `internal/flight/do_get.go` | Call `ScatterGather.ExecuteFlight()` instead of direct local query when sharding is active |
| `internal/api/routing.go` | No change — HTTP query forwarding kept as fallback |

### 5.6 Acceptance criteria

| Check | Verification |
|-------|-------------|
| Flight shard query results = HTTP shard query results | 2-node cluster integration test; compare outputs |
| Shard failure recovery | Inject node failure → verify timeout + retry |
| Latency comparison | Benchmark: Flight vs HTTP scatter-gather at 2 / 4 / 8 shards |

## 6. Summary

| Step | Content | Timeline | New files | Modified files |
|------|---------|----------|-----------|----------------|
| 1.1 DoGet | Flight query service + Flight SQL + client | 4–6 weeks | 8 | 1 (`main.go`) |
| 1.2 DoPut | Flight ingestion + zero-copy ArrowBuffer write | 4–6 weeks | 1 | 1 (`arrow_writer.go`) |
| 1.3 DoExchange | Flight shard query + connection pool | 4–6 weeks | 3 | 2 (`do_get.go`, `routing.go`) |

### 6.1 Unchanged

- Coordinator TCP protocol (heartbeat, join, WAL replication, file fetch, leader forwarding)
- Raft TCP transport
- HTTP API — fully retained, runs in parallel
- `database.Manager`, `auth.Manager`, `ingest.ArrowBuffer`, `ArrowViewManager` — interfaces unchanged

### 6.2 Key benefits

- **Zero-copy columnar transport** — query results go DuckDB → gRPC → client without JSON serialization
- **BI tool connectivity** — DBeaver, Tableau, Metabase connect natively via Flight SQL
- **Cluster query acceleration** — RecordBatch merging across shards; eliminates JSON encode/decode bottleneck
- **Incremental safety** — three independent steps, HTTP API retained throughout, each step individually rollbackable

## 7. Risks and Mitigations

| Risk | Probability | Mitigation |
|------|------------|------------|
| gRPC operational complexity (TLS, load balancing) | Medium | Step 1.1 starts insecure; TLS deferred; HTTP API runs in parallel throughout |
| Flight client ecosystem (beyond Python) | Low | Go / C++ / Java / Rust all have Flight clients; gRPC-gateway to REST as last resort |
| arrow-go Flight package stability | Low | v18.5.2 is an official Apache release; Flight protocol has been stable for years |
| Performance regresses vs HTTP Arrow IPC | Low | gRPC has HTTP/2 multiplexing advantage; benchmarks typically show 20–30% improvement |
| Team gRPC inexperience | Medium | gRPC learning curve is lower than custom TCP protocols; 2-week spike + prototype before production code |
