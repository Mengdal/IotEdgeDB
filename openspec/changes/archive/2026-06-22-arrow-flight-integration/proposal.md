## Why

IotEdgeDB currently forces all data through JSON serialization on query paths (HTTP API → JSON stream), MsgPack decoding on ingestion, and HTTP+JSON on cluster scatter-gather. Arrow RecordBatches are already in memory (DuckDB output, ingest buffer), but there is no zero-copy transport to get them to clients or between nodes. Apache Arrow Flight provides exactly that — a gRPC-based protocol that transmits Arrow RecordBatches with near-zero serialization cost, plus a standard SQL interface (Flight SQL) for BI tool connectivity.

## What Changes

- **New Flight Server** — Start a gRPC-based Arrow Flight server on an independent port (:9090), running alongside the existing HTTP server. Supports `DoGet` (query), `DoPut` (ingestion), `GetFlightInfo` / `ListFlights` (discovery), and `DoExchange` (cluster-internal query forwarding).
- **Flight SQL support** — Implement the `FlightSqlProducer` interface (`GetTables`, `GetSchemas`, `GetCatalogs`, `GetSqlInfo`) so BI tools (DBeaver, Tableau, Metabase) can connect natively.
- **Zero-copy Arrow ingestion** — `DoPut` accepts Arrow RecordBatches and writes them directly into `ArrowBuffer`, skipping MsgPack decode and ColumnData type conversion.
- **Flight-based cluster scatter-gather** — Replace HTTP-based query forwarding between cluster nodes with parallel Flight `DoGet` calls, enabling zero-copy RecordBatch merging across shards.
- **Flight client library** — New `internal/flight/client.go` wraps gRPC connection management so cluster code queries remote nodes the same way it queries locally: both return `array.RecordReader`.
- **No removals** — HTTP API, Coordinator TCP protocol, and Raft transport are all preserved. Flight is purely additive in v1.

## Capabilities

### New Capabilities

- `flight-query`: Flight DoGet query service with Flight SQL support. Clients query IotEdgeDB via Flight protocol and receive Arrow RecordBatch streams. Includes schema discovery (`ListFlights`, `GetTables`).
- `flight-ingest`: Flight DoPut ingestion. Clients write Arrow RecordBatches directly into the ingest buffer, bypassing MsgPack/LineProtocol parsing.
- `flight-cluster-query`: Flight-based intra-cluster query forwarding. Shard queries use parallel Flight DoGet instead of HTTP+JSON scatter-gather.

### Modified Capabilities

<!-- No existing specs to modify -->

## Impact

- **New dependency**: `github.com/apache/arrow-go/v18/arrow/flight` (already in `go.mod` as an unused transitive dependency; gRPC is already an indirect dependency via ClickHouse client)
- **New package**: `internal/flight/` (~10 files)
- **Modified files**: `cmd/iedb/main.go` (Flight server startup/shutdown), `internal/ingest/arrow_writer.go` (`WriteArrowRecord` method), `internal/cluster/sharding/` (Flight scatter-gather)
- **Configuration**: New `[flight]` section in `iedb.toml`
- **Ports**: New port :9090 for Flight (configurable)
- **No breaking changes**: HTTP API, Coordinator TCP, and Raft are untouched
