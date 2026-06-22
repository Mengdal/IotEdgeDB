## ADDED Requirements

### Requirement: Flight-based scatter-gather query
The system SHALL execute shard queries by sending parallel Flight `DoGet` requests to remote shard nodes and merging the resulting RecordBatch streams.

#### Scenario: Single-shard query goes local
- **WHEN** a query's data resides entirely on the local node (no remote shards)
- **THEN** the system executes the query locally via DuckDB
- **AND** no Flight client connection is established

#### Scenario: Multi-shard parallel query
- **WHEN** a query's data spans 3 remote shards
- **THEN** the system sends parallel Flight `DoGet` requests to all 3 shard nodes
- **AND** merges the 3 RecordBatch streams into a single `array.RecordReader`
- **AND** returns the merged stream to the caller

#### Scenario: Shard failure isolation
- **WHEN** one shard node fails during a multi-shard query
- **THEN** the system returns an error to the caller
- **AND** connections to healthy shards are released

#### Scenario: Result parity with HTTP scatter-gather
- **WHEN** the same multi-shard query is executed via Flight `DoGet` and via HTTP scatter-gather
- **THEN** the returned data is identical in values, types, and row count

### Requirement: Flight client connection pool
The system SHALL maintain a connection pool (`ClientPool`) that lazily creates and reuses gRPC connections to peer Flight servers.

#### Scenario: First request creates connection
- **WHEN** `ClientPool.Get("node2:9090")` is called for the first time
- **THEN** a new gRPC connection is established and stored in the pool

#### Scenario: Subsequent request reuses connection
- **WHEN** `ClientPool.Get("node2:9090")` is called again
- **THEN** the existing connection from the pool is returned (no new dial)

#### Scenario: Thread-safe concurrent access
- **WHEN** multiple goroutines concurrently call `ClientPool.Get(addr)` for the same address
- **THEN** only one gRPC connection is created (double-checked locking)
- **AND** all callers receive the same `Client` instance

### Requirement: HTTP fallback preserved
The system SHALL retain HTTP-based query forwarding as a fallback when Flight is unavailable on a peer node.

#### Scenario: Flight peer not available
- **WHEN** a shard node does not have Flight enabled
- **THEN** the scatter-gather system falls back to HTTP query forwarding
- **AND** the query completes successfully via HTTP

### Requirement: RecordReader merge
The system SHALL implement a `MergedReader` that concatenates RecordBatch streams from multiple `array.RecordReader` instances, preserving schema consistency.

#### Scenario: Merge same-schema readers
- **WHEN** two RecordReaders produce batches with identical schemas
- **THEN** `MergedReader` interleaves batches from both readers (one reader fully drained before moving to the next)
- **AND** `MergedReader.Next()` returns true until all readers are exhausted
