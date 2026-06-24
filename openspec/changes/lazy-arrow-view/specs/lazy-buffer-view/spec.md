## ADDED Requirements

### Requirement: Query-time buffer VIEW construction

The system SHALL construct DuckDB Arrow VIEWs from in-memory buffered data synchronously at query time, instead of eagerly refreshing them on a background timer.

#### Scenario: Query includes buffered measurement

- **WHEN** a SQL query references a measurement that has unflushed data in the ingestion buffer
- **THEN** the system SHALL snapshot the buffer entry columns with zero-copy slice header references
- **AND** build Arrow record batches from the snapshot'ed columns using vectorized Arrow Builders
- **AND** register the Arrow record batches as DuckDB VIEWs on the query connection via `arrow_scan`
- **AND** rewrite the query to UNION ALL the registered VIEWs with the Parquet data

#### Scenario: Query on measurement with no buffered data

- **WHEN** a SQL query references a measurement that has no unflushed data in the buffer
- **THEN** the system SHALL execute the query against Parquet data only, without registering any VIEWs

### Requirement: Automatic VIEW cleanup

The system SHALL release all Arrow resources (VIEWs, record batches, arrays) after query execution completes.

#### Scenario: Query completes successfully

- **WHEN** a query that registered buffer VIEWs completes
- **THEN** the system SHALL call the Arrow VIEW release function
- **AND** release the Arrow record reader
- **AND** release the Arrow record batch and its constituent arrays

#### Scenario: Query fails mid-execution

- **WHEN** a query that registered buffer VIEWs fails during execution
- **THEN** the system SHALL still release all Arrow resources via deferred cleanup

### Requirement: Zero-copy buffer snapshot correctness

The system SHALL return query results that include all buffered data present at the moment the query began executing, regardless of concurrent writes to the buffer.

#### Scenario: Concurrent write during query snapshot

- **WHEN** a query snapshots a buffer entry with N rows
- **AND** a concurrent writer appends M new rows to the same entry
- **THEN** the query result SHALL include exactly N rows from the buffered data (not N+M)
- **AND** the writer's append SHALL succeed without blocking on the query

#### Scenario: Buffer flushed during query snapshot

- **WHEN** a query snapshots a buffer entry
- **AND** a concurrent flush moves the entry's data to Parquet while the snapshot is building
- **THEN** the query SHALL still include the snapshot'ed buffered data in its results
- **AND** the system SHALL NOT double-count rows (buffered data must be disjoint from flushed Parquet data for the same time range)

### Requirement: No background VIEW refresh

The system SHALL NOT maintain any background goroutines, timers, or async notification channels for Arrow VIEW refresh.

#### Scenario: Write to buffer

- **WHEN** data is written to the ingestion buffer
- **THEN** the system SHALL NOT trigger any VIEW refresh, notification, or Arrow construction

#### Scenario: Periodic timer

- **WHEN** any amount of time passes (100ms, 1s, 1min)
- **THEN** the system SHALL NOT perform any automatic VIEW refresh operations

### Requirement: DuckDB connection for VIEW lifecycle

The system SHALL register Arrow VIEWs on the same DuckDB connection used to execute the query, and SHALL release all resources when the connection is returned to the pool.

#### Scenario: VIEW registration and query on same connection

- **WHEN** the system registers a buffer VIEW for a query
- **AND** executes the rewritten query
- **THEN** both operations SHALL use the same DuckDB connection
- **AND** the VIEW SHALL be released before the connection is returned to the pool
