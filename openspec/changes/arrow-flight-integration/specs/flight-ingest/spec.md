## ADDED Requirements

### Requirement: DoPut accepts Arrow RecordBatches
The system SHALL accept Arrow RecordBatch streams via Flight `DoPut`, authenticate the caller, deserialize the FlightDescriptor to determine target database and measurement, and write records into the ingest buffer.

#### Scenario: Successful ingestion
- **WHEN** client calls `DoPut` with a FlightDescriptor containing `{"database": "mydb", "measurement": "cpu"}`, followed by a stream of Arrow RecordBatches
- **THEN** each RecordBatch is written to `ArrowBuffer` via `WriteArrowRecord()`
- **AND** a `PutResult` acknowledgment is sent after each batch

#### Scenario: Invalid token
- **WHEN** client calls `DoPut` with a missing or invalid Bearer token
- **THEN** the system returns gRPC status `Unauthenticated`

#### Scenario: RBAC write denied
- **WHEN** client with read-only permissions calls `DoPut`
- **THEN** the system returns gRPC status `PermissionDenied`

### Requirement: WriteArrowRecord directly appends Arrow columns
The system SHALL implement `ArrowBuffer.WriteArrowRecord(ctx, database, measurement, record)` that appends Arrow RecordBatch columns to `bufferEntry` using typed dispatch, reusing the same lock, flush threshold, WAL, and VIEW refresh logic as the existing MsgPack path.

#### Scenario: First write to empty column
- **WHEN** `WriteArrowRecord` appends an `Int64` column to a `bufferEntry` that has no existing data for that column
- **THEN** the `ColumnData.Data` field directly references `arr.Int64Values()` (zero-copy)
- **AND** the entry's `recordCount` is incremented by `record.NumRows()`

#### Scenario: Append to existing column
- **WHEN** `WriteArrowRecord` appends to a column that already has data
- **THEN** the new values are batch-converted to the target type and appended to the existing slice

#### Scenario: String column copy
- **WHEN** `WriteArrowRecord` appends a `String` column
- **THEN** string values are copied (not referenced) due to variable-length backing

#### Scenario: Flush threshold triggers
- **WHEN** `WriteArrowRecord` causes the buffer entry to exceed the configured flush threshold
- **THEN** the entry is flushed to Parquet via the existing flush path

### Requirement: WAL compatibility
The system SHALL write WAL records for Flight-ingested data when WAL is enabled.

#### Scenario: WAL record created
- **WHEN** WAL is enabled and `WriteArrowRecord` appends data
- **THEN** a WAL record is written containing the ingested batch metadata
- **AND** the data is recoverable after crash via existing WAL replay

### Requirement: DuckDB VIEW refresh
The system SHALL mark the `bufferEntry` for DuckDB VIEW refresh when Flight data is written, so unflushed data is queryable via `_iedb_buffer_view`.

#### Scenario: Flight-written data queryable before flush
- **WHEN** client writes data via `DoPut`
- **AND** then queries via `DoGet` (or HTTP API)
- **THEN** the unflushed data is visible in query results

### Requirement: Data parity with MsgPack path
The system SHALL produce identical query results for data written via Flight `DoPut` and via HTTP MsgPack ingestion, given the same input values.

#### Scenario: Round-trip parity
- **WHEN** the same set of records is written once via Flight `DoPut` and once via HTTP MsgPack
- **AND** both are flushed to Parquet
- **THEN** query results from both paths are identical in values, types, and row count
