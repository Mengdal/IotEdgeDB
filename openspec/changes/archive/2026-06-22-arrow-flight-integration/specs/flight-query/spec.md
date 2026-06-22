## ADDED Requirements

### Requirement: Start Flight server on independent port
The system SHALL start a gRPC-based Arrow Flight server on a configurable port (default :9090), running in the same OS process as the HTTP server. Flight server startup and shutdown SHALL be managed by the existing shutdown coordinator.

#### Scenario: Flight server starts with valid config
- **WHEN** `iedb.toml` has `[flight] enabled = true` and a valid `addr = ":9090"`
- **THEN** the Flight server starts and listens on :9090
- **AND** logs "Flight server starting" at info level

#### Scenario: Flight server disabled by default
- **WHEN** `iedb.toml` has `[flight] enabled = false` or the section is absent
- **THEN** the Flight server does not start
- **AND** no gRPC port is bound

#### Scenario: Graceful shutdown
- **WHEN** the process receives SIGTERM or SIGINT
- **THEN** the Flight server calls `GracefulStop()` on the gRPC server
- **AND** in-flight RPCs complete within the configured drain timeout

### Requirement: DoGet query with authentication
The system SHALL accept SQL queries via Flight `DoGet`, authenticate the caller using Bearer token from gRPC metadata, authorize via RBAC, execute the query via DuckDB ArrowQueryContext, and stream results as Arrow RecordBatches.

#### Scenario: Successful query
- **WHEN** client calls `DoGet` with a valid Bearer token and a Ticket containing `{"sql": "SELECT * FROM cpu_usage LIMIT 10"}`
- **THEN** the system executes the SQL via `database.Manager.ArrowQueryContext()`
- **AND** returns a stream of Arrow RecordBatches matching the query schema

#### Scenario: Invalid token
- **WHEN** client calls `DoGet` with a missing or invalid Bearer token
- **THEN** the system returns gRPC status `Unauthenticated`

#### Scenario: RBAC denied
- **WHEN** client with limited read permissions queries a restricted measurement
- **THEN** the system returns gRPC status `PermissionDenied`

#### Scenario: Query returns same data as HTTP Arrow IPC
- **WHEN** the same SQL is executed via Flight `DoGet` and via `GET /api/v1/query/arrow`
- **THEN** the returned RecordBatch contents are byte-identical

### Requirement: GetFlightInfo returns query schema
The system SHALL return schema information via `GetFlightInfo` without executing the full query.

#### Scenario: Schema retrieval
- **WHEN** client calls `GetFlightInfo` with a FlightDescriptor containing `{"sql": "SELECT * FROM cpu_usage"}`
- **THEN** the system executes `SELECT * FROM (…) LIMIT 0` to extract schema only
- **AND** returns `FlightInfo` with serialized Arrow schema and a Ticket for subsequent `DoGet`

### Requirement: ListFlights exposes available datasets
The system SHALL list all measurements via `ListFlights`, each as a `FlightInfo` with schema metadata.

#### Scenario: List all measurements
- **WHEN** client calls `ListFlights`
- **THEN** the system returns one `FlightInfo` per measurement
- **AND** each includes the measurement's column schema

### Requirement: Flight SQL metadata
The system SHALL implement the `FlightSqlProducer` interface (`GetCatalogs`, `GetSchemas`, `GetTables`, `GetTableTypes`, `GetSqlInfo`) with schema mapping catalog="iedb" / schema=<database> / table=<measurement>.

#### Scenario: GetCatalogs returns fixed catalog
- **WHEN** client calls `GetCatalogs`
- **THEN** the system returns exactly one catalog: `"iedb"`

#### Scenario: GetSchemas returns database names
- **WHEN** client calls `GetSchemas` with catalog `"iedb"`
- **THEN** the system returns all database names as schema entries

#### Scenario: GetTables returns measurements grouped by database
- **WHEN** client calls `GetTables` with catalog `"iedb"` and a specific schema
- **THEN** the system returns all measurements in that database as table entries
- **AND** each entry includes the measurement's column schema

#### Scenario: BI tool connectivity
- **WHEN** a Flight SQL client (DBeaver, Tableau) connects to the Flight server
- **THEN** the client can browse `iedb` → databases → measurements as a standard SQL catalog hierarchy

### Requirement: Flight client library
The system SHALL provide `internal/flight/client.go` that wraps gRPC connection management and exposes `Query(ctx, sql) → array.RecordReader`.

#### Scenario: Client query transparently calls remote
- **WHEN** `Client.Query(ctx, "SELECT * FROM cpu_usage")` is called
- **THEN** the client internally calls `GetFlightInfo` → `DoGet`
- **AND** returns `array.RecordReader` — the caller does not know whether data came from local or remote

### Requirement: Decimal column normalization
The system SHALL normalize DuckDB `decimal(x,0)` columns to `int64` before streaming Flight responses, reusing the existing `normalizeDecimalSchema` / `castDecimalBatch` logic from `query_arrow.go`.

#### Scenario: Decimal-to-int64 conversion
- **WHEN** a query returns a `DECIMAL(18,0)` column
- **THEN** the Flight DoGet stream emits the column as `int64`

### Requirement: Configuration
The system SHALL support a `[flight]` section in `iedb.toml` with fields: `enabled` (bool), `addr` (string), `tls` (bool), `max_recv_msg_size` (int, bytes), `max_send_msg_size` (int, bytes).

#### Scenario: Custom port configuration
- **WHEN** `iedb.toml` sets `addr = ":9999"`
- **THEN** the Flight server starts on port 9999
