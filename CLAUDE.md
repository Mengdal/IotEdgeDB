# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
make build          # go build -v -tags=duckdb_arrow -o iedb ./cmd/iedb
make run            # go run with duckdb_arrow tag
make test           # all tests with -race and coverage
make bench          # all benchmarks
make fmt            # format all Go code
make lint           # golangci-lint run
make dev            # hot reload via air
make docker-build   # build Docker image
make clean          # remove binary, coverage, data/

# Single test/package
go test -v -tags=duckdb_arrow -run TestName ./internal/package/
```

The `duckdb_arrow` build tag is **required** for all go commands. The binary links against DuckDB's C extension (CGO).

## Architecture

**IotEdgeDB** — a high-performance time-series database on DuckDB + Parquet, written in Go. Single binary (no sidecars). Module path: `iedb`.

### Startup flow (`cmd/iedb/main.go`)

1. Config loaded from TOML (spf13/viper, env var overrides)
2. Logger, metrics, shutdown coordinator initialized
3. DuckDB connection pool created (`internal/database`)
4. Storage backend selected: local / S3 / Azure (`internal/storage`)
5. WAL initialized and recovered if enabled (`internal/wal`)
6. Arrow ingestion buffer created (`internal/ingest`)
7. MQTT subscriber started if configured (`internal/mqtt`)
8. Auth + RBAC initialized (`internal/auth`)
9. Compaction scheduler started — runs in **subprocesses** for memory isolation (`internal/compaction`)
10. Cluster coordinator started if enterprise (`internal/cluster`, Hashicorp Raft)
11. HTTP server started on Fiber (`internal/api`), all routes registered
12. Arrow Flight server started if enabled (`internal/flight`), on configurable port (default :9090)
13. Block until shutdown signal, then graceful drain

### Data path

```
HTTP ingestion (MsgPack/LineProto/TLE/Import)
  → Arrow buffer (in-memory, sharded)
    → WAL (optional durability)
      → periodic flush to Parquet on storage backend
        → compaction (hourly → daily tier, subprocess)
          → tiered storage migration (enterprise: hot→cold)
```

### Key packages

| Package | Role |
|---|---|
| `internal/api` | All HTTP handlers (Fiber), route registration in `server.go` |
| `internal/ingest` | MessagePack/LineProtocol/TLE deserialization and Arrow buffer writes |
| `internal/database` | DuckDB pool, Arrow IPC queries, SQL transform cache |
| `internal/storage` | `Backend` interface — local FS, S3, Azure Blob implementations |
| `internal/compaction` | Tiered Parquet merge in OS subprocesses (`iedb compact --job-stdin`) |
| `internal/wal` | Write-Ahead Log for crash durability and recovery |
| `internal/cluster` | Raft consensus, node roles (writer/reader/compactor), replication |
| `internal/config` | TOML config via viper, `iedb.toml` |
| `internal/auth` | Token auth, RBAC manager |
| `internal/query` | Parallel partition executor for query fan-out |
| `internal/backup` | Backup/restore for data, metadata, config |
| `internal/tiering` | Hot/cold storage lifecycle (enterprise) |
| `internal/scheduler` | Continuous queries and retention policy scheduling |
| `internal/reconciliation` | Manifest vs storage drift detection and repair |
| `internal/flight` | Arrow Flight gRPC server — DoGet (query), DoPut (ingestion), GetFlightInfo, Flight SQL metadata |
| `internal/mqtt` | MQTT topic subscription → measurement ingestion |
| `pkg/models` | Shared types: `Record`, `ColumnarRecord`, `MsgPackPayload` |

### Design decisions

- **Subprocess compaction**: DuckDB holds memory after queries. Compaction runs as `iedb compact --job-stdin` in a separate OS process so memory is fully released on exit.
- **Columnar ingestion**: MessagePack columnar format is the fast path (18.6M rec/s). Avoids row-by-row deserialization overhead.
- **Arrow everywhere**: Ingestion buffer uses Arrow record batches; query results can stream as Arrow IPC for 2x throughput over JSON.
- **License gating**: Enterprise features (clustering, RBAC, audit, tiered storage, governance, reconciliation) check `license.Client` at startup. Feature gates are in each package's init or construction.
- **TOML config**: All config in `iedb.toml` with env var overrides. Sections: server, log, database, storage, ingest, compaction, auth, delete, governance, retention, continuous_query, mqtt, query, telemetry, tiered_storage, audit_log, backup, query_management, wal, license.

### Tests

- Tests use the `duckdb_arrow` build tag (same as production).
- Integration tests may need a running DuckDB and local storage.
- Run with `-race` enabled by default in `make test`.

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **IotEdgeDB** (9305 symbols, 27877 relationships, 300 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> Index stale? Run `node .gitnexus/run.cjs analyze` from the project root — it auto-selects an available runner. No `.gitnexus/run.cjs` yet? `npx gitnexus analyze` (npm 11 crash → `npm i -g gitnexus`; #1939).

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows. For regression review, compare against the default branch: `detect_changes({scope: "compare", base_ref: "main"})`.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `query({query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `context({name: "symbolName"})`.

## Never Do

- NEVER edit a function, class, or method without first running `impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `rename` which understands the call graph.
- NEVER commit changes without running `detect_changes()` to check affected scope.

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/IotEdgeDB/context` | Codebase overview, check index freshness |
| `gitnexus://repo/IotEdgeDB/clusters` | All functional areas |
| `gitnexus://repo/IotEdgeDB/processes` | All execution flows |
| `gitnexus://repo/IotEdgeDB/process/{name}` | Step-by-step execution trace |

## CLI

| Task | Read this skill file |
|------|---------------------|
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->
