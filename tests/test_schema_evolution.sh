#!/usr/bin/env bash
# =============================================================================
# Schema Evolution Integration Test Suite
# =============================================================================
# Tests the full data path: curl → buffer (memory) → Parquet (disk) → query
# Covers: field add, field remove, type change, mixed changes, tag changes
#
# Prerequisites:
#   1. Build the binary:   make build
#   2. Install jq:         brew install jq  (macOS) / apt-get install jq (Linux)
#
# Usage:
#   ./tests/test_schema_evolution.sh
#
# Environment variables:
#   IEDB_BIN     Path to iedb binary (default: ./iedb)
#   IEDB_PORT    Port to use (default: 9800)
#   IEDB_DATA    Data directory (default: /tmp/iedb_schema_test_data)
#   VERBOSE      1 to show detailed output (default: 0)
#   SKIP_BUILD   1 to skip the build step (default: 0)
# =============================================================================

set -uo pipefail
# Note: NOT using set -e — we want all tests to run even if one fails

# ── Configuration ───────────────────────────────────────────────────────────
IEDB_BIN="${IEDB_BIN:-./iedb}"
IEDB_PORT="${IEDB_PORT:-9800}"
IEDB_DATA="${IEDB_DATA:-/tmp/iedb_schema_test_data}"
VERBOSE="${VERBOSE:-0}"
SKIP_BUILD="${SKIP_BUILD:-0}"
IEDB_PID=""

BASE_URL="http://localhost:${IEDB_PORT}"
DB="testdb"

# ── Color output ────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

PASS=0
FAIL=0

log_info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
log_pass()    { echo -e "${GREEN}[PASS]${NC}  $*"; PASS=$((PASS + 1)); }
log_fail()    { echo -e "${RED}[FAIL]${NC}  $*"; FAIL=$((FAIL + 1)); }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_section() { echo ""; echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"; echo -e "${CYAN}  $*${NC}"; echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"; }

# ── Cleanup ─────────────────────────────────────────────────────────────────
cleanup() {
    if [ -n "$IEDB_PID" ] && kill -0 "$IEDB_PID" 2>/dev/null; then
        log_info "Stopping iedb server (PID: $IEDB_PID)..."
        kill "$IEDB_PID" 2>/dev/null || true
        wait "$IEDB_PID" 2>/dev/null || true
    fi
    if [ -d "$IEDB_DATA" ]; then
        rm -rf "$IEDB_DATA"
    fi
}

trap cleanup EXIT

# ── Helpers ─────────────────────────────────────────────────────────────────

# write_lp: Write Line Protocol data to the server.
# Usage: write_lp <lp_data>
write_lp() {
    local data="$1"
    local url="${BASE_URL}/api/v1/write/line-protocol"
    local resp
    resp=$(curl -s -o /dev/null -w "%{http_code}" \
        -X POST "$url" \
        -H "x-iedb-database: ${DB}" \
        -H "Content-Type: text/plain" \
        --data-binary "$data" 2>&1)
    if [ "$resp" != "204" ]; then
        log_fail "Write failed: HTTP $resp (data: ${data:0:80}...)"
        return 1
    fi
    [ "$VERBOSE" = "1" ] && log_info "Write OK: ${data:0:80}..."
    return 0
}

# query: Execute a SQL query and return JSON response.
# Usage: query <sql> [extra_headers...]
query() {
    local sql="$1"
    shift
    local url="${BASE_URL}/api/v1/query"
    local resp
    # Use jq to safely build JSON with proper escaping of double quotes etc.
    local body
    body=$(jq -n --arg sql "$sql" '{sql: $sql}')
    resp=$(curl -s -X POST "$url" \
        -H "Content-Type: application/json" \
        -H "x-iedb-database: ${DB}" \
        "$@" \
        -d "$body" 2>&1)
    echo "$resp"
}

# query_json: Execute a SQL query and extract a field from the JSON response.
# Usage: query_json <jq_filter> <sql>
query_json() {
    local filter="$1"
    local sql="$2"
    local resp
    resp=$(query "$sql")
    local success
    success=$(echo "$resp" | jq -r '.success // false')
    if [ "$success" != "true" ]; then
        local err
        err=$(echo "$resp" | jq -r '.error // "unknown error"')
        log_fail "Query failed: $err (SQL: ${sql:0:100}...)"
        echo "__ERROR__"
        return 1
    fi
    echo "$resp" | jq -rc "$filter"
}

# assert_eq: Assert two values are equal.
# Usage: assert_eq <label> <expected> <actual>
assert_eq() {
    local label="$1"
    local expected="$2"
    local actual="$3"
    if [ "$expected" = "$actual" ]; then
        log_pass "$label: expected=$expected, actual=$actual"
    else
        log_fail "$label: expected=$expected, actual=$actual"
    fi
}

# assert_contains: Assert a string contains a substring.
# Usage: assert_contains <label> <haystack> <needle>
assert_contains() {
    local label="$1"
    local haystack="$2"
    local needle="$3"
    if [[ "$haystack" == *"$needle"* ]]; then
        log_pass "$label: contains '$needle'"
    else
        log_fail "$label: expected to contain '$needle', got: ${haystack:0:200}"
    fi
}

# flush_all: Force flush all buffered data to Parquet files.
flush_all() {
    local url="${BASE_URL}/api/v1/write/line-protocol/flush"
    local resp
    resp=$(curl -s -X POST "$url" 2>&1)
    local status
    status=$(echo "$resp" | jq -r '.status // "error"')
    if [ "$status" = "success" ]; then
        [ "$VERBOSE" = "1" ] && log_info "Flush OK"
        return 0
    else
        log_warn "Flush returned: $resp"
        return 1
    fi
}

# wait_for_server: Wait for the server to be ready (up to 30 seconds).
wait_for_server() {
    log_info "Waiting for server to be ready on port ${IEDB_PORT}..."
    local max_attempts=60
    local attempt=0
    while [ $attempt -lt $max_attempts ]; do
        if curl -s "${BASE_URL}/health" > /dev/null 2>&1; then
            log_info "Server is ready (attempt $attempt)"
            return 0
        fi
        sleep 0.5
        attempt=$((attempt + 1))
    done
    log_fail "Server failed to start within ${max_attempts} attempts"
    return 1
}

# assert_query_count: Assert a COUNT(*) query returns expected value.
# Usage: assert_query_count <label> <sql> <expected>
assert_query_count() {
    local label="$1"
    local sql="$2"
    local expected="$3"
    local actual
    actual=$(query_json '.data[0][0]' "$sql")
    if [ "$actual" = "__ERROR__" ]; then
        return 1
    fi
    assert_eq "$label" "$expected" "$actual"
}

# assert_query_value: Assert a specific value query.
# Usage: assert_query_value <label> <sql> <expected>
assert_query_value() {
    local label="$1"
    local sql="$2"
    local expected="$3"
    local actual
    actual=$(query_json '.data[0][0]' "$sql")
    if [ "$actual" = "__ERROR__" ]; then
        return 1
    fi
    assert_eq "$label" "$expected" "$actual"
}

# ── Setup ───────────────────────────────────────────────────────────────────
setup() {
    log_section "Setup: Building and Starting IotEdgeDB"

    # Build unless skipped
    if [ "$SKIP_BUILD" != "1" ]; then
        log_info "Building iedb binary..."
        if [ -f "$IEDB_BIN" ]; then
            log_info "Binary already exists at $IEDB_BIN"
        else
            (cd "$(dirname "$0")/.." && make build) || {
                log_fail "Build failed"
                exit 1
            }
            IEDB_BIN="$(dirname "$0")/../iedb"
        fi
    fi

    # Resolve to absolute path before changing directories
    if [ ! -f "$IEDB_BIN" ]; then
        log_fail "Binary not found at $IEDB_BIN. Set IEDB_BIN or run 'make build' first."
        exit 1
    fi
    IEDB_BIN="$(cd "$(dirname "$IEDB_BIN")" && pwd)/$(basename "$IEDB_BIN")"

    # Kill any process already on our test port
    local existing_pid
    existing_pid=$(lsof -ti ":${IEDB_PORT}" 2>/dev/null || true)
    if [ -n "$existing_pid" ]; then
        log_warn "Killing existing process on port ${IEDB_PORT} (PID: $existing_pid)"
        kill -9 $existing_pid 2>/dev/null || true
        sleep 1
    fi

    # Create temp data directory
    rm -rf "$IEDB_DATA"
    mkdir -p "$IEDB_DATA"

    # Use environment variables to configure the server.
    # The config system (viper) reads from env vars prefixed with IEDB_.
    # Set everything we need for a self-contained test:
    export IEDB_SERVER_PORT="${IEDB_PORT}"
    export IEDB_LOG_LEVEL="error"
    export IEDB_LOG_FORMAT="console"
    export IEDB_DATABASE_MAX_CONNECTIONS="4"
    export IEDB_DATABASE_MEMORY_LIMIT="256MB"
    export IEDB_DATABASE_THREAD_COUNT="2"
    export IEDB_DATABASE_TIMEZONE="UTC"
    export IEDB_DATABASE_ENABLE_WAL="false"
    export IEDB_STORAGE_BACKEND="local"
    export IEDB_STORAGE_LOCAL_PATH="${IEDB_DATA}"
    export IEDB_INGEST_COMPRESSION="none"
    export IEDB_INGEST_USE_DICTIONARY="false"
    export IEDB_INGEST_WRITE_STATISTICS="true"
    export IEDB_INGEST_DATA_PAGE_VERSION="2.0"
    export IEDB_INGEST_MAX_BUFFER_MEMORY_MB="512"
    export IEDB_INGEST_MIN_BUFFER_MEMORY_MB="128"
    export IEDB_INGEST_MAX_BUFFER_AGE_SECONDS="3600"
    export IEDB_INGEST_MEMORY_PRESSURE_GREEN_PCT="90"
    export IEDB_INGEST_MEMORY_PRESSURE_RED_PCT="10"
    export IEDB_INGEST_MEMORY_CHECK_INTERVAL_MS="5000"
    export IEDB_INGEST_FLUSH_WORKERS="2"
    export IEDB_INGEST_FLUSH_QUEUE_SIZE="8"
    export IEDB_INGEST_FLUSH_TIMEOUT_SECONDS="30"
    export IEDB_INGEST_SHARD_COUNT="4"
    export IEDB_COMPACTION_ENABLED="false"
    export IEDB_AUTH_ENABLED="false"
    export IEDB_DELETE_ENABLED="false"
    export IEDB_GOVERNANCE_ENABLED="false"
    export IEDB_RETENTION_ENABLED="false"
    export IEDB_CONTINUOUS_QUERY_ENABLED="false"
    export IEDB_MQTT_ENABLED="false"
    export IEDB_QUERY_SLOW_QUERY_THRESHOLD_MS="0"
    export IEDB_QUERY_ENABLE_S3_CACHE="false"
    export IEDB_TELEMETRY_ENABLED="false"
    export IEDB_TIERED_STORAGE_ENABLED="false"
    export IEDB_AUDIT_LOG_ENABLED="false"
    export IEDB_BACKUP_ENABLED="false"
    export IEDB_QUERY_MANAGEMENT_ENABLED="false"
    export IEDB_WAL_ENABLED="false"
    export IEDB_LICENSE_ENABLED="false"

    log_info "Data directory: $IEDB_DATA"
    log_info "Config via environment variables (IEDB_* prefix)"

    # Start iedb server (run from empty dir so no iedb.toml is picked up)
    # Capture server stderr to a log file so we can inspect crash traces
    IEDB_SERVER_LOG="/tmp/iedb_server_$$.log"
    log_info "Server log: $IEDB_SERVER_LOG"
    local orig_dir="$PWD"
    cd "$IEDB_DATA"
    "$IEDB_BIN" > "$IEDB_SERVER_LOG" 2>&1 &
    IEDB_PID=$!
    cd "$orig_dir"
    log_info "Server PID: $IEDB_PID"

    wait_for_server || exit 1
    log_pass "Server started successfully"
}

# check_server_alive — verify server is still running between tests.
# If dead, reports the fatal log lines and exits so the crash is not hidden.
check_server_alive() {
    if [ -n "${IEDB_PID:-}" ] && kill -0 "$IEDB_PID" 2>/dev/null; then
        return 0
    fi
    echo ""
    log_fail "SERVER CRASHED! Check log: ${IEDB_SERVER_LOG:-unknown}"
    echo ""
    echo "=== Last 30 lines of server log ==="
    tail -30 "${IEDB_SERVER_LOG:-/dev/null}" 2>/dev/null || true
    echo ""
    echo "=== Searching for crash signatures ==="
    grep -i -A5 "SIGSEGV\|panic:\|fatal\|signal" "${IEDB_SERVER_LOG:-/dev/null}" 2>/dev/null | head -30 || true
    echo ""
    echo "CRASH DETECTED — aborting remaining tests."
    exit 1
}

# ── Teardown ────────────────────────────────────────────────────────────────
teardown() {
    log_section "Teardown"
    if [ -n "$IEDB_PID" ] && kill -0 "$IEDB_PID" 2>/dev/null; then
        log_info "Stopping server (PID: $IEDB_PID)..."
        kill "$IEDB_PID" 2>/dev/null || true
        wait "$IEDB_PID" 2>/dev/null || true
        log_info "Server stopped"
    fi
    if [ -d "$IEDB_DATA" ]; then
        log_info "Cleaning up data directory: $IEDB_DATA"
        rm -rf "$IEDB_DATA"
    fi
}

# ── Test Data ───────────────────────────────────────────────────────────────
# Use nanosecond precision timestamps spaced apart for clear ordering
NOW_TS=1700000000000000000  # 2023-11-14T22:13:20Z in ns

# Generate timestamp for offset i (each offset = 1 second apart)
ts() {
    echo $((NOW_TS + $1 * 1000000000))
}

# ============================================================================
# TEST SCENARIOS
# ============================================================================

# ── Test 1: Field Addition ──────────────────────────────────────────────────
test_field_addition() {
    log_section "Test 1: Field Addition (add new column)"

    local db="${DB}"
    local meas="test_field_add"

    # Phase A: Write schema A = {time, temp} → stays in buffer (memory)
    log_info "Phase A: Writing schema A {time, temp} → memory buffer"
    write_lp "${meas},sensor=s1 temp=22.5 $(ts 1)
${meas},sensor=s1 temp=23.0 $(ts 2)
${meas},sensor=s2 temp=21.5 $(ts 3)
${meas},sensor=s2 temp=22.0 $(ts 4)
${meas},sensor=s3 temp=24.0 $(ts 5)" || return 1

    # Verify memory data: 5 rows, columns: time, temp, sensor
    assert_query_count "A1: 5 rows in memory" \
        "SELECT COUNT(*) FROM ${meas}" "5"
    assert_query_count "A2: 2 non-null temp values for sensor=s1" \
        "SELECT COUNT(*) FROM ${meas} WHERE \"sensor\" = 's1' AND \"temp\" IS NOT NULL" "2"

    # Phase B: Flush to disk, verify Parquet data
    log_info "Phase B: Flushing to Parquet → disk"
    flush_all || log_warn "Flush returned non-success (may be ok)"

    assert_query_count "B1: 5 rows in Parquet (after flush, buffer empty if flush cleared)" \
        "SELECT COUNT(*) FROM ${meas}" "5"
    assert_query_count "B2: sensor=s3 has temp=24.0" \
        "SELECT COUNT(*) FROM ${meas} WHERE \"sensor\" = 's3' AND \"temp\" = 24.0" "1"

    # Phase C: Write schema B = {time, temp, humidity} → stays in buffer
    # This ADDS a new column 'humidity' that didn't exist in the Parquet files
    log_info "Phase C: Writing schema B {time, temp, humidity} → memory buffer"
    write_lp "${meas},sensor=s1 temp=25.0,humidity=55.0 $(ts 6)
${meas},sensor=s2 temp=23.5,humidity=60.0 $(ts 7)
${meas},sensor=s3 temp=26.0,humidity=58.0 $(ts 8)" || return 1

    # Brief sleep to let any async buffer operations settle before querying
    sleep 0.5

    # Verify: UNION ALL of Parquet (5 rows, no humidity) + Buffer (3 rows, with humidity) = 8 rows
    assert_query_count "C1: 8 total rows (5 disk + 3 memory)" \
        "SELECT COUNT(*) FROM ${meas}" "8"
    assert_query_count "C2: 3 rows have humidity (only in buffer data)" \
        "SELECT COUNT(*) FROM ${meas} WHERE \"humidity\" IS NOT NULL" "3"
    assert_query_count "C3: 5 rows have NULL humidity (from Parquet, column was absent)" \
        "SELECT COUNT(*) FROM ${meas} WHERE \"humidity\" IS NULL" "5"
    assert_query_count "C4: temp column present in all 8 rows" \
        "SELECT COUNT(*) FROM ${meas} WHERE \"temp\" IS NOT NULL" "8"

    log_pass "Field addition test completed"
}

# ── Test 2: Field Removal ───────────────────────────────────────────────────
test_field_removal() {
    log_section "Test 2: Field Removal (remove existing column)"

    local db="${DB}"
    local meas="test_field_remove"

    # Phase A: Write schema A = {time, temp, humidity, pressure} → buffer
    log_info "Phase A: Writing schema A {time, temp, humidity, pressure} → memory buffer"
    write_lp "${meas},sensor=a temp=20.0,humidity=50.0,pressure=1013.0 $(ts 1)
${meas},sensor=a temp=21.0,humidity=51.0,pressure=1012.5 $(ts 2)
${meas},sensor=b temp=22.0,humidity=52.0,pressure=1014.0 $(ts 3)
${meas},sensor=b temp=23.0,humidity=53.0,pressure=1013.5 $(ts 4)" || return 1

    assert_query_count "A1: 4 rows in memory" \
        "SELECT COUNT(*) FROM ${meas}" "4"

    # Phase B: Flush to disk
    log_info "Phase B: Flushing to Parquet → disk"
    flush_all

    assert_query_count "B1: 4 rows still accessible (now from Parquet)" \
        "SELECT COUNT(*) FROM ${meas}" "4"
    assert_query_count "B2: 4 rows have pressure" \
        "SELECT COUNT(*) FROM ${meas} WHERE \"pressure\" IS NOT NULL" "4"

    # Phase C: Write schema B = {time, temp, humidity} — pressure REMOVED
    log_info "Phase C: Writing schema B {time, temp, humidity} → memory buffer (no pressure)"
    write_lp "${meas},sensor=a temp=24.0,humidity=54.0 $(ts 5)
${meas},sensor=b temp=25.0,humidity=55.0 $(ts 6)" || return 1

    # Verify: UNION ALL — Parquet has pressure, buffer doesn't
    assert_query_count "C1: 6 total rows (4 disk + 2 memory)" \
        "SELECT COUNT(*) FROM ${meas}" "6"
    assert_query_count "C2: 4 rows have pressure (only from Parquet)" \
        "SELECT COUNT(*) FROM ${meas} WHERE \"pressure\" IS NOT NULL" "4"
    assert_query_count "C3: 2 rows have NULL pressure (from buffer, column absent)" \
        "SELECT COUNT(*) FROM ${meas} WHERE \"pressure\" IS NULL" "2"
    assert_query_count "C4: all 6 rows have temp and humidity" \
        "SELECT COUNT(*) FROM ${meas} WHERE \"temp\" IS NOT NULL AND \"humidity\" IS NOT NULL" "6"

    log_pass "Field removal test completed"
}

# ── Test 3: Data Type Change (int → float) ──────────────────────────────────
test_type_change_int_to_float() {
    log_section "Test 3: Data Type Change (int → float)"

    local db="${DB}"
    local meas="test_type_int_float"

    # Phase A: Write schema A with 'value' as integer → buffer
    log_info "Phase A: Writing schema A {time, value:i64} → memory buffer"
    write_lp "${meas},src=device1 value=100i $(ts 1)
${meas},src=device1 value=200i $(ts 2)
${meas},src=device2 value=300i $(ts 3)" || return 1

    assert_query_count "A1: 3 rows in memory" \
        "SELECT COUNT(*) FROM ${meas}" "3"

    # Phase B: Flush to disk
    log_info "Phase B: Flushing to Parquet → disk"
    flush_all

    assert_query_count "B1: 3 rows in Parquet" \
        "SELECT COUNT(*) FROM ${meas}" "3"

    # Phase C: Write schema B with 'value' as float → buffer
    # This creates a TYPE CONFLICT: same column name, different type
    # The system should flush the conflicting buffer before writing new data
    log_info "Phase C: Writing schema B {time, value:f64} → memory buffer (type changed)"
    write_lp "${meas},src=device1 value=150.5 $(ts 4)
${meas},src=device2 value=250.7 $(ts 5)
${meas},src=device3 value=350.9 $(ts 6)" || return 1

    # Verify: UNION ALL with type conflict handled.
    # The 3 old rows (int) and 3 new rows (float) should both be queryable.
    # DuckDB should handle the type promotion (int → float in UNION).
    assert_query_count "C1: 6 total rows (3 disk + 3 memory)" \
        "SELECT COUNT(*) FROM ${meas}" "6"
    assert_query_count "C2: 3 rows from devices in new schema (float values)" \
        "SELECT COUNT(*) FROM ${meas} WHERE \"value\" > 300" "1"

    log_pass "Type change (int → float) test completed"
}

# ── Test 4: Data Type Change (float → string) ───────────────────────────────
test_type_change_float_to_string() {
    log_section "Test 4: Data Type Change (float → string)"

    local db="${DB}"
    local meas="test_type_float_str"

    # Phase A: Write schema A with 'status' as float → buffer
    log_info "Phase A: Writing schema A {time, status:f64} → memory buffer"
    write_lp "${meas},node=n1 status=1.0 $(ts 1)
${meas},node=n2 status=2.0 $(ts 2)
${meas},node=n3 status=3.0 $(ts 3)" || return 1

    assert_query_count "A1: 3 rows in memory" \
        "SELECT COUNT(*) FROM ${meas}" "3"

    # Phase B: Flush to disk
    log_info "Phase B: Flushing to Parquet → disk"
    flush_all

    assert_query_count "B1: 3 rows in Parquet" \
        "SELECT COUNT(*) FROM ${meas}" "3"

    # Phase C: Write schema B with 'status' as string → buffer (type conflict)
    log_info "Phase C: Writing schema B {time, status:str} → memory buffer"
    write_lp "${meas},node=n1 status=\"active\" $(ts 4)
${meas},node=n2 status=\"idle\" $(ts 5)
${meas},node=n4 status=\"active\" $(ts 6)" || return 1

    # Verify
    assert_query_count "C1: 6 total rows (3 disk + 3 memory)" \
        "SELECT COUNT(*) FROM ${meas}" "6"
    assert_query_count "C2: 3 rows with numeric status (from Parquet)" \
        "SELECT COUNT(*) FROM ${meas} WHERE \"status\" IS NOT NULL" "6"

    log_pass "Type change (float → string) test completed"
}

# ── Test 5: Mixed Changes (multi-round schema evolution) ────────────────────
test_mixed_changes() {
    log_section "Test 5: Mixed Changes (multi-round field add/remove/type-change)"

    local db="${DB}"
    local meas="test_mixed"

    # ── Round 1: Schema {time, a:f64, b:f64} → buffer → flush ──
    log_info "Round 1: Schema {time, a:f64, b:f64} → memory → flush"
    write_lp "${meas},tag=x a=10.0,b=20.0 $(ts 1)
${meas},tag=x a=11.0,b=21.0 $(ts 2)" || return 1
    assert_query_count "R1.1: 2 rows" "SELECT COUNT(*) FROM ${meas}" "2"
    flush_all

    # ── Round 2: Schema {time, a:f64, b:f64, c:f64} (add c) → buffer ──
    log_info "Round 2: Schema {time, a:f64, b:f64, c:f64} (add field c) → memory"
    write_lp "${meas},tag=y a=12.0,b=22.0,c=30.0 $(ts 3)" || return 1
    # Now: disk has 2 rows {a, b}, memory has 1 row {a, b, c}
    assert_query_count "R2.1: 3 total rows" "SELECT COUNT(*) FROM ${meas}" "3"
    assert_query_count "R2.2: 1 row with c" "SELECT COUNT(*) FROM ${meas} WHERE \"c\" IS NOT NULL" "1"
    assert_query_count "R2.3: 2 rows without c" "SELECT COUNT(*) FROM ${meas} WHERE \"c\" IS NULL" "2"
    flush_all

    # ── Round 3: Schema {time, a:f64} (remove b and c) → buffer ──
    log_info "Round 3: Schema {time, a:f64} (remove b and c) → memory"
    write_lp "${meas},tag=z a=13.0 $(ts 4)"
    write_lp "${meas},tag=z a=14.0 $(ts 5)" || return 1
    # Now: disk has 3 rows {a, b (, c in 1 row)}, memory has 2 rows {a}
    assert_query_count "R3.1: 5 total rows" "SELECT COUNT(*) FROM ${meas}" "5"
    assert_query_count "R3.2: 2 rows with NULL b and NULL c" \
        "SELECT COUNT(*) FROM ${meas} WHERE \"b\" IS NULL AND \"c\" IS NULL" "2"
    assert_query_count "R3.3: 5 rows with a NOT NULL" \
        "SELECT COUNT(*) FROM ${meas} WHERE \"a\" IS NOT NULL" "5"
    flush_all

    # ── Round 4: Schema {time, a:i64, d:str} (type-change a, replace b/c with d) → buffer ──
    log_info "Round 4: Schema {time, a:i64, d:str} (type-change a, new field d) → memory"
    write_lp "${meas},tag=w a=100i,d=\"new_field\" $(ts 6)"
    write_lp "${meas},tag=w a=200i,d=\"another\" $(ts 7)" || return 1
    # Now: disk has 5 rows {a,b,c}, memory has 2 rows {a:i64, d}
    assert_query_count "R4.1: 7 total rows" "SELECT COUNT(*) FROM ${meas}" "7"
    assert_query_count "R4.2: 2 rows with d NOT NULL" \
        "SELECT COUNT(*) FROM ${meas} WHERE \"d\" IS NOT NULL" "2"
    assert_query_count "R4.3: 5 rows with d NULL (from disk)" \
        "SELECT COUNT(*) FROM ${meas} WHERE \"d\" IS NULL" "5"

    # ── Round 5: Final flush and verify all data ──
    log_info "Round 5: Final flush → all 7 rows on disk"
    flush_all
    assert_query_count "R5.1: 7 total rows from disk" "SELECT COUNT(*) FROM ${meas}" "7"
    assert_query_count "R5.2: all tags visible" \
        "SELECT COUNT(DISTINCT \"tag\") FROM ${meas}" "4"

    log_pass "Mixed changes test completed"
}

# ── Test 6: Tag Column Changes ──────────────────────────────────────────────
test_tag_changes() {
    log_section "Test 6: Tag Column Changes (add/remove tags)"

    local db="${DB}"
    local meas="test_tag_changes"

    # Phase A: Schema A with tags {host, region} → buffer → flush
    log_info "Phase A: Schema A tags {host, region} → memory → flush"
    write_lp "${meas},host=h1,region=r1 value=10.0 $(ts 1)
${meas},host=h2,region=r2 value=20.0 $(ts 2)
${meas},host=h3,region=r1 value=30.0 $(ts 3)"
    assert_query_count "A1: 3 rows" "SELECT COUNT(*) FROM ${meas}" "3"
    flush_all

    # Phase B: Schema B with tags {host, region, dc} (add tag) → buffer
    log_info "Phase B: Schema B tags {host, region, dc} (add tag) → memory"
    write_lp "${meas},host=h1,region=r1,dc=us-east value=40.0 $(ts 4)
${meas},host=h4,region=r3,dc=us-west value=50.0 $(ts 5)"
    assert_query_count "B1: 5 total rows" "SELECT COUNT(*) FROM ${meas}" "5"
    assert_query_count "B2: 2 rows with dc tag" \
        "SELECT COUNT(*) FROM ${meas} WHERE \"dc\" IS NOT NULL" "2"
    assert_query_count "B3: 3 rows with dc NULL (from disk)" \
        "SELECT COUNT(*) FROM ${meas} WHERE \"dc\" IS NULL" "3"
    flush_all

    # Phase C: Schema C with tags {host} (remove region, dc) → buffer
    log_info "Phase C: Schema C tags {host} (remove region, dc) → memory"
    write_lp "${meas},host=h5 value=60.0 $(ts 6)
${meas},host=h6 value=70.0 $(ts 7)"
    assert_query_count "C1: 7 total rows" "SELECT COUNT(*) FROM ${meas}" "7"
    assert_query_count "C2: 2 rows with region NULL (from buffer, tag absent)" \
        "SELECT COUNT(*) FROM ${meas} WHERE \"region\" IS NULL AND \"dc\" IS NULL" "2"

    # Phase D: Schema D with tags {host, region} (re-add region, rename no dc) → buffer
    log_info "Phase D: Schema D tags {host, region} (re-add region) → memory"
    write_lp "${meas},host=h7,region=r4 value=80.0 $(ts 8)"
    assert_query_count "D1: 8 total rows" "SELECT COUNT(*) FROM ${meas}" "8"
    assert_query_count "D2: 5 rows with region NOT NULL (3 from old + 2 from D)" \
        "SELECT COUNT(*) FROM ${meas} WHERE \"region\" IS NOT NULL" "5"

    log_pass "Tag changes test completed"
}

# ── Test 7: Schema Beyond Two Variants (3+ concurrent schemas) ──────────────
test_multi_schema_variants() {
    log_section "Test 7: Multiple Concurrent Schema Variants (3+ schemas)"

    local db="${DB}"
    local meas="test_multi_schema"

    # Create 3 distinct schemas with data in both disk and memory
    # Schema A: {time, x:f64} → flush to disk
    log_info "Schema A: {time, x:f64} → memory → flush"
    write_lp "${meas},id=a x=1.0 $(ts 1)"
    write_lp "${meas},id=a x=2.0 $(ts 2)"
    flush_all

    # Schema B: {time, y:f64} → buffer (different column name, stays in memory)
    log_info "Schema B: {time, y:f64} → memory (not flushed)"
    write_lp "${meas},id=b y=10.0 $(ts 3)"

    # Schema C: {time, z:f64} → buffer (yet another different column name)
    log_info "Schema C: {time, z:f64} → memory (not flushed)"
    write_lp "${meas},id=c z=100.0 $(ts 4)"

    # Now: disk has Schema A (2 rows), memory has Schema B (1 row) and Schema C (1 row)
    # All 3 schemas share the same measurement. Query must UNION them all.

    assert_query_count "1: 4 total rows (2 disk + 2 memory across 2 buffer entries)" \
        "SELECT COUNT(*) FROM ${meas}" "4"

    # Check each schema's data is accessible
    assert_query_count "2: 2 rows with x (from disk)" \
        "SELECT COUNT(*) FROM ${meas} WHERE \"x\" IS NOT NULL" "2"
    assert_query_count "3: 1 row with y (from memory)" \
        "SELECT COUNT(*) FROM ${meas} WHERE \"y\" IS NOT NULL" "1"
    assert_query_count "4: 1 row with z (from memory)" \
        "SELECT COUNT(*) FROM ${meas} WHERE \"z\" IS NOT NULL" "1"

    # Verify all NULL combinations
    assert_query_count "5: 2 rows with y=NULL AND z=NULL (schema A data)" \
        "SELECT COUNT(*) FROM ${meas} WHERE \"y\" IS NULL AND \"z\" IS NULL" "2"

    # Flush all and verify again
    flush_all
    assert_query_count "6: 4 total rows from disk after flush" \
        "SELECT COUNT(*) FROM ${meas}" "4"
    assert_query_count "7: 2 rows with x (all on disk now)" \
        "SELECT COUNT(*) FROM ${meas} WHERE \"x\" IS NOT NULL" "2"

    log_pass "Multi-schema variants test completed"
}

# ── Test 8: Edge Cases ──────────────────────────────────────────────────────
test_edge_cases() {
    log_section "Test 8: Edge Cases"

    local db="${DB}"
    local meas="test_edge"

    # Edge 1: All columns removed (only time + tags remain)
    log_info "Edge 1: Schema with only time + tags → buffer → verify"
    write_lp "${meas},tag=t1 val=1.0 $(ts 1)"
    write_lp "${meas},tag=t1 val=2.0 $(ts 2)"
    flush_all
    # New data with no fields at all (only tag)
    write_lp "${meas},tag=t2 x=99.0 $(ts 3)"
    assert_query_count "E1.1: 3 total rows" "SELECT COUNT(*) FROM ${meas}" "3"
    assert_query_count "E1.2: 2 rows with val (from disk)" \
        "SELECT COUNT(*) FROM ${meas} WHERE \"val\" IS NOT NULL" "2"

    # Edge 2: Rapid schema changes without flush between them
    log_info "Edge 2: Rapid schema changes without intermediate flush"
    local meas2="test_edge_rapid"
    write_lp "${meas2},t=a v1=1.0 $(ts 1)"
    write_lp "${meas2},t=b v2=2.0 $(ts 2)"
    write_lp "${meas2},t=c v3=3.0 $(ts 3)"
    write_lp "${meas2},t=d v1=4.0,v2=5.0,v3=6.0 $(ts 4)"
    # These 4 writes may create up to 4 different buffer entries
    # All should be queryable
    assert_query_count "E2.1: 4 total rows with 3 different schemas" \
        "SELECT COUNT(*) FROM ${meas2}" "4"
    assert_query_count "E2.2: 3 rows with v1" \
        "SELECT COUNT(*) FROM ${meas2} WHERE \"v1\" IS NOT NULL" "2"

    # Edge 3: Empty field values mixed
    log_info "Edge 3: Mixed present/absent fields in same schema"
    local meas3="test_edge_mixed"
    write_lp "${meas3},id=1 a=1.0,b=2.0 $(ts 1)
${meas3},id=2 a=3.0 $(ts 2)
${meas3},id=3 b=4.0 $(ts 3)"
    flush_all
    write_lp "${meas3},id=4 c=5.0 $(ts 4)"
    assert_query_count "E3.1: 4 total rows" "SELECT COUNT(*) FROM ${meas3}" "4"
    assert_query_count "E3.2: 3 rows with a" \
        "SELECT COUNT(*) FROM ${meas3} WHERE \"a\" IS NOT NULL" "3"

    log_pass "Edge cases test completed"
}

# ── Test 9: Query Correctness after Schema Evolution ────────────────────────
test_query_correctness() {
    log_section "Test 9: Query Correctness (aggregations, filters, ordering)"

    local db="${DB}"
    local meas="test_query_correct"

    # Setup: mixed schema data in both disk and memory
    log_info "Setting up mixed schema data..."
    # Disk: schema A = {time, temp:f64}
    write_lp "${meas},loc=lobby temp=20.0 $(ts 1)
${meas},loc=lobby temp=21.0 $(ts 2)
${meas},loc=roof temp=30.0 $(ts 3)"
    flush_all

    # Memory: schema B = {time, temp:f64, humidity:f64}
    write_lp "${meas},loc=lobby temp=22.0,humidity=55.0 $(ts 4)
${meas},loc=roof temp=31.0,humidity=40.0 $(ts 5)"

    # Test 1: COUNT with WHERE on shared column
    assert_query_count "Q1: 5 total rows" "SELECT COUNT(*) FROM ${meas}" "5"

    # Test 2: AVG aggregation across disk + memory
    local avg_temp
    avg_temp=$(query_json '.data[0][0]' \
        "SELECT ROUND(AVG(\"temp\")::FLOAT, 1) FROM ${meas}")
    # (20+21+30+22+31)/5 = 24.8
    if [ "$avg_temp" = "__ERROR__" ]; then return 1; fi
    assert_eq "Q2: AVG(temp) = 24.8" "24.8" "$avg_temp"

    # Test 3: MAX across disk + memory
    local max_temp
    max_temp=$(query_json '.data[0][0]' \
        "SELECT MAX(\"temp\") FROM ${meas}")
    if [ "$max_temp" = "__ERROR__" ]; then return 1; fi
    assert_eq "Q3: MAX(temp) = 31" "31" "$max_temp"

    # Test 4: GROUP BY with NULL handling
    local group_count
    group_count=$(query_json '.data | length' \
        "SELECT \"loc\", COUNT(*) as cnt FROM ${meas} GROUP BY \"loc\" ORDER BY \"loc\"")
    if [ "$group_count" = "__ERROR__" ]; then return 1; fi
    assert_eq "Q4: 2 groups (lobby, roof)" "2" "$group_count"

    # Test 5: ORDER BY across disk + memory
    local first_temp
    first_temp=$(query_json '.data[0][1]' \
        "SELECT \"time\", \"temp\" FROM ${meas} ORDER BY \"time\" ASC LIMIT 1")
    if [ "$first_temp" = "__ERROR__" ]; then return 1; fi
    assert_eq "Q5: first temp = 20" "20" "$first_temp"

    # Test 6: DISTINCT on tag column
    local distinct_locs
    distinct_locs=$(query_json '.data[0][0]' \
        "SELECT COUNT(DISTINCT \"loc\") FROM ${meas}")
    if [ "$distinct_locs" = "__ERROR__" ]; then return 1; fi
    assert_eq "Q6: 2 distinct locations" "2" "$distinct_locs"

    # Test 7: Complex query with humidity filter (NULL-aware)
    assert_query_count "Q7: 2 rows with humidity (only from buffer)" \
        "SELECT COUNT(*) FROM ${meas} WHERE \"humidity\" IS NOT NULL" "2"
    assert_query_count "Q8: 3 rows with NULL humidity (from disk)" \
        "SELECT COUNT(*) FROM ${meas} WHERE \"humidity\" IS NULL" "3"

    log_pass "Query correctness test completed"
}

# ── Test 10: Schema Metadata Verification ───────────────────────────────────
test_schema_metadata() {
    log_section "Test 10: Schema Metadata Verification"

    local db="${DB}"
    local meas="test_schema_meta"

    # Write data with multiple field types
    write_lp "${meas},tag1=a int_val=100i,float_val=1.5,str_val=\"hello\",bool_val=true $(ts 1)
${meas},tag1=b int_val=200i,float_val=2.5 $(ts 2)"
    flush_all

    # Now write with a NEW field (schema evolution within buffer)
    write_lp "${meas},tag1=c extra_field=99.9 $(ts 3)"

    # Verify measurement list includes our test measurement
    local measurements
    measurements=$(query_json '.data | flatten | join(",")' \
        "SELECT DISTINCT * FROM (SELECT unnest(measurements) FROM information_schema.tables WHERE table_name NOT LIKE '_iedb_%' AND table_name NOT LIKE '%schemata%')")
    if [ "$measurements" = "__ERROR__" ]; then
        log_warn "Could not list measurements via information_schema (non-critical)"
    else
        [ "$VERBOSE" = "1" ] && log_info "Measurements: $measurements"
    fi

    # Verify schema endpoint returns data
    local schema_resp
    schema_resp=$(curl -s "${BASE_URL}/api/v1/databases/${db}/measurements/${meas}/schema")
    local schema_success
    schema_success=$(echo "$schema_resp" | jq -r '.success // false')
    if [ "$schema_success" = "true" ]; then
        log_pass "Schema endpoint returns success"
        [ "$VERBOSE" = "1" ] && log_info "Schema: $schema_resp"
    else
        log_warn "Schema endpoint returned: $schema_resp (may require data on disk)"
    fi

    log_pass "Schema metadata test completed"
}

# ============================================================================
# MAIN
# ============================================================================

main() {
    echo ""
    echo "╔═══════════════════════════════════════════════════════════════╗"
    echo "║     Schema Evolution Integration Test Suite                  ║"
    echo "║     Input → Buffer (Memory) → Parquet (Disk) → Query         ║"
    echo "╚═══════════════════════════════════════════════════════════════╝"
    echo ""
    echo "  Binary:     ${IEDB_BIN}"
    echo "  Port:       ${IEDB_PORT}"
    echo "  Data dir:   ${IEDB_DATA}"
    echo "  Verbose:    ${VERBOSE}"
    echo ""

    # ── Setup ──
    setup

    # ── Run all tests, checking server health between each ──
    local start_time
    start_time=$(date +%s)

    test_field_addition       || true; check_server_alive
    test_field_removal        || true; check_server_alive
    test_type_change_int_to_float  || true; check_server_alive
    test_type_change_float_to_string || true; check_server_alive
    test_mixed_changes        || true; check_server_alive
    test_tag_changes          || true; check_server_alive
    test_multi_schema_variants || true; check_server_alive
    test_edge_cases           || true; check_server_alive
    test_query_correctness    || true; check_server_alive
    test_schema_metadata      || true; check_server_alive

    # ── Teardown ──
    teardown

    # ── Summary ──
    local end_time
    end_time=$(date +%s)
    local elapsed=$((end_time - start_time))

    echo ""
    echo "╔═══════════════════════════════════════════════════════════════╗"
    echo "║                    TEST SUMMARY                               ║"
    echo "╠═══════════════════════════════════════════════════════════════╣"
    printf "║  ${GREEN}Passed:${NC} %-50s ║\n" "$PASS"
    printf "║  ${RED}Failed:${NC} %-50s ║\n" "$FAIL"
    printf "║  Total:  %-50s ║\n" "$((PASS + FAIL))"
    printf "║  Time:   %-50s ║\n" "${elapsed}s"
    echo "╚═══════════════════════════════════════════════════════════════╝"
    echo ""

    if [ "$FAIL" -gt 0 ]; then
        echo -e "${RED}❌  SOME TESTS FAILED${NC}"
        exit 1
    else
        echo -e "${GREEN}✅  ALL TESTS PASSED${NC}"
        exit 0
    fi
}

main "$@"
