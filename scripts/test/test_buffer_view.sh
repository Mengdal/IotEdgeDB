#!/bin/bash
# ============================================================================
# iedb Buffer View Test — verifies in-memory data is queryable before flush.
#
# This is the regression test for the buffer VIEW wrapping fix.
# It writes data and queries immediately WITHOUT flushing to Parquet.
#
# Usage:
#   ./scripts/test/test_buffer_view.sh [host] [port]
#
# Env vars:
#   IEDB_HOST  — defaults to localhost
#   IEDB_PORT  — defaults to 8000
#   IEDB_TOKEN — auth token (if auth is enabled)
# ============================================================================
set -euo pipefail

# Unset proxy for local network access
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY all_proxy ALL_PROXY 2>/dev/null || true


HOST="${1:-${IEDB_HOST:-localhost}}"
PORT="${2:-${IEDB_PORT:-8000}}"
TOKEN="${IEDB_TOKEN:-}"
BASE="http://${HOST}:${PORT}"

# Use a unique database per run to avoid pollution
DB="bvtest_$(date +%s)"
MEASUREMENT="cpu"
PASS=0
FAIL=0

auth_header() {
    if [ -n "$TOKEN" ]; then
        echo "-H Authorization: Bearer $TOKEN"
    fi
}

red()  { echo -e "\033[31m$*\033[0m"; }
green(){ echo -e "\033[32m$*\033[0m"; }

check() {
    local desc="$1" expected="$2" actual="$3"
    if [ "$actual" = "$expected" ]; then
        green "  ✓ $desc"
        PASS=$((PASS + 1))
    else
        red "  ✗ $desc (expected: $expected, got: $actual)"
        FAIL=$((FAIL + 1))
    fi
}

echo "=== iedb Buffer View Test ==="
echo "Target: $BASE"
echo "Database: $DB"
echo ""

# 1. Health check
echo "--- Health ---"
STATUS=$(curl -s "$BASE/health" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null || echo "FAIL")
check "health endpoint" "ok" "$STATUS"

# 2. Write data via Line Protocol
echo "--- Write ---"
TS=$(date +%s)
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE/api/v1/write/line-protocol" \
    -H "Content-Type: text/plain" \
    -H "x-iedb-database: $DB" \
    $(auth_header) \
    -d "${MEASUREMENT},host=srv1 usage=42.5 ${TS}000000000" 2>/dev/null)
check "write LP (204)" "204" "$HTTP_CODE"

# 3. Verify write stats
echo "--- Write Stats ---"
RECORDS=$(curl -s "$BASE/api/v1/write/line-protocol/stats" $(auth_header) | python3 -c "import sys,json; print(json.load(sys.stdin)['stats']['total_records'])" 2>/dev/null)
check "buffer has records" "1" "$RECORDS"

# 4. Query BEFORE flush — THE KEY TEST
echo "--- Query (before flush) ---"
RESP=$(curl -s -X POST "$BASE/api/v1/query" \
    -H "Content-Type: application/json" \
    -H "x-iedb-database: $DB" \
    $(auth_header) \
    -d "{\"sql\": \"SELECT * FROM $MEASUREMENT LIMIT 10\"}")
ROW_COUNT=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('row_count',-1))" 2>/dev/null)
check "query before flush sees data" "1" "$ROW_COUNT"

# 5. Flush + query again
echo "--- Flush ---"
FLUSH=$(curl -s -X POST "$BASE/api/v1/write/line-protocol/flush" $(auth_header) | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null)
check "flush success" "success" "$FLUSH"

echo "--- Query (after flush) ---"
RESP2=$(curl -s -X POST "$BASE/api/v1/query" \
    -H "Content-Type: application/json" \
    -H "x-iedb-database: $DB" \
    $(auth_header) \
    -d "{\"sql\": \"SELECT * FROM $MEASUREMENT LIMIT 10\"}")
ROW_COUNT2=$(echo "$RESP2" | python3 -c "import sys,json; print(json.load(sys.stdin).get('row_count',-1))" 2>/dev/null)
check "query after flush sees data" "1" "$ROW_COUNT2"

# 6. Cleanup — delete test data
echo "--- Cleanup ---"
curl -s -X POST "$BASE/api/v1/delete" \
    -H "Content-Type: application/json" \
    -H "x-iedb-database: $DB" \
    $(auth_header) \
    -d "{\"database\": \"$DB\"}" > /dev/null 2>&1 || true

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ] && green "✓ Buffer View test PASSED" || red "✗ Buffer View test FAILED"
exit $FAIL
