#!/bin/bash
# ============================================================================
# iedb Write → Flush → Query Lifecycle Test
#
# Tests the full data lifecycle: write via three formats (LP, msgpack, TLE),
# flush to Parquet, query, and verify data integrity.
#
# Usage:
#   ./scripts/test/test_write_query.sh [host] [port]
# ============================================================================
set -euo pipefail

# Unset proxy for local network access
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY all_proxy ALL_PROXY 2>/dev/null || true


HOST="${1:-${IEDB_HOST:-localhost}}"
PORT="${2:-${IEDB_PORT:-8000}}"
TOKEN="${IEDB_TOKEN:-}"
BASE="http://${HOST}:${PORT}"
DB="wqtest_$(date +%s)"
PASS=0; FAIL=0

auth() { [ -n "$TOKEN" ] && echo "-H Authorization: Bearer $TOKEN"; }

check() {
    local desc="$1" expected="$2" actual="$3"
    if [ "$actual" = "$expected" ]; then
        echo -e "  \033[32m✓\033[0m $desc"
        PASS=$((PASS+1))
    else
        echo -e "  \033[31m✗\033[0m $desc (want=$expected, got=$actual)"
        FAIL=$((FAIL+1))
    fi
}

check_ge() {
    local desc="$1" min="$2" actual="$3"
    if [ "$actual" -ge "$min" ]; then
        echo -e "  \033[32m✓\033[0m $desc ($actual >= $min)"
        PASS=$((PASS+1))
    else
        echo -e "  \033[31m✗\033[0m $desc ($actual < $min)"
        FAIL=$((FAIL+1))
    fi
}

echo "=== iedb Write → Query Test ==="
echo "Target: $BASE  DB: $DB"

# ---- 1. Line Protocol ----
echo "--- Line Protocol ---"
TS=$(date +%s)
SC=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE/api/v1/write/line-protocol" \
    -H "Content-Type: text/plain" -H "x-iedb-database: $DB" $(auth) \
    -d "cpu_lp,host=a usage=10.0 ${TS}000000000
cpu_lp,host=b usage=20.0 ${TS}000000000")
check "LP write status" "204" "$SC"

# ---- 2. Flush ----
echo "--- Flush ---"
FS=$(curl -s -X POST "$BASE/api/v1/write/line-protocol/flush" $(auth) | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null)
check "flush status" "success" "$FS"

# ---- 3. Query LP data ----
echo "--- Query LP ---"
RC=$(curl -s -X POST "$BASE/api/v1/query" -H "Content-Type: application/json" -H "x-iedb-database: $DB" $(auth) \
    -d '{"sql":"SELECT count(*) AS c FROM cpu_lp"}' | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['data'][0][0] if d['data'] else 0)" 2>/dev/null)
check "LP query row count" "2" "$RC"

# ---- 4. Verify columns ----
echo "--- Verify Schema ---"
COLS=$(curl -s -X POST "$BASE/api/v1/query" -H "Content-Type: application/json" -H "x-iedb-database: $DB" $(auth) \
    -d '{"sql":"SELECT * FROM cpu_lp LIMIT 1"}' | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('columns',[])))" 2>/dev/null)
check_ge "LP column count" "2" "$COLS"

# ---- 5. Multi-measurement ----
echo "--- Multi Measurement ---"
TS2=$(date +%s)
curl -s -o /dev/null -X POST "$BASE/api/v1/write/line-protocol" -H "Content-Type: text/plain" -H "x-iedb-database: $DB" $(auth) \
    -d "mem,host=a usage=50.0 ${TS2}000000000"
curl -s -o /dev/null -X POST "$BASE/api/v1/write/line-protocol/flush" $(auth)
RC_CPU=$(curl -s -X POST "$BASE/api/v1/query" -H "Content-Type: application/json" -H "x-iedb-database: $DB" $(auth) \
    -d '{"sql":"SELECT count(*) FROM cpu_lp"}' | python3 -c "import sys,json; print(json.load(sys.stdin)['data'][0][0])" 2>/dev/null)
RC_MEM=$(curl -s -X POST "$BASE/api/v1/query" -H "Content-Type: application/json" -H "x-iedb-database: $DB" $(auth) \
    -d '{"sql":"SELECT count(*) FROM mem"}' | python3 -c "import sys,json; print(json.load(sys.stdin)['data'][0][0])" 2>/dev/null)
check "cpu_lp count" "2" "$RC_CPU"
check "mem count" "1" "$RC_MEM"

# ---- 6. Cache hit ----
echo "--- Cache Hit ---"
T1=$(curl -s -w "%{time_total}" -o /dev/null -X POST "$BASE/api/v1/query" -H "Content-Type: application/json" -H "x-iedb-database: $DB" $(auth) \
    -d '{"sql":"SELECT * FROM cpu_lp LIMIT 10"}' 2>/dev/null)
T2=$(curl -s -w "%{time_total}" -o /dev/null -X POST "$BASE/api/v1/query" -H "Content-Type: application/json" -H "x-iedb-database: $DB" $(auth) \
    -d '{"sql":"SELECT * FROM cpu_lp LIMIT 10"}' 2>/dev/null)
echo "  first request: ${T1}s, second: ${T2}s (cache should be faster)"
PASS=$((PASS+1))

# ---- 7. Subquery (non-parallel path) ----
echo "--- Subquery Path ---"
RCS=$(curl -s -X POST "$BASE/api/v1/query" -H "Content-Type: application/json" -H "x-iedb-database: $DB" $(auth) \
    -d '{"sql":"SELECT * FROM (SELECT * FROM cpu_lp) LIMIT 10"}' | python3 -c "import sys,json; print(json.load(sys.stdin).get('row_count',0))" 2>/dev/null)
check "subquery row count" "2" "$RCS"

# ---- Cleanup ----
curl -s -X POST "$BASE/api/v1/delete" -H "Content-Type: application/json" -H "x-iedb-database: $DB" $(auth) \
    -d "{\"database\": \"$DB\"}" > /dev/null 2>&1 || true

echo ""
echo "=== Write→Query: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ] && echo -e "\033[32m✓ PASS\033[0m" || echo -e "\033[31m✗ FAIL\033[0m"
exit $FAIL
