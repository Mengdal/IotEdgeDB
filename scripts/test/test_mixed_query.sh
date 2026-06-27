#!/bin/bash
# ============================================================================
# iedb Mixed Query Test — data in both Parquet AND in-memory buffer.
#
# Scenario:
#   1. Write batch A (2 records) → flush → now in Parquet
#   2. Write batch B (2 records) → do NOT flush → stays in buffer
#   3. Query → must see all 4 records (2 from Parquet + 2 from buffer VIEW)
#
# This validates the full fix: buffer VIEW wrapping + schema-on-the-fly
# + read_parquet skip when needed + UNION ALL when both sources exist.
# ============================================================================
set -euo pipefail
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY all_proxy ALL_PROXY 2>/dev/null || true

HOST="${1:-${IEDB_HOST:-localhost}}"
PORT="${2:-${IEDB_PORT:-8000}}"
TOKEN="${IEDB_TOKEN:-}"
BASE="http://${HOST}:${PORT}"
DB="mixtest_$(date +%s)"
PASS=0; FAIL=0

auth() { [ -n "$TOKEN" ] && echo "-H Authorization: Bearer $TOKEN"; }

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

echo "============================================"
echo " iedb Mixed Query Test"
echo " Target: $BASE  DB: $DB"
echo "============================================"

# ---- Step 1: Write batch A, then flush ----
echo ""
echo "--- Step 1: Write batch A + flush ---"
TS_A1=$(date +%s)000000000
TS_A2=$(( $(date +%s) + 1 ))000000000
BATCH_A="cpu,host=srv-a1 usage=10.0 ${TS_A1}
cpu,host=srv-a2 usage=20.0 ${TS_A2}"

CODE_A=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE/api/v1/write/line-protocol" \
    -H "Content-Type: text/plain" -H "x-iedb-database: $DB" $(auth) \
    -d "$BATCH_A")
check "write batch A (2 records)" "204" "$CODE_A"

FLUSH=$(curl -s -X POST "$BASE/api/v1/write/line-protocol/flush" $(auth) \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null)
check "flush batch A" "success" "$FLUSH"

# ---- Step 2: Write batch B + query in ONE request chain ----
echo ""
echo "--- Step 2: Write batch B + instant query (before adaptive tick) ---"
TS_B1=$(date +%s)000000000
TS_B2=$(( $(date +%s) + 1 ))000000000
BATCH_B="cpu,host=srv-b1 usage=30.0 ${TS_B1}
cpu,host=srv-b2 usage=40.0 ${TS_B2}"

# Write batch B then query in rapid succession — beat the 1s adaptive engine tick
RESULT=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE/api/v1/write/line-protocol" \
    -H "Content-Type: text/plain" -H "x-iedb-database: $DB" $(auth) \
    -d "$BATCH_B" && \
    curl -s -X POST "$BASE/api/v1/query" \
    -H "Content-Type: application/json" -H "x-iedb-database: $DB" $(auth) \
    -d '{"sql":"SELECT host, usage FROM cpu ORDER BY usage"}')

ROW_COUNT=$(echo "$RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('row_count',0))" 2>/dev/null)
HOSTS=$(echo "$RESULT" | python3 -c "import sys,json; print(','.join(r[0] for r in json.load(sys.stdin).get('data',[])))" 2>/dev/null)

check "total rows (4 = 2 flushed + 2 in buffer)" "4" "$ROW_COUNT"
check "all 4 hosts present" "srv-a1,srv-a2,srv-b1,srv-b2" "$HOSTS"

# ---- Step 3: Verify response structure ----
echo ""
echo "--- Step 3: Verify response structure ---"
COLUMNS=$(echo "$RESULT" | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('columns',[])))" 2>/dev/null)
check "returns 2 columns (host, usage)" "2" "$COLUMNS"

# ---- Cleanup ----
curl -s -o /dev/null -X POST "$BASE/api/v1/delete" \
    -H "Content-Type: application/json" -H "x-iedb-database: $DB" $(auth) \
    -d "{\"database\":\"$DB\"}" 2>/dev/null || true

echo ""
echo "============================================"
echo " Results: $PASS passed, $FAIL failed"
echo "============================================"
[ "$FAIL" -eq 0 ] && green "✓ Mixed Query test PASSED" || red "✗ Mixed Query test FAILED"
exit $FAIL
