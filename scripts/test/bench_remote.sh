#!/bin/bash
# ============================================================================
# iedb Benchmark — runs from Mac/CI, targets remote ARM64 machine via HTTP.
#
# Usage:
#   ./scripts/test/bench_remote.sh [host] [port]
# ============================================================================
set -euo pipefail
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY all_proxy ALL_PROXY 2>/dev/null || true

HOST="${1:-192.168.0.230}"
PORT="${2:-8000}"
BASE="http://${HOST}:${PORT}"
BENCH_DB="bench_$$"
PASS=0; FAIL=0

green() { echo -e "\033[32m$*\033[0m"; }
cyan()  { echo -e "\033[36m$*\033[0m"; }
bold()  { echo -e "\033[1m$*\033[0m"; }

echo "============================================"
echo " iedb Benchmark — ARM64 Target"
echo " Target: $BASE"
echo " Time:   $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================"
echo ""

# ---------- helpers ----------
api_write() {
    curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE/api/v1/write/line-protocol" \
        -H "Content-Type: text/plain" \
        -H "x-iedb-database: $BENCH_DB" \
        --data-binary "@-"
}

api_query_ms() {
    # Returns: execution_time_ms from response (or 0 on error)
    local resp
    resp=$(curl -s -w "\n%{time_total}" -X POST "$BASE/api/v1/query" \
        -H "Content-Type: application/json" \
        -H "x-iedb-database: $BENCH_DB" \
        -d "$1" 2>/dev/null)
    echo "$resp" | tail -1
}

api_flush() {
    curl -s -o /dev/null -X POST "$BASE/api/v1/write/line-protocol/flush"
}

# ---------- 1. WRITE THROUGHPUT ----------
bold "=== 1. Write Throughput (Line Protocol) ==="
TS_BASE=$(date +%s)000000000
for BATCH in 1 10 100 500 1000; do
    PAYLOAD=""
    for ((i=0; i<BATCH; i++)); do
        PAYLOAD+="cpu_bench,host=srv${i} usage=$(( RANDOM % 100 )).$(( RANDOM % 10 )) ${TS_BASE}"$'\n'
    done

    ITER=10
    ERRORS=0
    START_TIME=$(python3 -c "import time; print(time.time())" 2>/dev/null || echo "0")
    for ((j=0; j<ITER; j++)); do
        CODE=$(echo "$PAYLOAD" | api_write)
        [ "$CODE" != "204" ] && ERRORS=$((ERRORS + 1))
    done
    END_TIME=$(python3 -c "import time; print(time.time())" 2>/dev/null || echo "0")

    ELAPSED=$(python3 -c "print(f'{float($END_TIME) - float($START_TIME):.3f}')" 2>/dev/null || echo "1")
    TOTAL_RECORDS=$((BATCH * ITER))
    RPS=$(python3 -c "print(int($TOTAL_RECORDS / float($ELAPSED)))" 2>/dev/null || echo "0")
    cyan "  batch=${BATCH} x${ITER}: ${TOTAL_RECORDS} rec / ${ELAPSED}s = ${RPS} rec/s (err=${ERRORS})"
done

# flush
api_flush > /dev/null
sleep 1

# ---------- 2. BUFFER VIEW QUERY ----------
echo ""
bold "=== 2. Buffer View Query (before flush) ==="
TS=$(date +%s)000000000
echo "cpu_bv,host=a usage=42.0 ${TS}" | api_write > /dev/null

T=$(api_query_ms '{"sql":"SELECT * FROM cpu_bv LIMIT 10"}')
cyan "  buffer view query: ${T}s (wall clock)"
# Verify
RESULT=$(curl -s -X POST "$BASE/api/v1/query" \
    -H "Content-Type: application/json" \
    -H "x-iedb-database: $BENCH_DB" \
    -d '{"sql":"SELECT * FROM cpu_bv LIMIT 10"}')
ROWS=$(echo "$RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('row_count',0))" 2>/dev/null || echo "0")
if [ "$ROWS" = "1" ]; then
    green "  ✓ buffer view: 1 row visible before flush"
else
    echo -e "  \033[31m✗ buffer view: $ROWS rows (expected 1)\033[0m"
    FAIL=$((FAIL+1))
fi
PASS=$((PASS+1))
api_flush > /dev/null

# ---------- 3. FLUSH PERFORMANCE ----------
echo ""
bold "=== 3. Flush Performance ==="
for N in 100 1000 5000; do
    PAYLOAD=""
    TS=$(date +%s)000000000
    for ((k=0; k<N; k++)); do
        PAYLOAD+="cpu_flush,host=srv${k} usage=$(( RANDOM % 100 )) ${TS}"$'\n'
    done
    echo "$PAYLOAD" | api_write > /dev/null

    START_TIME=$(python3 -c "import time; print(time.time())" 2>/dev/null || echo "0")
    api_flush > /dev/null
    END_TIME=$(python3 -c "import time; print(time.time())" 2>/dev/null || echo "0")
    ELAPSED=$(python3 -c "print(f'{float($END_TIME) - float($START_TIME):.3f}')" 2>/dev/null || echo "0")
    cyan "  flush ${N} records: ${ELAPSED}s"
done

# ---------- 4. QUERY LATENCY ----------
echo ""
bold "=== 4. Query Latency (warm) ==="

bench_query() {
    local name="$1" sql="$2"
    # Warm up: 3x
    for w in 1 2 3; do
        api_query_ms "{\"sql\":\"$sql\"}" > /dev/null 2>&1
    done
    # Measure 5x
    TOTAL_T=0
    for m in 1 2 3 4 5; do
        T=$(api_query_ms "{\"sql\":\"$sql\"}")
        TOTAL_T=$(python3 -c "print(f'{float($TOTAL_T) + float($T):.4f}')" 2>/dev/null || echo "0")
    done
    AVG=$(python3 -c "print(f'{float($TOTAL_T) / 5:.4f}')" 2>/dev/null || echo "0")
    cyan "  $name: avg ${AVG}s"
}

bench_query "simple"     "SELECT * FROM cpu_flush LIMIT 10"
bench_query "count"      "SELECT count(*) FROM cpu_flush"
bench_query "group"      "SELECT host, avg(usage) FROM cpu_flush GROUP BY host LIMIT 5"
bench_query "time_range" "SELECT * FROM cpu_flush WHERE host='srv50' LIMIT 10"

# ---------- 5. CACHE SPEEDUP ----------
echo ""
bold "=== 5. Cache Speedup ==="
SQL='{"sql":"SELECT host, count(*) FROM cpu_flush GROUP BY host"}'
# First query (cache miss)
T1=$(api_query_ms "$SQL")
# Second query (cache hit)
T2=$(api_query_ms "$SQL")
cyan "  first (cache miss): ${T1}s"
cyan "  second (cache hit): ${T2}s"
SPEEDUP=$(python3 -c "print(f'{float($T1) / max(float($T2), 0.0001):.1f}x')" 2>/dev/null || echo "N/A")
cyan "  speedup: ${SPEEDUP}"

# ---------- 6. PARALLEL vs NON-PARALLEL ----------
echo ""
bold "=== 6. Parallel vs Non-Parallel Path ==="
T_PAR=$(api_query_ms '{"sql":"SELECT * FROM cpu_flush LIMIT 100"}')
T_SUB=$(api_query_ms '{"sql":"SELECT * FROM (SELECT * FROM cpu_flush) LIMIT 100"}')
cyan "  parallel path:    ${T_PAR}s"
cyan "  non-parallel:     ${T_SUB}s"

# ---------- SUMMARY ----------
echo ""
echo "============================================"
bold "  SUMMARY"
echo "============================================"
green "  Tests passed: $PASS"
if [ "$FAIL" -gt 0 ]; then
    echo -e "  \033[31mTests failed: $FAIL\033[0m"
fi

# System info from target
echo ""
echo "--- Target System ---"
ssh -o ConnectTimeout=3 root@"$HOST" "
    echo \"  CPU:    \$(grep -c processor /proc/cpuinfo 2>/dev/null || echo N/A) cores\"
    echo \"  Memory: \$(free -m 2>/dev/null | grep Mem | awk '{print \$2\" MB\"}' || echo N/A)\"
    echo \"  Disk:   \$(df -h / 2>/dev/null | tail -1 | awk '{print \$4\" free of \"\$2}' || echo N/A)\"
    echo \"  DuckDB: \$(grep duckdb_version /home/iedb.log 2>/dev/null | head -1 | grep -o 'v[0-9.]*' || echo N/A)\"
" 2>/dev/null || echo "  (unable to SSH for system info)"

# cleanup
curl -s -o /dev/null -X POST "$BASE/api/v1/delete" \
    -H "Content-Type: application/json" \
    -H "x-iedb-database: $BENCH_DB" \
    -d "{\"database\":\"$BENCH_DB\"}" 2>/dev/null || true

echo ""
echo "Benchmark complete."
