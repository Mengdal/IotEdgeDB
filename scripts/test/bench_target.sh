#!/bin/sh
# ============================================================================
# iedb Benchmark — runs ON the target machine, measures real performance.
#
# Usage (on target machine):
#   chmod +x bench_target.sh
#   ./bench_target.sh [host] [port]
#
# Measures:
#   1. Write throughput — records/sec via Line Protocol (batch sizes: 1, 10, 100, 1000)
#   2. Buffer view query — latency before flush (in-memory data)
#   3. Flush performance — time to flush N records to Parquet
#   4. Query latency — simple SELECT, aggregation, time-range
#   5. Cache hit speedup — first vs second query
# ============================================================================
HOST="${1:-localhost}"
PORT="${2:-8000}"
BASE="http://${HOST}:${PORT}"
BENCH_DB="bench_$$"
TIMEFMT="real %R user %U sys %S"

echo "============================================"
echo " iedb Benchmark — ARM64 Target"
echo " Target: $BASE"
echo " Time:   $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================"
echo ""

# ---------- helper ----------
api_write() {
    curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE/api/v1/write/line-protocol" \
        -H "Content-Type: text/plain" \
        -H "x-iedb-database: $BENCH_DB" \
        --data-binary "@-"
}

api_query() {
    curl -s -X POST "$BASE/api/v1/query" \
        -H "Content-Type: application/json" \
        -H "x-iedb-database: $BENCH_DB" \
        -d "$1"
}

api_flush() {
    curl -s -X POST "$BASE/api/v1/write/line-protocol/flush"
}

# ---------- 1. WRITE THROUGHPUT ----------
echo "=== 1. Write Throughput (Line Protocol) ==="
for BATCH in 1 10 100 500; do
    TOTAL=$((BATCH * 10))
    # Generate LP data
    PAYLOAD=""
    TS=$(date +%s)000000000
    i=0
    while [ $i -lt $BATCH ]; do
        PAYLOAD="${PAYLOAD}cpu_bench,host=srv${i} usage=$(( RANDOM % 100 )).$(( RANDOM % 10 )) ${TS}\n"
        i=$((i + 1))
    done

    START=$(date +%s%3N 2>/dev/null || date +%s)
    j=0
    ERRORS=0
    while [ $j -lt 10 ]; do
        CODE=$(echo -e "$PAYLOAD" | api_write)
        if [ "$CODE" != "204" ]; then
            ERRORS=$((ERRORS + 1))
        fi
        j=$((j + 1))
    done
    END=$(date +%s%3N 2>/dev/null || date +%s)
    ELAPSED_MS=$((END - START))
    if [ $ELAPSED_MS -le 0 ]; then
        ELAPSED_MS=1
    fi
    RPS=$((TOTAL * 1000 / ELAPSED_MS))
    echo "  batch=$BATCH x10: ${TOTAL} records in ${ELAPSED_MS}ms = ${RPS} rec/s (errors=$ERRORS)"
done

# flush for next tests
api_flush > /dev/null

# ---------- 2. BUFFER VIEW QUERY ----------
echo ""
echo "=== 2. Buffer View Query (before flush) ==="
TS=$(date +%s)000000000
echo -e "cpu_bv,host=a usage=42.0 ${TS}" | api_write > /dev/null

START=$(date +%s%3N 2>/dev/null || date +%s)
RESULT=$(api_query '{"sql":"SELECT * FROM cpu_bv LIMIT 10"}')
END=$(date +%s%3N 2>/dev/null || date +%s)
ELAPSED=$((END - START))
ROWS=$(echo "$RESULT" 2>/dev/null | grep -o '"row_count":[0-9]*' | head -1 | cut -d: -f2)
echo "  buffer view query: ${ELAPSED}ms, rows=$ROWS"
if [ "$ROWS" = "1" ]; then
    echo "  ✓ buffer view working"
else
    echo "  ✗ buffer view returned $ROWS rows (expected 1)"
fi

api_flush > /dev/null

# ---------- 3. FLUSH PERFORMANCE ----------
echo ""
echo "=== 3. Flush Performance ==="
for N in 10 100 1000; do
    PAYLOAD=""
    TS=$(date +%s)000000000
    k=0
    while [ $k -lt $N ]; do
        PAYLOAD="${PAYLOAD}cpu_flush,host=srv${k} usage=$(( RANDOM % 100 )) ${TS}\n"
        k=$((k + 1))
    done
    echo -e "$PAYLOAD" | api_write > /dev/null

    START=$(date +%s%3N 2>/dev/null || date +%s)
    api_flush > /dev/null
    END=$(date +%s%3N 2>/dev/null || date +%s)
    ELAPSED=$((END - START))
    echo "  flush $N records: ${ELAPSED}ms"
done

# ---------- 4. QUERY LATENCY ----------
echo ""
echo "=== 4. Query Latency ==="
QUERIES="
  simple|SELECT * FROM cpu_flush LIMIT 10
  count|SELECT count(*) FROM cpu_flush
  group|SELECT host, avg(usage) FROM cpu_flush GROUP BY host LIMIT 5
  time_range|SELECT * FROM cpu_flush WHERE time > NOW() - INTERVAL 1 HOUR LIMIT 100
"

echo "$QUERIES" | while IFS='|' read -r name sql; do
    [ -z "$name" ] && continue
    # Warm up
    api_query "{\"sql\":\"$sql\"}" > /dev/null 2>&1
    # Measure
    START=$(date +%s%3N 2>/dev/null || date +%s)
    api_query "{\"sql\":\"$sql\"}" > /dev/null 2>&1
    END=$(date +%s%3N 2>/dev/null || date +%s)
    ELAPSED=$((END - START))
    echo "  $name: ${ELAPSED}ms"
done

# ---------- 5. CACHE SPEEDUP ----------
echo ""
echo "=== 5. Cache Speedup ==="
SQL='{"sql":"SELECT host, count(*) FROM cpu_flush GROUP BY host"}'
# First query (cache miss)
START=$(date +%s%3N 2>/dev/null || date +%s)
api_query "$SQL" > /dev/null
END=$(date +%s%3N 2>/dev/null || date +%s)
T1=$((END - START))
# Second query (cache hit)
START=$(date +%s%3N 2>/dev/null || date +%s)
api_query "$SQL" > /dev/null
END=$(date +%s%3N 2>/dev/null || date +%s)
T2=$((END - START))
echo "  first (cache miss): ${T1}ms"
echo "  second (cache hit): ${T2}ms"
if [ $T1 -gt 0 ]; then
    SPEEDUP=$(echo "scale=1; $T1 / $T2" 2>/dev/null || echo "N/A")
    echo "  speedup: ${SPEEDUP}x"
fi

# ---------- SUMMARY ----------
echo ""
echo "============================================"
echo " System Info"
echo "============================================"
echo "  CPU:    $(grep -c processor /proc/cpuinfo 2>/dev/null || echo 'N/A') cores"
echo "  Memory: $(free -m 2>/dev/null | grep Mem | awk '{print $2" MB"}' || echo 'N/A')"
echo "  Disk:   $(df -h / 2>/dev/null | tail -1 | awk '{print $4" free of "$2}' || echo 'N/A')"

# cleanup
curl -s -X POST "$BASE/api/v1/delete" -H "Content-Type: application/json" \
    -H "x-iedb-database: $BENCH_DB" -d "{\"database\":\"$BENCH_DB\"}" > /dev/null 2>&1

echo ""
echo "Benchmark complete."
