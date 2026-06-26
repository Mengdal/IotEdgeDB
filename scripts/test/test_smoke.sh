#!/bin/bash
# ============================================================================
# iedb Smoke Test — quick health + basic query + multi-endpoint check.
# Runs in <5 seconds. Safe for production.
#
# Usage:
#   ./scripts/test/test_smoke.sh [host] [port]
#   IEDB_TOKEN=xxx ./scripts/test/test_smoke.sh 192.168.0.230 8000
# ============================================================================
set -euo pipefail

# Unset proxy for local network access
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY all_proxy ALL_PROXY 2>/dev/null || true


HOST="${1:-${IEDB_HOST:-localhost}}"
PORT="${2:-${IEDB_PORT:-8000}}"
TOKEN="${IEDB_TOKEN:-}"
BASE="http://${HOST}:${PORT}"
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

echo "=== iedb Smoke Test ==="
echo "Target: $BASE"

# 1. Health
S=$(curl -s "$BASE/health" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null)
check "health endpoint" "ok" "$S"

# 2. Ready
R=$(curl -s "$BASE/ready" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null)
check "ready endpoint" "ready" "$R"

# 3. Metrics
HM=$(curl -s -o /dev/null -w "%{http_code}" "$BASE/metrics")
check "metrics endpoint" "200" "$HM"

# 4. Simple query (no data needed)
Q=$(curl -s -X POST "$BASE/api/v1/query" \
    -H "Content-Type: application/json" \
    $(auth) \
    -d '{"sql":"SELECT 1 AS value"}' | python3 -c "import sys,json; print(json.load(sys.stdin).get('success',''))" 2>/dev/null)
check "SELECT 1 query" "True" "$Q"

# 5. Logs endpoint
LC=$(curl -s -o /dev/null -w "%{http_code}" "$BASE/api/v1/logs" $(auth))
check "logs endpoint accessible" "200" "$LC"

# 6. API metrics
AM=$(curl -s -o /dev/null -w "%{http_code}" "$BASE/api/v1/metrics" $(auth))
check "api metrics endpoint" "200" "$AM"

echo ""
echo "=== Smoke: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ] && echo -e "\033[32m✓ PASS\033[0m" || echo -e "\033[31m✗ FAIL\033[0m"
exit $FAIL
