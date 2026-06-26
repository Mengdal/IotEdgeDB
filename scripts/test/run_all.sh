#!/bin/bash
# ============================================================================
# iedb Test Suite — runs all test scripts and reports results.
#
# Usage:
#   ./scripts/test/run_all.sh [host] [port] [flight_port]
#
#   # With auth:
#   IEDB_TOKEN=xxx ./scripts/test/run_all.sh 192.168.0.230 8000 9090
#
#   # Run locally:
#   ./scripts/test/run_all.sh
#
#   # Run specific tests:
#   ./scripts/test/run_all.sh localhost 8000 "" smoke buffer_view
# ============================================================================
set -euo pipefail

# Unset proxy for local network access
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY all_proxy ALL_PROXY 2>/dev/null || true


SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
HOST="${1:-${IEDB_HOST:-localhost}}"
PORT="${2:-${IEDB_PORT:-8000}}"
FLIGHT_PORT="${3:-${IEDB_FLIGHT_PORT:-9090}}"
shift 3 2>/dev/null || true
SELECTED="${@:-smoke buffer_view write_query flight}"

TOTAL=0; PASSED=0; FAILED=0; SKIPPED=0

run_test() {
    local name="$1" script="$2"
    TOTAL=$((TOTAL + 1))
    echo ""
    echo "============================================================"
    echo "  RUNNING: $name"
    echo "============================================================"
    if [ -x "$script" ]; then
        if "$script" "$HOST" "$PORT"; then
            PASSED=$((PASSED + 1))
            echo "  RESULT: $name — PASSED"
        else
            FAILED=$((FAILED + 1))
            echo "  RESULT: $name — FAILED"
        fi
    else
        echo -e "  \033[33m⚠ SKIP\033[0m: $script not found or not executable"
        SKIPPED=$((SKIPPED + 1))
    fi
}

# Make all scripts executable
chmod +x "$SCRIPT_DIR"/*.sh 2>/dev/null || true

for name in $SELECTED; do
    case "$name" in
        smoke)       run_test "Smoke"           "$SCRIPT_DIR/test_smoke.sh" ;;
        buffer_view) run_test "Buffer View"     "$SCRIPT_DIR/test_buffer_view.sh" ;;
        write_query) run_test "Write → Query"   "$SCRIPT_DIR/test_write_query.sh" ;;
        flight)      run_test "Arrow Flight"    "$SCRIPT_DIR/test_flight.sh" "$HOST" "$FLIGHT_PORT" ;;
        *)           echo "Unknown test: $name (available: smoke buffer_view write_query flight)" ;;
    esac
done

echo ""
echo "============================================================"
echo "  SUMMARY: $PASSED passed, $FAILED failed, $SKIPPED skipped (of $TOTAL)"
echo "============================================================"

[ "$FAILED" -eq 0 ] && echo -e "\033[32m✓ ALL TESTS PASSED\033[0m" || echo -e "\033[31m✗ $FAILED TEST(S) FAILED\033[0m"
exit $FAILED
