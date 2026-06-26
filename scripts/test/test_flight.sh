#!/bin/bash
# ============================================================================
# iedb Arrow Flight Test — verifies Flight gRPC server is operational.
#
# Requires: Python 3 + pyarrow (pip install pyarrow)
#
# Usage:
#   ./scripts/test/test_flight.sh [host] [flight_port]
# ============================================================================
set -euo pipefail

# Unset proxy for local network access
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY all_proxy ALL_PROXY 2>/dev/null || true


HOST="${1:-${IEDB_HOST:-localhost}}"
FLIGHT_PORT="${2:-${IEDB_FLIGHT_PORT:-9090}}"
PASS=0; FAIL=0

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

echo "=== iedb Arrow Flight Test ==="
echo "Target: $HOST:$FLIGHT_PORT"

# Check pyarrow
if ! python3 -c "import pyarrow" 2>/dev/null; then
    echo -e "  \033[33m⚠\033[0m pyarrow not installed, skipping Flight test"
    echo "    Install: pip install pyarrow"
    exit 0
fi

TMP_SCRIPT=$(mktemp /tmp/iedb_flight_test.XXXXXX.py)
cat > "$TMP_SCRIPT" << 'PYEOF'
import sys, json
import pyarrow as pa
import pyarrow.flight as flight

host = sys.argv[1]
port = int(sys.argv[2])
location = f"grpc://{host}:{port}"

try:
    client = flight.FlightClient(location)
    # Test: list flights
    flights = list(client.list_flights())
    print(f"FLIGHTS:{len(flights)}")

    # Test: simple DoGet query
    descriptor = flight.FlightDescriptor.for_command(json.dumps({"sql": "SELECT 1 AS value, 'hello' AS greeting"}))
    info = client.get_flight_info(descriptor)
    reader = client.do_get(info.endpoints[0].ticket)
    table = reader.read_all()
    print(f"ROWS:{table.num_rows}")
    print(f"COLS:{table.num_columns}")
except Exception as e:
    print(f"ERROR:{e}")
    sys.exit(1)
PYEOF

OUTPUT=$(python3 "$TMP_SCRIPT" "$HOST" "$FLIGHT_PORT" 2>&1) || true
rm -f "$TMP_SCRIPT"

FLIGHTS=$(echo "$OUTPUT" | grep "FLIGHTS:" | cut -d: -f2)
ROWS=$(echo "$OUTPUT" | grep "ROWS:" | cut -d: -f2)
COLS=$(echo "$OUTPUT" | grep "COLS:" | cut -d: -f2)
ERROR=$(echo "$OUTPUT" | grep "ERROR:" | cut -d: -f2- || echo "")

if [ -n "$ERROR" ]; then
    echo -e "  \033[31m✗\033[0m Flight error: $ERROR"
    FAIL=$((FAIL+1))
else
    check "Flight connected" "0" "${FLIGHTS:-x}"
    check "DoGet rows" "1" "${ROWS:-0}"
    check "DoGet columns" "2" "${COLS:-0}"
fi

echo ""
echo "=== Flight: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ] && echo -e "\033[32m✓ PASS\033[0m" || echo -e "\033[31m✗ FAIL\033[0m"
exit $FAIL
