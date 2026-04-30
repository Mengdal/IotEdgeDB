#!/bin/bash
# Diagnostic script to check max_buffer_age_ms configuration

echo "=== iedb Buffer Flush Configuration Diagnostic ==="
echo ""

# Check iedb.toml
echo "1. Checking iedb.toml configuration:"
if [ -f "iedb.toml" ]; then
    grep -n "max_buffer_age_ms" iedb.toml || echo "   max_buffer_age_ms not found in iedb.toml (will use default: 5000)"
else
    echo "   iedb.toml not found"
fi
echo ""

# Check environment variable
echo "2. Checking environment variable:"
if [ -z "$IEDB_INGEST_MAX_BUFFER_AGE_MS" ]; then
    echo "   IEDB_INGEST_MAX_BUFFER_AGE_MS is not set"
else
    echo "   IEDB_INGEST_MAX_BUFFER_AGE_MS=$IEDB_INGEST_MAX_BUFFER_AGE_MS"
fi
echo ""

# Check running process
echo "3. Checking running iedb process environment:"
IEDB_PID=$(pgrep -f "iedb" | head -1)
if [ -n "$IEDB_PID" ]; then
    echo "   Found iedb process: PID $IEDB_PID"
    if [ -f "/proc/$IEDB_PID/environ" ]; then
        cat /proc/$IEDB_PID/environ | tr '\0' '\n' | grep "IEDB_INGEST_MAX_BUFFER_AGE_MS" || echo "   No IEDB_INGEST_MAX_BUFFER_AGE_MS in process environment"
    elif command -v lsof >/dev/null 2>&1; then
        echo "   (Process environment check not available on this system)"
    fi
else
    echo "   No running iedb process found"
fi
echo ""

# Check recent logs for actual flush timing
echo "4. Analyzing recent flush logs (if available):"
if [ -f "iedb.log" ]; then
    echo "   Recent buffer flushes (showing 'age' field):"
    grep "Flushing aged buffer" iedb.log | tail -5 | grep -oP '"age":\K[0-9.]+' | while read age; do
        echo "   - Buffer flushed at age: ${age}ms"
    done
else
    echo "   No iedb.log file found"
    echo "   Check your logs for lines containing 'Flushing aged buffer' and look for the 'age' field"
fi
echo ""

echo "=== Analysis ==="
echo "If you configured max_buffer_age_ms=2000 and see flushes at age ~4000ms,"
echo "this confirms the ticker phase misalignment bug."
echo ""
echo "Expected behavior:"
echo "  - max_buffer_age_ms=2000 → flush at ~2000-2100ms"
echo "  - max_buffer_age_ms=5000 → flush at ~5000-5100ms"
echo ""
echo "Bug behavior (2x timing):"
echo "  - max_buffer_age_ms=2000 → flush at ~4000ms"
echo "  - max_buffer_age_ms=5000 → flush at ~10000ms"
