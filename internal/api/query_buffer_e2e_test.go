//go:build duckdb_arrow

package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"iedb/internal/config"
	"iedb/internal/database"
	"iedb/internal/ingest"
	"iedb/internal/storage"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

type e2eEnv struct {
	db         *database.DuckDB
	buf        *ingest.ArrowBuffer
	queryH     *QueryHandler
	lineProtoH *LineProtocolHandler
	app        *fiber.App
}

func setupE2E(t *testing.T) *e2eEnv {
	t.Helper()
	logger := zerolog.New(zerolog.NewTestWriter(t)).Level(zerolog.Disabled)

	db, err := database.New(&database.Config{
		MaxConnections: 4,
		MemoryLimit:    "256MB",
		ThreadCount:    1,
		TimeZone:       "UTC",
	}, logger)
	if err != nil {
		t.Fatalf("New DuckDB: %v", err)
	}

	// Use a real temp directory so Parquet files persist and read_parquet works
	tmpDir, err := os.MkdirTemp("", "iedb-e2e-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	store, err := storage.NewLocalBackend(tmpDir, logger)
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}

	buf := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize:           10000,
		MaxBufferMemoryMB:      128,
		ShardCount:             4,
		FlushWorkers:           1,
		FlushQueueSize:         16,
		MinBufferMemoryMB:      16,
		MaxBufferAgeMS:         60000,
		MaxBufferAgeSeconds:    900,
		FlushTimeoutSeconds:    30,
		Compression:            "none",
	}, store, logger)

	queryH := NewQueryHandler(db, store, logger, 30, 0)
	queryH.SetBuffer(buf)
	queryH.SetAuthAndRBAC(nil, nil)

	lineProtoH := NewLineProtocolHandler(buf, logger)
	lineProtoH.SetAuthAndRBAC(nil, nil)

	app := fiber.New(fiber.Config{
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		},
	})
	queryH.RegisterRoutes(app)
	lineProtoH.RegisterRoutes(app)

	env := &e2eEnv{db: db, buf: buf, queryH: queryH, lineProtoH: lineProtoH, app: app}
	t.Cleanup(func() { buf.Close(); db.Close() })
	return env
}

func mustWriteLP(t *testing.T, env *e2eEnv, dbName, lp string) {
	t.Helper()
	req := httptest.NewRequest("POST", "/api/v1/write/line-protocol", bytes.NewBufferString(lp))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("x-iedb-database", dbName)
	resp, err := env.app.Test(req)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if resp.StatusCode != 204 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("write failed: status=%d body=%s", resp.StatusCode, body)
	}
}

func mustFlush(t *testing.T, env *e2eEnv) {
	t.Helper()
	req := httptest.NewRequest("POST", "/api/v1/write/line-protocol/flush", nil)
	resp, _ := env.app.Test(req)
	if resp.StatusCode != 200 {
		t.Fatalf("flush failed: status=%d", resp.StatusCode)
	}
}

func mustQuery(t *testing.T, env *e2eEnv, dbName, sql string) QueryResponse {
	t.Helper()
	queryBody, _ := json.Marshal(QueryRequest{SQL: sql})
	req := httptest.NewRequest("POST", "/api/v1/query", bytes.NewReader(queryBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-iedb-database", dbName)
	resp, err := env.app.Test(req)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	var qr QueryResponse
	json.NewDecoder(resp.Body).Decode(&qr)
	return qr
}

// ============================================================================
// E2E Test 1: Write → Flush → Query (parquet files exist, UNION ALL works)
// ============================================================================

func TestE2E_WriteFlushQuery_SeesData(t *testing.T) {
	env := setupE2E(t)
	ts := time.Now().Unix()
	mustWriteLP(t, env, "e2edb", fmt.Sprintf("cpu,host=a usage=1.0 %d000000000\ncpu,host=b usage=2.0 %d000000000", ts, ts))
	mustFlush(t, env)

	qr := mustQuery(t, env, "e2edb", "SELECT * FROM cpu ORDER BY host LIMIT 10")
	if qr.RowCount != 2 {
		t.Errorf("expected 2 rows after flush, got %d", qr.RowCount)
	}
	t.Logf("✅ E2E flush+query: %d rows", qr.RowCount)
}

// ============================================================================
// E2E Test 2: Write → Query after flush → Cache hit still works
// ============================================================================

func TestE2E_CachedQueryAfterFlush(t *testing.T) {
	env := setupE2E(t)
	ts := time.Now().Unix()
	mustWriteLP(t, env, "cachedb", fmt.Sprintf("cpu,host=x usage=9.9 %d000000000", ts))
	mustFlush(t, env)

	// First query (cache miss)
	qr1 := mustQuery(t, env, "cachedb", "SELECT * FROM cpu LIMIT 10")
	// Second query (cache hit)
	qr2 := mustQuery(t, env, "cachedb", "SELECT * FROM cpu LIMIT 10")

	if qr1.RowCount != 1 || qr2.RowCount != 1 {
		t.Errorf("cached: first=%d rows, second=%d rows (both want 1)", qr1.RowCount, qr2.RowCount)
	}
	t.Logf("✅ E2E cache: %d → %d rows", qr1.RowCount, qr2.RowCount)
}

// ============================================================================
// E2E Test 3: Empty database returns 0 rows gracefully
// ============================================================================

func TestE2E_EmptyDB_ReturnsZeroRows(t *testing.T) {
	env := setupE2E(t)
	qr := mustQuery(t, env, "emptydb", "SELECT * FROM cpu LIMIT 10")
	if !qr.Success {
		t.Errorf("empty query should succeed, got error: %s", qr.Error)
	}
	if qr.RowCount != 0 {
		t.Errorf("expected 0 rows, got %d", qr.RowCount)
	}
	t.Logf("✅ E2E empty: 0 rows")
}

// ============================================================================
// E2E Test 4: Multiple measurements query independently
// ============================================================================

func TestE2E_MultiMeasurement(t *testing.T) {
	env := setupE2E(t)
	ts := time.Now().Unix()
	mustWriteLP(t, env, "multi", fmt.Sprintf("cpu,host=a v=1.0 %d000000000", ts))
	mustWriteLP(t, env, "multi", fmt.Sprintf("mem,host=a v=2.0 %d000000000", ts))
	mustFlush(t, env)

	qr1 := mustQuery(t, env, "multi", "SELECT * FROM cpu LIMIT 10")
	qr2 := mustQuery(t, env, "multi", "SELECT * FROM mem LIMIT 10")
	if qr1.RowCount != 1 || qr2.RowCount != 1 {
		t.Errorf("cpu=%d mem=%d rows (both want 1)", qr1.RowCount, qr2.RowCount)
	}
	t.Logf("✅ E2E multi: cpu=%d mem=%d", qr1.RowCount, qr2.RowCount)
}

// ============================================================================
// E2E Test 5: Buffer VIEW schema construction
// ============================================================================

func TestE2E_BuildArrowSchema_FromColumns(t *testing.T) {
	env := setupE2E(t)
	ts := time.Now().Unix()
	mustWriteLP(t, env, "schemadb", fmt.Sprintf("cpu,host=a usage=1.5 %d000000000", ts))

	// Access buffer entry directly to verify schema construction
	keys := env.buf.MeasurementBufferKeys("schemadb", "cpu")
	if len(keys) == 0 {
		t.Fatal("no buffer keys for schemadb/cpu")
	}
	snap := env.buf.SnapshotEntry(keys[0])
	if snap == nil {
		t.Fatal("SnapshotEntry returned nil")
	}

	schema := database.BuildArrowSchema(snap)
	if schema == nil {
		t.Fatal("BuildArrowSchema returned nil")
	}
	if len(schema.Fields()) == 0 {
		t.Fatal("BuildArrowSchema returned empty schema")
	}
	t.Logf("✅ BuildArrowSchema: %d fields", len(schema.Fields()))
	for _, f := range schema.Fields() {
		t.Logf("  field: %s (%s)", f.Name, f.Type.Name())
	}
}
