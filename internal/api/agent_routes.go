package api

import (
	"bytes"
	"fmt"
	"iedb/internal/agent"
	"iedb/internal/ingest"
	"sort"
	"time"

	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/gofiber/fiber/v2"
)

// AgentHandler handles agent registration and heartbeat.
type AgentHandler struct {
	registry *agent.AgentRegistry
	// arrowBuffer is the shared ingestion writer used to normalize the time
	// column (BIGINT → Timestamp) before persisting agent-uploaded parquet,
	// keeping every file in a partition schema-compatible with compaction.
	arrowBuffer *ingest.ArrowBuffer
}

// NewAgentHandler creates an AgentHandler.
func NewAgentHandler(registry *agent.AgentRegistry, arrowBuffer *ingest.ArrowBuffer) *AgentHandler {
	return &AgentHandler{registry: registry, arrowBuffer: arrowBuffer}
}

// RegisterRoutes registers agent API routes.
func (h *AgentHandler) RegisterRoutes(app *fiber.App) {
	app.Post("/api/v1/agents/register", h.handleRegister)
	app.Post("/api/v1/agents/heartbeat", h.handleHeartbeat)
	app.Post("/api/v1/ingest/parquet", h.handleIngestParquet)

	// Read-only monitoring views — any authenticated token, consistent with the
	// compaction/cluster read endpoints.
	app.Get("/api/v1/agents", h.handleListAgents)
	app.Get("/api/v1/agents/tables", h.handleListTableAgents)
}

type registerRequest struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

func (h *AgentHandler) handleRegister(c *fiber.Ctx) error {
	var req registerRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid request body",
		})
	}
	if req.ID == "" || req.URL == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "id and url are required",
		})
	}

	h.registry.Register(req.ID, req.URL)
	return c.Status(fiber.StatusCreated).JSON(fiber.Map{"status": "ok"})
}

type heartbeatRequest struct {
	ID string `json:"id"`
	// URL is optional. It lets a heartbeat auto-register an agent whose
	// registration was lost when the hub restarted; without it, an unknown id
	// is still ignored (an entry without a url is unreachable for query
	// merging).
	URL           string            `json:"url"`
	TablesChanged []agent.TableMeta `json:"tables_changed"`
}

func (h *AgentHandler) handleHeartbeat(c *fiber.Ctx) error {
	var req heartbeatRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid request body",
		})
	}
	if req.ID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "id is required",
		})
	}

	h.registry.Heartbeat(req.ID, req.URL, req.TablesChanged)
	return c.JSON(fiber.Map{"status": "ok"})
}

func (h *AgentHandler) handleIngestParquet(c *fiber.Ctx) error {
	db := c.Query("db")
	measurement := c.Query("measurement")

	if db == "" || measurement == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "db and measurement query params required",
		})
	}
	if !isValidDatabaseName(db) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid database name: must start with a letter and contain only alphanumeric characters, underscores, or hyphens (max 64 characters)",
		})
	}

	body := c.Body()
	if len(body) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "empty body",
		})
	}
	if h.arrowBuffer == nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "agent handler is misconfigured: ArrowBuffer is not set",
		})
	}

	// Decode the uploaded parquet into Arrow and re-write it through the shared
	// ingestion pipeline. Raw passthrough (storage.Write) left the agent's time
	// column as BIGINT nanoseconds, which breaks compaction when a partition
	// mixes BIGINT and TIMESTAMP files (DuckDB cannot cast BIGINT -> TIMESTAMP
	// during multi-file reads). Normalizing here keeps every agent file
	// schema-compatible with the rest of the storage.
	pf, err := file.NewParquetReader(bytes.NewReader(body))
	if err != nil {
		return c.Status(fiber.StatusUnprocessableEntity).JSON(fiber.Map{
			"error": "failed to read parquet file: " + err.Error(),
		})
	}
	defer pf.Close()

	arrowReader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, nil)
	if err != nil {
		return c.Status(fiber.StatusUnprocessableEntity).JSON(fiber.Map{
			"error": "failed to open parquet reader: " + err.Error(),
		})
	}

	ctx := c.Context()
	tbl, err := arrowReader.ReadTable(ctx)
	if err != nil {
		return c.Status(fiber.StatusUnprocessableEntity).JSON(fiber.Map{
			"error": "failed to read parquet table: " + err.Error(),
		})
	}
	defer tbl.Release()

	schema := tbl.Schema()
	header := make([]string, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		header[i] = schema.Field(i).Name
	}

	timeFieldIdx, herr := validateImportHeader(header, "time")
	if herr != nil {
		return c.Status(herr.StatusCode).JSON(fiber.Map{"error": herr.Message})
	}

	numRows := int(tbl.NumRows())
	if numRows == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "file contains no rows",
		})
	}

	// Reuse the import pipeline's typed extraction: the time column is
	// normalized to int64 microseconds (auto-detects s/ms/us/ns magnitude),
	// and every other column is converted to the typed slices ArrowWriter
	// expects.
	cols := make(map[string]interface{}, len(header))
	validity := make(map[string][]bool, len(header))
	var timeMicros []int64
	for i, name := range header {
		if i == timeFieldIdx {
			tm, terr := parquetColumnToTimeMicros(tbl.Column(i), "")
			if terr != nil {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
					"error": fmt.Sprintf("failed to parse time column %q: %v", name, terr),
				})
			}
			timeMicros = tm
			continue
		}
		vals, valid, conv := arrowColumnToTyped(tbl.Column(i))
		if conv != nil {
			return c.Status(fiber.StatusUnprocessableEntity).JSON(fiber.Map{
				"error": fmt.Sprintf("unsupported parquet column %q: %v", name, conv),
			})
		}
		cols[name] = vals
		if valid != nil {
			validity[name] = valid
		}
	}
	cols["time"] = timeMicros

	batch := &ingest.TypedColumnBatch{Data: cols, Validity: validity}
	if err := h.arrowBuffer.WriteTypedColumnarDirect(ctx, db, measurement, batch, numRows); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to ingest parquet data: " + err.Error(),
		})
	}
	if err := h.arrowBuffer.FlushAll(ctx); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to flush ingested data: " + err.Error(),
		})
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"status": "ok",
		"rows":   numRows,
	})
}

// handleListAgents returns every registered agent with liveness and table
// metadata, plus online/offline tallies.
func (h *AgentHandler) handleListAgents(c *fiber.Ctx) error {
	statuses := h.registry.List()
	items := make([]fiber.Map, 0, len(statuses))

	total, online := len(statuses), 0
	for _, s := range statuses {
		if s.Online {
			online++
		}
		items = append(items, agentStatusJSON(s))
	}

	return c.JSON(fiber.Map{
		"agents":  items,
		"total":   total,
		"online":  online,
		"offline": total - online,
	})
}

// handleListTableAgents returns the table-to-agent mapping for the monitoring
// view: "db.table" -> sorted online agent IDs.
func (h *AgentHandler) handleListTableAgents(c *fiber.Ctx) error {
	mapping := h.registry.ListTableAgents()

	// Keys sorted for a stable response; the inner slices are already sorted by
	// the registry.
	keys := make([]string, 0, len(mapping))
	for k := range mapping {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	tables := make(fiber.Map, len(keys))
	for _, k := range keys {
		tables[k] = mapping[k]
	}
	return c.JSON(fiber.Map{"tables": tables})
}

// agentStatusJSON renders one agent for the monitoring endpoints. Status is a
// string ("online"|"offline") so liveness is expressed consistently across
// monitoring views.
func agentStatusJSON(s agent.AgentStatus) fiber.Map {
	tables := make([]fiber.Map, 0, len(s.Tables))
	for _, t := range s.Tables {
		tables = append(tables, fiber.Map{
			"db":        t.DB,
			"table":     t.Table,
			"min_time":  t.MinTime,
			"max_time":  t.MaxTime,
			"row_count": t.RowCount,
		})
	}

	status := "online"
	if !s.Online {
		status = "offline"
	}

	return fiber.Map{
		"id":               s.ID,
		"url":              s.URL,
		"status":           status,
		"last_heartbeat":   heartbeatTimestamp(s.LastHeartbeat),
		"heartbeat_age_ms": heartbeatAgeMS(s.LastHeartbeat),
		"tables":           tables,
	}
}

// heartbeatTimestamp returns t as a JSON-encodable pointer, or nil when the
// timestamp is zero (an agent that has never heartbeated).
func heartbeatTimestamp(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	v := t
	return &v
}

// heartbeatAgeMS returns the age of a heartbeat in milliseconds, clamped at
// zero so a clock-skewed future timestamp cannot render as a negative age.
func heartbeatAgeMS(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	age := time.Since(t).Milliseconds()
	if age < 0 {
		return 0
	}
	return age
}
