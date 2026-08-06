package api

import (
	"fmt"
	"iedb/internal/agent"
	"iedb/internal/storage"
	"time"

	"github.com/gofiber/fiber/v2"
)

// AgentHandler handles agent registration and heartbeat.
type AgentHandler struct {
	registry *agent.AgentRegistry
	storage  storage.Backend
}

// NewAgentHandler creates an AgentHandler.
func NewAgentHandler(registry *agent.AgentRegistry, store storage.Backend) *AgentHandler {
	return &AgentHandler{registry: registry, storage: store}
}

// RegisterRoutes registers agent API routes.
func (h *AgentHandler) RegisterRoutes(app *fiber.App) {
	app.Post("/api/v1/agents/register", h.handleRegister)
	app.Post("/api/v1/agents/heartbeat", h.handleHeartbeat)
	app.Post("/api/v1/ingest/parquet", h.handleIngestParquet)
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
	ID            string            `json:"id"`
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

	h.registry.Heartbeat(req.ID, req.TablesChanged)
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

	body := c.Body()
	if len(body) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "empty body",
		})
	}

	// Build storage path
	now := time.Now().UTC()
	path := fmt.Sprintf("%s/%s/%s/%s/%s/%s/%s_%s_%09d.parquet",
		db, measurement,
		now.Format("2006"), now.Format("01"), now.Format("02"), now.Format("15"),
		measurement,
		now.Format("20060102_150405"),
		now.UnixNano()%1_000_000_000,
	)

	ctx := c.Context()
	if err := h.storage.Write(ctx, path, body); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fmt.Sprintf("storage write failed: %v", err),
		})
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"status": "ok",
		"path":   path,
		"bytes":  len(body),
	})
}
