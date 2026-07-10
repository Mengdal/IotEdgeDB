package api

import (
	"iedb/internal/agent"

	"github.com/gofiber/fiber/v2"
)

// AgentHandler handles agent registration and heartbeat.
type AgentHandler struct {
	registry *agent.AgentRegistry
}

// NewAgentHandler creates an AgentHandler.
func NewAgentHandler(registry *agent.AgentRegistry) *AgentHandler {
	return &AgentHandler{registry: registry}
}

// RegisterRoutes registers agent API routes.
func (h *AgentHandler) RegisterRoutes(app *fiber.App) {
	app.Post("/api/v1/agents/register", h.handleRegister)
	app.Post("/api/v1/agents/heartbeat", h.handleHeartbeat)
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
