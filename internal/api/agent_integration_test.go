package api

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"iedb/internal/agent"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAgentRegisterAndHeartbeat(t *testing.T) {
	registry := agent.NewAgentRegistry(5 * time.Second)
	defer registry.Stop()

	handler := NewAgentHandler(registry, nil) // nil storage for this test
	app := fiber.New()
	handler.RegisterRoutes(app)

	// Test register
	req := httptest.NewRequest("POST", "/api/v1/agents/register",
		strings.NewReader(`{"id":"test-agent","url":"http://localhost:8080"}`))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, 201, resp.StatusCode)

	// Test heartbeat
	req2 := httptest.NewRequest("POST", "/api/v1/agents/heartbeat",
		strings.NewReader(`{
		"id": "test-agent",
		"tables_changed": [
			{"db":"test","table":"cpu","min_time":100,"max_time":200,"row_count":100}
		]
	}`))
	req2.Header.Set("Content-Type", "application/json")

	resp2, err := app.Test(req2)
	require.NoError(t, err)
	assert.Equal(t, 200, resp2.StatusCode)

	// Verify registry
	agents := registry.GetAgentsForTable("test", "cpu")
	assert.Len(t, agents, 1)
	assert.Equal(t, "test-agent", agents[0].ID)
}

func TestAgentTimeout(t *testing.T) {
	registry := agent.NewAgentRegistry(100 * time.Millisecond)
	defer registry.Stop()

	registry.Register("test-agent", "http://localhost:8080")
	registry.Heartbeat("test-agent", []agent.TableMeta{
		{DB: "test", Table: "cpu", MinTime: 100, MaxTime: 200, RowCount: 100},
	})

	// Agent should be online
	agents := registry.GetAgentsForTable("test", "cpu")
	assert.Len(t, agents, 1)

	// Wait for timeout
	time.Sleep(200 * time.Millisecond)

	// Trigger cleanup now (the background loop runs every 10s, too slow for tests).
	registry.ForceCleanup()

	// Agent should be offline now
	agents = registry.GetAgentsForTable("test", "cpu")
	assert.Len(t, agents, 0)
}
