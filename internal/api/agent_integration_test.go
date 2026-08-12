package api

import (
	"encoding/json"
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
	registry.Heartbeat("test-agent", "http://localhost:8080", []agent.TableMeta{
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

func TestAgentMonitoringEndpoints(t *testing.T) {
	registry := agent.NewAgentRegistry(5 * time.Second)
	defer registry.Stop()

	handler := NewAgentHandler(registry, nil) // nil storage: monitoring routes do not touch it
	app := fiber.New()
	handler.RegisterRoutes(app)

	// Register two agents.
	for _, c := range []struct{ id, url string }{
		{"agent-02", "http://localhost:8082"},
		{"agent-01", "http://localhost:8081"},
	} {
		req := httptest.NewRequest("POST", "/api/v1/agents/register",
			strings.NewReader(`{"id":"`+c.id+`","url":"`+c.url+`"}`))
		req.Header.Set("Content-Type", "application/json")
		resp, err := app.Test(req)
		require.NoError(t, err)
		require.Equal(t, 201, resp.StatusCode)
	}

	// Heartbeat agent-01 with table metadata.
	req := httptest.NewRequest("POST", "/api/v1/agents/heartbeat",
		strings.NewReader(`{"id":"agent-01","tables_changed":[
			{"db":"test","table":"cpu","min_time":100,"max_time":200,"row_count":100}
		]}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)

	// GET /api/v1/agents — full list, sorted, with liveness tallies.
	resp, err = app.Test(httptest.NewRequest("GET", "/api/v1/agents", nil))
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	var list struct {
		Agents []struct {
			ID             string `json:"id"`
			URL            string `json:"url"`
			Status         string `json:"status"`
			HeartbeatAgeMS int64  `json:"heartbeat_age_ms"`
			Tables         []struct {
				DB       string `json:"db"`
				Table    string `json:"table"`
				MinTime  int64  `json:"min_time"`
				MaxTime  int64  `json:"max_time"`
				RowCount int    `json:"row_count"`
			} `json:"tables"`
		} `json:"agents"`
		Total   int `json:"total"`
		Online  int `json:"online"`
		Offline int `json:"offline"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&list))
	assert.Equal(t, 2, list.Total)
	assert.Equal(t, 2, list.Online)
	assert.Equal(t, 0, list.Offline)
	require.Len(t, list.Agents, 2)
	// Sorted by ID for a stable response.
	assert.Equal(t, "agent-01", list.Agents[0].ID)
	assert.Equal(t, "agent-02", list.Agents[1].ID)
	assert.Equal(t, "online", list.Agents[0].Status)
	assert.GreaterOrEqual(t, list.Agents[0].HeartbeatAgeMS, int64(0))
	require.Len(t, list.Agents[0].Tables, 1)
	assert.Equal(t, "test", list.Agents[0].Tables[0].DB)
	assert.Equal(t, "cpu", list.Agents[0].Tables[0].Table)
	assert.Equal(t, int64(100), list.Agents[0].Tables[0].MinTime)
	assert.Equal(t, int64(200), list.Agents[0].Tables[0].MaxTime)
	assert.Equal(t, 100, list.Agents[0].Tables[0].RowCount)
	// The heartbeated agent is the only one with table metadata.
	assert.Empty(t, list.Agents[1].Tables)

	// GET /api/v1/agents/tables — table-to-agent mapping.
	resp, err = app.Test(httptest.NewRequest("GET", "/api/v1/agents/tables", nil))
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	var mapping struct {
		Tables map[string][]string `json:"tables"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&mapping))
	agentsForTable, ok := mapping.Tables["test.cpu"]
	require.True(t, ok)
	assert.Equal(t, []string{"agent-01"}, agentsForTable)
}

func TestAgentMonitoringStale(t *testing.T) {
	registry := agent.NewAgentRegistry(100 * time.Millisecond)
	defer registry.Stop()

	handler := NewAgentHandler(registry, nil)
	app := fiber.New()
	handler.RegisterRoutes(app)

	registry.Register("agent-01", "http://localhost:8081")

	// Wait past the heartbeat timeout, then force cleanup (the background loop
	// runs every 10s, far slower than the test).
	time.Sleep(200 * time.Millisecond)
	registry.ForceCleanup()

	// The agent's status must flip to offline in the list view.
	resp, err := app.Test(httptest.NewRequest("GET", "/api/v1/agents", nil))
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	var list struct {
		Total   int `json:"total"`
		Online  int `json:"online"`
		Offline int `json:"offline"`
		Agents  []struct {
			ID             string `json:"id"`
			Status         string `json:"status"`
			HeartbeatAgeMS int64  `json:"heartbeat_age_ms"`
		} `json:"agents"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&list))
	assert.Equal(t, 1, list.Total)
	assert.Equal(t, 0, list.Online)
	assert.Equal(t, 1, list.Offline)
	require.Len(t, list.Agents, 1)
	assert.Equal(t, "agent-01", list.Agents[0].ID)
	assert.Equal(t, "offline", list.Agents[0].Status)
	assert.GreaterOrEqual(t, list.Agents[0].HeartbeatAgeMS, int64(0))
}

// TestHeartbeatAutoRegistersViaAPI simulates a hub restart end to end: the
// agent never re-registers, only heartbeats (with its url), and must appear in
// the monitoring list again.
func TestHeartbeatAutoRegistersViaAPI(t *testing.T) {
	registry := agent.NewAgentRegistry(5 * time.Second)
	defer registry.Stop()

	handler := NewAgentHandler(registry, nil)
	app := fiber.New()
	handler.RegisterRoutes(app)

	// No register call — the hub restarted and forgot this agent.
	req := httptest.NewRequest("POST", "/api/v1/agents/heartbeat",
		strings.NewReader(`{"id":"edge-node-1","url":"http://192.168.0.8:8080","tables_changed":[
			{"db":"mydb","table":"cpu","min_time":100,"max_time":200,"row_count":50}
		]}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	resp, err = app.Test(httptest.NewRequest("GET", "/api/v1/agents", nil))
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	var list struct {
		Total  int `json:"total"`
		Agents []struct {
			ID     string `json:"id"`
			URL    string `json:"url"`
			Status string `json:"status"`
		} `json:"agents"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&list))
	require.Len(t, list.Agents, 1)
	assert.Equal(t, "edge-node-1", list.Agents[0].ID)
	assert.Equal(t, "http://192.168.0.8:8080", list.Agents[0].URL)
	assert.Equal(t, "online", list.Agents[0].Status)
}
