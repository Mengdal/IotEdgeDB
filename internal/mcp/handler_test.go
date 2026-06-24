package mcp

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupMCPApp() *fiber.App {
	app := fiber.New()
	cfg := Config{
		BaseURL:          "http://localhost:8000",
		Token:            "test-token",
		MaxRows:          100,
		MaxResponseChars: 10000,
	}
	handler := NewHandler(cfg)
	handler.RegisterRoutes(app)
	return app
}

type sseMessage struct {
	Event string
	Data  string
}

func parseSSE(body string) []sseMessage {
	var msgs []sseMessage
	scanner := bufio.NewScanner(strings.NewReader(body))
	var event, data string
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "event: ") {
			event = strings.TrimPrefix(line, "event: ")
		} else if strings.HasPrefix(line, "data: ") {
			data = strings.TrimPrefix(line, "data: ")
		} else if line == "" && data != "" {
			msgs = append(msgs, sseMessage{Event: event, Data: data})
			event, data = "", ""
		}
	}
	if data != "" {
		msgs = append(msgs, sseMessage{Event: event, Data: data})
	}
	return msgs
}

func mcpRequestWithSession(t *testing.T, app *fiber.App, sessionID string, id interface{}, method string, params map[string]interface{}) (map[string]interface{}, string) {
	t.Helper()
	body := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  method,
	}
	if id != nil {
		body["id"] = id
	}
	if params != nil {
		body["params"] = params
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/api/v1/mcp", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if sessionID != "" {
		req.Header.Set("Mcp-Session-Id", sessionID)
	}

	resp, err := app.Test(req)
	require.NoError(t, err)

	// Get session ID from response
	newSessionID := resp.Header.Get("Mcp-Session-Id")
	if newSessionID == "" {
		newSessionID = sessionID
	}

	// Notifications (no id) return 202 with empty body
	if id == nil {
		assert.Equal(t, 202, resp.StatusCode)
		return nil, newSessionID
	}

	assert.Equal(t, 200, resp.StatusCode)

	var buf bytes.Buffer
	buf.ReadFrom(resp.Body)

	msgs := parseSSE(buf.String())
	require.NotEmpty(t, msgs, "expected at least one SSE message")

	var result map[string]interface{}
	err = json.Unmarshal([]byte(msgs[0].Data), &result)
	require.NoError(t, err, "failed to parse SSE data as JSON: %s", msgs[0].Data)
	return result, newSessionID
}

func TestMCPHandler_Initialize(t *testing.T) {
	app := setupMCPApp()

	result, sessionID := mcpRequestWithSession(t, app, "", 1, "initialize", map[string]interface{}{
		"protocolVersion": "2025-03-26",
		"capabilities":    map[string]interface{}{},
		"clientInfo": map[string]interface{}{
			"name":    "test-client",
			"version": "1.0.0",
		},
	})

	t.Logf("Session ID: %s", sessionID)
	assert.Equal(t, "2.0", result["jsonrpc"])
	assert.NotNil(t, result["result"])

	res := result["result"].(map[string]interface{})
	assert.Equal(t, "2025-03-26", res["protocolVersion"])

	serverInfo := res["serverInfo"].(map[string]interface{})
	assert.Equal(t, "iedb-mcp", serverInfo["name"])
}

func TestMCPHandler_ToolsList(t *testing.T) {
	app := setupMCPApp()

	// Initialize
	_, sessionID := mcpRequestWithSession(t, app, "", 1, "initialize", map[string]interface{}{
		"protocolVersion": "2025-03-26",
		"capabilities":    map[string]interface{}{},
		"clientInfo": map[string]interface{}{
			"name":    "test-client",
			"version": "1.0.0",
		},
	})
	t.Logf("Session ID: %s", sessionID)

	// Send initialized notification
	mcpRequestWithSession(t, app, sessionID, nil, "notifications/initialized", nil)

	// List tools
	result, _ := mcpRequestWithSession(t, app, sessionID, 2, "tools/list", nil)

	res, ok := result["result"].(map[string]interface{})
	require.True(t, ok, "expected result to be a map, got: %+v", result["result"])

	toolList := res["tools"].([]interface{})
	assert.Greater(t, len(toolList), 0, "should have at least one tool registered")

	toolNames := make([]string, 0, len(toolList))
	for _, tool := range toolList {
		name := tool.(map[string]interface{})["name"].(string)
		toolNames = append(toolNames, name)
	}
	t.Logf("Registered tools: %v", toolNames)

	assert.Contains(t, toolNames, "list_databases")
	assert.Contains(t, toolNames, "list_measurements")
	assert.Contains(t, toolNames, "query")
	assert.Contains(t, toolNames, "describe_measurement")
	assert.Contains(t, toolNames, "get_sample_data")
}
