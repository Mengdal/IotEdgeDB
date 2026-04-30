package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/gofiber/fiber/v2"
)

type mcpRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type mcpResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *mcpError       `json:"error,omitempty"`
}

type mcpError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type mcpToolDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

type mcpToolListResponse struct {
	Tools []mcpToolDefinition `json:"tools"`
}

type mcpCallToolParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

type mcpCallToolResult struct {
	Content []mcpContent `json:"content"`
}

type mcpContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type mcpResourceDefinition struct {
	Name        string `json:"name"`
	URI         string `json:"uri"`
	Description string `json:"description"`
	MimeType    string `json:"mimeType"`
}

type mcpResourceListResponse struct {
	Resources []mcpResourceDefinition `json:"resources"`
}

type mcpReadResourceParams struct {
	URI string `json:"uri"`
}

type mcpReadResourceResult struct {
	Contents []mcpResourceContent `json:"contents"`
}

type mcpResourceContent struct {
	URI      string `json:"uri"`
	MimeType string `json:"mimeType"`
	Text     string `json:"text"`
}

type mcpPromptDefinition struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Arguments   []mcpPromptArg `json:"arguments,omitempty"`
}

type mcpPromptArg struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Required    bool   `json:"required"`
}

type mcpPromptListResponse struct {
	Prompts []mcpPromptDefinition `json:"prompts"`
}

type mcpGetPromptParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

type mcpGetPromptResult struct {
	Messages []mcpPromptMessage `json:"messages"`
}

type mcpPromptMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type mcpHandler struct {
	client *http.Client
}

// NewMCPHandler creates a new MCP HTTP handler.
func NewMCPHandler() *mcpHandler {
	return &mcpHandler{client: &http.Client{Timeout: 30 * time.Second}}
}

func (h *mcpHandler) RegisterRoutes(app *fiber.App) {
	app.Post("/api/v1/mcp", h.handle)
}

func (h *mcpHandler) handle(c *fiber.Ctx) error {
	var req mcpRequest
	if err := c.BodyParser(&req); err != nil {
		return c.JSON(mcpResponse{
			JSONRPC: "2.0",
			ID:      json.RawMessage("null"),
			Error:   &mcpError{Code: -32700, Message: "parse error"},
		})
	}

	resp := h.dispatch(c, req)
	return c.JSON(resp)
}

func (h *mcpHandler) dispatch(c *fiber.Ctx, req mcpRequest) mcpResponse {
	switch req.Method {
	case "initialize":
		return mcpResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]any{
				"protocolVersion": "26.3.19",
				"capabilities": map[string]any{
					"tools":     map[string]any{},
					"resources": map[string]any{},
					"prompts":   map[string]any{},
				},
				"serverInfo": map[string]any{
					"name":    "iedb-mcp",
					"version": "1.0.0",
				},
			},
		}
	case "tools/list":
		return mcpResponse{JSONRPC: "2.0", ID: req.ID, Result: mcpToolListResponse{Tools: h.listTools()}}
	case "tools/call":
		var params mcpCallToolParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return mcpResponse{JSONRPC: "2.0", ID: req.ID, Error: &mcpError{Code: -32602, Message: "invalid params"}}
		}
		result, err := h.callTool(c, params.Name, params.Arguments)
		if err != nil {
			return mcpResponse{JSONRPC: "2.0", ID: req.ID, Error: &mcpError{Code: -32000, Message: err.Error()}}
		}
		return mcpResponse{JSONRPC: "2.0", ID: req.ID, Result: result}
	case "resources/list":
		return mcpResponse{JSONRPC: "2.0", ID: req.ID, Result: mcpResourceListResponse{Resources: h.listResources()}}
	case "resources/read":
		var params mcpReadResourceParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return mcpResponse{JSONRPC: "2.0", ID: req.ID, Error: &mcpError{Code: -32602, Message: "invalid params"}}
		}
		result, err := h.readResource(c, params.URI)
		if err != nil {
			return mcpResponse{JSONRPC: "2.0", ID: req.ID, Error: &mcpError{Code: -32000, Message: err.Error()}}
		}
		return mcpResponse{JSONRPC: "2.0", ID: req.ID, Result: result}
	case "prompts/list":
		return mcpResponse{JSONRPC: "2.0", ID: req.ID, Result: mcpPromptListResponse{Prompts: h.listPrompts()}}
	case "prompts/get":
		var params mcpGetPromptParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return mcpResponse{JSONRPC: "2.0", ID: req.ID, Error: &mcpError{Code: -32602, Message: "invalid params"}}
		}
		result, err := h.getPrompt(params.Name)
		if err != nil {
			return mcpResponse{JSONRPC: "2.0", ID: req.ID, Error: &mcpError{Code: -32000, Message: err.Error()}}
		}
		return mcpResponse{JSONRPC: "2.0", ID: req.ID, Result: result}
	case "ping":
		return mcpResponse{JSONRPC: "2.0", ID: req.ID, Result: map[string]any{}}
	default:
		return mcpResponse{JSONRPC: "2.0", ID: req.ID, Error: &mcpError{Code: -32601, Message: "method not found"}}
	}
}

func (h *mcpHandler) listTools() []mcpToolDefinition {
	return []mcpToolDefinition{
		{
			Name:        "list_databases",
			Description: "List all databases",
			InputSchema: json.RawMessage(`{"type":"object","properties":{},"required":[]}`),
		},
		{
			Name:        "list_measurements",
			Description: "List measurements in a database",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"database":{"type":"string"}},"required":["database"]}`),
		},
		{
			Name:        "get_measurement_schema",
			Description: "Get measurement columns by sampling one row",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"database":{"type":"string"},"measurement":{"type":"string"}},"required":["measurement"]}`),
		},
		{
			Name:        "execute_query",
			Description: "Execute a SQL query",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"database":{"type":"string"},"sql":{"type":"string"}},"required":["sql"]}`),
		},
		{
			Name:        "load_database_context",
			Description: "Load custom database context documentation",
			InputSchema: json.RawMessage(`{"type":"object","properties":{},"required":[]}`),
		},
		{
			Name:        "get_help",
			Description: "Get help and troubleshooting guidance",
			InputSchema: json.RawMessage(`{"type":"object","properties":{},"required":[]}`),
		},
		{
			Name:        "write_line_protocol",
			Description: "Write data using line protocol",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"database":{"type":"string"},"precision":{"type":"string"},"data":{"type":"string"}},"required":["data"]}`),
		},
		{
			Name:        "health_check",
			Description: "Check database health",
			InputSchema: json.RawMessage(`{"type":"object","properties":{},"required":[]}`),
		},
	}
}

func (h *mcpHandler) callTool(c *fiber.Ctx, name string, args json.RawMessage) (mcpCallToolResult, error) {
	switch name {
	case "list_databases":
		payload, err := h.forwardRequest(c, http.MethodGet, "/api/v1/databases", nil, "")
		if err != nil {
			return mcpCallToolResult{}, err
		}
		return mcpCallToolResult{Content: []mcpContent{{Type: "text", Text: string(payload)}}}, nil
	case "list_measurements":
		var input struct {
			Database string `json:"database"`
		}
		if err := json.Unmarshal(args, &input); err != nil {
			return mcpCallToolResult{}, fmt.Errorf("invalid arguments")
		}
		if input.Database == "" {
			return mcpCallToolResult{}, fmt.Errorf("database is required")
		}
		path := "/api/v1/databases/" + url.PathEscape(input.Database) + "/measurements"
		payload, err := h.forwardRequest(c, http.MethodGet, path, nil, "")
		if err != nil {
			return mcpCallToolResult{}, err
		}
		return mcpCallToolResult{Content: []mcpContent{{Type: "text", Text: string(payload)}}}, nil
	case "get_measurement_schema":
		var input struct {
			Database    string `json:"database"`
			Measurement string `json:"measurement"`
		}
		if err := json.Unmarshal(args, &input); err != nil {
			return mcpCallToolResult{}, fmt.Errorf("invalid arguments")
		}
		if input.Measurement == "" {
			return mcpCallToolResult{}, fmt.Errorf("measurement is required")
		}
		sql := fmt.Sprintf("SELECT * FROM %s LIMIT 1", input.Measurement)
		body, _ := json.Marshal(map[string]string{"sql": sql})
		payload, err := h.forwardRequest(c, http.MethodPost, "/api/v1/query", body, input.Database)
		if err != nil {
			return mcpCallToolResult{}, err
		}
		return mcpCallToolResult{Content: []mcpContent{{Type: "text", Text: string(payload)}}}, nil
	case "execute_query":
		var input struct {
			Database string `json:"database"`
			SQL      string `json:"sql"`
		}
		if err := json.Unmarshal(args, &input); err != nil {
			return mcpCallToolResult{}, fmt.Errorf("invalid arguments")
		}
		if input.SQL == "" {
			return mcpCallToolResult{}, fmt.Errorf("sql is required")
		}
		body, _ := json.Marshal(map[string]string{"sql": input.SQL})
		payload, err := h.forwardRequest(c, http.MethodPost, "/api/v1/query", body, input.Database)
		if err != nil {
			return mcpCallToolResult{}, err
		}
		return mcpCallToolResult{Content: []mcpContent{{Type: "text", Text: string(payload)}}}, nil
	case "load_database_context":
		content, err := h.readContextFile()
		if err != nil {
			return mcpCallToolResult{}, err
		}
		return mcpCallToolResult{Content: []mcpContent{{Type: "text", Text: content}}}, nil
	case "get_help":
		helpPayload, err := h.readHelpFileJSON()
		if err != nil {
			return mcpCallToolResult{}, err
		}
		return mcpCallToolResult{Content: []mcpContent{{Type: "text", Text: helpPayload}}}, nil
	case "write_line_protocol":
		var input struct {
			Database  string `json:"database"`
			Precision string `json:"precision"`
			Data      string `json:"data"`
		}
		if err := json.Unmarshal(args, &input); err != nil {
			return mcpCallToolResult{}, fmt.Errorf("invalid arguments")
		}
		if input.Data == "" {
			return mcpCallToolResult{}, fmt.Errorf("data is required")
		}
		query := ""
		if input.Database != "" {
			query = "?db=" + url.QueryEscape(input.Database)
		}
		if input.Precision != "" {
			sep := "?"
			if query != "" {
				sep = "&"
			}
			query += sep + "precision=" + url.QueryEscape(input.Precision)
		}
		payload, err := h.forwardRawRequest(c, http.MethodPost, "/write"+query, []byte(input.Data), input.Database, "text/plain")
		if err != nil {
			return mcpCallToolResult{}, err
		}
		return mcpCallToolResult{Content: []mcpContent{{Type: "text", Text: string(payload)}}}, nil
	case "health_check":
		payload, err := h.forwardRequest(c, http.MethodGet, "/health", nil, "")
		if err != nil {
			return mcpCallToolResult{}, err
		}
		return mcpCallToolResult{Content: []mcpContent{{Type: "text", Text: string(payload)}}}, nil
	default:
		return mcpCallToolResult{}, fmt.Errorf("unknown tool: %s", name)
	}
}

func (h *mcpHandler) listResources() []mcpResourceDefinition {
	return []mcpResourceDefinition{
		{
			Name:        "databases",
			URI:         "iedb://databases",
			Description: "List all databases",
			MimeType:    "application/json",
		},
		{
			Name:        "health",
			URI:         "iedb://health",
			Description: "Health check endpoint",
			MimeType:    "application/json",
		},
		{
			Name:        "context",
			URI:         "iedb://context",
			Description: "Custom database context documentation",
			MimeType:    "text/markdown",
		},
	}
}

func (h *mcpHandler) readResource(c *fiber.Ctx, uri string) (mcpReadResourceResult, error) {
	switch uri {
	case "iedb://databases":
		payload, err := h.forwardRequest(c, http.MethodGet, "/api/v1/databases", nil, "")
		if err != nil {
			return mcpReadResourceResult{}, err
		}
		return mcpReadResourceResult{Contents: []mcpResourceContent{{URI: uri, MimeType: "application/json", Text: string(payload)}}}, nil
	case "iedb://health":
		payload, err := h.forwardRequest(c, http.MethodGet, "/health", nil, "")
		if err != nil {
			return mcpReadResourceResult{}, err
		}
		return mcpReadResourceResult{Contents: []mcpResourceContent{{URI: uri, MimeType: "application/json", Text: string(payload)}}}, nil
	case "iedb://context":
		content, err := h.readContextFile()
		if err != nil {
			return mcpReadResourceResult{}, err
		}
		return mcpReadResourceResult{Contents: []mcpResourceContent{{URI: uri, MimeType: "text/markdown", Text: content}}}, nil
	default:
		return mcpReadResourceResult{}, fmt.Errorf("unknown resource: %s", uri)
	}
}

func (h *mcpHandler) listPrompts() []mcpPromptDefinition {
	return []mcpPromptDefinition{
		{
			Name:        "nl2sql",
			Description: "Prompt template for NL2SQL using iedb tools",
		},
		{
			Name:        "list-databases",
			Description: "Prompt to list all databases",
		},
		{
			Name:        "check-health",
			Description: "Prompt to check database health",
		},
		{
			Name:        "load-context",
			Description: "Prompt to load database context documentation",
		},
	}
}

func (h *mcpHandler) getPrompt(name string) (mcpGetPromptResult, error) {
	switch name {
	case "nl2sql":
		content := `You are an expert NL2SQL assistant for a Time-Series Database (using Apache Arrow/DataFusion engines).
	Your goal is to translate user natural language into accurate SQL.
	
	CRITICAL DATABASE CHARACTERISTICS (SPARSE WIDE TABLE):
	This database uses a sparse columnar layout. Multiple devices (dn) share the same table (e.g. 'mqtt').
	1. NULL FILTERING (MANDATORY): If querying specific fields (e.g. tag0003, temperature), ALWAYS append 'WHERE field_name IS NOT NULL' to prevent returning empty rows meant for other devices. 
	2. AGGREGATION: For math/aggregates (MAX, MIN, AVG) on sensor values, always SAFE CAST strings to FLOAT: e.g. 'MAX(CAST(field_name AS FLOAT))'.
	3. ROW WITH EXTREMES: To find the row containing a max/min value (e.g. "when did it reach max"), use 'ORDER BY CAST(field AS FLOAT) DESC LIMIT 1' with 'WHERE field IS NOT NULL'.
	
	WORKFLOW:
	1. If you don't know the table structures, call tool 'get_measurement_schema' ONCE to get the complete schema.
	2. Analyze the schema to find the correct table and column names. (Pay attention to TIMESTAMP data types).
	3. IMMEDIATELY output the final SQL query. DO NOT execute the query. DO NOT call any other tools.
	
	CONSTRAINTS:
	- Output ONLY the raw SQL string.
	- Do not wrap the SQL in markdown blocks.
	- Stop processing after outputting the SQL.`

		return mcpGetPromptResult{Messages: []mcpPromptMessage{{Role: "system", Content: content}}}, nil
	case "list-databases":
		content := "List all available databases by calling list_databases.\n" +
			"Return only the tool call result."
		return mcpGetPromptResult{Messages: []mcpPromptMessage{{Role: "system", Content: content}}}, nil
	case "check-health":
		content := "Check database health by calling health_check.\n" +
			"Return only the tool call result."
		return mcpGetPromptResult{Messages: []mcpPromptMessage{{Role: "system", Content: content}}}, nil
	case "load-context":
		content := "Load database context by calling load_database_context.\n" +
			"Return only the tool call result."
		return mcpGetPromptResult{Messages: []mcpPromptMessage{{Role: "system", Content: content}}}, nil
	default:
		return mcpGetPromptResult{}, fmt.Errorf("unknown prompt: %s", name)
	}
}

func (h *mcpHandler) forwardRequest(c *fiber.Ctx, method, path string, body []byte, database string) ([]byte, error) {
	return h.forwardRawRequest(c, method, path, body, database, "application/json")
}

func (h *mcpHandler) forwardRawRequest(c *fiber.Ctx, method, path string, body []byte, database, contentType string) ([]byte, error) {
	baseURL := c.BaseURL()
	fullURL := baseURL + path

	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(c.UserContext(), method, fullURL, reader)
	if err != nil {
		return nil, err
	}

	if auth := c.Get("Authorization"); auth != "" {
		req.Header.Set("Authorization", auth)
	}
	if apiKey := c.Get("x-api-key"); apiKey != "" {
		req.Header.Set("x-api-key", apiKey)
	}
	if database != "" {
		req.Header.Set("x-iedb-database", database)
	}
	if body != nil && contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("iedb api error: %s", string(payload))
	}

	return payload, nil
}

// TODO 动态编辑md文档 实现用户注入promt
func (h *mcpHandler) readContextFile() (string, error) {
	path := "cmd/tools/iedb_mcp_server/context/database-context.md"
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("failed to read context file: %w", err)
	}
	return string(data), nil
}

func (h *mcpHandler) readHelpFileJSON() (string, error) {
	path := "cmd/tools/iedb_mcp_server/help/database-help.md"
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("failed to read help file: %w", err)
	}
	return string(data), nil
}
