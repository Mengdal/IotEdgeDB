// Package tools implements the MCP tool handlers exposed by iedb-mcp.
package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Error is a classified error returned by the iedb client. Kind is a short
// machine-readable label; use it in tool handlers to produce user-facing
// messages without leaking internal detail (stack traces, paths, SQL context)
// to the LLM.
type Error struct {
	Kind   string // "auth", "not_found", "server", "network", "too_large", "parse", "query"
	Detail string // full detail — log to stderr, never send to LLM
}

func (e *Error) Error() string { return e.Kind + ": " + e.Detail }

// iedbErrorFrom classifies a non-2xx HTTP response into an Error and logs
// the full body snippet to stderr. Callers receive only the Kind.
func iedbErrorFrom(op string, statusCode int, snippet []byte) *Error {
	detail := fmt.Sprintf("%s: HTTP %d: %s", op, statusCode, strings.TrimSpace(string(snippet)))
	log.Printf("iedb error: %s", detail)
	kind := "server"
	switch statusCode {
	case 401, 403:
		kind = "auth"
	case 404:
		kind = "not_found"
	}
	return &Error{Kind: kind, Detail: detail}
}

// UserMessage returns a short, safe error message suitable for the LLM.
// It never leaks internal detail (stack traces, paths, iedb query context).
func UserMessage(err error) string {
	var ae *Error
	if ae, _ = err.(*Error); ae != nil { //nolint:errorlint
		switch ae.Kind {
		case "auth":
			return "iedb authentication failed — check the API token."
		case "not_found":
			return "The requested database or measurement was not found in iedb."
		case "too_large":
			return "iedb returned a response that was too large to process."
		case "parse":
			return "iedb returned an unexpected response format."
		case "query":
			return "iedb rejected the query — check the SQL syntax."
		default:
			return "iedb returned an error — see server logs for details."
		}
	}
	return fmt.Sprintf("iedb error: %v", err)
}

// maxIedbResponseBytes caps how much we read from iedb in a single response to
// protect the MCP process from OOM if iedb (or a MITM) returns a huge body.
const maxIedbResponseBytes = 64 << 20 // 64 MiB

// maxErrorBodyBytes caps how much of a non-2xx response body we include in
// error messages logged to stderr.
const maxErrorBodyBytes = 4 << 10 // 4 KiB

// DatabaseInfo matches iedb's API response for a database.
type DatabaseInfo struct {
	Name             string `json:"name"`
	MeasurementCount int    `json:"measurement_count"`
}

// DatabaseListResponse matches iedb's GET /api/v1/databases response.
type DatabaseListResponse struct {
	Databases []DatabaseInfo `json:"databases"`
	Count     int            `json:"count"`
}

// MeasurementInfo matches iedb's API response for a measurement.
type MeasurementInfo struct {
	Name string `json:"name"`
}

// MeasurementListResponse matches iedb's GET /api/v1/databases/:name/measurements response.
type MeasurementListResponse struct {
	Measurements []MeasurementInfo `json:"measurements"`
	Count        int               `json:"count"`
}

// QueryRequest is the body sent to POST /api/v1/query.
type QueryRequest struct {
	SQL string `json:"sql"`
}

// QueryResponse matches iedb's POST /api/v1/query response.
type QueryResponse struct {
	Success         bool            `json:"success"`
	Columns         []string        `json:"columns"`
	Data            [][]interface{} `json:"data"`
	RowCount        int             `json:"row_count"`
	ExecutionTimeMs float64         `json:"execution_time_ms"`
	Timestamp       string          `json:"timestamp"`
	Error           string          `json:"error,omitempty"`
}

// SchemaResponse matches iedb's GET /api/v1/databases/:db/measurements/:m/schema response.
type SchemaResponse struct {
	Success         bool              `json:"success"`
	Database        string            `json:"database"`
	Measurement     string            `json:"measurement"`
	Tags            []string          `json:"tags"`
	Fields          []string          `json:"fields"`
	Types           map[string]string `json:"types"`
	ExecutionTimeMs float64           `json:"execution_time_ms"`
}

// Client is an HTTP client for the iedb REST API.
type Client struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

// NewClient creates a new iedb API client.
func NewClient(baseURL, token string, timeout time.Duration) *Client {
	return &Client{
		baseURL: baseURL,
		token:   token,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

// Health checks if the iedb instance is reachable.
func (c *Client) Health(ctx context.Context) error {
	healthURL, err := c.buildURL("health")
	if err != nil {
		return fmt.Errorf("building health URL: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("connecting to iedb at %s: %w", c.baseURL, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("iedb health check failed: HTTP %d", resp.StatusCode)
	}
	return nil
}

// ListDatabases returns all databases from iedb.
func (c *Client) ListDatabases(ctx context.Context) (*DatabaseListResponse, error) {
	listURL, err := c.buildURL("api", "v1", "databases")
	if err != nil {
		return nil, fmt.Errorf("building list databases URL: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, listURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("listing databases: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body := io.LimitReader(resp.Body, maxIedbResponseBytes+1)

	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(body, maxErrorBodyBytes))
		return nil, iedbErrorFrom("list databases", resp.StatusCode, snippet)
	}

	var result DatabaseListResponse
	if err := json.NewDecoder(body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}
	return &result, nil
}

// ListMeasurements returns all measurements in a database.
func (c *Client) ListMeasurements(ctx context.Context, database string) (*MeasurementListResponse, error) {
	measurementsURL, err := c.buildURL("api", "v1", "databases", database, "measurements")
	if err != nil {
		return nil, fmt.Errorf("building measurements URL: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, measurementsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("listing measurements: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body := io.LimitReader(resp.Body, maxIedbResponseBytes+1)

	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(body, maxErrorBodyBytes))
		return nil, iedbErrorFrom("list measurements", resp.StatusCode, snippet)
	}

	var result MeasurementListResponse
	if err := json.NewDecoder(body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}
	return &result, nil
}

// GetSchema returns the schema (tags, fields, types) for a measurement.
func (c *Client) GetSchema(ctx context.Context, database, measurement string) (*SchemaResponse, error) {
	schemaURL, err := c.buildURL("api", "v1", "databases", database, "measurements", measurement, "schema")
	if err != nil {
		return nil, fmt.Errorf("building schema URL: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, schemaURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("getting schema: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body := io.LimitReader(resp.Body, maxIedbResponseBytes+1)

	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(body, maxErrorBodyBytes))
		return nil, iedbErrorFrom("get schema", resp.StatusCode, snippet)
	}

	var result SchemaResponse
	if err := json.NewDecoder(body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}
	return &result, nil
}

// buildURL safely composes the request URL by appending escaped path segments
// onto c.baseURL. This prevents path traversal or query/fragment injection via
// user-influenced segments (e.g., a malicious database name).
func (c *Client) buildURL(segments ...string) (string, error) {
	u, err := url.Parse(c.baseURL)
	if err != nil {
		return "", err
	}
	basePath := strings.TrimRight(u.Path, "/")
	var b strings.Builder
	b.WriteString(basePath)
	for _, s := range segments {
		b.WriteByte('/')
		b.WriteString(url.PathEscape(s))
	}
	u.RawPath = b.String()
	u.Path = basePath
	for _, s := range segments {
		u.Path += "/" + s
	}
	return u.String(), nil
}

// Query executes a SQL query against iedb.
func (c *Client) Query(ctx context.Context, database, sql string) (*QueryResponse, error) {
	body, err := json.Marshal(QueryRequest{SQL: sql})
	if err != nil {
		return nil, fmt.Errorf("marshaling request: %w", err)
	}

	queryURL, err := c.buildURL("api", "v1", "query")
	if err != nil {
		return nil, fmt.Errorf("building query URL: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, queryURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if database != "" {
		req.Header.Set("x-iedb-database", database)
	}
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing query: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	respBody := io.LimitReader(resp.Body, maxIedbResponseBytes+1)

	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(respBody, maxErrorBodyBytes))
		return nil, iedbErrorFrom("query", resp.StatusCode, snippet)
	}

	var result QueryResponse
	if err := json.NewDecoder(respBody).Decode(&result); err != nil {
		detail := fmt.Sprintf("query: decoding response: %v", err)
		log.Printf("iedb error: %s", detail)
		return nil, &Error{Kind: "parse", Detail: detail}
	}

	if !result.Success {
		detail := fmt.Sprintf("query: iedb error: %s", result.Error)
		log.Printf("iedb error: %s", detail)
		return nil, &Error{Kind: "query", Detail: detail}
	}
	return &result, nil
}

// WriteLineProtocol writes data to iedb using line protocol format.
func (c *Client) WriteLineProtocol(ctx context.Context, database, precision, data string) error {
	writeURL, err := c.buildURL("write")
	if err != nil {
		return fmt.Errorf("building write URL: %w", err)
	}
	if database != "" {
		writeURL += "?db=" + url.QueryEscape(database)
	}
	if precision != "" {
		sep := "?"
		if database != "" {
			sep = "&"
		}
		writeURL += sep + "precision=" + url.QueryEscape(precision)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, writeURL, strings.NewReader(data))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain")
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("writing data: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
		return iedbErrorFrom("write", resp.StatusCode, snippet)
	}
	return nil
}

func (c *Client) setAuth(req *http.Request) {
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
}
