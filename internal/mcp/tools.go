// Package tools implements the MCP tool handlers exposed by iedb-mcp.
package mcp

import (
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Config holds configuration for the MCP tools.
type Config struct {
	// BaseURL is the iedb instance URL (e.g., http://localhost:8000)
	BaseURL string
	// Token is the API authentication token
	Token string
	// MaxRows is the maximum number of rows per query (default: 500)
	MaxRows int
	// MaxResponseChars is the maximum response size in characters (default: 50000)
	MaxResponseChars int
	// Timeout is the HTTP client timeout (default: 30s)
	Timeout time.Duration
}

// DefaultConfig returns a Config with default values.
func DefaultConfig() Config {
	return Config{
		BaseURL:          "http://localhost:8000",
		MaxRows:          500,
		MaxResponseChars: 50000,
		Timeout:          30 * time.Second,
	}
}

// RegisterAll registers all iedb tools with the MCP server.
func RegisterAll(server *mcp.Server, cfg Config) {
	// Create client
	client := NewClient(cfg.BaseURL, cfg.Token, cfg.Timeout)

	// Register all tools
	RegisterListDatabases(server, client)
	RegisterListMeasurements(server, client)
	RegisterDescribeMeasurement(server, client, cfg.MaxResponseChars)
	RegisterQuery(server, client, cfg.MaxRows, cfg.MaxResponseChars)
	RegisterGetSampleData(server, client, cfg.MaxResponseChars)
	RegisterWriteLineProtocol(server, client)
	RegisterLoadDatabaseContext(server, client)
	RegisterGetHelp(server, client)
}
