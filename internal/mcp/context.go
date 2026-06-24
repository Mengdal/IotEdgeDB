package mcp

import (
	"context"
	"fmt"
	"os"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// LoadDatabaseContextArgs is the input schema for load_database_context (no args needed).
type LoadDatabaseContextArgs struct{}

// GetHelpArgs is the input schema for get_help (no args needed).
type GetHelpArgs struct{}

// RegisterLoadDatabaseContext registers the load_database_context tool.
func RegisterLoadDatabaseContext(server *mcp.Server, client *Client) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        "load_database_context",
		Description: "Load custom database context documentation. Returns markdown documentation about the database schema and usage.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ LoadDatabaseContextArgs) (out *mcp.CallToolResult, _ any, _ error) {
		defer RecoverToolPanic(&out)
		content, err := readContextFile()
		if err != nil {
			return errorResult(fmt.Sprintf("Failed to load context: %v", err)), nil, nil
		}
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: content}},
		}, nil, nil
	})
}

// RegisterGetHelp registers the get_help tool.
func RegisterGetHelp(server *mcp.Server, client *Client) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_help",
		Description: "Get help and troubleshooting guidance for using iedb.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ GetHelpArgs) (out *mcp.CallToolResult, _ any, _ error) {
		defer RecoverToolPanic(&out)
		content, err := readHelpFile()
		if err != nil {
			return errorResult(fmt.Sprintf("Failed to load help: %v", err)), nil, nil
		}
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: content}},
		}, nil, nil
	})
}

// readContextFile reads the database context documentation.
func readContextFile() (string, error) {
	// Try multiple possible paths
	paths := []string{
		"docs/database-context.md",
		"database-context.md",
	}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err == nil {
			return string(data), nil
		}
	}
	return "", fmt.Errorf("context file not found")
}

// readHelpFile reads the help documentation.
func readHelpFile() (string, error) {
	// Try multiple possible paths
	paths := []string{
		"docs/database-help.md",
		"database-help.md",
	}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err == nil {
			return string(data), nil
		}
	}
	return "", fmt.Errorf("help file not found")
}
