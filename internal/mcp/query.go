package mcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// QueryArgs is the input for the query tool.
type QueryArgs struct {
	Database string `json:"database" jsonschema:"Database name"`
	SQL      string `json:"sql"      jsonschema:"Read-only SQL query (DuckDB dialect) IEDB supports standard analytical SQL including aggregations, JOINs, CTEs, window functions, and time helpers like time_bucket() and date_trunc()."`
}

// GetSampleDataArgs is the input for get_sample_data.
type GetSampleDataArgs struct {
	Database    string `json:"database"    jsonschema:"Database name"`
	Measurement string `json:"measurement" jsonschema:"Measurement (table) name"`
	Limit       int    `json:"limit"       jsonschema:"Number of rows to return (default 10 and max 100)"`
}

// RegisterQuery registers the query tool.
func RegisterQuery(server *mcp.Server, client *Client, maxRows int, maxResponseChars int) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        "query",
		Description: "Execute a read-only SQL query against iedb (DuckDB SQL dialect). Supports SELECT, aggregations, JOINs, CTEs, window functions, and more. Write operations (INSERT, UPDATE, DELETE, DROP) are blocked.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args QueryArgs) (out *mcp.CallToolResult, _ any, _ error) {
		defer RecoverToolPanic(&out)
		if args.Database == "" {
			return errorResult("database name is required"), nil, nil
		}
		if err := ValidateIdentifier(args.Database); err != nil {
			return errorResult("invalid database name"), nil, nil
		}
		if args.SQL == "" {
			return errorResult("SQL query is required"), nil, nil
		}

		// Safety: reject write operations. Advisory only — iedb server-side is authoritative.
		if err := ValidateReadOnly(args.SQL); err != nil {
			return errorResult(fmt.Sprintf("Blocked: %v", err)), nil, nil
		}

		// Enforce row limit
		sql := EnforceRowLimit(args.SQL, maxRows)

		result, err := client.Query(ctx, args.Database, sql)
		if err != nil {
			return errorResult(UserMessage(err)), nil, nil
		}

		text := formatQueryResult(result)
		if maxResponseChars > 0 {
			text = TruncateResponse(text, maxResponseChars)
		}

		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: text}},
		}, nil, nil
	})
}

// RegisterGetSampleData registers the get_sample_data tool.
func RegisterGetSampleData(server *mcp.Server, client *Client, maxResponseChars int) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_sample_data",
		Description: "Get recent sample rows from a measurement, ordered by time descending. Useful for understanding the data shape and recent values.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args GetSampleDataArgs) (out *mcp.CallToolResult, _ any, _ error) {
		defer RecoverToolPanic(&out)
		if args.Database == "" || args.Measurement == "" {
			return errorResult("both database and measurement are required"), nil, nil
		}
		if err := ValidateIdentifier(args.Database); err != nil {
			return errorResult("invalid database name"), nil, nil
		}
		if err := ValidateIdentifier(args.Measurement); err != nil {
			return errorResult("invalid measurement name"), nil, nil
		}

		limit := args.Limit
		if limit <= 0 {
			limit = 10
		}
		if limit > 100 {
			limit = 100
		}

		// Safe to interpolate: ValidateIdentifier ensured args.Measurement is [A-Za-z_][A-Za-z0-9_]*.
		sql := fmt.Sprintf("SELECT * FROM %s ORDER BY time DESC LIMIT %d", args.Measurement, limit)
		result, err := client.Query(ctx, args.Database, sql)
		if err != nil {
			return errorResult(UserMessage(err)), nil, nil
		}

		text := formatQueryResult(result)
		if maxResponseChars > 0 {
			text = TruncateResponse(text, maxResponseChars)
		}

		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: text}},
		}, nil, nil
	})
}

// formatQueryResult formats a QueryResponse as a readable markdown table.
func formatQueryResult(result *QueryResponse) string {
	if len(result.Columns) == 0 {
		return fmt.Sprintf("Query returned no columns. (%d rows, %.1fms)", result.RowCount, result.ExecutionTimeMs)
	}

	var sb strings.Builder

	// Header
	sb.WriteString("| ")
	for i, col := range result.Columns {
		if i > 0 {
			sb.WriteString(" | ")
		}
		sb.WriteString(escapeMarkdownCell(col))
	}
	sb.WriteString(" |\n")

	// Separator
	sb.WriteString("|")
	for range result.Columns {
		sb.WriteString("---|")
	}
	sb.WriteString("\n")

	// Rows
	for _, row := range result.Data {
		sb.WriteString("| ")
		for i, val := range row {
			if i > 0 {
				sb.WriteString(" | ")
			}
			sb.WriteString(escapeMarkdownCell(val))
		}
		sb.WriteString(" |\n")
	}

	fmt.Fprintf(&sb, "\n*%d rows returned in %.1fms*", result.RowCount, result.ExecutionTimeMs)

	return sb.String()
}
