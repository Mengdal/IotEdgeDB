package mcp

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ListMeasurementsArgs is the input for list_measurements.
type ListMeasurementsArgs struct {
	Database string `json:"database" jsonschema:"Database name"`
}

// DescribeMeasurementArgs is the input for describe_measurement.
type DescribeMeasurementArgs struct {
	Database    string `json:"database"    jsonschema:"Database name"`
	Measurement string `json:"measurement" jsonschema:"Measurement (table) name"`
}

// RegisterListMeasurements registers the list_measurements tool.
func RegisterListMeasurements(server *mcp.Server, client *Client) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_measurements",
		Description: "List all measurements (tables) in a database. Returns measurement names.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args ListMeasurementsArgs) (out *mcp.CallToolResult, _ any, _ error) {
		defer RecoverToolPanic(&out)
		if args.Database == "" {
			return errorResult("database name is required"), nil, nil
		}
		if err := ValidateIdentifier(args.Database); err != nil {
			log.Printf("list_measurements: invalid database identifier %q: %v", args.Database, err)
			return errorResult("invalid database name"), nil, nil
		}

		result, err := client.ListMeasurements(ctx, args.Database)
		if err != nil {
			return errorResult(UserMessage(err)), nil, nil
		}

		if len(result.Measurements) == 0 {
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("No measurements found in database '%s'.", args.Database)}},
			}, nil, nil
		}

		var sb strings.Builder
		fmt.Fprintf(&sb, "Database '%s' has %d measurement(s):\n\n", args.Database, result.Count)
		for _, m := range result.Measurements {
			fmt.Fprintf(&sb, "- %s\n", m.Name)
		}
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: sb.String()}},
		}, nil, nil
	})
}

// RegisterDescribeMeasurement registers the describe_measurement tool.
func RegisterDescribeMeasurement(server *mcp.Server, client *Client, maxResponseChars int) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        "describe_measurement",
		Description: "Describe a measurement's schema — column names, types, and whether each column is a tag or field. Use this before writing queries to understand the data structure.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args DescribeMeasurementArgs) (out *mcp.CallToolResult, _ any, _ error) {
		defer RecoverToolPanic(&out)
		if args.Database == "" || args.Measurement == "" {
			return errorResult("both database and measurement are required"), nil, nil
		}
		if err := ValidateIdentifier(args.Database); err != nil {
			log.Printf("describe_measurement: invalid database identifier %q: %v", args.Database, err)
			return errorResult("invalid database name"), nil, nil
		}
		if err := ValidateIdentifier(args.Measurement); err != nil {
			log.Printf("describe_measurement: invalid measurement identifier %q: %v", args.Measurement, err)
			return errorResult("invalid measurement name"), nil, nil
		}

		schema, err := client.GetSchema(ctx, args.Database, args.Measurement)
		if err != nil {
			return errorResult(UserMessage(err)), nil, nil
		}

		// Build tag set for quick lookup
		tagSet := make(map[string]bool, len(schema.Tags))
		for _, t := range schema.Tags {
			tagSet[t] = true
		}

		var sb strings.Builder
		fmt.Fprintf(&sb, "## Measurement: %s.%s\n\n", args.Database, args.Measurement)

		// Columns table
		sb.WriteString("### Columns\n\n")
		sb.WriteString("| Column | Type | IsTag |\n|--------|------|-------|\n")

		// time column is always present
		sb.WriteString("| time | TIMESTAMP | No |\n")

		// tags first, then fields
		for _, t := range schema.Tags {
			colType := schema.Types[t]
			if colType == "" {
				colType = "VARCHAR"
			}
			fmt.Fprintf(&sb, "| %s | %s | Yes |\n", escapeMarkdownCell(t), escapeMarkdownCell(colType))
		}
		for _, f := range schema.Fields {
			colType := schema.Types[f]
			fmt.Fprintf(&sb, "| %s | %s | No |\n", escapeMarkdownCell(f), escapeMarkdownCell(colType))
		}

		text := sb.String()
		if maxResponseChars > 0 {
			text = TruncateResponse(text, maxResponseChars)
		}

		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: text}},
		}, nil, nil
	})
}
