package mcp

import (
	"context"
	"fmt"
	"log"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// WriteLineProtocolArgs is the input for write_line_protocol.
type WriteLineProtocolArgs struct {
	Database  string `json:"database"  jsonschema:"Database name"`
	Precision string `json:"precision" jsonschema:"Precision (ns, u, ms, s, m, h). Default: ns"`
	Data      string `json:"data"      jsonschema:"Line protocol data to write. Example: 'measurement,tag1=value1 field1=1.0,field2=2.0 timestamp'"`
}

// RegisterWriteLineProtocol registers the write_line_protocol tool.
func RegisterWriteLineProtocol(server *mcp.Server, client *Client) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        "write_line_protocol",
		Description: "Write data using InfluxDB line protocol format. Example: 'measurement,tag1=value1 field1=1.0,field2=2.0 timestamp'",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args WriteLineProtocolArgs) (out *mcp.CallToolResult, _ any, _ error) {
		defer RecoverToolPanic(&out)
		if args.Data == "" {
			return errorResult("data is required"), nil, nil
		}
		if err := ValidateIdentifier(args.Database); err != nil {
			log.Printf("write_line_protocol: invalid database identifier %q: %v", args.Database, err)
			return errorResult("invalid database name"), nil, nil
		}

		if err := client.WriteLineProtocol(ctx, args.Database, args.Precision, args.Data); err != nil {
			return errorResult(UserMessage(err)), nil, nil
		}

		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Data written successfully to database '%s'.", args.Database)}},
		}, nil, nil
	})
}
