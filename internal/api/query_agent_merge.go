//go:build duckdb_arrow

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"iedb/internal/agent"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
)

// QueryRow represents a single row from an agent query response.
type QueryRow struct {
	Time   int64                  `json:"time"`
	Tags   map[string]string      `json:"tags"`
	Fields map[string]interface{} `json:"fields"`
}

// AgentQueryResult holds rows fetched from one agent.
type AgentQueryResult struct {
	AgentID string
	Rows    []QueryRow
	Error   error
}

var agentHTTPClient = &http.Client{Timeout: 2 * time.Second}

// fetchAgentData queries all relevant agents in parallel for a table's in-memory data.
func fetchAgentData(
	ctx context.Context,
	logger zerolog.Logger,
	registry *agent.AgentRegistry,
	db, table string,
	startNs, endNs *int64,
) []AgentQueryResult {
	agents := registry.GetAgentsForTable(db, table)
	if len(agents) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	results := make([]AgentQueryResult, len(agents))

	for i, ag := range agents {
		wg.Add(1)
		go func(idx int, ag *agent.AgentInfo) {
			defer wg.Done()

			url := fmt.Sprintf("%s/query?db=%s&table=%s", ag.URL, db, table)
			if startNs != nil {
				url += fmt.Sprintf("&start=%d", *startNs)
			}
			if endNs != nil {
				url += fmt.Sprintf("&end=%d", *endNs)
			}

			req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
			if err != nil {
				results[idx] = AgentQueryResult{AgentID: ag.ID, Error: err}
				return
			}

			resp, err := agentHTTPClient.Do(req)
			if err != nil {
				logger.Warn().Err(err).Str("agent", ag.ID).Msg("Agent query failed")
				results[idx] = AgentQueryResult{AgentID: ag.ID, Error: err}
				return
			}
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				results[idx] = AgentQueryResult{AgentID: ag.ID, Error: err}
				return
			}

			var payload struct {
				Rows []QueryRow `json:"rows"`
			}
			if err := json.Unmarshal(body, &payload); err != nil {
				results[idx] = AgentQueryResult{AgentID: ag.ID, Error: err}
				return
			}

			results[idx] = AgentQueryResult{AgentID: ag.ID, Rows: payload.Rows}
		}(i, ag)
	}

	wg.Wait()
	return results
}

// rowsToArrow converts agent query rows to an Arrow RecordBatch.
func rowsToArrow(rows []QueryRow) (arrow.Record, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	pool := memory.NewGoAllocator()

	// Collect all unique field names across all rows
	fieldNames := make(map[string]arrow.DataType)
	for _, r := range rows {
		for k, v := range r.Fields {
			if _, ok := fieldNames[k]; !ok {
				fieldNames[k] = inferArrowType(v)
			}
		}
	}

	// Also collect tag keys
	tagKeys := make(map[string]bool)
	for _, r := range rows {
		for k := range r.Tags {
			tagKeys[k] = true
		}
	}

	// Sort keys for deterministic column ordering
	tagKeyList := make([]string, 0, len(tagKeys))
	for k := range tagKeys {
		tagKeyList = append(tagKeyList, k)
	}
	sort.Strings(tagKeyList)

	fieldNameList := make([]string, 0, len(fieldNames))
	for name := range fieldNames {
		fieldNameList = append(fieldNameList, name)
	}
	sort.Strings(fieldNameList)

	// Build schema: time column first, then tag columns (sorted), then field columns (sorted)
	fields := []arrow.Field{{Name: "time", Type: arrow.PrimitiveTypes.Int64, Nullable: false}}
	for _, k := range tagKeyList {
		fields = append(fields, arrow.Field{Name: k, Type: arrow.BinaryTypes.String, Nullable: true})
	}
	for _, name := range fieldNameList {
		fields = append(fields, arrow.Field{Name: name, Type: fieldNames[name], Nullable: true})
	}

	schema := arrow.NewSchema(fields, nil)

	// Build column builders matching schema order
	builders := make([]array.Builder, 0, len(fields))
	builders = append(builders, array.NewInt64Builder(pool))
	for range tagKeyList {
		builders = append(builders, array.NewStringBuilder(pool))
	}
	for _, name := range fieldNameList {
		switch fieldNames[name] {
		case arrow.PrimitiveTypes.Float64:
			builders = append(builders, array.NewFloat64Builder(pool))
		case arrow.PrimitiveTypes.Int64:
			builders = append(builders, array.NewInt64Builder(pool))
		default:
			builders = append(builders, array.NewStringBuilder(pool))
		}
	}

	// Populate rows
	for _, r := range rows {
		colIdx := 0
		builders[colIdx].(*array.Int64Builder).Append(r.Time)
		colIdx++

		for _, k := range tagKeyList {
			v := r.Tags[k]
			builders[colIdx].(*array.StringBuilder).Append(v)
			colIdx++
		}

		for _, name := range fieldNameList {
			v := r.Fields[name]
			switch b := builders[colIdx].(type) {
			case *array.Float64Builder:
				if f, ok := toFloat64(v); ok {
					b.Append(f)
				} else {
					b.AppendNull()
				}
			case *array.Int64Builder:
				if i, ok := toInt64(v); ok {
					b.Append(i)
				} else {
					b.AppendNull()
				}
			case *array.StringBuilder:
				if s, ok := v.(string); ok {
					b.Append(s)
				} else if v != nil {
					b.Append(fmt.Sprintf("%v", v))
				} else {
					b.AppendNull()
				}
			}
			colIdx++
		}
	}

	// Build arrays from builders
	arrs := make([]arrow.Array, len(builders))
	for i, b := range builders {
		arrs[i] = b.NewArray()
		b.Release()
	}

	rec := array.NewRecord(schema, arrs, int64(len(rows)))
	return rec, nil
}

func inferArrowType(v interface{}) arrow.DataType {
	switch v.(type) {
	case float64, float32:
		return arrow.PrimitiveTypes.Float64
	case int, int32, int64:
		return arrow.PrimitiveTypes.Int64
	default:
		return arrow.BinaryTypes.String
	}
}

func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case json.Number:
		f, err := val.Float64()
		return f, err == nil
	}
	return 0, false
}

func toInt64(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int64:
		return val, true
	case int:
		return int64(val), true
	case float64:
		return int64(val), true
	case json.Number:
		i, err := val.Int64()
		return i, err == nil
	}
	return 0, false
}

// buildAgentViewsSQL returns a UNION ALL SQL fragment for agent data,
// or empty string if no agents returned data.
func buildAgentViewsSQL(results []AgentQueryResult) string {
	var parts []string
	for _, r := range results {
		if r.Error != nil || len(r.Rows) == 0 {
			continue
		}
		// DuckDB can read from registered Arrow views
		viewName := fmt.Sprintf("_agent_%s", strings.ReplaceAll(r.AgentID, "-", "_"))
		parts = append(parts, fmt.Sprintf("SELECT * FROM %s", viewName))
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, " UNION ALL ")
}
