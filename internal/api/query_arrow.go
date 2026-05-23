//go:build duckdb_arrow

package api

import (
	"bufio"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/gofiber/fiber/v2"

	"iedb/internal/database"
	"iedb/internal/metrics"
)

func init() {
	bufferViewInjectFunc = injectBufferViewsImpl
}

// arrowBatchSize is the number of rows per Arrow record batch.
// Smaller batches reduce peak memory usage and enable streaming.
// 10K rows is a good balance between overhead and memory efficiency.
const arrowBatchSize = 10000

// executeQueryArrow handles POST/GET /api/v1/query/arrow - returns Arrow IPC stream
// Optimized to stream rows directly into Arrow batches without intermediate buffering.
func (h *QueryHandler) executeQueryArrow(c *fiber.Ctx) error {
	start := time.Now()
	m := metrics.Get()

	// Parse request body
	var req QueryRequest

	// Support both GET (query param) and POST (JSON body)
	if c.Method() == fiber.MethodGet {
		// Try 'sql' or 'q' query parameters
		req.SQL = c.Query("sql")
		if req.SQL == "" {
			req.SQL = c.Query("q")
		}
	} else {
		// Parse request body for POST
		if err := c.BodyParser(&req); err != nil {
			m.IncQueryErrors()
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"success": false,
				"error":   "Invalid request body: " + err.Error(),
			})
		}
	}

	// Validate SQL (empty, max length, dangerous patterns)
	if err := ValidateSQLRequest(req.SQL); err != nil {
		m.IncQueryErrors()
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

	// Extract x-iedb-database header for optimized query path
	headerDB := c.Get("x-iedb-database")
	// If header is set, reject cross-database syntax (db.table not allowed)
	if headerDB != "" && hasCrossDatabaseSyntax(req.SQL) {
		m.IncQueryErrors()
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Cross-database queries (db.table syntax) not allowed when x-iedb-database header is set",
		})
	}

	// Check RBAC permissions for all tables referenced in the query
	if err := h.checkQueryPermissions(c, req.SQL, "read"); err != nil {
		m.IncQueryErrors()
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

	// Convert SQL to storage paths (with caching)
	// If headerDB is set, uses optimized path that skips db.table regex patterns
	convertedSQL, _ := h.getTransformedSQL(req.SQL, headerDB)

	h.logger.Debug().
		Str("original_sql", req.SQL).
		Str("converted_sql", convertedSQL).
		Str("header_db", headerDB).
		Msg("Executing Arrow query")

	// Create context with timeout if configured
	// Use context.Background() instead of c.UserContext() because SetBodyStreamWriter
	// runs asynchronously after the handler returns, and c.UserContext() would be cancelled
	// Note: We don't use defer cancel() here because the streaming callback runs after
	// this handler returns - cancel is called inside the callback after rows are consumed
	ctx := context.Background()
	var cancel context.CancelFunc
	if h.queryTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, h.queryTimeout)
	}

	// Execute query using DuckDB's native Arrow API — returns record batches
	// directly from DuckDB's internal columnar chunks, no row-by-row scanning.
	reader, conn, err := h.db.ArrowQueryContext(ctx, convertedSQL)
	if err != nil {
		if cancel != nil {
			cancel()
		}
		if h.queryTimeout > 0 && ctx.Err() == context.DeadlineExceeded {
			m.IncQueryTimeouts()
			h.logger.Error().Err(err).Str("sql", req.SQL).Dur("timeout", h.queryTimeout).Msg("Arrow query timed out")
			return c.Status(fiber.StatusGatewayTimeout).JSON(fiber.Map{
				"success": false,
				"error":   "Query timed out",
			})
		}
		m.IncQueryErrors()
		h.logger.Error().Err(err).Str("sql", req.SQL).Msg("Arrow query execution failed")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

	schema := reader.Schema()

	c.Set("Content-Type", "application/vnd.apache.arrow.stream")

	c.Context().SetBodyStreamWriter(func(w *bufio.Writer) {
		ipcWriter := ipc.NewWriter(w, ipc.WithSchema(schema))

		var totalRows int64
		for reader.Next() {
			batch := reader.Record()
			if batch == nil {
				break
			}
			totalRows += batch.NumRows()

			if err := ipcWriter.Write(batch); err != nil {
				h.logger.Error().Err(err).Msg("Failed to write Arrow batch")
				break
			}
			w.Flush()
		}

		if err := reader.Err(); err != nil {
			h.logger.Error().Err(err).Msg("Error iterating Arrow batches")
		}

		if err := ipcWriter.Close(); err != nil {
			h.logger.Error().Err(err).Msg("Failed to close Arrow IPC writer")
		}
		reader.Release()
		conn.Close()
		if cancel != nil {
			cancel()
		}

		h.logger.Info().
			Int64("row_count", totalRows).
			Float64("execution_time_ms", float64(time.Since(start).Milliseconds())).
			Msg("Arrow streaming query completed")
	})

	return nil
}

// sqlTypeToArrowType converts SQL type names to Arrow types
func sqlTypeToArrowType(sqlType string) arrow.DataType {
	sqlType = strings.ToUpper(sqlType)
	switch {
	case strings.Contains(sqlType, "INT64"), strings.Contains(sqlType, "BIGINT"):
		return arrow.PrimitiveTypes.Int64
	case strings.Contains(sqlType, "INT32"), strings.Contains(sqlType, "INTEGER"), strings.Contains(sqlType, "INT"):
		return arrow.PrimitiveTypes.Int64 // Use Int64 for safety
	case strings.Contains(sqlType, "FLOAT"), strings.Contains(sqlType, "DOUBLE"), strings.Contains(sqlType, "REAL"):
		return arrow.PrimitiveTypes.Float64
	case strings.Contains(sqlType, "BOOL"):
		return arrow.FixedWidthTypes.Boolean
	case strings.Contains(sqlType, "TIMESTAMP"), strings.Contains(sqlType, "DATETIME"):
		return arrow.FixedWidthTypes.Timestamp_us
	case strings.Contains(sqlType, "DATE"):
		return arrow.FixedWidthTypes.Date32
	default:
		return arrow.BinaryTypes.String
	}
}

// appendValueToBuilder appends a value to the appropriate Arrow builder
func appendValueToBuilder(builder array.Builder, val interface{}, _ arrow.DataType) {
	if val == nil {
		builder.AppendNull()
		return
	}

	switch b := builder.(type) {
	case *array.Int64Builder:
		switch v := val.(type) {
		case int64:
			b.Append(v)
		case int32:
			b.Append(int64(v))
		case int:
			b.Append(int64(v))
		case float64:
			b.Append(int64(v))
		default:
			b.AppendNull()
		}
	case *array.Float64Builder:
		switch v := val.(type) {
		case float64:
			b.Append(v)
		case float32:
			b.Append(float64(v))
		case int64:
			b.Append(float64(v))
		case int:
			b.Append(float64(v))
		default:
			b.AppendNull()
		}
	case *array.StringBuilder:
		switch v := val.(type) {
		case string:
			b.Append(v)
		case []byte:
			b.Append(string(v))
		case time.Time:
			b.Append(v.Format(time.RFC3339Nano))
		default:
			b.Append(fmt.Sprintf("%v", v))
		}
	case *array.BooleanBuilder:
		switch v := val.(type) {
		case bool:
			b.Append(v)
		default:
			b.AppendNull()
		}
	case *array.TimestampBuilder:
		switch v := val.(type) {
		case time.Time:
			b.Append(arrow.Timestamp(v.UnixMicro()))
		case string:
			if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
				b.Append(arrow.Timestamp(t.UTC().UnixMicro()))
			} else {
				b.AppendNull()
			}
		default:
			b.AppendNull()
		}
	case *array.Date32Builder:
		switch v := val.(type) {
		case time.Time:
			b.Append(arrow.Date32FromTime(v))
		default:
			b.AppendNull()
		}
	default:
		builder.AppendNull()
	}
}

// registerArrowRoutes registers Arrow-specific query endpoints
func (h *QueryHandler) registerArrowRoutes(app *fiber.App) {
	app.Post("/api/v1/query/arrow", h.executeQueryArrow)
	app.Get("/api/v1/query/arrow", h.executeQueryArrow)
}

// injectBufferViewsImpl creates DuckDB TEMP VIEWs for buffered data matching the given tables.
// This is set as bufferViewInjectFunc in init(). It returns a map of original table name to
// created view name. If buffer data is not found or view creation fails, the table is
// silently skipped — queries fall back to Parquet-only.
//
// LIMITATION: TEMP VIEWs are connection-scoped in DuckDB. See RegisterArrowView for details.
// The current implementation registers the view but it will not be visible to the query
// connection unless connection affinity is added. For now, this provides the infrastructure
// that will be enabled when the DuckDB connection scoping issue is resolved.
func injectBufferViewsImpl(h *QueryHandler, tableNames []string) map[string]string {
	viewNames := make(map[string]string)
	if h.arrowBuffer == nil {
		return viewNames
	}

	for _, tableName := range tableNames {
		batches, count, ok := h.arrowBuffer.GetEntry(tableName)
		if !ok || count == 0 {
			continue
		}

		arrays, schema, err := h.arrowBuffer.BatchesToArrow(batches)
		if err != nil {
			h.logger.Warn().Err(err).Str("table", tableName).
				Msg("Failed to convert buffer to Arrow, skipping injection")
			continue
		}

		viewName := "_buf_" + strings.ReplaceAll(tableName, ".", "_")

		// Register Arrow data as DuckDB view
		if err := database.RegisterArrowView(context.Background(), h.db.DB(), viewName, schema, arrays); err != nil {
			h.logger.Warn().Err(err).Str("table", tableName).
				Str("view", viewName).
				Msg("Failed to register buffer view, skipping injection")
			_ = schema
			for _, a := range arrays {
				if a != nil {
					a.Release()
				}
			}
			continue
		}

		viewNames[tableName] = viewName
		h.logger.Info().
			Str("table", tableName).
			Str("view", viewName).
			Int("buffered_records", count).
			Msg("Buffer view registered for query injection")

		// Schema and arrays are now owned by DuckDB — no release needed.
	}

	return viewNames
}

