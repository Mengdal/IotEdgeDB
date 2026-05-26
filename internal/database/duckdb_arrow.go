//go:build duckdb_arrow

package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	duckdb "github.com/duckdb/duckdb-go/v2"
)

func init() {
	ArrowEnabled = true
}

// arrowQueryOnConn executes a query via the Arrow API on a raw driver connection.
func arrowQueryOnConn(ctx context.Context, driverConn any, query string) (array.RecordReader, error) {
	dc, ok := driverConn.(driver.Conn)
	if !ok {
		return nil, fmt.Errorf("connection does not implement driver.Conn")
	}
	arrowAPI, err := duckdb.NewArrowFromConn(dc)
	if err != nil {
		return nil, fmt.Errorf("failed to create Arrow interface: %w", err)
	}
	return arrowAPI.QueryContext(ctx, query)
}

// ArrowQueryContext executes a query using DuckDB's native Arrow API,
// returning an array.RecordReader that yields Arrow record batches directly
// from DuckDB's internal columnar chunks — no row-by-row scanning.
//
// The caller MUST close both resources when done:
//  1. reader.Release() — releases Arrow batches and closes underlying rows
//  2. conn.Close() — returns the connection to the pool
func (d *DuckDB) ArrowQueryContext(ctx context.Context, query string) (array.RecordReader, *sql.Conn, error) {
	conn, err := d.db.Conn(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to acquire connection: %w", err)
	}

	var reader array.RecordReader
	err = conn.Raw(func(driverConn any) error {
		var rawErr error
		reader, rawErr = arrowQueryOnConn(ctx, driverConn, query)
		return rawErr
	})

	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("arrow query failed: %w", err)
	}

	d.logger.Debug().
		Str("query", query).
		Msg("Arrow query executed")

	return reader, conn, nil
}

// ArrowQueryOnConn executes a query via the Arrow API on an already-acquired connection.
// The caller must pin the connection (acquire via db.Conn()) and is responsible for
// calling conn.Close() when done. This allows TEMP VIEWs to be registered on the same
// connection before the query.
func (d *DuckDB) ArrowQueryOnConn(ctx context.Context, conn *sql.Conn, query string) (array.RecordReader, error) {
	var reader array.RecordReader
	err := conn.Raw(func(driverConn any) error {
		var rawErr error
		reader, rawErr = arrowQueryOnConn(ctx, driverConn, query)
		return rawErr
	})

	if err != nil {
		return nil, fmt.Errorf("arrow query failed: %w", err)
	}

	d.logger.Debug().
		Str("query", query).
		Msg("Arrow query executed on pinned connection")

	return reader, nil
}

// ArrowQueryWithProfileContext executes a query with DuckDB profiling enabled,
// returning Arrow record batches and a QueryProfile with timing breakdown.
//
// The caller MUST close both resources when done:
//  1. reader.Release() — releases Arrow batches and closes underlying rows
//  2. conn.Close() — returns the connection to the pool
func (d *DuckDB) ArrowQueryWithProfileContext(ctx context.Context, query string) (array.RecordReader, *sql.Conn, *QueryProfile, error) {
	conn, err := d.db.Conn(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to acquire connection: %w", err)
	}

	// Create temp file for profiling output
	tmpFile, err := os.CreateTemp("", "duckdb_profile_*.json")
	if err != nil {
		// Fall back to non-profile Arrow query
		var reader array.RecordReader
		rawErr := conn.Raw(func(driverConn any) error {
			var err error
			reader, err = arrowQueryOnConn(ctx, driverConn, query)
			return err
		})
		if rawErr != nil {
			conn.Close()
			return nil, nil, nil, rawErr
		}
		return reader, conn, nil, nil
	}
	profilePath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(profilePath)

	// Enable profiling on this specific connection
	if _, err := conn.ExecContext(ctx, "PRAGMA enable_profiling='json'"); err != nil {
		d.logger.Warn().Err(err).Msg("Failed to enable profiling")
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA profiling_output='%s'", profilePath)); err != nil {
		d.logger.Warn().Err(err).Msg("Failed to set profiling output")
	}
	if _, err := conn.ExecContext(ctx, "SET custom_profiling_settings='{\"PLANNER\": \"true\", \"PLANNER_BINDING\": \"true\", \"PHYSICAL_PLANNER\": \"true\", \"OPERATOR_TIMING\": \"true\", \"OPERATOR_CARDINALITY\": \"true\"}'"); err != nil {
		d.logger.Warn().Err(err).Msg("Failed to set custom profiling settings")
	}

	// Execute query via Arrow API
	start := time.Now()
	var reader array.RecordReader
	rawErr := conn.Raw(func(driverConn any) error {
		var err error
		reader, err = arrowQueryOnConn(ctx, driverConn, query)
		return err
	})
	totalTime := time.Since(start)

	// Disable profiling
	conn.ExecContext(ctx, "PRAGMA disable_profiling")

	if rawErr != nil {
		conn.Close()
		return nil, nil, nil, fmt.Errorf("arrow query failed: %w", rawErr)
	}

	// Read profile data
	profile := &QueryProfile{
		TotalMs: float64(totalTime.Milliseconds()),
	}
	if profileData, err := os.ReadFile(profilePath); err == nil && len(profileData) > 0 {
		var profileJSON map[string]interface{}
		if err := json.Unmarshal(profileData, &profileJSON); err == nil {
			if timing, ok := profileJSON["timing"].(float64); ok {
				profile.Latency = timing * 1000
			}
			if children, ok := profileJSON["children"].([]interface{}); ok {
				for _, child := range children {
					if childMap, ok := child.(map[string]interface{}); ok {
						if name, ok := childMap["name"].(string); ok && name == "PLANNER" {
							if timing, ok := childMap["timing"].(float64); ok {
								profile.PlannerMs = timing * 1000
							}
						}
					}
				}
			}
			profile.ExecutionMs = profile.TotalMs - profile.PlannerMs
		}
	}

	return reader, conn, profile, nil
}

// RegisterArrowView registers Arrow arrays as a DuckDB TEMP VIEW on a specific connection.
// The view is connection-scoped — the caller must use this same connection for queries that
// reference the view. The connection should be pinned (acquired via db.Conn()) and not returned
// to the pool until the query completes.
//
// Returns a release function that must be called after the query is done to unregister the view
// and release its memory. If an error occurs, returns (nil, error).
func RegisterArrowView(ctx context.Context, conn *sql.Conn, viewName string, schema *arrow.Schema, arrays []arrow.Array) (release func(), err error) {
	// Determine number of rows from the first array
	numRows := int64(0)
	if len(arrays) > 0 && arrays[0] != nil {
		numRows = int64(arrays[0].Len())
	}
	if numRows == 0 {
		return func() {}, nil
	}

	// Build a single record batch and wrap in a RecordReader
	record := array.NewRecord(schema, arrays, numRows)
	reader := newSingleRecordReader(record)

	err = conn.Raw(func(driverConn any) error {
		dc, ok := driverConn.(driver.Conn)
		if !ok {
			return fmt.Errorf("connection does not implement driver.Conn")
		}
		a, err := duckdb.NewArrowFromConn(dc)
		if err != nil {
			return fmt.Errorf("failed to create Arrow interface: %w", err)
		}
		rel, err := a.RegisterView(reader, viewName)
		if err != nil {
			return fmt.Errorf("failed to register view %s: %w", viewName, err)
		}
		release = rel
		return nil
	})

	if err != nil {
		record.Release()
		return nil, err
	}

	return release, nil
}

// singleRecordReader wraps a single arrow.RecordBatch as a RecordReader.
// It yields the record exactly once on the first Next() call.
type singleRecordReader struct {
	record arrow.RecordBatch
	done   bool
}

func newSingleRecordReader(record arrow.RecordBatch) *singleRecordReader {
	return &singleRecordReader{record: record}
}

func (r *singleRecordReader) Retain()               { r.record.Retain() }
func (r *singleRecordReader) Release()              { r.record.Release() }
func (r *singleRecordReader) Schema() *arrow.Schema  { return r.record.Schema() }
func (r *singleRecordReader) Next() bool              { if !r.done { r.done = true; return true }; return false }
func (r *singleRecordReader) Record() arrow.RecordBatch      { return r.record }
func (r *singleRecordReader) RecordBatch() arrow.RecordBatch { return r.record }
func (r *singleRecordReader) Err() error              { return nil }
