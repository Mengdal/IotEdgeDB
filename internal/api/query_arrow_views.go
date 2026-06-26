//go:build duckdb_arrow

package api

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"iedb/internal/database"
	"iedb/internal/ingest"

	"github.com/apache/arrow-go/v18/arrow/array"
	duckdb "github.com/duckdb/duckdb-go/v2"
)

func init() {
	registerViewsFunc = registerPendingViews
	queryWithViewsFunc = queryWithViews
}

// registerPendingViews registers buffer VIEWs (one per schema variant) on a
// raw DuckDB driver connection. VIEWs are connection-scoped, so this function
// must be called on the same connection that will execute the query.
//
// The views map values are *ingest.Entry snapshots; each is converted to an
// Arrow RecordBatch and registered as a DuckDB Arrow VIEW.
//
// Returns a combined release function that MUST be called after the query
// referencing these VIEWs has completed.
func registerPendingViews(driverConn any, views map[string]any) (func(), error) {
	dc, ok := driverConn.(driver.Conn)
	if !ok {
		return nil, fmt.Errorf("connection does not implement driver.Conn")
	}
	if len(views) == 0 {
		return func() {}, nil
	}

	var releases []func()
	for viewName, val := range views {
		entry, ok := val.(*ingest.Entry)
		if !ok || entry == nil {
			continue
		}
		schema := entry.GetArrowSchema()
		if schema == nil {
			// Schema not yet inferred (data is still in-memory, not flushed).
			// Build the Arrow schema on the fly from the column data types.
			schema = database.BuildArrowSchema(entry)
			if schema == nil {
				continue
			}
		}

		rec, err := database.BuildArrowRecordBatch(entry, schema)
		if err != nil {
			// Clean up previously registered views on error.
			for _, r := range releases {
				r()
			}
			return nil, fmt.Errorf("failed to build RecordBatch for view %q: %w", viewName, err)
		}

		release, err := database.RegisterBufferView(dc, viewName, rec)
		if err != nil {
			rec.Release()
			for _, r := range releases {
				r()
			}
			return nil, fmt.Errorf("failed to register view %q: %w", viewName, err)
		}
		releases = append(releases, release)
	}

	return func() {
		for _, r := range releases {
			r()
		}
	}, nil
}

// queryWithViews executes a SQL query via the database/sql path on a pinned
// connection with pending buffer VIEWs registered. This is the fallback for
// when the Arrow-native path is unavailable (e.g., driver issue).
//
// The caller MUST close rows AND conn when done. The viewRelease function
// MUST be called after the query results (rows) have been fully consumed.
func queryWithViews(h *QueryHandler, ctx context.Context, query string, views map[string]any) (rows *sql.Rows, conn *sql.Conn, viewRelease func(), err error) {
	conn, err = h.db.DB().Conn(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to acquire connection: %w", err)
	}

	err = conn.Raw(func(driverConn any) error {
		var regErr error
		viewRelease, regErr = registerViewsFunc(driverConn, views)
		return regErr
	})
	if err != nil {
		conn.Close()
		return nil, nil, nil, fmt.Errorf("failed to register buffer views: %w", err)
	}

	rows, err = conn.QueryContext(ctx, query)
	if err != nil {
		viewRelease()
		conn.Close()
		return nil, nil, nil, err
	}

	return rows, conn, viewRelease, nil
}

// registerViewsOnConnArrow registers pending buffer VIEWs on the given
// connection and then executes an Arrow query on the same connection.
// Returns the reader, connection, view release function, and any error.
func arrowQueryWithViews(ctx context.Context, conn *sql.Conn, query string, views map[string]any) (reader array.RecordReader, viewRelease func(), err error) {
	err = conn.Raw(func(driverConn any) error {
		dc, ok := driverConn.(driver.Conn)
		if !ok {
			return fmt.Errorf("connection does not implement driver.Conn")
		}

		// Register buffer VIEWs.
		var regErr error
		viewRelease, regErr = registerViewsFunc(driverConn, views)
		if regErr != nil {
			return regErr
		}

		// Execute Arrow query on the same connection.
		arrowAPI, aErr := duckdb.NewArrowFromConn(dc)
		if aErr != nil {
			return fmt.Errorf("failed to create Arrow interface: %w", aErr)
		}
		reader, aErr = arrowAPI.QueryContext(ctx, query)
		return aErr
	})
	if err != nil {
		return nil, nil, fmt.Errorf("arrow query with views failed: %w", err)
	}

	return reader, viewRelease, nil
}
// arrowQueryWithViewsProfiled executes an Arrow query with pending buffer VIEWs
// registered, with DuckDB profiling enabled. Returns the reader, view release
// function, profile, and error.
func arrowQueryWithViewsProfiled(ctx context.Context, conn *sql.Conn, query string, views map[string]any) (array.RecordReader, func(), *database.QueryProfile, error) {
	// Enable profiling PRAGMAs.
	tmpFile, err := os.CreateTemp("", "duckdb_profile_*.json")
	if err != nil {
		// Fall back to non-profiled query.
		reader, release, qErr := arrowQueryWithViews(ctx, conn, query, views)
		return reader, release, nil, qErr
	}
	profilePath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(profilePath)

	conn.ExecContext(ctx, "PRAGMA enable_profiling='json'")
	conn.ExecContext(ctx, `SET custom_profiling_settings='{"PLANNER": "true", "PLANNER_BINDING": "true", "PHYSICAL_PLANNER": "true", "OPERATOR_TIMING": "true", "OPERATOR_CARDINALITY": "true"}'`)
	conn.ExecContext(ctx, fmt.Sprintf("PRAGMA profiling_output='%s'", profilePath))

	start := time.Now()
	reader, viewRelease, err := arrowQueryWithViews(ctx, conn, query, views)
	totalTime := time.Since(start)

	conn.ExecContext(ctx, "PRAGMA disable_profiling")

	if err != nil {
		if viewRelease != nil {
			viewRelease()
		}
		return nil, nil, nil, err
	}

	// Parse profile.
	profile := &database.QueryProfile{
		TotalMs: float64(totalTime.Milliseconds()),
	}
	if profileData, readErr := os.ReadFile(profilePath); readErr == nil && len(profileData) > 0 {
		var profileJSON map[string]interface{}
		if jsonErr := json.Unmarshal(profileData, &profileJSON); jsonErr == nil {
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

	return reader, viewRelease, profile, nil
}

// viewReleaseConn wraps an Arrow query connection so that Close() also
// releases the registered buffer VIEWs' Arrow memory. The viewRelease
// is called at most once via sync.Once.
type viewReleaseConn struct {
	Conn         interface{ Close() error }
	once         sync.Once
	viewRelease  func()
}

func (c *viewReleaseConn) Close() error {
	c.once.Do(func() {
		if c.viewRelease != nil {
			c.viewRelease()
		}
	})
	return c.Conn.Close()
}
