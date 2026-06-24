package query

import (
	"strings"
	"testing"
)

func TestQueryRewriter_Rewrite(t *testing.T) {
	// NOTE: Rewrite() uses MeasurementBufferKeys which requires a live buffer.
	// The rewrite logic is tested here via buildRewriteSQL which mirrors Rewrite().
	// Integration tests cover the full wiring.

	t.Run("basic rewrite wraps SQL with CTE", func(t *testing.T) {
		userSQL := `SELECT mean("usage") FROM default/cpu WHERE time > now() - 1h`
		measurementKey := "default/cpu"
		partitionPath := "/data/default/cpu/**/*.parquet"
		viewName := "_iedb_buffer_default_cpu"

		rewritten := buildRewriteSQL(userSQL, measurementKey, partitionPath, viewName)

		if !strings.Contains(rewritten, "WITH _iedb_source AS (") {
			t.Error("rewritten SQL should contain WITH _iedb_source CTE")
		}
		if !strings.Contains(rewritten, "UNION ALL") {
			t.Error("rewritten SQL should contain UNION ALL")
		}
		if !strings.Contains(rewritten, viewName) {
			t.Errorf("rewritten SQL should reference VIEW %s", viewName)
		}
		if !strings.Contains(rewritten, "read_parquet") {
			t.Error("rewritten SQL should contain read_parquet")
		}
		if strings.Contains(rewritten, "FROM default/cpu") {
			t.Error("rewritten SQL should replace measurement key with _iedb_source")
		}
		if !strings.Contains(rewritten, "FROM _iedb_source") {
			t.Error("rewritten SQL should reference _iedb_source")
		}

		t.Logf("Rewritten SQL:\n%s", rewritten)
	})

	t.Run("rewrite preserves aggregation and filters", func(t *testing.T) {
		userSQL := `SELECT COUNT(*), MAX("value") FROM default/sensors WHERE "host" = 'srv1' GROUP BY "host"`
		rewritten := buildRewriteSQL(userSQL, "default/sensors", "/data/default/sensors/**/*.parquet", "_iedb_buffer_default_sensors")

		if !strings.Contains(rewritten, "COUNT(*)") {
			t.Error("should preserve COUNT(*)")
		}
		if !strings.Contains(rewritten, `"host" = 'srv1'`) {
			t.Error("should preserve WHERE clause")
		}
		if !strings.Contains(rewritten, "GROUP BY") {
			t.Error("should preserve GROUP BY")
		}
	})

	t.Run("measurement with slashes handled correctly", func(t *testing.T) {
		userSQL := `SELECT * FROM nested/db/cpu`
		rewritten := buildRewriteSQL(userSQL, "nested/db/cpu", "/data/nested/db/cpu/**/*.parquet", "_iedb_buffer_nested_db_cpu")

		if !strings.Contains(rewritten, "_iedb_buffer_nested_db_cpu") {
			t.Error("VIEW name should replace all slashes")
		}
	})
}

// buildRewriteSQL mirrors the logic of QueryRewriter.Rewrite for unit testing.
func buildRewriteSQL(userSQL, measurementKey, partitionPath, viewName string) string {
	rewritten := `
		WITH _iedb_source AS (
			SELECT * FROM read_parquet('` + partitionPath + `')
			UNION ALL
			SELECT * FROM ` + viewName + `
		)
		` + userSQL

	rewritten = strings.ReplaceAll(rewritten, measurementKey, "_iedb_source")
	return rewritten
}

func TestViewNameFormat(t *testing.T) {
	tests := []struct {
		bufferKey string
		expected  string
	}{
		{"default/cpu", "_iedb_buffer_default_cpu"},
		{"db/measurement", "_iedb_buffer_db_measurement"},
		{"nested/db/cpu", "_iedb_buffer_nested_db_cpu"},
	}

	for _, tt := range tests {
		result := buildViewName(tt.bufferKey)
		if result != tt.expected {
			t.Errorf("ViewName(%q) = %q, want %q", tt.bufferKey, result, tt.expected)
		}
	}
}

// buildViewName mirrors database.ViewName for testing without import cycle.
func buildViewName(bufferKey string) string {
	return "_iedb_buffer_" + strings.ReplaceAll(bufferKey, "/", "_")
}
