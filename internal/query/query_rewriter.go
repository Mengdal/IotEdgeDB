package query

import (
	"strings"

	"iedb/internal/database"
)

// QueryRewriter 将用户 SQL 改写为包含缓冲数据的 UNION 查询。
type QueryRewriter struct {
	viewMgr *database.ArrowViewManager
}

// NewQueryRewriter 创建查询改写器。
func NewQueryRewriter(viewMgr *database.ArrowViewManager) *QueryRewriter {
	return &QueryRewriter{viewMgr: viewMgr}
}

// Rewrite 将用户查询改写为同时查询 Parquet 文件和所有 schema 变体缓冲临时表的查询。
func (r *QueryRewriter) Rewrite(userSQL, measurementKey, partitionPath string) string {
	// Extract database and measurement from the key
	parts := strings.SplitN(measurementKey, "/", 2)
	if len(parts) != 2 {
		return userSQL
	}
	viewNames := r.viewMgr.MeasurementViewNames(parts[0], parts[1])
	if len(viewNames) == 0 {
		return userSQL
	}

	// Build UNION ALL over all schema VIEWs
	var buf strings.Builder
	buf.WriteString("WITH _iedb_source AS (\n")
	buf.WriteString("\tSELECT * FROM read_parquet('")
	buf.WriteString(partitionPath)
	buf.WriteString("')\n")
	for _, vn := range viewNames {
		buf.WriteString("\tUNION ALL\n")
		buf.WriteString("\tSELECT * FROM ")
		buf.WriteString(database.QuoteIdent(vn))
		buf.WriteByte('\n')
	}
	buf.WriteString(")\n")
	buf.WriteString(userSQL)

	return strings.ReplaceAll(buf.String(), measurementKey, "_iedb_source")
}

// HasBufferData 判断指定 measurement 是否有缓冲数据。
func (r *QueryRewriter) HasBufferData(measurementKey string) bool {
	parts := strings.SplitN(measurementKey, "/", 2)
	if len(parts) != 2 {
		return false
	}
	return r.viewMgr.HasMeasurementData(parts[0], parts[1])
}
