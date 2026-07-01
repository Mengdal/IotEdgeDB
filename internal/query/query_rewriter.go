package query

import (
	"strings"

	"iedb/internal/database"
	"iedb/internal/ingest"
)

// QueryRewriter 将用户 SQL 改写为包含缓冲数据的 UNION 查询。
type QueryRewriter struct {
	buffer *ingest.ArrowBuffer
}

// NewQueryRewriter 创建查询改写器。
func NewQueryRewriter(buffer *ingest.ArrowBuffer) *QueryRewriter {
	return &QueryRewriter{buffer: buffer}
}

// Rewrite 将用户查询改写为同时查询 Parquet 文件和所有 schema 变体缓冲临时表的查询。
func (r *QueryRewriter) Rewrite(userSQL, measurementKey, partitionPath string) string {
	// Extract database and measurement from the key
	parts := strings.SplitN(measurementKey, "/", 2)
	if len(parts) != 2 {
		return userSQL
	}
	keys := r.buffer.MeasurementBufferKeys(parts[0], parts[1])
	if len(keys) == 0 {
		return userSQL
	}

	// Build UNION ALL over all schema VIEWs
	var buf strings.Builder
	buf.WriteString("WITH _iedb_source AS (\n")
	buf.WriteString("\tSELECT * FROM read_parquet('")
	buf.WriteString(partitionPath)
	buf.WriteString("')\n")
	for _, k := range keys {
		buf.WriteString("\tUNION ALL BY NAME\n")
		buf.WriteString("\tSELECT * FROM ")
		buf.WriteString(database.QuoteIdent(database.ViewName(k)))
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
	keys := r.buffer.MeasurementBufferKeys(parts[0], parts[1])
	return len(keys) > 0
}
