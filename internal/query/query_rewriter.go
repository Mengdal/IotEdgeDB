package query

import (
	"fmt"
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

// Rewrite 将用户查询改写为同时查询 Parquet 文件和缓冲临时表的查询。
func (r *QueryRewriter) Rewrite(userSQL, measurementKey, partitionPath string) string {
	viewName := database.ViewName(measurementKey)

	rewritten := fmt.Sprintf(`
		WITH _iedb_source AS (
			SELECT * FROM read_parquet('%s')
			UNION ALL
			SELECT * FROM %s
		)
		%s
	`, partitionPath, viewName, userSQL)

	rewritten = strings.ReplaceAll(rewritten, measurementKey, "_iedb_source")
	return rewritten
}

// HasBufferData 判断指定 measurement 是否有缓冲数据。
func (r *QueryRewriter) HasBufferData(measurementKey string) bool {
	return r.viewMgr.HasData(measurementKey)
}
