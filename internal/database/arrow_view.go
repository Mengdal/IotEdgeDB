package database

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"iedb/internal/ingest"

	"github.com/rs/zerolog"
)

// arrowViewState 追踪单个 measurement 的缓冲 VIEW 状态。
type arrowViewState struct {
	schema    string
	rowCount  int
	createdAt time.Time
}

// ArrowViewManager 管理缓冲数据到 DuckDB 临时表的注册和增量刷新。
// 实现 ingest.BufferChangeNotifier 接口。
type ArrowViewManager struct {
	db     *DuckDB
	buffer *ingest.ArrowBuffer

	mu    sync.Mutex
	views map[string]*arrowViewState

	notifyCh chan string
	dirty    map[string]struct{}
	closeCh  chan struct{}
	logger   zerolog.Logger
}

// NewArrowViewManager 创建 VIEW 管理器并启动后台刷新循环。
func NewArrowViewManager(db *DuckDB, buffer *ingest.ArrowBuffer, logger zerolog.Logger) *ArrowViewManager {
	m := &ArrowViewManager{
		db:       db,
		buffer:   buffer,
		views:    make(map[string]*arrowViewState),
		notifyCh: make(chan string, 256),
		dirty:    make(map[string]struct{}),
		closeCh:  make(chan struct{}),
		logger:   logger,
	}
	go m.refreshLoop()
	return m
}

// OnNewData 实现 ingest.BufferChangeNotifier。
func (m *ArrowViewManager) OnNewData(bufferKey string) {
	select {
	case m.notifyCh <- bufferKey:
	default:
	}
}

// OnFlushComplete 实现 ingest.BufferChangeNotifier。
func (m *ArrowViewManager) OnFlushComplete(bufferKey string) {
	m.mu.Lock()
	delete(m.views, bufferKey)
	m.mu.Unlock()
	m.OnNewData(bufferKey)
}

// HasData 判断指定 bufferKey 是否有活跃的缓冲 VIEW。
func (m *ArrowViewManager) HasData(bufferKey string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.views[bufferKey]
	return exists
}

// ViewName 将 bufferKey 转为 DuckDB 临时表名称。
func ViewName(bufferKey string) string {
	return "_iedb_buffer_" + strings.ReplaceAll(bufferKey, "/", "_")
}

// refreshLoop 以 100ms 合并窗口批量处理待刷新的 bufferKey。
func (m *ArrowViewManager) refreshLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-m.closeCh:
			return
		case key := <-m.notifyCh:
			m.mu.Lock()
			m.dirty[key] = struct{}{}
			m.mu.Unlock()
		case <-ticker.C:
			m.mu.Lock()
			if len(m.dirty) == 0 {
				m.mu.Unlock()
				continue
			}
			keys := make([]string, 0, len(m.dirty))
			for k := range m.dirty {
				keys = append(keys, k)
			}
			m.dirty = make(map[string]struct{})
			m.mu.Unlock()

			for _, key := range keys {
				m.refreshView(key)
			}
		}
	}
}

// refreshView 增量刷新单个 measurement 的临时表。
func (m *ArrowViewManager) refreshView(bufferKey string) {
	newBatches, err := m.buffer.SinceRefresh(bufferKey)
	if err != nil || len(newBatches) == 0 {
		return
	}

	// 检测 schema 变更
	m.mu.Lock()
	state, exists := m.views[bufferKey]
	m.mu.Unlock()

	schemaChanged := false
	if exists && len(newBatches) > 0 {
		schemaChanged = (newBatches[0].Signature != "" && newBatches[0].Signature != state.schema)
	}

	if !exists || schemaChanged {
		m.createOrReplaceTable(bufferKey, newBatches)
	} else {
		m.appendToTable(bufferKey, newBatches)
	}

	m.buffer.MarkRefreshed(bufferKey)
}

// createOrReplaceTable 创建或重建临时表。
func (m *ArrowViewManager) createOrReplaceTable(bufferKey string, batches []*ingest.TypedColumnBatch) {
	viewName := ViewName(bufferKey)
	if len(batches) == 0 {
		return
	}

	conn, err := m.db.DB().Conn(nil)
	if err != nil {
		m.logger.Error().Err(err).Str("table", viewName).Msg("Failed to get connection")
		return
	}
	defer conn.Close()

	// DROP + CREATE
	conn.ExecContext(nil, "DROP TABLE IF EXISTS "+viewName)
	createSQL := m.buildCreateTableSQL(viewName, batches[0])
	if _, err := conn.ExecContext(nil, createSQL); err != nil {
		m.logger.Error().Err(err).Str("sql", createSQL).Msg("Failed to create temp table")
		return
	}

	totalRows := 0
	for _, batch := range batches {
		n, _ := m.appendBatchToTable(conn, viewName, batch)
		totalRows += n
	}

	m.mu.Lock()
	m.views[bufferKey] = &arrowViewState{
		schema:    batches[0].Signature,
		rowCount:  totalRows,
		createdAt: time.Now(),
	}
	m.mu.Unlock()
}

// appendToTable 增量追加 batch 到已有临时表。
func (m *ArrowViewManager) appendToTable(bufferKey string, batches []*ingest.TypedColumnBatch) {
	viewName := ViewName(bufferKey)
	conn, err := m.db.DB().Conn(nil)
	if err != nil {
		return
	}
	defer conn.Close()

	totalRows := 0
	for _, batch := range batches {
		n, err := m.appendBatchToTable(conn, viewName, batch)
		if err != nil {
			m.createOrReplaceTable(bufferKey, batches)
			return
		}
		totalRows += n
	}

	m.mu.Lock()
	if state, exists := m.views[bufferKey]; exists {
		state.rowCount += totalRows
	}
	m.mu.Unlock()
}

// appendBatchToTable 使用 DuckDB Appender 列式写入单个 batch 到临时表。
func (m *ArrowViewManager) appendBatchToTable(conn *sql.Conn, viewName string, batch *ingest.TypedColumnBatch) (int, error) {
	colNames := make([]string, 0, len(batch.Data))
	for name := range batch.Data {
		colNames = append(colNames, name)
	}
	sort.Strings(colNames)

	if len(colNames) == 0 {
		return 0, nil
	}

	rowCount := 0
	for _, col := range batch.Data {
		switch v := col.(type) {
		case []int64:
			rowCount = len(v)
		case []float64:
			rowCount = len(v)
		case []string:
			rowCount = len(v)
		case []bool:
			rowCount = len(v)
		}
		break
	}

	// 构建 INSERT 语句
	placeholders := make([]string, len(colNames))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	insertSQL := fmt.Sprintf("INSERT INTO %s VALUES (%s)", viewName, strings.Join(placeholders, ", "))

	// 逐行插入（后续可优化为 Appender API）
	for row := 0; row < rowCount; row++ {
		values := make([]interface{}, len(colNames))
		for i, name := range colNames {
			values[i] = columnValue(batch.Data[name], batch.Validity[name], row)
		}
		conn.ExecContext(nil, insertSQL, values...)
	}

	return rowCount, nil
}

// buildCreateTableSQL 从 TypedColumnBatch 构建 CREATE TABLE 语句。
func (m *ArrowViewManager) buildCreateTableSQL(viewName string, batch *ingest.TypedColumnBatch) string {
	colNames := make([]string, 0, len(batch.Data))
	for name := range batch.Data {
		colNames = append(colNames, name)
	}
	sort.Strings(colNames)

	colDefs := make([]string, len(colNames))
	for i, name := range colNames {
		colDefs[i] = fmt.Sprintf(`"%s" %s`, name, duckTypeFor(batch.Data[name]))
	}
	return fmt.Sprintf("CREATE TEMP TABLE %s (%s)", viewName, strings.Join(colDefs, ", "))
}

func duckTypeFor(col interface{}) string {
	switch col.(type) {
	case []int64:
		return "BIGINT"
	case []float64:
		return "DOUBLE"
	case []string:
		return "VARCHAR"
	case []bool:
		return "BOOLEAN"
	default:
		return "VARCHAR"
	}
}

func columnValue(col interface{}, validity []bool, row int) interface{} {
	if validity != nil && row < len(validity) && !validity[row] {
		return nil
	}
	switch v := col.(type) {
	case []int64:
		if row < len(v) {
			return v[row]
		}
	case []float64:
		if row < len(v) {
			return v[row]
		}
	case []string:
		if row < len(v) {
			return v[row]
		}
	case []bool:
		if row < len(v) {
			return v[row]
		}
	}
	return nil
}

// Close 关闭 VIEW 管理器。
func (m *ArrowViewManager) Close() error {
	close(m.closeCh)
	m.mu.Lock()
	defer m.mu.Unlock()
	for bufferKey := range m.views {
		viewName := ViewName(bufferKey)
		m.db.DB().Exec("DROP TABLE IF EXISTS " + viewName)
		delete(m.views, bufferKey)
	}
	return nil
}
