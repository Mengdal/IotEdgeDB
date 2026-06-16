package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"iedb/internal/ingest"

	duckdb "github.com/duckdb/duckdb-go/v2"
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

	mu    sync.RWMutex
	views map[string]*arrowViewState

	// measurementViews maps "db/measurement" → set of schema-specific buffer keys.
	// Enables query-time UNION of all schema variants for a measurement.
	measurementViews map[string]map[string]struct{}

	notifyCh      chan string
	dirty         map[string]struct{}
	closeCh       chan struct{}
	closing       atomic.Bool // prevents double-close panic on closeCh
	needsFullScan atomic.Bool // set when notifyCh overflows; triggers full buffer scan on next tick
	logger        zerolog.Logger
}

// NewArrowViewManager 创建 VIEW 管理器并启动后台刷新循环。
func NewArrowViewManager(db *DuckDB, buffer *ingest.ArrowBuffer, logger zerolog.Logger) *ArrowViewManager {
	m := &ArrowViewManager{
		db:               db,
		buffer:           buffer,
		views:            make(map[string]*arrowViewState),
		measurementViews: make(map[string]map[string]struct{}),
		notifyCh:         make(chan string, 256),
		dirty:            make(map[string]struct{}),
		closeCh:          make(chan struct{}),
		logger:           logger,
	}
	go m.refreshLoop()
	return m
}

// OnNewData 实现 ingest.BufferChangeNotifier。
func (m *ArrowViewManager) OnNewData(bufferKey string) {
	select {
	case m.notifyCh <- bufferKey:
	default:
		// notifyCh full — request a full buffer scan on the next tick
		// to catch any keys dropped during this burst window.
		m.needsFullScan.Store(true)
	}
}

// OnFlushComplete 实现 ingest.BufferChangeNotifier。
func (m *ArrowViewManager) OnFlushComplete(bufferKey string) {
	m.mu.Lock()
	delete(m.views, bufferKey)
	m.removeFromMeasurementIndexLocked(bufferKey)
	m.mu.Unlock()
	m.OnNewData(bufferKey)
}

// HasData 判断指定 bufferKey 是否有活跃的缓冲 VIEW。
func (m *ArrowViewManager) HasData(bufferKey string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, exists := m.views[bufferKey]
	return exists
}

// HasMeasurementData 判断指定 (database, measurement) 下是否有任何活跃 VIEW。
// 用于查询端快速判断是否需要 UNION 缓冲数据。
func (m *ArrowViewManager) HasMeasurementData(database, measurement string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := database + "/" + measurement
	keys, exists := m.measurementViews[key]
	return exists && len(keys) > 0
}

// MeasurementViewNames 返回指定 (database, measurement) 下所有活跃 schema 的 VIEW 名称。
// 用于查询端构造 multi-schema UNION ALL 子句。
func (m *ArrowViewManager) MeasurementViewNames(database, measurement string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := database + "/" + measurement
	keys := m.measurementViews[key]
	result := make([]string, 0, len(keys))
	for k := range keys {
		if _, ok := m.views[k]; ok {
			result = append(result, ViewName(k))
		}
	}
	return result
}

// registerInMeasurementIndex adds the buffer key to the measurement index.
// Must be called with m.mu held.
func (m *ArrowViewManager) registerInMeasurementIndexLocked(bufferKey string) {
	// Extract "db/measurement" from "db/measurement__hash"
	baseKey, _ := ingest.StripSchemaHash(bufferKey)
	if baseKey == "" {
		return
	}
	if m.measurementViews[baseKey] == nil {
		m.measurementViews[baseKey] = make(map[string]struct{})
	}
	m.measurementViews[baseKey][bufferKey] = struct{}{}
}

// removeFromMeasurementIndex removes the buffer key from the measurement index.
// Must be called with m.mu held.
func (m *ArrowViewManager) removeFromMeasurementIndexLocked(bufferKey string) {
	baseKey, _ := ingest.StripSchemaHash(bufferKey)
	if keys, ok := m.measurementViews[baseKey]; ok {
		delete(keys, bufferKey)
		if len(keys) == 0 {
			delete(m.measurementViews, baseKey)
		}
	}
}

// ViewName 将 bufferKey 转为 DuckDB 临时表名称。
func ViewName(bufferKey string) string {
	return "_iedb_buffer_" + strings.ReplaceAll(bufferKey, "/", "_")
}

// QuoteIdent wraps a SQL identifier in double quotes and escapes internal
// double-quotes by doubling them, per the SQL standard. DuckDB follows the
// same convention as PostgreSQL for quoted identifiers.
func QuoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
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
			// Full scan: notifyCh overflowed during this window.
			// Walk all buffer keys to recover any dropped notifications.
			if m.needsFullScan.Swap(false) {
				allKeys := m.buffer.AllBufferKeys()
				m.mu.Lock()
				for _, key := range allKeys {
					m.dirty[key] = struct{}{}
				}
				m.mu.Unlock()
			}

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
		m.buffer.MarkRefreshed(bufferKey)
	} else if err := m.appendToTable(bufferKey, newBatches); err != nil {
		// Append failed — old VIEW data is intact (table not dropped).
		// Skip MarkRefreshed so the next refresh cycle retries these batches.
		m.logger.Warn().Err(err).Str("buffer_key", bufferKey).Msg("Incremental VIEW append failed, will retry next cycle")
	} else {
		m.buffer.MarkRefreshed(bufferKey)
	}
}

// createOrReplaceTable 创建或重建临时表。
func (m *ArrowViewManager) createOrReplaceTable(bufferKey string, batches []*ingest.TypedColumnBatch) {
	viewName := ViewName(bufferKey)
	if len(batches) == 0 {
		return
	}

	conn, err := m.db.DB().Conn(context.Background())
	if err != nil {
		m.logger.Error().Err(err).Str("table", viewName).Msg("Failed to get connection")
		return
	}
	defer conn.Close()

	// DROP + CREATE
	if _, err := conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+QuoteIdent(viewName)); err != nil {
		m.logger.Warn().Err(err).Str("table", viewName).Msg("Failed to drop old VIEW table — CREATE may be skipped by IF NOT EXISTS")
	}
	createSQL := m.buildCreateTableSQL(viewName, batches[0])
	if _, err := conn.ExecContext(context.Background(), createSQL); err != nil {
		m.logger.Error().Err(err).Str("sql", createSQL).Msg("Failed to create VIEW table")
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
	m.registerInMeasurementIndexLocked(bufferKey)
	m.mu.Unlock()
}

// appendToTable 增量追加 batch 到已有临时表。
// Returns nil on success, or the DuckDB append error on failure.
// On failure, the old VIEW table is NOT dropped — the caller (refreshView)
// skips MarkRefreshed so the failed batches will be retried next cycle.
func (m *ArrowViewManager) appendToTable(bufferKey string, batches []*ingest.TypedColumnBatch) error {
	viewName := ViewName(bufferKey)
	conn, err := m.db.DB().Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()

	totalRows := 0
	for _, batch := range batches {
		n, err := m.appendBatchToTable(conn, viewName, batch)
		if err != nil {
			return err
		}
		totalRows += n
	}

	m.mu.Lock()
	if state, exists := m.views[bufferKey]; exists {
		state.rowCount += totalRows
	}
	m.mu.Unlock()
	return nil
}

// appendBatchToTable 使用 DuckDB Appender API 批量列式写入单个 batch 到临时表。
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

	if rowCount == 0 {
		return 0, nil
	}

	var appender *duckdb.Appender
	err := conn.Raw(func(driverConn any) error {
		dc, ok := driverConn.(driver.Conn)
		if !ok {
			return fmt.Errorf("connection does not implement driver.Conn")
		}
		var appErr error
		appender, appErr = duckdb.NewAppenderFromConn(dc, "", viewName)
		if appErr != nil {
			return fmt.Errorf("failed to create appender for %s: %w", viewName, appErr)
		}

		// Batch append all rows via the Appender API
		for row := 0; row < rowCount; row++ {
			values := make([]driver.Value, len(colNames))
			for i, name := range colNames {
				values[i] = columnValue(batch.Data[name], batch.Validity[name], row)
			}
			if err := appender.AppendRow(values...); err != nil {
				_ = appender.Close()
				appender = nil // prevent double-close in outer block
				return fmt.Errorf("appender row %d: %w", row, err)
			}
		}
		return nil
	})

	if appender != nil {
		_ = appender.Close() // Flush + close
	}
	if err != nil {
		return 0, err
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
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", QuoteIdent(viewName), strings.Join(colDefs, ", "))
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
	if !m.closing.CompareAndSwap(false, true) {
		return nil // already closed
	}
	close(m.closeCh)
	m.mu.Lock()
	defer m.mu.Unlock()
	for bufferKey := range m.views {
		viewName := ViewName(bufferKey)
		m.db.DB().Exec("DROP TABLE IF EXISTS " + QuoteIdent(viewName))
		delete(m.views, bufferKey)
	}
	// Clear measurement index
	for k := range m.measurementViews {
		delete(m.measurementViews, k)
	}
	return nil
}
