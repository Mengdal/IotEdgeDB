# Growing Columnar Buffer

**日期**: 2026-06-15  
**分支**: feat/adaptive-buffer  
**状态**: design

## 动机

当前 `bufferEntry` 以 `[]*TypedColumnBatch` 数组持有缓冲数据。每次写入追加一个 batch 指针，flush 时通过 `mergeBatches` 将所有 batch 的列数组垂直拼接为连续内存，然后写入 Parquet。

这种"到达时追加指针，flush 时缝合"的模式有两个代价：
1. **flush 时全量拷贝**：`mergeBatches` 三阶段（扫描统计 → 预分配 → 逐 batch memmove），O(总行数×列数)
2. **失败恢复链长**：`mergeBatches` 失败 → `restoreBufferEntry`（95 行），`flushPartitionedData` 失败 → `writeBackMergedData` → `typedBatchToColumns` → `WriteColumnarDirectNoWAL`（50 行）

改为增长式列存——数据写入时直接 `append` 到 entry 的列数组中——flush 时数据已是连续内存，无需 merge。

## 核心变化

```
当前:
  bufferEntry.batches = []*TypedColumnBatch     ← batch 指针数组
  flush: mergeBatches(batches) → 1 个大 TypedColumnBatch

改为:
  bufferEntry.data     = map[string]interface{}  ← 直接增长的列数组
  bufferEntry.validity = map[string][]bool       ← 同步增长的 validity
  bufferEntry.tagColumns = []string               ← 该 schema 的 tag 列（稳定不变）
  flush: 直接包装为 TypedColumnBatch，无需 merge
```

## 设计决策

### TypedColumnBatch 保留

`TypedColumnBatch` 作为类型转换的输出（`convertColumnsToTyped`）、flush 的传输载体（`flushPartitionedData` 入参）、sort/slice 的操作单元，以及 VIEW 刷新的数据包装，全部保留。只删除 `bufferEntry.batches` 数组，不删除 `TypedColumnBatch` 类型。

### Validity 在 append 时递增加入

当前 validity 随 batch 独立存储，`mergeBatches` 时统一处理。增长式下 validity 需随数据同步增长：

- 列无 null（`typedColumns.Validity` 为 nil）：跳过，`entry.validity` 不创建该项
- 列有 null：`append(entry.validity[name], valid...)`
- 已有 validity 的列，本 batch 无显式条目：补齐 `batchRows` 个 `true`

时序数据中 null 稀少（通常在数值型传感器数据中不存在），热路径无额外开销。内存总量与 mergeBatches 的 Phase 2 分配等价（从集中分配变为渐进分配）。

### 失败恢复：prependFlushData

增长式下 flush 失败时数据直接 prepend 回 entry，替代当前的 `writeBackMergedData` → `typedBatchToColumns` → `WriteColumnarDirectNoWAL` 三步链路：

```go
func (b *ArrowBuffer) prependFlushData(bufferKey string, data map[string]interface{}, validity map[string][]bool, flushedRows int) {
    shard := b.getShard(bufferKey)
    shard.mu.Lock()
    entry := shard.buffers[bufferKey]
    for name, col := range data {
        // prepend: 旧数据在前，flush 期间到达的新数据在后
        entry.data[name] = append(col.([]T), entry.data[name].([]T)...)
    }
    // validity 同理合并
    entry.recordCount += flushedRows
    shard.mu.Unlock()
}
```

仅约 15 行，不需要类型反向转换，不需要重新走写入链路。

### SinceRefresh / MarkRefreshed：batch 索引 → 行索引

```diff
- refreshIndex = len(batches)       // 第几个 batch
+ refreshIndex = recordCount        // 第几行

- 返回 batches[refreshIndex:]       // batch 指针切片
+ 返回 data["time"][refreshIdx:]    // sub-slice，零拷贝
```

## 改动清单

### 修改

| 文件 | 位置 | 改动 |
|------|------|------|
| `bufferEntry` | arrow_writer.go:506 | `batches` → `data` + `validity` + `tagColumns` |
| `isEmpty()` | arrow_writer.go:519 | `len(batches)==0` → `len(data)==0` |
| `writeColumnarInternal` | arrow_writer.go:1380 | append 逻辑：batch 指针 → 按列 growth + validity 合并 |
| `writeTypedColumnarInternal` | arrow_writer.go:1552 | 同上 |
| `flushCandidate` | adaptive_flush.go:221 | 提取 `data` 替代 `batches` 指针列表 |
| `evictOldestEntries` | arrow_writer.go:2154 | 同上，直接调 `flushPartitionedData` |
| `flushBufferLocked` | arrow_writer.go:2710 | 同上 |
| `flushWorker` | arrow_writer.go:2063 | 跳过 `flushRecordsAsync`，直接调 `flushPartitionedData` |
| `flushTask` | arrow_writer.go:561 | `records []interface{}` → `data map[string]interface{}` + `validity` + `tagColumns` |
| `SinceRefresh` | arrow_writer.go:3636 | 返回 sub-slice 包装，非 batch 指针列表 |
| `MarkRefreshed` | arrow_writer.go:3653 | `len(batches)` → `recordCount` |
| `gcEmptyEntries` | arrow_writer.go:2031 | `len(batches)` → `entry.isEmpty()` |
| `convertColumnsToTyped` | arrow_writer.go:1731 | 调用方负责将结果 append 到 entry（返回 TypedColumnBatch 不变） |

### 新增

| 函数 | 说明 |
|------|------|
| `prependFlushData` | flush 失败时将提取的数据 prepend 回 entry（~15 行） |

### 删除

| 函数 | 行数 | 原因 |
|------|------|------|
| `mergeBatches` | ~200 | 数据已连续 |
| `flushRecordsAsync` | ~75 | 不再需要 merge + 恢复包装 |
| `restoreBufferEntry` | ~95 | 不再有 merge 失败场景 |
| `writeBackMergedData` | ~50 | 替换为 prependFlushData |
| `typedBatchToColumns` | ~35 | 不再需要 TypedColumnBatch → map[string][]interface{} 反向转换 |

**净删约 440 行。**

### 不受影响

WAL（写入和恢复）、`WriteParquetColumnar`、`flushPartitionedData`、storage 写入、tiering 注册、VIEW 通知、自适应引擎决策逻辑、所有触发条件（size/age/pressure/hard_limit/schema_conflict）、sort/slice/groupByHour 等工具函数。

## 并发安全

- **写入 + flush**：flush 提取 data 后立即替换为新空 map，并发写入操作新 map，不冲突
- **写入 + VIEW**：`SinceRefresh` 返回 sub-slice，Go slice 引用语义保证旧底层数组不被 GC
- **flush 失败 + 新写入**：prepend 将旧数据放在新数据前面，数据时间顺序正确
- **硬限制淘汰**：entry 整体删除，不涉及 prepend

## 风险评估

| 风险 | 概率 | 影响 | 缓解 |
|------|------|------|------|
| Validity 合并引入 bug | 中 | 数据正确性 | 重点测试有 null 列的写入路径 |
| enqueue 失败 prepend 路径未被充分测试 | 低 | 数据丢失 | 仅队列满/关闭中触发，WAL 兜底 |
| VIEW sub-slice 生命周期 | 低 | 悬空指针 | Go slice 引用语义保证安全 |
| 单 batch flush 退化 | 低 | 性能微降 | flush 省掉的 mergeBatches 远超此开销 |
