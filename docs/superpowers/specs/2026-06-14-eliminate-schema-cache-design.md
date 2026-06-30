# 删除 schemaLRUCache，改为 bufferEntry 持有 *arrow.Schema

## 概述

将 Arrow Schema 缓存从独立的 `schemaLRUCache`（双向链表 + HashMap，~120 行）移动到 `bufferEntry` 中，让 buffer map 兼任 schema 缓存。flush 成功后保留空壳 entry（含 `*arrow.Schema`），下次写入直接复用。

**改动范围：** 仅 `internal/ingest/arrow_writer.go` 一个文件。

## 动机

当前存在两套列描述逻辑：
- `getSchema()` 内部扫描列构造缓存键（无排序，依赖 map 遍历随机顺序）
- `getColumnSignature()` 扫描列生成签名（排序，格式不同）

且 `schemaLRUCache` 是一个独立的并发数据结构，与 buffer map 生命周期独立管理，增加心智负担。实际部署中 measurement 数量 <1000，空壳内存占用 <200KB，不需要独立的淘汰机制。

## 设计

### 结构体变更

**删除：**
- `schemaCacheEntry` 结构体
- `schemaLRUCache` 结构体及其 7 个方法（get/set/moveToFront/addToFront/removeEntry/evictLRU/newSchemaLRUCache）
- `ArrowWriter.schemaCache` 字段
- `getSchema()` 方法

**新增：**
- `bufferEntry.arrowSchema *arrow.Schema` 字段

**简化：**
- `ArrowWriter` 不再持有 schema 缓存，变为纯函数式转换
- `NewArrowWriter` 不再初始化 `schemaCache`

### Schema 推断时机前移

```
修改前（延迟推断）:
  writeColumnarInternal → 追加 batch 到 entry
  ... 几秒后 ...
  flush → WriteParquetColumnar → getSchema → 缓存未命中 → inferSchema

修改后（创建时推断）:
  writeColumnarInternal → 创建 entry → inferSchema → 存入 entry.arrowSchema
  ... 几秒后 ...
  flush → WriteParquetColumnar → 直接使用 entry.arrowSchema（nil 检查兜底）
```

`WriteParquetColumnar` 新增 `schema *arrow.Schema` 参数，nil 时走 inferSchema 兜底。

### 空壳保留

flush 成功后不再 `delete(shard.buffers, key)`，改为清空数据字段：

```go
entry.batches = nil
entry.recordCount = 0
entry.estimatedBytes = 0
// 保留: schema, arrowSchema, startTime
```

### 条件变更（7 处）

所有需要感知空壳的位置加 `len(entry.batches) == 0` 检查：

| 位置 | 变更 |
|------|------|
| `restoreBufferEntry` | `!exists` → `!exists \|\| entry.isEmpty()` |
| `flushTypeConflicts` | 跳过空壳 |
| `flushAgedBuffers` | 跳过空壳 |
| `computeNextFlushDeadline` | 跳过空壳 |
| `Close()` 最终刷盘 | 跳过空壳 |
| `FlushAll()` | 跳过空壳 |
| `AllBufferKeys()` | 过滤掉空壳 |

### 空壳 GC

在 `periodicFlush` 中每 30 次循环执行一次。删除同时满足以下条件的 entry：
- `len(entry.batches) == 0`（空壳）
- `time.Since(entry.startTime) > maxBufferAge * 2`（空闲超过两倍缓冲时长）

`maxBufferAge * 2` 留出安全余量，避免刚清空的 entry 被误 GC。

### 不影响恢复逻辑

两条失败恢复路径（同步 `flushBufferLocked` 和异步 `flushRecordsAsync`）的行为在改前改后语义等价：空壳 entry 被 `restoreBufferEntry` 或 `writeBackMergedData` 正确覆写/追加。

## 代码量

| 类别 | 行数 |
|------|------|
| 删除（schemaLRUCache 全套） | ~120 |
| 删除（getSchema） | ~55 |
| 新增（空壳判定 + GC + 推断前移） | ~45 |
| 修改（条件变更 7 处 + 调用链传参） | ~30 |
| **净变化** | **约 -100 行** |

## 风险

- **低风险：** 改动集中在一个文件内，恢复路径语义等价可逐条验证
- **空壳 GC 遗漏：** CI/CD 随机 measurement 名场景下空壳可能堆积。GC 阈值 `maxBufferAge * 2` 可能在 `maxBufferAge` 很大时（如 30 分钟）延迟回收。可通过单独调小 GC 阈值或增加主动清理点缓解
- **并发：** 不改动锁模型，不改动锁持有时间。shard 锁保护 entry 的所有字段（包括新增的 `arrowSchema`）

## 测试策略

现有 11 个测试文件保持不变，预计全部通过：
- 恢复路径测试（`arrow_writer_flush_failure_test.go`）— 验证空壳覆写
- 竞态测试（`arrow_writer_close_race_test.go`）— 验证空壳并发安全
- Schema 演进测试（`arrow_writer_schema_evolution_test.go`）— 验证空壳被新 schema 覆写
- 混沌测试（`arrow_writer_chaos_test.go`）— 验证空壳 GC 不误删活跃 entry
