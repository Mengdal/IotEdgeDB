# Design: data + validity 合并为 ColumnData

**日期**: 2026-06-19  
**分支**: feat/growing-columnar-buffer  
**状态**: 设计中  
**前提**: bufferEntry 统一 + 类型派发收拢 已完成

## 问题

当前 `bufferEntry` 用两个独立 map 分别存储列数据和 null bitmap：

```go
data     map[string]interface{}  // 列名 → typed slice
validity map[string][]bool       // 列名 → null bitmap
```

导致：
1. **双 map lookup** — 每列操作要查两次 `data[name]` + `validity[name]`
2. **同步风险** — validity backfill 逻辑复杂（`appendEntryToEntry` 中 27 行），历史出过 bug
3. **函数传参冗余** — `(data map, validity map)` 成对传递，sort/slice/flush 路径都要分别处理

## 设计

### 第一层：ColumnData — 原子化的列数据

```go
// ColumnData bundles a typed column slice with its null bitmap.
// Validity is nil when all values are valid (same contract as before).
type ColumnData struct {
    Data     any      // []int64 | []float64 | []string | []bool | []decimal128.Num
    Validity []bool   // nil = all valid; len must equal len(Data) when non-nil
}
```

### 第二层：bufferEntry — 双 map 合为单 map

```go
type bufferEntry struct {
    columns       map[string]ColumnData  // 曾: data + validity 两个 map
    tagColumns    []string
    schema        string
    startTime     time.Time
    recordCount   int
    estimatedBytes uint64
    arrowSchema   *arrow.Schema
    refreshIndex  int
}
```

### 第三层：派发函数升级 — 签名从 any 变为 ColumnData

| 函数 | 之前 | 之后 | validity 处理 |
|---|---|---|---|
| `colLen(c)` | `any` → int | `ColumnData` → int | 读 `.Data` 取 len |
| `colMake(firstVal, n)` | `any, int` → `any` | `any, int` → `ColumnData` | 返回 Data=nil 的 ColumnData |
| `colAppend(dst, src)` | `any, any` → `any` | `ColumnData, ColumnData` → `ColumnData` | **原子化合并 dst+src Validity** |
| `colSlice(c, indices)` | `any, []int` → `any` | `ColumnData, []int` → `ColumnData` | **同步切片 Validity** |
| `colPermute(c, indices)` | `any, []int` → `any` | `ColumnData, []int` → `ColumnData` | **同步重排 Validity** |
| `colSliceFrom(c, start)` | `any, int` → `any` | `ColumnData, int` → `ColumnData` | **同步切片 Validity** |
| `colLess(c, i, j)` | `any, int, int` → bool | `ColumnData, int, int` → bool | 不变（忽略 Validity） |
| `colEstBytesPerRow(c)` | `any` → uint64 | `ColumnData` → uint64 | 不变 |
| `colIthVal(c, i)` | `any, int` → `any` | `ColumnData, int` → `any` | **新增：内部检查 Validity，null 返回 nil** |
| `colTypeTag(c)` | `any` → string | `ColumnData` → string | 不变（读 `.Data` 类型） |

### 第四层：关键路径简化

**appendEntryToEntry — 27 行 validity backfill 消失：**

```go
func appendEntryToEntry(dst, src *bufferEntry) {
    if dst.columns == nil {
        dst.columns = make(map[string]ColumnData)
    }
    for name, col := range src.columns {
        if existing, ok := dst.columns[name]; ok {
            dst.columns[name] = colAppend(existing, col)
        } else {
            dst.columns[name] = col
        }
    }
    if len(dst.tagColumns) == 0 && len(src.tagColumns) > 0 {
        dst.tagColumns = src.tagColumns
    }
    dst.recordCount += src.recordCount
    dst.estimatedBytes += src.estimatedBytes
}
```

**convertAndAppendToEntry — 不再手动管理 entry.validity：**

```go
// 之前：每列转换后 if colHasNils { if entry.validity == nil { make(map...) } entry.validity[name] = append(...) }
// 之后：
entry.columns[name] = colAppend(entry.columns[name], ColumnData{Data: arr, Validity: valid})
// colAppend 内部处理 validity 合并 — 调用方零负担
```

**sortEntryByKeys — validity 随 data 一起排序：**

```go
// 之前：sortedData + 手动重排 sortedValidity
// 之后：
sortedColumns := make(map[string]ColumnData, len(entry.columns))
for name, col := range entry.columns {
    sortedColumns[name] = colPermute(col, indices)
}
```

**sliceEntryByIndices — validity 随 data 一起切片：**

```go
// 之前：slicedData + 手动切片 slicedValidity
// 之后：
slicedColumns := make(map[string]ColumnData, len(entry.columns))
for name, col := range entry.columns {
    slicedColumns[name] = colSlice(col, indices)
}
```

### 第五层：跨包 API

```go
// Entry 访问器 — 之前
func (e *bufferEntry) GetData() map[string]interface{}  { return e.data }
func (e *bufferEntry) GetValidity() map[string][]bool     { return e.validity }

// 之后 — ColumnData 已导出
func (e *bufferEntry) GetColumns() map[string]ColumnData { return e.columns }
```

`arrow_view.go` 中 `colIthVal(cd, row)` 自身处理 null 检查，不再需要独立 `columnValue` 函数。

## 改动清单

### 新增/修改类型

| 项 | 说明 |
|---|---|
| `ColumnData` struct | 新增导出类型 |
| `bufferEntry.columns` | `map[string]ColumnData` 替代 data + validity |

### 派发函数 — 签名全改

| 函数 | 改动 |
|---|---|
| `colLen` | 参数 `ColumnData`，读 `.Data` |
| `colMake` | 返回 `ColumnData` |
| `colAppend` | 参数/返回 `ColumnData`，合并 validity |
| `colSlice` | 参数/返回 `ColumnData`，切片 validity |
| `colPermute` | 参数/返回 `ColumnData`，重排 validity |
| `colSliceFrom` | 参数/返回 `ColumnData`，切片 validity |
| `colIthVal` | 参数 `ColumnData`，检查 validity 返回 nil |
| `colLess` | 参数 `ColumnData` |
| `colEstBytesPerRow` | 参数 `ColumnData` |
| `colTypeTag` | 参数 `ColumnData` |

### 调用点 — 简化

| 函数 | 改动 |
|---|---|
| `appendEntryToEntry` | 删 27 行 validity backfill，单循环 colAppend |
| `prependFlushDataToEntry` | 同上模式 |
| `convertAndAppendToEntry` | 删手动 validity 管理，`colAppend(entry.columns[name], ColumnData{Data: arr, Validity: valid})` |
| `sortEntryByKeys` | 删手动 validity 重排 |
| `sliceEntryByIndices` | 删手动 validity 切片 |
| `SinceRefresh` | 单次 colSliceFrom 同时产出 data + validity |
| `WriteParquetColumnar` | 读 `entry.columns`，拆 `.Data`/`.Validity` 构建 Arrow arrays |
| `estimateBytesPerRow` / `estimateBytesFromData` | 迭代 `entry.columns` |
| `getColumnSignature` | 迭代 `entry.columns`，`colTypeTag` |
| `inferSchema` | 迭代 `entry.columns` |
| `flushPartitionedData` | 签名从 `(data, validity)` → `(columns)` |
| `prependFlushData` | 同上 |
| `getColumnSignatureLight` | 不受影响（操作 raw `[]interface{}`） |
| `groupByHour` | 读 `entry.columns["time"]` |
| `arrow_view.go` | `GetColumns()` 替代 `GetData()`+`GetValidity()` |

### 删除

| 项 | 说明 |
|---|---|
| `bufferEntry.data` | 合并进 columns |
| `bufferEntry.validity` | 合并进 columns |
| `bufferEntry.GetData()` | 替换为 `GetColumns()` |
| `bufferEntry.GetValidity()` | 同上 |
| `columnValue()` (arrow_view.go) | 被 `colIthVal` 内置替代 |

## 收益

| 维度 | 之前 | 之后 |
|---|---|---|
| map lookup 次数 | 每列 2 次 | 每列 1 次 |
| validity 同步风险 | backfill 逻辑 27 行，历史出 bug | 结构保证原子性，0 backfill 代码 |
| 函数传参 | `(data map, validity map)` 成对 | `(columns map)` 一个 |
| sort/slice 路径 | 分别处理 data + validity | colPermute/colSlice 一次完成 |
| 新增列类型 | 所有 dispatch 函数 + validity backfill | 所有 dispatch 函数（backfill 逻辑删了） |

## 不做

- 不改 parser 层
- 不改 WAL 层
- 不改 sort 算法、partition 策略、flush 策略
- 不改 shard 锁策略
- `entryToWALRecords` 结构适配但逻辑不变

## 风险

- **改动面大**：ColumnData 触及 ~15 个函数，但每个改动是机械的（`.data[name]` → `.columns[name].Data`）
- **导出 ColumnData**：database 包依赖 `ColumnData` 类型，但只读不写，耦合可控
- **性能**：无回退风险 — 少一次 map lookup，colAppend 内部 validity 操作是纯 slice append，与原逻辑等价
