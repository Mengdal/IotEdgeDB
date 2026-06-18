# Design: columnSlice Consolidation — TypedColumnBatch 撤出热路径 + 类型派发收拢

**日期**: 2026-06-18  
**分支**: feat/growing-columnar-buffer  
**状态**: 设计中

## 问题

1. **热路径存在无意义的"创建-拆包"循环**：`convertColumnsToTyped()` 创建 `TypedColumnBatch`（分配 `map[string]interface{}` + struct），立刻在 `appendTypedBatchToEntry()` 中拆包并二次遍历
2. **type switch 散落在 10+ 个函数中**：`appendTypedBatchToEntry`、`prependFlushDataToEntry`、`sliceColumnsByIndices`、`applyPermutation`、`compareMultiKeyCached`、`sortColumnsByKeysWithPermutation`、`estimateBytesPerRow`、`typedBatchToWALRecords`、`SinceRefresh`、`WriteParquetColumnar`、`inferSchema`、`getColumnSignature` —— 每个都针对 5 种类型（int64 / float64 / string / bool / decimal128）各写一套 switch case
3. **新增第 6 种类型需要改 12 个位置**

## 设计

### 第一层：columnSlice — 带类型标签的列值对象

```go
type columnKind uint8

const (
    colInt64 columnKind = iota
    colFloat64
    colString
    colBool
    colDecimal128
)

type columnSlice struct {
    kind columnKind
    data any  // 底层: []int64 | []float64 | []string | []bool | []decimal128.Num
}
```

**8 个集中派发函数**（全库所有 type switch 收拢于此）：

| 函数 | 用途 | type switch |
|---|---|---|
| `colLen(c columnSlice) int` | 取行数 | 5 case |
| `colMake(kind columnKind, n int) columnSlice` | 分配列 | 5 case |
| `colAppend(dst, src columnSlice) columnSlice` | 追加 | 5 case |
| `colSlice(c columnSlice, indices []int) columnSlice` | 按索引切片 | 5 case |
| `colPermute(c columnSlice, indices []int) columnSlice` | 按排列重排 | 5 case |
| `colLess(c columnSlice, i, j int) bool` | 排序比较 | 5 case |
| `colEstBytesPerRow(c columnSlice) uint64` | 内存估算 | 5 case |
| `colIthVal(c columnSlice, i int) any` | 取第 i 行值（WAL 转换用） | 5 case |

**凡是对列做操作的代码，不再写 type switch，只调用集中函数。**

### 第二层：TypedColumnBatch / bufferEntry 字段类型变更

```go
// 之前
type TypedColumnBatch struct {
    Data       map[string]interface{}  // []int64|[]float64|...
    Validity   map[string][]bool
    TagColumns []string
    Signature  string
}

// 之后
type TypedColumnBatch struct {
    Data       map[string]columnSlice  // 列名 → 类型标签 + typed slice
    Validity   map[string][]bool
    TagColumns []string
    Signature  string
}

// bufferEntry 同样变更
type bufferEntry struct {
    columns       map[string]columnSlice  // 曾: data map[string]interface{}
    validity      map[string][]bool
    tagColumns    []string
    startTime     time.Time
    recordCount   int
    estimatedBytes uint64
    schema        string
    arrowSchema   *arrow.Schema
    refreshIndex  int
}
```

### 第三层：热路径优化 — convert 直接进 entry

```
之前:
  convertColumnsToTyped()           ← 锁外：分配 typed map + validity map + struct
    └─ 逐列类型转换                  第一趟遍历
  Lock
  appendTypedBatchToEntry()         ← 锁内：拆包 batch
    └─ 再次逐列遍历 + type switch    第二趟遍历
  Unlock

之后:
  getColumnSignatureLight()         ← 锁外：只取每列第一个非 nil 值的类型标签，0 分配
  Lock
  convertAndAppendToEntry()         ← 锁内：单趟
    └─ 类型转换 + colAppend() 直接追加到 entry.columns
  Unlock
```

### TypedColumnBatch 角色变化

| | 之前 | 之后 |
|---|---|---|
| 创建者 | `convertColumnsToTyped()`（hot path）、`flushWorker` 内联包装、`SinceRefresh` | `flushWorker` 内联包装、`flushCandidate`、`SinceRefresh` |
| 使用者 | `appendTypedBatchToEntry`（拆包）、`flushPartitionedData`、`sortTypedColumnBatchByKeys`、`sliceTypedColumnBatchByIndices`、`appendBatchToTable` | 同左，但不再出现在 ingest 热路径 |
| 语义 | 通用数据载体 | flush / VIEW 操作单元 |

## 受影响函数枚举

| 函数 | 文件 | 改动性质 |
|---|---|---|
| `getColumnSignature` | arrow_writer.go | 适配 columnSlice |
| `inferSchema` | arrow_writer.go | 适配 columnSlice |
| `WriteParquetColumnar` | arrow_writer.go | Arrow 构建仍需 type switch（箭头的 `field.Type.ID()` 派发），但用集中函数取列数据 |
| `appendTypedBatchToEntry` | arrow_writer.go | type switch → `colAppend` 调用 |
| `prependFlushDataToEntry` | arrow_writer.go | type switch → `colAppend` 调用 |
| `estimateBytesPerRow` | arrow_writer.go | type switch → `colEstBytesPerRow` 调用 |
| `estimateBytesFromData` | arrow_writer.go | 同上 |
| `sliceColumnsByIndices` | arrow_writer.go | type switch → `colSlice` 调用 |
| `applyPermutation` | arrow_writer.go | type switch → `colPermute` 调用 |
| `compareMultiKeyCached` | arrow_writer.go | type switch → `colLess` 调用 |
| `sortColumnsByKeysWithPermutation` | arrow_writer.go | 取行数用 `colLen` |
| `sortTypedColumnBatchByKeys` | arrow_writer.go | 数据流适配 |
| `sliceTypedColumnBatchByIndices` | arrow_writer.go | 数据流适配 |
| `typedBatchToWALRecords` | arrow_writer.go | type switch → `colIthVal` 调用 |
| `SinceRefresh` | arrow_writer.go | type switch → `colSlice` (from index) |
| `convertColumnsToTyped` | arrow_writer.go | **删除**，逻辑并入新增函数 |
| `convertAndAppendToEntry` | arrow_writer.go | **新增** — 单趟转换 + 追加 |
| `getColumnSignatureLight` | arrow_writer.go | **新增** — 轻量 schema 推断 |
| `writeColumnarInternal` | arrow_writer.go | 调用 `convertAndAppendToEntry` 替代 convert+append 两步 |
| `appendBatchToTable` | arrow_view.go | 适配 columnSlice |
| `buildCreateTableSQL` | arrow_view.go | 适配 columnSlice |

## 不改的部分

- `TypedColumnBatch` 类型**保留**，只是字段类型变
- `flushPartitionedData` / `flushWorker` / `evictOldestEntries` / `flushCandidate` / `flushBufferLocked` — 逻辑不动，自然适配新类型
- `groupByHour` — 只读 time 列，不受影响
- `generateStoragePath` — 不受影响
- 所有 parser（msgpack / lineprotocol / tle）— 不受影响
- 所有测试 — 类型层面适配（字段名/类型变更），逻辑不变

## 基线 & 目标

基于 `feat/growing-columnar-buffer` 分支的当前基准：

| Benchmark | 当前 (feature) | 目标 |
|---|---|---|
| Ingest_WriteBuffer/batch=100 | 3935 ns/op, 20 allocs/op, 25.5M rec/s | ~3400 ns/op, ~17 allocs/op |
| Ingest_WriteBuffer/batch=10000 | 185620 ns/op, 72 allocs/op, 54.8M rec/s | ~176000 ns/op, ~69 allocs/op |
| ConcurrentWrites/32_shards | 3211 ns/op, 124 allocs/op | ~2900 ns/op, ~121 allocs/op |
| BufferEntry_SingleRow | 162208 ns/op, 3029 allocs/op | ~130000 ns/op, ~2000 allocs/op |

## 风险

- **回退成本低**：只改 2 个文件（arrow_writer.go + arrow_view.go），columnSlice 是对 interface{} 的透明替换
- **性能回退风险极低**：kind 派发（uint8 比较）性能 ≥ type pointer 比较，append 路径持平
- **测试覆盖**：现有 25+ 测试文件的类型层面适配需仔细处理，但逻辑不变

## 不做

- 不改 parser 层 — parser 继续产出 `map[string][]interface{}`
- 不改 WAL 层 — WAL 继续写 row format
- 不改 sort 算法、partition 算法、flush 策略
- 不引入泛型 — 当前类型种类固定（5 种），不需过度设计
