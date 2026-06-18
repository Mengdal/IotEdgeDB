# Design: bufferEntry 统一 + 类型派发收拢

**日期**: 2026-06-18  
**分支**: feat/growing-columnar-buffer  
**状态**: 设计中

## 问题

1. **热路径存在无意义的"创建-拆包"循环**：`convertColumnsToTyped()` 创建 `TypedColumnBatch`（分配 map + struct + 逐列类型转换），立刻在 `appendTypedBatchToEntry()` 中拆包并二次遍历
2. **两个同构类型并存**：`TypedColumnBatch` 和 `bufferEntry` 有 4 个字段完全同构（Data↔data、Validity↔validity、TagColumns↔tagColumns、Signature↔schema），flush/sort/slice/VIEW 操作在两者间反复包装/拆包
3. **type switch 散落在 10+ 个函数中**：每种操作都针对 5 种类型 (int64/float64/string/bool/decimal128) 各写一套 switch case
4. **新增第 6 种类型需要改 12 个位置**

## 设计

### 第一层：类型结构化 — bufferEntry 作为唯一数据结构

`TypedColumnBatch` 删除，统一使用 `bufferEntry`：

```go
type bufferEntry struct {
    data           map[string]any  // 列名 → typed slice: []int64|[]float64|[]string|[]bool|[]decimal128.Num
    validity       map[string][]bool
    tagColumns     []string
    schema         string                 // 列签名，等同于原 TypedColumnBatch.Signature
    startTime      time.Time              // live entry: 首条到达时间; snapshot: 零值
    recordCount    int                    // 通用
    estimatedBytes uint64                 // live entry: 估算内存; snapshot: 可为零
    arrowSchema    *arrow.Schema          // live entry / flush task 有意义
    refreshIndex   int                    // live entry: VIEW 增量游标
}
```

snapshot 场景（sort 中间结果、VIEW 返回）中 `startTime`/`estimatedBytes`/`refreshIndex` 为零值死字段，语义上不完美，但功能正确。

### 第二层：8 个集中派发函数 — 全库 type switch 收拢

所有操作列数据的函数集中在此，参数/返回值使用 `any`，内部做一次 type switch：

| 函数 | 用途 |
|---|---|
| `colLen(col any) int` | 取列行数 |
| `colMake(firstVal any, n int) any` | 分配指定类型的列 |
| `colAppend(dst, src any) any` | 追加两列 |
| `colSlice(col any, indices []int) any` | 按索引切片 |
| `colPermute(col any, indices []int) any` | 按排列重排 |
| `colLess(col any, i, j int) bool` | 排序比较 |
| `colEstBytesPerRow(col any) uint64` | 单行内存估算 |
| `colIthVal(col any, i int) any` | 取第 i 行值（WAL 转换） |

**8 个函数替代 10+ 个调用点中散落的 50+ 个 type switch case。**

### 第三层：Entry 间操作 — appendEntryToEntry

```go
func appendEntryToEntry(dst, src *bufferEntry) {
    for name, col := range src.data {
        if existing, ok := dst.data[name]; ok {
            dst.data[name] = colAppend(existing, col)
        } else {
            dst.data[name] = col
        }
    }
    // validity 合并（逻辑不变）
    // tagColumns / schema 继承
    dst.recordCount += src.recordCount
    dst.estimatedBytes += src.estimatedBytes
}
```

替代 `appendTypedBatchToEntry` 和 `prependFlushDataToEntry` 中的 inline type switch。

### 第四层：热路径优化 — convert 直接进 entry

```
之前:
  convertColumnsToTyped()           ← 锁外：分配 typed map + struct，逐列类型转换
  Lock
  appendTypedBatchToEntry()         ← 锁内：拆包，二次遍历 + inline type switch
  Unlock

之后:
  computeColumnSignature()          ← 锁外：只取每列第一个非 nil 类型标签，计算 bufferKey
  Lock
  convertAndAppendToEntry()         ← 锁内：单趟
    └─ 每列：类型转换 → colAppend() 直接追加到 entry.data
  Unlock
```

### 各操作路径数据流

```
┌─ ingest ─────────────────────────────────────────────────────────────┐
│                                                                      │
│  convertAndAppendToEntry(entry, raw columns)                         │
│        │                                                             │
│        ▼                                                             │
│  entry (live in shard.buffers)                                       │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
    ┌─ flush ───┐     ┌─ sort ────┐     ┌── VIEW ──────┐
    │            │     │            │     │               │
    │ entry 取出  │     │ sortEntry  │     │ SinceRefresh  │
    │ ↓          │     │  ByKeys()  │     │ ↓             │
    │ groupByHour│     │ ↓          │     │ 返回 *entry   │
    │ ↓          │     │ colSlice   │     │ (snapshot)    │
    │ colSlice   │     │ colPermute │     │ ↓             │
    │ ↓          │     │            │     │ DuckDB VIEW   │
    │ colAppend  │     │ 返回 *entry │     │ 增量刷新      │
    │ sortEntry  │     │ (sorted)   │     │               │
    │  ByKeys()  │     │            │     │               │
    │ ↓          │     └────────────┘     └───────────────┘
    │ Arrow 构建 │
    │ Parquet 写 │
    └────────────┘
```

## 改动清单

### 删除

| 删除项 | 说明 |
|---|---|
| `TypedColumnBatch` 类型定义 | 合并进 `bufferEntry` |
| `convertColumnsToTyped()` | 逻辑并入 `convertAndAppendToEntry` |

### 新增

| 新增项 | 行数估计 | 说明 |
|---|---|---|
| 8 个集中派发函数 | ~80 | `colLen`/`colAppend`/`colSlice`/`colPermute`/`colLess`/`colEstBytesPerRow`/`colIthVal`/`colMake` |
| `appendEntryToEntry()` | ~30 | 替代 `appendTypedBatchToEntry` |
| `convertAndAppendToEntry()` | ~100 | 合并 convert + append |
| `computeColumnSignature()` | ~30 | 轻量签名推断（锁外） |

### 修改

| 修改项 | 行数估计 | 说明 |
|---|---|---|
| `bufferEntry` 类型 | ±2 | 无结构变化，仅注释更新 |
| `flushTask` | ~5 | 用 `*bufferEntry` 替代分散的 5 个字段 |
| `flushWorker` | ~10 | 直接传 entry，去掉内联 TypedColumnBatch 包装 |
| `evictOldestEntries` | ~5 | 同上 |
| `flushBufferLocked` | ~5 | 同上 |
| `flushCandidate` | ~5 | 同上 |
| `flushPartitionedData` | ~10 | 签名改为 `*bufferEntry` |
| `sortBufferEntryByKeys` (曾: sortTypedColumnBatchByKeys) | ~5 | 签名改为 `*bufferEntry` |
| `sliceBufferEntryByIndices` (曾: sliceTypedColumnBatchByIndices) | ~5 | 同上 |
| `prependFlushData` / `prependFlushDataToEntry` | ~20 | inline switch → `colAppend` + `appendEntryToEntry` |
| `estimateBytesPerRow` / `estimateBytesFromData` | ~10 | inline switch → `colEstBytesPerRow` |
| `sliceColumnsByIndices` | ~20 | inline switch → `colSlice` |
| `applyPermutation` | ~20 | inline switch → `colPermute` |
| `compareMultiKeyCached` | ~15 | inline switch → `colLess` |
| `sortColumnsByKeysWithPermutation` | ~5 | `colLen` |
| `typedBatchToWALRecords` | ~20 | inline switch → `colIthVal` |
| `SinceRefresh` | ~15 | 返回 `[]*bufferEntry`，inline switch → `colSlice` |
| `getColumnSignature` | ~10 | inline switch → 首值 Go 类型判断 |
| `inferSchema` | ~10 | inline switch → 首值 Go 类型判断 |
| `writeColumnarInternal` | ~15 | 调用 `convertAndAppendToEntry` |
| `WriteParquetColumnar` | ~10 | 适配 entry.data |
| `appendBatchToTable` (arrow_view.go) | ~10 | inline switch → 集中函数 |
| `buildCreateTableSQL` (arrow_view.go) | ~5 | 字段映射 |
| `SinceRefresh` callers (arrow_view.go) | ~5 | `[]*bufferEntry` |
| **合计** | **~480 行** | |

## 不改

- `flushPartitionedData` 核心逻辑 — 只签名适配
- `groupByHour` — 只读 time 列
- `generateStoragePath` — 不受影响
- 所有 parser（msgpack / lineprotocol / tle）
- WAL 层
- sort 算法、partition 策略、flush 策略

## 基线 & 目标

基于 `feat/growing-columnar-buffer`：

| Benchmark | 当前 | 目标 |
|---|---|---|
| Ingest_WriteBuffer/batch=100 | 3935 ns/op, 20 allocs/op | ~3400 ns/op, ~17 allocs/op |
| Ingest_WriteBuffer/batch=10000 | 185620 ns/op, 72 allocs/op | ~176000 ns/op, ~69 allocs/op |
| ConcurrentWrites/32_shards | 3211 ns/op, 124 allocs/op | ~2900 ns/op, ~121 allocs/op |

## 风险

- **回退成本低**：只改 2 个文件，无新类型引入，`any` 与 `interface{}` 语义等价
- **Merge 冲突**：`bufferEntry` 和 `TypedColumnBatch` 的合并涉及 flushTask 等结构体字段，需一次性改完所有引用点
- **测试适配**：测试中大量构造 `TypedColumnBatch` 的地方需改为 `bufferEntry`，量虽大但机械

## 不做

- 不改 parser 层
- 不改 WAL 层
- 不引入新类型包装（columnSlice 等）
- 不改变 `bufferEntry` 的 shard 锁策略
