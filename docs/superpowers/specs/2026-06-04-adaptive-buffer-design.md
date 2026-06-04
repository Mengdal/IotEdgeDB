# 自适应内存缓冲与缓冲可查询设计

**日期**: 2026-06-04  
**状态**: 设计完成，待评审

---

## 概述

将固定大小的写入缓冲改造为**内存压力驱动自适应的智能缓冲**，同时通过 DuckDB Arrow C Data Interface 将缓冲数据**零拷贝暴露**给查询引擎，实现写入立即可见。

### 改造前后对对比

```
改造前：
  写入 → WAL → 缓冲(固定50k条/5秒) → 刷盘 → Parquet文件 → DuckDB查询

改造后：
  写入 → WAL → 缓冲(自适应,最大15分钟) → 刷盘 → Parquet文件 ──┐
              │                                                 ├→ DuckDB查询
              └→ Arrow VIEW (零拷贝,增量刷新) ──────────────────┘
```

### 核心原则

- WAL 不动（持久性 + 崩溃恢复）
- 存储不动、压不动
- 写入路径最小改动
- 查询路径透明改写

---

## 核心决策

| 决策项 | 选择 |
|---|---|
| 自适应策略 | 内存压力驱动 + 写入速率辅助，两者结合 |
| 查询语义 | 强一致性：写入立即可见 |
| WAL 定位 | 保留 WAL 作为崩溃恢复机制（双层保障） |
| Arrow VIEW 刷新 | 常驻 VIEW + 写入时增量刷新 |
| 并发安全 | 利用 batch 不可变性，shard RWMutex 仅保护列表操作 |
| 内存安全 | 默认自动检测 + 用户可手动覆盖 |
| 配置复杂度 | 最小化（6 个新参数） |
| 刷盘决策 | 内存压力驱动，优先刷 recordCount 最大的 measurement，15 分钟兜底 |

---

## 架构

### 改造后数据流

```
写入请求（LP/MsgPack/TLE）
    │
    ▼
解析 & 类型转换（现有逻辑，不改）
    │
    ├──→ WAL 写入（现有逻辑，不改）
    │       └── 崩溃恢复时回放到缓冲
    │
    ▼
缓冲追加（per-measurement, shard 分片, 合并为 bufferEntry struct）
    │
    ├──→ 通知 Arrow VIEW 管理器：有新数据
    │       └── 100ms 合并窗口批量增量刷新 DuckDB Arrow VIEW
    │
    └──→ 全局内存监控器 每秒评估压力等级
            │
            ├── 绿色：不干预（仅 15 分钟兜底）
            ├── 黄色：按 recordCount 降序刷最大的缓冲
            └── 红色：并行激进刷盘（最低保留 min_buffer_memory）
                    │
                    ▼
              走已有 flush queue（非阻塞）
                    │
                    ▼
              刷盘到 Parquet → 通知 VIEW 管理器重建（清空已刷数据）
```

### 查询数据流

```
用户查询 SQL
    │
    ▼
查询改写器 → 检测缓冲是否有数据
    │
    ├── 无缓冲数据 → 原始路径（read_parquet only）
    │
    └── 有缓冲数据 → CTE 改写：
            WITH _source AS (
              SELECT * FROM read_parquet('...')
              UNION ALL
              SELECT * FROM _iedb_buffer_measurement
            )
            <user original query>
    │
    ▼
DuckDB 引擎
    ├── read_parquet(files)         ← 已刷盘数据
    └── _iedb_buffer_measurement   ← 缓冲数据（零拷贝 Arrow）
    │
    ▼
返回结果（Arrow IPC / sql.Rows）
```

---

## 跨包通信：BufferChangeNotifier 接口

`AdaptiveFlushEngine`（`internal/ingest/`）需要通知 `ArrowViewManager`（`internal/database/`）刷盘完成和新增数据。直接引用会造成循环依赖。通过在 `ingest` 包定义接口解耦：

```go
// internal/ingest/notifier.go

// BufferChangeNotifier 由 database 包实现，接收缓冲变更通知
type BufferChangeNotifier interface {
    // OnFlushComplete 刷盘完成后调用，bufferKey 的缓冲数据已写入 Parquet
    OnFlushComplete(bufferKey string)
    // OnNewData 有新 batch 追加到缓冲后调用
    OnNewData(bufferKey string)
}
```

`ArrowBuffer` 持有 `BufferChangeNotifier` 引用（可选，nil 时通知被跳过）。

`ArrowViewManager` 在 `database` 包中实现此接口，启动时由 `main.go` 注入：

```go
// cmd/iedb/main.go
arrowViewMgr := database.NewArrowViewManager(db, arrowBuffer)
arrowBuffer.SetNotifier(arrowViewMgr)  // ArrowViewManager 实现 BufferChangeNotifier
```

### 通知调用点

| 位置 | 通知 | 说明 |
|---|---|---|
| `writeColumnarInternal` 末尾 | `OnNewData(bufferKey)` | 新 batch 追加后 |
| `flushBufferLocked` / flush worker 完成 | `OnFlushComplete(bufferKey)` | 刷盘完成后 |

---

## 模块一：缓冲数据结构重构

将 `bufferShard` 中 4 个独立 map 合并为单个结构体，减少 hash 查找。

### 合并前

```go
type bufferShard struct {
    mu                  sync.RWMutex
    buffers             map[string][]interface{}
    bufferStartTimes    map[string]time.Time
    bufferRecordCounts  map[string]int
    bufferSchemas       map[string]string
}
```

### 合并后

```go
type bufferEntry struct {
    batches        []*TypedColumnBatch  // 缓冲的数据批次（不可变）
    startTime      time.Time            // 第一条记录到达时间
    recordCount    int                  // 总记录数
    estimatedBytes uint64               // 估算内存占用
    schema         string               // 列签名
    refreshIndex   int                  // Arrow VIEW 增量刷新游标
}

type bufferShard struct {
    mu      sync.RWMutex
    buffers map[string]*bufferEntry  // bufferKey → 一个 struct
}
```

### 收益

- 写入路径：5+ 次 hash 查找 → 1 次
- 刷盘后清理：5 次 delete → 1 次
- 快照收集：遍历一个 map 即可

---

## 模块二：内存监控器

### 结构体

```go
type PressureLevel int

const (
    PressureGreen  PressureLevel = iota  // 可用内存 > green_pct
    PressureYellow                       // 介于 green_pct 和 red_pct 之间
    PressureRed                          // 可用内存 < red_pct
)

type MemoryMonitor struct {
    cfg         MemoryMonitorConfig
    pressure    atomic.Int32        // 当前压力等级，写入线程无锁读取
    totalMemory uint64              // 系统总内存（启动时测一次）
    duckdbLimit uint64              // DuckDB memory_limit
    stopCh      chan struct{}
}

type MemoryMonitorConfig struct {
    MaxBufferMemoryMB int    // 0=自动检测
    MinBufferMemoryMB int    // 最低保证
    GreenPct          int    // 默认 50
    RedPct            int    // 默认 20
    CheckIntervalMS   int    // 默认 1000
}
```

### 自动检测（`MaxBufferMemoryMB = 0`）

```
1. 容器环境：cgroup v2 memory.max / memory.current
   → cgroup v1 fallback: memory.limit_in_bytes / memory.usage_in_bytes
   → 物理机 fallback: /proc/meminfo + runtime.MemStats

2. 缓冲上限 = 系统总内存 × 50% - DuckDB memory_limit

3. 不低于 min_buffer_memory_mb
```

### 压力等级判定

```
可用内存占比 = available / total × 100

 > green_pct  → 绿色
 < red_pct    → 红色
 之间         → 黄色
```

### 监控循环

每秒运行一次，更新 atomic 压力等级。

---

## 模块三：自适应刷盘决策引擎

### 结构体

```go
type AdaptiveFlushEngine struct {
    buffer         *ArrowBuffer
    monitor        *MemoryMonitor
    maxBufferBytes uint64
    minBufferBytes uint64
    maxAge         time.Duration
    candidatesBuf  []flushCandidate  // 预分配复用
}

type flushCandidate struct {
    shardIdx  int
    bufferKey string
    entry     *bufferEntry  // 直接引用，不拷贝字段
}
```

### 主循环

每秒评估一次（改造现有 `periodicFlush`）：

```
1. 遍历所有 shard（读锁），计算缓冲总 estimatedBytes

2. 硬上限检查：totalBytes > maxBufferBytes
   → 按 recordCount 降序刷最大的，直到低于上限

3. 15 分钟兜底：startTime 超时的直接刷（不论压力等级）

4. 压力等级决策：
   绿色 → 不做额外处理
   黄色 → 按 recordCount 降序刷，直到回到绿色
   红色 → 按 recordCount 降序并行刷（每 measurement 保留 minPerMeasurement）
```

### 核心算法

```go
func (e *AdaptiveFlushEngine) collectCandidates() []flushCandidate {
    e.candidatesBuf = e.candidatesBuf[:0]
    for i := 0; i < e.buffer.shardCount; i++ {
        shard := &e.buffer.shards[i]
        shard.mu.RLock()
        for key, entry := range shard.buffers {
            e.candidatesBuf = append(e.candidatesBuf, flushCandidate{
                shardIdx: i, bufferKey: key, entry: entry,
            })
        }
        shard.mu.RUnlock()
    }
    return e.candidatesBuf
}

// 按 recordCount 降序排序
sort.Slice(candidates, func(i, j int) bool {
    return candidates[i].entry.recordCount > candidates[j].entry.recordCount
})
```

### 触发刷盘

走已有 flush queue（非阻塞），复用现有 flush 生命周期。

### 指标暴露

新增指标：

| 指标 | 类型 | 说明 |
|---|---|---|
| `iedb_memory_pressure_level` | gauge | 0=green, 1=yellow, 2=red |
| `iedb_buffer_estimated_bytes` | gauge | 当前缓冲总估算内存 |
| `iedb_adaptive_flush_total` | counter | 自适应触发的刷盘次数 |
| `iedb_hard_limit_flush_total` | counter | 硬上限触发的刷盘次数 |
| `iedb_age_expired_flush_total` | counter | 超时触发的刷盘次数 |
| `iedb_buffer_flush_records_distribution` | histogram | 每次刷盘记录数分布 |

刷盘记录数分布 histogram buckets: `[100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000]`，带 `trigger` label（size/age/hard_limit/manual）。

---

## 模块四：Arrow VIEW 管理器

### 核心思路

利用项目已有的 `duckdb_arrow` build tag 和 DuckDB Arrow C Data Interface，将内存中的 `*TypedColumnBatch` 列表注册为 DuckDB 可查询的 VIEW。

### 结构体

```go
type ArrowViewManager struct {
    db     *sql.DB
    buffer *ingest.ArrowBuffer           // 用于调用 SinceRefresh/MarkRefreshed（database → ingest 单向依赖）
    
    mu       sync.Mutex
    views    map[string]*arrowViewState  // bufferKey → 状态
    
    notifyCh chan string                 // 写入通知 channel
    dirty    map[string]struct{}         // 待刷新集合
    closeCh  chan struct{}
}

// 实现 ingest.BufferChangeNotifier 接口
func (m *ArrowViewManager) OnNewData(bufferKey string) {
    select {
    case m.notifyCh <- bufferKey:
    default:
        // channel 满，下次 refresh 循环全量扫描 dirty
    }
}

func (m *ArrowViewManager) OnFlushComplete(bufferKey string) {
    m.mu.Lock()
    delete(m.views, bufferKey)  // 旧 VIEW 失效
    m.mu.Unlock()
    // 如果刷盘期间有新数据写入，触发重建
    m.OnNewData(bufferKey)
}

type arrowViewState struct {
    arrowArray unsafe.Pointer  // Arrow C Data 句柄
    schema     string           // 列签名
}
```

### 初始化

启动时创建 ArrowViewManager，启动 refreshLoop goroutine。

### 通知路径

写入线程通过 `BufferChangeNotifier.OnNewData(bufferKey)` 通知（`writeColumnarInternal` 末尾调用），非阻塞写入 notifyCh。

刷盘完成后通过 `BufferChangeNotifier.OnFlushComplete(bufferKey)` 通知。

### 刷新循环（100ms 合并窗口）

每 100ms 合并收集期间到达的所有 dirty bufferKey，逐个调用 refreshView()。

### 增量刷新

```
refreshView(bufferKey):
  1. 调用 ArrowBuffer.SinceRefresh(bufferKey) → 获取 refreshIndex 之后的新 batch
  2. 如果没有新 batch → 跳过
  3. mergeBatches(newBatches) → 合并为单个 TypedColumnBatch
  4. 如果已有 VIEW → 检查 schema：
     - schema 相同 → duckdb_arrow_append() 增量追加
     - schema 不同 → 重建 VIEW
  5. 如果没有 VIEW → duckdb_arrow_register() 创建
  6. 调用 ArrowBuffer.MarkRefreshed(bufferKey) → 更新 refreshIndex
```

### 并发安全

- batch 内容不可变，DuckDB 读取不需要锁
- 列表追加/遍历由 shard RWMutex 保护（持锁微秒级）
- VIEW 管理器的内部状态由 `mu sync.Mutex` 保护

### ArrowBuffer 新增方法

```go
// SetNotifier 设置缓冲变更通知回调（由 database 包注入）
func (b *ArrowBuffer) SetNotifier(n BufferChangeNotifier)

// SinceRefresh 返回自上次 VIEW 刷新以来新增的 batch
func (b *ArrowBuffer) SinceRefresh(bufferKey string) ([]*TypedColumnBatch, error)

// MarkRefreshed 更新 refreshIndex
func (b *ArrowBuffer) MarkRefreshed(bufferKey string)
```

---

## 模块五：查询路径改造

### SQL 改写器

```go
type QueryRewriter struct {
    viewMgr *ArrowViewManager
}

func (r *QueryRewriter) Rewrite(userSQL string, measurement MeasurementRef) string {
    return fmt.Sprintf(`
        WITH _source AS (
            SELECT * FROM read_parquet('%s')
            UNION ALL
            SELECT * FROM %s
        ) %s
    `, measurement.PartitionPath(), measurement.ArrowViewName(), userSQL)
}
```

### 查询执行

```go
func (e *ParallelExecutor) ExecuteWithBuffer(ctx context.Context, m MeasurementRef, sql string) (*sql.Rows, error) {
    if e.viewMgr.HasData(m.BufferKey()) {
        rewritten := e.rewriter.Rewrite(sql, m)
        return e.db.QueryContext(ctx, rewritten)
    }
    return e.ExecutePartitioned(ctx, m.Partitions(), sql)
}
```

### 空缓冲优化

`HasData()` 快速判断，无缓冲数据时跳过改写，直接走原始 Parquet 路径，无性能损失。

---

## 改动范围

| 文件 | 改动类型 | 估模 |
|---|---|---|
| `internal/ingest/notifier.go` | **新增** | ~15 行 |
| `internal/ingest/memory_monitor.go` | **新增** | ~150 行 |
| `internal/ingest/adaptive_flush.go` | **新增** | ~200 行 |
| `internal/database/arrow_view.go` | **新增** | ~200 行 |
| `internal/query/query_rewriter.go` | **新增** | ~80 行 |
| `internal/ingest/arrow_writer.go` | 修改 | ~100 行（bufferShard 重构、新增方法） |
| `internal/query/parallel_executor.go` | 修改 | ~50 行 |
| `internal/config/config.go` | 修改 | ~30 行 |
| `internal/metrics/metrics.go` | 修改 | ~40 行 |
| `iedb.toml` | 修改 | ~10 行 |
| `internal/wal/` | **不改** | - |
| `internal/storage/` | **不改** | - |
| `internal/compaction/` | **不改** | - |

---

## 配置

```toml
[ingest]
# === 新增：自适应缓冲 ===
max_buffer_memory_mb = 0            # 0=自动检测（默认），>0=手动指定硬上限
min_buffer_memory_mb = 128          # 最低保证缓冲内存 MB
max_buffer_age_seconds = 900        # 15 分钟兜底，超时强制刷盘
memory_pressure_green_pct = 50      # 可用内存 >50% 绿色
memory_pressure_red_pct = 20        # 可用内存 <20% 红色
memory_check_interval_ms = 1000     # 内存检查间隔

# === 废弃（保留字段，显式配置时 WARN 日志，不报错）===
# max_buffer_size = 50000
# max_buffer_age_ms = 1000
```

---

## 错误处理与边界情况

| 场景 | 处理方式 |
|---|---|
| WAL 恢复 | 恢复数据通过 WriteColumnarDirectNoWAL 写入缓冲，触发增量 VIEW 刷新 |
| Schema 变更 | 现有 schema 检测逻辑刷旧缓冲，VIEW 管理器检测 schema 不同 → 重建 |
| 15 分钟兜底 | 无论压力等级，超时强制刷盘，保证数据最终落地 |
| 空缓冲查询 | HasData() 快速判断，跳过 UNION，无性能损失 |
| VIEW 注册失败 | 重试一次，仍失败记录 error metric，查询退化为仅 Parquet |
| 进程重启 | WAL 回放恢复缓冲 → VIEW 重建 → 查询正常 |
| 配置热加载 | 第一版不支持，配置变更需重启 |

---

## 测试策略

| 层级 | 内容 |
|---|---|
| 单元测试 | 内存监控器（mock cgroup/meminfo）、压力等级判定、自适应决策排序逻辑、SQL 改写正确性 |
| 集成测试 | 写入 → 缓冲 → 查询可见性（UNION 结果校验）、刷盘后数据持久化 + 查询仍可见、WAL 恢复后查询可见性、schema 变更后 VIEW 重建 |
| 性能测试 | 零拷贝 VIEW vs 拷贝方案性能对比、自适应 vs 固定缓冲吞吐对比、高并发写入 + 查询下的锁竞争 |
| 混沌测试 | 内存压力震荡下决策稳定性、bufferKey 竞争（同一 measurement 高频写入 + 查询） |

---

## 后续扩展（不在本设计范围）

- SIGHUP 配置热加载
- 写入速率趋势因子加入决策权重
- Per-measurement 自定义 max_age 覆盖
- DuckDB Arrow VIEW 的持久化快照（加速重启后的查询可用性）
