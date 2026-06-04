# IotEdgeDB 数据流程架构

## 目录

1. [总览](#总览)
2. [阶段 1：HTTP 接入与协议解码](#阶段-1http-接入与协议解码)
3. [阶段 2：ArrowBuffer 内存缓冲](#阶段-2arrowbuffer-内存缓冲)
4. [阶段 3：Flush 执行与 Parquet 写入](#阶段-3flush-执行与-parquet-写入)
5. [阶段 4：WAL 耐久性层](#阶段-4wal-耐久性层)
6. [阶段 5：Compaction 压缩合并](#阶段-5compaction-压缩合并)
7. [阶段 6：查询路径](#阶段-6查询路径)
8. [企业版扩展架构](#企业版扩展架构)
9. [关机与故障恢复](#关机与故障恢复)
10. [性能设计要点](#性能设计要点)

---

## 总览

IotEdgeDB 是一个基于 DuckDB 构建的高性能时间序列数据库（Go 语言实现）。其数据流架构以 **列式 Arrow 缓冲区** 为中心，从上到下依次经过：HTTP 接入层 → 协议解码 → 内存缓冲 → WAL 持久化 → Parquet 写入存储 → Compaction 压缩合并 → 查询执行。

```
                          HTTP API Layer (Fiber)
   ┌─────────────────────┬──────────────────┬──────────────────┐
   ▼                     ▼                  ▼                  ▼
MessagePack         Line Protocol        TLE            Import(Bulk)
 Decoder              Parser          Stream Parser      CSV/Parquet
(1860万rec/s)       (370万rec/s)
   │                     │                  │                  │
   └─────────┬───────────┴──────────────────┴──────────────────┘
             ▼
   ┌─────────────────────────────────────────────┐
   │        ArrowBuffer (锁分片内存缓冲)           │
   │   ┌──────┐ ┌──────┐ ┌──────┐    ┌──────┐   │
   │   │Shard0│ │Shard1│ │Shard2│ ...│ShardN│   │
   │   └──────┘ └──────┘ └──────┘    └──────┘   │
   └───────────────────┬─────────────────────────┘
                       │
          ┌────────────┴────────────┐
          ▼                         ▼
   ┌─────────────┐         ┌──────────────────┐
   │  WAL Writer │         │  Flush Workers   │
   │  (耐久性)    │         │  (合并→排序→分区)  │
   └─────────────┘         └────────┬─────────┘
                                    ▼
                          ┌────────────────────┐
                          │   Storage Backend   │
                          │ Local / S3 / Azure  │
                          └─────────┬──────────┘
                                    ▼
                          ┌────────────────────┐
                          │    Compaction       │
                          │  (子进程内存隔离)    │
                          │ Hourly → Daily Tier │
                          └────────────────────┘
```

**核心性能指标：**

| 协议 | 吞吐量 | p50 延迟 | p99 延迟 |
|------|--------|---------|---------|
| MessagePack 列式 | 1860 万 rec/s | 0.46 ms | 3.68 ms |
| MessagePack + Zstd | 1680 万 rec/s | 0.55 ms | 3.23 ms |
| Line Protocol | 370 万 rec/s | 2.63 ms | 10.63 ms |

---

## 阶段 1：HTTP 接入与协议解码

### 1.1 接入架构

所有数据写入入口统一在 `internal/api/` 下，通过 Fiber HTTP 框架注册路由：

| 端点 | 处理器 | 特性 |
|------|--------|------|
| `/api/v1/write/msgpack` | `MsgPackHandler` | 最快路径，列式零拷贝 |
| `/api/v1/write/lp` | `LineProtocolHandler` | InfluxDB 兼容 |
| `/api/v1/write/tle` | `TLEHandler` | 流式 JSON Lines |
| `/api/v1/import` | `ImportHandler` | CSV / Parquet 批量导入 |

每个处理器接收请求后：
1. 可选解压缩（gzip / zstd / snappy）
2. 调用对应的协议解析器
3. 将解析结果传递给 `ArrowBuffer`

### 1.2 MessagePack 解码（最快路径）

源码位置：`internal/ingest/msgpack.go`

**解码流程：**

```
MessagePack Binary Data
    │
    ▼
msgpack.Unmarshal → 通用 interface{}
    │
    ├── map[string]interface{}  ──→  decodeMapPayload()
    │       ├── 检测 batch 格式: {"batch": [...子元素...]}  → 递归解码
    │       └── 解析 MsgPackPayload: {m, t, h, f, fields, tags, columns}
    │               ├── columns 存在  ──→  decodeColumnar() 【列式零拷贝路径】
    │               └── columns 不存在 ──→  decodeRow()     【行式回退路径】
    │
    └── []interface{}  ──→  批量处理，每个元素递归 decodeMapPayload()
```

**列式路径（decodeColumnar）详细过程：**

```
1. 提取 measurement (m 字段)
2. 验证所有列数组长度一致
3. 确保 time 列存在（缺失时自动生成 UTC 微秒时间戳）
4. 时间戳归一化:
   - 秒级 (10 位)    × 1,000,000 → 微秒
   - 毫秒级 (13 位)   × 1,000     → 微秒
   - 微秒级 (16 位)   保持不变
   - 纳秒级 (19 位)   ÷ 1,000     → 微秒
5. UTF-8 字符串净化（防止 DuckDB 查询失败）
6. 返回 ColumnarRecord{RawPayload: originalBytes} 【零拷贝 WAL】
```

**关键优化：**

- `RawPayload` 保留原始 MessagePack 字节，WAL 写入时无需重新序列化
- 时间戳归一化使用单次遍历，就地修改避免分配新 slice
- `sync.Pool` 复用解压缩缓冲区（256KB 初始，> 1MB 丢弃）

### 1.3 Line Protocol 解码

源码位置：`internal/ingest/lineprotocol.go`

Line Protocol 按行解析，直接构建预类型化的 `TypedColumnBatch`，绕过通用的 `[]interface{}` 类型转换路径。因为 Line Protocol 的字段类型在解析时已知（int64、float64、string、bool），可以避免运行时的类型推断开销。

```
Line Protocol 文本
    → 预验证 UTF-8 (不同于 MsgPack 二进制格式)
    → 逐行解析: measurement,tag1=val1,tag2=val2 field1=100,field2=0.5 timestamp
    → 构建 TypedColumnBatch{Data: map[string]typedArray}
    → ArrowBuffer.WriteTypedColumnarDirect()  // 绕过 convertColumnsToTyped
```

---

## 阶段 2：ArrowBuffer 内存缓冲

源码位置：`internal/ingest/arrow_writer.go:730-1780`

ArrowBuffer 是整个写入路径的核心数据结构，负责数据缓冲、Schema 管理、批量 Flush 和内存优化。

### 2.1 锁分片设计

```
bufferKey = database + "/" + measurement   // 例: "default/cpu_metrics"
shardIndex = FNV-1a(bufferKey) % shardCount  // 默认 32 shards
```

每个 shard 拥有独立的 `sync.RWMutex`，不同 measurement 的写入可以并行进行。FNV-1a hash 确保均匀分布。

```go
type bufferShard struct {
    buffers            map[string][]interface{}    // bufferKey → 批量数据
    bufferStartTimes   map[string]time.Time        // bufferKey → 创建时间
    bufferRecordCounts map[string]int              // bufferKey → 行数（增量累加）
    bufferSchemas      map[string]string           // bufferKey → 列签名（Schema 演化）
    mu                 sync.RWMutex
}
```

### 2.2 写入流程

`writeColumnarInternal()` 完整路径：

```
Step 1: 构造 bufferKey = database + "/" + measurement

Step 2: WAL 写入 (如果启用且非恢复模式)
    ├── 零拷贝路径: 有 RawPayload (MessagePack 来源)
    │       → wal.AppendRawWithMeta(database, rawBytes)
    │       → WAL 失败不阻塞写入 (WAL 是耐久性优化，非正确性保证)
    │       → 区分反压丢弃 (ErrWALDropped) 和真实 I/O 错误
    └── 回退路径: 无 RawPayload (LineProtocol 等)
            → columnarToWALRecords() 列转行
            → wal.Append(records)

Step 3: 类型转换 convertColumnsToTyped()
    ├── 快速路径 (零拷贝): 同质且无 nil 的 []int64/[]float64/[]string/[]bool
    ├── 慢速路径: 含 nil 或混合类型 → 逐元素转换 + 构建 validity bitmap
    └── Decimal128 路径: 按配置 precision/scale 转换

Step 4: Schema 演化检测 flushOnSchemaChangeLocked()
    ├── 计算 getColumnSignature() → "col1:i64,col2:f64,time:i64"
    ├── 与 shard.bufferSchemas[bufferKey] 比较
    ├── 不一致则先 flush 旧 Schema 数据
    └── 最多迭代 8 次 (schemaEvolutionMaxIters)
        超过 → 返回 ErrSchemaChurnExceeded → HTTP 503

Step 5: 追加数据到 shard.buffers[bufferKey]
    ├── 首次创建 → 更新 bufferStartTimes + 通知 periodicFlush
    └── 增量累加 bufferRecordCounts

Step 6: 大小检查
    totalBuffered >= MaxBufferSize → 提取副本 → delete 清空 → 入队 flushQueue
```

### 2.3 Flush 触发机制

**两种触发方式：**

| 触发方式 | 条件 | 执行路径 |
|---------|------|---------|
| **大小触发** | buffer 行数 ≥ `MaxBufferSize` | 非阻塞入队 `flushQueue` → worker pool |
| **时间触发** | buffer 存活 ≥ `MaxBufferAgeMS` | `periodicFlush()` goroutine 自适应定时器 |

**自适应定时器设计：**

```
periodicFlush() 循环:
    │
    ├── newBufferCh 信号 (新 buffer 创建)
    │   └── computeNextFlushDeadline()
    │       扫描所有 shard → 找最老的 bufferStartTime
    │       → 返回其到期时间 (now + maxAge)
    │       如果早于当前预定 → 重新设置 timer
    │       ★ 优化: 空闲→活跃转换才触发扫描，已有 buffer 时不扫描
    │
    └── flushTimer.C 到期
        └── flushAgedBuffers()
            遍历所有 shard → 检查每个 buffer 的 age
            → age >= maxAge → flushBufferLocked() 同步 flush
            → 重新 arm timer 到下一个最早到期时间
```

### 2.4 Worker Pool

```
flushQueue (buffered channel, 默认容量 100)
    │
    ├── worker 0 ─┐
    ├── worker 1  ─┤
    ├── ...        ─┤── flushRecordsAsync(task)
    ├── worker N-1 ─┘     │
    │                      ├── mergeBatches(records)
    │                      ├── flushPartitionedData()
    │                      └── 错误处理: markFlushFailure() → 依赖 WAL 恢复
    │
    └── Queue Full 保护:
        tryEnqueueFlush() 非阻塞发送
        ├── closing flag → 跳过，数据在 WAL
        ├── ctx.Done()  → 跳过，数据在 WAL
        └── default    → 跳过，数据在 WAL
        ★ 写入路径永不阻塞在 flush 上
```

---

## 阶段 3：Flush 执行与 Parquet 写入

### 3.1 批次合并（mergeBatches）

多批 `TypedColumnBatch` 合并为单个批次，三阶段执行：

```
Phase 1: 扫描
    ├── 遍历所有批次统计总行数 (从 time 列获取)
    ├── 收集列名和类型
    └── 检测 sparsity (不同批次可能有不同列)

Phase 2: 预分配
    ├── 每种类型分配精确 totalRows 的数组 (避免 append 扩容)
    └── 需要 validity tracking 时分配 validity bitmaps (默认全 false)

Phase 3: 逐批拷贝
    ├── 按行偏移 copy 各列数据到合并数组
    ├── 按行偏移 copy validity bitmaps
    ├── batch 中没有的列 → validity 保持 false (null)
    └── 优化: 剔除全 valid 的 validity 条目 (节省内存)
```

### 3.2 时间分区（groupByHour）

```
输入: times []int64 (微秒)

for i, t := range times:
    hourID = t / microPerHour          // 整数除法，无 time.Time 分配
    buckets[hourID].indices.append(i)   // 记录行索引，不拷贝数据
    更新 bucket minTime/maxTime
    更新 globalMin/globalMax

★ 单小时快速路径:
    minHour == maxHour → 跳过分割，直接排序 + 写一个文件
    避免不必要的 slice-by-indices 开销

★ 数据时钟检测:
    minTime < now - 7天  → Warn (可能是回填或时钟偏移)
    minTime > now + 1小时 → Warn (可能是时钟偏移)
```

### 3.3 排序（sortTypedColumnBatchByKeys）

```
sortKeys = 用户配置的额外键 + ["time"]  // time 始终在末尾

单键 time 排序 (最常见):
    ├── O(n) 检查是否已排序 (时间序列生产者通常有序)
    │   已排序 → 直接返回，无内存分配
    └── 未排序 → 创建排列索引 → sort.Slice → applyPermutation

多键排序:
    ├── 缓存列指针避免 map 查找开销 (O(n log n) 次比较)
    ├── sort.Slice(indices, compareMultiKeyCached)
    └── applyPermutation 到所有列 + validity bitmaps
```

### 3.4 Parquet 序列化

源码位置：`internal/ingest/arrow_writer.go:525-676`

```
WriteParquetColumnar():
    1. getSchema() ← LRU 缓存 (capacity=1000)
       cacheKey = "measurement:[colNames]:[typeNames]:[tagColumns]"
       未命中 → inferSchema()
         "time" 列 → arrow.Timestamp_us
         其他 int64 → arrow.Int64
         float64/string/bool → 对应 Arrow 类型
         Decimal128 → 带 precision/scale 的 Decimal128Type
         Metadata: iedb:tags (压缩去重) + iedb:decimals

    2. 构建 Arrow Arrays
       ├── 共享 allocator (memory.NewGoAllocator, 线程安全)
       ├── 零拷贝: []int64 → []arrow.Timestamp (unsafe.Pointer)
       └── 逐列构建 Builder → AppendValues → NewArray

    3. writeRecordToParquet()
       ├── NewRecord(schema, arrays, -1)
       ├── pqarrow.NewFileWriter (预构建 WriterProperties, 不可变)
       ├── writer.Write(record)
       └── → []byte (in-memory buffer)
```

### 3.5 存储写入

```
存储路径格式:
{database}/{measurement}/{YYYY}/{MM}/{DD}/{HH}/{measurement}_{timestamp}_{nanos}.parquet

示例:
default/cpu/2026/05/29/14/cpu_20260529_143052_123456789.parquet

此分层结构支持 DuckDB 目录级分区裁剪:
  read_parquet('bucket/db/cpu/2026/11/*/*/*.parquet')  -- 整个月
  read_parquet('bucket/db/cpu/2026/11/25/*/*.parquet')  -- 某一天
  read_parquet('bucket/db/cpu/2026/11/25/16/*.parquet') -- 某小时

写入流程:
    1. sha256.Sum256(parquetData) → hex 校验和
    2. storage.Write(ctx, storagePath, parquetData)
    3. registerFileInTiering():
       a. tieringManager.RecordFile() → 热层元数据
       b. fileRegistrar.RegisterFile() → 集群 Raft manifest (企业版)
```

---

## 阶段 4：WAL 耐久性层

### 4.1 文件格式

```
WAL 文件头 (7 bytes):
┌──────────┬──────────┬───────────┐
│ Magic(4) │ Ver(2)   │ CkType(1) │
│ "IEDB"   │ 0x0001   │ CRC32=0x01│
└──────────┴──────────┴───────────┘

每条 Entry (16 + N bytes):
┌────────────┬───────────────┬──────────────┬──────────────────┐
│ Length(4)  │ Timestamp(8)  │ CRC32(4)     │ Payload(N bytes) │
└────────────┴───────────────┴──────────────┴──────────────────┘

Envelope 格式 (支持 database 元数据):
┌────────┬───────────┬─────────────┬───────────────────────────┐
│ 0x01   │ dbLen(2)  │ dbName(N)   │ Original MsgPack bytes    │
└────────┴───────────┴─────────────┴───────────────────────────┘
```

### 4.2 写入策略

```go
// 零拷贝路径 (MessagePack 来源)
wal.AppendRawWithMeta(database, rawMsgPackBytes)
// → 封装 envelope 后异步写入

// 回退路径 (LineProtocol 等)
columnarToWALRecords() → wal.Append(records)
// → MsgPack 序列化后异步写入

// WAL 错误分类
├── ErrWALDropped: 反压丢弃 (channel 满)
│   → sampled Warn (1次/秒) + 增量 totalWALDropped 计数器
└── 其他 I/O 错误: 真实故障
    → unsampled Error + 增量 totalWALErrors 计数器
```

### 4.3 恢复架构

```
启动时恢复:
    WAL Recovery → 扫描 WAL 目录中的段文件
    ├── 按时间顺序读取每个段文件
    ├── 跳过当前活跃 segment (正在写入的)
    ├── 逐条 CRC32 校验
    ├── recoveryCallback → WriteColumnarDirectNoWAL() (跳过二次 WAL 写入)
    └── columnarCallback → 列式条目零拷贝恢复

运行时恢复 (存储故障):
    Periodic WAL Maintenance Goroutine (configurable interval)
        ├── arrowBuffer.HasFlushFailure()?
        │   ├── Yes: PurgeOlderThan(safeAge) → RecoverWithOptions()
        │   │       → replay 后 ResetFlushFailure()
        │   └── No:  PurgeOlderThan(safeAge) → 正常清理
        │
        └── safeAge = MaxBufferAgeMS × 3 (默认 ≥ 30s)
            确保数据已通过正常 flush 周期写入 Parquet
```

---

## 阶段 5：Compaction 压缩合并

### 5.1 子进程隔离设计

Compaction 以独立 OS 子进程运行，原因：DuckDB 在查询后通过 Go GC 无法回收 C 分配的内存。子进程退出时 OS 强制回收所有内存。

```
父进程 (iedb):
    ├── Compaction Scheduler (cron)
    │   └── 触发时:
    │       1. 编译 Job 配置 JSON
    │       2. exec: iedb compact --job-stdin < job.json
    │       3. 通过 stdin/stdout JSON 与子进程通信
    │       4. 解析结果 → 更新 manifest → 删旧文件
    │          → OnCompactionComplete callback:
    │             ├── db.ClearHTTPCache() (本地 DuckDB)
    │             ├── queryHandler.InvalidateCaches()
    │             └── 通知集群其他节点刷新缓存
    │
    └── Completion Watcher (企业版集群)
        轮询 completionDir → 处理子进程写入的完成清单

子进程 (iedb compact):
    ├── json.Unmarshal stdin → SubprocessJobConfig
    ├── compaction.RunSubprocessJob(&cfg)
    │   ├── 打开 DuckDB 连接
    │   ├── 多键排序 + 去重
    │   ├── 写入合并 Parquet
    │   └── 验证 + 写 completion manifest
    └── json.Encode stdout → 结果 → exit(0)
```

### 5.2 两级压缩

```
Hourly Tier (按小时):
    触发: MinAgeHours + MinFiles (如 0.5h 前，≥3 个文件)
    操作: 合并同小时内的多个小 Parquet
    优势: 低延迟清理碎片，查询更快

Daily Tier (按天):
    触发: MinAgeHours + MinFiles (如 48h 前，≥2 个文件)
    操作: 合并已被 Hourly 压缩过的日级文件
    优势: 更大粒度合并，进一步减少文件数和存储

压缩效果 (实测):
┌──────────┬──────────┬──────────┬──────────┐
│ 指标     │ 压缩前   │ 压缩后   │ 减少率   │
├──────────┼──────────┼──────────┼──────────┤
│ 文件数   │ 43       │ 1        │ 97.7%    │
│ 大小     │ 372 MB   │ 36 MB    │ 90.4%    │
└──────────┴──────────┴──────────┴──────────┘
```

### 5.3 去重策略

Compaction 利用 Parquet Schema Metadata 中存储的 `iedb:tags` 字段识别哪些列是 tag 列，按 tag 列 + time 进行去重，保留最新的记录。

---

## 阶段 6：查询路径

源码位置：`internal/api/query.go`，`internal/query/parallel_executor.go`

### 6.1 查询流程

```
Client SQL Query
    │
    ▼
QueryHandler.HandleQuery()
    │
    ├── Step 1: SQL 解析和表引用提取
    │   正则匹配:
    │   ├── FROM db.table / FROM table
    │   ├── JOIN db.table / JOIN table (含 LATERAL)
    │   ├── CTE 名称提取 (排除，不转换为存储路径)
    │   └── skipPrefixes 过滤: read_parquet, information_schema, pg_, duckdb_
    │
    ├── Step 2: 存储路径转换
    │   database.measurement → {bucket}/{database}/{measurement}/*/*/*/*/*.parquet
    │
    ├── Step 3: 分区发现和裁剪
    │   ├── storage.List(prefix) → 发现所有 Parquet 文件
    │   └── 从 WHERE time 条件提取时间范围
    │       → 过滤匹配的 year/month/day/hour 目录
    │       → 排除不相关分区 (partition pruning)
    │
    ├── Step 4: SQL 重写
    │   ├── FROM table → FROM read_parquet([path1, path2, ...], options)
    │   ├── time_bucket() → 原生 DuckDB 函数 (提取引用列名用)
    │   └── date_trunc() → 原生 DuckDB 函数
    │
    ├── Step 5: 权限和治理
    │   ├── RBAC: CheckPermissionsBatch() 对涉及的 db.measurement
    │   └── Governance: 速率限制 + 行数限制 + 并发限制
    │
    ├── Step 6: 查询执行
    │   ├── 分区数 >= MinPartitionsForParallel(默认3)?
    │   │   └── Yes → ParallelExecutor
    │   │       ├── Semaphore 控制并发 (默认 MaxConcurrent=4)
    │   │       ├── 每个分区独立查询 + UNION ALL 合并
    │   │       └── 跟踪到 QueryRegistry (企业版查询管理)
    │   └── No → 直接执行单条 DuckDB SQL
    │
    └── Step 7: 结果返回
        ├── Arrow IPC: 零拷贝流式传输, 2x 吞吐量 vs JSON
        ├── JSON: 兼容性模式
        └── 慢查询日志 (≥ SlowQueryThresholdMs)
```

### 6.2 并行执行器

```go
type ParallelExecutor struct {
    db     *sql.DB
    sem    chan struct{}  // Semaphore 并发控制
}
```

每个分区查询在独立 goroutine 中执行，通过 semaphore 限制并发数防止耗尽 DuckDB 连接池。结果通过 `UNION ALL` 合并返回。

---

## 企业版扩展架构

### 集群模式（Hashicorp Raft）

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Writer     │     │   Reader     │     │  Compactor   │
│              │     │              │     │              │
│ 接受写入      │────→│ 服务查询      │     │ 运行压缩      │
│ WAL 复制      │     │ 对等拉取文件  │     │ 写完成清单     │
└──────────────┘     └──────────────┘     └──────────────┘
        │                     │                     │
        └─────────────────────┼─────────────────────┘
                              │
                    ┌─────────────────┐
                    │   Raft Cluster  │
                    │  (共识 + FSM)   │
                    └─────────────────┘

节点角色分离:
  Writer:    接受写入 + WAL 复制到其他 Writer
  Reader:    只读查询 + 从其他节点拉取文件
  Compactor: 执行 Compaction + 通知缓存刷新

请求转发:
  Reader 收到写请求 → 转发到 Writer
  Compactor 收到查询 → 转发到 Reader/Writer
```

### 冷热分层存储

```
Hot Tier (本地 SSD)
    │
    │  根据策略自动迁移 (如 data_age > 30天)
    ▼
Cold Tier (S3 / Azure Blob)

查询自动路由:
  查询涉及的数据 → 检查 tiering metadata
    ├── 在 Hot → 直接读取本地 Parquet
    ├── 在 Cold → DuckDB httpfs 直接读取 S3/Azure
    └── 跨层 → 合并来自两层的 read_parquet 结果
```

---

## 关机与故障恢复

### 关机顺序

源码位置：`internal/shutdown/shutdown.go`

```
Priority 10:  HTTP Server              停止接受新请求
Priority 20:  Ingest / MQTT            排空写入缓冲
Priority 30:  ArrowBuffer              排空内存缓冲 → FlushAll → 写入 Storage
Priority 35:  WAL Purge                清除已持久化的 WAL 段文件
Priority 40:  WAL Close                关闭 WAL 文件句柄
Priority 45:  Storage Close            关闭存储连接
Priority 50:  Compaction / Cluster     停止 Raft 协调器
```

### 故障场景

| 故障 | 保护机制 |
|------|---------|
| 进程崩溃 | WAL 中未 Flush 的数据在重启时恢复 |
| 存储临时不可用 (S3 故障) | Flush 失败 → WAL 保留数据 → Periodic Recovery 重试 |
| Flush Queue 满 | 数据保留在 WAL，非阻塞返回，不丢数据 |
| Schema 演化风暴 | 最多 8 次循环 → 503 → 上游重试 |
| 子进程 Compaction 崩溃 | 父进程检测 exit code + temp dir 清理 |
| 时钟偏移 | 数据 timestamp > 1h 未来或 < 7 天前 → Warn 告警 |

---

## 性能设计要点

### 零拷贝路径汇总

| 优化点 | 技术 |
|--------|------|
| MsgPack → WAL | `RawPayload` 直传，不重新序列化 |
| []int64 → []arrow.Timestamp | `unsafe.Pointer` 类型转换 |
| 同质 []int64 列 | 单次类型断言遍历，无逐元素判断 |
| Line Protocol → Buffer | 预类型化 `TypedColumnBatch` 跳过类型推断 |
| 共享 Arrow Allocator | `memory.NewGoAllocator()` 线程安全共享 |

### 内存管理

| 机制 | 说明 |
|------|------|
| `sync.Pool` 解压缩缓冲 | 256KB 初始, > 1MB 丢弃避免内存膨胀 |
| `sync.Pool` gzip Reader | 复用 32KB 内部解压状态 |
| LRU Schema 缓存 | 容量 1000, 通常 < 5KB 内存占用 |
| Shard 锁分片 | 32 shards → N × 并发度提升 |
| 预分配合并数组 | 精确 totalRows → 避免 append 扩容 + GC |
| Flush 后立即 nil | 批量清空引用帮助 GC 回收 |

### 存储效率

| 特性 | 效果 |
|------|------|
| 分层目录分区 | DuckDB 目录级分区裁剪，跳过不相关数据 |
| Parquet 压缩 (Zstd/Snappy/Gzip) | 典型 90%+ 压缩率 |
| 按 time 排序写入 | 最大化 Parquet 列编码效率 |
| Compaction 去重 | 同一 tag 组合保留最新记录 |
| Tiered Storage | 冷数据迁移到低成本 S3/Azure |
