# IotEdgeDB 系统模块架构

## 目录

1. [架构总览](#架构总览)
2. [模块依赖图](#模块依赖图)
3. [核心模块详解](#核心模块详解)
   - [3.1 配置管理](#31-配置管理-internalconfig)
   - [3.2 数据库层](#32-数据库层-internaldatabase)
   - [3.3 存储后端](#33-存储后端-internalstorage)
   - [3.4 数据摄入](#34-数据摄入-internalingest)
   - [3.5 WAL 耐久层](#35-wal-耐久层-internalwal)
   - [3.6 数据压缩](#36-数据压缩-internalcompaction)
   - [3.7 认证授权](#37-认证授权-internalauth)
   - [3.8 HTTP API 层](#38-http-api-层-internalapi)
   - [3.9 查询引擎](#39-查询引擎-internalquery)
4. [企业版模块](#企业版模块)
   - [4.1 集群协调](#41-集群协调-internalcluster)
   - [4.2 冷热分层](#42-冷热分层-internaltiering)
   - [4.3 许可证管理](#43-许可证管理-internallicense)
   - [4.4 查询治理](#44-查询治理-internalgovernance)
   - [4.5 审计日志](#45-审计日志-internalaudit)
   - [4.6 RBAC 权限](#46-rbac-权限)
   - [4.7 数据对账](#47-数据对账-internalreconciliation)
5. [支撑模块](#支撑模块)
   - [5.1 优雅关机](#51-优雅关机-internalshutdown)
   - [5.2 任务调度](#52-任务调度-internalscheduler)
   - [5.3 备份恢复](#53-备份恢复-internalbackup)
   - [5.4 MQTT 接入](#54-mqtt-接入-internalmqtt)
   - [5.5 遥测采集](#55-遥测采集-internaltelemetry)
   - [5.6 监控指标](#56-监控指标-internalmetrics)
   - [5.7 共享数据模型](#57-共享数据模型-pkgmodels)
6. [启动流程](#启动流程)
7. [模块间交互协议](#模块间交互协议)
8. [许可证特性门控矩阵](#许可证特性门控矩阵)

---

## 架构总览

IotEdgeDB 采用分层模块化架构，共 34 个包（33 个 `internal/` + 1 个 `pkg/`）。系统以 **Arrow Buffer（列式内存缓冲）** 为核心数据枢纽，向上对接多种协议接入，向下桥接存储、压缩和查询。

```
┌────────────────────────────────────────────────────────────────────────┐
│                          HTTP API (Fiber)                              │
│    api/msgpack  api/lineprotocol  api/tle  api/query  api/import       │
│    api/auth     api/rbac         api/backup  api/cluster  api/mqtt     │
│    api/compaction  api/delete  api/databases  api/tiering  ...         │
└────────────┬──────────────┬──────────────┬──────────────┬─────────────┘
             │              │              │              │
    ┌────────▼──────┐ ┌────▼─────┐ ┌──────▼──────┐ ┌─────▼──────┐
    │   auth        │ │ ingest   │ │  scheduler  │ │ governance  │
    │ (token/RBAC)  │ │ (buffer) │ │ (CQ/retent) │ │ (rate限流)   │
    └────────┬──────┘ └────┬─────┘ └──────┬──────┘ └─────┬──────┘
             │              │              │              │
    ┌────────▼──────┐ ┌────▼─────┐ ┌──────▼──────┐ ┌─────▼──────┐
    │   license     │ │   wal    │ │ compaction  │ │  tiering   │
    │ (特性门控)     │ │ (耐久性)  │ │ (子进程合并) │ │ (冷热分层)  │
    └────────┬──────┘ └────┬─────┘ └──────┬──────┘ └─────┬──────┘
             │              │              │              │
             └──────────────┼──────────────┼──────────────┘
                            │              │
                    ┌───────▼──────┐ ┌─────▼──────────┐
                    │   storage    │ │   database      │
                    │ (local/S3/   │ │ (DuckDB pool)   │
                    │  Azure)      │ │                 │
                    └───────┬──────┘ └─────┬───────────┘
                            │              │
                    ┌───────▼──────┐ ┌─────▼──────────┐
                    │   query      │ │ cluster (Raft)  │
                    │ (并行执行器)  │ │ (企业版集群)     │
                    └──────────────┘ └─────────────────┘
```

---

## 模块依赖图

```
api ──────────────────────────────────────────────────────────────┐
 │ 依赖几乎所有模块                                                 │
 ├── auth (token验证 + RBAC)                                       │
 ├── audit (审计中间件)                                             │
 ├── backup ────────→ storage                                      │
 ├── cluster ───────→ config, ingest, license, storage, wal        │
 │   ├── cluster/raft (Hashicorp Raft FSM)                         │
 │   ├── cluster/protocol (TCP消息协议)                             │
 │   ├── cluster/filereplication (对等文件拉取)                     │
 │   ├── cluster/replication (WAL流复制)                            │
 │   ├── cluster/security (HMAC认证, TLS)                          │
 │   └── cluster/sharding (数据分片)                                │
 ├── compaction ────→ storage                                      │
 ├── config (viper, 独立)                                          │
 ├── database ──────→ duckdb-go (独立)                             │
 ├── governance ────→ config, metrics                              │
 ├── ingest ────────→ config, metrics, storage, tiering, wal       │
 ├── license (HTTP phone-home, 独立)                               │
 ├── logger (zerolog, 独立)                                        │
 ├── metrics (Prometheus, 独立)                                    │
 ├── mqtt ──────────→ ingest, pkg/models                           │
 ├── pruning ───────→ storage, sql                                 │
 ├── query ─────────→ database/sql (独立)                          │
 ├── queryregistry (查询追踪, 独立)                                 │
 ├── reconciliation → cluster/raft, storage, audit                 │
 ├── scheduler ─────→ api, license                                 │
 ├── shutdown (信号+优先级, 独立)                                   │
 ├── sql (独立)                                                    │
 ├── storage (Backend接口 + 3实现, 独立)                            │
 ├── telemetry ─────→ net/http (独立)                              │
 ├── tiering ───────→ config, license, storage                     │
 └── wal ───────────→ metrics                                      │

pkg/models (独立, 被 ingest/mqtt/api 引用)
```

---

## 核心模块详解

### 3.1 配置管理 (`internal/config`)

**职责**：统一配置入口，从 TOML 文件和环境变量加载，驱动整个系统的行为。

**核心数据结构：**

```
Config
├── ServerConfig         HTTP 服务配置 (端口/超时/TLS/最大载荷)
├── DatabaseConfig       DuckDB 配置 (连接数/内存/线程/时区)
├── StorageConfig        存储后端选择 + S3/Azure/Local 凭据
├── IngestConfig         摄入缓冲配置
│   ├── MaxBufferSize        缓冲区最大行数 (50000)
│   ├── MaxBufferAgeMS       缓冲区最大存活毫秒 (5000)
│   ├── FlushWorkers         Flush 工作线程数
│   ├── FlushQueueSize       Flush 队列容量
│   ├── ShardCount           Shard 分片数 (32)
│   ├── Compression          压缩算法 (snappy/zstd/gzip)
│   ├── SortKeys / DefaultSortKeys   排序键配置
│   └── DecimalColumns       Decimal128 精度配置
├── CompactionConfig     压缩调度配置
│   ├── HourlyEnabled / DailyEnabled
│   ├── HourlySchedule / DailySchedule  (cron 表达式)
│   ├── HourlyMinAgeHours / DailyMinAgeHours
│   ├── HourlyMinFiles / DailyMinFiles
│   ├── MaxConcurrent           最大并发
│   ├── TempDirectory           临时目录
│   └── CompletionDir           完成清单目录 (集群模式)
├── WALConfig            预写日志配置
│   ├── Enabled / Directory / SyncMode
│   ├── MaxSizeMB / MaxAgeSeconds
│   └── RecoveryBatchSize / RecoveryIntervalSeconds
├── AuthConfig           认证配置 (DBPath/CacheTTL/BootstrapToken)
├── ClusterConfig        集群配置 (Role/Seeds/Raft/Replication/Security)
├── TieredStorageConfig  冷热分层配置 (MigrationSchedule/Cold backend)
├── QueryConfig          查询配置 (Timeout/SlowQueryThreshold/S3Cache)
├── GovernanceConfig     治理配置 (默认速率限制/配额)
├── ReconciliationConfig 对账配置 (Schedule/GraceWindow/MaxDeletes)
├── BackupConfig         备份配置
├── RetentionConfig      保留策略配置
├── ContinuousQueryConfig 连续查询配置
├── MetricsConfig        指标配置
├── AuditLogConfig       审计日志配置
├── TelemetryConfig      遥测配置
└── LicenseConfig        许可证配置
```

**加载机制：**
```
config.Load()
  → viper 读取 TOML 文件 (iedb.toml)
  → viper 合并环境变量 (前缀 IEDB_)
  → viper.Unmarshal → Config 结构体
  → ValidateTLS() 验证证书

环境变量映射:
  IEDB_SERVER_PORT=9000          → Config.Server.Port = 9000
  IEDB_STORAGE_BACKEND=s3        → Config.Storage.Backend = "s3"
  IEDB_INGEST_MAX_BUFFER_SIZE=100000  → Config.Ingest.MaxBufferSize = 100000
```

---

### 3.2 数据库层 (`internal/database`)

**职责**：管理 DuckDB 连接池，提供查询执行、性能分析和缓存管理。

**核心类型：**

```go
type DuckDB struct {
    db     *sql.DB       // 标准 database/sql 连接池
    logger zerolog.Logger
}

type Config struct {
    MaxConnections int    // 最大连接数 (默认 2*CPU)
    MemoryLimit    string // 内存上限 ("8GB")
    ThreadCount    int    // 线程数 (默认 NumCPU)
    EnableWAL      bool   // DuckDB 内部 WAL
    TimeZone       string // 时区
    // S3 httpfs 扩展配置
    S3Region, S3AccessKey, S3SecretKey, S3Endpoint string
    S3UseSSL, S3PathStyle bool
    // Azure 扩展配置
    AzureAccountName, AzureAccountKey, AzureEndpoint string
    // S3 缓存配置
    EnableS3Cache     bool
    S3CacheSize       string
    S3CacheTTLSeconds int
}
```

**关键方法：**

| 方法 | 功能 |
|------|------|
| `New(cfg)` | 创建连接池 → 配置内存/线程 → 安装 httpfs/azure/cache_httpfs 扩展 |
| `Query(sql)` / `QueryContext(ctx, sql)` | 执行查询，返回 `*sql.Rows` |
| `QueryWithProfile(ctx, sql)` | 带性能分析（从 DuckDB profiling JSON 提取 planner/execution timing） |
| `Exec(sql)` | 执行 DDL/DML |
| `ConfigureS3(cfg)` | 运行时重新配置 S3 凭据（用于冷热分层） |
| `ClearHTTPCache()` | 清空 httpfs 缓存 + parquet_metadata_cache（Compaction 后调用） |
| `DB()` | 暴露底层 `*sql.DB`（供 Compaction 子进程使用） |
| `Stats()` | 返回连接池统计 |
| `Close()` | 关闭所有连接 |

**连接池配置：**
- `MaxConnections`：默认 CPU 核数 × 2
- DuckDB 内部 `max_memory`：估算物理内存的 50%
- DuckDB 内部 `threads`：默认 CPU 核数

**扩展支持：**
```sql
-- 启动时执行
INSTALL httpfs;        -- S3/MinIO 文件系统访问
LOAD httpfs;
INSTALL azure;         -- Azure Blob 访问
LOAD azure;
INSTALL cache_httpfs FROM community;  -- S3 文件缓存
LOAD cache_httpfs;
```

---

### 3.3 存储后端 (`internal/storage`)

**职责**：定义统一的存储抽象，支持本地文件系统、S3/MinIO 和 Azure Blob Storage 三种实现。

**核心接口：**

```go
type Backend interface {
    Write(ctx, path, data []byte) error
    WriteReader(ctx, path, reader io.Reader, size int64) error
    Read(ctx, path) ([]byte, error)
    ReadTo(ctx, path, writer io.Writer) error
    ReadToAt(ctx, path, writer io.Writer, offset int64) error  // 断点续传
    StatFile(ctx, path) (int64, error)                          // 返回文件大小
    List(ctx, prefix) ([]string, error)                         // 列出对象
    Delete(ctx, path) error
    Exists(ctx, path) (bool, error)
    Close() error
    Type() string           // "local" / "s3" / "azure"
    ConfigJSON() string     // 序列化配置 (供子进程重建)
}

// 可选扩展接口

type AppendingBackend interface {  // 仅 Local 实现
    Backend
    AppendReader(ctx, path, reader io.Reader, appendSize int64) error
}

type DirectoryLister interface {   // SHOW DATABASES/TABLES
    ListDirectories(ctx, prefix) ([]string, error)
}

type BatchDeleter interface {      // 批量删除 (S3 批量 1000 对象)
    DeleteBatch(ctx, paths []string) error
}

type ObjectLister interface {      // 列出含元数据的对象 (保留策略)
    ListObjects(ctx, prefix) ([]ObjectInfo, error)
}

type DirectoryRemover interface {  // 删除空目录
    RemoveDirectory(ctx, path) error
}

type ObjectInfo struct {
    Path         string
    Size         int64
    LastModified time.Time
}
```

**三种实现：**

| 实现 | 文件 | 适用场景 |
|------|------|---------|
| `LocalBackend` | `local.go` | 单机部署，本地磁盘 |
| `S3Backend` | `s3.go` | AWS S3 / MinIO，对象存储 |
| `AzureBlobBackend` | `azure.go` | Azure Blob Storage |
| `ResilientBackend` | `resilient.go` | 包装任意后端，添加重试和熔断器 |

**存储路径结构：**
```
{bucket}/{database}/{measurement}/{YYYY}/{MM}/{DD}/{HH}/{measurement}_{timestamp}_{nanos}.parquet
             ↑
          (可配置 prefix)
```

**ResilientBackend 设计：**
```
ResilientBackend 包装原始 Backend
  ├── 每个操作最多重试 3 次
  ├── 指数退避 (100ms → 200ms → 400ms)
  └── 电路熔断器 (连续 5 次失败 → 熔断 30s)
```

---

### 3.4 数据摄入 (`internal/ingest`)

这是系统的**核心数据枢纽**。所有数据写入都经过此模块。

#### 3.4.1 模块结构

```
internal/ingest/
├── arrow_writer.go    ← ArrowBuffer (缓冲核心) + ArrowWriter (Parquet序列化)
├── msgpack.go         ← MessagePack 解码器
├── lineprotocol.go    ← Line Protocol 解析器 (InfluxDB 兼容)
├── tle.go             ← TLE 流式解析器
├── utf8.go            ← UTF-8 验证接口
├── utf8_sanitize.go   ← 通用 UTF-8 净化
└── utf8_simdutf.go    ← SIMD 加速 UTF-8 验证
```

#### 3.4.2 ArrowBuffer

```go
type ArrowBuffer struct {
    config          *config.IngestConfig
    storage         storage.Backend       // 写入目标
    writer          *ArrowWriter          // Parquet 序列化器
    wal             WALWriter             // 可选 WAL
    tieringManager  *tiering.Manager      // 可选: 分层存储元数据
    fileRegistrar   FileRegistrar         // 可选: 集群文件清单

    // 锁分片
    shards     []*bufferShard             // 默认 32 个
    shardCount uint32

    // Worker Pool
    flushQueue   chan flushTask           // 有界通道
    flushWorkers int                      // 工作线程数

    // 时间触发 flush
    flushTimer    *time.Timer             // 自适应定时器
    flushDeadline time.Time               // 下次 flush 到期时间
    newBufferCh   chan struct{}           // 唤醒信号

    // 原子计数器 (无锁指标)
    totalRecordsBuffered / totalRecordsWritten / totalFlushes / totalErrors ...
}
```

**锁分片设计：**
```
bufferShard (N = 32):
├── buffers map[bufferKey][]interface{}        // "db/measurement" → 批次数列
├── bufferStartTimes map[bufferKey]time.Time   // 每个 buffer 的创建时间
├── bufferRecordCounts map[bufferKey]int       // 增量计数 (避免 O(n) 扫描)
├── bufferSchemas map[bufferKey]string         // 列签名 ("col:i64,time:i64")
└── mu sync.RWMutex

FNV-1a("database/measurement") % 32 → shard index
```

**Flush Worker Pool 架构：**
```
                    ┌──────────────────┐
   writeColumnar()  │  flushQueue      │
   ─────────────────→  (容量 100)       │
  (非阻塞发送)       └───────┬──────────┘
                            │
          ┌─────────────────┼─────────────────┐
          ▼                 ▼                  ▼
     Worker 0         Worker 1    ...    Worker N-1
          │                 │                  │
          └─────────────────┼──────────────────┘
                            ▼
                   flushRecordsAsync()
                    ├── mergeBatches()
                    ├── groupByHour()
                    ├── sortTypedColumnBatchByKeys()
                    ├── WriteParquetColumnar()
                    ├── sha256.Sum256()
                    ├── storage.Write()
                    └── registerFileInTiering()
```

#### 3.4.3 ArrowWriter

```go
type ArrowWriter struct {
    compression     compress.Compression  // snappy/zstd/gzip
    writerProps     *parquet.WriterProperties    // 预构建 (不可变)
    arrowProps      pqarrow.ArrowWriterProperties
    schemaCache     *schemaLRUCache       // 容量 1000 的 LRU
}
```

**Schema 推断与缓存：**
```
WriteParquetColumnar(measurement, columns, validity, tagColumns, decimalCols)
  │
  ├── getSchema(measurement, columns, tagColumns, decimalCols)
  │   ├── 计算 cacheKey: "measurement:[colNames]:[typeNames]:[tagColumns]"
  │   ├── LRU 查询 → 命中 → 返回缓存 Schema
  │   └── 未命中 → inferSchema() → 存入缓存 → 返回
  │
  ├── 构建 Arrow Arrays (共享 Allocator)
  │   ├── []int64 → Int64Builder.AppendValues (含 validity)
  │   ├── []int64 → TimestampBuilder (unsafe.Pointer 零拷贝)
  │   ├── []float64 → Float64Builder
  │   ├── []string → StringBuilder
  │   ├── []bool → BooleanBuilder
  │   └── []decimal128.Num → Decimal128Builder (带 precision/scale)
  │
  └── writeRecordToParquet()
      ├── NewRecord(schema, arrays, -1)
      ├── pqarrow.NewFileWriter(buffer, writerProps, arrowProps)
      ├── writer.Write(record)
      └── → []byte (Parquet 字节)
```

#### 3.4.4 MessagePack 解码器

```
MsgPackDecoder.Decode(data []byte)
  │
  ├── msgpack.Unmarshal(data, &rawPayload)
  │
  ├── map[string]interface{} (标准格式)
  │   ├── 检测 "columns" 字段 → decodeColumnar() 【列式零拷贝】
  │   │   ├── 验证列长度一致性
  │   │   ├── normalizeTimestamps() (秒/毫秒/微秒/纳秒 → 微秒)
  │   │   ├── sanitizeStringColumns() (非法 UTF-8 净化)
  │   │   └── ColumnarRecord{RawPayload: originalBytes} ← 零拷贝 WAL
  │   │
  │   ├── 检测 "batch" 字段 → 递归解码每个子元素
  │   └── 否则 → decodeRow() 【行式回退】
  │       ├── extractMeasurement() / extractTimestamp() / extractHost()
  │       └── Record{Measurement, Time, Fields, Tags}
  │
  └── []interface{} (数组格式)
      └── 逐元素递归 decodeMapPayload()
```

#### 3.4.5 Line Protocol 解析器

与 MessagePack 不同，LP 解析器在解析时已知字段类型，直接构建预类型化的 `TypedColumnBatch`：

```
LineProtocol text
  → UTF-8 预验证
  → 逐行解析: measurement,tag=val field1=100i,field2=0.5 timestamp
  → 类型映射:
      100i   → int64
      0.5    → float64
      "str"  → string
      true   → bool
  → 构建 TypedColumnBatch{Data: map[string]typedSlice}
  → WriteTypedColumnarDirect()  // 绕过 convertColumnsToTyped()
```

---

### 3.5 WAL 耐久层 (`internal/wal`)

**职责**：在数据写入 Parquet 之前提供崩溃恢复能力。

**文件结构：**

```
WAL 目录
├── wal-000000001.seg  ← 已轮转 (不可变)
├── wal-000000002.seg  ← 已轮转 (不可变)
└── wal-000000003.seg  ← 当前活跃 (正在写入)
```

**二进制格式：**
```
文件头 (7 bytes):
┌────────────┬───────────┬──────────────┐
│ Magic (4B) │ Ver (2B)  │ CkAlg (1B)   │
│  "IEDB"    │  0x0001   │ CRC32 = 0x01 │
└────────────┴───────────┴──────────────┘

每条 Entry:
┌──────────┬──────────────┬───────────┬──────────────┐
│ Len (4B) │ Timestamp(8B)│ CRC32(4B) │ Payload(N B) │
└──────────┴──────────────┴───────────┴──────────────┘

Envelope Payload (database 感知):
┌────────┬──────────┬──────────┬───────────────────┐
│ 0x01   │ dblen(2) │ dbname   │ original msgpack  │
└────────┴──────────┴──────────┴───────────────────┘
```

**Writer 架构：**

```go
type Writer struct {
    config    WriterConfig
    file      *os.File
    asyncChan chan walEntry      // 异步写入通道 (容量 10000)
    wg        sync.WaitGroup
}

type WriterConfig struct {
    WALDir       string          // WAL 文件目录
    SyncMode     SyncMode        // fsync / fdatasync / async
    MaxSizeBytes int64           // 轮转大小 (默认 100MB)
    MaxAge       time.Duration   // 轮转时间 (默认 1h)
    SyncInterval time.Duration   // 同步间隔 (默认 100ms)
    SyncBytes    int64           // 字节同步阈值 (默认 1MB)
    BufferSize   int             // 异步通道容量 (默认 10000)
}
```

**写入异步模型：**
```
AppendRaw(data) / AppendRawWithMeta(db, data)
  → 序列化 entry: [4B长度][8B时间戳][4B CRC32][原始数据]
  → 非阻塞发送到 asyncChan
  → channel 满 → 返回 ErrWALDropped (数据保留在内存 buffer 中)

后台 Writer Goroutine:
  for entry := range asyncChan:
    file.Write(entry.data)
    bytesWritten += len(entry.data)
    if bytesWritten >= SyncBytes || time since last sync >= SyncInterval:
      file.Sync()                              // fdatasync
    if fileSize >= MaxSizeBytes || age >= MaxAge:
      rotate()                                  // 创建新 segment
```

**Recovery 架构：**
```go
type Recovery struct {
    walDir string
}

type RecoveryCallback func(ctx, records []map[string]interface{}) error

type RecoveryOptions struct {
    SkipActiveFile   string                // 跳过正在写入的 segment
    BatchSize        int                   // 批次大小
    ColumnarCallback ColumnarRecoveryCallback  // 列式零拷贝恢复
}

RecoverWithOptions(ctx, callback, opts):
  1. filepath.Glob("wal-*.seg") → 按序号排序
  2. 跳过 activeFile
  3. 逐文件读取:
     a. 验证 Magic + Version + ChecksumType
     b. 逐条读取 [Length][Timestamp][CRC32][Payload]
     c. CRC32 校验 → 失败则记录 corrupted
     d. ColumnarCallback 或 RowCallback → 重新摄入 ArrowBuffer
  4. 返回 RecoveryStats
```

---

### 3.6 数据压缩 (`internal/compaction`)

**职责**：合并小的 Parquet 文件成为更大的文件，减少文件数，提高查询效率。

**架构概览：**

```
CompactionManager
├── StorageBackend      ← 扫描候选文件
├── LockManager         ← 分区锁 (防止并发压缩同一分区)
├── ManifestManager     ← 跟踪进行中/失败的 Job
├── Tier[]              ← [HourlyTier, DailyTier]
├── SortKeysConfig      ← 保持与摄入相同的排序键
└── SubprocessManager   ← 子进程启动与监控
```

**两级压缩策略：**

| 维度 | Hourly Tier | Daily Tier |
|------|------------|------------|
| 触发条件 | MinAgeHours (0.5h) + MinFiles (3) | MinAgeHours (48h) + MinFiles (2) |
| 合并粒度 | 同小时 | 同天 |
| 调度方式 | Cron (默认 "0 * * * *") | Cron (默认 "0 2 * * *") |
| 目的 | 快速清理写入碎片 | 深度合并减少文件数 |

**子进程隔离模型：**
```
Parent Process (iedb server)
  │
  ├── Scheduler.Start() → cron 触发
  │   └── Manager.RunCycle()
  │       ├── FindCandidates() → 按 tier 扫描
  │       ├── SplitIntoBatches() → 限制单次合并文件数
  │       └── compactFilesAdaptively()
  │           └── RunJobInSubprocess()
  │               ├── 构建 SubprocessJobConfig JSON
  │               ├── cmd := exec.Command("iedb", "compact", "--job-stdin")
  │               ├── stdin.Write(jsonBytes)
  │               ├── cmd.Run() → 等待子进程退出
  │               └── stdout.Read() → 解析 JobResult JSON

Subprocess (iedb compact --job-stdin)
  │
  ├── stdin.ReadAll() → json.Unmarshal → SubprocessJobConfig
  ├── compaction.RunSubprocessJob(&cfg)
  │   ├── 打开 DuckDB (独立连接，独立内存)
  │   ├── 读取所有输入 Parquet 文件
  │   ├── 多键排序 (按 SortKeys 指定的列)
  │   ├── 去重 (基于 iedb:tags metadata)
  │   ├── 写入合并 Parquet
  │   └── 验证文件完整性
  ├── stdout.Write(json.Marshal(result))
  └── os.Exit(0) → OS 回收所有 DuckDB jemalloc 内存
```

**Completion Manifest（Phase 4，集群模式）：**
```
Subprocess 完成压缩后:
  1. 写入 CompletionManifest JSON 到 {CompletionDir}/job-{id}.json
  2. CompletionWatcher (父进程 goroutine) 轮询该目录
  3. 发现 manifest → 提取文件操作 (add/delete)
  4. 通过 CompactionBridge → Raft FSM BatchFileOpsInManifest()
  5. 删旧文件 + 注册新文件
  6. 触发 OnCompactionComplete callback:
     ├── db.ClearHTTPCache()           // 本地 DuckDB
     ├── queryHandler.InvalidateCaches()
     └── 通知所有集群节点刷新缓存
```

---

### 3.7 认证授权 (`internal/auth`)

**职责**：API Token 认证，支持 Bearer/API Key/查询参数多种方式。

**认证流程：**

```
HTTP Request
  │
  ├── AuthMiddleware
  │   ├── 检查 PublicRoutes / PublicPrefixes → 跳过
  │   ├── 提取 Token:
  │   │   Authorization: Bearer <token>
  │   │   Authorization: Token <token>
  │   │   Authorization: <token>          (兼容)
  │   │   x-api-key: <token>              (兼容)
  │   │   ?p=<token>                       (InfluxDB 1.x 兼容)
  │   │
  │   └── VerifyToken(token):
  │       ├── SHA256(token) → cacheKey
  │       ├── LRU 缓存查询 (命中率优化)
  │       │   ├── 命中 → 返回 TokenInfo
  │       │   └── 未命中:
  │       │       ├── SHA256(token)[:16] → token_prefix (DB 索引)
  │       │       ├── SELECT ... WHERE token_prefix = ? AND enabled = 1
  │       │       ├── bcrypt.CompareHashAndPassword(hash, token)
  │       │       ├── UPDATE last_used_at
  │       │       └── 存入缓存
  │       └── 返回 TokenInfo{ID, Name, Permissions, ...}
  │
  └── 后续处理:
      ├── ctx.Locals("tokenInfo", tokenInfo)
      ├── RBAC 中间件 → 资源级权限检查
      └── Audit 中间件 → 记录操作日志
```

**SQLite 表结构：**
```sql
CREATE TABLE api_tokens (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    token_hash  TEXT NOT NULL,          -- bcrypt hash
    token_prefix TEXT NOT NULL,         -- SHA256 前 16 字符 (索引)
    description TEXT DEFAULT '',
    permissions TEXT DEFAULT '[]',      -- JSON array
    enabled     INTEGER DEFAULT 1,
    expires_at  DATETIME,
    last_used_at DATETIME,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_token_prefix ON api_tokens(token_prefix);
```

---

### 3.8 HTTP API 层 (`internal/api`)

**职责**：所有 HTTP 路由和请求处理器，是整个系统最大的包（~30 个文件）。

**路由注册架构（`server.go`）：**

```
Fiber App
├── 公共端点 (无认证):
│   ├── /health                  → 健康检查
│   ├── /ready                   → 就绪检查
│   ├── /metrics                 → Prometheus 指标
│   ├── /debug/pprof/*           → 性能分析
│   └── /api/v1/internal/cache/invalidate  → 集群缓存刷新 (内部)
│
├── Auth 中间件 (认证):
│   ├── /api/v1/auth/*           → Token 管理 (CRUD)
│   ├── /api/v1/rbac/*           → RBAC 管理 (企业版)
│   └── /api/v1/permissions/*    → Token 成员管理
│
├──审计中间件 (企业版):
│   ├── /api/v1/write/msgpack    → MessagePack 写入
│   ├── /api/v1/write/lp         → Line Protocol 写入
│   ├── /api/v1/write/tle        → TLE 流式写入
│   ├── /api/v1/import           → CSV/Parquet 批量导入
│   ├── /api/v1/query            → SQL 查询 (支持 Arrow IPC)
│   ├── /api/v1/delete           → 数据删除
│   └── /api/v1/proxy            → 外部 URL 代理
│
├── 管理端点:
│   ├── /api/v1/databases        → 数据库/测量列表
│   ├── /api/v1/compaction       → 压缩状态/触发
│   ├── /api/v1/backup           → 备份/恢复
│   ├── /api/v1/tiering/*        → 冷热分层策略
│   ├── /api/v1/tiering/policies → 分层策略管理
│   ├── /api/v1/mqtt/*           → MQTT 订阅管理
│   ├── /api/v1/scheduler/*      → CQ/保留调度状态
│   ├── /api/v1/reconciliation/* → 对账状态
│   ├── /api/v1/governance/*     → 治理策略 (企业版)
│   ├── /api/v1/query-management/* → 查询管理 (企业版)
│   ├── /api/v1/retention/*      → 保留策略
│   ├── /api/v1/continuous-query/* → 连续查询
│   └── /api/v1/cluster/*        → 集群状态
│
└── 集群路由 (自动转发):
    非 Writer 节点收到写请求 → 转发到 Writer 节点
    非 Reader 节点收到读请求 → 转发到 Reader/Writer 节点
```

**处理器文件对应关系：**

| 文件 | 处理器 | 端点前缀 |
|------|--------|---------|
| `msgpack.go` | `MsgPackHandler` | `/api/v1/write/msgpack` |
| `lineprotocol.go` | `LineProtocolHandler` | `/api/v1/write/lp` |
| `tle.go` | `TLEHandler` | `/api/v1/write/tle` |
| `query.go` | `QueryHandler` | `/api/v1/query` |
| `query_arrow.go` | Arrow IPC 流式返回 | 同上 (Accept 头判断) |
| `import.go` | `ImportHandler` | `/api/v1/import` |
| `delete.go` | `DeleteHandler` | `/api/v1/delete` |
| `databases.go` | `DatabasesHandler` | `/api/v1/databases` |
| `auth_routes.go` | `AuthHandler` | `/api/v1/auth/*` |
| `rbac_routes.go` | `RBACHandler` | `/api/v1/rbac/*` |
| `compaction.go` | `CompactionHandler` | `/api/v1/compaction` |
| `backup_routes.go` | `BackupHandler` | `/api/v1/backup` |
| `tiering.go` | `TieringHandler` | `/api/v1/tiering` |
| `tiering_policies.go` | `TieringPoliciesHandler` | `/api/v1/tiering/policies` |
| `mqtt.go` | `MQTTHandler` | `/api/v1/mqtt` |
| `mqtt_subscriptions.go` | `MQTTSubscriptionHandler` | `/api/v1/mqtt/subscriptions` |
| `cluster.go` | `ClusterHandler` | `/api/v1/cluster` |
| `scheduler.go` | `SchedulerHandler` | `/api/v1/scheduler` |
| `reconciliation.go` | `ReconciliationHandler` | `/api/v1/reconciliation` |
| `governance.go` | `GovernanceHandler` | `/api/v1/governance` |
| `query_management.go` | `QueryManagementHandler` | `/api/v1/query-management` |
| `retention.go` | `RetentionHandler` | `/api/v1/retention` |
| `continuous_query.go` | `ContinuousQueryHandler` | `/api/v1/continuous-query` |
| `proxy.go` | `ProxyHandler` | `/api/v1/proxy` |
| `audit_routes.go` | `AuditHandler` | `/api/v1/audit` |
| `mcp.go` | MCP 集成 | `/api/v1/mcp` |
| `permissions.go` | 权限管理 | `/api/v1/permissions` |
| `routing.go` | 集群请求转发逻辑 | (内部) |
| `like_optimizer.go` | SQL LIKE 优化 | (查询用) |
| `regex_rewriter.go` | SQL 正则重写 | (查询用) |

---

### 3.9 查询引擎 (`internal/query`)

**职责**：并行分区查询执行。

```go
type ParallelExecutor struct {
    db     *sql.DB
    config *ParallelExecutorConfig
    sem    chan struct{}  // 并发控制信号量
}

type ParallelExecutorConfig struct {
    MaxConcurrentPartitions int  // 最大并发分区数 (默认 4)
    MinPartitionsForParallel int // 触发并行的最小分区数 (默认 3)
}
```

**并行执行流程：**
```
ExecutePartitioned(ctx, paths[], queryTemplate, options):
  1. 检查 paths 数量 >= MinPartitionsForParallel
  2. 否则 → 单查询直接执行
  3. 创建 semaphore (容量 = MaxConcurrentPartitions)
  4. 为每个路径启动 goroutine:
     for i, path := range paths:
       sem <- struct{}{}  // 获取槽位
       go func(i, path):
         query := strings.Replace(queryTemplate, "{PARTITION_PATH}",
                    fmt.Sprintf("read_parquet('%s')", path), -1)
         rows, err := db.QueryContext(ctx, query)
         results[i] = &PartitionResult{Rows: rows, Columns: cols, Error: err}
         <-sem  // 释放槽位
  5. WaitGroup.Wait() → 收集所有结果
  6. 合并: 逐分区读取 Rows → 统一输出
```

**相关的查询辅助模块：**

| 模块 | 功能 |
|------|------|
| `internal/pruning` | 分区裁剪：从 WHERE 条件提取时间范围，过滤不相关分区路径 |
| `internal/sql` | SQL 工具：字符串字面量掩码（防止正则匹配字符串内部内容）|
| `internal/queryregistry` | 查询注册表：跟踪活跃/已完成的查询，支持取消和历史查询 |

---

## 企业版模块

### 4.1 集群协调 (`internal/cluster`)

**职责**：多节点集群管理，基于 Hashicorp Raft 共识算法。

**目录结构：**

```
internal/cluster/
├── coordinator.go           ← 中央协调器 (主入口)
├── node.go                  ← 节点定义 (ID, Role, State, Capabilities)
├── registry.go              ← 内存节点注册表
├── router.go                ← 请求路由器 (Round-Robin 负载均衡)
├── health.go                ← 健康检查器
├── role.go                  ← 角色定义 + 能力计算
├── errors.go                ← 错误哨兵
├── compaction_bridge.go     ← 压缩 → Raft 桥接
├── compactor_failover.go    ← 压缩器故障转移
├── writer_failover.go       ← 写入器故障转移
├── forward_apply.go         ← Leader 转发缓存
├── file_registrar.go        ← 文件清单注册器
│
├── raft/                    ← Raft 共识子包
│   ├── node.go              ←  Raft Node 包装
│   └── fsm.go               ←  ClusterFSM (集群状态机)
│
├── protocol/                ← 节点间通信协议
│   ├── messages.go          ←  所有消息类型
│   └── codec.go             ←  MessagePack 编解码
│
├── replication/             ← WAL 流复制
│   ├── sender.go            ←  发送端 (Writer → Reader)
│   ├── receiver.go          ←  接收端
│   └── protocol.go          ←  复制协议定义
│
├── filereplication/         ← 对等文件复制
│   ├── puller.go            ←  后台拉取工作池
│   ├── fetch_client.go      ←  TCP 文件获取客户端
│   └── catchup.go           ←  启动追赶扫描器
│
├── security/                ← 集群安全
│   ├── auth.go              ←  HMAC 消息认证
│   ├── nonce_cache.go       ←  重放保护
│   ├── tls.go               ←  TLS 配置
│   └── raft_stream.go       ←  TLS-wrapped Raft 流
│
└── sharding/                ← 数据分片 (Phase 4)
    ├── config.go            ←  分片配置
    ├── shardmap.go          ←  分片映射
    ├── shard_raft.go        ←  分片 Raft 操作
    ├── shard_replication.go ←  分片复制
    ├── router.go            ←  分片路由
    ├── meta.go / meta_fsm.go←  分片元数据状态机
    ├── failover.go          ←  分片故障转移
    ├── aggregation.go       ←  跨分片聚合
    └── scatter_gather.go    ←  分散-收集查询
```

**节点角色：**

```go
type NodeRole string
const (
    RoleStandalone NodeRole = "standalone"  // 单机模式
    RoleWriter     NodeRole = "writer"      // 接受写入 + 复制 WAL
    RoleReader     NodeRole = "reader"      // 只读查询 + 拉取文件
    RoleCompactor  NodeRole = "compactor"   // 运行压缩任务
)

type RoleCapabilities struct {
    CanIngest  bool  // Writer 或 Standalone
    CanQuery   bool  // Reader, Writer 或 Standalone
    CanCompact bool  // Compactor 或 Standalone
}
```

**Coordinator 架构：**

```go
type Coordinator struct {
    config       *config.ClusterConfig
    registry     *Registry            // 节点注册表
    raftNode     *raft.Node           // Raft 共识节点
    router       *Router              // 请求路由器
    health       *HealthChecker       // 健康检查
    puller       *filereplication.Puller  // 对等文件拉取
    replSender   *replication.Sender     // WAL 复制发送
    replReceiver *replication.Receiver   // WAL 复制接收
    writerFailover  *WriterFailover      // 写入器故障转移
    compactorFailover *CompactorFailover  // 压缩器故障转移
    deleteQueue  chan []string         // 本地文件清理队列
    nonceCache   *security.NonceCache  // 防重放
}
```

**Raft FSM（集群状态机）：**
```
ClusterFSM (raft/fsm.go):
├── NodeRegistry    ← 节点加入/离开/健康状态
│   └── Snapshot: JSON 序列化节点列表
└── FileManifest    ← 文件清单 (路径 → {Size, Origin, PartitionTime, SHA256})
    └── Snapshot: JSON 序列化文件清单
```

**集群协议消息：**
```
JoinRequest    → 节点加入申请 (含角色/版本/API地址)
JoinResponse   → Leader 返回 (含节点ID/集群配置)
Heartbeat      → 定期心跳 (含健康状态/指标)
LeaveNotify    → 节点离开通知
ReplicateSync  → WAL 复制同步请求
FetchFileRequest → 文件拉取请求 (含 HMAC 签名/字节偏移)
FetchFileAck   → 文件拉取确认
ForwardApply   → Leader 转发 Raft 命令
```

---

### 4.2 冷热分层 (`internal/tiering`)

**职责**：两层级存储（热层 → 冷层）的自动迁移和查询路由。

**架构：**

```go
type Manager struct {
    hotBackend    storage.Backend     // 热层 (本地 SSD)
    coldBackend   storage.Backend     // 冷层 (S3 / Azure)
    metadata      *MetadataStore      // SQLite 文件元数据
    policies      *PolicyStore        // 分层策略规则
    migrator      *Migrator           // 热 → 冷迁移执行器
    scheduler     *Scheduler          // Cron 调度
    router        *Router             // 查询时路由到正确 Tier
    licenseClient *license.Client
}
```

**模块组成：**

| 组件 | 文件 | 职责 |
|------|------|------|
| `MetadataStore` | `metadata.go` | SQLite 记录每个 Parquet 文件的 tier、路径、大小、分区时间 |
| `PolicyStore` | `policy.go` | 每个 database 的分层策略（hot-only / 迁移年龄等） |
| `Migrator` | `migrator.go` | 执行热→冷迁移：拷贝文件 → 验证 → 删热文件 → 更新元数据 |
| `Scheduler` | `scheduler.go` | Cron 触发迁移周期 |
| `Router` | `router.go` | 查询时根据时间范围路由到对应 tier 的 read_parquet |

**迁移流程：**
```
Scheduler 触发 → Manager.RunMigrationCycle()
  1. ScanAndRegisterFiles() → 扫描热层新文件 → 注册到 MetadataStore
  2. 查找热层中超过热保留期的文件
  3. 逐文件迁移:
     a. coldBackend.WriteReader(path, hotBackend.ReadTo(path)...)
     b. 验证冷层文件完整性
     c. hotBackend.Delete(path)
     d. MetadataStore.UpdateFileTier(path, TierCold)
  4. 清理元数据中的孤儿记录
```

**查询路由：**
```
查询涉及 database.measurement:
  → Router.GetBackendForTier(timeRange)
  → 检查 MetadataStore 中该时间段的文件 tier
  → 构建分层 read_parquet:
    SELECT ... FROM read_parquet([
      's3://cold-bucket/db/cpu/2025/*/*/*/*.parquet',   ← Cold
      '/data/hot/db/cpu/2026/*/*/*/*.parquet'           ← Hot
    ]) WHERE time BETWEEN ... AND ...
```

---

### 4.3 许可证管理 (`internal/license`)

**职责**：企业许可证验证和特性门控。

**License 模型：**
```go
type Tier string
const (
    TierStarter      Tier = "starter"      // 基础功能
    TierProfessional Tier = "professional" // 中级功能
    TierEnterprise   Tier = "enterprise"   // 全部功能
    TierUnlimited    Tier = "unlimited"    // 无限
)

type Feature string
const (
    FeatureCQScheduler      Feature = "cq_scheduler"
    FeatureRetentionScheduler Feature = "retention_scheduler"
    FeatureClustering       Feature = "clustering"
    FeatureRBAC             Feature = "rbac"
    FeatureTieredStorage    Feature = "tiering"
    FeatureAuditLogging     Feature = "audit_logging"
    FeatureWriterFailover   Feature = "writer_failover"
    FeatureQueryGovernance  Feature = "query_governance"
    FeatureQueryManagement  Feature = "query_management"
)

type License struct {
    LicenseKey    string
    CustomerID    string
    Tier          Tier
    MaxCores      int
    Status        string       // "active" / "expired" / "revoked"
    Features      []Feature
    ExpiresAt     time.Time
    DaysRemaining int
}
```

**验证流程：**
```
启动时:
  LicenseClient.ActivateOrVerify(ctx)
    → POST http://license-server/api/v1/license/activate
       body: {license_key, fingerprint (MAC+hostname+machineID), hostname, cores}
    → 服务器返回 License 结构体
    → 缓存本地
    → 应用核心限制: runtime.GOMAXPROCS(licensedCores)
                    database.ThreadCount = licensedCores
                    ingest.FlushWorkers = min(cfg, licensedCores)

运行时:
  StartPeriodicValidation(4h 间隔)
    → GET http://license-server/api/v1/license/verify?key=xxx&fingerprint=yyy
    → 更新本地缓存

特性门控模式:
  if licenseClient == nil || !licenseClient.CanUseClustering() {
      log.Warn("需要企业版许可证")
      return  // 禁用集群功能
  }
```

**核心限制执行：**
```
license.MaxCores > 0:
  → GOMAXPROCS ← min(machineCores, licensedCores)    ← 真正的 CPU 限制
  → DuckDB threads ← licensedCores
  → FlushWorkers   ← min(configValue, licensedCores)
```

---

### 4.4 查询治理 (`internal/governance`)

**职责**：基于 Token 的查询速率限制和配额管理。

```go
type Manager struct {
    db         *sql.DB       // SQLite 策略存储
    limiters   map[string]*SlidingWindowLimiter   // tokenID → 滑动窗口
    trackers   map[string]*QuotaTracker           // tokenID → 配额追踪
    policies   map[string]*Policy                 // tokenID → 策略
}

type Policy struct {
    TokenID            string
    RateLimitPerMin    int  // 每分钟最大查询数
    RateLimitPerHour   int  // 每小时最大查询数
    MaxQueriesPerHour  int  // 每小时总查询配额
    MaxQueriesPerDay   int  // 每天总查询配额
    MaxRowsPerQuery    int  // 单次查询最大行数
    MaxDurationSeconds int  // 单次查询最大时长
}
```

**滑动窗口限流器：**
```
60 个子窗口 (每秒一个), 每个窗口记录该秒内的请求数
total = sum(last 60 windows)  → 每分钟请求数
total = sum(last 3600 windows) → 每小时请求数
total > limit → 429 Too Many Requests + Retry-After header
```

**查询执行前检查：**
```
HandleQuery():
  1. tokenInfo := GetTokenInfo(ctx)
  2. result := governance.CheckRateLimit(tokenInfo.ID)
      → Allowed / Denied (429)
  3. result := governance.CheckQuota(tokenInfo.ID)
      → Allowed / Exhausted (429)
  4. 执行查询 (受到 maxRows / maxDuration 约束)
  5. governance.RecordQueryCompletion(tokenInfo.ID, duration, rowCount)
```

---

### 4.5 审计日志 (`internal/audit`)

**职责**：记录所有 API 操作到 SQLite，支持查询和保留清理。

**架构：**
```go
type Logger struct {
    db      *sql.DB
    config  *config.AuditLogConfig
    events  chan *AuditEvent     // 异步批次通道
    wg      sync.WaitGroup
}

type AuditEvent struct {
    Timestamp   time.Time
    EventType   string          // "data.write" / "data.query" / "auth" / "admin"
    Actor       string          // Token ID
    Method      string          // HTTP method
    Path        string          // API path
    Database    string
    Measurement string
    StatusCode  int
    IPAddress   string
    UserAgent   string
    DurationMs  int64
    Detail      map[string]interface{}  // 额外上下文
}
```

**事件类型分类：**
```
/api/v1/write/*           → "data.write"
/api/v1/query             → "data.query"
/api/v1/delete            → "data.delete"
/api/v1/import            → "data.import"
/api/v1/auth/*            → "auth"
/api/v1/rbac/*            → "admin.rbac"
/api/v1/databases         → "admin.database"
/api/v1/compaction        → "admin.compaction"
/api/v1/tiering/*         → "admin.tiering"
/api/v1/mqtt/*            → "admin.mqtt"
/api/v1/governance/*      → "admin.governance"
```

**写入与保留：**
```
Middleware → audit.LogEvent(event):
  → 非阻塞发送到 events channel (100 容量)
  → channel 满 → 丢弃 (不阻塞请求)

Writer Goroutine:
  for {
    收集事件直到 100 条或 1 秒超时
    → 批量 INSERT
    → 清理超过 retention_days 的旧事件
    → VACUUM 回收空间
  }
```

---

### 4.6 RBAC 权限

**数据模型（SQLite 表前缀 `rbac_`）：**

```
Organizations ──→ Teams ──→ Roles
                      │          │
                      │          ├── Database Patterns (通配符规则)
                      │          │   如: "iot_*" → 匹配所有 iot_ 开头的数据库
                      │          │
                      │          └── Measurement Permissions
                      │              ├── 特定 measurement: "sensors", "metrics"
                      │              └── 通配符: "*" → 所有 measurement
                      │
                      └── Token Memberships (token → team 多对多)
```

**权限检查流程：**
```
RequireResourcePermission(database, measurement, action):
  1. tokenInfo := GetTokenInfo(ctx)
  2. tokenInfo.IsAdmin → 直接允许
  3. 查询 token 所属的 team(s)
  4. 查询 team 的 role(s)
  5. 对每个 role:
     a. 匹配 Database Pattern (glob 匹配)
     b. 检查 target database 是否匹配
     c. 检查 measurement 权限 (精确匹配或 "*")
     d. 检查 action 权限 (read/write/delete/admin)
  6. 任一 role 匹配 → 允许
  7. 全部不匹配 → 403 Forbidden
```

---

### 4.7 数据对账 (`internal/reconciliation`)

**职责**：定期检测 Raft 文件清单与物理存储之间的漂移，并自动修复。

**架构：**

```go
type Reconciler struct {
    config       Config
    coordinator  Coordinator     // 访问 Raft FSM
    storage      storage.Backend
    gate         Gate            // 控制是否执行扫描/清理
    audit        AuditWriter     // 审计删除操作
    runs         []*Run          // 最近 10 次运行结果
}

type Config struct {
    GraceWindow          time.Duration  // 新文件保护窗口 (默认 24h)
    ClockSkewAllowance   time.Duration  // 时钟偏差容忍
    MaxManifestSize      int            // Manifest 大小上限 (200k 条目)
    MaxDeletesPerRun     int            // 单次运行最大删除数 (10k)
    BatchSize            int            // 批量操作大小
    DeletePreManifestOrphans bool        // 是否删除 Manifest 之前的孤儿文件
    ManifestOnlyDryRun   bool            // 仅扫描不删除
}
```

**对账算法：**
```
Reconcile():
  Phase 1: 快照 Manifest
    coordinator.GetFileManifest() → 全量文件清单快照
    if len(manifest) > MaxManifestSize → 中止 (清单过大)

  Phase 2: 遍历存储
    WalkStorage(prefixes) → 实际存储文件集合
    与 Manifest 比较:
      ├── 在 Manifest 中但不在存储上 → 孤儿 Manifest 条目 (待删除)
      └── 在存储上但不在 Manifest 中 → 孤儿存储文件 (待删除)

  Phase 3: 扫描孤儿 Manifest (步骤 4)
    对每个孤儿 Manifest 条目:
      ├── 检查 GraceWindow (新文件保护)
      ├── 检查 ClockSkewAllowance
      └── BatchFileOpsInManifest(删除)

  Phase 4: 清理孤儿存储 (步骤 5, 必须在步骤 4 之后)
    对每个孤儿存储文件:
      ├── 检查是否在 Manifest DryRun 模式
      ├── storage.Delete(path)
      └── 到达 MaxDeletesPerRun → 停止 (爆炸保护)
```

---

## 支撑模块

### 5.1 优雅关机 (`internal/shutdown`)

```go
type Coordinator struct {
    components []shutdownEntry    // Shutdownable 接口
    hooks      []hookEntry        // 函数式钩子
    timeout    time.Duration      // 全局超时
}

// 优先级 (数字越小越先执行)
const (
    PriorityHTTPServer = 10    // 停止接受新请求
    PriorityIngest     = 20    // 排空写入
    PriorityBuffer     = 30    // 排空内存缓冲
    PriorityWAL        = 40    // 清理 WAL
    PriorityCompaction = 50    // 停止集群/压缩
    PriorityTelemetry  = 60    // 最后遥测
    PriorityAuth       = 70    // 认证管理器
    PriorityStorage    = 80    // 存储后端
    PriorityDatabase   = 90    // 数据库连接
)
```

**关机流程：**
```
收到 SIGINT/SIGTERM/SIGQUIT
  → shutdownCoordinator.WaitForSignal()
  → shutdownCoordinator.Shutdown()
    1. 按优先级升序执行所有 hooks (context timeout 保护)
    2. 按优先级升序关闭所有 components
    3. 全局超时控制 (默认 30s)
```

### 5.2 任务调度 (`internal/scheduler`)

**CQ Scheduler：**
```
NewCQScheduler(CQHandler, LicenseClient, ClusterGate, Logger)
  → 启动时加载所有活动的连续查询
  → 为每个 CQ 创建独立的 goroutine + ticker
  → 每次 tick:
    ├── 检查许可证有效性
    ├── 检查集群门控 (IsPrimaryWriter)
    └── CQHandler.ExecuteCQ(cqID)
  → done channel 保证不重复执行
  → ReloadCQ() / StartJobDirect() 动态管理
```

**Retention Scheduler：**
```
NewRetentionScheduler(RetentionHandler, LicenseClient, ClusterGate, Schedule, Logger)
  → 使用 robfig/cron 按 Schedule 触发
  → 每次执行:
    ├── 检查许可证
    ├── 检查集群门控
    └── RetentionHandler.RunAllPolicies()
  → runningJob 标志防止并发执行
```

**集群门控接口：**
```go
type WriterGate interface {
    IsPrimaryWriter() bool  // 只有主写入器执行定时任务
    Role() string
}
```

### 5.3 备份恢复 (`internal/backup`)

**备份流程：**
```
POST /api/v1/backup
  → Manager.CreateBackup(includeMetadata, includeConfig)
    1. 获取互斥锁
    2. 生成 UUID backupID
    3. ObjectLister → 发现所有 .parquet 文件
    4. 构建 Manifest (database → measurements → files)
    5. 复制 Parquet 文件到 {backupPath}/{backupID}/data/
    6. (可选) SQLite 备份:
       a. PRAGMA wal_checkpoint(TRUNCATE)  ← WAL 刷入主文件
       b. 复制 SQLite 文件到 metadata/
    7. (可选) 复制 iedb.toml 到 config/
    8. 写入 manifest.json
    9. 返回 BackupResult
```

**恢复流程：**
```
POST /api/v1/backup/{backupID}/restore
  → Manager.RestoreBackup(backupID)
    1. 读取 manifest.json
    2. 恢复数据文件 (解析路径，stream 写入)
    3. 恢复 SQLite (先创建 .before-restore 备份)
    4. 恢复配置 (先创建 .before-restore 备份)
```

### 5.4 MQTT 接入 (`internal/mqtt`)

**架构：**
```
MQTT Broker ←→ SubscriptionManager
                   ├── Repository (SQLite CRUD)
                   ├── PasswordEncryptor (AES-256 密码加密)
                   └── Subscriber[] (per-subscription goroutine)
                        └── paho.mqtt Client
                             └── messageHandler:
                                  ├── MessagePack 或 JSON 解码
                                  ├── TopicMapping 转换
                                  └── ArrowBuffer.Write()
```

**订阅生命周期：**
```
POST /api/v1/mqtt/subscriptions
  → 创建 Subscription 配置 + 加密密码 → 存入 SQLite
  → 可选 auto_start → 立即 StartSubscription()

StartSubscription(id):
  → Repository.Get(id)
  → 创建 paho.mqtt Client (配置 TLS/KeepAlive/CleanSession)
  → 设置消息处理器:
      解码 → TopicMapping → ArrowBuffer
  → Connect → Subscribe(topic)
  → 后台 goroutine 维护连接 (断线重连)
```

### 5.5 遥测采集 (`internal/telemetry`)

```
Collector.Start():
  → 生成/读取 instanceID (.instance_id 文件)
  → 定期 POST http://telemetry-server/api/v1/telemetry
     body: {
       instance_id, version,
       os, arch, num_cpu, memory_mb,
       uptime_seconds,
       total_databases, total_measurements,
       total_parquet_files, total_data_size_bytes,
       records_ingested, queries_executed,
       license_tier (如果适用)
     }
```

### 5.6 监控指标 (`internal/metrics`)

**Metrics 结构（简略）：**
```go
type Metrics struct {
    // HTTP 指标
    HTTPRequests, HTTPSuccess, HTTPErrors  atomic.Int64
    HTTPLatencyHistogram                    [10]bucket

    // 摄入指标
    IngestRecords, IngestBytes, IngestErrors  atomic.Int64
    MsgPackRecords, LineProtocolRecords       atomic.Int64

    // 查询指标
    QueryRequests, QuerySuccess, QueryErrors, QueryTimeouts, QuerySlow  atomic.Int64
    QueryRowsReturned, QueryLatencyMs                                    atomic.Int64

    // 缓冲指标
    BufferRecordsBuffered, BufferRecordsWritten, BufferFlushes  atomic.Int64
    BufferErrors, BufferFlushFailures, BufferQueueDepth         atomic.Int64

    // 存储指标
    StorageWrites, StorageReads, StorageBytes, StorageErrors  atomic.Int64

    // 压缩指标
    CompactionJobs, CompactionFiles, CompactionBytes, CompactionManifestsRecovered  atomic.Int64

    // WAL 指标
    WALRecordsPreserved, WALRecoveryTotal, WALRecoveryRecords, WALDroppedEntries  atomic.Int64

    // 认证指标
    AuthRequests, AuthCacheHits, AuthCacheMisses  atomic.Int64

    // 更多: MQTT, Audit, Governance, QueryManagement, Replication ...
}
```

**输出格式：**
- `Snapshot()` → JSON map (用于 API)
- `PrometheusFormat()` → text/plain Prometheus 格式 (/metrics 端点)
- `TimeSeriesCollector` → 3 个环形缓冲区（system/application/API），5s 采样间隔

### 5.7 共享数据模型 (`pkg/models`)

```go
// 行格式 (传统)
type Record struct {
    Measurement string
    Time        time.Time
    Timestamp   int64                    // 微秒 (优先于 Time)
    Fields      map[string]interface{}   // 字段值
    Tags        map[string]string        // 标签
}

// 列格式 (零拷贝快路径)
type ColumnarRecord struct {
    Measurement string
    Columns     map[string][]interface{} // 列名 → 值数组
    TagColumns  []string                 // 识别 tag 列 (用于压缩去重)
    RawPayload  []byte                   // 原始 MsgPack 字节 (WAL 零拷贝)
    Columnar    bool                     // 标记为列式格式
    TimeUnit    string                   // "us" (微秒)
}

// MessagePack 载荷 (协议层)
type MsgPackPayload struct {
    M       interface{}                  // measurement
    T       interface{}                  // timestamp
    H       interface{}                  // host
    F       interface{}                  // compact fields array
    Fields  map[string]interface{}       // named fields
    Tags    map[string]string            // tags
    Columns map[string][]interface{}     // columnar data
    Batch   []interface{}                // batch of records
}
```

---

## 启动流程

`cmd/iedb/main.go` 的完整初始化顺序：

```
1. checkSubcommand()
   ├── "compact" → runCompactSubprocess() → os.Exit()  (子进程路径)
   └── 否则 → 继续正常启动

2. config.Load()  →  TOML + 环境变量

3. ValidateTLS()  →  证书验证

4. logger.Setup()  →  zerolog 初始化

5. license.Client.ActivateOrVerify()
   ├── 成功 → 应用核心限制 (GOMAXPROCS / DuckDB threads / FlushWorkers)
   └── 失败 → licenseClient = nil (禁用企业功能)

6. metrics.Init() + TimeSeriesCollector.Init()

7. shutdown.New(30s)  →  优雅关机协调器

8. database.New(config)
   ├── 安装 httpfs / azure / cache_httpfs 扩展
   └── 测试查询验证连接

9. storage.New{Local,S3,Azure}Backend(config)
   └── 设置 shutdown hook

10. wal.NewWriter(config) (如果启用)
    ├── 准备 Recovery 对象
    └── 设置 shutdown hook

11. ingest.NewArrowBuffer(config, storage)
    ├── 初始化 shards / worker pool / periodic flush
    └── arrowBuffer.SetWAL(walWriter) (如果启用)

12. WAL Recovery (如果启用)
    ├── 扫描 segment 文件
    ├── CRC32 校验 → callback → WriteColumnarDirectNoWAL
    └── 启动 Periodic WAL Maintenance goroutine

13. MQTT SubscriptionManager (如果启用)
    ├── 加载 SQLite 中的订阅
    └── auto_start → 启动 MQTT 连接

14. auth.NewAuthManager() (如果启用)
    ├── 创建初始 admin token (或使用 IEDB_AUTH_BOOTSTRAP_TOKEN)
    └── RBAC Manager (企业版)

15. Compaction Scheduler (如果启用)
    ├── HourlyScheduler + DailyScheduler
    └── CompletionWatcher (集群模式)

16. Telemetry Collector (如果启用)

17. License Periodic Validation (如果有 license)

18. Cluster Coordinator (企业版 + license)
    ├── Raft Node 启动
    ├── Health Checker
    ├── Peer Discovery
    ├── WAL Replication (如果启用)
    ├── File Registar + Puller (如果启用)
    ├── Writer/Compactor Failover (如果启用)
    └── CompactionBridge + CompletionWatcher 动态启停

19. HTTP Server (Fiber)
    ├── 注册所有路由 (30+ 处理器)
    ├── 注册 Auth / Audit 中间件
    ├── 注册集群路由器 (请求转发)
    └── server.Start()

20. WaitForSignal() → Shutdown() (按优先级)
```

---

## 模块间交互协议

### 写入路径交互

```
HTTP → api/handler → ingest/ArrowBuffer → wal/Writer → storage/Backend
                                                           │
                                            tiering/Manager (元数据)
                                            cluster/FileRegistrar (集群清单)
```

1. **API → ArrowBuffer**: 直接函数调用 `Write()`, `WriteColumnarDirect()`
2. **ArrowBuffer → WAL**: 接口 `WALWriter` (非阻塞 Append)
3. **ArrowBuffer → Storage**: 接口 `storage.Backend` (同步 Write)
4. **ArrowBuffer → Tiering**: 直接调用 `tieringManager.RecordFile()`
5. **ArrowBuffer → Cluster**: 接口 `FileRegistrar.RegisterFile()` (非阻塞)

### 查询路径交互

```
HTTP → api/QueryHandler → storage/Backend (分区发现)
                        → pruning/PartitionPruner (时间裁剪)
                        → query/ParallelExecutor (并行执行)
                        → database/DuckDB (SQL)
                        → tiering/Router (冷热路由)
```

1. **QueryHandler → Storage**: `List()` 发现 Parquet 文件
2. **QueryHandler → Pruning**: `ExtractTimeRange()` + `GeneratePartitionPaths()`
3. **QueryHandler → DuckDB**: `Query(sql)` 执行重写后的 SQL
4. **QueryHandler → Governance**: `CheckRateLimit()` + `CheckQuota()`
5. **QueryHandler → RBAC**: `CheckPermissionsBatch()` 资源级权限

### 集群间交互

```
Writer Node ←→ TCP Protocol ←→ Reader Node
     │                              │
     ├── WAL Replication ──────────→│  (Sender → Receiver)
     ├── File Fetch Request ←───────│  (Puller → FetchClient)
     ├── Cache Invalidate ─────────→│  (Compaction 后)
     └── Request Forward ←─────────│  (Reader 转发写请求到 Writer)
```

---

## 许可证特性门控矩阵

| 特性 | 模块 | Feature 标志 | 无 License 行为 |
|------|------|-------------|----------------|
| 集群 (Raft) | `cluster` | `clustering` | 静默降级为 standalone |
| WAL 复制 | `cluster/replication` | `clustering` | 不启动 |
| 对等文件复制 | `cluster/filereplication` | `clustering` | 不启动 |
| 写入器故障转移 | `cluster/writer_failover` | `writer_failover` | 无自动故障转移 |
| 压缩器故障转移 | `cluster/compactor_failover` | `clustering` | 基于静态角色 |
| RBAC | `auth/rbac_manager` | `rbac` | RBAC 方法返回 false |
| 冷热分层 | `tiering` | `tiering` | Manager 不启动 |
| 审计日志 | `audit` | `audit_logging` | 中间件不注册 |
| 查询治理 | `governance` | `query_governance` | 不检查速率/配额 |
| 查询管理 | `queryregistry` | `query_management` | 不跟踪查询 |
| CQ 调度器 | `scheduler/cq_scheduler` | `cq_scheduler` | CQ 仅手动触发 |
| 保留调度器 | `scheduler/retention_scheduler` | `retention_scheduler` | 保留仅手动触发 |
| 数据对账 | `reconciliation` | `clustering` | Scheduler 不启动 |
| 核心限制 | `license` | (MaxCores) | 无 CPU 限制 |
