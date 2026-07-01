# IotEdgeDB

基于 DuckDB 构建的高性能时间序列数据库。Go语言实现。

---

## 问题背景

工业物联网大规模生成海量数据：

- **赛车运动**：每场比赛超过1亿个传感器读数
- **智慧城市**：每天数十亿基础设施事件
- **采矿和制造业**：前所未有的设备遥测规模
- **能源和公用事业**：电网监控、智能电表、可再生能源输出
- **石油天然气**：管道传感器、钻井遥测、炼油厂监控
- **物流车队**：车辆跟踪、路线优化、交付指标
- **医疗保健**：患者监测、临床睡眠研究、设备遥测
- **可观测性**：分布式系统的指标、日志、追踪

传统时间序列数据库跟不上需求。它们速度慢、昂贵，并将您的数据锁定在专有格式中。

**IotEdgeDB 解决了这个问题：每秒2299万条记录的写入，数十亿行数据的亚秒级查询，便携式Parquet文件完全由您掌控。**

```sql
-- 分析各设施的设备异常
SELECT
  device_id,
  facility_name,
  AVG(temperature) OVER (
    PARTITION BY device_id
    ORDER BY timestamp
    ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
  ) as temp_moving_avg,
  MAX(pressure) as peak_pressure
FROM iot.sensors
WHERE timestamp > NOW() - INTERVAL '24 hours'
  AND facility_id IN ('plant_7', 'mining_site_42')
HAVING MAX(pressure) > 850;
```

**标准 DuckDB SQL. Window functions, CTEs, joins. 无需专有查询语言.**

---

## 性能

基准测试基于苹果 MacBook Pro M3 Max（16 核心，48GB 内存，1TB NVMe）。
测试配置：12 个并发工作进程，1000 条记录批次，IoT 传感器数据。

### 数据写入

| 协议 | 吞吐量 | p50延迟 | p99延迟 |
|------|--------|---------|---------|
| MessagePack 列式 | **2299万条记录/秒** | 0.39毫秒 | 2.57毫秒 |
| MessagePack + Zstd | 1921万条记录/秒 | 0.50毫秒 | 2.28毫秒 |
| MessagePack + GZIP | 1928万条记录/秒 | 0.50毫秒 | 2.28毫秒 |
| Line Protocol | 610万条记录/秒 | 1.62毫秒 | 6.22毫秒 |

### 压缩

自动后台压缩将小的 Parquet 文件合并为优化的大文件：

| 指标 | 压缩前 | 压缩后 | 减少率 |
|------|--------|--------|--------|
| 文件数量 | 43 | 1 | 97.7% |
| 大小 | 372 MB | 36 MB | **90.4%** |

优势：
- **10倍存储减少** 通过更好的压缩和编码
- **更快查询** - 扫描1个文件而非43个文件
- **更低云成本** - 更少存储，更少API调用

### 查询（2026年7月）

对于大型结果集，Arrow IPC 格式比 JSON 提供 2.5 倍以上吞吐量：

| 查询 | Arrow (毫秒) | JSON (毫秒) | 加速比 |
|------|--------------|-------------|--------|
| COUNT(*) - 全表 | 4 | 2 | 0.5x |
| SELECT LIMIT 10K | 7 | 9 | 1.3x |
| SELECT LIMIT 100K | 20 | 37 | 1.8x |
| SELECT LIMIT 500K | 67 | 168 | **2.5x** |
| SELECT LIMIT 1M | 127 | 331 | **2.6x** |
| AVG/MIN/MAX 聚合 | 96 | 92 | 1.0x |
| GROUP BY host (Top 10) | 32 | 32 | 1.0x |
| 最近1小时过滤 | 5 | 3 | 0.6x |

**最佳吞吐量：**
- Arrow: **787万行/秒** (100万行SELECT)
- JSON: **302万行/秒** (100万行SELECT)
- COUNT(*): **415亿行/秒** (8300万行，2毫秒)

> 数据集 8300 万行，存储为 80 个未经压缩的 Parquet 文件。
> 聚合查询的延迟可通过启用自动后台压缩大幅降低。

---

## 为什么选择 Go

- **稳定内存**：Go的垃圾回收器将内存归还给操作系统。无泄漏。
- **单一二进制文件**：部署单个可执行文件。无依赖项。
- **原生并发**：Goroutines 高效处理数千个连接。
- **生产级垃圾回收**：大规模下的亚毫秒暂停时间。

---

## 快速开始

```bash
# Build
make build

# Run
./iedb

# Verify
curl http://localhost:8000/health
```

---

## 安装

### Docker

```bash
docker run -d \
  --name iedb \
  -p 8000:8000 \
  -v iedb-data:/app/data \
  ghcr.io/mengdal/iedb:latest
```

## 特性

- **数据写入**：MessagePack列式（最快）、InfluxDB Line Protocol
- **查询**：DuckDB SQL引擎，JSON和Apache Arrow IPC响应
- **存储**：本地文件系统、S3、MinIO
- **认证**：基于令牌的认证，内存缓存
- **持久性**：可选预写日志（WAL）
- **压缩**：分层（按小时/天）自动文件合并
- **数据管理**：保留策略、连续查询、符合GDPR的删除
- **可观测性**：Prometheus指标、结构化日志、优雅关闭
- **可靠性**：断路器、指数退避重试

---

## 配置

IotEdgeDB 使用 TOML 配置并支持环境变量覆盖。

```toml
[server]
host = "0.0.0.0"
port = 8000

[storage]
backend = "local"        # local, s3, minio
local_path = "./data/iedb"

[ingest]
flush_interval = "5s"
max_buffer_size = 50000

[auth]
enabled = true
```

环境变量使用 `IEDB_` 前缀:

```bash
export IEDB_SERVER_PORT=8000
export IEDB_STORAGE_BACKEND=s3
export IEDB_AUTH_ENABLED=true
```

---

## 项目结构

```
iedb/
├── cmd/iedb/              # Application entry point
├── internal/
│   ├── api/              # HTTP handlers (Fiber) — query, write, import, TLE, admin
│   ├── audit/            # Audit logging for API operations
│   ├── auth/             # Token authentication and RBAC
│   ├── backup/           # Backup and restore (data, metadata, config)
│   ├── circuitbreaker/   # Resilience patterns (retry, backoff)
│   ├── cluster/          # Raft consensus, node roles, WAL replication
│   ├── compaction/       # Tiered hourly/daily Parquet file merging
│   ├── config/           # TOML configuration with env var overrides
│   ├── database/         # DuckDB connection pool
│   ├── governance/       # Per-token query quotas and rate limiting
│   ├── ingest/           # MessagePack, Line Protocol, TLE, Arrow writer
│   ├── license/          # License validation and feature gating
│   ├── logger/           # Structured logging (zerolog)
│   ├── metrics/          # Prometheus metrics
│   ├── mqtt/             # MQTT subscriber — topic-to-measurement ingestion
│   ├── pruning/          # Query-time partition pruning
│   ├── query/            # Parallel partition executor
│   ├── queryregistry/    # Active/completed query tracking
│   ├── scheduler/        # Continuous queries and retention policies
│   ├── shutdown/         # Graceful shutdown coordinator
│   ├── sql/              # SQL parsing utilities
│   ├── storage/          # Local, S3, Azure backends
│   ├── telemetry/        # Usage telemetry
│   ├── tiering/          # Hot/cold storage lifecycle management
│   └── wal/              # Write-ahead log
├── pkg/models/           # Shared data structures (Record, ColumnarRecord)
├── benchmarks/           # Performance benchmarking suites
├── deploy/               # Docker Compose and Kubernetes configs
├── helm/                 # Helm charts
├── scripts/              # Utility scripts (analysis, backfill, debugging)
├── iedb.toml              # Configuration file
├── Makefile              # Build commands
└── go.mod
```

---

## 开发

```bash
make deps           # 安装依赖
make build          # 构建二进制文件
make run            # 运行而不构建
make test           # 运行测试
make test-coverage  # 运行带覆盖率的测试
make bench          # 运行基准测试
make lint           # 运行代码检查
make fmt            # 格式化代码
make clean          # 清理构建产物
```