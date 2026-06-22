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

**IotEdgeDB 解决了这个问题：每秒1860万条记录的写入，数十亿行数据的亚秒级查询，便携式Parquet文件完全由您掌控。**

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

基准测试基于苹果 MacBook Pro M3 Max（14 核心，36GB 内存，1TB NVMe）。
测试配置：12 个并发工作进程，1000 条记录批次，IoT 传感器数据。

### 数据写入

| 协议 | 吞吐量 | p50延迟 | p99延迟 |
|------|--------|---------|---------|
| MessagePack 列式 | **1860万条记录/秒** | 0.46毫秒 | 3.68毫秒 |
| MessagePack + Zstd | 1680万条记录/秒 | 0.55毫秒 | 3.23毫秒 |
| MessagePack + GZIP | 1540万条记录/秒 | 0.63毫秒 | 3.17毫秒 |
| Line Protocol | 370万条记录/秒 | 2.63毫秒 | 10.63毫秒 |

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

### 查询（2026年1月）

对于大型结果集，Arrow IPC 格式比 JSON 提供 2 倍吞吐量：

| 查询 | Arrow (毫秒) | JSON (毫秒) | 加速比 |
|------|--------------|-------------|--------|
| COUNT(*) - 全表 | 6.7 | 9.0 | 1.35x |
| SELECT LIMIT 10K | 27 | 31 | 1.14x |
| SELECT LIMIT 100K | 55 | 103 | 1.88x |
| SELECT LIMIT 500K | 201 | 420 | **2.10x** |
| SELECT LIMIT 1M | 379 | 789 | **2.08x** |
| AVG/MIN/MAX 聚合 | 146 | 146 | 1.00x |
| GROUP BY host (Top 10) | 107 | 104 | 0.98x |
| 最近1小时过滤 | 12 | 11 | 0.96x |

**最佳吞吐量：**
- Arrow: **264万行/秒** (100万行SELECT)
- JSON: **127万行/秒** (100万行SELECT)
- COUNT(*): **120-1900亿行/秒** (1.34亿行，7-11毫秒)

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
  371562656/iedb:latest
```

## 特性

- **数据写入**：MessagePack列式（最快）、InfluxDB Line Protocol、Arrow Flight
- **查询**：DuckDB SQL引擎，JSON、Arrow IPC、Arrow Flight（gRPC零拷贝流式）
- **存储**：本地文件系统、S3、MinIO
- **认证**：基于令牌的认证，内存缓存
- **持久性**：可选预写日志（WAL）
- **压缩**：分层（按小时/天）自动文件合并
- **数据管理**：保留策略、连续查询、符合GDPR的删除
- **可观测性**：Prometheus指标、结构化日志、优雅关闭
- **可靠性**：断路器、指数退避重试

---

## Arrow Flight

IotEdgeDB 内置 Apache Arrow Flight 服务器——基于 gRPC 的高性能 Arrow 数据传输协议。Flight 提供零拷贝 Arrow RecordBatch 流式传输，比 HTTP JSON 快 2-50×，比 HTTP Arrow IPC 快 7-27%。

### 启用 Flight

```toml
[flight]
enabled = true
addr = ":9090"
```

### Flight vs HTTP 性能对比 (Apple M5)

| 查询规模 | Flight DoGet | HTTP Arrow IPC | Flight 优势 |
|----------|-------------|----------------|------------|
| `SELECT 1` | 77 µs | 106 µs | **27% faster** |
| 1K 行 | 146 µs | 179 µs | **19% faster** |
| 100K 行 | 4.2 ms | 4.5 ms | **7% faster** |

### Flight 写入 (DoPut)

Arrow RecordBatch 直写缓冲区，跳过 MsgPack 解码：

| 批次大小 | 写入吞吐 |
|----------|---------|
| 1K 行 | **94M 行/秒** (10.6 µs) |
| 10K 行 | 106M 行/秒 (94 µs) |

### Python 客户端示例

```bash
pip install pyarrow
python examples/flight_client.py --host localhost --port 9090
```

```python
import pyarrow.flight as flight
import json

client = flight.FlightClient("grpc://localhost:9090")

# 查询
desc = flight.FlightDescriptor.for_command(json.dumps({"sql": "SELECT 1 AS val"}))
info = client.get_flight_info(desc)
reader = client.do_get(info.endpoints[0].ticket)
table = reader.read_all()
print(table)

# 写入
schema = pa.schema([("value", pa.int64()), ("time", pa.timestamp("us"))])
data = pa.record_batch([[1, 2, 3], [now, now, now]], schema=schema)
desc = flight.FlightDescriptor.for_command(
    json.dumps({"database": "mydb", "measurement": "cpu"}))
writer, reader = client.do_put(desc, schema)
writer.write(data)
writer.close()
```

### Go 客户端示例

```go
import "iedb/internal/flight"

client, _ := flight.NewClient("localhost:9090")
defer client.Close()

reader, _ := client.Query(ctx, "SELECT * FROM cpu_usage LIMIT 100")
defer reader.Release()
for reader.Next() {
    batch := reader.RecordBatch()
    // 处理 Arrow RecordBatch
}
```

### Flight SQL

支持 BI 工具直接连接（DBeaver, Tableau, Metabase）：

```
catalog: iedb
  └── schema: <database>
      └── table: <measurement>
```

### 集群 Scatter-Gather

集群模式下，Flight 替代 HTTP JSON 进行跨分片查询，实现零拷贝 RecordBatch 合并：

```
2 节点 × 250 行: 444 µs
4 节点 × 250 行: 536 µs
```

---

## 配置

ieDB 使用 TOML 配置并支持环境变量覆盖。

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

[flight]
enabled = true             # 启用 Arrow Flight gRPC 服务器
addr = ":9090"             # Flight 监听地址
tls = false                # TLS（默认关闭）
max_recv_msg_size = 67108864   # 最大接收消息 64MB
max_send_msg_size = 67108864   # 最大发送消息 64MB
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
│   ├── flight/           # Arrow Flight gRPC server (DoGet/DoPut/Flight SQL)
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