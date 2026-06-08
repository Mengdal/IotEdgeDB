# iedb Benchmarks

端到端基准测试工具集。所有程序需要先启动 iedb 实例，再单独 `go run` 运行。

## 目录

- [sustained_bench](#sustained_bench) — 持续写入压力测试
- [log_bench](#log_bench) — 日志写入吞吐对比
- [query_bench](#query_bench) — 单条查询延迟测试
- [query_suite](#query_suite) — 查询性能回归测试套件
- [clickbench](#clickbench) — 标准 ClickBench 分析查询测试
- [bloom_filter_bench](#bloom_filter_bench) — Bloom Filter 效果测试
- [c_bench](#c_bench) — C 语言持续压力测试
- [loki-benchmark.yaml](#loki-benchmarkyaml) — Loki 本地配置

---

## sustained_bench

**用途**：持续写入压力测试，测长时间高负载下的写入吞吐和稳定性。支持多种数据库对比。

**数据来源**：程序自动生成，无需外部数据集。

**支持的数据类型**（`--data-type`）：
- `iot` — IoT 传感器数据（host、cpu_idle、cpu_user、value）
- `financial` — 金融数据
- `industrial` — 工业数据
- `aerospace` — 航空数据
- `energy` — 能源数据
- `racing` — 赛车数据（车号、车手、速度、转速、油门）

**支持的数据库**（`--target`）：`iedb`、`clickhouse`、`clickhouse-http`、`timescaledb`、`influxdb3`

**主要参数**：
- `--duration` — 测试时长（秒）
- `--workers` — 并发写入数
- `--batch-size` — 每批记录数
- `--protocol` — 协议：`msgpack`（默认）、`lineprotocol`
- `--compress` — 压缩：`none`、`gzip`、`zstd`

**示例**：
```bash
# iedb 写入吞吐，跑 60 秒，50 并发
go run benchmarks/sustained_bench/main.go --duration 60 --workers 50

# 和 ClickHouse 对比 IoT 数据
go run benchmarks/sustained_bench/main.go --target clickhouse --data-type iot --duration 60

# 大批量 + zstd 压缩
go run benchmarks/sustained_bench/main.go --batch-size 100000 --workers 10 --compress zstd
```

---

## log_bench

**用途**：日志写入吞吐对比。支持 iedb vs Elasticsearch / OpenSearch / ClickHouse / VictoriaLogs / Loki / Quickwit。

**数据来源**：程序自动生成随机日志记录（level、service、host、message、trace_id 等字段）。

**支持的数据库**（`--target`）：

| Target | 默认端口 | 说明 |
|--------|---------|------|
| `iedb` | 8000 | 默认 |
| `elastic` | 9200 | Elasticsearch |
| `opensearch` | 9200 | OpenSearch |
| `clickhouse` | 8123 | ClickHouse HTTP |
| `clickhouse-native` | 9000 | ClickHouse Native |
| `victorialogs` | 9428 | VictoriaLogs |
| `loki` | 3100 | Grafana Loki |
| `quickwit` | 7280 | Quickwit |

**主要参数**：
- `--duration` — 测试时长（秒）
- `--workers` — 并发数
- `--batch-size` — 每批记录数
- `--compress` — 压缩：`none`、`gzip`、`zstd`

**示例**：
```bash
go run benchmarks/log_bench/main.go --target iedb --duration 60
go run benchmarks/log_bench/main.go --target elastic --workers 50 --compress gzip
```

---

## query_bench

**用途**：手动指定一条 SQL，测量查询延迟和吞吐。支持 JSON 和 Arrow IPC 两种响应格式。

**数据来源**：需要先在 iedb 中导入数据。

**主要参数**：
- `--query` — SQL 查询语句
- `--database` — 数据库名
- `--format` — 响应格式：`json`（默认）、`arrow`
- `--iterations` — 重复次数
- `--rows` — 预期行数（用于计算吞吐）

**示例**：
```bash
go run benchmarks/query_bench/main.go --database production --query "SELECT * FROM cpu LIMIT 100000"
go run benchmarks/query_bench/main.go --database production --format arrow --query "SELECT * FROM cpu LIMIT 500000"
go run benchmarks/query_bench/main.go --iterations 10 --format json
```

---

## query_suite

**用途**：查询性能回归测试套件。跑一组预定义查询，检测代码改动后查询有没有变慢。

**数据来源**：需要先在 iedb 中导入数据。

**预设**：
- `generic`（默认）— 通用时序数据，20 条查询（LIMIT 扫描、时间范围、time_bucket 聚合、GROUP BY、分位数等）
- `logs` — 日志数据，9 条查询（level/service 过滤、全文搜索、GROUP BY 等）

**主要参数**：
- `--preset` — 预设：`generic`、`logs`
- `--database` — 数据库名
- `--measurement` — measurement 名
- `--target` — 输出格式：`iedb`（JSON）、`iedb-arrow`（Arrow IPC）
- `--iterations` — 每条查询重复次数

**示例**：
```bash
go run benchmarks/query_suite/main.go --database hb --measurement meter
go run benchmarks/query_suite/main.go --preset logs --database logs --measurement logs
go run benchmarks/query_suite/main.go --target iedb-arrow --database hb --measurement meter
```

---

## clickbench

**用途**：标准 ClickBench 测试。运行 43 条分析查询（COUNT、GROUP BY、LIKE、REGEXP、窗口函数等），是业界通用的 OLAP 性能基准。

**数据来源**：需要先导入 ClickBench hits 数据集（~1 亿行网站访问日志）。

**导入数据**：
```bash
# 下载数据集
wget https://datasets.clickhouse.com/hits_compatible/hits.parquet

# 导入到 iedb
curl -X POST http://localhost:8000/api/v1/import/parquet \
  -H "x-iedb-database: clickbench" \
  -H "Authorization: Bearer <token>" \
  -F "file=@hits.parquet" \
  -F "measurement=hits"
```

**示例**：
```bash
go run benchmarks/clickbench/main.go
go run benchmarks/clickbench/main.go --iterations 5 --csv
```

---

## bloom_filter_bench

**用途**：测试 Bloom Filter 对不同类型查询的加速效果。分 4 类查询对比：

| 类别 | 示例 | Bloom Filter 效果 |
|------|------|-------------------|
| 精确匹配 | `WHERE level = 'ERROR'` | 收益最大 |
| 前缀搜索 | `WHERE message LIKE 'Failed%'` | 部分有效 |
| 子串搜索 | `WHERE message LIKE '%timeout%'` | 无效（基线） |
| 聚合查询 | `GROUP BY level` | 无关 |

**数据来源**：程序自动生成 100 万条日志记录并导入 iedb。

**流程**：先写入数据（ingestion phase），等待 flush，再跑 13 条查询（query phase）。

**示例**：
```bash
go run benchmarks/bloom_filter_bench/main.go
```

---

## c_bench

**用途**：C 语言实现的持续压力测试。需要先编译 C 代码。

**示例**：
```bash
cd benchmarks/c_bench
gcc -O2 -o sustained_bench sustained_bench.c
./sustained_bench
```

---

## loki-benchmark.yaml

**用途**：Grafana Loki 本地配置文件，给 `log_bench --target loki` 对比测试用。

**配置要点**：本地文件系统存储、关闭认证、限速拉满。

**用法**：
```bash
# 启动 Loki
loki -config.file=benchmarks/loki-benchmark.yaml

# 跑对比测试
go run benchmarks/log_bench/main.go --target loki --duration 60
```
