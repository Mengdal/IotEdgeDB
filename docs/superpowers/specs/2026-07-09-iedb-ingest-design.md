# iedb-agent + iotededb 联合设计 Spec

**日期**: 2026-07-09 | **状态**: 草稿

---

## 1. 概述

iedb-agent 是 iotededb 的边缘采集代理（Rust），配合 iotededb 主库（Go）实现：

- 边缘 LP 写入 → WAL + 内存缓冲（按 snapshot_interval 分 chunk）
- 增量 Flush：仅持久化旧 chunk 到 Parquet → S3 或 HTTP，保留最近 chunk 在内存
- iotededb DuckDB 联合查询：S3 Parquet + agent 内存缓冲

### 1.1 部署模式

**模式 A: HTTP 上传（默认，简单部署）**

```
iedb-agent → flush chunk → POST /api/v1/ingest/parquet → iotededb → storage.Backend
iotededb → DuckDB read_parquet('{storage_path}...')
```

**模式 B: S3 直写（多 agent，生产环境）**

```
iedb-agent-01 → flush chunk → S3
iedb-agent-02 → flush chunk → S3
iotededb → DuckDB read_parquet('s3://...')
```

### 1.2 组件职责

| | iedb-agent (Rust) | iotededb (Go) |
|---|---|---|
| 平台 | 边缘（含 ARM32） | 中心服务器 |
| 写入 | LP → WAL → 内存缓冲（chunk 分桶） | — |
| 查询 | Tag + 时间范围，仅内存缓冲 | SQL，DuckDB |
| Flush | 增量：旧 chunk → Parquet → S3 或 HTTP | —（S3 模式）/ 接收 Parquet 存 storage（HTTP 模式） |
| 数据窗口 | snapshot_interval 内的 chunk + 未 flush 的旧 chunk | 全量历史数据 |

---

## 2. iedb-agent 设计

### 2.1 目录结构

```
rwork/iedb-agent/
├── Cargo.toml
├── iedb-agent.toml
├── cross/
│   └── armv7-unknown-linux-gnueabihf.toml
└── src/
    ├── main.rs
    ├── config.rs
    ├── http/
    │   ├── mod.rs
    │   ├── write.rs              # POST /write
    │   └── query.rs              # GET /query
    ├── wal/
    │   ├── mod.rs                # WAL 追加/回放/清理
    │   ├── serialize.rs          # bitcode 序列化
    │   └── snapshot_tracker.rs   # 决定何时 snapshot
    ├── buffer/
    │   ├── mod.rs                # TableBuffer: BTreeMap<chunk_time, Chunk>
    │   ├── chunk.rs              # Chunk: rows + tag_index + min/max time
    │   └── query.rs              # tag + 时间范围过滤
    ├── flush/
    │   ├── mod.rs                # snapshot 调度 + 排序去重
    │   ├── parquet.rs            # Chunk → Parquet bytes
    │   ├── s3.rs                 # S3 上传
    │   └── http_upload.rs        # HTTP 上传
    └── agent/
        └── mod.rs                # 注册 + 心跳
```

### 2.2 核心依赖（ARM32 兼容）

```toml
[dependencies]
# 运行时
tokio = { version = "1", default-features = false, features = ["rt-multi-thread", "macros", "sync", "fs", "io-util", "time"] }
hyper = { version = "1", default-features = false, features = ["server", "http1"] }
hyper-util = "0.1"

# 序列化
bitcode = "0.6"                         # 纯 Rust，ARM32 OK
serde = { version = "1", features = ["derive"] }
serde_json = "1"

# Parquet（仅用于 flush 写出）
parquet = { version = "59", default-features = false, features = ["snap", "flate2"] }
# snap: 纯 Rust 实现，flate2: miniz_oxide 纯 Rust 后端

# LP 解析（crates.io 已发布，纯 Rust，ARM32 兼容）
influxdb-line-protocol = "1.0"

# HTTP 客户端（心跳 + HTTP flush + 查询响应）
reqwest = { version = "0.12", default-features = false, features = ["rustls-tls", "json"] }

# S3（仅 S3 模式启用，轻量方案：reqwest + aws-sigv4）
aws-sigv4 = "1"
http = "1"

# 日志
tracing = "0.1"
tracing-subscriber = "0.3"

# 配置
toml = "0.8"
clap = { version = "4", features = ["derive"] }
```

**ARM32 策略**：禁用所有 C 依赖（zstd, lz4, openssl），TLS 走 rustls+ring。

### 2.3 核心概念：Chunk

```
snapshot_interval = 10m

时间轴:
  │  chunk_0   │  chunk_1   │  chunk_2   │  chunk_3   │
  │ (10:00)    │ (10:10)    │ (10:20)    │ (10:30)    │
  └────────────┴────────────┴────────────┴────────────┘
                                          ↑
                                   end_time_marker
                                   (now - snapshot_interval, 向下取整)

  chunk_0 ~ chunk_2: 可 flush（早于 end_time_marker）
  chunk_3:          保留在内存（晚于 end_time_marker）
```

```rust
// 字段值
enum FieldValue {
    I64(i64),
    F64(f64),
    U64(u64),
    Bool(bool),
    String(String),
}

// 字段类型
enum FieldType {
    I64, F64, U64, Bool, String,
}

// 字段定义
struct FieldDef {
    name: String,
    value_type: FieldType,
}

// 表级 schema（所有 row 和 chunk 共享）
// 遇到新 field 时自动扩展（schema evolution）
struct TableSchema {
    tag_keys: Vec<String>,         // ["host", "region"]
    field_defs: Vec<FieldDef>,     // [{name:"cpu", type:F64}, {name:"mem", type:F64}]
}

// 行 — 仅存值，key 和类型由 TableSchema 定义
struct Row {
    time: i64,                          // 纳秒时间戳
    tag_values: Vec<String>,            // 按 schema.tag_keys 顺序
    field_values: Vec<Option<FieldValue>>, // 按 schema.field_defs 顺序，None = 无值
}

struct Chunk {
    chunk_time: i64,                    // snapshot_interval 对齐后的时间边界
    rows: Vec<Row>,
    tag_index: HashMap<String, HashMap<String, Vec<usize>>>,
    time_min: i64,
    time_max: i64,
    avg_row_bytes: usize,              // 内存估算，rows.len() * avg_row_bytes
    min_wal_seq: u64,                  // 该 chunk 中最早的行来自哪个 WAL 文件
    max_wal_seq: u64,                  // 该 chunk 中最晚的行来自哪个 WAL 文件
}

struct Table {
    name: String,
    schema: TableSchema,                 // 所有 chunk 共享
    chunks: Vec<Chunk>,                  // 按 chunk_time 排序，通常 1~2 个
}
```

**chunk_time 计算**：

```
chunk_time = floor(row.time / snapshot_interval) * snapshot_interval

snapshot_interval = 10m:
  row.time = 10:03 → chunk_time = 10:00
  row.time = 10:08 → chunk_time = 10:00  } 同一个 chunk
  row.time = 10:09 → chunk_time = 10:00
  row.time = 10:12 → chunk_time = 10:10  } 新 chunk
```

写入时：计算 `chunk_time`，二分查找 `Vec<Chunk>` 中对应 chunk，没有则 push 新 chunk。
flush 时：移除 `chunk_time < end_time_marker` 的头部 chunk（通常 1 个）。

**Schema 演化**：写入携带新 field 时，`Table.schema` 自动追加。已有行对应位置为 `None`。

```
写入1: cpu,host=srv01 cpu=75.5,mem=62.3
       → schema.field_defs = [{cpu,F64}, {mem,F64}]
       → row.field_values = [Some(75.5), Some(62.3)]

写入2: cpu,host=srv01 cpu=80.0,dsk=50.0
       → schema.field_defs = [{cpu,F64}, {mem,F64}, {dsk,F64}]
       → 写入2 row = [Some(80.0), None, Some(50.0)]
       → 写入2 的 mem 列为 None
```

### 2.4 配置

```toml
[server]
port = 8080

[data]
dir = "/var/lib/iedb-agent"

[wal]
flush_interval_secs = 1             # WAL buffer 刷盘间隔
max_write_buffer_ops = 100000       # WAL buffer 最大操作数，超限拒绝写入

[flush]
snapshot_interval = "10m"           # snapshot 间隔，也是 chunk 边界
backend = "http"                    # "http" | "s3"
memory_limit = "512MB"              # 内存超限时强制 snapshot + 释放 staging 已覆盖的 chunk

[s3]
bucket = "mybucket"
region = "us-east-1"
endpoint = "https://s3.amazonaws.com"
access_key = "..."
secret_key = "..."

[iotedgedb]
url = "http://iotedgedb:8000"

[agent]
id = "agent-01"
```

### 2.5 HTTP API

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/write?db=<name>` | 写入 LP，text/plain body, 204 |
| GET | `/query?db=<name>&table=<name>&start=<ts>&end=<ts>&tag=<k>=<v>` | 查询内存中所有 chunk |
| GET | `/health` | 200 |

### 2.6 写入链路

```
POST /write?db=mydb
  ↓
1. LP 解析 → Vec<Row>
2. 按 measurement 分组
3. 为每行计算 chunk_time = floor(time / snapshot_interval) * snapshot_interval
4. WAL::write_ops(WalOp::Write(WriteBatch { db, table, rows, chunk_time }))
   → 缓冲在内存 WalBuffer 中
   → 定时 flush_wal(): 序列化所有缓冲 ops → {data_dir}/wal/{seq:020}.wal
   → 调用 WalFileNotifier（即 Buffer）写入内存 chunk
5. 204
```

### 2.7 WAL 设计

```
WAL 角色（双缓冲模式，类似 influxdb3_wal）:

写入:
  LP → WalBuffer（内存，op_limit 限制） → 定时 flush（默认 1s）
       → {data_dir}/wal/{seq:020}.wal（bitcode 序列化）
       → 通知 Buffer: 将 WriteBatch 写入对应 (chunk_time) 的 Chunk

  反压: WalBuffer.op_count >= op_limit → 返回 BufferFull → HTTP 503
        WalBuffer 在每次 flush 后清空，op_count 归零

Snapshot 触发:
  SnapshotTracker 根据 snapshot_interval 决定触发时机
  定时器检查: 上次 snapshot 距今 >= snapshot_interval → 触发 snapshot
  SnapshotDetails.end_time_marker = now - snapshot_interval, 向下取整到 snapshot_interval 边界

回放:
  启动时:
    1. 读 {data_dir}/meta/last_snapshot.json → last_snapshot_seq
       格式: { "snapshot_seq": 5, "flushed_wal_seq": 25 }
    2. 扫描 wal/ 目录，只回放 seq > flushed_wal_seq 的 WAL 文件
    3. 按 seq 排序 → 逐个反序列化 → Buffer::insert()
    4. seq ≤ flushed_wal_seq 的 WAL 已在远端持久化，跳过

清理:
  snapshot 成功后:
    1. 遍历所有 table 的剩余 chunk，取 min_wal_seq 的最小值
    2. snapshot_wal_seq = min(remaining_chunks.min_wal_seq) - 1
    3. 删除 seq ≤ snapshot_wal_seq 的 WAL 文件

    例:
      表 cpu: chunk_10:30 (未 flush), min_wal_seq = 50
      表 mem: chunk_10:30 (未 flush), min_wal_seq = 48
      → snapshot_wal_seq = min(50, 48) - 1 = 47
      → 删除 seq ≤ 47，保留 seq ≥ 48

    原理: WAL 文件可能包含多张表的混合数据
         只有所有表都不再需要某个 WAL 文件时才能删除

文件格式:
  FILE_TYPE_IDENTIFIER ("iedb.a01") + CRC32 + bitcode(WalContents)
  WalContents { wal_file_number, ops: Vec<WalOp>, snapshot: Option<SnapshotDetails> }
```

### 2.8 Snapshot（增量 Flush）

```
触发:
  - 定时器: 上次 snapshot 距今 >= snapshot_interval（默认 10m）
  - 内存超限: buffer 总大小 >= memory_limit → 立即触发 force snapshot
  - force_flush: shutdown 前确保所有数据持久化

流程:
  1. 确定 end_time_marker:
     end_time_marker = floor((now - snapshot_interval) / snapshot_interval) * snapshot_interval
     例如: snapshot_interval=10m, now=10:37 → end_time_marker=10:20

  2. 收集可持久化 chunk:
     Buffer 中所有 chunk_time < end_time_marker 的 Chunk → Vec<SnapshotChunk>

  3. 每个 table 的 chunk 合并:
     a. 排序: 按 time 列排序（chunk 内已按时间有序，多 chunk merge-sort）
     b. 去重: 相同 (tags, time) 的行保留最新的（覆盖写入语义）
     c. Parquet writer → Vec<u8>

  4. 上传:
     S3:   PUT {bucket}/{db}/{table}/{YYYY}/{MM}/{DD}/{HH}/{agent_id}_{ts}_{nanos}.parquet
     HTTP: POST {iotededb}/api/v1/ingest/parquet?db=X&measurement=Y
           Body: parquet bytes

  5. 成功（严格顺序）:
     a. 从 Buffer 移除已持久化的 chunk（释放内存）
     b. 计算 snapshot_wal_seq（见 2.7 节）
     c. 写 {data_dir}/meta/last_snapshot.json，fsync
     d. 删除 WAL 文件（seq ≤ snapshot_wal_seq）
     e. 清理 staging/ 中同名 table 旧文件

     崩溃安全:
       步骤 a-b-c 之间崩溃 → 元数据未写入 → 重启后 WAL 回放重建 chunk
       → 下次 flush 重复上传（写入幂等）
       步骤 c 后 fsync → 元数据已落盘 → 重启后正确跳过已 flush 的 WAL

  6. 失败:
     - Parquet 存 {data_dir}/staging/{db}/{table}/{ts}.parquet
     - chunk 保留在内存（查询仍可见这些数据）
     - WAL 文件保留
     - 后台重试任务定期上传 staging/ 中的文件
```

### 2.9 内存保护

```
后台任务（每 5s）:

  检查 Buffer 总大小:
    total_bytes = sum(chunk.estimated_size() for all chunks)
    
    if total_bytes < memory_limit:
      return  // 一切正常

   chunk 一直保留在内存      → staging/ 中有对应 parquet 💚    ← 内存超限
    就是因为上传没成功          → staging/ 空的                      ← 没有 snapshot 覆盖到这些 chunk

    处理:
    1. 找出 staging/ 已覆盖的 chunk（chunk_time 相同的）→ 安全释放内存
       逻辑: staging/ 中的 parquet 包含该 chunk 的所有数据
             chunk 只是查询缓存，可安全移除
             查询不再看到这些数据（下次心跳更新 min/max 时间范围）
       
       chunk_0 (已 flush 失败，staging 有 parquet)
       chunk_1 (已 flush 失败，staging 有 parquet)  
       chunk_2 (未 flush，staging 无) → 保留
       
       释放 chunk_0, chunk_1 → total_bytes 下降

    2. 释放后仍超限:
       → force snapshot（立即触发 snapshot 给 chunk_2 等未 flush 的 chunk）
       → 新 staging/ 文件产生 → chunk_2 可释放

    3. 全部释放后仍超限（极端情况）:
       → 拒绝写入, HTTP 503
```

**chunk 释放的优先级**：从最老的 chunk 开始释放（chunk_time 最小）。

**Chunk 大小估算**:
```rust
impl Chunk {
    fn estimated_size(&self) -> usize {
        // 每行估算: time(8) + avg tag bytes + avg field bytes + 索引开销
        // 简化：row_count * 平均行大小（运行时统计）
        self.row_count * self.avg_row_bytes
    }
}
```


### 2.10 查询链路

```
GET /query?db=mydb&table=cpu&start=T1&end=T2&tag=host=srv01
  ↓
1. Buffer::get_table(db, table) → 遍历所有 chunk
2. 每个 chunk: tag_index filter → 时间过滤 [T1, T2]
3. 合并所有匹配 Row → JSON 返回
```

### 2.11 Agent 注册与心跳

```
启动:
  POST {iotededb}/api/v1/agents/register { "id": "agent-01", "url": "http://..." }

心跳 (10s):
  POST {iotededb}/api/v1/agents/heartbeat {
    "id": "agent-01",
    "tables_changed": [
      {"db":"metrics","table":"cpu","min_time":T1,"max_time":T2,"row_count":15000}
    ]
  }

tables_changed: 仅包含上次心跳后 min/max/row_count 有变化的表。
min_time/max_time 反映内存中所有 chunk 的总体时间范围。
表所有 chunk 都被 flush 后（row_count=0），从 tables_changed 中移除。
```

---

## 3. iotededb 改动设计

### 3.1 新增配置

```toml
# iedb.toml
[ingest]
agent_heartbeat_timeout = "30s"
agent_disabled = false
```

### 3.2 新增 API

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/v1/agents/register` | agent 注册 |
| POST | `/api/v1/agents/heartbeat` | 心跳 + 表变更 |
| POST | `/api/v1/ingest/parquet` | **HTTP 模式**：接收 Parquet，直接写 storage |

**`POST /api/v1/ingest/parquet`**:
```
Query: db, measurement
Body: application/octet-stream (raw parquet bytes)
行为: storage.Write({db}/{measurement}/{YYYY}/{MM}/{DD}/{HH}/{ts}.parquet)
     不走 ArrowBuffer
```

### 3.3 Agent 注册表

```go
AgentRegistry:
  agents: agent_id → { id, url, last_heartbeat, online }
  table_agents: "db.table" → [agent_id]
  agent_tables: agent_id → { "db.table" → {min_time, max_time, row_count} }

超时清理: last_heartbeat > 30s → offline, 清除关联
```

### 3.4 查询合并

```
POST /api/v1/query { "sql": "SELECT * FROM cpu WHERE time > now() - 5m" }
  ↓
1. SQL 解析 → 目标表及时间范围（复用 partition pruner 时间提取）
2. 查 AgentRegistry → 关联 agent 列表
3. 并行 HTTP GET agent/query?db=...&table=...&start=T1&end=T2
   （无 time 过滤则不传 start/end）
4. JSON → Arrow RecordBatch → DuckDB VIEW _agent_{id}
5. SQL 改写:
     FROM cpu
     → FROM (
       SELECT * FROM read_parquet('{storage}/cpu/**/*.parquet')
       UNION ALL SELECT * FROM _agent_agent_01
       UNION ALL SELECT * FROM _agent_agent_02
     )
6. DuckDB 执行 → 返回
```

容错: 单 agent 超时 → 跳过 + WARN，全部失败 → 仅查 Parquet。

### 3.5 文件改动

| 文件 | 操作 | 说明 |
|------|------|------|
| `internal/agent/registry.go` | 新增 | Agent 注册表 |
| `internal/api/agent_routes.go` | 新增 | register + heartbeat + ingest/parquet |
| `internal/api/query_agent_merge.go` | 新增 | 查询合并（HTTP + DuckDB VIEW） |
| `internal/config/config.go` | 修改 | ingest 配置段 |
| `internal/api/query.go` | 修改 | SQL 改写增加 agent VIEW |
| `cmd/iedb/main.go` | 修改 | 初始化 AgentRegistry |

---

## 4. S3 路径约定

```
{bucket}/{db}/{table}/{YYYY}/{MM}/{DD}/{HH}/{agent_id}_{timestamp}_{nanos}.parquet

示例:
mybucket/metrics/cpu/2026/07/09/14/agent-01_20260709_140523_000123456.parquet
```

---

## 5. 错误处理

| 场景 | 行为 |
|------|------|
| WAL buffer 满 | 拒绝写入，HTTP 503 |
| S3/HTTP upload 失败 | Parquet → staging/，chunk 保留内存（查询可见），WAL 保留，后台重试 |
| 内存超限 + staging 覆盖 chunk | 安全释放 chunk（staging 已持久化）, 查询不可见 |
| 内存超限 + staging 为空 | force snapshot → 新 staging 文件 → 释放 chunk → 仍超限则 503 |
| 心跳失败 | 退避重试 |
| 查询 agent 超时(2s) | 跳过该 agent，仅查 Parquet + 其他 agent，WARN |
| 启动 WAL 回放 | 重建所有 chunk 到内存 |
| 启动时 staging/ 中有文件 | 后台重试上传 |
| Agent 重启（id 不变） | register 覆盖旧条目 |

### 内存反压链

```
1. WAL buffer op_limit    → 写入太快 → BufferFull → HTTP 503
2. memory_limit           → 缓冲太大 → force snapshot → 释放 staging 已覆盖 chunk
3. 仍超限                 → 拒绝写入 → HTTP 503
```

---

## 6. ARM32 兼容性

| 依赖 | ARM32 | 说明 |
|------|-------|------|
| tokio, hyper, reqwest | ✅ | 纯 Rust |
| bitcode, serde_json | ✅ | 纯 Rust |
| influxdb_line_protocol | ✅ | 纯 Rust |
| parquet (snap + flate2) | ✅ | 纯 Rust 压缩 |
| ring (rustls) | ✅ | arm 汇编 |
| aws-sigv4 | ✅ | 纯 Rust |
| zstd, lz4, openssl | ❌ | C 依赖，禁用 |

交叉编译: `cargo build --target armv7-unknown-linux-gnueabihf --release`

---

## 7. 非目标

- iotededb 不新增自身写入缓冲
- iedb-agent 不支持 SQL
- 不做 agent 间数据同步
- iedb-agent 不依赖 Arrow/DataFusion/Flight/object_store
