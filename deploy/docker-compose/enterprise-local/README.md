# IEDB 集群 — 本地存储与节点间复制

本部署方案使用**本地文件系统存储**，节点之间通过点对点文件复制实现数据同步，无需共享 S3。**Traefik v3.6** 作为反向代理，通过 Docker 提供者（Docker provider）自动发现服务。

## 架构

```
                          ┌─────────────────────────────────────┐
                          │             Traefik v3.6            │
                          │      (entrypoint :8000, dash :8080) │
                          └───────────────────┬─────────────────┘
                                              │
          ┌───────────────────────────────────┼───────────────────────────────────┐
          │                                   │                                   │
     writes only                         writes only                        queries only
          │                                   │                                   │
          ▼                                   ▼                                   ▼
 ┌─────────────────┐                ┌─────────────────┐                ┌─────────────────┐
 │    Writer 1     │                │    Writer 2     │                │    Reader 1     │
 │  (Raft Leader)  │                │  (Raft Voter)   │                │  (Query Only)   │
 │                 │                │                 │                │                 │
 │ ┌─────────────┐ │   Raft WAL     │ ┌─────────────┐ │   WAL Repl.    │ ┌─────────────┐ │
 │ │ Local Vol.  │ │◄──────────────►│ │ Local Vol.  │ │──────────────► │ │ Local Vol.  │ │
 │ │ /app/data   │ │                │ │ /app/data   │ │                │ │ /app/data   │ │
 │ └─────────────┘ │                │ └─────────────┘ │                │ └─────────────┘ │
 └─────────────────┘                └─────────────────┘                └─────────────────┘
          │                                   │
          │              Raft WAL             │
          │◄─────────────────────────────────►│
          │                                   │
          ▼                                   ▼
 ┌─────────────────┐
 │    Writer 3     │
 │  (Raft Voter)   │
 │                 │
 │ ┌─────────────┐ │
 │ │ Local Vol.  │ │
 │ │ /app/data   │ │
 │ └─────────────┘ │
 └─────────────────┘
```

## 本地存储 vs 共享存储

| 维度 | 本地存储（本目录） | 共享存储（[../docker-compose/](../docker-compose/)） |
|------|---------------------|---------------------------------------------------|
| **存储方式** | 每个节点独立挂载卷 | 所有节点共用同一存储桶 |
| **数据访问** | 通过节点间文件复制获取 | 直接从共享存储桶读取 |
| **数据复制** | **必需** — 文件传输附带 SHA-256 校验 | 无需复制（共享桶可直接读取） |
| **读取节点扩缩** | 每个读取节点保存完整数据副本 | 直接增加节点即可（无数据拷贝成本） |
| **数据持久性** | 依赖卷的备份策略 | 由存储桶保障数据持久性 |
| **适用场景** | 物理机、本地部署、边缘计算、离线环境 | 云原生、托管 Kubernetes |

## 数据流

### 本地存储模式

```
Write Request
     │
     ▼
┌─────────────┐     Raft Log      ┌─────────────┐
│   Writer 1  │◄─────────────────►│   Writer 2  │
│   (Leader)  │                   │  (Follower) │
└──────┬──────┘                   └─────────────┘
       │
       │ Raft announces new file (manifest)
       │ Peers pull bytes with SHA-256 verification
       ▼
┌─────────────┐
│   Reader 1  │
└─────────────┘
```

1. 写入到达领导者（写入节点 1）
2. 领导者提交 Raft 日志，并将 Parquet 文件刷写到本地卷
3. Raft 将新文件在集群清单中广播
4. 跟随者和读取节点从对等节点拉取数据，通过 SHA-256 校验完整性
5. 每个节点最终在本地磁盘上拥有**完整的独立数据副本**

### 查询路径

```
Query Request
     │
     ▼
┌─────────────┐
│   Reader 1  │ ──► Reads from /app/data (local replica)
└─────────────┘     Bytes fetched peer-to-peer from writers
```

**重要提示：** 读取节点只能查询已拉取到本地的数据。需在所有节点上设置 `IEDB_CLUSTER_REPLICATION_ENABLED=true`（本 compose 文件中已默认配置）。

## 适用场景

**适合以下场景：**
- 开发与测试环境
- 边缘部署（无云环境访问）
- 单机部署
- 低延迟需求（无需访问 S3 等外部存储）

**不推荐用于：**
- 大规模生产环境（卷管理较复杂）
- 跨地域多数据中心部署
- 需要读取节点即时弹性扩缩的场景

## 卷目录结构

每个节点的 `/app/data` 卷包含以下内容：

```
/app/data/
├── raft/           # Raft 共识状态（仅写入节点）
│   ├── logs/       # Raft 日志条目
│   └── snapshots/  # Raft 快照
├── wal/            # 预写日志（Write-Ahead Log）
├── storage/        # Parquet 数据文件
│   └── {database}/
│       └── {measurement}/
│           └── YYYY/MM/DD/HH/*.parquet
└── auth.db         # SQLite 数据库（认证、审计等）
```

## 使用方式

```bash
# 启动集群
export IEDB_LICENSE_KEY="your-enterprise-license-key"
docker compose up -d

# 查看集群状态
curl http://localhost:8000/api/v1/cluster/status

# 写入数据（路由至领导者，复制到跟随者，再同步至读取节点）
curl -X POST "http://localhost:8000/write?db=mydb" \
  -d 'cpu,host=server01 usage=42.5'

# 查询数据（读取节点从其本地副本响应）
curl -X POST "http://localhost:8000/api/v1/query" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM mydb.cpu"}'

# 打开 Traefik 面板（实时查看路由和后端）
open http://localhost:8080
```

## 端口说明

| 服务 | 端口 | 用途 |
|---------|------|---------|
| Traefik | 8000 | 客户端 API（负载均衡） |
| Traefik | 8080 | Traefik 管理面板（生产环境建议关闭） |

内部端口（不对外暴露）：
- 8000：IEDB HTTP API
- 9100：集群协调器（节点间通信）
- 9200：Raft 共识协议

## 与共享存储模式对比

共享存储版本（使用 MinIO）请参见 [../docker-compose/](../docker-compose/)。

| 目录 | 存储后端 | 是否共享存储 |
|--------|---------|----------------|
| `docker-compose/` | S3（MinIO） | 是 — 所有节点读取同一存储桶 |
| `docker-compose-local/` | 本地文件系统 | 否 — 每个节点持有独立副本，文件通过节点间传输 |
