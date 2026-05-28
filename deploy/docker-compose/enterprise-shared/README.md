# IEDB 集群 — Docker Compose（共享存储）

3 写入节点 + 1 读取节点的 IEDB 集群，使用 **Traefik v3.6** 作为反向代理，**MinIO** 提供 S3 兼容的共享存储。Traefik 通过 Docker 提供者自动发现服务 — 路由规则通过容器标签定义，添加或移除写入节点/读取节点时自动负载均衡。

## 架构

```
                         ┌─────────────────────────────────────┐
                         │             Traefik v3.6            │
                         │      (entrypoint :8000, dash :8080) │
                         └───────────────────┬─────────────────┘
                                             │
                    ┌────────────────────────┼────────────────────────┐
                    │                        │                        │
               writes only             writes only              queries only
                    │                        │                        │
                    ▼                        ▼                        ▼
         ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
         │     Writer 1     │    │     Writer 2     │    │     Reader 1     │
         │   (Raft Leader)  │    │   (Raft Voter)   │    │   (Query Only)   │
         │                  │    │                  │    │                  │
         │  • Ingestion ✓   │    │  • Ingestion ✓   │    │  • Ingestion ✗   │
         │  • Query ✓       │    │  • Query ✓       │    │  • Query ✓       │
         │  • Raft ✓        │    │  • Raft ✓        │    │  • Raft ✗        │
         └────────┬─────────┘    └────────┬─────────┘    └────────┬─────────┘
                  │                       │                       │
                  │    ┌──────────────────┤                       │
                  │    │                  │                       │
                  │    ▼                  │                       │
         ┌────────────────────┐           │                       │
         │     Writer 3      │            │                       │
         │   (Raft Voter)    │            │                       │
         │                   │            │                       │
         │  • Ingestion ✓    │            │                       │
         │  • Query ✓        │            │                       │
         │  • Raft ✓         │            │                       │
         └────────┬──────────┘            │                       │
                  │                       │                       │
                  └───────────┬───────────┴───────────────────────┘
                              │
                              ▼
                  ┌───────────────────────┐
                  │    MinIO (S3)         │
                  │   Shared Storage      │
                  │                       │
                  │  All nodes read/write │
                  │  to the same bucket   │
                  └───────────────────────┘
```

## 工作原理

### 节点角色

| 角色 | 数据写入 | 查询 | Raft 共识 | 用途 |
|------|-----------|-------|----------------|----------|
| **Writer（写入节点）** | ✓ | ✓ | ✓ | 接收写入，参与领导者选举 |
| **Reader（读取节点）** | ✗ | ✓ | ✗ | 仅查询，水平扩展读取能力 |

### 请求流程

#### 写入（数据摄取）

1. 客户端向 Traefik 发送写入请求（端口 8000）
2. Traefik 将写入路径（见下方路由表）路由到各写入节点
3. 如果接收请求的写入节点**不是** Raft 领导者 → IEDB 自动转发给领导者
4. Raft 领导者提交 WAL 并复制给跟随者
5. 数据写入 MinIO（S3）

```
Client → Traefik → Writer2 → (forward) → Writer1 (Leader) → MinIO
```

#### 查询

1. 客户端向 Traefik 发送查询请求（端口 8000）
2. Traefik 将 `/api/v1/query` 路由到读取节点
3. 读取节点直接从 MinIO（S3）查询数据
4. 结果返回给客户端

```
Client → Traefik → Reader1 → MinIO → Reader1 → Client
```

**注意：** 写入节点也可提供查询服务。Traefik 将查询路由到读取节点以减轻写入节点负载，但如果查询落到写入节点，它会直接响应 — 无需转发，因为所有节点从同一共享存储读取。

### 为什么需要 3 个写入节点？

Raft 共识需要多数派（quorum）才能选举领导者：

| 写入节点数 | 多数派 | 可容忍故障 |
|---------|--------|--------------|
| 1 | 1 | 0 台故障 |
| 2 | 2 | 0 台故障（存在脑裂风险） |
| **3** | **2** | **1 台故障** |
| 5 | 3 | 2 台故障 |

3 个写入节点时，集群可容忍 1 台写入节点故障，仍能选举出领导者。

### 数据流：WAL 复制 + 共享存储

IEDB 采用两层数据流：

1. **WAL 复制（近期数据）**
   - 写入节点实时将 WAL 条目复制到读取节点
   - 实现刚写入数据的低延迟查询（秒级延迟）
   - 读取节点需配置 `IEDB_CLUSTER_REPLICATION_ENABLED=true`

2. **共享 S3 存储（持久化数据）**
   - 写入节点将数据刷写为 Parquet 文件存储到 S3（MinIO）
   - 所有节点直接从 S3 查询持久化数据
   - 数据一旦刷写到 S3 即持久化

**查询一致性：**

| 数据时效 | 数据来源 | 是否需要 WAL 复制？ |
|----------|--------|---------------------------|
| < 刷写间隔 | 写入节点缓冲区 + WAL | 是（读取节点需要） |
| > 刷写间隔 | S3 Parquet 文件 | 否 |

**共享存储的优势：**
- **无状态计算** — 节点可随时替换，无需数据迁移
- **水平读取扩展** — 新增读取节点无需拷贝数据
- **单一数据源** — S3 作为持久化存储层，全局一致

## 端口说明

| 服务 | 端口 | 用途 |
|---------|------|---------|
| Nginx | 8000 | 客户端 API（负载均衡） |
| MinIO API | 9000 | S3 API |
| MinIO 控制台 | 9001 | Web 管理界面 |

内部端口（不对外暴露）：
- 8000：IEDB HTTP API
- 9100：集群协调器（节点间通信）
- 9200：Raft 共识协议

## 使用方式

```bash
# 启动集群
export IEDB_LICENSE_KEY="your-enterprise-license-key"
docker-compose up -d

# 查看集群状态
curl http://localhost:8000/api/v1/cluster/status

# 写入数据
curl -X POST "http://localhost:8000/write?db=mydb" \
  -d 'cpu,host=server01 usage=42.5'

# 查询数据
curl -X POST "http://localhost:8000/api/v1/query" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM mydb.cpu"}'
```

## 扩缩容

### 添加读取节点

编辑 `docker-compose.yml`，添加一个读取节点配置块 — 无需修改 Traefik 配置，Docker 提供者会自动从 YAML 锚点继承的标签中识别新节点：

```yaml
iedb-reader2:
  <<: *iedb-common
  container_name: iedb-reader2
  hostname: iedb-reader2
  volumes:
    - iedb-reader2-data:/app/data
  environment:
    <<: *iedb-env
    IEDB_CLUSTER_NODE_ID: "iedb-reader2"
    IEDB_CLUSTER_ROLE: "reader"
    IEDB_CLUSTER_ADVERTISE_ADDR: "iedb-reader2:9100"
    IEDB_CLUSTER_REPLICATION_ENABLED: "true"
  labels:
    <<: *traefik-reader-labels
  depends_on:
    - iedb-writer1
```

执行 `docker compose up -d iedb-reader2`，新节点立即出现在 Traefik 的 `iedb-readers` 服务后端池中。

## 生产环境注意事项

1. **使用外部 S3** — 将 MinIO 替换为 AWS S3 或其他托管对象存储
2. **外部负载均衡器** — 在上游终结 TLS（AWS ALB、GCP LB、Cloudflare），然后转发给 Traefik
3. **关闭 Traefik 面板** — 生产环境中移除 `--api.insecure=true` 和 `:8080` 端口映射
4. **持久化存储** — 确保卷已备份
5. **监控** — 配置 Prometheus 指标采集（IEDB 暴露 `/metrics`；Traefik 启用后可在 `:8082` 暴露其自身指标）
