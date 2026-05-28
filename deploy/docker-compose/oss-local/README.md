# IEDB OSS — 单节点，本地存储

最简单的 IEDB 部署：一个容器、一个命名卷、暴露 `:8000` 端口。无集群、无许可证、无反向代理。

## 架构

```
┌──────────────────────────┐
│          IEDB            │
│     (port 8000)          │
│                          │
│  ┌────────────────────┐  │
│  │  /app/data         │  │
│  │    ├── iedb/       │  │
│  │    │   (Parquet)   │  │
│  │    └── wal/        │  │
│  └────────────────────┘  │
└──────────────────────────┘
```

## 使用方式

```bash
# 可选：预设管理员令牌（否则需从日志中获取）
export IEDB_AUTH_BOOTSTRAP_TOKEN=$(openssl rand -hex 32)

docker compose up -d

# 获取管理员令牌（如未预设）
docker compose logs iedb | grep -i "initial admin token"

# 写入数据（行协议格式）
curl -X POST "http://localhost:8000/write?db=mydb" \
  -H "Authorization: Bearer $IEDB_AUTH_BOOTSTRAP_TOKEN" \
  -d 'cpu,host=server01 usage=42.5'

# 查询数据
curl -X POST http://localhost:8000/api/v1/query \
  -H "Authorization: Bearer $IEDB_AUTH_BOOTSTRAP_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM mydb.cpu"}'
```

## 适用场景

- 本地开发和评估
- 单机部署（嵌入式设备、边缘网关）
- 单机足以支撑的小规模负载

S3 存储版本请参见 [`../oss-s3/`](../oss-s3/)。带 Traefik 的版本请参见 [`../oss-traefik/`](../oss-traefik/)。多节点集群请参见 Enterprise 示例。
