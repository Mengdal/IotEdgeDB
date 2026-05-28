# IEDB OSS — 单节点，S3 存储（内置 MinIO）

单个 IEDB 容器，后端使用内置 MinIO 提供 S3 兼容存储。仍为单节点 IEDB — 仅将本地磁盘替换为对象存储。如需接入 AWS S3 / R2 / Tigris / Azure，移除 `minio` 服务并指向您自己的端点即可。

## 架构

```
┌──────────────┐      ┌──────────────┐
│     IEDB     │─────▶│    MinIO     │
│  (port 8000) │      │  (port 9000) │
└──────────────┘      └──────────────┘
```

## 使用方式

```bash
# 可选：覆盖 MinIO 根凭证 + 预设管理员令牌
export MINIO_ROOT_USER=iedbminio
export MINIO_ROOT_PASSWORD=$(openssl rand -hex 32)
export IEDB_AUTH_BOOTSTRAP_TOKEN=$(openssl rand -hex 32)

docker compose up -d

# 写入数据
curl -X POST "http://localhost:8000/write?db=mydb" \
  -H "Authorization: Bearer $IEDB_AUTH_BOOTSTRAP_TOKEN" \
  -d 'cpu,host=server01 usage=42.5'

# 查询数据
curl -X POST http://localhost:8000/api/v1/query \
  -H "Authorization: Bearer $IEDB_AUTH_BOOTSTRAP_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM mydb.cpu"}'

# MinIO 控制台 — 浏览桶内文件
open http://localhost:9001
```

## 接入外部 S3 / R2 / Tigris / Azure

1. 移除 `minio` 服务和 `depends_on.minio` 配置块
2. 将 `IEDB_STORAGE_S3_*` 环境变量替换为您的凭证和端点：

```yaml
      IEDB_STORAGE_S3_ENDPOINT: "https://s3.us-east-1.amazonaws.com"
      IEDB_STORAGE_S3_FORCE_PATH_STYLE: "false"
      IEDB_STORAGE_S3_USE_SSL: "true"
      IEDB_STORAGE_S3_ACCESS_KEY: "${AWS_ACCESS_KEY_ID}"
      IEDB_STORAGE_S3_SECRET_KEY: "${AWS_SECRET_ACCESS_KEY}"
```

## 适用场景

- 在生产环境之前本地测试 S3 代码路径
- 需要对象存储持久化能力（快照、存储桶版本管理）的单机部署
- 作为 Enterprise 集群部署的过渡方案（Enterprise 模式下所有节点共享同一存储桶，参见 [`../enterprise-shared/`](../enterprise-shared/)）

本地磁盘版本请参见 [`../oss-local/`](../oss-local/)；带 Traefik 的 OSS 部署请参见 [`../oss-traefik/`](../oss-traefik/)。
