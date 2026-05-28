# IEDB OSS — 单节点 + Traefik

IEDB OSS 运行在 **Traefik v3.6** 之后，通过 Docker 提供者自动发现服务。单个 IEDB 容器，本地存储，Traefik 监听 `:8000` 作为入口、`:8080` 作为管理面板。适用于需要 TLS 终结、请求日志或与同主机其他服务共享代理的场景 — 无需 Enterprise 集群。

## 架构

```
┌───────────────────────────────────────┐
│              Traefik v3.6             │
│        (entry :8000, dash :8080)      │
└───────────────────┬───────────────────┘
                    │
                    ▼
           ┌──────────────────┐
           │       IEDB       │
           │ (Docker-network) │
           └──────────────────┘
```

## 使用方式

```bash
export IEDB_AUTH_BOOTSTRAP_TOKEN=$(openssl rand -hex 32)
docker compose up -d

# 通过 Traefik 写入和查询
curl -X POST "http://localhost:8000/write?db=mydb" \
  -H "Authorization: Bearer $IEDB_AUTH_BOOTSTRAP_TOKEN" \
  -d 'cpu,host=server01 usage=42.5'

curl -X POST http://localhost:8000/api/v1/query \
  -H "Authorization: Bearer $IEDB_AUTH_BOOTSTRAP_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM mydb.cpu"}'

# Traefik 管理面板
open http://localhost:8080
```

## 配置 TLS

Traefik 的 `tls` 配置通过标签或 `cert-resolver` 命令行参数接入。以 Let's Encrypt 为例：

```yaml
    command:
      - --entrypoints.websecure.address=:443
      - --certificatesresolvers.le.acme.email=you@example.com
      - --certificatesresolvers.le.acme.storage=/letsencrypt/acme.json
      - --certificatesresolvers.le.acme.tlschallenge=true
    labels:
      traefik.http.routers.iedb.entrypoints: "websecure"
      traefik.http.routers.iedb.tls.certresolver: "le"
```

## 生产环境注意事项

- 将 Traefik 对外暴露前，务必移除 `--api.insecure=true` 和 `:8080` 端口映射
- 配置 TLS（如上）或在上游负载均衡器处终结 TLS
- 可将 `:8000` 绑定到 `127.0.0.1`，由您现有的边缘负载均衡器对外提供服务

## 适用场景

- 需要 TLS、基于路径的路由或访问日志的单机部署
- IEDB 与其他服务共存在同一台主机，共享同一个 Traefik

无代理的最小化 IEDB 部署请参见 [`../oss-local/`](../oss-local/)。S3 存储版本请参见 [`../oss-s3/`](../oss-s3/)。多节点故障转移集群请参见 Enterprise 示例。
