# IEDB — Docker Compose 示例

即开即用的 Docker Compose 配置，覆盖所有常见 IEDB 部署拓扑。按需选择对应目录。

## 示例一览

| 目录 | 版本 | 存储 | 代理 | 适用场景 |
|--------|---------|---------|-------|----------|
| [`oss-local/`](./oss-local/) | OSS | 本地磁盘 | 无 | 本地开发、评估、单机小规模负载 |
| [`oss-s3/`](./oss-s3/) | OSS | S3（内置 MinIO） | 无 | 单机对象存储持久化；测试 S3 代码路径 |
| [`oss-traefik/`](./oss-traefik/) | OSS | 本地磁盘 | Traefik v3.6 | 单机需要 TLS/访问日志/共享反向代理 |
| [`enterprise-shared/`](./enterprise-shared/) | Enterprise | 共享 S3（MinIO） | Traefik v3.6 | 高可用集群共享同一存储桶；云原生部署 |
| [`enterprise-local/`](./enterprise-local/) | Enterprise | 本地磁盘 + 节点间复制 | Traefik v3.6 | 物理机/边缘/离线环境高可用集群（无共享存储） |

## 快速启动

```bash
cd deploy/docker-compose/<example>
docker compose up -d
```

每个目录下均有独立 README，包含详细配置说明、环境变量和规模建议。

## OSS 与 Enterprise 对比

- **OSS** — 单节点 IEDB。运行简单，无需许可证。完整的写入+查询功能，无集群/故障转移/RBAC/备份/自动聚合。
- **Enterprise** — 多节点集群，支持角色分离（写入节点/读取节点/压缩节点）、自动故障转移、TLS + 共享密钥认证、分层存储、RBAC 等。需许可证，申请请发邮件至 `luomigateway@vip.qq.com`。

详细对比请参见 [OSS 与 Enterprise 对比]()，两种 Enterprise 拓扑的设计思路请参见 [部署模式]()。
