# iedb Enterprise Helm Chart

Multi-node iedb Enterprise deployment for Kubernetes with role separation (writer / reader / compactor), automatic failover, and a choice of storage pattern.

## Prerequisites

- Kubernetes 1.24+
- Helm 3.10+
- An iedb Enterprise license key (request at enterprise@luomigateway@vip.qq.com)
- A cluster shared secret (any strong random string)

## Quick Start

### Pattern A — Shared Object Storage (bundled MinIO)

```bash
helm install iedb-ent helm/iedb-enterprise \
  -f helm/iedb-enterprise/values-shared-storage.yaml \
  --set license.key=IEDB-ENT-XXXX-XXXX-XXXX-XXXX \
  --set cluster.sharedSecret.value=$(openssl rand -hex 32) \
  --set minio.credentials.rootUser=iedbminio \
  --set minio.credentials.rootPassword=$(openssl rand -hex 32) \
  --namespace iedb --create-namespace
```

This deploys:
- 1 MinIO pod (bundled S3-compatible storage)
- 1 writer, 2 readers, 1 compactor
- Each iedb pod points at the MinIO bucket as shared storage

The chart refuses to install if any of the required credentials are missing
— there are no weak defaults.

### Pattern B — Local Storage with Peer Replication

```bash
helm install iedb-ent helm/iedb-enterprise \
  -f helm/iedb-enterprise/values-local-storage.yaml \
  --set license.key=IEDB-ENT-XXXX-XXXX-XXXX-XXXX \
  --set cluster.sharedSecret.value=$(openssl rand -hex 32) \
  --namespace iedb --create-namespace
```

This deploys:
- No MinIO
- 1 writer, 2 readers, 1 compactor, each with its own PersistentVolume
- Peer-to-peer file replication keeps nodes in sync

## Choosing a Pattern

| Use case | Recommended pattern |
|----------|--------------------|
| Cloud-native deployment on managed Kubernetes (EKS/GKE/AKS) | Shared |
| Already running S3, MinIO, or Azure Blob in production | Shared |
| Bare metal or on-prem Kubernetes | Local |
| Air-gapped / edge / defense deployments | Local |
| Need lowest possible query latency (local NVMe) | Local |


## Common Configuration

### External S3 (AWS / Azure / Tigris / R2)

```yaml
# values-override.yaml
storage:
  mode: shared
  shared:
    external: true
    bucket: my-iedb-bucket
    region: us-east-1
    endpoint: https://s3.us-east-1.amazonaws.com
    useSSL: true
    usePathStyle: false
    credentials:
      existingSecret: iedb-s3-credentials   # must contain access-key, secret-key

minio:
  enabled: false                           # don't deploy bundled MinIO
```

### Cluster TLS

```yaml
cluster:
  tls:
    enabled: true
    existingSecret: iedb-cluster-tls        # must contain tls.crt, tls.key
```

Create the TLS secret first:

```bash
kubectl create secret tls iedb-cluster-tls --cert=server.crt --key=server.key -n iedb
```

### High Availability (3 writers for failover)

```yaml
writer:
  replicas: 3                              # Raft quorum + failover target
cluster:
  failover:
    enabled: true                          # automatic writer + compactor failover
```

Only the pod with ordinal `-0` bootstraps Raft on first install; the other
writers join via the seed list. Never use `writer.replicas=2` — the chart
rejects it because a Raft quorum of 2 requires both pods to be reachable
(a single failure takes down the cluster). Stick with 1 (dev) or 3+ (HA).

## Operations

### Check cluster status

```bash
kubectl -n iedb port-forward svc/iedb-ent-iedb-enterprise-reader 8000:8000
curl http://localhost:8000/api/v1/cluster | jq
```

### Scale readers

```bash
helm upgrade iedb-ent helm/iedb-enterprise --reuse-values --set reader.replicas=4
```

### Upgrade iedb version

```bash
helm upgrade iedb-ent helm/iedb-enterprise --reuse-values --set image.tag=26.05.1
```

## Uninstall

```bash
helm uninstall iedb-ent -n iedb
kubectl delete pvc -l app.kubernetes.io/instance=iedb-ent -n iedb   # removes data
```

## Full Values Reference

See [values.yaml](./values.yaml) for every configurable option with inline documentation.
