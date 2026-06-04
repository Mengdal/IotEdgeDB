# IotEdgeDB 集群设计文档

## 目录

1. [设计目标](#设计目标)
2. [架构总览](#架构总览)
3. [节点模型](#节点模型)
4. [Raft 共识层](#raft-共识层)
5. [集群状态机（FSM）](#集群状态机fsm)
6. [节点发现与健康检查](#节点发现与健康检查)
7. [请求路由](#请求路由)
8. [WAL 流复制](#wal-流复制)
9. [对等文件复制](#对等文件复制)
10. [故障转移](#故障转移)
11. [Leader 转发](#leader-转发)
12. [Compaction Bridge](#compaction-bridge)
13. [集群安全](#集群安全)
14. [数据分片（Phase 4）](#数据分片phase-4)
15. [部署拓扑](#部署拓扑)
16. [配置参考](#配置参考)
17. [故障场景矩阵](#故障场景矩阵)

---

## 设计目标

IotEdgeDB 集群子系统实现以下核心目标：

| 目标 | 实现方式 |
|------|---------|
| **角色分离** | Writer（写入）、Reader（查询）、Compactor（压缩）独立部署，资源隔离 |
| **共识协调** | 基于 Hashicorp Raft 的集群状态机，保证节点列表和文件清单的一致性 |
| **数据复制** | WAL 实时流复制（Writer → Reader）+ 对等 Parquet 文件拉取 |
| **自动故障转移** | Writer 主备切换 + Compactor 租约动态转移 |
| **请求路由** | 非写入节点自动转发写请求到 Writer，非查询节点转发读请求到 Reader |
| **安全通信** | HMAC 消息认证 + TLS 加密 + Nonce 防重放 |

---

## 架构总览

### 包结构

```
internal/cluster/
├── coordinator.go              ← 中央协调器（主入口，约 1700 行）
├── node.go                     ← 节点定义（身份、角色、状态、统计）
├── registry.go                 ← 内存节点注册表（线程安全，支持回调）
├── router.go                   ← HTTP 请求路由器（负载均衡）
├── health.go                   ← 健康检查器
├── role.go                     ← 角色定义和能力矩阵
├── errors.go                   ← 错误哨兵
├── file_registrar.go           ← 文件清单异步注册器
├── compaction_bridge.go        ← Compaction → Raft 桥接适配器
├── compactor_failover.go       ← Compactor 故障转移管理器
├── writer_failover.go          ← Writer 故障转移管理器
├── forward_apply.go            ← Leader 转发（客户端 + 服务端）
│
├── raft/                       ← Raft 共识子系统
│   ├── node.go                 ←   Raft Node 包装（BoltDB + SnapStore）
│   ├── fsm.go                  ←   集群状态机（节点 + 文件清单）
│   ├── errors.go               ←   错误哨兵
│   └── logger.go               ←   zerolog → hclog 适配器
│
├── protocol/                   ← 节点间 TCP 通信协议
│   ├── messages.go             ←   所有消息类型定义
│   └── codec.go                ←   长度前缀 JSON 编解码
│
├── replication/                ← WAL 流复制
│   ├── protocol.go             ←   复制协议消息定义
│   ├── sender.go               ←   发送端（Writer 侧）
│   └── receiver.go             ←   接收端（Reader 侧）
│
├── filereplication/            ← 对等文件复制
│   ├── puller.go               ←   后台拉取工作池
│   ├── fetch_client.go         ←   TCP 文件获取客户端
│   └── catchup.go              ←   启动追赶扫描器
│
├── security/                   ← 集群安全
│   ├── auth.go                 ←   HMAC-SHA256 消息认证
│   ├── nonce_cache.go          ←   防重放缓存
│   ├── tls.go                  ←   TLS 配置
│   └── raft_stream.go          ←   TLS-wrapped Raft 传输层
│
└── sharding/                   ← 数据分片（Phase 4，早期实现）
    ├── config.go               ←   分片配置
    ├── shardmap.go             ←   分片 → 节点映射
    ├── router.go               ←   分片路由器
    ├── meta.go / meta_fsm.go   ←   元数据集群
    ├── shard_raft.go           ←   每分片 Raft 节点
    ├── shard_replication.go    ←   每分片 WAL 复制
    ├── failover.go             ←   分片故障转移
    ├── aggregation.go          ←   两阶段聚合
    └── scatter_gather.go       ←   分散-收集查询
```

### 组件交互全景图

```
                        ┌──────────────────────────┐
                        │     Raft Cluster          │
                        │  ┌────────────────────┐   │
                        │  │  ClusterFSM        │   │
                        │  │  ├─ Nodes[]        │   │
                        │  │  ├─ PrimaryWriter  │   │
                        │  │  ├─ ActiveCompactor│   │
                        │  │  └─ FileManifest[] │   │
                        │  └────────┬───────────┘   │
                        │           │               │
                        │  ┌────────▼───────────┐   │
                        │  │  Raft Node         │   │
                        │  │  (BoltDB + Snap)   │   │
                        │  └────────────────────┘   │
                        └──────────┬───────────────┘
                                   │ Raft Log 复制
                                   │ (TCP/TLS)
    ┌──────────────────────────────┼──────────────────────────────┐
    │                    Coordinator (每节点 1 个)                  │
    │                                                              │
    │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────────┐  │
    │  │ Registry │  │  Health  │  │  Router  │  │  Forward   │  │
    │  │(节点注册) │  │ Checker  │  │(请求路由) │  │  Apply     │  │
    │  └────┬─────┘  └────┬─────┘  └────┬─────┘  │(Leader转发) │  │
    │       │             │             │         └──────┬─────┘  │
    │       │    ┌────────┴──────┐      │                │        │
    │       │    │  Failover     │      │                │        │
    │       │    │  ├─ Writer    │      │                │        │
    │       │    │  └─ Compactor │      │                │        │
    │       │    └───────────────┘      │                │        │
    └───────┼──────────┼───────────────┼────────────────┼────────┘
            │          │               │                │
   ┌────────▼──┐ ┌────▼─────┐  ┌──────▼──────┐  ┌─────▼──────────┐
   │ WAL Sender│ │  File    │  │HTTP Request │  │ Compaction     │
   │ (Writer)  │ │  Puller  │  │ Forwarding  │  │ Bridge         │
   └─────┬─────┘ └────┬─────┘  └─────────────┘  └────────────────┘
         │            │
   ┌─────▼─────┐ ┌───▼────────┐
   │WAL Receiv│ │Fetch Client│
   │ (Reader) │ │(TCP stream)│
   └───────────┘ └────────────┘
```

---

## 节点模型

### 节点角色

```go
type NodeRole string

const (
    RoleStandalone NodeRole = "standalone"  // 单机模式（OSS 默认）
    RoleWriter     NodeRole = "writer"      // 接受写入，WAL 复制源
    RoleReader     NodeRole = "reader"      // 只读查询，文件拉取
    RoleCompactor  NodeRole = "compactor"   // 运行 Compaction 任务
)
```

### 能力矩阵

| 角色 | CanIngest | CanQuery | CanCompact | CanCoordinate |
|------|-----------|----------|------------|---------------|
| `standalone` | ✓ | ✓ | ✓ | ✗ |
| `writer` | ✓ | ✓ | ✗ | ✓ |
| `reader` | ✗ | ✓ | ✗ | ✗ |
| `compactor` | ✗ | ✗ | ✓ | ✗ |

- `CanCoordinate` 表示该节点可以参与 Raft 共识（作为 Voter）
- Reader 和 Compactor 通常作为 Raft Learner（非投票节点）

### 节点状态机

```
     ┌─────────┐
     │ Joining │ ← 新节点加入集群
     └────┬────┘
          │
     ┌────▼─────┐
     │ Healthy  │ ← 心跳正常
     └────┬─────┘
          │ 连续 N 次心跳失败
     ┌────▼──────┐
     │ Unhealthy │ ← 可能恢复
     └────┬──────┘
          │ 连续 M 次心跳失败
     ┌────▼─────┐
     │  Dead    │ ← 确认死亡，触发故障转移
     └──────────┘

     Healthy / Unhealthy 均可主动转为:
     ┌─────────┐
     │ Leaving │ ← 节点优雅退出
     └─────────┘
```

### Node 结构

```go
type Node struct {
    ID          string        // 唯一标识（来自配置）
    Name        string        // 可读名称
    Role        NodeRole      // 角色
    ClusterName string        // 所属集群名
    State       NodeState     // 当前状态
    WriterState WriterState   // "primary" / "standby" / ""
    Version     string        // IotEdgeDB 版本

    // 网络地址
    RaftAddr    string        // Raft 通信地址
    APIAddr     string        // HTTP API 地址
    CoordAddr   string        // Coordinator TCP 地址

    // 运行时统计（每次心跳更新）
    Stats       NodeStats     // CPU/Memory/IngestRate/QueryRate/Storage

    // 内部追踪
    LastHeartbeat    time.Time
    LastStateChange  time.Time
    ConsecutiveFails int
}
```

---

## Raft 共识层

### 架构

使用 `hashicorp/raft` 库，自定义：

- **FSM**（`ClusterFSM`）：管理集群状态的有限状态机
- **Log Store**（BoltDB）：持久化 Raft Log
- **Stable Store**（BoltDB）：持久化 CurrentTerm 和 VotedFor
- **Snapshot Store**（文件系统）：Raft Snapshot 持久化
- **Transport**（TCP/TLS）：节点间 Raft 通信

```
┌───────────────────────────────────────────────┐
│                 raft.Node                     │
│                                               │
│  ┌─────────┐  ┌──────────┐  ┌─────────────┐  │
│  │ Log     │  │ Stable   │  │ Snapshot    │  │
│  │ Store   │  │ Store    │  │ Store       │  │
│  │(BoltDB) │  │(BoltDB)  │  │(File-based) │  │
│  └─────────┘  └──────────┘  └─────────────┘  │
│                                               │
│  ┌──────────────────────────────────────┐     │
│  │           ClusterFSM                 │     │
│  │  nodes:     map[string]*NodeInfo     │     │
│  │  files:     map[string]*FileEntry    │     │
│  │  primaryID: string                   │     │
│  │  compactorID: string                 │     │
│  └──────────────────────────────────────┘     │
│                                               │
│  ┌──────────────────────────────────────┐     │
│  │         TLSStreamLayer               │     │
│  │    (TLS-wrapped TCP Transport)       │     │
│  └──────────────────────────────────────┘     │
└───────────────────────────────────────────────┘
```

### Raft 配置

```go
type NodeConfig struct {
    NodeID           string        // 本节点 ID
    DataDir          string        // Raft 数据目录
    BindAddr         string        // Raft 绑定地址
    AdvertiseAddr    string        // Raft 广播地址
    Bootstrap        bool          // 是否初始化集群
    Peers            []string      // 初始对等节点

    ElectionTimeout    time.Duration  // 选举超时 (默认 1s)
    HeartbeatTimeout   time.Duration  // 心跳超时 (默认 500ms)
    LeaderLeaseTimeout time.Duration  // Leader 租约超时 (默认 500ms)
    CommitTimeout      time.Duration  // 提交超时 (默认 50ms)

    SnapshotInterval   time.Duration  // 快照间隔 (默认 5min)
    SnapshotThreshold  uint64         // 快照阈值 (默认 10000 条 Log)
    TrailingLogs       uint64         // 快照后保留的 Log 数 (默认 10000)

    TLSConfig          *tls.Config    // 可选 TLS
}
```

### Raft 命令流向

```
调用方 (Coordinator / Bridge)
    │
    ▼
raftNode.Apply(cmd Command, timeout) error
    │
    ├── ① 序列化 Command 为 JSON
    ├── ② ra.Apply(jsonBytes, timeout)
    │       ├── 写入 Raft Log
    │       ├── 复制到 Follower
    │       ├── 多数派确认 (quorum commit)
    │       └── 应用到 FSM
    ├── ③ 检查 FSM Apply 返回值
    │       ├── nil → 成功
    │       └── error → 包装为 ErrManifestApply 返回
    └── ④ 返回给调用方
```

---

## 集群状态机（FSM）

### 管理的状态

`ClusterFSM` 维护四类集群状态：

```
ClusterFSM
├── nodes:  map[string]*NodeInfo     // 节点列表
│   ├── ID, Role, State, Addresses
│   ├── WriterState (primary/standby)
│   └── CoreCount
│
├── primaryWriterID:  string         // 当前主 Writer
├── activeCompactorID: string        // 当前活跃 Compactor（Phase 5 租约）
│
└── files:  map[string]*FileEntry    // 文件清单
    └── filesByDB: map[string]map[string]struct{}  // 数据库 → 文件索引
```

### FSM 命令类型

```go
const (
    CommandAddNode          // 添加节点到集群
    CommandRemoveNode       // 从集群移除节点
    CommandUpdateNode       // 更新节点信息
    CommandUpdateNodeState  // 更新节点状态
    CommandPromoteWriter    // 提升 Standby Writer 为 Primary
    CommandDemoteWriter     // 降级 Primary Writer 为 Standby
    CommandRegisterFile     // 注册新文件到清单
    CommandDeleteFile       // 从清单删除文件
    CommandAssignCompactor  // 指定活跃 Compactor（Phase 5）
    CommandBatchFileOps     // 批量文件操作（单 Raft Entry）
    CommandUpdateFile       // 更新文件元数据
)
```

### FileEntry（文件清单条目）

```go
type FileEntry struct {
    Path          string    // 存储相对路径
    SHA256        string    // 文件内容校验和（对等拉取验证）
    SizeBytes     int64     // 文件大小
    Database      string    // 所属数据库
    Measurement   string    // 所属测量
    PartitionTime time.Time // 分区时间（路由拉取请求）
    OriginNodeID  string    // 源节点 ID（对等方知道从哪拉取）
    Tier          string    // 存储层级（hot/cold）
    CreatedAt     time.Time // 创建时间
    LSN           uint64    // Raft Log Sequence Number（FSM 自动填充）
}
```

### Snapshot 与 Restore

```
Snapshot():
  1. 深拷贝 nodes map → []NodeInfo
  2. 深拷贝 files map → []FileEntry
  3. JSON 序列化 → io.ReadCloser

Restore(rc):
  1. JSON 反序列化
  2. 替换 nodes map
  3. 替换 files map + filesByDB 索引
```

### FSM Callbacks

FSM 提供回调机制，让外部组件响应状态变更：

```go
// 节点变更
fsm.SetCallbacks(onNodeAdded, onNodeRemoved, onNodeUpdated)

// Writer 变更
fsm.SetWriterPromotedCallback(onWriterPromoted)

// Compactor 变更
fsm.SetCompactorAssignedCallback(onCompactorAssigned)

// 文件变更
fsm.SetFileCallbacks(onFileRegistered, onFileDeleted)
```

---

## 节点发现与健康检查

### 启动发现流程

```
Node A (新节点) 启动:
  │
  ├── 1. 创建本地 Node 标识
  ├── 2. 创建 Registry（注册自己）
  ├── 3. 创建 Raft Node（如果是 Bootstrap 节点则初始化集群）
  ├── 4. 创建 HealthChecker
  │
  ├── 5. 启动 acceptLoop() — TCP 监听 CoordinatorAddr
  ├── 6. 启动 discoveryLoop():
  │   │
  │   ├── 如果配置了 Seeds[]:
  │   │   for each seed:
  │   │       ├── TCP Dial seed
  │   │       ├── 发送 MsgJoinRequest（含 HMAC 认证）
  │   │       └── 接收 MsgJoinResponse:
  │   │           ├── 获取 Leader 信息
  │   │           ├── 获取已知节点列表 → 注册到 Registry
  │   │           └── 如果是 Raft Voter → bootstrap Raft
  │   │
  │   └── 定时重试（5s 间隔）
  │
  └── 7. 启动心跳循环（heartbeatLoop）
```

### 心跳协议

```
Heartbeat 格式:
  NodeID + State + IsLeader + Timestamp

发送端（每个节点）:
  heartbeatLoop():
    ticker = 5s
    for each tick:
      for each peer (excluding self):
        TCP Dial → sendHeartbeatToNode()
          → 构建 MsgHeartbeat{NodeID, State, IsLeader, Timestamp}
          → Encode + Write
          → Read MsgHeartbeatAck

接收端（每个节点）:
  handleHeartbeat(conn, msg):
    → 提取 NodeID + State
    → registry.RecordHeartbeat(nodeID, stats) // 更新 LastHeartbeat
    → 发送 MsgHeartbeatAck
```

### 健康判定

```go
// HealthChecker 运行逻辑
checkLoop():
  ticker = HealthCheckInterval (默认 5s)
  for each tick:
    for each node in registry:
      checkNode(node):
        // 检查心跳新鲜度
        if time.Since(node.LastHeartbeat) > 3 * checkInterval {
            healthy = false
        } else {
            healthy = true
        }

        // 状态转换
        if healthy {
            node.UpdateState(StateHealthy)
        } else {
            fails := node.RecordFailedCheck()
            if fails >= UnhealthyThreshold (3) {
                node.UpdateState(StateUnhealthy)
                registry.NotifyUnhealthy(node)  // 触发故障转移
            }
            if fails >= DeadThreshold (9) {
                node.UpdateState(StateDead)
            }
        }
```

---

## 请求路由

### 路由策略

```go
type LoadBalanceStrategy string

const (
    LoadBalanceRoundRobin      // 轮询（默认）
    LoadBalanceLeastConnections // 最少连接
    LoadBalanceRandom          // 随机
)
```

### 写入路由

```
Client → 任意节点 POST /api/v1/write/msgpack
  │
  ├── 本节点 CanIngest?  (Writer / Standalone)
  │   └── Yes → 本地处理
  │
  └── 本节点不能写入 (Reader / Compactor)
      └── Router.RouteWrite(ctx, req)
          │
          ├── 1. 从 Registry 获取 PrimaryWriter
          ├── 2. PrimaryWriter 不可用 → 获取 Standby Writers
          ├── 3. 按 LoadBalanceStrategy 选择目标节点
          ├── 4. 缓冲请求 Body
          ├── 5. HTTP 转发到目标节点 APIAddr
          │       ├── 成功 → 返回响应
          │       └── 失败 → 重试（最多 RouteRetries 次）
          └── 6. 所有候选节点都失败 → ErrRoutingFailed
```

### 查询路由

```
Client → 任意节点 POST /api/v1/query
  │
  ├── 本节点 CanQuery?  (Reader / Writer / Standalone)
  │   └── Yes → 本地处理
  │
  └── 本节点不能查询 (Compactor)
      └── Router.RouteQuery(ctx, req)
          │
          ├── 1. 优先选择 Readers（负载均衡）
          ├── 2. Readers 不可用 → 选择 Writers（负载均衡）
          └── 3. HTTP 转发 + 重试
```

---

## WAL 流复制

### 目的

Writer 节点接受写入时，数据首先进入本地 WAL。WAL 复制将 WAL Entry **实时流式传输**给所有 Reader 节点，确保 Reader 在数据 Flush 到 Parquet 之前就能获取最新数据。

### 架构

```
┌──────────────────────┐          ┌──────────────────────┐
│   Writer Node        │          │   Reader Node        │
│                      │          │                      │
│  WAL Writer          │          │  WAL Receiver        │
│    │                 │          │    │                 │
│    │ Append(data)    │          │    │ connect()       │
│    │                 │          │    │                 │
│    ├──→ 写本地 WAL   │          │    ├──→ TCP Dial    │
│    │                 │          │    │    Writer       │
│    └──→ Replicate()  │          │    │                 │
│         │            │          │    ├──→ MsgReplicate │
│         ▼            │          │    │    Sync         │
│  ┌──────────────┐    │          │    │                 │
│  │ Sender       │    │  TCP     │    ├──→ receiveLoop │
│  │              │────┼──────────┼───→│    │            │
│  │ entryChan    │    │  Stream  │    │    ├── Apply    │
│  │ readers map  │    │          │    │    │  to local  │
│  │ seq counter  │    │          │    │    │  WAL       │
│  └──────────────┘    │          │    │    │            │
│                      │          │    │    └── Apply    │
│                      │          │    │       to       │
│                      │          │    │       Ingest   │
│                      │          │    │       Buffer   │
│                      │          │    │                │
│                      │          │    └── ackLoop()    │
│                      │          │         │           │
│                      │          │         └── 定期 ───┼──→ Sender
│                      │          │            MsgReplicateAck
└──────────────────────┘          └──────────────────────┘
```

### Sender（Writer 侧）

```go
type Sender struct {
    readers   map[string]*ReaderConnection  // 已连接的 Reader
    entryChan chan *ReplicationEntry        // 待发送 Entry（容量 10000）
    sequence  uint64                         // 单调递增序列号
}
```

**发送流程：**
```
WAL Writer 写入 → ReplicationHook 回调
  → entryChan ← Sender.Replicate(entry)
      │
      ▼
  distributionLoop():
    entry := <-entryChan
    for each reader in readers:
      → sendToReader(reader, entry)  // 并行广播
        ├── WriteEntry(conn, entry)  // 长度前缀二进制
        └── 写入失败 → RemoveReader()（Reader 断连）
```

**反压处理：**
```go
// entryChan 满时丢弃 Entry（非阻塞）
select {
case s.entryChan <- entry:
default:
    s.droppedEntries.Add(1)  // 指标计数
    // 数据已在 Writer 本地 WAL 中，Reader 可通过文件拉取追赶
}
```

### Receiver（Reader 侧）

```go
type Receiver struct {
    localWAL      WALWriter          // Reader 本地 WAL
    ingestHandler IngestHandler      // 应用到本地 ArrowBuffer
}
```

**接收流程：**
```
connect():
  → TCP Dial Writer
  → 发送 MsgReplicateSync{ReaderID, LastKnownSequence}
  → 接收 MsgReplicateSyncAck{CurrentSequence, CanResume}

receiveLoop():
  for {
    entry := ReadEntry(conn)
    → applyEntry(entry):
        ├── localWAL.AppendRaw(entry.Payload)   // 写本地 WAL
        │   └── ErrWALDropped → 非致命（仅反压信号）
        └── ingestHandler.ApplyReplicatedEntry(ctx, entry.Payload)
            └── 解析 envelope → 解 MessagePack → ArrowBuffer
  }

ackLoop():
  ticker = 100ms
  → 发送 MsgReplicateAck{LastSequence, ReaderID}
```

### 连接恢复

```
Receiver.connectionLoop():
  for {
    err := connect()
    if err == nil { break }
    sleep(ReconnectInterval)  // 默认 5s
  }

  // 连接成功后
  receiveLoop()
  // 断开后
  → 重新进入 connectionLoop()
```

---

## 对等文件复制

### 目的

当文件通过 Compaction 生成或由 Writer 直接 Flush 后，文件元数据通过 Raft Manifest 广播到所有节点。Reader 节点通过 **对等文件复制**从 Origin 节点拉取文件到本地存储，从而服务查询。

### 架构

```
┌──────────────────────────────┐    ┌──────────────────────────────┐
│   Origin Node (Writer/       │    │   Peer Node (Reader)         │
│   Compactor)                 │    │                              │
│                              │    │                              │
│  Raft FSM                    │    │  Raft FSM                    │
│    │                         │    │    │                         │
│    │ onFileRegistered        │    │    │ onFileRegistered        │
│    │ callback                │    │    │ callback                │
│    │                         │    │    │                         │
│    ▼                         │    │    ▼                         │
│  Coordinator                 │    │  Puller.Enqueue(entry)      │
│    │                         │    │    │                         │
│    │ handleFetchFile()       │    │    │ processEntry()          │
│    │  ├── HMAC 验证          │    │    │                         │
│    │  ├── Path 净化          │    │    │  ┌──────────────────┐   │
│    │  ├── Manifest 检查      │    │    │  │ FetchClient      │   │
│    │  └── Stream 文件 ───────┼────┼────┼──→ Fetch()         │   │
│    │                         │    │    │  │  ├── TCP Dial    │   │
│    │                         │TCP │    │  │  ├── MsgFetch    │   │
│    │                         │    │    │  │  │   File        │   │
│    │                         │    │    │  │  ├── Read Header │   │
│    │                         │    │    │  │  ├── Stream Body │   │
│    │                         │    │    │  │  ├── SHA256      │   │
│    │                         │    │    │  │  │   Verify      │   │
│    │                         │    │    │  │  └── Write to    │   │
│    │                         │    │    │  │     Backend     │   │
│    │                         │    │    │  └──────────────────┘   │
└──────────────────────────────┘    └──────────────────────────────┘
```

### Puller

```go
type Puller struct {
    backend   storage.Backend       // 本地存储
    fetcher   Fetcher               // FetchClient（TCP 文件获取）
    resolver  PeerResolver          // 解析 OriginNodeID → peer addresses
    workers   int                   // 默认 4
    queue     chan *raft.FileEntry  // 容量 1024
    inflight  map[string]struct{}   // 去重
}
```

**拉取流程：**
```
FSM onFileRegistered callback:
  → Puller.Enqueue(entry)
      ├── 跳过 self (OriginNodeID == SelfNodeID)
      ├── 去重检查 (inflight set)
      └── 非阻塞入队

worker():
  for entry := range queue:
    → processEntry(entry):
        重试循环 (最多 3 次):
          ├── 1. 检查本地是否已有文件 (StatFile)
          ├── 2. ResolvePeers(originNodeID, path) → 候选 peer 列表
          ├── 3. 遍历候选 peer:
          │     └── pullOnce(entry, peerAddr, attempt):
          │           ├── tryResumeFromPartial(entry) → 断点续传
          │           ├── fetcher.Fetch(ctx, addr, entry, dst, offset)
          │           │     ├── TCP Dial peer
          │           │     ├── 发送 MsgFetchFile{Path, NodeID, Nonce, HMAC, ByteOffset}
          │           │     ├── 读取 MsgFetchFileAck{Status, SizeBytes, SHA256}
          │           │     ├── 流式读取文件体 → MultiWriter(dst + hasher)
          │           │     └── SHA256 验证
          │           └── writeFileTail() → 写入本地 Backend
          └── 4. 所有 peer 失败 → sleepBackoff(attempt)
```

### 断点续传

```
tryResumeFromPartial(entry):
  if backend is AppendingBackend:
      stat := StatFile(path)
      if stat.Size > 0:
          // 对已有部分进行 SHA-256 哈希
          prefixHasher := sha256FromPartialFile(path, stat.Size)
          return stat.Size, prefixHasher

Fetch(peer, entry, dst, offset, prefixHasher):
  → MsgFetchFile{Path, ByteOffset: offset}
  → 服务端: ReadToAt(path, conn, offset) // 从 offset 开始流式传输
  → 客户端: prefixHasher.Write(body)     // 继续哈希
  → 最终验证: fullHash == entry.SHA256
```

### Catch-up（启动追赶）

```
Coordinator.Start() 时:
  if ReplicationCatchUpEnabled:
    catchupOnce.Do():
      entries := fsm.GetAllFiles()
      过滤: OriginNodeID != SelfNodeID 且本地不存在
      → Puller.RunCatchUp(ctx, entries)
          ├── 遍历所有缺失文件
          ├── 入队（应用高水位反压）
          └── 等待拉取完成或超时
```

---

## 故障转移

### Writer 故障转移

```
┌─────────────────────────────────────────────────────────┐
│                  Writer Failover                        │
│                                                         │
│  正常状态:                                               │
│    Primary Writer ←── WAL Replication ──→ Readers       │
│    Standby Writer(s)  等待接管                           │
│                                                         │
│  故障检测:                                               │
│    HealthChecker → Primary 连续 3 次心跳失败              │
│      → registry.NotifyUnhealthy(node)                   │
│        → WriterFailoverManager.HandleWriterUnhealthy()  │
│                                                         │
│  故障转移流程:                                            │
│    1. 检查 CooldownPeriod (60s) — 防止抖动               │
│    2. selectNewPrimary(exclude primaryID):              │
│       ├── 优先选择 Standby Writers                       │
│       └── 回退选择 Healthy Writers                       │
│    3. raftNode.PromoteWriter(newPrimary, oldPrimary):    │
│       └── Raft Log: CommandPromoteWriter                │
│           → FSM: primaryWriterID = newPrimary           │
│    4. WAL Replication Sender 切换:                       │
│       ├── 旧 Primary 停止 Sender                         │
│       └── 新 Primary 启动 Sender                         │
│    5. Reader 重连到新 Primary                             │
│    6. onFailoverComplete callback                       │
└─────────────────────────────────────────────────────────┘
```

### Compactor 故障转移（Phase 5）

```
┌─────────────────────────────────────────────────────────┐
│               Compactor Failover                        │
│                                                         │
│  Active Compactor 通过 Raft 租约确定:                     │
│    FSM.activeCompactorID = "node-x"                     │
│                                                         │
│  故障检测:                                               │
│    CompactorFailoverManager.checkLoop() (10s)            │
│    → activeCompactor 心跳超时?                           │
│      → triggerFailover()                                │
│                                                         │
│  故障转移流程:                                            │
│    1. selectNewCompactor(exclude oldID):                │
│       ├── 优先选择 RoleCompactor 节点                     │
│       └── 回退选择 RoleWriter 节点                        │
│    2. raftNode.AssignCompactor(newID, oldID):            │
│       └── Raft Log: CommandAssignCompactor              │
│           → FSM: activeCompactorID = newID              │
│    3. FSM callback:                                     │
│       ├── 旧 Compactor: OnLoseCompactor()               │
│       │   └── 停止 Compaction Scheduler + Watcher        │
│       └── 新 Compactor: OnBecomeCompactor()              │
│           └── 启动 Compaction Scheduler + Watcher        │
└─────────────────────────────────────────────────────────┘
```

**动态租约回调（main.go 中的实现）：**
```go
coordinator.SetCompactorCallbacks(
    func() {  // OnBecomeCompactor
        hourlyScheduler.Start()
        dailyScheduler.Start()
        watcher.Start(ctx)
    },
    func() {  // OnLoseCompactor
        hourlyScheduler.Stop()
        dailyScheduler.Stop()
        watcher.Stop()
    },
)
```

---

## Leader 转发

### 问题

非 Leader 节点（如 Compactor）需要操作 Raft Manifest（RegisterFile / DeleteFile），但只有 Leader 可以提交 Raft Log Entry。

### 解决方案

**Forward Apply**：非 Leader 节点将 Raft 命令通过 TCP 转发给 Leader，由 Leader 代为 Apply。

### 协议

```
┌──────────────────────┐          ┌──────────────────────┐
│  Non-Leader Node     │          │  Leader Node         │
│  (Compactor)         │          │  (Writer)            │
│                      │          │                      │
│  Bridge              │          │  handleForwardApply()│
│    │                 │          │    │                 │
│    │ RegisterFile()  │          │    │                 │
│    ▼                 │          │    │                 │
│  forwardApplyTo      │  TCP     │    │                 │
│  Leader(ctx, cmd)    │──────────┼───→│                 │
│    │                 │          │    │                 │
│    ├── 获取 Leader   │          │    ├── HMAC 验证      │
│    │   ID + Address  │          │    ├── Nonce 防重放   │
│    │                 │          │    ├── 角色授权       │
│    ├── getOrDial     │          │    ├── Leader 检查    │
│    │   Leader()      │          │    ├── 命令白名单     │
│    │   (连接缓存)    │          │    │   ├── Register  │
│    │                 │          │    │   ├── Delete    │
│    ├── 构建 HMAC     │          │    │   └── BatchOps  │
│    │   (cmd JSON)    │          │    └── ra.Apply(cmd) │
│    │                 │          │         │            │
│    ├── 发送 Msg      │          │         ▼            │
│    │   ForwardApply  │          │    Raft Log → FSM    │
│    │                 │          │                      │
│    └── 接收 Msg      │          │    MsgForwardApplyAck│
│       ForwardApplyAck│←─────────┼─── {Status, Code}   │
│                      │          │                      │
└──────────────────────┘          └──────────────────────┘
```

### 错误处理

```go
// 瞬时错误 → 返回 ErrNotLeader（Watcher 保留 manifest，稍后重试）
var ErrNoLeaderKnown    = errors.New("no leader known")      // Leader 尚未选举
var ErrLeaderUnreachable = errors.New("leader unreachable")   // Leader 在 Raft 中但不在 Registry

// ForwardApplyAck 错误码
ForwardCodeNotLeader       // 目标节点已不是 Leader → 重新解析 Leader
ForwardCodeAuth            // HMAC 验证失败
ForwardCodeInvalidCommand  // 命令类型不在白名单中
ForwardCodeApplyFailed     // Raft Apply 失败
ForwardCodeRaftUnavailable // Raft 不可用
```

---

## Compaction Bridge

### 目的

`CompactionBridge` 是压缩子系统与集群子系统之间的**唯一接触点**，实现了依赖倒置原则。

```
compaction 包                        cluster 包
     │                                    │
     │ ManifestBridge (接口)               │
     │ ← 定义在 compaction/watcher.go     │
     │                                    │
     └──── CompactionBridge (实现) ────────┘
           ← 定义在 cluster/compaction_bridge.go
```

### 接口

```go
// compaction/watcher.go
type ManifestBridge interface {
    RegisterCompactedFile(ctx, CompactedFile) error
    DeleteCompactedSource(ctx, path, reason) error
    BatchFileOps(ctx, []CompactedFile, []DeleteSourceOp) error
}
```

### 字段翻译

```go
// compaction.CompactedFile → raft.FileEntry
BatchFileOps():
  for each register:
    raft.FileEntry{
        Path:          file.Path,           // 直接映射
        SHA256:        file.SHA256,          // 对等拉取验证
        SizeBytes:     file.SizeBytes,
        Database:      file.Database,
        Measurement:   file.Measurement,
        PartitionTime: file.PartitionTime,   // 路由拉取请求
        OriginNodeID:  coord.LocalNodeID(),  // ← Bridge 自动填充
        Tier:          file.Tier,
        CreatedAt:     file.CreatedAt,
        // LSN: FSM Apply 时自动填充
    }

  // 单次 Raft Apply → O(1) 复杂度（而非 O(N)）
  coord.BatchFileOpsInManifest(ops)
```

---

## 集群安全

### HMAC 消息认证

所有节点间通信（Join、FetchFile、ForwardApply）使用 HMAC-SHA256 认证。

**签名材料（不同操作有不同绑定）：**

| 操作 | 签名格式 | 绑定内容 |
|------|---------|---------|
| JoinRequest | `nonce:nodeID:clusterName:timestamp` | 节点身份 + 集群名 |
| FetchFile | `nonce:nodeID:clusterName:path:timestamp` | + 文件路径（防路径替换） |
| ForwardApply | `nonce:nodeID:clusterName:SHA256(payload):timestamp` | + 命令内容（防命令篡改） |

**防重放机制：**
```go
type NonceCache struct {
    entries map[string]time.Time  // key: "nodeID:nonce"
    ttl     time.Duration         // TTL 内拒绝重复 nonce
}

Track(nodeID, nonce) bool:
  key := nodeID + ":" + nonce
  if _, exists := cache[key]; exists {
      return false  // 重放攻击！
  }
  cache[key] = time.Now()
  return true
```

### TLS 传输加密

```go
func ClusterTLSConfig(cfg *config.ClusterConfig) (*tls.Config, error) {
    // 加载证书和私钥
    cert, _ := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)

    // 最小 TLS 1.2
    return &tls.Config{
        Certificates: []tls.Certificate{cert},
        MinVersion:   tls.VersionTLS12,
        // 可选 mTLS
        ClientAuth:   tls.RequireAndVerifyClientCert,  // 当提供 CA 时
    }
}
```

### 路径净化（FetchFile）

```go
// handleFetchFile 中的防护
sanitizeFetchPath(rawPath):
    ├── filepath.Clean(rawPath)        // 消除 ".." 和多余斜杠
    ├── 拒绝绝对路径
    ├── 拒绝包含 ".." 的相对路径
    └── 拒绝 null 字节注入
```

---

## 数据分片（Phase 4）

### 概览

分片子系统在基础集群之上叠加了**数据分片**层，允许将数据按 database 哈希分布到不同的分片组。

**注意**：分片子系统目前处于早期实现阶段。每分片 Raft 节点运行在"轻量模式"，分片领导权由元数据集群确定。

### 分片映射

```go
type ShardMap struct {
    numShards   int                       // 分片数量（默认 3）
    shards      map[int]*ShardGroup       // shardID → {Primary, Replicas[]}
}

GetShard(database string) int:
    hash := FNV-1a(database)
    return hash % numShards
```

### 元数据集群

```
Meta Cluster (独立 Raft)
  ├── MetaFSM
  │   ├── shardAssignments:  map[int]*ShardGroup
  │   └── nodes:             map[string]*NodeInfo
  │
  └── 自动分片分配:
      OnNodeJoin(node):
        → AddNode → rebalanceShards()
          └── 确保每个分片有 ReplicationFactor 个副本

      OnNodeLeave(nodeID):
        → 移除副本 → 提升 Replica → 重新平衡
```

### 分散-收集查询

```
ScatterGather.Query(ctx, databases, buildRequest):
  │
  ├── 1. 为每个涉及的 database 确定 ShardID
  ├── 2. 去重（多个 database 可能在同一 shard）
  │
  ├── 3. 并行查询每个 shard（Semaphore 控制并发）:
  │     for each shard:
  │       go queryOneShard(ctx, shardID, databases, buildRequest):
  │           ├── 获取 ShardGroup
  │           ├── SelectNode (优先 Replica)
  │           └── HTTP 转发查询
  │
  └── 4. 合并结果:
        ├── 简单查询 → 合并 JSON 数组
        └── 聚合查询 → TwoStageAggregation:
            Phase 1: 各分片执行 PARTIAL 聚合
            Phase 2: 协调节点执行 FINAL 聚合
```

### 两阶段聚合

```
原始 SQL: SELECT AVG(temperature), COUNT(*) FROM sensors WHERE ...

Phase 1 (Partial — 每个分片):
  SELECT SUM(temperature) as __partial_sum_temp,
         COUNT(temperature) as __partial_count_temp,
         COUNT(*) as __partial_count_star
  FROM sensors WHERE ...

Phase 2 (Final — 协调节点):
  SELECT __partial_sum_temp / __partial_count_temp as avg_temp,
         SUM(__partial_count_star) as total_count
  FROM [partial_results...]
```

---

## 部署拓扑

### 最小集群

```
┌──────────────┐
│   Writer     │  ← Primary Writer + Raft Leader
│   (节点 1)    │
└──────┬───────┘
       │ WAL Replication
┌──────▼───────┐
│   Reader     │  ← 查询服务
│   (节点 2)    │
└──────────────┘
```

### 推荐的 5 节点集群

```
┌──────────────┐   ┌──────────────┐
│   Writer     │   │   Writer     │
│   (Primary)  │   │  (Standby)   │
│   Raft Voter │   │  Raft Voter  │
└──────┬───────┘   └──────┬───────┘
       │                   │
       │  WAL Replication  │
       │                   │
┌──────▼───────┐   ┌──────▼───────┐   ┌──────────────┐
│   Reader     │   │   Reader     │   │  Compactor   │
│   (查询服务)  │   │   (查询服务)  │   │  (压缩任务)   │
│ Raft Learner │   │ Raft Learner │   │ Raft Learner │
└──────────────┘   └──────────────┘   └──────────────┘

Raft Quorum: 2/2 Voters → 需要 2 票多数
```

### 共享存储 vs 本地存储

| 拓扑 | 存储 | 文件复制 | 适用场景 |
|------|------|---------|---------|
| **共享存储** | S3/MinIO/Azure | 不需要 | 云部署，节点无状态 |
| **本地存储** | 本地磁盘 | 需要 Peer File Replication | 边缘部署，本地 SSD 性能 |

共享存储模式下，所有节点直接读写同一 S3 Bucket，不需要文件复制。
本地存储模式下，Writer 写入本地磁盘，Reader 通过 Puller 拉取文件。

---

## 配置参考

```toml
[cluster]
enabled = true
node_id = "node-1"
role = "writer"                 # standalone / writer / reader / compactor
cluster_name = "production"

# 网络
coordinator_addr = "0.0.0.0:9001"  # Coordinator TCP 监听
advertise_addr = "10.0.0.1:9001"   # 对外广播地址
seeds = ["10.0.0.1:9001", "10.0.0.2:9001"]  # 种子节点

# Raft
raft_data_dir = "./data/raft"
raft_bind_addr = "0.0.0.0:9002"
raft_bootstrap = false           # 首个节点设为 true
raft_election_timeout = "1s"
raft_heartbeat_timeout = "500ms"
raft_snapshot_interval = "5m"
raft_snapshot_threshold = 10000

# 健康检查
health_check_interval = "5s"
health_check_timeout = "3s"
unhealthy_threshold = 3

# 请求路由
route_timeout = "30s"
route_retries = 3

# WAL 复制
replication_enabled = true
replication_buffer_size = 10000
replication_ack_interval = "100ms"

# 故障转移
failover_enabled = true
failover_timeout_seconds = 30
failover_cooldown_seconds = 60

# 对等文件复制
file_replication_pull_workers = 4
file_replication_queue_size = 1024
file_replication_retry_max_attempts = 3
file_replication_fetch_timeout_ms = 60000
file_replication_catch_up_enabled = true
file_replication_catch_up_barrier_timeout_ms = 300000

# 安全
shared_secret = "${IEDB_CLUSTER_SHARED_SECRET}"  # 必须通过环境变量设置
tls_enabled = true
tls_cert_file = "/etc/iedb/certs/cluster.crt"
tls_key_file = "/etc/iedb/certs/cluster.key"

# 分片（实验性）
[cluster.sharding]
enabled = false
num_shards = 3
shard_key = "database"            # database / measurement
replication_factor = 3
```

---

## 故障场景矩阵

| 故障场景 | 检测方式 | 恢复机制 | 恢复时间 | 数据影响 |
|---------|---------|---------|---------|---------|
| **Writer 进程崩溃** | HealthChecker 心跳超时 → Unhealthy | Writer Failover: Standby 接管 | ~30s (FailoverTimeout) | 崩溃前未复制的 WAL 条目可能丢失；Writer 本地 WAL 重启后恢复 |
| **Reader 进程崩溃** | 同上 | 无自动故障转移（Reader 是无状态查询节点） | 手动/自动重启 | 无数据丢失（Reader 是无状态的） |
| **Compactor 进程崩溃** | 同上 | Compactor Failover: 新节点接管租约 | ~30s | 未完成的 Compaction Job 在下一个 Cycle 重试 |
| **Raft Leader 崩溃** | Raft 选举超时 | 自动选举新 Leader | ~1s (ElectionTimeout) | 无数据丢失（Raft Log 已复制到多数派） |
| **S3 存储不可用** | Storage 操作超时 | Flush 失败 → WAL 保留数据 → 定期重试 | 依赖 S3 恢复 | 无数据丢失（数据在 WAL 中） |
| **网络分区** | 心跳失败 | 多数派分区继续服务；少数派降级为只读 | 分区恢复后自动合并 | 少数派节点数据可能滞后 |
| **WAL 复制链路断开** | Sender → Reader 连接断开 | Receiver 自动重连（5s 间隔） | ~5s | Reader 可能短暂落后，通过文件拉取追赶 |
| **文件拉取失败** | FetchClient SHA-256 校验失败 | ErrChecksumMismatch → 重试（最多 3 次） | 取决于文件大小 | 无影响（不完整文件不提交） |
| **HMAC 重放攻击** | NonceCache 检测 | 拒绝连接 + 日志告警 | 即时 | N/A（安全机制） |
| **Leader 转发失败** | ForwardApply 返回错误 | Watcher 保留 manifest，下个 Cycle 重试 | ~1s (PollInterval) | 无数据丢失（manifest 保留在磁盘） |
