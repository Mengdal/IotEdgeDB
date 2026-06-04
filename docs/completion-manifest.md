# Completion Manifest（Phase 4，集群模式）详解

## 目录

1. [问题背景](#问题背景)
2. [两种 Manifest 的区别](#两种-manifest-的区别)
3. [架构概览](#架构概览)
4. [状态机](#状态机)
5. [完整生命周期](#完整生命周期)
6. [子进程侧](#子进程侧)
7. [父进程侧（CompletionWatcher）](#父进程侧completionwatcher)
8. [Bridge 桥接层](#bridge-桥接层)
9. [故障恢复](#故障恢复)
10. [安全设计](#安全设计)

---

## 问题背景

### 为什么需要 Completion Manifest

在单机 OSS 版中，Compaction 子进程完成压缩后直接操作存储（写新文件 → 删旧文件），父进程不需要知道细节。

但在**集群模式**中出现了新问题：

```
集群拓扑:
  Node A (Compactor)     Node B (Reader)      Node C (Writer/Reader)
     │                         │                       │
     ├── 运行 compaction       │                       │
     ├── 产生新 Parquet 文件    │                       │
     └── 删除旧源文件           │                       │
```

**核心矛盾**：集群中所有节点通过 **Raft File Manifest**（Raft 共识文件清单）来知道哪些文件存在、由哪个节点持有。Reader 节点根据 Manifest 去对应节点拉取文件来服务查询。如果 Compactor 节点生成了新文件但 Manifest 没有更新，**集群中其他节点根本不知道新文件存在**。

**Completion Manifest 解决的就是这个跨进程、跨节点的通信问题**：
- 子进程完成压缩后，需要通过某种机制通知父进程
- 父进程需要将文件变更（新增/删除）同步到 Raft Manifest
- 由于父进程和子进程是独立的 OS 进程（内存隔离），它们不能共享内存

---

## 两种 Manifest 的区别

IotEdgeDB 中有两个名为 "Manifest" 但完全不同的概念，需要严格区分：

| 维度 | Manifest (`manifest.go`) | CompletionManifest (`completion.go`) |
|------|--------------------------|--------------------------------------|
| **存放位置** | 存储后端（S3/Local/Azure）的 `_compaction_state/` 目录 | **本地磁盘**的 `{TempDir}/.completion/pending/` |
| **持久性** | 跨 Pod 重启存活（存储是耐久的） | 仅本机磁盘存活，Pod 重启后丢失 |
| **作用** | 记录"这个 Job 是否上传成功？如果崩溃了，还有哪些源文件没删除？" | 记录"这个 Job 完成了吗？产出文件是什么？应执行哪些 Raft 命令？" |
| **读者** | 父进程的恢复逻辑（重启后清理残留） | `CompletionWatcher`（父进程侧轮询者） |
| **写入者** | 子进程（通过存储后端写入） | 子进程（通过本地文件系统写入） |
| **跨节点可见** | 是（在共享存储上，其他节点可读） | **否**（本地磁盘，仅当前节点可见） |

### 为什么 CompletionManifest 必须放本地磁盘

```
场景：CompletionManifest 如果放在共享存储（S3）：

  1. Node A (Compactor) 写完 completion manifest 到 S3
  2. Node B (非 Compactor) 的 Watcher 轮询到该 manifest
  3. Node B 尝试执行 Raft 命令 → 但它不是压缩节点, OriginNodeID 错误
     → 或者 Node B 不是 Leader → 转发失败
     → 多个节点争抢同一个 manifest → 竞态条件

  结论：handoff（交接）是单主机的——只有产生该 Job 的节点需要处理它。
        放在本地磁盘避免了跨节点竞态。
```

---

## 架构概览

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         单节点 (Compactor Role)                           │
│                                                                          │
│  ┌──────────────────────┐        ┌──────────────────────────────────┐   │
│  │   Parent Process     │        │   Subprocess (iedb compact)       │   │
│  │                      │        │                                    │   │
│  │  Compaction Manager  │        │   Job.Run()                        │   │
│  │       │              │        │     │                              │   │
│  │       │ 启动子进程    │        │     ├─ Phase 1: 下载文件           │   │
│  │       │──────────────┼────────┼────→│                              │   │
│  │       │              │ stdin  │     ├─ Phase 2: 压缩合并           │   │
│  │       │              │        │     │                              │   │
│  │       │              │        │     ├─ Phase 3: 上传新文件         │   │
│  │       │              │        │     │   ┌──────────────────────┐  │   │
│  │       │              │        │     │   │ write manifest:      │  │   │
│  │       │              │        │     │   │ output_written       │  │   │
│  │       │              │        │     │   │ → {CompletionDir}/   │  │   │
│  │       │              │        │     │   │   job-xxx.json       │  │   │
│  │       │              │        │     │   └──────────┬───────────┘  │   │
│  │       │              │        │     │              │              │   │
│  │  ┌────▼──────────┐   │        │     ├─ Phase 4: 删除旧源文件      │   │
│  │  │ Watcher       │   │        │     │   ┌──────────────────────┐  │   │
│  │  │ (轮询线程)     │   │        │     │   │ rewrite manifest:    │  │   │
│  │  │               │   │        │     │   │ sources_deleted      │  │   │
│  │  │ 定时扫描 Dir  │   │        │     │   └──────────┬───────────┘  │   │
│  │  │       │       │   │        │     │              │              │   │
│  │  │  发现 manifest │   │        │     └──────────────┼──────────────┘   │
│  │  │       │       │   │        │                    │                  │
│  │  │       ├───────┼───┼────────┼── 本地磁盘文件交换 ─┘                  │
│  │  │       │       │   │        │                                       │
│  │  │  applyOne()   │   │        │                                       │
│  │  │       │       │   │        │                                       │
│  │  │  ┌────▼────┐  │   │        │                                       │
│  │  │  │ Bridge  │  │   │        │                                       │
│  │  │  │ (适配器) │  │   │        │                                       │
│  │  │  └────┬────┘  │   │        │                                       │
│  │  │       │       │   │        │                                       │
│  │  └───────┼───────┘   │        │                                       │
│  │          │           │        │                                       │
│  │  ┌───────▼────────┐  │        │                                       │
│  │  │ Raft FSM       │  │        │                                       │
│  │  │ (集群状态机)    │  │        │                                       │
│  │  │                │  │        │                                       │
│  │  │ RegisterFile() │  │        │  其他 Reader 节点                     │
│  │  │ DeleteFile()   │──┼────────┼──→ 通过 Raft Log 复制                │
│  │  │ BatchFileOps() │  │        │     获知新文件 + 拉取                  │
│  │  └────────────────┘  │        │                                       │
│  └──────────────────────┘        └──────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────┘
```

**核心流程三步走：**
1. **子进程**压缩完成 → 写本地 JSON manifest → 继续删除源文件 → 更新 manifest
2. **Watcher**（父进程）定时轮询本地目录 → 发现 manifest → 转换为 Raft 命令
3. **Bridge** 将压缩层的通用结构翻译为 Raft 专用的 `FileEntry` 结构 → 提交给集群

---

## 状态机

CompletionManifest 有三个状态，子进程在运行过程中逐步推进：

```
                    ┌──────────────────┐
                    │  writing_output  │  ← 子进程启动时写入
                    │  (正在写入输出)   │
                    └────────┬─────────┘
                             │
                    ┌────────▼─────────┐
                    │  output_written  │  ← 上传成功后写入 (关键耐久点)
                    │  (输出已写入)     │
                    │  Outputs[] 已填充 │
                    └────────┬─────────┘
                             │
                    ┌────────▼─────────┐
                    │ sources_deleted  │  ← 源文件删除成功后写入
                    │ (源文件已删除)    │
                    │ DeletedSources[] │
                    │ 已填充            │
                    └──────────────────┘
```

### 状态详解

| 状态 | 含义 | Watcher 行为 | 子进程在做什么 |
|------|------|-------------|--------------|
| `writing_output` | 子进程已开始，尚未上传成功 | **忽略**（不读取） | 正在下载文件、执行 DuckDB 压缩、上传新 Parquet |
| `output_written` | 新文件已上传到存储，SHA-256 已知，源文件尚未删除 | 执行 **RegisterFile** 到 Raft Manifest（注册新文件）| 正在逐个删除旧源文件 |
| `sources_deleted` | 所有源文件已从存储中删除 | 执行 **RegisterFile** + **DeleteFile** 到 Raft Manifest，完成后**删除 manifest 文件** | Job 已完成，子进程即将退出 |

### 为什么不是两个状态而是三个

```
如果只有两个状态:
  "未完成" →  Watcher 什么都不做
  "已完成"  → Watcher 同时 RegisterFile + DeleteFile

问题: 如果子进程在删除源文件之前崩溃了:
  - Watcher 已经执行了 DeleteFile → Manifest 认为源文件已删除
  - 但源文件实际上还在存储上
  → "孤儿存储": 文件在磁盘上但 Manifest 不知道
  → Phase 5 Reconciliation 需要清理
  → 更糟: 如果 RegisterFile 也成功了，新文件注册了但旧文件还在

三个状态的设计保证了 "Manifest 先知道新文件，再删除旧文件":
  output_written → RegisterFile (Manifest 记录新文件)
  sources_deleted → DeleteFile (Manifest 删除旧文件)
```

---

## 完整生命周期

### Step 1: 子进程启动 → writing_output

```go
// job.go:261-277
func (j *Job) Run(ctx context.Context) error {
    if j.clusterMode() {
        m := &CompletionManifest{
            JobID:   j.JobID,
            State:   CompletionStateWritingOutput,
            // ... database, measurement, partition_path, tier ...
        }
        writeCompletionManifest(j.CompletionDir, m) // → {CompletionDir}/job-xxx.json
    }
    // ... 继续下载文件 ...
}
```

**写入方式：原子 tmp+rename**

```go
// completion.go:138-186
func writeCompletionManifest(dir string, m *CompletionManifest) error {
    // 1. json.MarshalIndent
    // 2. os.OpenFile(tmp, O_WRONLY|O_CREAT|O_TRUNC, 0600)
    // 3. f.Write(data) + f.Sync()          ← fsync 到磁盘
    // 4. f.Close()
    // 5. os.Rename(tmp, final)             ← 原子替换
    // 任何步骤失败 → 清理 .tmp 残留
}
```

`tmp + rename` 保证了读取者要么看到旧版本（rename 前），要么看到完整的新版本（rename 后），**绝不可能看到半写入的文件**。

### Step 2: 上传成功 → output_written

```go
// job.go:370-375
if j.clusterMode() {
    // 这是关键耐久点！上传成功后才写入此 manifest
    if err := j.writeOutputWrittenManifest(ctx, compactedFile, compactedKey); err != nil {
        // 非致命但 job 失败 → 下个 cycle 重试
        return j.fail(fmt.Errorf("failed to write completion manifest: %w", err))
    }
}
```

`writeOutputWrittenManifest` 做了：
1. **流式 SHA-256** 哈希本地已压缩文件（不重读存储，本地磁盘几乎零开销）
2. 填充 `Outputs[]` → `[{Path, SHA256, SizeBytes, Database, Measurement, PartitionTime, Tier, CreatedAt}]`
3. 状态 → `output_written`
4. 原子写入本地磁盘

```
Manifest JSON:
{
  "job_id": "mydb_cpu_2026_05_29_14_1766998765432100000",
  "database": "mydb",
  "measurement": "cpu",
  "partition_path": "mydb/cpu/2026/05/29/14",
  "tier": "hourly",
  "state": "output_written",
  "outputs": [{
    "path": "mydb/cpu/2026/05/29/14/cpu_20260529_143052_123456789_compacted.parquet",
    "sha256": "a1b2c3d4e5f6...",
    "size_bytes": 36700160,
    "database": "mydb",
    "measurement": "cpu",
    "partition_time": "2026-05-29T14:30:00Z",
    "tier": "hot",
    "created_at": "2026-05-29T14:31:23Z"
  }],
  "deleted_sources": null,
  "created_at": "2026-05-29T14:31:22Z",
  "updated_at": "2026-05-29T14:31:23Z"
}
```

### Step 3: Watcher 发现 manifest → RegisterFile

Watcher 定时（默认 1s）扫描 `CompletionDir`：

```go
// watcher.go:279-301
func (w *CompletionWatcher) poll(ctx context.Context) {
    paths, _ := listPendingCompletionManifests(w.cfg.Dir)
    for _, path := range paths {
        w.applyOne(ctx, path)
    }
}

// watcher.go:305-427
func (w *CompletionWatcher) applyOne(ctx context.Context, path string) {
    manifest, _ := readCompletionManifest(path)

    // 状态 == writing_output → 跳过（子进程还在工作）
    if manifest.State == CompletionStateWritingOutput {
        return
    }

    // 构建 registers 列表（仅在首次看到时）
    if !alreadyRegistered {
        registers := convertOutputsToCompactedFiles(manifest.Outputs)
    }

    // 构建 deletes 列表（仅在 sources_deleted 状态时）
    if manifest.State == CompletionStateSourcesDeleted {
        deletes := convertDeletedSources(manifest.DeletedSources)
    }

    // 一次 Raft 调用完成所有操作
    bridge.BatchFileOps(ctx, registers, deletes)

    // output_written 状态：保留 manifest，等待子进程推进到 sources_deleted
    // sources_deleted 状态：删除 manifest 文件，清理 registeredOutputs 追踪
}
```

**防重复注册机制**：Watcher 维护 `registeredOutputs map[string]struct{}`，记录已注册的 JobID。当 manifest 处于 `output_written` 状态但子进程还在删除文件时，下一个轮询周期不会重复发送 RegisterFile。

### Step 4: 源文件删除成功 → sources_deleted

```go
// job.go:395-399
if j.clusterMode() {
    if err := j.writeSourcesDeletedManifest(); err != nil {
        // 非致命: 源文件已删除，manifest 仍为 output_written
        // Watcher 已注册新文件，下个 cycle 会重试
        j.logger.Warn()...
    }
}
```

`writeSourcesDeletedManifest` 使用 **read-modify-write** 模式：
1. 读回当前的 `output_written` manifest
2. 状态 → `sources_deleted`
3. 填充 `DeletedSources[]`（从 `j.compactedFiles` 获取）
4. 原子写回（tmp+rename）

### Step 5: Watcher 发现 sources_deleted → 执行 DeleteFile + 清理

```
Watcher 下个轮询周期:
  1. 读取 manifest → State == sources_deleted
  2. registers = Outputs → 已注册（通过 registeredOutputs 跳过）
  3. deletes = DeletedSources → 构建 DeleteSourceOp 列表
  4. bridge.BatchFileOps(ctx, registers=nil, deletes) → 只执行删除
  5. deleteCompletionManifest(path) → 删除本地 manifest 文件
  6. delete(registeredOutputs, jobID) → 清理追踪
```

---

## 子进程侧

### SubprocessJobConfig（传入）

父进程通过 stdin JSON 传入配置，包括 `CompletionDir` 和 `JobID`：

```go
// subprocess.go:28-53
type SubprocessJobConfig struct {
    Database      string
    Measurement   string
    PartitionPath string
    Files         []string    // 待压缩源文件列表
    Tier          string
    TempDirectory string
    SortKeys      []string
    MemoryLimit   string      // DuckDB 内存限制

    // Phase 4 字段
    JobID         string      // 父进程生成，确保双方对 manifest 文件名一致
    CompletionDir string      // 本地磁盘路径，非空表示集群模式
    PartitionTime time.Time   // 分区的时间戳（来自 tier scanner）
}
```

### RunSubprocessJob（子进程入口）

```
main.go: compact 子命令
  → runCompactSubcommand()
    → stdin 读取 JSON → SubprocessJobConfig
    → compaction.RunSubprocessJob(&cfg)
      → 重建 Storage Backend (从序列化配置)
      → 新建独立 DuckDB 连接
      → NewJob(&JobConfig{
            Database:      ...,
            CompletionDir: config.CompletionDir,
            JobID:         config.JobID,
            PartitionTime: config.PartitionTime,
         })
      → job.Run(ctx)
        ├── writeCompletionManifest() → writing_output
        ├── downloadFiles() → compactFiles() → uploadFile()
        ├── writeOutputWrittenManifest() → output_written (关键耐久点)
        ├── deleteOldFiles()
        └── writeSourcesDeletedManifest() → sources_deleted
      → stdout JSON → SubprocessJobResult
      → os.Exit(0) → 释放所有 DuckDB 内存
```

### clusterMode() 门控

```go
// job.go:240-242
func (j *Job) clusterMode() bool {
    return j.CompletionDir != ""
}
```

OSS 版 `CompletionDir` 始终为空字符串 → `clusterMode() == false` → 所有 manifest 写入代码路径被跳过 → 行为与 pre-Phase-4 完全一致。

---

## 父进程侧（CompletionWatcher）

### 配置

```go
// watcher.go:96-117
type CompletionWatcherConfig struct {
    Dir          string              // 扫描目录 = {TempDirectory}/.completion/pending
    Bridge       ManifestBridge      // Raft 桥接
    PollInterval time.Duration      // 轮询间隔 (默认 1s)
    ApplyTimeout time.Duration      // 单次桥接调用超时 (默认 5s)
    Logger       zerolog.Logger
}
```

### 轮询循环

```go
// watcher.go:250-272
func (w *CompletionWatcher) loop() {
    ticker := time.NewTicker(w.cfg.PollInterval)
    for {
        select {
        case <-w.ctx.Done():
            // 关机时最后一次排空扫描 (10s 超时上下文)
            drainCtx, _ := context.WithTimeout(context.Background(), 10*time.Second)
            w.poll(drainCtx)
            return
        case <-ticker.C:
            w.poll(w.ctx)  // 正常扫描
        }
    }
}
```

### 错误处理策略

| 错误类型 | 处理方式 |
|---------|---------|
| `ErrNotLeader` | Debug 日志，manifest 保留在磁盘，下个 tick 重试 |
| 其他 Bridge 错误 | Error 日志，manifest 保留在磁盘，下个 tick 重试 |
| 读取 Manifest 失败 | Warn 日志，跳过该文件 |
| 删除 Manifest 失败 (apply 已成功) | Warn 日志，下个 tick 重试删除（幂等） |

### 指标

```go
// watcher.go:155-163
pollsTotal         // 总扫描次数
manifestsSeen      // 观察到的 manifest 数
manifestsApplied   // 成功应用并删除的 manifest 数
manifestsNotLeader // 因非 Leader 跳过的次数
applyErrors        // 非 Leader 的其他错误
registerCalls      // 注册文件总数
deleteCalls        // 删除文件总数
batchCalls         // BatchFileOps 调用次数
lastPollAt         // 最后一次 Poll 的时间
```

---

## Bridge 桥接层

`CompactionBridge` 是压缩包和集群包的**唯一接触点**，实现了 `compaction.ManifestBridge` 接口：

```go
// compaction/watcher.go:84-92
type ManifestBridge interface {
    RegisterCompactedFile(ctx, file CompactedFile) error
    DeleteCompactedSource(ctx, path, reason string) error
    BatchFileOps(ctx, registers []CompactedFile, deletes []DeleteSourceOp) error
}

// cluster/compaction_bridge.go:57
type CompactionBridge struct {
    coord bridgeCoordinator  // 协调器的窄接口
}
```

### 为什么需要 Bridge

```
依赖倒置原则:

  compaction 包 ───── 不应知道 ─────→ Raft / cluster 包
       │                                    │
       │   ManifestBridge (接口)             │
       │   ← 定义在 compaction 包            │
       │                                    │
       └──── CompactionBridge (实现) ────────┘
             ← 定义在 cluster 包

好处:
  1. compaction 包可以单元测试: 用 mock bridge，不需要真实的 Raft 集群
  2. Raft FileEntry 结构变更不影响 compaction 包
  3. 编译时检查: var _ compaction.ManifestBridge = (*CompactionBridge)(nil)
```

### BatchFileOps

Watcher 总是使用 `BatchFileOps`，将一次 manifest 的所有注册+删除操作打包成**单个 Raft log entry**：

```go
// bridge.go:166-210
func (b *CompactionBridge) BatchFileOps(ctx, registers, deletes) error {
    ops := []raft.BatchFileOp{}

    for _, file := range registers {
        ops = append(ops, raft.BatchFileOp{
            Type:    raft.CommandRegisterFile,
            Payload: marshal(RegisterFilePayload{File: raft.FileEntry{...}}),
        })
    }
    for _, del := range deletes {
        ops = append(ops, raft.BatchFileOp{
            Type:    raft.CommandDeleteFile,
            Payload: marshal(DeleteFilePayload{Path, Reason}),
        })
    }

    coord.BatchFileOpsInManifest(ops)  // 1 个 Raft apply，N 个操作
}
```

### 字段翻译

`compaction.CompactedFile` → `raft.FileEntry`：

| 压缩侧字段 | Raft 侧字段 | 说明 |
|-----------|------------|------|
| `Path` | `Path` | 直接映射 |
| `SHA256` | `SHA256` | 对等方验证拉取文件完整性 |
| `SizeBytes` | `SizeBytes` | 文件大小 |
| `Database` | `Database` | 数据库名 |
| `Measurement` | `Measurement` | 测量名 |
| `PartitionTime` | `PartitionTime` | 分区时间，用于 Phase 2/3 的对等拉取路由 |
| — | `OriginNodeID` | Bridge 自动填入 `LocalNodeID()`：压缩节点即文件的源节点 |
| — | `LSN` | FSM 在 apply 时自动填入 Raft log index |
| `Tier` | `Tier` | 存储层级 |
| `CreatedAt` | `CreatedAt` | 文件创建时间 |

---

## 故障恢复

### 场景 1：子进程在 writing_output 时崩溃

```
状态: manifest 文件存在磁盘，State == writing_output
Watcher: 跳过（writing_output 被视为"进行中"）
启动恢复: CleanupOrphanedCompletionManifests()
```

```go
// main.go:681-684
if completionDir != "" {
    orphanTimeout := time.Duration(cfg.Compaction.CompletionOrphanTimeoutMS) * time.Millisecond
    compactionManager.CleanupOrphanedCompletionManifests(orphanTimeout)
}
```

`CleanupOrphanedCompletionManifests` 扫描 `CompletionDir`，找到状态为 `writing_output` 且超过超时时间的 manifest → 删除它们（该 job 未产生有效输出，下个 compaction cycle 会重新执行）。

### 场景 2：子进程在 output_written 后、删除源文件前崩溃

```
状态: manifest == output_written
已发生: 新文件已上传到存储，Raft Manifest 已注册新文件
未发生: 源文件尚未删除
Watcher: 已执行了 RegisterFile
```

下个 compaction cycle：
- 重试 job → 发现新文件已存在 → 跳过压缩
- 执行 `deleteOldFiles()` → 删除源文件
- 写入 `sources_deleted` manifest

### 场景 3：子进程在 sources_deleted 后、manifest 被清理前崩溃

```
状态: manifest == sources_deleted
已发生: 新文件已注册，源文件已删除
Watcher: 执行 DeleteFile + 删除 manifest 文件
```

由于 `deleteCompletionManifest` 是幂等的（文件不存在也不报错），Watcher 下次轮询时直接成功。

### 场景 4：Watcher 在 output_written 状态下关机

```
状态: manifest == output_written，RegisterFile 已执行
关机: Watcher 停止
```

**关机时的排空扫描**：

```go
// watcher.go:258-266
case <-w.ctx.Done():
    // 关闭前最后一次扫描 (10s 超时上下文)
    drainCtx, _ := context.WithTimeout(context.Background(), 10*time.Second)
    w.poll(drainCtx)
    return
```

重启后，Watcher 重新启动 → 扫描 `CompletionDir` → 发现 `output_written` manifest → `registeredOutputs` 为空（内存重启）→ 重新执行 RegisterFile（**幂等操作**，FSM 会处理重复注册）→ 保留 manifest 等待 sources_deleted。

### 场景 5：Raft Leader 不可达

```go
// watcher.go:378-385
if errors.Is(err, ErrNotLeader) {
    w.manifestsNotLeader.Add(1)
    w.logger.Debug()...
    return  // manifest 保留在磁盘，下个 tick 重试
}
```

Bridge 在 Leader 不可达时返回 `ErrNotLeader`，Watcher 不做任何处理，保留 manifest，下个轮询周期自动重试。当新 Leader 选举完成后，Bridge 的 `forward_apply` 机制会自动将 Raft 命令转发到新 Leader。

---

## 安全设计

### JobID 验证

```go
// completion.go:270-287
func validateJobID(id string) error {
    if id == ""                          → 拒绝空 ID
    if strings.ContainsAny(id, "/\\")    → 拒绝路径分隔符
    if strings.ContainsRune(id, 0)       → 拒绝 null 字节 (C 字符串截断防御)
    if strings.Contains(id, "..")        → 拒绝目录穿越
    if strings.HasPrefix(id, ".")        → 拒绝隐藏文件
}
```

JobID 由 `"database_partitionPath_nanos"` 生成，上游已将 `/` 替换为 `_`，但 `validateJobID` 作为**纵深防御**在此再次验证，防止任何未来重构或恶意输入导致的路径穿越攻击。

### 文件权限

```go
// completion.go:148
os.MkdirAll(dir, 0o700)     // CompletionDir: owner only

// completion.go:163
os.OpenFile(tmp, ..., 0o600) // Manifest 文件: owner read/write only
```

### 原子写入保证

`write-tmp + fsync + rename` 模式保证：
- 任何读取者要么看到旧状态，要么看到完整新状态
- 绝不可能看到写入一半的损坏 JSON
- Pod 崩溃/断电时，最多丢失正在写入的 tmp 文件（下次启动可安全清理）

### .tmp 过滤

```go
// completion.go:239-241
if strings.HasSuffix(name, ".tmp") {
    continue  // 跳过正在写入的临时文件
}
```

`listPendingCompletionManifests` 排除了 `.tmp` 后缀文件，确保 Watcher 不会读取子进程正在写入的半成品。

---

## 小结

Completion Manifest 是 Phase 4 在集群模式下实现**压缩子进程 → 父进程 → Raft 集群**三者之间可靠通信的核心机制。其设计遵循：

1. **单一交接点**：manifest 只在产生 Job 的节点本地磁盘上，避免跨节点竞态
2. **原子状态推进**：tmp+rename 原子写入，三态状态机严格有序
3. **容错重试**：Watcher 保留 manifest 直到全部 Raft 命令成功，任何错误都触发重试
4. **幂等操作**：RegisterFile 在 FSM 中幂等，即使 Watcher 重启重放也无副作用
5. **僵尸清理**：启动时清理孤儿 writing_output manifest，防止无限累积
6. **测试友好**：Bridge 接口隔离 Raft 依赖，compaction 包可独立单元测试
