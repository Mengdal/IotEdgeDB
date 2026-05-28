# Arrow IPC Stream Buffer — 零 WAL 数据缓冲设计

## 概述

用文件系统中的 Arrow IPC 流格式替代当前堆内存 ArrowBuffer + WAL。每 measurement 一个常规文件（`os.File`，非 mmap），数据以 Arrow record batch 格式逐批追加写入。文件达到大小阈值后关闭，投递给后台 worker 转换为 Parquet 落盘。文件本身提供崩溃持久化，不再需要 WAL。

## direct write vs mmap 的选择

| | mmap | direct write（选用） |
|---|---|---|
| 追加写入 | copy 进映射区 | `file.Write()` 系统调用 (~2μs) |
| 空间管理 | 预分配+usedBytes 追踪 | `file.Write()` 后检查 size，超额一个 batch 可接受 |
| msync | 需要定时器 | 不需要，走 OS page cache |
| 读回 | 切片直接读（零拷贝） | `os.ReadFile()` (~5ms/10MB，在 Parquet 编码开销面前可忽略) |
| 平台依赖 | mmap/ftruncate/msync | 纯 Go `os.File` |
| 文件句柄 | — | 句柄存于 shard entry，flush 时关闭 |
| 代码复杂度 | 高 | 低 |

## 核心变化

| | 当前 | 新方案 |
|---|---|---|
| 缓冲介质 | 堆内存 `TypedColumnBatch` | 磁盘文件，Arrow IPC 流格式 |
| 触发条件 | 大小/时间/内存 三条 | 仅文件大小阈值一条 |
| 崩溃保护 | WAL 独立序列化/异步写/回放 | 缓冲文件本身就是持久化副本 |
| Flush 执行 | 16 worker + 100 queue 池 | 单转换 worker（Arrow→Parquet） |
| 查询注入 | Arrow API 桥接 + 连接固定 | 同（一期），`read_arrow()` 零桥接（二期） |
| Schema 变化 | 更新 signature | Union schema 扩展 |
| 内存追踪 | `totalMemoryBytes` + `checkMemoryAndEvict` | 不需要 |
| 文件句柄 | — | 句柄存于 shard entry，flush 时关闭 |

## 数据流

```
写入:
  API → parse → 类型化数组 → 编码 Arrow IPC batch
    → 获取/打开文件句柄（LRU 缓存）
    → file.Write(batchBytes)
    → 更新 union schema（如需要）
    → 文件 size > 阈值？
        ├─ 否 → 返回
        └─ 是 → 关闭文件 → 投递转换任务 → 开新文件

转换:
  后台 worker:
    读取 .arrow 文件所有 batch
    → mergeBatches
    → sortTypedColumnBatchByKeys
    → WriteParquetColumnar
    → storage.Write
    → 删除 .arrow 文件

查询 (一期 5B):
  获取 pinned conn
  → 读 .arrow 文件获取 record batches
  → RegisterArrowView 注册 TEMP VIEW
  → ArrowQueryOnConn 执行
  → cleanup

查询 (二期 5A):
  直接拼 read_arrow([buffer_file, ...]) UNION ALL read_parquet([...])

崩溃恢复:
  启动时扫描 tmpdir/*.arrow
  → ipc.NewReader 读取
  → 截断尾部不完整 batch
  → merge → sort → Parquet 落盘
  → 删除临时文件
```

## 组件设计

### ArrowFileBuffer（替代当前 ArrowBuffer）

```
ArrowFileBuffer:
  shards [32]*fileShard            // 同当前 shard 模型
  maxFileSize int64                // 10MB
  tmpDir string                    // /tmp/iedb_buf
  convertQueue chan *convertTask   // cap 16
  convertWorker goroutine          // 1 个 worker
```

### 文件句柄管理

每个 shard entry 直接持有 `*os.File` 句柄，不通过缓存层。首次写入时 `os.OpenFile(path, O_APPEND|O_CREATE|O_WRONLY, 0644)` 打开文件，句柄存于 entry 中。后续写入复用同一句柄。文件达到大小阈值后关闭句柄并投递转换任务，entry 从 shard map 中移除。系统 ulimit 默认 65536 可支持同时打开几万个 measurement 文件，句柄上限远低于此实际活跃数。

### Shard entry

```
fileEntry:
  file       *os.File           // 当前活跃 .arrow 文件
  path       string             // 文件路径
  size       int64              // 已写入字节数
  schema     *arrow.Schema      // union schema（内存中）
  database   string
  measurement string
```

### 写入路径

```
writeBatch(database, measurement, typedColumns, numRecords):
  batchBytes := encodeToArrowIPC(typedColumns, &entry.schema)  // 可能扩展 schema

  shard.mu.Lock()
  entry := getOrCreateEntry(key)  // 不存在则创建新 .arrow 文件
  entry.file.Write(batchBytes)
  entry.size += int64(len(batchBytes))

  if entry.size > maxFileSize:
    delete(shard.entries, key)
    shard.mu.Unlock()
    entry.file.Close()
    submitConvert(entry)
  else:
    shard.mu.Unlock()
```

`getOrCreateEntry`：查 shard map → 不存在则 `os.OpenFile` 打开新 `.arrow` 文件并将句柄存入 entry。

### Arrow IPC 编码

将 `*TypedColumnBatch` 编码为 Arrow IPC 流格式的 record batch：

1. 检查当前 union schema，扩展新列（仅增不删）
2. `array.NewRecord(schema, arrays, numRows)` 创建 Arrow record
3. `ipc.NewWriter` (stream writer) 将 record 编码为 `[]byte`
4. `file.Write(batchBytes)` 写入

### Flush（Arrow → Parquet 转换）

```
convertWorker:
  for task := range convertQueue:
    entry := task.entry
    data := os.ReadFile(entry.path)
    reader := ipc.NewReader(bytes.NewReader(data))
    batches := readAllBatches(reader)
    merged := mergeBatches(batches)
    sorted := sortTypedColumnBatchByKeys(merged, sortKeys)
    parquetBytes := WriteParquetColumnar(sorted)
    storage.Write(storagePath, parquetBytes)
    os.Remove(entry.path)
```

### 持久化策略

- 写入数据通过 `file.Write()` 进 OS page cache，由内核在合适时机刷盘
- 进程 crash（SIGSEGV/SIGKILL）：内核刷脏页，文件数据安全
- 内核 panic / 硬件断电：page cache 中未刷出的数据可能丢失。等效于当前 WAL `fdatasync` 模式下 async channel 中未刷出的数据丢失窗口
- 如需断电级持久化：转换 worker 在 `os.Remove` 前对 `.arrow` 文件调用 `fdatasync()` 确保完全落盘，再删文件

### 查询注入（一期 5B）

```
injectBufferViews(h, conn, tableNames):
  for each tableName:
    entry := buffer.GetEntry(tableName)
    data := os.ReadFile(entry.path)
    reader := ipc.NewReader(bytes.NewReader(data))
    batches := reader.ReadAll()
    arrays, schema := batchesToArrowArrays(batches)
    release := database.RegisterArrowView(ctx, conn, viewName, schema, arrays)
    releases = append(releases, release)
  return viewNames, releases
```

### Schema 演化（B 方案：Union Schema）

- 内存中维护 `entry.schema` 作为 union schema（所有见过列的最大集合）
- 新列出现时：`entry.schema` 扩展，新 batch 编码时包含新列
- 已有 batch 在文件中保持原样（缺失列）
- 转换时以 union schema 写 Parquet，缺失列对应旧行的位置填 null

### 崩溃恢复

```
startupRecovery():
  for each .arrow file in tmpDir:
    data := os.ReadFile(filepath)
    reader := ipc.NewReader(bytes.NewReader(data))
    var batches []arrow.Record
    for reader.Next():
      batches = append(batches, reader.Record())
    // 不完整 batch 自动截断（ipc.NewReader 在读到半截 batch 时报错退出）
    if len(batches) == 0:
      os.Remove(filepath)
      continue
    merged := mergeBatches(batches)
    sorted := sortTypedColumnBatchByKeys(merged, sortKeys)
    parquetBytes := WriteParquetColumnar(sorted)
    storage.Write(storagePath, parquetBytes)
    os.Remove(filepath)
```

## 配置

```toml
[ingest]
buffer_file_size_mb = 10          # 每个 measurement .arrow 文件大小阈值
buffer_tmp_dir = "/tmp/iedb_buf"  # .arrow 临时文件目录
# 删除: max_buffer_size, max_buffer_age_ms, min_flush_age_seconds
# 删除: flush_timeout, flush_queue_size, flush_workers
# 删除所有 [wal] 配置段
```

## 删除的模块

- `internal/ingest/buffer_pool.go` — 已删除
- `internal/wal/` — WAL 全部代码（约 4 个 Go 文件 + 2 个测试文件）
- `cmd/iedb/main.go` 中 WAL 初始化、启动恢复、定时维护、关闭清理代码
- ArrowBuffer 中的 `totalMemoryBytes`、`checkMemoryAndEvict`、`findOldestEntry`、`minAgeFlushLoop`、`flushAgedEntries`
- flush worker 池（16 workers + 100 queue）— 替换为单 worker + cap 16 channel
- `convertColumnsToTyped` 中不再需要的 `[]interface{}` → 类型化数组的中间路径（如果直接编码到 Arrow）
- 配置中的 `[wal]` 段和 buffer 触发相关字段

## 保留未变的模块

- shard 模型和 FNV-1a 哈希分区
- `mergeBatches`、`sortTypedColumnBatchByKeys`
- `WriteParquetColumnar`、`writeRecordToParquet`
- `storage.Backend` — 存储写入
- `generateStoragePath` — 路径生成
- `groupByHour` — 时间分区
- `RegisterArrowView`、`ArrowQueryOnConn` — 查询注入
- API handler 层 — LineProtocol / MsgPack / TLE / Import / CQ
