# mmap Arrow Buffer — 零 WAL 数据缓冲设计

## 概述

用 mmap 文件中的 Arrow IPC 流格式替代当前堆内存 ArrowBuffer + WAL。每 measurement 一个固定大小的 mmap 文件，数据以 Arrow record batch 格式追加。文件满后异步 flush 为 Parquet 落盘。mmap 本身提供崩溃持久化，不再需要 WAL。

## 核心变化

| | 当前 | 新方案 |
|---|---|---|
| 缓冲介质 | 堆内存 `TypedColumnBatch` | mmap 文件，Arrow IPC 流格式 |
| 触发条件 | 大小/时间/内存 三条 | 仅空间阈值（10MB）一条 |
| 崩溃保护 | WAL 独立序列化/异步写/回放 | mmap 文件 + 定时 `msync` |
| Flush 执行 | 16 worker + 100 queue 池 | 轻量异步 worker |
| 查询注入 | Arrow API 桥接 + 连接固定 | 同（一期），零桥接 `read_arrow()`（二期）|
| Schema 变化 | 更新 signature | Union schema 扩展 |
| 内存追踪 | `totalMemoryBytes` + `checkMemoryAndEvict` | 不需要 |

## 数据流

```
写入:
  API → parse → 类型化数组 → 编码 Arrow IPC batch
    → 检查 mmap 剩余空间
      ├─ 够: 追加 batch 到 mmap
      └─ 不够: 提交异步 flush → 等待新 mmap → 追加 batch
    → 定时 msync (100ms)

Flush:
  异步 worker:
    读回 mmap 文件所有 batch
    → mergeBatches
    → sortTypedColumnBatchByKeys
    → WriteParquetColumnar
    → storage.Write
    → 清理 mmap 临时文件

查询 (一期 5B):
  获取 pinned conn
  → ipc.NewReader 从 mmap 读 batch
  → RegisterArrowView 注册 TEMP VIEW
  → ArrowQueryOnConn 执行
  → cleanup: reader.Release(), release views, conn.Close()

查询 (二期 5A):
  直接拼 read_parquet([buffer_file, flushed_files])

崩溃恢复:
  扫描 tmpdir/*.arrow
  → ipc.NewReader 流式读取
  → 截断尾部不完整 batch
  → merge + sort + Parquet 落盘
  → 删除临时文件
```

## 组件设计

### MmapArrowBuffer

替代当前 `ArrowBuffer`。结构：

```
MmapArrowBuffer:
  shards [32]*mmapShard          // 同当前 shard 模型
  msyncTicker *time.Ticker       // 100ms
  maxFileSize int64              // 10MB
  tmpDir string                  // /tmp/iedb_buf
  flushQueue chan *flushTask     // 轻量，cap 16
  flushWorker goroutine          // 1 个 worker
```

每个 mmapShard entry：

```
mmapEntry:
  file       *os.File           // mmap 文件
  data       []byte             // mmap 映射区域
  usedBytes  int64              // 已用字节数
  schema     *arrow.Schema      // union schema（内存中）
  startTime  time.Time
  database   string
  measurement string
```

### 写入路径

```
writeBatch(database, measurement, typedColumns, numRecords):
  batchBytes := encodeToArrowIPC(typedColumns, &entry.schema)  // 可能扩展 schema

  // 单批超过文件大小，跳过 mmap 直接走 Parquet（含当前已累积数据）
  if len(batchBytes) > maxFileSize:
    shard.mu.Lock()
    entry := shard.entries[key]
    if entry != nil:
      delete(shard.entries, key)
      shard.mu.Unlock()
      submitFlush(entry)        // 先 flush 积累的数据
    else:
      shard.mu.Unlock()
    submitDirectFlush(typedColumns)  // 这批直接写 Parquet
    return

  shard.mu.Lock()
  entry := getOrCreateEntry(key)     // 不存在则创建新 mmap 文件并映射
  if entry.usedBytes + len(batchBytes) > entry.fileSize:
    // 空间不够：删除旧 entry，释放锁，提交异步 flush，然后开新 entry
    delete(shard.entries, key)
    shard.mu.Unlock()
    submitFlush(entry)
    shard.mu.Lock()
    entry = createEntry(key, database, measurement)  // 新 mmap
    shard.entries[key] = entry
  copy(entry.data[entry.usedBytes:], batchBytes)
  entry.usedBytes += int64(len(batchBytes))
  shard.mu.Unlock()
```

`createEntry` 操作：`os.CreateTemp(tmpDir, "*.arrow")` → `ftruncate(maxFileSize)` → `mmap(0, maxFileSize, PROT_READ|PROT_WRITE, MAP_SHARED)`

### AppendRecordBatch 到 mmap

将 `*TypedColumnBatch` 编码为 Arrow IPC 流格式的 record batch：

1. `array.NewRecord(schema, arrays, numRows)` 创建 Arrow record
2. 通过 `ipc.NewWriter` (stream writer) 将 record 编码为 `[]byte`
3. `copy()` 到 mmap 映射区域

### msync 策略

- 100ms 定时器触发 `msync(MS_ASYNC)` — 非阻塞，通知 OS 尽快刷脏页
- 关闭 mmap 文件前（flush 时）调用 `msync(MS_SYNC)` — 阻塞等待数据完全落盘后，才提交 Parquet 写入任务
- 进程 crash（SIGSEGV/SIGKILL）：内核刷脏页，mmap 数据安全
- 内核 panic / 硬件断电：未 msync 的数据丢失，等效于当前 WAL 在 `fdatasync` 模式下 async channel 中未刷出的数据丢失窗口。如需断电级持久化，可将 `msync_interval_ms` 设为 0（每批次同步），或以 `MS_SYNC` 替代 `MS_ASYNC`

### Flush 执行

```
flushWorker:
  for task := range flushQueue:
    entry := task.entry
    // 读回所有 batch
    reader := ipc.NewReader(bytes.NewReader(entry.data[:entry.usedBytes]))
    batches := readAllBatches(reader)
    merged := mergeBatches(batches)
    sorted := sortTypedColumnBatchByKeys(merged, sortKeys)
    parquetBytes := WriteParquetColumnar(sorted)
    storage.Write(storagePath, parquetBytes)
    entry.file.Close()
    os.Remove(entry.filePath)
```

### 查询注入（一期 5B）

```
injectBufferViews(h, conn, tableNames):
  for each tableName:
    entry := buffer.GetEntry(tableName)
    reader := ipc.NewReader(bytes.NewReader(entry.data[:entry.usedBytes]))
    batches := reader.ReadAll()
    arrays, schema := batchesToArrowArrays(batches)
    release := database.RegisterArrowView(ctx, conn, viewName, schema, arrays)
    releases = append(releases, release)
  return viewNames, releases
```

### Schema 演化（B 方案：Union Schema）

- 内存中维护 `entry.schema` 作为 union schema（所有见到的列的最大集合）
- 新列出现时：`entry.schema` 扩展，新 batch 编码时包含该列
- 已有 batch 在 mmap 中保持原样（缺失列）
- flush 时以 union schema 写 Parquet，缺失列对应旧行的位置填 null

### 崩溃恢复

```
startupRecovery():
  for each .arrow file in tmpDir:
    data := os.ReadFile(filepath)
    reader := ipc.NewReader(bytes.NewReader(data))
    var batches []arrow.Record
    for reader.Next():
      batches = append(batches, reader.Record())
    // reader 在遇到不完整 batch 时自动截断
    merged := mergeBatches(batches)
    sorted := sortTypedColumnBatchByKeys(merged, sortKeys)
    parquetBytes := WriteParquetColumnar(sorted)
    storage.Write(storagePath, parquetBytes)
    os.Remove(filepath)
```

## 配置

```toml
[ingest]
buffer_mmap_size_mb = 10         # 每个 measurement mmap 文件大小
buffer_tmp_dir = "/tmp/iedb_buf" # mmap 临时文件目录  
buffer_msync_interval_ms = 100   # msync 间隔
# 删除: max_buffer_size, max_buffer_age_ms, min_flush_age_seconds
# 删除: flush_timeout, flush_queue_size, flush_workers
# 删除所有 [wal] 配置段
```

## 删除的模块

- `internal/ingest/buffer_pool.go` — 已删除
- `internal/wal/` — WAL 全部代码
- ArrowBuffer 中的 `totalMemoryBytes`、`checkMemoryAndEvict`、`findOldestEntry`、`minAgeFlushLoop`、`flushAgedEntries`
- flush worker 池（16 workers + 100 queue）— 替换为单 worker + cap 16 channel
- 配置中的 WAL 段和 buffer 触发相关字段

## 保留未变的模块

- `convertColumnsToTyped` / `writeTypedColumnarInternal` — 类型转换和 shard 逻辑
- `mergeBatches`、`sortTypedColumnBatchByKeys` — flush 路径
- `WriteParquetColumnar`、`writeRecordToParquet` — Parquet 编码
- `storage.Backend` — 存储写入
- `generateStoragePath` — 路径生成
- `groupByHour` — 时间分区
- `RegisterArrowView`、`ArrowQueryOnConn` — 查询注入
- API handler 层 — LineProtocol / MsgPack / TLE / Import / CQ
