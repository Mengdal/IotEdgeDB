# IotEdgeDB WAL 耐久性层设计文档

## 目录

1. [设计目标](#1-设计目标)
2. [架构总览](#2-架构总览)
3. [文件格式规范](#3-文件格式规范)
   - [3.1 文件头](#31-文件头)
   - [3.2 条目格式](#32-条目格式)
   - [3.3 数据库名信封](#33-数据库名信封)
   - [3.4 文件命名](#34-文件命名)
4. [写入路径](#4-写入路径)
   - [4.1 三条写入路径](#41-三条写入路径)
   - [4.2 异步写入循环](#42-异步写入循环)
   - [4.3 文件轮转](#43-文件轮转)
   - [4.4 背压与丢弃处理](#44-背压与丢弃处理)
5. [同步策略](#5-同步策略)
   - [5.1 三种同步模式](#51-三种同步模式)
   - [5.2 批量同步算法](#52-批量同步算法)
6. [恢复机制](#6-恢复机制)
   - [6.1 启动时恢复](#61-启动时恢复)
   - [6.2 周期性维护](#62-周期性维护)
   - [6.3 故障恢复](#63-故障恢复)
   - [6.4 防数据重复机制](#64-防数据重复机制)
7. [读取器](#7-读取器)
   - [7.1 条目反序列化策略](#71-条目反序列化策略)
   - [7.2 损坏容错](#72-损坏容错)
8. [清理策略](#8-清理策略)
   - [8.1 三种清理方法](#81-三种清理方法)
   - [8.2 安全边界计算](#82-安全边界计算)
9. [与 ArrowBuffer 的集成](#9-与-arrowbuffer-的集成)
   - [9.1 WALWriter 接口](#91-walwriter-接口)
   - [9.2 写入调用链](#92-写入调用链)
   - [9.3 恢复回调](#93-恢复回调)
10. [生命周期管理](#10-生命周期管理)
    - [10.1 启动顺序](#101-启动顺序)
    - [10.2 优雅关闭](#102-优雅关闭)
11. [集群复制（企业版）](#11-集群复制企业版)
12. [配置参考](#12-配置参考)
13. [监控指标](#13-监控指标)
14. [关键设计决策](#14-关键设计决策)

---

## 1. 设计目标

WAL（Write-Ahead Log，预写日志）是 IotEdgeDB 的**数据耐久性层**。在数据从内存缓冲区刷写到 Parquet 存储之前，WAL 先将数据持久化到磁盘，确保系统崩溃或存储后端故障时数据不丢失。

| 目标 | 实现方式 |
|------|---------|
| **耐久性** | 数据先写 WAL 再进内存缓冲，崩溃后可恢复 |
| **低延迟** | 异步非阻塞写入，不阻塞热路径 |
| **高吞吐** | 避免重新序列化的列式路径 + 批量同步 |
| **可恢复** | 文件级恢复粒度，失败保留文件待重试 |
| **安全清理** | 仅删除已确认刷盘的 WAL 文件，防止数据丢失 |
| **无重复** | 周期性维护使用清理而非重放，避免数据重复 |

---

## 2. 架构总览

```
                         ┌─────────────────────┐
                         │    ArrowBuffer      │
                         │   (内存列式缓冲)      │
                         └──────────┬──────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    │                               │
                    ▼                               ▼
          ┌─────────────────┐             ┌──────────────────┐
          │   WAL Writer     │             │  Flush Workers   │
          │  (耐久性保证)     │             │  (→ Parquet)     │
          └────────┬────────┘             └──────────────────┘
                   │
       ┌───────────┼───────────┐
       │           │           │
       ▼           ▼           ▼
   ┌──────┐   ┌──────┐   ┌──────┐
   │.wal  │   │.wal  │   │.wal  │    ← 文件轮转
   │  #1  │   │  #2  │   │  #3  │
   └──────┘   └──────┘   └──────┘
       │           │           │
       └───────────┴───────────┘
                   │
                   ▼
          ┌─────────────────┐
          │  WAL Recovery   │   ← 崩溃后按序回放
          │  (文件级粒度)    │
          └─────────────────┘
```

核心组件：

```
internal/wal/
├── wal.go          Writer: 异步写入、同步、轮转、清理
├── reader.go       Reader: 文件读取、反序列化、校验
└── recovery.go     Recovery: 目录扫描、按序回放、文件清理
```

---

## 3. 文件格式规范

### 3.1 文件头

每个 WAL 文件以 7 字节固定头开始：

```
┌────────────┬──────────────┬───────────────┐
│  Magic (4) │ Version (2)  │ ChecksumType  │
│  "IEDB"    │   0x0001     │   0x01=CRC32  │
└────────────┴──────────────┴───────────────┘
```

| 字段 | 偏移 | 长度 | 值 | 说明 |
|------|------|------|-----|------|
| Magic | 0 | 4 | `IEDB` | 文件格式标识 |
| Version | 4 | 2 | `0x0001` | 格式版本（大端序） |
| ChecksumType | 6 | 1 | `0x01` | CRC32-IEEE |

### 3.2 条目格式

每个 WAL 条目由 16 字节固定头 + 可变长度载荷组成：

```
┌──────────────┬────────────────┬───────────────┬──────────────────┐
│  Length (4)  │ TimestampUS (8)│ Checksum (4)  │   Payload (N)    │
│  大端序 u32   │   大端序 u64    │  大端序 u32   │   最大 100MB     │
└──────────────┴────────────────┴───────────────┴──────────────────┘
│              │                                                  │
└── WALEntryHeaderSize = 16 字节 ──┘                              │
```

| 字段 | 大小 | 说明 |
|------|------|------|
| Length | 4 字节 | 载荷长度（不含头），最大 100MB |
| TimestampUS | 8 字节 | 写入时刻 Unix 微秒时间戳 |
| Checksum | 4 字节 | 载荷 CRC32-IEEE 校验和 |
| Payload | N 字节 | MessagePack 序列化的数据 |

**载荷上限**：`MaxWALPayloadSize = 100MB`，防止损坏文件导致整数溢出（CWE-190）。

### 3.3 数据库名信封

列式路径写入的条目在载荷头部附加数据库名元数据，避免对已序列化的数据重新编码：

```
┌─────────┬──────────────┬──────────────┬──────────────────┐
│ Marker  │  DbNameLen   │   DbName     │  Original MsgPack│
│  0x01   │  2字节 u16    │   UTF-8      │                  │
└─────────┴──────────────┴──────────────┴──────────────────┘
│         │                            │                   │
└─ 1字节 ─┴── 2字节 ──┴── DbNameLen字节 ──┘                   │
```

**为什么选择 `0x01` 作为标记字节？** MessagePack 规范中，map 类型以 `0x80-0x8f` 开头，array 以 `0x90-0x9f` 开头，所以 `0x01` 不会与合法 MessagePack 数据冲突。这避免了额外的转义开销。

**解析逻辑**（`ParseEnvelope`）：

```
if payload[0] == 0x01 && len(payload) > 3:
    dbLen = ReadUint16BE(payload[1:3])
    return payload[3:3+dbLen], payload[3+dbLen:]
else:
    return "default", payload   // 无信封，兼容旧格式
```

### 3.4 文件命名

```
iedb-YYYYMMDD_HHMMSS.nnnnnnnnn.wal

示例: iedb-20260606_143025.123456789.wal
```

文件名包含创建时间戳（精确到纳秒），确保按文件名排序即为时间顺序。

---

## 4. 写入路径

### 4.1 三条写入路径

```
                     HTTP API 摄入
                          │
            ┌─────────────┼─────────────┐
            ▼             ▼             ▼
       MessagePack   LineProtocol      TLE
       (列式路径)     (行式路径)     (类型化路径)
            │             │             │
            ▼             ▼             ▼
    AppendRawWithMeta   Append       Append
   (原始字节+信封)  (msgpack序列化) (列→行转换)
            │             │             │
            └─────────────┼─────────────┘
                          ▼
                    tryEnqueue()
                    (非阻塞发送)
                          │
              ┌───────────┴───────────┐
              ▼                       ▼
        entryChan 成功           entryChan 满
              │                       │
              ▼                       ▼
        writerLoop 写入         ErrWALDropped
                                   (背压丢弃)
```

**路径对比：**

| 路径 | 方法 | 序列化开销 | 内存分配 | 数据库路由 |
|------|------|-----------|---------|-----------|
| 列式原生 | `AppendRawWithMeta` | 无（保留原始 msgpack） | 1 次 make + 1 次 copy | 信封中携带 |
| 列式回退 | `Append` | msgpack.Marshal + make+copy | 2 次 make + 2 次 copy | `_database` 键 |
| TLE 类型化 | `Append` | typedBatchToWALRecords + msgpack.Marshal | 多次分配 | `_database` 键 |

**`AppendRawWithMeta` 内存模型分析**：

```go
// 单次分配: header(16) + 信封头(1+2+dbLen) + 载荷(N)
entryData := make([]byte, WALEntryHeaderSize + totalPayloadLen)  // 分配

// CRC32 流式计算 —— 对两块独立内存分别 hash，不合并拷贝
crc := crc32.NewIEEE()
crc.Write(envelopeHeader)  // 栈上信封头
crc.Write(payload)          // 原始 msgpack（直接 hash，无中间拷贝）

// 构造完整条目 —— 此时必须拷贝：Go 的 os.File.Write 需要连续的 []byte
copy(entryData[WALEntryHeaderSize:], envelopeHeader)             // 拷贝信封
copy(entryData[WALEntryHeaderSize+envelopeHeaderLen:], payload)  // 拷贝载荷 ← 一次拷贝
```

**关键优化点**（与 `Append` 路径对比）：

| 步骤 | `Append(records)` | `AppendRawWithMeta(payload)` |
|------|-------------------|------------------------------|
| 序列化 | `msgpack.Marshal(records)` → 分配+编码 | **无**（payload 已是 msgpack） |
| CRC32 | 对 Marshal 结果计算 | 流式 hash，不合并内存 |
| 构造条目 | `make` + `copy` payload | `make` + `copy` payload |
| 总 payload 拷贝次数 | 2 次（Marshal 一次 + 构造条目一次） | **1 次**（仅构造条目） |

所以它**不是零拷贝**（payload 仍然被 copy 进 entryData），而是**避免重新序列化**和**单次分配**的优化。Go 的 `os.File.Write` 接口要求连续的 `[]byte`，使得至少一次 payload copy 不可避免。

### 4.2 异步写入循环

`writerLoop` goroutine 是 WAL 写入的核心调度器：

```
writerLoop:
  loop:
    select {
      entry <- entryChan:
        writeEntry(entry)
          ├── currentFile.Write(entry.data)         // 写入文件
          ├── currentSize += n; bytesSinceSync += n
          ├── if bytesSinceSync >= SyncBytes:       // 字节阈值触发
          │     sync()                              // fdatasync
          │     bytesSinceSync = 0
          └── if size >= MaxSize || age >= MaxAge:  // 轮转条件
                rotate()

      <- syncTicker.C:                              // 定时器触发
        if bytesSinceSync > 0:
          sync()                                    // fdatasync
          bytesSinceSync = 0

      <- done:                                      // 关闭信号
        drain remaining entries from entryChan      // 排空
        final sync()                                // 最终刷盘
        return
    }
```

### 4.3 文件轮转

轮转由两个条件触发（**满足任一即轮转**）：

| 条件 | 默认值 | 说明 |
|------|--------|------|
| 文件大小 ≥ `MaxSizeBytes` | 100MB | 防止单文件过大 |
| 文件存活 ≥ `MaxAge` | 1 小时 | 保证恢复粒度 |

轮转流程：

```
rotate():
  1. 当前文件 final sync（确保数据落盘）
  2. 关闭当前文件句柄
  3. 生成新文件名: iedb-<UTC时间戳>.wal
  4. 创建新文件 (权限 0600)
  5. 写入 7 字节文件头
  6. 重置计数器: currentSize=0, bytesSinceSync=0
```

### 4.4 背压与丢弃处理

所有 Append 方法最终都汇聚到 `tryEnqueue()`：

```go
func (w *Writer) tryEnqueue(entryData []byte) error {
    select {
    case w.entryChan <- walEntry{data: entryData}:
        return nil                          // 成功入队
    default:
        atomic.AddInt64(&w.DroppedEntries, 1)
        metrics.Get().IncWALDroppedEntries()
        return ErrWALDropped                // 通道满
    }
}
```

**调用方处理**（ArrowBuffer 侧）：

```
walErr := b.wal.AppendRawWithMeta(database, rawPayload)
if errors.Is(walErr, wal.ErrWALDropped):
    log.Warn("WAL backpressure — entry dropped")   // 采样记录
    increment totalWALDropped counter
else if walErr != nil:
    log.Error("WAL write error")                    // 未采样记录
    increment totalWALErrors counter

// 数据始终写入内存缓冲，不受 WAL 结果影响
buffer.records = append(buffer.records, record)
```

**关键设计**：WAL 写入失败不阻塞数据摄入。数据仍然进入内存缓冲，后续通过正常 Flush 写入 Parquet。WAL 是耐久性"优化"而非正确性"保证"——这一点与经典数据库 WAL（如 PostgreSQL）不同。

---

## 5. 同步策略

### 5.1 三种同步模式

| 模式 | 系统调用 | 同步内容 | 安全性 | 性能 | 适用场景 |
|------|---------|---------|--------|------|---------|
| `fsync` | `fsync()` | 数据 + 文件元数据 | 最高 | 最慢 | 金融/强一致性 |
| `fdatasync` | `fdatasync()` / `fsync()` | 仅数据 | 高 | 平衡 | **默认，生产推荐** |
| `async` | 无 | 依赖 OS 缓冲 | 低 | 最快 | 测试/低风险数据 |

> **注意**：由于 Go 标准库未直接暴露 `fdatasync`，当前实现使用 `os.File.Sync()`（内部调用 `fsync`）。未来可通过 `unix.Fdatasync(fd)` 实现真正的 `fdatasync`。

### 5.2 批量同步算法

同步采用**双阈值策略**，取最早满足者：

```
SyncDecision:
  if bytesSinceSync >= SyncBytes:    → sync()   // 默认: 每 1MB
  if timeSinceLastSync >= SyncInterval: → sync() // 默认: 每 1s
```

```
时间线示例 (SyncBytes=1MB, SyncInterval=1s):

 0s    0.3s   0.7s    1s     1.2s
 │      │      │       │       │
 ▼      ▼      ▼       ▼       ▼
write  write  write   sync    write
300KB  800KB  500KB   (定时)  1.2MB
                      ↑ 累计    ↑
                      1.6MB    字节触发, 距上次sync仅0.2s
```

**效果**：
- 高吞吐时：每 1MB 同步一次，减少 fsync 调用次数
- 低吞吐时：每 1s 同步一次，保证最坏情况延迟
- 与每条写入都同步相比，fsync 次数降低 **1000-10000 倍**

---

## 6. 恢复机制

### 6.1 启动时恢复

启动时恢复在主 goroutine 中同步执行，处理所有遗留 WAL 文件：

```
RecoverWithOptions(ctx, callback, opts):

  WAL 目录不存在?
    → 返回空统计，跳过恢复

  扫描 *.wal 文件
    → 按修改时间排序（旧→新）

  for each walFile:
    ctx 已取消?
      → 返回已恢复的统计

    walFile == SkipActiveFile?
      → 跳过（周期性恢复时保护活跃文件）

    reader := NewReader(walFile)
    entries := reader.ReadAll()

    重放条目:
      for each entry:
        if entry 是列式格式 AND 有 ColumnarCallback:
          ColumnarCallback(database, measurement, columns)
        elif entry 是行式格式:
          if BatchSize > 0 AND len(records) > BatchSize:
            分批 replay(records[i:i+BatchSize])
          else:
            整批 replay(records)

    结果判定:
      全部成功 AND len(entries) > 0:
        → 删除 WAL 文件 ✅
        → stats.RecoveredFiles++

      全部成功 AND len(entries) == 0:
        → 删除空 WAL 文件（仅含 7 字节头）

      部分失败:
        → 保留文件，下次重试 ⚠️
        → 记录 Warn 日志

  清理旧版本 *.wal.recovered 文件
```

### 6.2 周期性维护

启动后，后台 goroutine 按固定间隔执行维护：

```
periodicMaintenance (每 RecoveryIntervalSeconds，默认 300s):

  if arrowBuffer.HasFlushFailure():
    ┌─ 故障恢复模式 ──────────────────────────┐
    │ 1. PurgeOlderThan(safeAge)              │  ← 先清理已刷盘的
    │ 2. RecoverWithOptions(                  │
    │      SkipActiveFile=当前活跃文件,         │
    │      BatchSize=RecoveryBatchSize)        │  ← 重放未刷盘的
    │ 3. arrowBuffer.ResetFlushFailure()       │
    └──────────────────────────────────────────┘

  else:
    ┌─ 正常模式 ──────────────────────────────┐
    │ PurgeOlderThan(safeAge)                  │  ← 仅清理，不重放
    └──────────────────────────────────────────┘
```

### 6.3 故障恢复

```
场景: S3 存储后端断连 5 分钟

  正常写入
    → WAL 文件持续写入 (wal1 → wal2 → wal3)
    → ArrowBuffer 定期 Flush 到 S3
      → S3 断连，Flush 失败
      → arrowBuffer.markFlushFailure()
      → 缓冲数据被清空（已失败的数据无法保留）

  周期性维护检测到 HasFlushFailure()
    → 执行恢复:
      1. PurgeOlderThan(safeAge)        ← 删除已成功刷盘的旧 WAL
      2. 重放剩余 WAL 文件                ← 未刷盘的数据回到 Buffer
      3. ResetFlushFailure()            ← 标记恢复完成

  S3 恢复连接
    → 下次 Flush 将恢复的数据写入 Parquet
    → 下个维护周期 PurgeOlderThan(safeAge) 清理已刷盘的 WAL
```

### 6.4 防数据重复机制

**旧版本问题**：周期性维护使用恢复重放，每次把 WAL 文件中所有数据重新写入 Buffer，导致已经刷盘到 Parquet 的数据被重复摄入。

**新版本方案**：

```
旧版（有重复问题）:
  ticker → RecoverWithOptions() → 已刷盘的数据再次进入 Buffer → 重复！

新版（无重复）:
  ticker → PurgeOlderThan(safeAge)  → 删除已刷盘的 WAL 文件 ✓
  ticker → HasFlushFailure() 为真 → 才执行 RecoverWithOptions
```

**安全边界计算**：

```go
safeAge = MaxBufferAgeMS * 3            // 3倍缓冲年龄余量
if safeAge < 30 * time.Second:           // 下限：30秒
    safeAge = 30 * time.Second
```

`3x` 余量考虑了 Flush Worker 排队延迟和时钟偏差，确保被清理的 WAL 文件数据已经确定写入 Parquet。

---

## 7. 读取器

### 7.1 条目反序列化策略

```
readEntry(file):
  1. 读取 16 字节头
  2. 解析: payloadLen, timestampUS, expectedChecksum
  3. 验证 payloadLen ≤ MaxWALPayloadSize（防 OOM）
  4. 读取 payload 字节
  5. 验证 CRC32(expectedChecksum == actualChecksum)
  6. 解析信封提取 database + innerPayload

  7. 反序列化（尝试顺序）:
     ┌─ 第一步: msgpack.Unmarshal → []map[string]interface{}
     │  成功 → 行式条目（来自 Append 路径）
     │
     └─ 第二步: msgpack.Unmarshal → map[string]interface{}
        成功 AND 包含 "m" + "columns" 键
          → 列式条目（来自 AppendRaw/AppendRawWithMeta 路径）
        else
          → 标记为损坏条目
```

### 7.2 损坏容错

读取器采用**跳过损坏条目，继续读取**的策略：

```
循环读取条目:
  entry, err = readEntry()
  
  err == io.EOF:
    break                           ← 正常结束
  
  err != nil:
    CorruptedEntries++
    continue                        ← 跳过损坏条目，继续读下一条
  
  正常条目:
    entries = append(entries, entry)
```

**防 OOM**：读取前验证 `payloadLen ≤ 100MB`，防止损坏文件中的恶意长度值导致巨量内存分配。

---

## 8. 清理策略

### 8.1 三种清理方法

三种方法共用内部辅助函数 `purgeWALFiles(shouldDelete func(path string) bool)`：

| 方法 | 删除条件 | 使用时机 | 安全级别 |
|------|---------|---------|---------|
| `PurgeAll()` | 所有 `*.wal` 文件 | 优雅关闭 | 🔴 仅关闭时 |
| `PurgeInactive()` | 除当前活跃文件外的所有 `*.wal` | 手动维护 | 🟡 保护活跃文件 |
| `PurgeOlderThan(age)` | 修改时间超过阈值，且非活跃文件 | 周期性维护 | 🟢 双重保护 |

```
purgeWALFiles(shouldDelete):
  files = filepath.Glob("*.wal")
  for each file:
    if shouldDelete(file):
      os.Remove(file)
```

**保护机制**：
- `PurgeInactive` 和 `PurgeOlderThan` 通过 mutex 读取 `currentPath`，**绝不删除当前正在写入的文件**。
- `PurgeOlderThan` 额外检查文件修改时间，只有超过 `safeAge` 的才删除。

### 8.2 安全边界计算

```
安全边界 = max(MaxBufferAgeMS × 3, 30秒)

示例:
  MaxBufferAgeMS = 5000ms → safeAge = 15秒 → 下限修正 → 30秒
  MaxBufferAgeMS = 30000ms → safeAge = 90秒
```

保证被清理的 WAL 文件数据一定已经通过正常 Flush 流程写入 Parquet。

---

## 9. 与 ArrowBuffer 的集成

### 9.1 WALWriter 接口

在 `internal/ingest/arrow_writer.go` 中定义接口，解耦 Buffer 与 WAL 实现：

```go
type WALWriter interface {
    Append(records []map[string]interface{}) error
    AppendRaw(payload []byte) error
    AppendRawWithMeta(database string, payload []byte) error
    Stats() map[string]interface{}
    Close() error
}
```

### 9.2 写入调用链

```
ArrowBuffer.writeColumnarInternal()
  │
  ├── WAL 未启用 OR skipWAL == true:
  │     跳过 WAL 写入
  │
  └── WAL 已启用 AND skipWAL == false:
       │
       ├── record.RawPayload != nil (原始字节路径):
       │     b.wal.AppendRawWithMeta(database, record.RawPayload)
       │       → 信封包装 [0x01][dbLen][db][原始msgpack]
       │       → 单次内存分配
       │
       └── record.RawPayload == nil (回退路径):
             walRecords = columnarToWALRecords(record)
             b.wal.Append(walRecords)
               → msgpack.Marshal 序列化
               → _database, _measurement 键前缀
```

**避免恢复循环**：恢复时调用 `WriteColumnarDirectNoWAL`（设置 `skipWAL=true`），恢复的数据不会再写入 WAL。

### 9.3 恢复回调

两套回调适配不同的 WAL 条目格式：

**行式回调**（`createWALRecoveryCallback`）：

```
replayRowEntry(records []map[string]interface{}):
  for each rec:
    measurement = rec["_measurement"] || rec["measurement"] || rec["m"]
    database    = rec["_database"]    || rec["database"]    || "default"
    构建 columns map (跳过元数据键)
    arrowBuffer.WriteColumnarDirectNoWAL(database, measurement, columns)
```

**列式回调**（`createColumnarRecoveryCallback`）：

```
replayColumnarEntry(database, measurement, columns map[string][]interface{}):
  if database == "": database = "default"
  arrowBuffer.WriteColumnarDirectNoWAL(database, measurement, columns)
```

---

## 10. 生命周期管理

### 10.1 启动顺序

```
main() 启动流程:

  1. 配置加载 ────────────────────────────────────┐
     cfg.WAL.Enabled == true?                      │
       → walWriter = NewWriter(config)             │  WAL Writer 创建
       → walRecovery = NewRecovery(dir)            │  Recovery 准备
                                                   │
  2. 存储后端初始化                                 │
     storageBackend = NewLocalStorage/S3/Azure      │
                                                   │
  3. ArrowBuffer 创建 ─────────────────────────────┤
     arrowBuffer = NewArrowBuffer(config, storage)  │
     arrowBuffer.SetWAL(walWriter)   ← WAL 注入     │
                                                   │
  4. 关闭钩子注册 ─────────────────────────────────┤
     wal-purge (优先级 35): PurgeAll()              │  ← Buffer(30) 之后
                                                   │     WAL(40) 之前
  5. 启动时恢复 ───────────────────────────────────┘
     walRecovery.RecoverWithOptions(ctx, callbacks, opts)
       → 恢复数据通过 WriteColumnarDirectNoWAL 进入 Buffer
       → 成功的文件被删除

  6. 周期性维护启动
     go periodMaintenance()
       interval = RecoveryIntervalSeconds (default 300s)
       safeAge  = MaxBufferAgeMS × 3 (min 30s)
```

**关闭钩子优先级**：

```
优先级    组件              操作
────────────────────────────────────
  10     HTTP Server        停止接收新请求
  20     MQTT Manager       取消订阅
  30     ArrowBuffer        Flush 所有缓冲数据 → Parquet
  35     wal-purge          PurgeAll() ← WAL 文件已安全可删
  40     WAL Writer         Close() (排空 + 最终 sync)
  50     Database           关闭连接池
  60     Storage            关闭存储后端
```

`wal-purge`（优先级 35）的关键位置：**在 Buffer Flush(30) 之后、WAL Close(40) 之前**。此时所有数据已安全写入 Parquet，删除 WAL 文件不会丢数据。

### 10.2 优雅关闭

```
Close():
  1. close(w.done)                          → 通知 writerLoop 停止
  2. wg.Wait()                              → 等待 goroutine 退出
     writerLoop 内部:
       ├── drain 所有 entryChan 中剩余条目   → 不漏掉任何数据
       └── 最终 sync()                       → 数据落盘
  3. currentFile.Close()                    → 关闭文件句柄
  4. 记录关闭日志 (含 dropped_entries 统计)
```

---

## 11. 集群复制（企业版）

WAL 支持通过复制钩子将条目实时流式传输到集群读节点：

```go
type ReplicationHook func(entry *ReplicationEntry)

type ReplicationEntry struct {
    Sequence    uint64   // 单调递增序号（全局有序）
    TimestampUS uint64   // 微秒时间戳
    Payload     []byte   // 原始 msgpack
}
```

**复制流程**：

```
Writer.AppendRaw/AppendRawWithMeta:
  │
  ├── 1. CRC32 计算校验和
  ├── 2. 时间戳生成
  │
  ├── 3. replicationHook != nil?
  │     YES:
  │       sequence++ (mutex 保护)
  │       hook(&ReplicationEntry{seq, ts, payload})
  │         → 同步调用，阻塞直到复制完成
  │         → 保证本地写之前数据已发送到读节点
  │
  └── 4. tryEnqueue → 本地 WAL 写入
```

**设计要点**：
- 复制钩子在本地写入**之前**同步调用，保证先复制后本地持久化
- Sequence 是全局单调递增的 uint64，用于读节点排序
- 钩子接收的是 payload 副本（独立内存），避免并发问题

**集成点**（`cmd/iedb/main.go`）：

```go
if cfg.Cluster.ReplicationEnabled && walWriter != nil {
    clusterCoordinator.SetWAL(walWriter)
    clusterCoordinator.StartReplication()
}
```

---

## 12. 配置参考

```toml
[wal]
# === 基础配置 ===
enabled = false                           # 是否启用 WAL（默认关闭）
directory = "./data/wal"                  # WAL 文件目录

# === 同步策略 ===
sync_mode = "fdatasync"                   # fsync | fdatasync | async

# === 轮转策略 ===
max_size_mb = 100                         # 单文件最大大小（MB）
max_age_seconds = 3600                    # 单文件最大存活时间（秒）

# === 异步写入 ===
buffer_size = 10000                       # 异步通道容量（条目数，最大 1,000,000）

# === 恢复 ===
recovery_interval_seconds = 300           # 周期性维护间隔（秒）
recovery_batch_size = 10000               # 恢复批次大小（行数，用于限速）
```

**包内默认值**：

```go
SyncMode:     "fdatasync"
MaxSizeBytes: 100 * 1024 * 1024   // 100MB
MaxAge:       1 * time.Hour
SyncInterval: 1 * time.Second
SyncBytes:    1 * 1024 * 1024     // 1MB
BufferSize:   10000               // 条目数，上限 1,000,000
```

---

## 13. 监控指标

### Writer 指标

| 指标 | 类型 | 说明 |
|------|------|------|
| `total_entries` | atomic int64 | 累计写入条目数 |
| `total_bytes` | atomic int64 | 累计写入字节数 |
| `total_syncs` | atomic int64 | 累计 fsync 调用次数 |
| `total_rotations` | atomic int64 | 累计文件轮转次数 |
| `dropped_entries` | atomic int64 | 因通道满丢弃的条目数 |
| `current_size_mb` | float64 | 当前文件大小 |
| `current_age_seconds` | float64 | 当前文件存活时间 |
| `buffer_size` | int | 通道容量 |
| `buffer_used` | int | 通道当前占用数 |

### 全局指标（`metrics` 包）

| 指标 | 说明 |
|------|------|
| `wal_dropped_entries` | WAL 丢弃条目总数 |
| `wal_recovery_total` | WAL 恢复执行次数 |
| `wal_recovery_records` | WAL 恢复记录总数 |
| `wal_records_preserved` | Flush 队列满时 WAL 中保留的记录数 |

### Stats() 返回示例

```json
{
  "current_file": "./data/wal/iedb-20260606_143025.123456789.wal",
  "current_size_mb": 45.2,
  "current_age_seconds": 1200.5,
  "sync_mode": "fdatasync",
  "total_entries": 1500000,
  "total_bytes": 524288000,
  "total_syncs": 150,
  "total_rotations": 5,
  "dropped_entries": 3,
  "buffer_size": 10000,
  "buffer_used": 32
}
```

---

## 14. 关键设计决策

### 决策 1：异步非阻塞写入 vs 同步写入

**选择**：异步非阻塞，通道满时返回 `ErrWALDropped`。

**理由**：
- WAL 是耐久性优化，不是正确性保证。数据同时存在于内存缓冲中。
- 同步写入会使 WAL 成为热路径瓶颈（fsync 延迟 1-10ms）。
- 返回明确错误（而非静默丢弃）让调用方了解真实的耐久性状态。

### 决策 2：批量同步 vs 每条同步

**选择**：双阈值批量同步（每 1s 或每 1MB）。

**理由**：
- 每条 fsync 在高吞吐下会成为瓶颈：1M rec/s × 每条 fsync ≈ 1,000,000 fsync/s，磁盘无法承受。
- 批量同步将 fsync 频率降低到每秒 1 次（1s）或每 1000 条（1MB），吞吐提升 1000-10000 倍。
- 1s 为默认值，平衡了 NVMe/SSD/HDD 等不同存储介质的开销。低延迟场景可通过配置降低到 100-500ms。
- 最坏情况丢失 1s 的数据，对 IoT 场景可接受。

### 决策 3：文件级恢复粒度 vs 条目级

**选择**：文件级——完全恢复则删文件，部分失败则保留整个文件。

**理由**：
- 实现简单，无需维护每条目的恢复位点。
- 部分失败的文件保留后，下个维护周期重新尝试。
- WAL 文件通常较小（100MB），重复重放的浪费有限。

### 决策 4：周期性维护改为清理而非重放

**选择**：正常模式下使用 `PurgeOlderThan` 清理，仅故障模式下使用 `RecoverWithOptions` 重放。

**理由**：
- **这是修复数据重复 bug 的关键决策**。旧版本每次维护都重放所有 WAL，导致已刷盘的数据被重复摄入。
- 新方案利用文件修改时间作为"已刷盘"的信号，安全且高效。
- `3x safeAge` 余量确保不会误删。

### 决策 5：单次分配 + 信封格式

**选择**：在载荷前附加 `[0x01][dbLen][dbName]` 而非单独的元数据头。完整 WAL 条目（头+信封+载荷）在一次 `make` 中分配。

**理由**：
- 相比 `Append` 路径（msgpack.Marshal 一次分配 + 构造条目又一次分配），`AppendRawWithMeta` 将 payload 的拷贝次数从 2 次降为 1 次。
- 利用 MessagePack 编码特性（`0x80+` 开头）使 `0x01` 成为无歧义的标记字节，无需转义开销。
- CRC32 对两块独立内存（栈上信封头 + 原始 payload）流式计算，不引入中间合并拷贝。

### 决策 6：WAL 默认关闭

**选择**：`enabled = false`。

**理由**：
- WAL 引入额外的磁盘 I/O 开销（写入 + fsync），默认情况下数据通过定期 Flush 到 Parquet 获得持久性。
- 在已经启用 DuckDB 内部 WAL（`DatabaseConfig.EnableWAL`）的场景下，应用层 WAL 可能是冗余的。
- 用户应根据耐久性需求显式启用。

---

> **相关文档**：
> - [数据流程架构](./data-flow-architecture.md) — 阶段 4：WAL 耐久性层
> - [系统模块架构](./module-architecture.md) — 3.5 WAL 耐久层
