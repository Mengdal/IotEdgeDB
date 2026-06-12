# SIGHUP 配置热加载设计

**日期**: 2026-06-12
**状态**: 设计完成，待评审

---

## 概述

让自适应缓冲的 5 个运行时参数支持通过 `kill -HUP` 热加载，无需重启进程。基于 `Reloadable` 接口 + `ReloadCoordinator` 的统一抽象，后续其他模块可直接实现接口加入热加载体系。

### 改动前后对比

```
改动前：
  修改 iedb.toml → 重启进程 → WAL 回放 + DuckDB 重建 + VIEW 重建（数十秒不可用）

改动后：
  修改 iedb.toml → kill -HUP <pid> → 参数热生效（毫秒级，零停机）
```

### 核心原则

- 全量校验，单点中止：ValidateConfig 阶段任一组件校验失败，整次重载取消，旧配置不受影响。ReloadConfig 阶段单个组件失败仅记录日志，不阻塞其他组件
- 不阻塞写入/查询：`ReloadConfig()` 内部仅做 atomic store，无锁竞争
- 不可热加载的参数不放入 `ReloadPayload`，无从分发的通道
- YAGNI：不引入不必要的锁和拷贝

---

## 架构

### 组件关系

```
iedb.toml
    │
    ▼
┌─────────────────────────────┐
│  ReloadCoordinator          │
│  - components []Reloadable  │
│  - configPath string        │
│  - Reload() error           │
│  - reloading atomic.Bool    │
└──────────┬──────────────────┘
           │ 遍历调用 ValidateConfig() → ReloadConfig()
           │
    ┌──────┴──────┬────────────────┐
    ▼             ▼                ▼
MemoryMonitor  AdaptiveFlush    (未来: CompactionScheduler,
                Engine                   QueryExecutor, ...)
```

### SIGHUP 数据流

```
SIGHUP 信号 (goroutine 2)
    │
    ▼
reloadCoordinator.Reload()
    ├── 1. parseConfigFile()         → 重新解析完整 TOML
    ├── 2. 构建 ReloadPayload        → 只提取可热加载字段（白名单）
    ├── 3. 遍历 ValidateConfig()     → 第一阶段：全部校验（无副作用）
    ├── 4. 遍历 ReloadConfig()       → 第二阶段：全部应用
    └── 5. 日志记录变更
```

### 生命周期

```
main() goroutine:                     sighup goroutine:
  Register("memory-monitor", mm)
  Register("adaptive-flush", ae)
  go sighupLoop() ───────────────────→ for range sighupCh { Reload() }
  WaitForSignal() [阻塞]
  signal.Stop(sighupCh) ─────────────→ goroutine 退出
  shutdown 流程
```

---

## 接口定义

### Reloadable (`internal/config/reloader.go`)

```go
package config

// ReloadPayload 携带一次热加载的完整配置快照。
// 每个 section 按需填充；nil 的 section 表示该部分没有可热加载的参数。
type ReloadPayload struct {
    Ingest *IngestReloadConfig
    // 未来: Compaction *CompactionReloadConfig
    // 未来: Query      *QueryReloadConfig
}

// IngestReloadConfig 包含 [ingest] 段中支持热加载的参数。
type IngestReloadConfig struct {
    MaxBufferMemoryMB   int // 0=自动检测，>0=手动指定硬上限
    MinBufferMemoryMB   int // 最低保证缓冲内存
    MaxBufferAgeSeconds int // 超时强制刷盘（秒）
    GreenPct            int // 可用内存 > 此值 = 绿色
    RedPct              int // 可用内存 < 此值 = 红色
    // 注意：CheckIntervalMS 不支持热加载——修改 ticker 间隔需要停旧启新，
    // 1000ms 对所有场景都是合理默认值，热加载此参数的价值不抵复杂度。
}

// Reloadable 由需要响应配置热加载的组件实现。
type Reloadable interface {
    // ValidateConfig 校验新配置是否合法。纯函数，无副作用。
    // 从 payload 中提取自己关心的 section 进行校验。
    ValidateConfig(payload *ReloadPayload) error

    // ReloadConfig 应用新配置。仅在所有 ValidateConfig 通过后被调用。
    // 从 payload 中提取自己关心的 section 进行应用。
    ReloadConfig(payload *ReloadPayload) error
}
```

### ReloadCoordinator

```go
type ReloadCoordinator struct {
    components []namedReloadable
    configPath string
    logger     zerolog.Logger
    reloading  atomic.Bool  // 防重入
}

func NewReloadCoordinator(configPath string, logger zerolog.Logger) *ReloadCoordinator

// Register 注册一个可热加载组件（仅启动阶段调用，非并发安全）。
func (c *ReloadCoordinator) Register(name string, component Reloadable)

// Reload 完整流程：解析 → 校验 → 分发。
func (c *ReloadCoordinator) Reload() error
```

### Reload() 两阶段流程

```
第一阶段 — 校验：
  遍历所有组件调用 ValidateConfig(payload)
    → 任意一个返回 error → 中止整个 Reload，日志 WARN，旧配置保留

第二阶段 — 应用：
  遍历所有组件调用 ReloadConfig(payload)
    → 单个组件返回 error → 日志 ERROR，继续遍历剩余组件
```

---

## 组件改造

### MemoryMonitor (`internal/ingest/memory_monitor.go`)

将可变参数从值拷贝字段改为 atomic 字段：

```go
type MemoryMonitor struct {
    cfg         MemoryMonitorConfig  // 仅用于初始赋值
    pressure    atomic.Int32         // 不变
    totalMemory uint64               // 不变：启动时检测一次
    duckdbLimit uint64               // 不变

    // === 可热加载 ===
    greenPct       atomic.Int64
    redPct         atomic.Int64
    bufferLimit    atomic.Uint64
    minBufferBytes atomic.Uint64
}
```

读路径（热路径，无锁）：
```go
func (m *MemoryMonitor) check() PressureLevel {
    // ...
    redPct := int(m.redPct.Load())
    greenPct := int(m.greenPct.Load())
    if availPct < redPct { return PressureRed }
    if availPct < greenPct { return PressureYellow }
    return PressureGreen
}
```

新增方法：
- `computeBufferLimitFor(maxBufferMemoryMB int) uint64` — 从构造函数中抽出 bufferLimit 计算逻辑，支持重算
- `ValidateConfig(payload) error` — 校验 greenPct > redPct，minBufferMB >= 16
- `ReloadConfig(payload) error` — atomic store 新值 + 重算 bufferLimit

### AdaptiveFlushEngine (`internal/ingest/adaptive_flush.go`)

```go
type AdaptiveFlushEngine struct {
    buffer         *ArrowBuffer
    monitor        *MemoryMonitor
    maxBufferBytes atomic.Uint64   // 改为 atomic
    minBufferBytes atomic.Uint64
    maxAge         atomic.Int64    // 改为 atomic（纳秒）
    // ...
}
```

新增方法：
- `ValidateConfig(payload) error` — 校验 maxAge >= 10s
- `ReloadConfig(payload) error` — atomic store maxAge；从 monitor 读取最新 BufferLimit/MinBufferBytes 并 store

---

## 信号处理改造 (`cmd/iedb/main.go`)

在 `main()` 中新增 SIGHUP 监听 goroutine，不修改现有 `WaitForSignal()`：

```go
sighupCh := make(chan os.Signal, 1)
signal.Notify(sighupCh, syscall.SIGHUP)
go func() {
    for range sighupCh {
        if err := reloadCoordinator.Reload(); err != nil {
            log.Warn().Err(err).Msg("配置热加载失败")
        }
    }
}()

// 原有阻塞等待（不变）
sig := shutdownCoordinator.WaitForSignal()
signal.Stop(sighupCh)  // 终止信号到达后，停止 SIGHUP 监听
```

### 竞态分析

| 场景 | 处理 |
|------|------|
| SIGHUP 和 SIGTERM 同时到达 | Go signal 通知序列化，无竞态 |
| SIGHUP 后立刻 SIGTERM | `signal.Stop(sighupCh)` 在 `WaitForSignal()` 返回后同步执行 → goroutine 退出 → shutdown 开始 |
| shutdown 期间收到 SIGHUP | channel 已 close → goroutine 已退出 → 无操作 |
| 连续快速 SIGHUP | `reloading` atomic.Bool 防重入，第二次跳过 |

---

## 不可热加载参数的处理

`shard_count`、`flush_workers`、`flush_queue_size`、`compression` 等参数不包含在 `IngestReloadConfig` 中。修改这些参数后执行 SIGHUP，新值会被解析但不会被分发到任何组件——即静默不生效。

如需明确提示用户，可在 coordinator 中对比旧新配置，打印 WARN：

```go
if oldCfg.ShardCount != newCfg.ShardCount {
    logger.Warn().Msg("shard_count 需重启生效，本次热加载不会应用此变更")
}
```

此对比逻辑作为可选增强，不在首版设计中强制要求。

---

## 错误处理与边界情况

| 场景 | 处理 |
|------|------|
| TOML 文件损坏 | parseConfigFile 失败 → ERROR 日志 + 保留旧配置 |
| 新配置值非法 | ValidateConfig 失败 → WARN 日志 + Reload 中止 |
| configPath 为空 | Reload() 直接返回 nil |
| max_buffer_memory_mb 自动→手动切换 | computeBufferLimitFor 重算 bufferLimit |
| 组件 ReloadConfig 返回 error | ERROR 日志 + 继续遍历剩余组件 |
| macOS | SIGHUP 是 POSIX 标准信号，兼容 |

---

## 改动范围

| 文件 | 改动类型 | 估模 |
|------|---------|------|
| `internal/config/reloader.go` | **新增** | ~100 行 |
| `internal/config/reloader_test.go` | **新增** | ~80 行 |
| `internal/ingest/memory_monitor.go` | 修改 | ~60 行 |
| `internal/ingest/memory_monitor_test.go` | 修改 | ~40 行 |
| `internal/ingest/adaptive_flush.go` | 修改 | ~40 行 |
| `internal/ingest/adaptive_flush_test.go` | 修改 | ~30 行 |
| `cmd/iedb/main.go` | 修改 | ~20 行 |
| **总计** | | **~370 行** |

---

## 测试策略

| 层级 | 内容 |
|------|------|
| 单元：ValidateConfig | MemoryMonitor 校验：greenPct==redPct 拒绝、minBufferMB=1 拒绝、maxBufferMB < minBufferMB 拒绝、合法值通过 |
| 单元：ValidateConfig | AdaptiveFlushEngine 校验：maxAge < 10 拒绝、合法值通过 |
| 单元：ReloadCoordinator | 空组件列表、单组件、校验失败中止分发、重入保护 |
| 单元：MemoryMonitor | 热加载后 check() 使用新阈值、bufferLimit 重算正确性 |
| 单元：AdaptiveFlushEngine | 热加载后 filterExpired 使用新 maxAge、maxBufferBytes 从 monitor 读取新值 |
| 集成：端到端 | 启动 → 修改 toml → kill -HUP → 检查日志确认新值生效 → 验证新配置行为 |
| 集成：并发安全 | 持续写入 + 并发 SIGHUP → 验证无数据丢失、无 panic、无死锁 |

---

## 后续扩展（不在本设计范围）

配置变更日志中可以记录新旧值对比，便于故障排查时追溯某次刷盘决策对应哪套配置参数。
