# SIGHUP 配置热加载 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让自适应缓冲的 5 个运行时参数支持 `kill -HUP` 热加载，无需重启进程。

**Architecture:** 新增 `Reloadable` 接口 + `ReloadCoordinator`（`internal/config/reloader.go`），遵循两阶段模式：先全量校验，再全量应用。`MemoryMonitor` 和 `AdaptiveFlushEngine` 实现接口，可变字段改造为 atomic。`main.go` 新增 SIGHUP goroutine 驱动重载流程。

**Tech Stack:** Go 1.x, `sync/atomic`, `os/signal`, `syscall.SIGHUP`, spf13/viper (复用现有配置解析), zerolog

---

### Task 1: 创建 Reloadable 接口和 ReloadCoordinator

**Files:**
- Create: `internal/config/reloader.go`

- [ ] **Step 1: 写入 reloader.go 完整实现**

```go
package config

import (
	"fmt"
	"sync/atomic"

	"github.com/rs/zerolog"
	"github.com/spf13/viper"
)

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
}

// Reloadable 由需要响应配置热加载的组件实现。
type Reloadable interface {
	ValidateConfig(payload *ReloadPayload) error
	ReloadConfig(payload *ReloadPayload) error
}

type namedReloadable struct {
	name      string
	component Reloadable
}

// ReloadCoordinator 管理配置热加载的生命周期。
type ReloadCoordinator struct {
	components []namedReloadable
	configPath string
	logger     zerolog.Logger
	reloading  atomic.Bool // 防重入
}

// NewReloadCoordinator 创建配置热加载协调器。
func NewReloadCoordinator(configPath string, logger zerolog.Logger) *ReloadCoordinator {
	return &ReloadCoordinator{
		configPath: configPath,
		logger:     logger.With().Str("component", "config-reloader").Logger(),
	}
}

// Register 注册一个可热加载组件。仅在启动阶段调用，非并发安全。
func (c *ReloadCoordinator) Register(name string, component Reloadable) {
	c.components = append(c.components, namedReloadable{name, component})
}

// Reload 完整流程：解析 → 校验 → 分发。
func (c *ReloadCoordinator) Reload() error {
	if c.configPath == "" {
		c.logger.Debug().Msg("未设置配置文件路径，跳过热加载")
		return nil
	}

	if !c.reloading.CompareAndSwap(false, true) {
		c.logger.Warn().Msg("前一次热加载仍在进行中，跳过本次")
		return nil
	}
	defer c.reloading.Store(false)

	// 1. 重新解析配置文件
	ingestCfg, err := parseIngestConfig(c.configPath)
	if err != nil {
		c.logger.Error().Err(err).Msg("配置重载失败：无法解析配置文件")
		return err
	}

	// 2. 构建 ReloadPayload
	payload := &ReloadPayload{
		Ingest: ingestCfg,
	}

	// 3. 第一阶段：全部校验
	for _, nc := range c.components {
		if err := nc.component.ValidateConfig(payload); err != nil {
			c.logger.Warn().
				Err(err).
				Str("component", nc.name).
				Msg("配置校验失败，保留旧配置")
			return fmt.Errorf("component %s validation failed: %w", nc.name, err)
		}
	}

	// 4. 第二阶段：全部应用
	for _, nc := range c.components {
		if err := nc.component.ReloadConfig(payload); err != nil {
			c.logger.Error().
				Err(err).
				Str("component", nc.name).
				Msg("组件应用配置失败")
			// 不阻塞其他组件
		}
	}

	c.logger.Info().Msg("配置热加载完成")
	return nil
}

// parseIngestConfig 重新解析 TOML 文件，只提取 [ingest] 段中的可热加载字段。
func parseIngestConfig(configPath string) (*IngestReloadConfig, error) {
	v := viper.New()
	v.SetConfigFile(configPath)
	v.SetConfigType("toml")

	// 复用与 Load() 一致的默认值
	v.SetDefault("ingest.max_buffer_memory_mb", 0)
	v.SetDefault("ingest.min_buffer_memory_mb", 128)
	v.SetDefault("ingest.max_buffer_age_seconds", 900)
	v.SetDefault("ingest.memory_pressure_green_pct", 50)
	v.SetDefault("ingest.memory_pressure_red_pct", 20)

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	return &IngestReloadConfig{
		MaxBufferMemoryMB:   v.GetInt("ingest.max_buffer_memory_mb"),
		MinBufferMemoryMB:   v.GetInt("ingest.min_buffer_memory_mb"),
		MaxBufferAgeSeconds: v.GetInt("ingest.max_buffer_age_seconds"),
		GreenPct:            v.GetInt("ingest.memory_pressure_green_pct"),
		RedPct:              v.GetInt("ingest.memory_pressure_red_pct"),
	}, nil
}
```

- [ ] **Step 2: 编译验证**

Run: `go build -tags=duckdb_arrow ./internal/config/`
Expected: 无编译错误

- [ ] **Step 3: Commit**

```bash
git add internal/config/reloader.go
git commit -m "feat: add Reloadable interface and ReloadCoordinator for config hot reload"
```

---

### Task 2: 测试 ReloadCoordinator

**Files:**
- Create: `internal/config/reloader_test.go`

- [ ] **Step 1: 写入测试文件**

```go
package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

// mockReloadable 是一个记录 ValidateConfig 和 ReloadConfig 调用顺序的测试桩。
type mockReloadable struct {
	name          string
	validateErr   error
	reloadErr     error
	validateCalls int
	reloadCalls   int
}

func (m *mockReloadable) ValidateConfig(payload *ReloadPayload) error {
	m.validateCalls++
	return m.validateErr
}

func (m *mockReloadable) ReloadConfig(payload *ReloadPayload) error {
	m.reloadCalls++
	return m.reloadErr
}

func TestReloadCoordinator_EmptyComponents(t *testing.T) {
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	coord := NewReloadCoordinator("", logger)
	// 无组件且无配置路径：应直接返回 nil
	if err := coord.Reload(); err != nil {
		t.Errorf("expected nil error for empty coordinator, got %v", err)
	}
}

func TestReloadCoordinator_ConfigPathEmpty(t *testing.T) {
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	coord := NewReloadCoordinator("", logger)
	coord.Register("test", &mockReloadable{name: "test"})
	// configPath 为空，应跳过
	if err := coord.Reload(); err != nil {
		t.Errorf("expected nil when configPath is empty, got %v", err)
	}
}

func TestReloadCoordinator_ValidateFailureAbortsReload(t *testing.T) {
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	// 写入一个合法的临时 TOML 文件
	dir := t.TempDir()
	configPath := filepath.Join(dir, "iedb.toml")
	content := `
[ingest]
max_buffer_memory_mb = 0
min_buffer_memory_mb = 128
max_buffer_age_seconds = 900
memory_pressure_green_pct = 50
memory_pressure_red_pct = 20
`
	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	coord := NewReloadCoordinator(configPath, logger)
	pass := &mockReloadable{name: "pass"}
	fail := &mockReloadable{name: "fail", validateErr: os.ErrInvalid}
	coord.Register("pass", pass)
	coord.Register("fail", fail)

	err := coord.Reload()
	if err == nil {
		t.Error("expected error from validation failure")
	}

	// pass 应该被调用了 ValidateConfig
	if pass.validateCalls != 1 {
		t.Errorf("expected pass.validateCalls=1, got %d", pass.validateCalls)
	}
	// fail 应该被调用了 ValidateConfig
	if fail.validateCalls != 1 {
		t.Errorf("expected fail.validateCalls=1, got %d", fail.validateCalls)
	}
	// pass 不应该被调用了 ReloadConfig（校验阶段就中止了）
	if pass.reloadCalls != 0 {
		t.Errorf("expected pass.reloadCalls=0 (aborted), got %d", pass.reloadCalls)
	}
	// fail 不应该被调用了 ReloadConfig
	if fail.reloadCalls != 0 {
		t.Errorf("expected fail.reloadCalls=0 (aborted), got %d", fail.reloadCalls)
	}
}

func TestReloadCoordinator_ReloadContinuesOnApplyFailure(t *testing.T) {
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	dir := t.TempDir()
	configPath := filepath.Join(dir, "iedb.toml")
	content := `
[ingest]
max_buffer_memory_mb = 512
min_buffer_memory_mb = 128
max_buffer_age_seconds = 600
memory_pressure_green_pct = 60
memory_pressure_red_pct = 25
`
	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	coord := NewReloadCoordinator(configPath, logger)
	pass := &mockReloadable{name: "pass"}
	failApply := &mockReloadable{name: "failApply", reloadErr: os.ErrClosed}
	coord.Register("pass", pass)
	coord.Register("failApply", failApply)

	err := coord.Reload()
	// Reload 应返回 nil（单个应用失败不导致整体失败）
	if err != nil {
		t.Errorf("expected nil (apply failures don't abort), got %v", err)
	}

	// pass 应该被调用了 ValidateConfig 和 ReloadConfig
	if pass.validateCalls != 1 {
		t.Errorf("expected pass.validateCalls=1, got %d", pass.validateCalls)
	}
	if pass.reloadCalls != 1 {
		t.Errorf("expected pass.reloadCalls=1, got %d", pass.reloadCalls)
	}
	// failApply 应该被调用了 ValidateConfig 和 ReloadConfig（validate passed）
	if failApply.validateCalls != 1 {
		t.Errorf("expected failApply.validateCalls=1, got %d", failApply.validateCalls)
	}
	if failApply.reloadCalls != 1 {
		t.Errorf("expected failApply.reloadCalls=1, got %d", failApply.reloadCalls)
	}
}

func TestReloadCoordinator_ReentrancyProtection(t *testing.T) {
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	dir := t.TempDir()
	configPath := filepath.Join(dir, "iedb.toml")
	content := `
[ingest]
max_buffer_memory_mb = 0
min_buffer_memory_mb = 128
max_buffer_age_seconds = 900
memory_pressure_green_pct = 50
memory_pressure_red_pct = 20
`
	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	coord := NewReloadCoordinator(configPath, logger)
	coord.Register("test", &mockReloadable{name: "test"})

	// 第一次 Reload 应成功
	if err := coord.Reload(); err != nil {
		t.Errorf("first Reload should succeed: %v", err)
	}
	// 手动设置 reloading flag 模拟重入
	coord.reloading.Store(true)
	err := coord.Reload()
	if err != nil {
		t.Errorf("reentrant Reload should return nil (skip), got %v", err)
	}
	// 清理后可再次 Reload
	coord.reloading.Store(false)
	if err := coord.Reload(); err != nil {
		t.Errorf("Reload after reset should succeed: %v", err)
	}
}

func TestReloadCoordinator_SuccessfulReload(t *testing.T) {
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	dir := t.TempDir()
	configPath := filepath.Join(dir, "iedb.toml")
	content := `
[ingest]
max_buffer_memory_mb = 512
min_buffer_memory_mb = 256
max_buffer_age_seconds = 600
memory_pressure_green_pct = 60
memory_pressure_red_pct = 25
`
	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	coord := NewReloadCoordinator(configPath, logger)
	m := &mockReloadable{name: "test"}
	coord.Register("test", m)

	if err := coord.Reload(); err != nil {
		t.Errorf("expected successful reload, got %v", err)
	}
	if m.validateCalls != 1 || m.reloadCalls != 1 {
		t.Errorf("expected validate+reload called once, got validate=%d reload=%d",
			m.validateCalls, m.reloadCalls)
	}
}
```

- [ ] **Step 2: 运行测试**

Run: `go test -v -tags=duckdb_arrow ./internal/config/ -run TestReloadCoordinator`
Expected: 全部 PASS

- [ ] **Step 3: Commit**

```bash
git add internal/config/reloader_test.go
git commit -m "test: add ReloadCoordinator unit tests"
```

---

### Task 3: MemoryMonitor — 抽取 computeBufferLimitFor 并引入 atomic 字段

**Files:**
- Modify: `internal/ingest/memory_monitor.go`

- [ ] **Step 1: 重构 MemoryMonitor 结构体字段**

将 `check()` 和读路径中对 `cfg.GreenPct`/`cfg.RedPct` 的直接访问改为 atomic 读取。

定位到 `internal/ingest/memory_monitor.go:34-39`，将 `MemoryMonitor` 结构体修改为：

```go
type MemoryMonitor struct {
	cfg         MemoryMonitorConfig  // 仅用于初始赋值（GreenPct/RedPct 废弃，改用 atomic）
	pressure    atomic.Int32         // 当前压力等级（无锁读取）
	totalMemory uint64               // 系统总内存（字节），启动时检测一次
	duckdbLimit uint64               // DuckDB memory_limit（字节）
	stopCh      chan struct{}
	logger      zerolog.Logger

	// === 可热加载 ===
	greenPct       atomic.Int64
	redPct         atomic.Int64
	bufferLimit    atomic.Uint64
	minBufferBytes atomic.Uint64

	// getAvailableMem is overridden in tests to mock system memory state.
	getAvailableMem func() uint64
}
```

- [ ] **Step 2: 修改构造函数，从 cfg 初始化 atomic 字段**

在 `NewMemoryMonitor` 函数末尾（`computeBufferLimit()` 调用之后，`logger.Info()` 之前）添加 atomic 初始化：

```go
func NewMemoryMonitor(cfg MemoryMonitorConfig, duckdbLimitMB int, logger zerolog.Logger) *MemoryMonitor {
	m := &MemoryMonitor{
		cfg:         cfg,
		stopCh:      make(chan struct{}),
		duckdbLimit: uint64(duckdbLimitMB) * 1024 * 1024,
		logger:      logger,
	}
	m.totalMemory = m.detectSystemMemory()
	m.greenPct.Store(int64(cfg.GreenPct))
	m.redPct.Store(int64(cfg.RedPct))
	m.minBufferBytes.Store(uint64(cfg.MinBufferMemoryMB) * 1024 * 1024)
	m.bufferLimit.Store(m.computeBufferLimitFor(cfg.MaxBufferMemoryMB))

	logger.Info().
		Uint64("total_memory_mb", m.totalMemory/(1024*1024)).
		Uint64("buffer_limit_mb", m.BufferLimit()/(1024*1024)).
		Int("green_pct", cfg.GreenPct).
		Int("red_pct", cfg.RedPct).
		Msg("Memory monitor initialized")

	return m
}
```

- [ ] **Step 3: 抽取 computeBufferLimitFor 方法**

将原有 `computeBufferLimit()` 的逻辑抽取为参数化的 `computeBufferLimitFor(maxBufferMemoryMB int) uint64`，原有方法改为调用它：

```go
// computeBufferLimitFor 根据 maxBufferMemoryMB 计算缓冲内存硬上限。
// 0 表示自动检测。此方法在启动和热加载时均可调用。
func (m *MemoryMonitor) computeBufferLimitFor(maxBufferMemoryMB int) uint64 {
	var limit uint64
	if maxBufferMemoryMB > 0 {
		limit = uint64(maxBufferMemoryMB) * 1024 * 1024
	} else {
		half := m.totalMemory / 2
		if half > m.duckdbLimit {
			limit = half - m.duckdbLimit
		} else {
			limit = half
		}
	}
	minBytes := m.MinBufferBytes()
	if limit < minBytes {
		limit = minBytes
	}
	return limit
}
```

删除原有的 `computeBufferLimit()` 方法（它已被 `computeBufferLimitFor` 替代）。

- [ ] **Step 4: 修改 check() 和热路径读方法**

`check()` 方法中将 `m.cfg.RedPct` 和 `m.cfg.GreenPct` 改为 atomic 读取：

```go
func (m *MemoryMonitor) check() PressureLevel {
	var available uint64
	if m.getAvailableMem != nil {
		available = m.getAvailableMem()
	} else {
		available = m.getAvailableMemory()
	}
	if m.totalMemory == 0 {
		return PressureGreen
	}
	availPct := int(available * 100 / m.totalMemory)

	redPct := int(m.redPct.Load())
	greenPct := int(m.greenPct.Load())

	if availPct < redPct {
		return PressureRed
	}
	if availPct < greenPct {
		return PressureYellow
	}
	return PressureGreen
}
```

`BufferLimit()` 和 `MinBufferBytes()` 方法改为从 atomic 字段读取：

```go
func (m *MemoryMonitor) BufferLimit() uint64 {
	return m.bufferLimit.Load()
}

func (m *MemoryMonitor) MinBufferBytes() uint64 {
	return m.minBufferBytes.Load()
}
```

- [ ] **Step 5: 编译验证**

Run: `go build -tags=duckdb_arrow ./internal/ingest/`
Expected: 无编译错误

- [ ] **Step 6: 更新现有测试中对 computeBufferLimit() 的调用**

现有 `TestMemoryMonitor_BufferLimit` 测试直接调用 `m.computeBufferLimit()`。改为调用 `m.computeBufferLimitFor(m.cfg.MaxBufferMemoryMB)`：

定位到 `internal/ingest/memory_monitor_test.go:104` 和 `121` 行，将：
```go
m.bufferLimit = m.computeBufferLimit()
```
替换为：
```go
m.bufferLimit = m.computeBufferLimitFor(m.cfg.MaxBufferMemoryMB)
```

- [ ] **Step 7: 运行现有测试确保不退化**

Run: `go test -v -tags=duckdb_arrow ./internal/ingest/ -run TestMemoryMonitor`
Expected: 全部 PASS

- [ ] **Step 7: Commit**

```bash
git add internal/ingest/memory_monitor.go
git commit -m "refactor: extract computeBufferLimitFor, use atomic fields for hot-reloadable params"
```

---

### Task 4: MemoryMonitor — 实现 ValidateConfig 和 ReloadConfig

**Files:**
- Modify: `internal/ingest/memory_monitor.go`

- [ ] **Step 1: 在 memory_monitor.go 末尾添加 Reloadable 接口实现**

```go
// ValidateConfig 实现 config.Reloadable 接口。
// 校验 [ingest] 段中与内存监控相关的配置。
func (m *MemoryMonitor) ValidateConfig(payload *config.ReloadPayload) error {
	if payload == nil || payload.Ingest == nil {
		return nil
	}
	ic := payload.Ingest

	if ic.GreenPct <= ic.RedPct {
		return fmt.Errorf("green_pct (%d) must be > red_pct (%d)", ic.GreenPct, ic.RedPct)
	}
	if ic.MinBufferMemoryMB < 16 {
		return fmt.Errorf("min_buffer_memory_mb must be >= 16, got %d", ic.MinBufferMemoryMB)
	}
	if ic.MaxBufferMemoryMB > 0 && ic.MaxBufferMemoryMB < ic.MinBufferMemoryMB {
		return fmt.Errorf("max_buffer_memory_mb (%d) must be >= min_buffer_memory_mb (%d)",
			ic.MaxBufferMemoryMB, ic.MinBufferMemoryMB)
	}
	return nil
}

// ReloadConfig 实现 config.Reloadable 接口。
// 将校验通过后的配置应用到内存监控器。
func (m *MemoryMonitor) ReloadConfig(payload *config.ReloadPayload) error {
	if payload == nil || payload.Ingest == nil {
		return nil
	}
	ic := payload.Ingest

	m.greenPct.Store(int64(ic.GreenPct))
	m.redPct.Store(int64(ic.RedPct))
	m.minBufferBytes.Store(uint64(ic.MinBufferMemoryMB) * 1024 * 1024)
	m.bufferLimit.Store(m.computeBufferLimitFor(ic.MaxBufferMemoryMB))

	m.logger.Info().
		Int("green_pct", ic.GreenPct).
		Int("red_pct", ic.RedPct).
		Uint64("buffer_limit_mb", m.BufferLimit()/(1024*1024)).
		Msg("内存监控器配置已热加载")

	return nil
}
```

需要在文件头部添加 `"fmt"` 和 `"iedb/internal/config"` import：

```go
import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"iedb/internal/config"

	"github.com/rs/zerolog"
)
```

- [ ] **Step 2: 编译验证**

Run: `go build -tags=duckdb_arrow ./internal/ingest/`
Expected: 无编译错误（可能需要在 go.mod 中确认不存在循环依赖）

- [ ] **Step 3: Commit**

```bash
git add internal/ingest/memory_monitor.go
git commit -m "feat: implement Reloadable on MemoryMonitor for SIGHUP hot reload"
```

---

### Task 5: 测试 MemoryMonitor 热加载

**Files:**
- Modify: `internal/ingest/memory_monitor_test.go`

- [ ] **Step 1: 在测试文件末尾添加热加载测试**

```go
func TestMemoryMonitor_ValidateConfig(t *testing.T) {
	tests := []struct {
		name    string
		payload *config.ReloadPayload
		wantErr bool
		errMsg  string
	}{
		{
			name:    "nil payload",
			payload: nil,
			wantErr: false,
		},
		{
			name:    "nil ingest section",
			payload: &config.ReloadPayload{Ingest: nil},
			wantErr: false,
		},
		{
			name: "valid config",
			payload: &config.ReloadPayload{
				Ingest: &config.IngestReloadConfig{
					GreenPct:          50,
					RedPct:            20,
					MinBufferMemoryMB: 128,
					MaxBufferMemoryMB: 0, // auto
				},
			},
			wantErr: false,
		},
		{
			name: "green not greater than red",
			payload: &config.ReloadPayload{
				Ingest: &config.IngestReloadConfig{
					GreenPct:          20,
					RedPct:            20,
					MinBufferMemoryMB: 128,
				},
			},
			wantErr: true,
			errMsg:  "must be > red_pct",
		},
		{
			name: "green less than red",
			payload: &config.ReloadPayload{
				Ingest: &config.IngestReloadConfig{
					GreenPct:          15,
					RedPct:            20,
					MinBufferMemoryMB: 128,
				},
			},
			wantErr: true,
			errMsg:  "must be > red_pct",
		},
		{
			name: "min buffer too small",
			payload: &config.ReloadPayload{
				Ingest: &config.IngestReloadConfig{
					GreenPct:          50,
					RedPct:            20,
					MinBufferMemoryMB: 1,
				},
			},
			wantErr: true,
			errMsg:  "must be >= 16",
		},
		{
			name: "max less than min",
			payload: &config.ReloadPayload{
				Ingest: &config.IngestReloadConfig{
					GreenPct:          50,
					RedPct:            20,
					MinBufferMemoryMB: 256,
					MaxBufferMemoryMB: 128,
				},
			},
			wantErr: true,
			errMsg:  "must be >= min_buffer_memory_mb",
		},
		{
			name: "max equal to min (ok)",
			payload: &config.ReloadPayload{
				Ingest: &config.IngestReloadConfig{
					GreenPct:          50,
					RedPct:            20,
					MinBufferMemoryMB: 256,
					MaxBufferMemoryMB: 256,
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
			m := NewMemoryMonitor(MemoryMonitorConfig{
				GreenPct:          50,
				RedPct:            20,
				MinBufferMemoryMB: 128,
			}, 0, logger)

			err := m.ValidateConfig(tt.payload)
			if tt.wantErr && err == nil {
				t.Errorf("expected error containing '%s', got nil", tt.errMsg)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("expected no error, got %v", err)
			}
			if tt.wantErr && err != nil && !strings.Contains(err.Error(), tt.errMsg) {
				t.Errorf("expected error containing '%s', got '%s'", tt.errMsg, err.Error())
			}
		})
	}
}

func TestMemoryMonitor_ReloadConfig(t *testing.T) {
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	m := NewMemoryMonitor(MemoryMonitorConfig{
		GreenPct:          50,
		RedPct:            20,
		MinBufferMemoryMB: 128,
	}, 0, logger)

	// 验证初始值
	if m.PressureLevel() != PressureGreen {
		t.Fatalf("initial pressure should be green, got %d", m.PressureLevel())
	}

	// 热加载：将 redPct 调高到 50（可用内存 100% 都不会触发红色）
	payload := &config.ReloadPayload{
		Ingest: &config.IngestReloadConfig{
			GreenPct:          60,
			RedPct:            55,
			MinBufferMemoryMB: 256,
			MaxBufferMemoryMB: 0, // auto
		},
	}

	if err := m.ReloadConfig(payload); err != nil {
		t.Fatalf("ReloadConfig failed: %v", err)
	}

	// 验证 atomic 字段已更新
	if int(m.greenPct.Load()) != 60 {
		t.Errorf("expected greenPct=60, got %d", m.greenPct.Load())
	}
	if int(m.redPct.Load()) != 55 {
		t.Errorf("expected redPct=55, got %d", m.redPct.Load())
	}
	if m.MinBufferBytes() != 256*1024*1024 {
		t.Errorf("expected minBufferBytes=%d, got %d", 256*1024*1024, m.MinBufferBytes())
	}
}

func TestMemoryMonitor_CheckAfterReload(t *testing.T) {
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	m := &MemoryMonitor{
		cfg:         MemoryMonitorConfig{GreenPct: 50, RedPct: 20},
		totalMemory: 1000,
		logger:      logger,
	}
	m.greenPct.Store(50)
	m.redPct.Store(20)
	m.bufferLimit.Store(128 * 1024 * 1024)

	// 初始：available=400（40%），< 50% → yellow
	m.getAvailableMem = func() uint64 { return 400 }
	if level := m.check(); level != PressureYellow {
		t.Errorf("before reload: expected yellow (400/1000=40%%), got %d", level)
	}

	// 热加载：greenPct → 30，redPct → 10
	m.greenPct.Store(30)
	m.redPct.Store(10)

	// 现在 available=400（40%）> 30% → green
	if level := m.check(); level != PressureGreen {
		t.Errorf("after reload: expected green (400/1000=40%% > 30%%), got %d", level)
	}

	// available=50（5%）< 10% → red
	m.getAvailableMem = func() uint64 { return 50 }
	if level := m.check(); level != PressureRed {
		t.Errorf("after reload: expected red (50/1000=5%% < 10%%), got %d", level)
	}
}

func TestMemoryMonitor_BufferLimitAfterReload(t *testing.T) {
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)

	// 16GB 系统内存，4GB DuckDB
	m := &MemoryMonitor{
		cfg:         MemoryMonitorConfig{MinBufferMemoryMB: 128},
		totalMemory: 16 * 1024 * 1024 * 1024, // 16GB
		duckdbLimit: 4 * 1024 * 1024 * 1024,  // 4GB
		logger:      logger,
	}
	m.greenPct.Store(50)
	m.redPct.Store(20)
	m.minBufferBytes.Store(128 * 1024 * 1024)
	m.bufferLimit.Store(m.computeBufferLimitFor(0)) // auto: (8GB - 4GB) = 4GB

	if limit := m.BufferLimit(); limit != 4*1024*1024*1024 {
		t.Fatalf("initial limit: expected 4GB, got %d", limit)
	}

	// 热加载：手动指定 2GB
	m.bufferLimit.Store(m.computeBufferLimitFor(2048))
	if limit := m.BufferLimit(); limit != 2*1024*1024*1024 {
		t.Errorf("after manual reload: expected 2GB, got %d", limit)
	}

	// 热加载：切回自动
	m.bufferLimit.Store(m.computeBufferLimitFor(0))
	if limit := m.BufferLimit(); limit != 4*1024*1024*1024 {
		t.Errorf("after auto reload: expected 4GB, got %d", limit)
	}
}
```

需要在文件头部添加 `"strings"` 和 `"iedb/internal/config"` import。

- [ ] **Step 2: 运行测试**

Run: `go test -v -tags=duckdb_arrow ./internal/ingest/ -run "TestMemoryMonitor_ValidateConfig|TestMemoryMonitor_ReloadConfig|TestMemoryMonitor_CheckAfterReload|TestMemoryMonitor_BufferLimitAfterReload"`
Expected: 全部 PASS

- [ ] **Step 3: Commit**

```bash
git add internal/ingest/memory_monitor_test.go
git commit -m "test: add MemoryMonitor hot reload unit tests"
```

---

### Task 6: AdaptiveFlushEngine — 改造为 atomic 字段

**Files:**
- Modify: `internal/ingest/adaptive_flush.go`

- [ ] **Step 1: 修改结构体字段**

将 `maxBufferBytes`、`minBufferBytes`、`maxAge` 从普通字段改为 atomic：

```go
type AdaptiveFlushEngine struct {
	buffer         *ArrowBuffer
	monitor        *MemoryMonitor
	maxBufferBytes atomic.Uint64
	minBufferBytes atomic.Uint64
	maxAge         atomic.Int64 // 纳秒
	candidatesBuf  []flushCandidate
	logger         zerolog.Logger
}
```

- [ ] **Step 2: 修改构造函数**

```go
func NewAdaptiveFlushEngine(
	buffer *ArrowBuffer,
	monitor *MemoryMonitor,
	maxAge time.Duration,
	logger zerolog.Logger,
) *AdaptiveFlushEngine {
	e := &AdaptiveFlushEngine{
		buffer:         buffer,
		monitor:        monitor,
		logger:         logger,
	}
	e.maxBufferBytes.Store(monitor.BufferLimit())
	e.minBufferBytes.Store(monitor.MinBufferBytes())
	e.maxAge.Store(int64(maxAge))
	return e
}
```

- [ ] **Step 3: 修改所有读路径**

`evaluate()` 中 `totalBytes > e.maxBufferBytes` → `totalBytes > e.maxBufferBytes.Load()`：

```go
func (e *AdaptiveFlushEngine) evaluate() {
	candidates := e.collectCandidates()

	var totalBytes uint64
	for _, c := range candidates {
		totalBytes += c.entry.estimatedBytes
	}

	metrics.Get().SetBufferEstimatedBytes(int64(totalBytes))

	if totalBytes > e.maxBufferBytes.Load() {
		e.logger.Debug().
			Uint64("total_bytes", totalBytes).
			Uint64("limit_bytes", e.maxBufferBytes.Load()).
			Msg("Buffer hard limit exceeded, flushing largest")
		metrics.Get().IncHardLimitFlush()
		e.flushUntilBelow(candidates, totalBytes, e.maxBufferBytes.Load())
		return
	}

	expired := e.filterExpired(candidates)
	// ... 其余不变
}
```

`filterExpired()` 中将 `time.Since(c.entry.startTime) >= e.maxAge` → `time.Since(c.entry.startTime) >= time.Duration(e.maxAge.Load())`：

```go
func (e *AdaptiveFlushEngine) filterExpired(candidates []flushCandidate) []flushCandidate {
	var expired []flushCandidate
	maxAge := time.Duration(e.maxAge.Load())
	for _, c := range candidates {
		if time.Since(c.entry.startTime) >= maxAge {
			c.trigger = "age"
			expired = append(expired, c)
		}
	}
	return expired
}
```

`flushLargestUntil()` 中将 `e.minBufferBytes / uint64(max(len(candidates), 1))` 改为 `e.minBufferBytes.Load() / uint64(max(len(candidates), 1))`：

```go
func (e *AdaptiveFlushEngine) flushLargestUntil(
	candidates []flushCandidate,
	shouldStop func() bool,
) {
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].entry.recordCount > candidates[j].entry.recordCount
	})

	minPerMeasurement := e.minBufferBytes.Load() / uint64(max(len(candidates), 1))

	for i := range candidates {
		if shouldStop() {
			break
		}
		if candidates[i].entry.estimatedBytes < minPerMeasurement {
			continue
		}
		if candidates[i].trigger == "" {
			candidates[i].trigger = "pressure"
		}
		e.flushCandidate(candidates[i])
	}
}
```

- [ ] **Step 4: 编译验证**

Run: `go build -tags=duckdb_arrow ./internal/ingest/`
Expected: 无编译错误

- [ ] **Step 5: 运行现有测试**

Run: `go test -v -tags=duckdb_arrow ./internal/ingest/ -run TestAdaptive`
Expected: 全部 PASS

- [ ] **Step 6: Commit**

```bash
git add internal/ingest/adaptive_flush.go
git commit -m "refactor: use atomic fields in AdaptiveFlushEngine for hot reload"
```

---

### Task 7: AdaptiveFlushEngine — 实现 Reloadable 接口

**Files:**
- Modify: `internal/ingest/adaptive_flush.go`

- [ ] **Step 1: 在文件末尾添加接口实现**

```go
// ValidateConfig 实现 config.Reloadable 接口。
func (e *AdaptiveFlushEngine) ValidateConfig(payload *config.ReloadPayload) error {
	if payload == nil || payload.Ingest == nil {
		return nil
	}
	if payload.Ingest.MaxBufferAgeSeconds < 10 {
		return fmt.Errorf("max_buffer_age_seconds must be >= 10, got %d", payload.Ingest.MaxBufferAgeSeconds)
	}
	return nil
}

// ReloadConfig 实现 config.Reloadable 接口。
func (e *AdaptiveFlushEngine) ReloadConfig(payload *config.ReloadPayload) error {
	if payload == nil || payload.Ingest == nil {
		return nil
	}
	ic := payload.Ingest

	oldMaxAge := time.Duration(e.maxAge.Load())
	newMaxAge := time.Duration(ic.MaxBufferAgeSeconds) * time.Second
	e.maxAge.Store(int64(newMaxAge))

	// 从 monitor 读取最新值（monitor 的 ReloadConfig 先于本组件被调用）
	e.maxBufferBytes.Store(e.monitor.BufferLimit())
	e.minBufferBytes.Store(e.monitor.MinBufferBytes())

	e.logger.Info().
		Dur("old_max_age", oldMaxAge).
		Dur("new_max_age", newMaxAge).
		Uint64("buffer_limit_mb", e.maxBufferBytes.Load()/(1024*1024)).
		Msg("自适应刷盘引擎配置已热加载")

	return nil
}
```

需要在文件头部添加 `"fmt"` 和 `"iedb/internal/config"` import（如果还没）。

- [ ] **Step 2: 编译验证**

Run: `go build -tags=duckdb_arrow ./internal/ingest/`
Expected: 无编译错误

- [ ] **Step 3: Commit**

```bash
git add internal/ingest/adaptive_flush.go
git commit -m "feat: implement Reloadable on AdaptiveFlushEngine"
```

---

### Task 8: 测试 AdaptiveFlushEngine 热加载

**Files:**
- Modify: `internal/ingest/adaptive_flush_test.go`

- [ ] **Step 1: 在测试文件末尾添加测试**

```go
func TestAdaptiveFlushEngine_ValidateConfig(t *testing.T) {
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)

	tests := []struct {
		name    string
		payload *config.ReloadPayload
		wantErr bool
	}{
		{
			name:    "nil payload",
			payload: nil,
			wantErr: false,
		},
		{
			name: "valid config",
			payload: &config.ReloadPayload{
				Ingest: &config.IngestReloadConfig{MaxBufferAgeSeconds: 900},
			},
			wantErr: false,
		},
		{
			name: "max age too small",
			payload: &config.ReloadPayload{
				Ingest: &config.IngestReloadConfig{MaxBufferAgeSeconds: 5},
			},
			wantErr: true,
		},
		{
			name: "max age at boundary (10s ok)",
			payload: &config.ReloadPayload{
				Ingest: &config.IngestReloadConfig{MaxBufferAgeSeconds: 10},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 使用 nil buffer + nil monitor 构造 engine（ValidateConfig 不访问它们）
			engine := &AdaptiveFlushEngine{logger: logger}
			err := engine.ValidateConfig(tt.payload)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("expected no error, got %v", err)
			}
		})
	}
}

func TestAdaptiveFlushEngine_ReloadConfig(t *testing.T) {
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)

	// 创建真实的 MemoryMonitor 以提供 BufferLimit/MinBufferBytes
	monitor := NewMemoryMonitor(MemoryMonitorConfig{
		MaxBufferMemoryMB: 512,
		MinBufferMemoryMB: 128,
		GreenPct:          50,
		RedPct:            20,
	}, 0, logger)

	engine := NewAdaptiveFlushEngine(nil, monitor, 900*time.Second, logger)

	// 验证初始值
	initialMaxAge := time.Duration(engine.maxAge.Load())
	if initialMaxAge != 900*time.Second {
		t.Fatalf("expected initial maxAge=900s, got %v", initialMaxAge)
	}

	// 热加载：maxAge 改为 600s
	payload := &config.ReloadPayload{
		Ingest: &config.IngestReloadConfig{
			MaxBufferAgeSeconds: 600,
			MaxBufferMemoryMB:   1024,
			MinBufferMemoryMB:   256,
			GreenPct:            60,
			RedPct:              25,
		},
	}

	// 先更新 monitor（模拟 coordinator 的调用顺序）
	_ = monitor.ReloadConfig(payload)

	if err := engine.ReloadConfig(payload); err != nil {
		t.Fatalf("ReloadConfig failed: %v", err)
	}

	// 验证 maxAge 已更新
	newMaxAge := time.Duration(engine.maxAge.Load())
	if newMaxAge != 600*time.Second {
		t.Errorf("expected maxAge=600s, got %v", newMaxAge)
	}

	// 验证 maxBufferBytes 从 monitor 读取了新值
	expectedLimit := monitor.BufferLimit()
	if engine.maxBufferBytes.Load() != expectedLimit {
		t.Errorf("expected maxBufferBytes=%d, got %d",
			expectedLimit, engine.maxBufferBytes.Load())
	}

	// 验证 minBufferBytes 从 monitor 读取了新值
	expectedMin := monitor.MinBufferBytes()
	if engine.minBufferBytes.Load() != expectedMin {
		t.Errorf("expected minBufferBytes=%d, got %d",
			expectedMin, engine.minBufferBytes.Load())
	}
}
```

需要在文件头部添加 `"iedb/internal/config"` import（如果还没）。

- [ ] **Step 2: 运行测试**

Run: `go test -v -tags=duckdb_arrow ./internal/ingest/ -run "TestAdaptiveFlushEngine_ValidateConfig|TestAdaptiveFlushEngine_ReloadConfig"`
Expected: 全部 PASS

- [ ] **Step 3: Commit**

```bash
git add internal/ingest/adaptive_flush_test.go
git commit -m "test: add AdaptiveFlushEngine hot reload tests"
```

---

### Task 9: 在 Config 结构体中存储配置文件路径

**Files:**
- Modify: `internal/config/config.go`

- [ ] **Step 1: 添加 ConfigFilePath 字段**

在 `Config` 结构体末尾（`Reconciliation ReconciliationConfig` 之后，`}` 之前）添加：

```go
// ConfigFilePath is the absolute path to the config file used at startup.
// Empty if no config file was found (defaults-only mode).
ConfigFilePath string
```

- [ ] **Step 2: 在 Load() 中捕获配置文件路径**

在 `Load()` 函数中，`v.ReadInConfig()` 之后、错误检查之前，添加：

```go
// Capture the config file path BEFORE building the Config struct.
// ConfigFileUsed() returns empty string when no file was found,
// which disables hot reload (correct: nothing to reload).
configFilePath := ""
if err := v.ReadInConfig(); err != nil {
    if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
        return nil, fmt.Errorf("failed to read config: %w", err)
    }
    // Config file not found is OK, use defaults
} else {
    configFilePath = v.ConfigFileUsed()
}
```

并将 `ConfigFilePath` 赋值加入 cfg 构造中：

```go
cfg := &Config{
    // ... existing fields ...
    ConfigFilePath: configFilePath,
}
```

注意：原来 `Load()` 中 `v.ReadInConfig()` 的调用已内联到上面的 if-else 块中，需要替换掉原有的单独调用。

- [ ] **Step 3: 编译验证**

Run: `go build -tags=duckdb_arrow ./internal/config/`
Expected: 无编译错误

- [ ] **Step 4: Commit**

```bash
git add internal/config/config.go
git commit -m "feat: store ConfigFilePath in Config struct for hot reload"
```

---

### Task 10: 在 main.go 中接入 SIGHUP 信号处理

**Files:**
- Modify: `cmd/iedb/main.go`

- [ ] **Step 1: 找到自适应缓冲注入代码段**

定位到 `cmd/iedb/main.go` 中 MemoryMonitor 和 AdaptiveFlushEngine 创建的位置（约 329-351 行）。

在 AdaptiveFlushEngine 创建之后、ArrowViewManager 之前，插入 ReloadCoordinator 创建和 SIGHUP 监听代码：

```go
// === SIGHUP 配置热加载 ===
reloadCoordinator := config.NewReloadCoordinator(
	cfg.ConfigFilePath,
	logger.Get("config-reloader"),
)
reloadCoordinator.Register("memory-monitor", memoryMonitor)
reloadCoordinator.Register("adaptive-flush", adaptiveEngine)

sighupCh := make(chan os.Signal, 1)
signal.Notify(sighupCh, syscall.SIGHUP)
go func() {
	for range sighupCh {
		if err := reloadCoordinator.Reload(); err != nil {
			log.Warn().Err(err).Msg("配置热加载失败")
		}
	}
}()
```

- [ ] **Step 2: 在 shutdown 前停止 SIGHUP 监听**

找到 `shutdownCoordinator.WaitForSignal()` 调用位置（约 1699 行），在它之后立即添加 `signal.Stop(sighupCh)`：

```go
sig := shutdownCoordinator.WaitForSignal()
signal.Stop(sighupCh) // 停止 SIGHUP 监听，防止 shutdown 期间热加载
```

- [ ] **Step 3: 确保需要的 import 存在**

检查文件头部是否已包含以下 import（大部分应该已有）：
- `"os"` — 已有（用于 env/signal）
- `"os/signal"` — 已有（shutdown.WaitForSignal 中使用）
- `"syscall"` — 已有
- `"iedb/internal/config"` — 应有（Load 配置时使用）

如果缺少 `"os/signal"` 或 `"syscall"`，手动添加。

- [ ] **Step 4: 编译验证**

Run: `go build -v -tags=duckdb_arrow -o iedb ./cmd/iedb`
Expected: 无编译错误

- [ ] **Step 5: 手动烟雾测试**

启动进程后，发送 SIGHUP：
```bash
./iedb &
PID=$!
kill -HUP $PID
sleep 1
# 检查日志中是否有 "配置热加载完成" 或 "未设置配置文件路径" 字样
kill $PID
```

- [ ] **Step 6: Commit**

```bash
git add cmd/iedb/main.go
git commit -m "feat: wire SIGHUP config hot reload in main.go"
```

---

### Task 11: 最终验证与清理

- [ ] **Step 1: 运行全部相关测试**

```bash
go test -v -tags=duckdb_arrow ./internal/config/ ./internal/ingest/
```
Expected: 全部 PASS，无 race 错误

- [ ] **Step 2: 运行 race 检测**

```bash
go test -race -tags=duckdb_arrow ./internal/config/ ./internal/ingest/
```
Expected: 全部 PASS，无 data race 警告

- [ ] **Step 3: 运行 lint**

```bash
make lint
```
修复任何 lint 问题。

- [ ] **Step 4: 最终 commit**

```bash
git add -A
git commit -m "chore: final lint and race check for SIGHUP hot reload"
```
