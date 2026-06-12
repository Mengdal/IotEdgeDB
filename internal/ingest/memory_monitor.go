package ingest

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"iedb/internal/config"
)

// PressureLevel 内存压力等级
type PressureLevel int32

const (
	PressureGreen  PressureLevel = 0 // 可用内存充裕，正常缓冲
	PressureYellow PressureLevel = 1 // 内存中等压力，选择性刷盘
	PressureRed    PressureLevel = 2 // 内存紧张，激进刷盘
)

// MemoryMonitorConfig 内存监控器配置
type MemoryMonitorConfig struct {
	MaxBufferMemoryMB int // 0=自动检测, >0=手动指定硬上限
	MinBufferMemoryMB int // 最低保证缓冲内存
	GreenPct          int // 可用内存 > 此百分比 = 绿色
	RedPct            int // 可用内存 < 此百分比 = 红色
	CheckIntervalMS   int // 检查间隔（毫秒）
}

// MemoryMonitor 持续监控系统可用内存并发布压力等级。
type MemoryMonitor struct {
	cfg         MemoryMonitorConfig // 仅用于初始赋值（GreenPct/RedPct 废弃，改用 atomic）
	pressure    atomic.Int32        // 当前压力等级（无锁读取）
	totalMemory uint64              // 系统总内存（字节），启动时检测一次
	duckdbLimit uint64              // DuckDB memory_limit（字节）
	stopCh      chan struct{}
	logger      zerolog.Logger

	// === 可热加载 ===
	greenRedPct    atomic.Int64 // packed greenPct<<32 | redPct (avoids torn read)
	bufferLimit    atomic.Uint64
	minBufferBytes atomic.Uint64

	// getAvailableMem is overridden in tests to mock system memory state.
	getAvailableMem func() uint64
}

// NewMemoryMonitor 创建内存监控器并执行一次性容量检测。
func NewMemoryMonitor(cfg MemoryMonitorConfig, duckdbLimitMB int, logger zerolog.Logger) *MemoryMonitor {
	m := &MemoryMonitor{
		cfg:         cfg,
		stopCh:      make(chan struct{}),
		duckdbLimit: uint64(duckdbLimitMB) * 1024 * 1024,
		logger:      logger,
	}
	m.totalMemory = m.detectSystemMemory()
	m.greenRedPct.Store(int64(cfg.GreenPct)<<32 | int64(cfg.RedPct))
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

// detectSystemMemory 检测系统总内存。
// 优先级：cgroup v2 > cgroup v1 > /proc/meminfo (Linux) > 保守默认
func (m *MemoryMonitor) detectSystemMemory() uint64 {
	// 1. cgroup v2: /sys/fs/cgroup/memory.max
	if max, err := readUint64File("/sys/fs/cgroup/memory.max"); err == nil && max > 0 && max < (1<<50) {
		return max
	}
	// 2. cgroup v1: /sys/fs/cgroup/memory/memory.limit_in_bytes
	if max, err := readUint64File("/sys/fs/cgroup/memory/memory.limit_in_bytes"); err == nil && max > 0 && max < (1<<50) {
		return max
	}
	// 3. /proc/meminfo (Linux)
	if data, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "MemTotal:") {
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					if kb, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
						return kb * 1024
					}
				}
			}
		}
	}
	// 4. 保守估算 (macOS 等)
	return uint64(8 * 1024 * 1024 * 1024) // 8GB
}

func readUint64File(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	s := strings.TrimSpace(string(data))
	if s == "max" {
		return 0, nil
	}
	return strconv.ParseUint(s, 10, 64)
}

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

// getAvailableMemory 获取当前可用内存（实时）。
func (m *MemoryMonitor) getAvailableMemory() uint64 {
	// 1. cgroup v2
	if current, err := readUint64File("/sys/fs/cgroup/memory.current"); err == nil && current > 0 {
		return m.totalMemory - current
	}
	// 2. cgroup v1
	if usage, err := readUint64File("/sys/fs/cgroup/memory/memory.usage_in_bytes"); err == nil && usage > 0 {
		return m.totalMemory - usage
	}
	// 3. runtime 估算
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	heapIdle := ms.HeapIdle - ms.HeapReleased
	return heapIdle + (m.totalMemory / 10)
}

// check 读取当前可用内存并计算压力等级。
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

	v := m.greenRedPct.Load()
	greenPct := int(v >> 32)
	redPct := int(v & 0xFFFFFFFF)

	if availPct < redPct {
		return PressureRed
	}
	if availPct < greenPct {
		return PressureYellow
	}
	return PressureGreen
}

// Run 启动监控循环。按配置的间隔检查内存。
func (m *MemoryMonitor) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(m.cfg.CheckIntervalMS) * time.Millisecond)
	defer ticker.Stop()

	m.pressure.Store(int32(m.check()))

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.pressure.Store(int32(m.check()))
		}
	}
}

// PressureLevel 返回当前内存压力等级（无锁，安全热路径调用）。
func (m *MemoryMonitor) PressureLevel() PressureLevel {
	return PressureLevel(m.pressure.Load())
}

// BufferLimit 返回缓冲内存硬上限（字节）。
func (m *MemoryMonitor) BufferLimit() uint64 {
	return m.bufferLimit.Load()
}

// MinBufferBytes 返回最低保证缓冲内存（字节）。
func (m *MemoryMonitor) MinBufferBytes() uint64 {
	return m.minBufferBytes.Load()
}

// Stop 停止监控循环。
func (m *MemoryMonitor) Stop() {
	close(m.stopCh)
}

// ValidateConfig 实现 config.Reloadable 接口。
func (m *MemoryMonitor) ValidateConfig(payload *config.ReloadPayload) error {
	if payload == nil || payload.Ingest == nil {
		return nil
	}
	ic := payload.Ingest
	if ic.GreenPct < 0 || ic.GreenPct > 100 || ic.RedPct < 0 || ic.RedPct > 100 {
		return fmt.Errorf("green_pct (%d) and red_pct (%d) must be in range [0, 100]",
			ic.GreenPct, ic.RedPct)
	}
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
func (m *MemoryMonitor) ReloadConfig(payload *config.ReloadPayload) error {
	if payload == nil || payload.Ingest == nil {
		return nil
	}
	ic := payload.Ingest
	m.greenRedPct.Store(int64(ic.GreenPct)<<32 | int64(ic.RedPct))
	m.minBufferBytes.Store(uint64(ic.MinBufferMemoryMB) * 1024 * 1024)
	m.bufferLimit.Store(m.computeBufferLimitFor(ic.MaxBufferMemoryMB))
	m.logger.Info().
		Int("green_pct", ic.GreenPct).
		Int("red_pct", ic.RedPct).
		Uint64("buffer_limit_mb", m.BufferLimit()/(1024*1024)).
		Msg("内存监控器配置已热加载")
	return nil
}
