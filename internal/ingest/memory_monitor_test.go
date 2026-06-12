package ingest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

func TestMemoryMonitor_PressureLevels(t *testing.T) {
	t.Run("green when available above green pct", func(t *testing.T) {
		m := &MemoryMonitor{
			cfg:         MemoryMonitorConfig{GreenPct: 50, RedPct: 20},
			totalMemory: 1000,
			getAvailableMem: func() uint64 { return 600 },
		}
		m.greenPct.Store(50)
		m.redPct.Store(20)
		if level := m.check(); level != PressureGreen {
			t.Errorf("expected PressureGreen, got %d (available=600/1000=60%%)", level)
		}
	})

	t.Run("yellow when between green and red", func(t *testing.T) {
		m := &MemoryMonitor{
			cfg:         MemoryMonitorConfig{GreenPct: 50, RedPct: 20},
			totalMemory: 1000,
			getAvailableMem: func() uint64 { return 350 },
		}
		m.greenPct.Store(50)
		m.redPct.Store(20)
		if level := m.check(); level != PressureYellow {
			t.Errorf("expected PressureYellow, got %d (available=350/1000=35%%)", level)
		}
	})

	t.Run("red when available below red pct", func(t *testing.T) {
		m := &MemoryMonitor{
			cfg:         MemoryMonitorConfig{GreenPct: 50, RedPct: 20},
			totalMemory: 1000,
			getAvailableMem: func() uint64 { return 100 },
		}
		m.greenPct.Store(50)
		m.redPct.Store(20)
		if level := m.check(); level != PressureRed {
			t.Errorf("expected PressureRed, got %d (available=100/1000=10%%)", level)
		}
	})

	t.Run("red boundary at red pct", func(t *testing.T) {
		m := &MemoryMonitor{
			cfg:         MemoryMonitorConfig{GreenPct: 50, RedPct: 20},
			totalMemory: 1000,
			getAvailableMem: func() uint64 { return 200 },
		}
		m.greenPct.Store(50)
		m.redPct.Store(20)
		// 200/1000 = 20%, which is NOT < 20% (strict less-than)
		// So it should be yellow, not red
		if level := m.check(); level != PressureYellow {
			t.Errorf("expected PressureYellow at red boundary, got %d", level)
		}
	})

	t.Run("yellow boundary at green pct", func(t *testing.T) {
		m := &MemoryMonitor{
			cfg:         MemoryMonitorConfig{GreenPct: 50, RedPct: 20},
			totalMemory: 1000,
			getAvailableMem: func() uint64 { return 500 },
		}
		m.greenPct.Store(50)
		m.redPct.Store(20)
		// 500/1000 = 50%, which is NOT < 50% (strict less-than)
		// So it should be green
		if level := m.check(); level != PressureGreen {
			t.Errorf("expected PressureGreen at green boundary, got %d", level)
		}
	})

	t.Run("green when totalMemory is 0 (safety)", func(t *testing.T) {
		m := &MemoryMonitor{
			cfg:         MemoryMonitorConfig{GreenPct: 50, RedPct: 20},
			totalMemory: 0,
		}
		if level := m.check(); level != PressureGreen {
			t.Errorf("expected PressureGreen (safety fallback), got %d", level)
		}
	})
}

func TestMemoryMonitor_BufferLimit(t *testing.T) {
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)

	t.Run("manual override", func(t *testing.T) {
		m := NewMemoryMonitor(MemoryMonitorConfig{
			MaxBufferMemoryMB: 512,
			MinBufferMemoryMB: 64,
		}, 0, logger)
		if m.BufferLimit() != 512*1024*1024 {
			t.Errorf("expected 512MB, got %d bytes", m.BufferLimit())
		}
	})

	t.Run("auto detect with duckdb limit", func(t *testing.T) {
		m := &MemoryMonitor{
			cfg: MemoryMonitorConfig{
				MaxBufferMemoryMB: 0, // auto
				MinBufferMemoryMB: 64,
			},
			totalMemory: 16 * 1024 * 1024 * 1024, // 16GB
			duckdbLimit: 4 * 1024 * 1024 * 1024,  // 4GB
		}
		m.minBufferBytes.Store(64 * 1024 * 1024)
		m.bufferLimit.Store(m.computeBufferLimitFor(m.cfg.MaxBufferMemoryMB))
		// half = 8GB, minus duckdb 4GB = 4GB
		expected := uint64(4 * 1024 * 1024 * 1024)
		if m.BufferLimit() != expected {
			t.Errorf("expected 4GB (8GB - 4GB), got %d bytes", m.BufferLimit())
		}
	})

	t.Run("auto detect with min floor", func(t *testing.T) {
		m := &MemoryMonitor{
			cfg: MemoryMonitorConfig{
				MaxBufferMemoryMB: 0,   // auto
				MinBufferMemoryMB: 512, // floor
			},
			totalMemory: 100 * 1024 * 1024, // 100MB
			duckdbLimit: 0,
		}
		m.minBufferBytes.Store(512 * 1024 * 1024)
		m.bufferLimit.Store(m.computeBufferLimitFor(m.cfg.MaxBufferMemoryMB))
		// half = 50MB, but min floor = 512MB
		expected := uint64(512 * 1024 * 1024)
		if m.BufferLimit() != expected {
			t.Errorf("expected min floor 512MB, got %d bytes", m.BufferLimit())
		}
	})
}

func TestMemoryMonitor_DetectSystemMemory(t *testing.T) {
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)

	t.Run("cgroup v2", func(t *testing.T) {
		// Create a temporary cgroup v2-like file
		dir := t.TempDir()
		cgroupV2Path := filepath.Join(dir, "memory.max")
		os.WriteFile(cgroupV2Path, []byte("8589934592\n"), 0644)
		// We can't easily mock the filesystem, but the function tries /sys/fs/cgroup/memory.max
		// On macOS this will fall through to the 8GB default.
		// This test documents the expected behavior on Linux.
		_ = cgroupV2Path
	})

	t.Run("fallback default", func(t *testing.T) {
		m := &MemoryMonitor{cfg: MemoryMonitorConfig{}, logger: logger}
		result := m.detectSystemMemory()
		if result == 0 {
			t.Error("detectSystemMemory should return non-zero even on unsupported platforms")
		}
		t.Logf("Detected/fallback memory: %d MB", result/(1024*1024))
	})
}

func TestPressureLevel_AtomicConcurrency(t *testing.T) {
	m := &MemoryMonitor{
		cfg:         MemoryMonitorConfig{GreenPct: 50, RedPct: 20},
		totalMemory: 1000,
	}
	m.greenPct.Store(50)
	m.redPct.Store(20)
	// Verify atomic store/load works across concurrent access
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			m.pressure.Store(int32(PressureGreen))
			m.pressure.Store(int32(PressureYellow))
			m.pressure.Store(int32(PressureRed))
		}
		close(done)
	}()

	for i := 0; i < 1000; i++ {
		level := m.PressureLevel()
		if level < PressureGreen || level > PressureRed {
			t.Errorf("invalid pressure level: %d", level)
		}
	}
	<-done
}
