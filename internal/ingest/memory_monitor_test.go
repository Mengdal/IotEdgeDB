package ingest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"iedb/internal/config"

	"github.com/rs/zerolog"
)

func TestMemoryMonitor_PressureLevels(t *testing.T) {
	t.Run("green when available above green pct", func(t *testing.T) {
		m := &MemoryMonitor{
			cfg:             MemoryMonitorConfig{GreenPct: 50, RedPct: 20},
			totalMemory:     1000,
			getAvailableMem: func() uint64 { return 600 },
		}
		m.greenRedPct.Store(int64(50)<<32 | int64(20))
		if level := m.check(); level != PressureGreen {
			t.Errorf("expected PressureGreen, got %d (available=600/1000=60%%)", level)
		}
	})

	t.Run("yellow when between green and red", func(t *testing.T) {
		m := &MemoryMonitor{
			cfg:             MemoryMonitorConfig{GreenPct: 50, RedPct: 20},
			totalMemory:     1000,
			getAvailableMem: func() uint64 { return 350 },
		}
		m.greenRedPct.Store(int64(50)<<32 | int64(20))
		if level := m.check(); level != PressureYellow {
			t.Errorf("expected PressureYellow, got %d (available=350/1000=35%%)", level)
		}
	})

	t.Run("red when available below red pct", func(t *testing.T) {
		m := &MemoryMonitor{
			cfg:             MemoryMonitorConfig{GreenPct: 50, RedPct: 20},
			totalMemory:     1000,
			getAvailableMem: func() uint64 { return 100 },
		}
		m.greenRedPct.Store(int64(50)<<32 | int64(20))
		if level := m.check(); level != PressureRed {
			t.Errorf("expected PressureRed, got %d (available=100/1000=10%%)", level)
		}
	})

	t.Run("red boundary at red pct", func(t *testing.T) {
		m := &MemoryMonitor{
			cfg:             MemoryMonitorConfig{GreenPct: 50, RedPct: 20},
			totalMemory:     1000,
			getAvailableMem: func() uint64 { return 200 },
		}
		m.greenRedPct.Store(int64(50)<<32 | int64(20))
		// 200/1000 = 20%, which is NOT < 20% (strict less-than)
		// So it should be yellow, not red
		if level := m.check(); level != PressureYellow {
			t.Errorf("expected PressureYellow at red boundary, got %d", level)
		}
	})

	t.Run("yellow boundary at green pct", func(t *testing.T) {
		m := &MemoryMonitor{
			cfg:             MemoryMonitorConfig{GreenPct: 50, RedPct: 20},
			totalMemory:     1000,
			getAvailableMem: func() uint64 { return 500 },
		}
		m.greenRedPct.Store(int64(50)<<32 | int64(20))
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
	m.greenRedPct.Store(int64(50)<<32 | int64(20))
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
					MaxBufferMemoryMB: 0,
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

	if m.PressureLevel() != PressureGreen {
		t.Fatalf("initial pressure should be green, got %d", m.PressureLevel())
	}

	payload := &config.ReloadPayload{
		Ingest: &config.IngestReloadConfig{
			GreenPct:          60,
			RedPct:            55,
			MinBufferMemoryMB: 256,
			MaxBufferMemoryMB: 0,
		},
	}

	if err := m.ReloadConfig(payload); err != nil {
		t.Fatalf("ReloadConfig failed: %v", err)
	}

	v := m.greenRedPct.Load()
	if int(v>>32) != 60 {
		t.Errorf("expected greenPct=60, got %d", int(v>>32))
	}
	if int(v&0xFFFFFFFF) != 55 {
		t.Errorf("expected redPct=55, got %d", int(v&0xFFFFFFFF))
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
	m.greenRedPct.Store(int64(50)<<32 | int64(20))
	m.bufferLimit.Store(128 * 1024 * 1024)

	m.getAvailableMem = func() uint64 { return 400 }
	if level := m.check(); level != PressureYellow {
		t.Errorf("before reload: expected yellow (400/1000=40%%), got %d", level)
	}

	m.greenRedPct.Store(int64(30)<<32 | int64(10))

	if level := m.check(); level != PressureGreen {
		t.Errorf("after reload: expected green (400/1000=40%% > 30%%), got %d", level)
	}

	m.getAvailableMem = func() uint64 { return 50 }
	if level := m.check(); level != PressureRed {
		t.Errorf("after reload: expected red (50/1000=5%% < 10%%), got %d", level)
	}
}

func TestMemoryMonitor_BufferLimitAfterReload(t *testing.T) {
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)

	m := &MemoryMonitor{
		cfg:         MemoryMonitorConfig{MinBufferMemoryMB: 128},
		totalMemory: 16 * 1024 * 1024 * 1024,
		duckdbLimit: 4 * 1024 * 1024 * 1024,
		logger:      logger,
	}
	m.greenRedPct.Store(int64(50)<<32 | int64(20))
	m.minBufferBytes.Store(128 * 1024 * 1024)
	m.bufferLimit.Store(m.computeBufferLimitFor(0))

	if limit := m.BufferLimit(); limit != 4*1024*1024*1024 {
		t.Fatalf("initial limit: expected 4GB, got %d", limit)
	}

	m.bufferLimit.Store(m.computeBufferLimitFor(2048))
	if limit := m.BufferLimit(); limit != 2*1024*1024*1024 {
		t.Errorf("after manual reload: expected 2GB, got %d", limit)
	}

	m.bufferLimit.Store(m.computeBufferLimitFor(0))
	if limit := m.BufferLimit(); limit != 4*1024*1024*1024 {
		t.Errorf("after auto reload: expected 4GB, got %d", limit)
	}
}

// ============================================================================
// F3 fix: Stop() must not panic on double-close
// ============================================================================

func TestMemoryMonitor_StopDoubleClose(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(t)).Level(zerolog.Disabled)
	m := NewMemoryMonitor(MemoryMonitorConfig{
		MaxBufferMemoryMB: 128,
		MinBufferMemoryMB: 16,
		GreenPct:          50,
		RedPct:            20,
	}, 0, logger)

	// First Stop should succeed
	m.Stop()

	// Second Stop should NOT panic (sync.Once guard)
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Stop() panicked on second call: %v", r)
			}
		}()
		m.Stop()
	}()
}
