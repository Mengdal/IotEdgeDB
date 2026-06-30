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

	if pass.validateCalls != 1 {
		t.Errorf("expected pass.validateCalls=1, got %d", pass.validateCalls)
	}
	if fail.validateCalls != 1 {
		t.Errorf("expected fail.validateCalls=1, got %d", fail.validateCalls)
	}
	if pass.reloadCalls != 0 {
		t.Errorf("expected pass.reloadCalls=0 (aborted), got %d", pass.reloadCalls)
	}
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
	if err != nil {
		t.Errorf("expected nil (apply failures don't abort), got %v", err)
	}

	if pass.validateCalls != 1 {
		t.Errorf("expected pass.validateCalls=1, got %d", pass.validateCalls)
	}
	if pass.reloadCalls != 1 {
		t.Errorf("expected pass.reloadCalls=1, got %d", pass.reloadCalls)
	}
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
