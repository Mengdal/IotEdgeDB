package config

import (
	"fmt"
	"strings"
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
// 使用与 Load() 一致的 viper 默认值和环境变量绑定。
func parseIngestConfig(configPath string) (*IngestReloadConfig, error) {
	v := viper.New()
	v.SetConfigFile(configPath)
	v.SetConfigType("toml")

	// 与 Load() 一致的默认值
	v.SetDefault("ingest.max_buffer_memory_mb", 0)
	v.SetDefault("ingest.min_buffer_memory_mb", 128)
	v.SetDefault("ingest.max_buffer_age_seconds", 900)
	v.SetDefault("ingest.memory_pressure_green_pct", 50)
	v.SetDefault("ingest.memory_pressure_red_pct", 20)

	// 与 Load() 一致的环境变量绑定
	v.SetEnvPrefix("IEDB")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

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
