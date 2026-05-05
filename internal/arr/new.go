package arr

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"time"
)

// Config is the input to New. Both instances are optional; an empty Config
// produces a noopFilter.
type Config struct {
	Radarr InstanceConfig
	Sonarr InstanceConfig

	// PerCallTimeout bounds each HTTP round-trip. Default: 3s.
	PerCallTimeout time.Duration

	// CacheTTL is the TTL for non-terminal SYNC decisions (e.g. empty history, not rejected).
	// Terminal SKIP decisions (ignored, failed) use a fixed long TTL regardless of this value.
	// Default: 15s. Callers typically pass SleepInterval/2.
	CacheTTL time.Duration

	// BreakerMaxFailures triggers the breaker after N consecutive failures. <=0 disables.
	BreakerMaxFailures int

	// BreakerResetTimeout is the cool-down before half-open. Default: 60s.
	BreakerResetTimeout time.Duration
}

// InstanceConfig is one *arr instance's connection + routing.
type InstanceConfig struct {
	URL        string
	APIKey     string
	Categories []string
}

const (
	defaultCacheTTL            = 15 * time.Second
	defaultBreakerResetTimeout = 60 * time.Second
)

// IsZero reports whether the instance is unconfigured.
func (i InstanceConfig) IsZero() bool {
	return i.URL == "" && i.APIKey == "" && len(i.Categories) == 0
}

// New builds a Filter from cfg. Always returns a usable Filter (never nil).
// If both Radarr and Sonarr are unconfigured, returns a noopFilter.
func New(cfg Config, logger *slog.Logger) (Filter, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.PerCallTimeout <= 0 {
		cfg.PerCallTimeout = defaultPerCallTimeout
	}
	if cfg.CacheTTL <= 0 {
		cfg.CacheTTL = defaultCacheTTL
	}
	if cfg.BreakerResetTimeout <= 0 {
		cfg.BreakerResetTimeout = defaultBreakerResetTimeout
	}

	if cfg.Radarr.IsZero() && cfg.Sonarr.IsZero() {
		return noopFilter{}, nil
	}

	instances := make(map[string]*instanceState)
	routes := make(map[string]string)

	if err := addInstance(
		instances,
		routes,
		"radarr",
		cfg.Radarr,
		cfg.PerCallTimeout,
		cfg.BreakerMaxFailures,
		cfg.BreakerResetTimeout,
	); err != nil {
		return nil, err
	}
	if err := addInstance(
		instances,
		routes,
		"sonarr",
		cfg.Sonarr,
		cfg.PerCallTimeout,
		cfg.BreakerMaxFailures,
		cfg.BreakerResetTimeout,
	); err != nil {
		return nil, err
	}

	return &Service{
		instances: instances,
		routes:    routes,
		cache:     newVerdictCache(),
		cacheTTL:  cfg.CacheTTL,
		logger:    logger,
	}, nil
}

// addInstance validates and inserts an instance into the maps.
func addInstance(
	instances map[string]*instanceState,
	routes map[string]string,
	name string,
	cfg InstanceConfig,
	perCall time.Duration,
	breakerMax int,
	breakerReset time.Duration,
) error {
	if cfg.IsZero() {
		return nil
	}
	if cfg.URL == "" {
		return fmt.Errorf("arr.%s: URL is required when instance is configured", name)
	}
	if cfg.APIKey == "" {
		return fmt.Errorf("arr.%s: API key is required when URL is set", name)
	}
	if len(cfg.Categories) == 0 {
		return fmt.Errorf("arr.%s: at least one category must be configured", name)
	}
	if _, err := url.Parse(cfg.URL); err != nil {
		return fmt.Errorf("arr.%s: invalid URL: %w", name, err)
	}
	for _, cat := range cfg.Categories {
		if existing, dup := routes[cat]; dup {
			return fmt.Errorf("arr.%s: category %q is also assigned to %q", name, cat, existing)
		}
		routes[cat] = name
	}

	inst := &instanceState{
		name:       name,
		client:     NewClient(cfg.URL, cfg.APIKey, perCall),
		categories: cfg.Categories,
	}
	if breakerMax > 0 {
		attachBreaker(inst, breakerConfig{MaxFailures: breakerMax, ResetTimeout: breakerReset})
	}
	instances[name] = inst
	return nil
}

// PingAll calls Ping on every configured instance and returns a map
// of instance-name to error (nil on success). Used by the runner for
// startup logging only — never gates startup.
func PingAll(ctx context.Context, f Filter) map[string]error {
	svc, ok := f.(*Service)
	if !ok {
		return nil
	}
	out := make(map[string]error, len(svc.instances))
	for name, inst := range svc.instances {
		out[name] = inst.client.Ping(ctx)
	}
	return out
}
