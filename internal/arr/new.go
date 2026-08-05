package arr

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"github.com/arsac/qb-sync/internal/utils"
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
//
// This is also the shape held by the process config, so there is one
// declaration rather than a parallel struct copied across the boundary: a new
// field would otherwise need adding in two places and copying in two more, and
// a forgotten copy compiles cleanly while silently doing nothing.
type InstanceConfig struct {
	URL    string
	APIKey string
}

// String returns a redacted representation suitable for logging. Config is
// logged at startup and dumped by the health endpoint, so a plain struct would
// put the API key in both.
func (i InstanceConfig) String() string {
	return fmt.Sprintf("InstanceConfig{URL:%q, APIKey:%s}", i.URL, redactAPIKey(i.APIKey))
}

// MarshalJSON masks the API key in JSON output.
func (i InstanceConfig) MarshalJSON() ([]byte, error) {
	type alias struct {
		URL    string `json:"url"`
		APIKey string `json:"api_key"`
	}

	return json.Marshal(alias{URL: i.URL, APIKey: redactAPIKey(i.APIKey)})
}

// redactAPIKey returns a fixed mask rather than a prefix: a few characters of a
// key are still worth withholding, and a fixed mask cannot leak length either.
func redactAPIKey(key string) string {
	if key == "" {
		return "<unset>"
	}
	return "***"
}

// Validate rejects a partially configured instance. Both fields or neither: a
// URL with no key, or a key with no URL, is a deployment mistake that would
// otherwise present as the filter silently never running.
//
// Categories are not configured. They are discovered from the instance, which
// is the authority on which ones it assigns.
func (i InstanceConfig) Validate(name string) error {
	if i.IsZero() {
		return nil
	}
	if i.URL == "" {
		return fmt.Errorf("%s: URL is required when an API key is set", name)
	}
	if i.APIKey == "" {
		return fmt.Errorf("%s: API key is required when a URL is set", name)
	}
	if _, err := url.Parse(i.URL); err != nil {
		return fmt.Errorf("%s: invalid URL: %w", name, err)
	}
	return nil
}

// Validate checks both instances.
//
// The single rule set: process config delegates here rather than keeping its
// own copy, so a rule tightened in one place cannot fail to apply on the other
// path. New must validate regardless, since the destination builds a Config
// directly rather than through process config.
func (c Config) Validate() error {
	if err := c.Radarr.Validate(instanceRadarr); err != nil {
		return err
	}
	return c.Sonarr.Validate(instanceSonarr)
}

// CategoryRefreshInterval is how often routing is rediscovered. It lives here
// because both sides refresh on their own schedule - the source against its own
// instances or the destination's relay, the destination against its instances -
// and a knob tuned on one side but not the other would drift silently.
const CategoryRefreshInterval = 5 * time.Minute

const (
	defaultCacheTTL            = 15 * time.Second
	defaultBreakerResetTimeout = 60 * time.Second

	// Instance names, used for routing, error text and metric labels.
	instanceRadarr = "radarr"
	instanceSonarr = "sonarr"
)

// IsZero reports whether the instance is unconfigured.
func (i InstanceConfig) IsZero() bool {
	return i.URL == "" && i.APIKey == ""
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

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	if cfg.Radarr.IsZero() && cfg.Sonarr.IsZero() {
		return noopFilter{}, nil
	}

	instances := make(map[string]*instanceState)

	addInstance(
		instances,
		instanceRadarr,
		cfg.Radarr,
		cfg.PerCallTimeout,
		cfg.BreakerMaxFailures,
		cfg.BreakerResetTimeout,
	)
	addInstance(
		instances,
		instanceSonarr,
		cfg.Sonarr,
		cfg.PerCallTimeout,
		cfg.BreakerMaxFailures,
		cfg.BreakerResetTimeout,
	)

	return &Service{
		instances: instances,
		routes:    make(map[string]string),
		cache:     newVerdictCache(),
		cacheTTL:  cfg.CacheTTL,
		logger:    logger,
		now:       time.Now,
	}, nil
}

// addInstance registers an instance and its category routes. Shape and routing
// rejected by Config.Validate, which New runs first. Routing is not set here:
// it is discovered from the instance by RefreshCategories.
func addInstance(
	instances map[string]*instanceState,
	name string,
	cfg InstanceConfig,
	perCall time.Duration,
	breakerMax int,
	breakerReset time.Duration,
) {
	if cfg.IsZero() {
		return
	}

	inst := &instanceState{
		name:   name,
		client: NewClient(cfg.URL, cfg.APIKey, perCall),
	}
	// attachBreaker already returns early on a non-positive threshold.
	attachBreaker(inst, utils.CircuitBreakerConfig{MaxFailures: breakerMax, ResetTimeout: breakerReset})
	instances[name] = inst
}

// PingAll calls Ping on every configured instance and returns a map
// of instance-name to error (nil on success). Used by the runner for
// startup logging only - never gates startup.
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
