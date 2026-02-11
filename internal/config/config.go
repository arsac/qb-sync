package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Default configuration values.
const (
	defaultMinSpaceGB           = 50
	defaultMinSeedingTimeSec    = 3600
	defaultSleepIntervalSec     = 30
	defaultPollIntervalSec      = 2
	defaultPollTimeoutSec       = 300 // 5 minutes
	defaultPieceTimeoutSec      = 60
	defaultListenAddr           = ":50051"
	defaultHealthAddr           = ":8080"
	defaultSyncedTag            = "synced"
	defaultSourceRemovedTag     = "source-removed"
	defaultReconnectMaxDelaySec = 30
	defaultNumSenders           = 4
	defaultMinGRPCConnections   = 2
	defaultMaxGRPCConnections   = 8
	DefaultDrainTimeoutSec      = 300 // 5 minutes
	defaultMaxStreamBufferMB    = 512
)

// BaseConfig contains configuration shared between hot and cold servers.
type BaseConfig struct {
	// qBittorrent connection
	QBURL      string
	QBUsername string
	QBPassword string

	// Data path where torrent content is stored/written
	DataPath string

	// Health server
	HealthAddr string // HTTP health endpoint address (e.g., ":8080")

	// Tag to apply to synced torrents (for visibility in qBittorrent UI)
	SyncedTag string

	// Logging
	LogLevel string // Log level: debug, info, warn, error (default: info)

	DryRun bool
}

// HotConfig contains configuration for the hot (source) server.
type HotConfig struct {
	BaseConfig

	// Streaming destination
	ColdAddr string // gRPC address of cold server

	// Migration settings
	MinSpaceGB     int64
	MinSeedingTime time.Duration
	SleepInterval  time.Duration

	// Drain annotation key checked on shutdown to gate drain (empty = drain unconditionally on SIGTERM)
	DrainAnnotation string

	// Timeout for shutdown drain operation (default: 5m)
	DrainTimeout time.Duration

	// Streaming tuning
	PieceTimeout       time.Duration // Timeout for stale in-flight pieces (default: 60s)
	MaxBytesPerSec     int64
	ReconnectMaxDelay  time.Duration // Max reconnect backoff delay (default: 30s)
	NumSenders         int           // Concurrent sender workers for streaming (default: 4)
	MinGRPCConnections int           // Minimum TCP connections to cold server (default: 2)
	MaxGRPCConnections int           // Maximum TCP connections to cold server (default: 8)
	SourceRemovedTag   string        // Tag applied on cold when torrent is removed from hot (empty to disable)
	ExcludeCleanupTag  string        // Tag that prevents torrents from being cleaned up from hot (empty to disable)
}

// Validate validates the base configuration shared by hot and cold.
func (c *BaseConfig) Validate() error {
	if c.DataPath == "" {
		return errors.New("data path is required")
	}
	return nil
}

// Validate validates the hot configuration.
func (c *HotConfig) Validate() error {
	if err := c.BaseConfig.Validate(); err != nil {
		return err
	}
	if c.QBURL == "" {
		return errors.New("qBittorrent URL is required")
	}
	if c.ColdAddr == "" {
		return errors.New("cold server address is required")
	}
	if c.MinSpaceGB < 0 {
		return errors.New("minimum space cannot be negative")
	}
	if c.SleepInterval < 0 {
		return errors.New("sleep interval cannot be negative")
	}
	if c.PieceTimeout < 0 {
		return errors.New("piece timeout cannot be negative")
	}
	if c.MaxBytesPerSec < 0 {
		return errors.New("max bytes per second cannot be negative")
	}
	if c.ReconnectMaxDelay < 0 {
		return errors.New("reconnect max delay cannot be negative")
	}
	if c.MinGRPCConnections < 0 {
		return errors.New("min connections cannot be negative")
	}
	if c.MaxGRPCConnections < 0 {
		return errors.New("max connections cannot be negative")
	}
	if c.MinGRPCConnections > 0 && c.MaxGRPCConnections > 0 && c.MinGRPCConnections > c.MaxGRPCConnections {
		return errors.New("min connections cannot exceed max connections")
	}
	return nil
}

// ColdConfig contains configuration for the cold (destination) server.
type ColdConfig struct {
	BaseConfig

	// gRPC server
	ListenAddr string

	// SavePath is the path as cold qBittorrent sees it (container mount point).
	// Defaults to DataPath when empty.
	SavePath string

	// Polling settings for torrent verification
	PollInterval time.Duration
	PollTimeout  time.Duration

	// Streaming tuning
	StreamWorkers     int // Number of concurrent piece writers (0 = use default 8)
	MaxStreamBufferMB int // Global memory budget in MB for buffered piece data (default: 512)
}

// Validate validates the cold configuration.
func (c *ColdConfig) Validate() error {
	if err := c.BaseConfig.Validate(); err != nil {
		return err
	}
	if c.ListenAddr == "" {
		return errors.New("listen address is required")
	}
	if c.PollInterval < 0 {
		return errors.New("poll interval cannot be negative")
	}
	if c.PollTimeout < 0 {
		return errors.New("poll timeout cannot be negative")
	}
	if c.StreamWorkers < 0 {
		return errors.New("stream workers cannot be negative")
	}
	if c.MaxStreamBufferMB < 0 {
		return errors.New("max stream buffer cannot be negative")
	}
	return nil
}

// SetupHotFlags sets up flags for the hot command.
func SetupHotFlags(cmd *cobra.Command) {
	flags := cmd.Flags()

	flags.String("data", "", "Data directory path where torrent content is stored")
	flags.String("qb-url", "", "qBittorrent WebUI URL")
	flags.String("qb-username", "", "qBittorrent username")
	flags.String("qb-password", "", "qBittorrent password")
	flags.String("cold-addr", "", "Cold server gRPC address (e.g., 192.168.1.100:50051)")
	flags.Int64("min-space", defaultMinSpaceGB, "Minimum free space in GB before moving torrents")
	flags.Int("min-seeding-time", defaultMinSeedingTimeSec, "Minimum seeding time in seconds before moving")
	flags.Int("sleep", defaultSleepIntervalSec, "Sleep interval between checks in seconds")
	flags.String(
		"drain-annotation",
		"qbsync/drain",
		"Pod annotation key checked on shutdown to gate drain (empty = drain unconditionally on SIGTERM)",
	)
	flags.Int("drain-timeout", DefaultDrainTimeoutSec, "Timeout in seconds for shutdown drain operation")
	flags.Int64("rate-limit", 0, "Max bytes/sec for streaming (0 = unlimited)")
	flags.Int(
		"piece-timeout",
		defaultPieceTimeoutSec,
		"Timeout in seconds for stale in-flight pieces (increase for high-latency links)",
	)
	flags.Int(
		"reconnect-max-delay",
		defaultReconnectMaxDelaySec,
		"Max reconnect backoff delay in seconds (decrease for unstable links)",
	)
	flags.Int(
		"num-senders",
		defaultNumSenders,
		"Concurrent sender workers for streaming (increase for high-throughput links)",
	)
	flags.Int(
		"min-connections",
		defaultMinGRPCConnections,
		"Minimum TCP connections for gRPC streaming (connections scale up from this)",
	)
	flags.Int(
		"max-connections",
		defaultMaxGRPCConnections,
		"Maximum TCP connections for gRPC streaming (connections scale up to this)",
	)
	flags.String(
		"source-removed-tag",
		defaultSourceRemovedTag,
		"Tag to apply on cold torrent when source is removed from hot (empty to disable)",
	)
	flags.String(
		"exclude-cleanup-tag",
		"",
		"Tag that prevents torrents from being cleaned up from hot (empty to disable)",
	)
	flags.String("health-addr", defaultHealthAddr, "HTTP health endpoint address (empty to disable)")
	flags.String("synced-tag", defaultSyncedTag, "Tag to apply to synced torrents (empty to disable)")
	flags.Bool("dry-run", false, "Run without making changes")
	flags.String("log-level", "info", "Log level: debug, info, warn, error")
}

// SetupColdFlags sets up flags for the cold command.
func SetupColdFlags(cmd *cobra.Command) {
	flags := cmd.Flags()

	flags.String("listen", defaultListenAddr, "gRPC listen address")
	flags.String("data", "", "Data directory path where torrent content will be written")
	flags.String("save-path", "", "Save path as cold qBittorrent sees it (defaults to --data)")
	flags.String("qb-url", "", "qBittorrent WebUI URL (for adding verified torrents)")
	flags.String("qb-username", "", "qBittorrent username")
	flags.String("qb-password", "", "qBittorrent password")
	flags.Int("poll-interval", defaultPollIntervalSec, "Poll interval in seconds for torrent verification")
	flags.Int("poll-timeout", defaultPollTimeoutSec, "Poll timeout in seconds for torrent verification")
	flags.Int("stream-workers", 0, "Number of concurrent piece writers (0 = auto: 8, increase for SSD/NVMe)")
	flags.Int(
		"max-stream-buffer",
		defaultMaxStreamBufferMB,
		"Global memory budget in MB for buffered piece data across all streams",
	)
	flags.String("health-addr", defaultHealthAddr, "HTTP health endpoint address (empty to disable)")
	flags.String("synced-tag", defaultSyncedTag, "Tag to apply to synced torrents (empty to disable)")
	flags.Bool("dry-run", false, "Run without making changes")
	flags.String("log-level", "info", "Log level: debug, info, warn, error")
}

// bindFlags configures viper with an env prefix and binds the given flag names.
func bindFlags(cmd *cobra.Command, v *viper.Viper, envPrefix string, flags []string) error {
	v.SetEnvPrefix(envPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	v.AutomaticEnv()

	for _, flag := range flags {
		if err := v.BindPFlag(flag, cmd.Flags().Lookup(flag)); err != nil {
			return fmt.Errorf("binding flag %s: %w", flag, err)
		}
	}

	return nil
}

// BindHotFlags binds hot command flags to viper.
func BindHotFlags(cmd *cobra.Command, v *viper.Viper) error {
	return bindFlags(cmd, v, "QBSYNC_HOT", []string{
		"data", "qb-url", "qb-username", "qb-password",
		"cold-addr", "min-space", "min-seeding-time", "sleep",
		"rate-limit", "piece-timeout", "reconnect-max-delay",
		"num-senders", "min-connections", "max-connections",
		"source-removed-tag", "exclude-cleanup-tag", "health-addr", "synced-tag",
		"dry-run", "log-level", "drain-annotation", "drain-timeout",
	})
}

// BindColdFlags binds cold command flags to viper.
func BindColdFlags(cmd *cobra.Command, v *viper.Viper) error {
	return bindFlags(cmd, v, "QBSYNC_COLD", []string{
		"listen", "data", "save-path", "qb-url", "qb-username", "qb-password",
		"poll-interval", "poll-timeout", "stream-workers", "max-stream-buffer",
		"health-addr", "synced-tag", "dry-run", "log-level",
	})
}

// seconds returns a viper int key as a time.Duration in seconds.
func seconds(v *viper.Viper, key string) time.Duration {
	return time.Duration(v.GetInt(key)) * time.Second
}

// loadBase loads the base configuration shared by hot and cold.
func loadBase(v *viper.Viper) BaseConfig {
	return BaseConfig{
		QBURL:      v.GetString("qb-url"),
		QBUsername: v.GetString("qb-username"),
		QBPassword: v.GetString("qb-password"),
		DataPath:   v.GetString("data"),
		HealthAddr: v.GetString("health-addr"),
		SyncedTag:  v.GetString("synced-tag"),
		LogLevel:   v.GetString("log-level"),
		DryRun:     v.GetBool("dry-run"),
	}
}

// LoadHot loads the hot server configuration from viper.
func LoadHot(v *viper.Viper) (*HotConfig, error) {
	cfg := &HotConfig{
		BaseConfig:         loadBase(v),
		ColdAddr:           v.GetString("cold-addr"),
		MinSpaceGB:         v.GetInt64("min-space"),
		MinSeedingTime:     seconds(v, "min-seeding-time"),
		SleepInterval:      seconds(v, "sleep"),
		DrainAnnotation:    v.GetString("drain-annotation"),
		DrainTimeout:       seconds(v, "drain-timeout"),
		PieceTimeout:       seconds(v, "piece-timeout"),
		MaxBytesPerSec:     v.GetInt64("rate-limit"),
		ReconnectMaxDelay:  seconds(v, "reconnect-max-delay"),
		NumSenders:         v.GetInt("num-senders"),
		MinGRPCConnections: v.GetInt("min-connections"),
		MaxGRPCConnections: v.GetInt("max-connections"),
		SourceRemovedTag:   v.GetString("source-removed-tag"),
		ExcludeCleanupTag:  v.GetString("exclude-cleanup-tag"),
	}

	// Support conventional env vars as fallbacks
	if cfg.HealthAddr == defaultHealthAddr {
		cfg.HealthAddr = getEnvWithFallbacks(cfg.HealthAddr, "HTTP_PORT", "HEALTH_PORT")
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// LoadCold loads the cold server configuration from viper.
func LoadCold(v *viper.Viper) (*ColdConfig, error) {
	cfg := &ColdConfig{
		BaseConfig:        loadBase(v),
		ListenAddr:        v.GetString("listen"),
		SavePath:          v.GetString("save-path"),
		PollInterval:      seconds(v, "poll-interval"),
		PollTimeout:       seconds(v, "poll-timeout"),
		StreamWorkers:     v.GetInt("stream-workers"),
		MaxStreamBufferMB: v.GetInt("max-stream-buffer"),
	}

	// Support conventional env vars as fallbacks
	if cfg.ListenAddr == defaultListenAddr {
		cfg.ListenAddr = getEnvWithFallbacks(cfg.ListenAddr, "GRPC_PORT", "PORT")
	}
	if cfg.HealthAddr == defaultHealthAddr {
		cfg.HealthAddr = getEnvWithFallbacks(cfg.HealthAddr, "HTTP_PORT", "HEALTH_PORT")
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// getEnvWithFallbacks returns the first non-empty env var value, or the default.
// For port-only values (e.g., "8080"), it prepends ":" to make a valid address.
func getEnvWithFallbacks(defaultVal string, envVars ...string) string {
	for _, env := range envVars {
		if val := os.Getenv(env); val != "" {
			// If it's just a port number, prepend ":"
			if !strings.Contains(val, ":") {
				val = ":" + val
			}
			return val
		}
	}
	return defaultVal
}
