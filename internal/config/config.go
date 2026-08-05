package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Flag names used in both registration (flags.String/Bool) and viper lookups
// (v.GetString/GetBool). Defined as constants so a typo at one site cannot
// silently fail to bind without surfacing as a compile error.
const (
	flagData       = "data"
	flagSyncedTag  = "synced-tag"
	flagDryRun     = "dry-run"
	flagQBURL      = "qb-url"
	flagQBUsername = "qb-username"
	flagQBPassword = "qb-password"
	flagHealthAddr = "health-addr"
	flagLogLevel   = "log-level"
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
	defaultSyncFailedTag        = "sync-failed"
	defaultArrSkippedTag        = "arr-skipped"
	DefaultSyncFailedGuard      = 4 * time.Hour
	defaultSourceRemovedTag     = "source-removed"
	defaultReconnectMaxDelaySec = 30
	defaultNumSenders           = 4
	defaultMinGRPCConnections   = 2
	defaultMaxGRPCConnections   = 8
	DefaultDrainTimeoutSec      = 300 // 5 minutes
	defaultMaxStreamBufferMB    = 512
)

// BaseConfig contains configuration shared between source and destination servers.
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

	// Radarr and Sonarr gate syncing on whether *arr rejected the download.
	// They live on the shared config because either side may own them: whichever
	// process is colocated with the *arr instances configures them, and the
	// source relays to the destination when it has none of its own. A zero
	// instance disables filtering for its categories.
	Radarr ArrInstanceConfig
	Sonarr ArrInstanceConfig
}

// SourceConfig contains configuration for the source server.
type SourceConfig struct {
	BaseConfig

	// Streaming destination
	DestinationAddr string // gRPC address of destination server

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
	MinGRPCConnections int           // Minimum TCP connections to destination server (default: 2)
	MaxGRPCConnections int           // Maximum TCP connections to destination server (default: 8)
	SourceRemovedTag   string        // Tag applied on destination when torrent is removed from source (empty to disable)
	ExcludeCleanupTag  string        // Tag that prevents torrents from being cleaned up from source (empty to disable)
	SyncFailedTag      string        // Tag applied on source when verification fails repeatedly (empty to disable; remove tag to retry)
	// SyncFailedGuard is how long a torrent must fail continuously before it is
	// quarantined. Quarantine is duration-based, not attempt-based: a brief
	// destination outage must not permanently sideline every torrent that
	// happened to be finalizing. See docs/adr/0001.
	SyncFailedGuard time.Duration
	ExcludeSyncTag  string // Tag that prevents torrents from being synced (empty to disable)

	// ArrSkippedTag marks source torrents the *arr filter skipped (empty
	// disables). The instances themselves are configured on the destination,
	// which is where the lookup runs; the source only applies the marker.
	ArrSkippedTag string
}

// Validate validates the base configuration shared by source and destination.
// Note: QBURL is not validated here because it is required for source but
// optional for destination (destination can run without qBittorrent integration).
func (c *BaseConfig) Validate() error {
	if c.DataPath == "" {
		return errors.New("data path is required")
	}

	if err := validateArrInstance("radarr", c.Radarr); err != nil {
		return err
	}
	if err := validateArrInstance("sonarr", c.Sonarr); err != nil {
		return err
	}
	if conflict := overlappingCategory(c.Radarr.Categories, c.Sonarr.Categories); conflict != "" {
		return fmt.Errorf("category %q is configured for both radarr and sonarr", conflict)
	}

	return nil
}

// Validate validates the source configuration.
func (c *SourceConfig) Validate() error {
	if err := c.BaseConfig.Validate(); err != nil {
		return err
	}
	if c.QBURL == "" {
		return errors.New("qBittorrent URL is required")
	}
	if c.DestinationAddr == "" {
		return errors.New("destination server address is required")
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
	if c.MinSeedingTime < 0 {
		return errors.New("min seeding time cannot be negative")
	}
	if c.DrainTimeout < 0 {
		return errors.New("drain timeout cannot be negative")
	}

	return nil
}

// DestinationConfig contains configuration for the destination server.
type DestinationConfig struct {
	BaseConfig

	// gRPC server
	ListenAddr string

	// SavePath is the path as destination qBittorrent sees it (container mount point).
	// Defaults to DataPath when empty.
	SavePath string

	// Polling settings for torrent verification
	PollInterval time.Duration
	PollTimeout  time.Duration

	// Streaming tuning
	StreamWorkers     int // Number of concurrent piece writers (0 = use default 8)
	MaxStreamBufferMB int // Global memory budget in MB for buffered piece data (default: 512)
	VerifyConcurrency int // Concurrent piece-read goroutines during finalize verification (0 = use default 4)

	// QBFinalizeConcurrency is how many torrents may concurrently occupy the
	// destination qB add/recheck stage (0 = default 1, max 8).
	QBFinalizeConcurrency int
}

// Validate validates the destination configuration.
func (c *DestinationConfig) Validate() error {
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
	// Keep the bound in sync with maxVerifyConcurrencyCap in
	// internal/destination (the server clamps defensively, but this is the
	// check that produces a clear startup error for operators).
	if c.VerifyConcurrency < 0 || c.VerifyConcurrency > 16 {
		return errors.New("verify concurrency must be between 0 and 16 (0 = default 4)")
	}
	if c.MaxStreamBufferMB < 0 {
		return errors.New("max stream buffer cannot be negative")
	}
	// Keep the bound in sync with maxQBFinalizeConcurrency in
	// internal/destination (the server clamps defensively, but this is the
	// check that produces a clear startup error for operators).
	if c.QBFinalizeConcurrency < 0 || c.QBFinalizeConcurrency > 8 {
		return errors.New("qb finalize concurrency must be between 0 and 8 (0 = default 1)")
	}

	return nil
}

// setupCommonFlags adds flags shared between source and destination commands.
// qbURLHelp differs between source (required, no extra context) and destination
// (optional, used for adding verified torrents), so it is passed in.
func setupCommonFlags(flags *pflag.FlagSet, dataHelp, qbURLHelp string) {
	flags.String(flagData, "", dataHelp)
	flags.String(flagQBURL, "", qbURLHelp)
	flags.String(flagQBUsername, "", "qBittorrent username")
	flags.String(flagQBPassword, "", "qBittorrent password")
	flags.String(flagHealthAddr, defaultHealthAddr, "HTTP health endpoint address (empty to disable)")
	flags.String(flagSyncedTag, defaultSyncedTag, "Tag to apply to synced torrents (empty to disable)")
	flags.Bool(flagDryRun, false, "Run without making changes")
	flags.String(flagLogLevel, "info", "Log level: debug, info, warn, error")
}

// SetupSourceFlags sets up flags for the source command.
func SetupSourceFlags(cmd *cobra.Command) {
	flags := cmd.Flags()

	setupCommonFlags(
		flags,
		"Data directory path where torrent content is stored",
		"qBittorrent WebUI URL",
	)
	flags.String("destination-addr", "", "Destination server gRPC address (e.g., 192.168.1.100:50051)")
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
		"Tag to apply on destination torrent when removed from source (empty to disable)",
	)
	flags.String(
		"exclude-cleanup-tag",
		"",
		"Tag that prevents torrents from being cleaned up from source (empty to disable)",
	)
	flags.String(
		"sync-failed-tag",
		defaultSyncFailedTag,
		"Tag applied on source when verification fails repeatedly (empty to disable; remove tag to retry)",
	)
	flags.String(
		"exclude-sync-tag",
		"",
		"Tag that prevents torrents from being synced (empty to disable)",
	)
	flags.Int(
		"sync-failed-guard",
		int(DefaultSyncFailedGuard.Seconds()),
		"How long a torrent must fail continuously (seconds) before it is tagged sync-failed",
	)

	flags.String("arr-skipped-tag", defaultArrSkippedTag,
		"Tag applied to source torrents skipped by the arr filter (empty to disable)")
	addArrFlags(flags)
}

// addArrFlags registers the Sonarr/Radarr flags. Both commands accept them:
// whichever process sits with the instances configures them, and the source
// relays to the destination when it has none.
func addArrFlags(flags *pflag.FlagSet) {
	flags.String("radarr-url", "", "Radarr URL (e.g. http://radarr:7878). Empty disables the Radarr filter.")
	flags.String("radarr-api-key", "", "Radarr API key (sent via X-Api-Key header)")
	flags.StringSlice("radarr-categories", nil,
		"qBittorrent categories routed to Radarr. Filtering only applies to torrents Radarr grabbed "+
			"itself; cross-seed and manually added torrents have no history and are synced.")

	flags.String("sonarr-url", "", "Sonarr URL (e.g. http://sonarr:8989). Empty disables the Sonarr filter.")
	flags.String("sonarr-api-key", "", "Sonarr API key (sent via X-Api-Key header)")
	flags.StringSlice("sonarr-categories", nil,
		"qBittorrent categories routed to Sonarr. Same scope and limitations as --radarr-categories.")
}

// SetupDestinationFlags sets up flags for the destination command.
func SetupDestinationFlags(cmd *cobra.Command) {
	flags := cmd.Flags()

	setupCommonFlags(
		flags,
		"Data directory path where torrent content will be written",
		"qBittorrent WebUI URL (for adding verified torrents)",
	)
	flags.String("listen", defaultListenAddr, "gRPC listen address")
	flags.String("save-path", "", "Save path as destination qBittorrent sees it (defaults to --data)")
	flags.Int("poll-interval", defaultPollIntervalSec, "Poll interval in seconds for torrent verification")
	flags.Int("poll-timeout", defaultPollTimeoutSec, "Poll timeout in seconds for torrent verification")
	flags.Int("stream-workers", 0, "Number of concurrent piece writers (0 = auto: 8, increase for SSD/NVMe)")
	flags.Int(
		"max-stream-buffer",
		defaultMaxStreamBufferMB,
		"Global memory budget in MB for buffered piece data across all streams",
	)
	flags.Int(
		"qb-finalize-concurrency",
		0,
		"Max torrents concurrently in the destination qB add/recheck stage (0 = default 1, max 8). "+
			"Values >1 increase qB API and disk load; on NFS/spinning rust concurrent rechecks compete "+
			"for I/O — raise only on SSD-backed storage",
	)
	flags.Int(
		"verify-concurrency",
		0,
		"Concurrent piece-read goroutines during finalize verification (0 = default 4, max 16). Raise on healthy storage to speed finalize; lower if your NFS server can't handle the burst.",
	)
	addArrFlags(flags)
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

// BindSourceFlags binds source command flags to viper.
func BindSourceFlags(cmd *cobra.Command, v *viper.Viper) error {
	return bindFlags(cmd, v, "QBSYNC_SOURCE", []string{
		flagData, flagQBURL, flagQBUsername, flagQBPassword,
		"destination-addr", "min-space", "min-seeding-time", "sleep",
		"rate-limit", "piece-timeout", "reconnect-max-delay",
		"num-senders", "min-connections", "max-connections",
		"source-removed-tag", "exclude-cleanup-tag", "sync-failed-tag", "exclude-sync-tag",
		"sync-failed-guard",
		"arr-skipped-tag",
		"radarr-url", "radarr-api-key", "radarr-categories",
		"sonarr-url", "sonarr-api-key", "sonarr-categories",
		flagHealthAddr, flagSyncedTag,
		flagDryRun, flagLogLevel, "drain-annotation", "drain-timeout",
	})
}

// BindDestinationFlags binds destination command flags to viper.
func BindDestinationFlags(cmd *cobra.Command, v *viper.Viper) error {
	return bindFlags(cmd, v, "QBSYNC_DESTINATION", []string{
		"listen", flagData, "save-path", flagQBURL, flagQBUsername, flagQBPassword,
		"poll-interval", "poll-timeout", "stream-workers", "max-stream-buffer",
		"qb-finalize-concurrency", "verify-concurrency",
		"radarr-url", "radarr-api-key", "radarr-categories",
		"sonarr-url", "sonarr-api-key", "sonarr-categories",
		flagHealthAddr, flagSyncedTag, flagDryRun, flagLogLevel,
	})
}

// seconds returns a viper int key as a [time.Duration] in seconds.
func seconds(v *viper.Viper, key string) time.Duration {
	return time.Duration(v.GetInt(key)) * time.Second
}

// loadBase loads the base configuration shared by source and destination.
func loadBase(v *viper.Viper) BaseConfig {
	return BaseConfig{
		QBURL:      v.GetString(flagQBURL),
		QBUsername: v.GetString(flagQBUsername),
		QBPassword: v.GetString(flagQBPassword),
		DataPath:   v.GetString(flagData),
		HealthAddr: v.GetString(flagHealthAddr),
		SyncedTag:  v.GetString(flagSyncedTag),
		LogLevel:   v.GetString(flagLogLevel),
		DryRun:     v.GetBool(flagDryRun),
		Radarr: ArrInstanceConfig{
			URL:        v.GetString("radarr-url"),
			APIKey:     v.GetString("radarr-api-key"),
			Categories: v.GetStringSlice("radarr-categories"),
		},
		Sonarr: ArrInstanceConfig{
			URL:        v.GetString("sonarr-url"),
			APIKey:     v.GetString("sonarr-api-key"),
			Categories: v.GetStringSlice("sonarr-categories"),
		},
	}
}

// applyEnvFallback overrides addr with the first set env var only if addr still
// matches its default. This preserves "explicit flag overrides env" semantics.
func applyEnvFallback(addr, defaultAddr string, envVars ...string) string {
	if addr != defaultAddr {
		return addr
	}
	return getEnvWithFallbacks(addr, envVars...)
}

// LoadSource loads the source server configuration from viper.
func LoadSource(v *viper.Viper) (*SourceConfig, error) {
	cfg := &SourceConfig{
		BaseConfig:         loadBase(v),
		DestinationAddr:    v.GetString("destination-addr"),
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
		SyncFailedTag:      v.GetString("sync-failed-tag"),
		SyncFailedGuard:    seconds(v, "sync-failed-guard"),
		ExcludeSyncTag:     v.GetString("exclude-sync-tag"),
		ArrSkippedTag:      v.GetString("arr-skipped-tag"),
	}

	cfg.HealthAddr = applyEnvFallback(cfg.HealthAddr, defaultHealthAddr, "HTTP_PORT", "HEALTH_PORT")

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// LoadDestination loads the destination server configuration from viper.
func LoadDestination(v *viper.Viper) (*DestinationConfig, error) {
	cfg := &DestinationConfig{
		BaseConfig:            loadBase(v),
		ListenAddr:            v.GetString("listen"),
		SavePath:              v.GetString("save-path"),
		PollInterval:          seconds(v, "poll-interval"),
		PollTimeout:           seconds(v, "poll-timeout"),
		StreamWorkers:         v.GetInt("stream-workers"),
		MaxStreamBufferMB:     v.GetInt("max-stream-buffer"),
		QBFinalizeConcurrency: v.GetInt("qb-finalize-concurrency"),
		VerifyConcurrency:     v.GetInt("verify-concurrency"),
	}

	cfg.ListenAddr = applyEnvFallback(cfg.ListenAddr, defaultListenAddr, "GRPC_PORT", "PORT")
	cfg.HealthAddr = applyEnvFallback(cfg.HealthAddr, defaultHealthAddr, "HTTP_PORT", "HEALTH_PORT")

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
