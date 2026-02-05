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
	defaultMinSpaceGB        = 50
	defaultMinSeedingTimeSec = 3600
	defaultSleepIntervalSec  = 30
	defaultPollIntervalSec   = 2
	defaultPollTimeoutSec    = 300 // 5 minutes
	defaultListenAddr        = ":50051"
	defaultHealthAddr        = ":8080"
)

// HotConfig contains configuration for the hot (source) server.
type HotConfig struct {
	// qBittorrent (source)
	QBURL      string
	QBUsername string
	QBPassword string

	// Data path where torrent content is stored
	DataPath string

	// Streaming destination
	ColdAddr string // gRPC address of cold server

	// Migration settings
	MinSpaceGB     int64
	MinSeedingTime time.Duration
	Force          bool
	SleepInterval  time.Duration

	// Rate limiting
	MaxBytesPerSec int64

	// Health server
	HealthAddr string // HTTP health endpoint address (e.g., ":8080")

	DryRun bool
}

// Validate validates the hot configuration.
func (c *HotConfig) Validate() error {
	if c.DataPath == "" {
		return errors.New("data path is required")
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
	if c.MaxBytesPerSec < 0 {
		return errors.New("max bytes per second cannot be negative")
	}
	return nil
}

// ColdConfig contains configuration for the cold (destination) server.
type ColdConfig struct {
	// gRPC server
	ListenAddr string

	// Data path where torrent content will be written
	DataPath string

	// qBittorrent (destination) - optional, for auto-adding verified torrents
	QBURL      string
	QBUsername string
	QBPassword string

	// Polling settings for torrent verification
	PollInterval time.Duration
	PollTimeout  time.Duration

	// Health server
	HealthAddr string // HTTP health endpoint address (e.g., ":8080")

	DryRun bool
}

// Validate validates the cold configuration.
func (c *ColdConfig) Validate() error {
	if c.DataPath == "" {
		return errors.New("data path is required")
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
	flags.Bool("force", false, "Force move torrents regardless of space")
	flags.Int("sleep", defaultSleepIntervalSec, "Sleep interval between checks in seconds")
	flags.Int64("rate-limit", 0, "Max bytes/sec for streaming (0 = unlimited)")
	flags.String("health-addr", defaultHealthAddr, "HTTP health endpoint address (empty to disable)")
	flags.Bool("dry-run", false, "Run without making changes")
}

// SetupColdFlags sets up flags for the cold command.
func SetupColdFlags(cmd *cobra.Command) {
	flags := cmd.Flags()

	flags.String("listen", defaultListenAddr, "gRPC listen address")
	flags.String("data", "", "Data directory path where torrent content will be written")
	flags.String("qb-url", "", "qBittorrent WebUI URL (for adding verified torrents)")
	flags.String("qb-username", "", "qBittorrent username")
	flags.String("qb-password", "", "qBittorrent password")
	flags.Int("poll-interval", defaultPollIntervalSec, "Poll interval in seconds for torrent verification")
	flags.Int("poll-timeout", defaultPollTimeoutSec, "Poll timeout in seconds for torrent verification")
	flags.String("health-addr", defaultHealthAddr, "HTTP health endpoint address (empty to disable)")
	flags.Bool("dry-run", false, "Run without making changes")
}

// BindHotFlags binds hot command flags to viper.
func BindHotFlags(cmd *cobra.Command, v *viper.Viper) error {
	v.SetEnvPrefix("QBSYNC_HOT")
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	v.AutomaticEnv()

	flags := []string{
		"data", "qb-url", "qb-username", "qb-password",
		"cold-addr", "min-space", "min-seeding-time",
		"force", "sleep", "rate-limit", "health-addr", "dry-run",
	}

	for _, flag := range flags {
		if err := v.BindPFlag(flag, cmd.Flags().Lookup(flag)); err != nil {
			return fmt.Errorf("binding flag %s: %w", flag, err)
		}
	}

	return nil
}

// BindColdFlags binds cold command flags to viper.
func BindColdFlags(cmd *cobra.Command, v *viper.Viper) error {
	v.SetEnvPrefix("QBSYNC_COLD")
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	v.AutomaticEnv()

	flags := []string{
		"listen", "data", "qb-url", "qb-username", "qb-password",
		"poll-interval", "poll-timeout", "health-addr", "dry-run",
	}

	for _, flag := range flags {
		if err := v.BindPFlag(flag, cmd.Flags().Lookup(flag)); err != nil {
			return fmt.Errorf("binding flag %s: %w", flag, err)
		}
	}

	return nil
}

// LoadHot loads the hot server configuration from viper.
func LoadHot(v *viper.Viper) (*HotConfig, error) {
	cfg := &HotConfig{
		DataPath:       v.GetString("data"),
		QBURL:          v.GetString("qb-url"),
		QBUsername:     v.GetString("qb-username"),
		QBPassword:     v.GetString("qb-password"),
		ColdAddr:       v.GetString("cold-addr"),
		MinSpaceGB:     v.GetInt64("min-space"),
		MinSeedingTime: time.Duration(v.GetInt("min-seeding-time")) * time.Second,
		Force:          v.GetBool("force"),
		SleepInterval:  time.Duration(v.GetInt("sleep")) * time.Second,
		MaxBytesPerSec: v.GetInt64("rate-limit"),
		HealthAddr:     v.GetString("health-addr"),
		DryRun:         v.GetBool("dry-run"),
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
		ListenAddr:   v.GetString("listen"),
		DataPath:     v.GetString("data"),
		QBURL:        v.GetString("qb-url"),
		QBUsername:   v.GetString("qb-username"),
		QBPassword:   v.GetString("qb-password"),
		PollInterval: time.Duration(v.GetInt("poll-interval")) * time.Second,
		PollTimeout:  time.Duration(v.GetInt("poll-timeout")) * time.Second,
		HealthAddr:   v.GetString("health-addr"),
		DryRun:       v.GetBool("dry-run"),
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
