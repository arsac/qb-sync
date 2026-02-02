package config

import (
	"errors"
	"fmt"
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
)

type Config struct {
	SrcPath        string
	DestPath       string
	SrcURL         string
	DestURL        string
	SrcUsername    string
	SrcPassword    string
	DestUsername   string
	DestPassword   string
	MinSpaceGB     int64
	MinSeedingTime time.Duration
	Force          bool
	DryRun         bool
	SleepInterval  time.Duration
}

func (c *Config) Validate() error {
	if c.SrcPath == "" {
		return errors.New("source path is required")
	}
	if c.DestPath == "" {
		return errors.New("destination path is required")
	}
	if c.SrcURL == "" {
		return errors.New("source qBittorrent URL is required")
	}
	if c.DestURL == "" {
		return errors.New("destination qBittorrent URL is required")
	}
	return nil
}

func SetupFlags(cmd *cobra.Command) {
	flags := cmd.Flags()

	flags.String("src", "", "Source path to sync from")
	flags.String("dest", "", "Destination path to sync to")
	flags.String("src-url", "", "Source qBittorrent URL")
	flags.String("dest-url", "", "Destination qBittorrent URL")
	flags.String("src-username", "", "Source qBittorrent username")
	flags.String("src-password", "", "Source qBittorrent password")
	flags.String("dest-username", "", "Destination qBittorrent username")
	flags.String("dest-password", "", "Destination qBittorrent password")
	flags.Int64("min-space", defaultMinSpaceGB, "Minimum free space in GB before moving torrents")
	flags.Int("min-seeding-time", defaultMinSeedingTimeSec, "Minimum seeding time in seconds before moving")
	flags.Bool("force", false, "Force move torrents regardless of space")
	flags.Bool("dry-run", false, "Run without making changes")
	flags.Int("sleep", defaultSleepIntervalSec, "Sleep interval between checks in seconds")
}

func BindFlags(cmd *cobra.Command, v *viper.Viper) error {
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	v.AutomaticEnv()

	flags := []string{
		"src", "dest", "src-url", "dest-url",
		"src-username", "src-password",
		"dest-username", "dest-password",
		"min-space", "min-seeding-time",
		"force", "dry-run", "sleep",
	}

	for _, flag := range flags {
		if err := v.BindPFlag(flag, cmd.Flags().Lookup(flag)); err != nil {
			return fmt.Errorf("binding flag %s: %w", flag, err)
		}
	}

	return nil
}

func Load(v *viper.Viper) (*Config, error) {
	cfg := &Config{
		SrcPath:        v.GetString("src"),
		DestPath:       v.GetString("dest"),
		SrcURL:         v.GetString("src-url"),
		DestURL:        v.GetString("dest-url"),
		SrcUsername:    v.GetString("src-username"),
		SrcPassword:    v.GetString("src-password"),
		DestUsername:   v.GetString("dest-username"),
		DestPassword:   v.GetString("dest-password"),
		MinSpaceGB:     v.GetInt64("min-space"),
		MinSeedingTime: time.Duration(v.GetInt("min-seeding-time")) * time.Second,
		Force:          v.GetBool("force"),
		DryRun:         v.GetBool("dry-run"),
		SleepInterval:  time.Duration(v.GetInt("sleep")) * time.Second,
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}
