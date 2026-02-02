package config

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{
			name: "valid config",
			cfg: Config{
				SrcPath:  "/src",
				DestPath: "/dest",
				SrcURL:   "http://src:8080",
				DestURL:  "http://dest:8080",
			},
			wantErr: "",
		},
		{
			name: "missing src path",
			cfg: Config{
				DestPath: "/dest",
				SrcURL:   "http://src:8080",
				DestURL:  "http://dest:8080",
			},
			wantErr: "source path is required",
		},
		{
			name: "missing dest path",
			cfg: Config{
				SrcPath: "/src",
				SrcURL:  "http://src:8080",
				DestURL: "http://dest:8080",
			},
			wantErr: "destination path is required",
		},
		{
			name: "missing src URL",
			cfg: Config{
				SrcPath:  "/src",
				DestPath: "/dest",
				DestURL:  "http://dest:8080",
			},
			wantErr: "source qBittorrent URL is required",
		},
		{
			name: "missing dest URL",
			cfg: Config{
				SrcPath:  "/src",
				DestPath: "/dest",
				SrcURL:   "http://src:8080",
			},
			wantErr: "destination qBittorrent URL is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.cfg.Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tt.wantErr)
			}
		})
	}
}

func TestLoad(t *testing.T) {
	t.Parallel()

	t.Run("loads config from viper", func(t *testing.T) {
		t.Parallel()
		v := viper.New()
		v.Set("src", "/src/path")
		v.Set("dest", "/dest/path")
		v.Set("src-url", "http://src:8080")
		v.Set("dest-url", "http://dest:8080")
		v.Set("src-username", "admin")
		v.Set("src-password", "secret")
		v.Set("min-space", 100)
		v.Set("min-seeding-time", 7200)
		v.Set("force", true)
		v.Set("dry-run", true)
		v.Set("sleep", 60)

		cfg, err := Load(v)
		require.NoError(t, err)

		assert.Equal(t, "/src/path", cfg.SrcPath)
		assert.Equal(t, "/dest/path", cfg.DestPath)
		assert.Equal(t, "http://src:8080", cfg.SrcURL)
		assert.Equal(t, "http://dest:8080", cfg.DestURL)
		assert.Equal(t, "admin", cfg.SrcUsername)
		assert.Equal(t, "secret", cfg.SrcPassword)
		assert.Equal(t, int64(100), cfg.MinSpaceGB)
		assert.Equal(t, 7200, int(cfg.MinSeedingTime.Seconds()))
		assert.True(t, cfg.Force)
		assert.True(t, cfg.DryRun)
		assert.Equal(t, 60, int(cfg.SleepInterval.Seconds()))
	})
}

func TestSetupFlags(t *testing.T) {
	t.Parallel()

	cmd := &cobra.Command{Use: "test"}
	SetupFlags(cmd)

	flags := []string{
		"src", "dest", "src-url", "dest-url",
		"src-username", "src-password",
		"dest-username", "dest-password",
		"min-space", "min-seeding-time",
		"force", "dry-run", "sleep",
	}

	for _, flag := range flags {
		assert.NotNil(t, cmd.Flags().Lookup(flag), "flag %s should exist", flag)
	}
}
