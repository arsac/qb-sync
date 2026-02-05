package config

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHotConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     HotConfig
		wantErr string
	}{
		{
			name: "valid config",
			cfg: HotConfig{
				DataPath: "/data",
				QBURL:    "http://qb:8080",
				ColdAddr: "cold:50051",
			},
			wantErr: "",
		},
		{
			name: "missing data path",
			cfg: HotConfig{
				QBURL:    "http://qb:8080",
				ColdAddr: "cold:50051",
			},
			wantErr: "data path is required",
		},
		{
			name: "missing qb URL",
			cfg: HotConfig{
				DataPath: "/data",
				ColdAddr: "cold:50051",
			},
			wantErr: "qBittorrent URL is required",
		},
		{
			name: "missing cold addr",
			cfg: HotConfig{
				DataPath: "/data",
				QBURL:    "http://qb:8080",
			},
			wantErr: "cold server address is required",
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

func TestColdConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     ColdConfig
		wantErr string
	}{
		{
			name: "valid config",
			cfg: ColdConfig{
				DataPath:   "/data",
				ListenAddr: ":50051",
			},
			wantErr: "",
		},
		{
			name: "missing data path",
			cfg: ColdConfig{
				ListenAddr: ":50051",
			},
			wantErr: "data path is required",
		},
		{
			name: "missing listen addr",
			cfg: ColdConfig{
				DataPath: "/data",
			},
			wantErr: "listen address is required",
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

func TestLoadHot(t *testing.T) {
	t.Parallel()

	t.Run("loads config from viper", func(t *testing.T) {
		t.Parallel()
		v := viper.New()
		v.Set("data", "/data/path")
		v.Set("qb-url", "http://qb:8080")
		v.Set("qb-username", "admin")
		v.Set("qb-password", "secret")
		v.Set("cold-addr", "cold:50051")
		v.Set("min-space", 100)
		v.Set("min-seeding-time", 7200)
		v.Set("force", true)
		v.Set("dry-run", true)
		v.Set("sleep", 60)
		v.Set("rate-limit", int64(1000000))

		cfg, err := LoadHot(v)
		require.NoError(t, err)

		assert.Equal(t, "/data/path", cfg.DataPath)
		assert.Equal(t, "http://qb:8080", cfg.QBURL)
		assert.Equal(t, "admin", cfg.QBUsername)
		assert.Equal(t, "secret", cfg.QBPassword)
		assert.Equal(t, "cold:50051", cfg.ColdAddr)
		assert.Equal(t, int64(100), cfg.MinSpaceGB)
		assert.Equal(t, 7200, int(cfg.MinSeedingTime.Seconds()))
		assert.True(t, cfg.Force)
		assert.True(t, cfg.DryRun)
		assert.Equal(t, 60, int(cfg.SleepInterval.Seconds()))
		assert.Equal(t, int64(1000000), cfg.MaxBytesPerSec)
	})
}

func TestLoadCold(t *testing.T) {
	t.Parallel()

	t.Run("loads config from viper", func(t *testing.T) {
		t.Parallel()
		v := viper.New()
		v.Set("listen", ":50051")
		v.Set("data", "/data/path")
		v.Set("qb-url", "http://qb:8080")
		v.Set("qb-username", "admin")
		v.Set("qb-password", "secret")
		v.Set("poll-interval", 5)
		v.Set("poll-timeout", 600)
		v.Set("dry-run", true)

		cfg, err := LoadCold(v)
		require.NoError(t, err)

		assert.Equal(t, ":50051", cfg.ListenAddr)
		assert.Equal(t, "/data/path", cfg.DataPath)
		assert.Equal(t, "http://qb:8080", cfg.QBURL)
		assert.Equal(t, "admin", cfg.QBUsername)
		assert.Equal(t, "secret", cfg.QBPassword)
		assert.Equal(t, 5, int(cfg.PollInterval.Seconds()))
		assert.Equal(t, 600, int(cfg.PollTimeout.Seconds()))
		assert.True(t, cfg.DryRun)
	})
}

func TestSetupHotFlags(t *testing.T) {
	t.Parallel()

	cmd := &cobra.Command{Use: "test"}
	SetupHotFlags(cmd)

	flags := []string{
		"data", "qb-url", "qb-username", "qb-password",
		"cold-addr", "min-space", "min-seeding-time",
		"force", "dry-run", "sleep", "rate-limit",
	}

	for _, flag := range flags {
		assert.NotNil(t, cmd.Flags().Lookup(flag), "flag %s should exist", flag)
	}
}

func TestSetupColdFlags(t *testing.T) {
	t.Parallel()

	cmd := &cobra.Command{Use: "test"}
	SetupColdFlags(cmd)

	flags := []string{
		"listen", "data", "qb-url", "qb-username", "qb-password",
		"poll-interval", "poll-timeout", "dry-run",
	}

	for _, flag := range flags {
		assert.NotNil(t, cmd.Flags().Lookup(flag), "flag %s should exist", flag)
	}
}

func TestGetEnvWithFallbacks(t *testing.T) {
	tests := []struct {
		name       string
		defaultVal string
		envVars    []string
		envValues  map[string]string
		want       string
	}{
		{
			name:       "returns default when no env vars set",
			defaultVal: ":8080",
			envVars:    []string{"TEST_PORT_1", "TEST_PORT_2"},
			envValues:  map[string]string{},
			want:       ":8080",
		},
		{
			name:       "returns first env var when set",
			defaultVal: ":8080",
			envVars:    []string{"TEST_PORT_1", "TEST_PORT_2"},
			envValues:  map[string]string{"TEST_PORT_1": ":9090"},
			want:       ":9090",
		},
		{
			name:       "returns second env var when first not set",
			defaultVal: ":8080",
			envVars:    []string{"TEST_PORT_1", "TEST_PORT_2"},
			envValues:  map[string]string{"TEST_PORT_2": ":9091"},
			want:       ":9091",
		},
		{
			name:       "prepends colon for port-only value",
			defaultVal: ":8080",
			envVars:    []string{"TEST_PORT"},
			envValues:  map[string]string{"TEST_PORT": "3000"},
			want:       ":3000",
		},
		{
			name:       "does not prepend colon when address has colon",
			defaultVal: ":8080",
			envVars:    []string{"TEST_PORT"},
			envValues:  map[string]string{"TEST_PORT": "0.0.0.0:3000"},
			want:       "0.0.0.0:3000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set env vars
			for k, v := range tt.envValues {
				t.Setenv(k, v)
			}

			got := getEnvWithFallbacks(tt.defaultVal, tt.envVars...)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLoadCold_EnvFallbacks(t *testing.T) {
	t.Run("uses GRPC_PORT for listen address", func(t *testing.T) {
		t.Setenv("GRPC_PORT", "50052")

		v := viper.New()
		v.Set("listen", defaultListenAddr) // default value
		v.Set("data", "/data")

		cfg, err := LoadCold(v)
		require.NoError(t, err)
		assert.Equal(t, ":50052", cfg.ListenAddr)
	})

	t.Run("uses HTTP_PORT for health address", func(t *testing.T) {
		t.Setenv("HTTP_PORT", "9090")

		v := viper.New()
		v.Set("listen", ":50051")
		v.Set("data", "/data")
		v.Set("health-addr", defaultHealthAddr) // default value

		cfg, err := LoadCold(v)
		require.NoError(t, err)
		assert.Equal(t, ":9090", cfg.HealthAddr)
	})

	t.Run("explicit flag overrides env var", func(t *testing.T) {
		t.Setenv("GRPC_PORT", "50052")

		v := viper.New()
		v.Set("listen", ":60000") // explicit non-default value
		v.Set("data", "/data")

		cfg, err := LoadCold(v)
		require.NoError(t, err)
		assert.Equal(t, ":60000", cfg.ListenAddr) // flag wins over env
	})
}

func TestLoadHot_EnvFallbacks(t *testing.T) {
	t.Run("uses HTTP_PORT for health address", func(t *testing.T) {
		t.Setenv("HTTP_PORT", "9090")

		v := viper.New()
		v.Set("data", "/data")
		v.Set("qb-url", "http://qb:8080")
		v.Set("cold-addr", "cold:50051")
		v.Set("health-addr", defaultHealthAddr) // default value

		cfg, err := LoadHot(v)
		require.NoError(t, err)
		assert.Equal(t, ":9090", cfg.HealthAddr)
	})
}
