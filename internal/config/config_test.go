package config

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSourceConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     SourceConfig
		wantErr string
	}{
		{
			name: "valid config",
			cfg: SourceConfig{
				BaseConfig:      BaseConfig{DataPath: "/data", QBURL: "http://qb:8080"},
				DestinationAddr: "cold:50051",
			},
			wantErr: "",
		},
		{
			name: "missing data path",
			cfg: SourceConfig{
				BaseConfig:      BaseConfig{QBURL: "http://qb:8080"},
				DestinationAddr: "cold:50051",
			},
			wantErr: "data path is required",
		},
		{
			name: "missing qb URL",
			cfg: SourceConfig{
				BaseConfig:      BaseConfig{DataPath: "/data"},
				DestinationAddr: "cold:50051",
			},
			wantErr: "qBittorrent URL is required",
		},
		{
			name: "missing destination addr",
			cfg: SourceConfig{
				BaseConfig: BaseConfig{DataPath: "/data", QBURL: "http://qb:8080"},
			},
			wantErr: "destination server address is required",
		},
		{
			name: "negative min connections",
			cfg: SourceConfig{
				BaseConfig:         BaseConfig{DataPath: "/data", QBURL: "http://qb:8080"},
				DestinationAddr:    "cold:50051",
				MinGRPCConnections: -1,
			},
			wantErr: "min connections cannot be negative",
		},
		{
			name: "negative max connections",
			cfg: SourceConfig{
				BaseConfig:         BaseConfig{DataPath: "/data", QBURL: "http://qb:8080"},
				DestinationAddr:    "cold:50051",
				MaxGRPCConnections: -1,
			},
			wantErr: "max connections cannot be negative",
		},
		{
			name: "min exceeds max",
			cfg: SourceConfig{
				BaseConfig:         BaseConfig{DataPath: "/data", QBURL: "http://qb:8080"},
				DestinationAddr:    "cold:50051",
				MinGRPCConnections: 5,
				MaxGRPCConnections: 2,
			},
			wantErr: "min connections cannot exceed max connections",
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

func TestDestinationConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     DestinationConfig
		wantErr string
	}{
		{
			name: "valid config",
			cfg: DestinationConfig{
				BaseConfig: BaseConfig{DataPath: "/data"},
				ListenAddr: ":50051",
			},
			wantErr: "",
		},
		{
			name: "missing data path",
			cfg: DestinationConfig{
				ListenAddr: ":50051",
			},
			wantErr: "data path is required",
		},
		{
			name: "missing listen addr",
			cfg: DestinationConfig{
				BaseConfig: BaseConfig{DataPath: "/data"},
			},
			wantErr: "listen address is required",
		},
		{
			name: "negative max stream buffer",
			cfg: DestinationConfig{
				BaseConfig:        BaseConfig{DataPath: "/data"},
				ListenAddr:        ":50051",
				MaxStreamBufferMB: -1,
			},
			wantErr: "max stream buffer cannot be negative",
		},
		{
			name: "zero max stream buffer uses default",
			cfg: DestinationConfig{
				BaseConfig: BaseConfig{DataPath: "/data"},
				ListenAddr: ":50051",
			},
			wantErr: "",
		},
		{
			name: "positive max stream buffer",
			cfg: DestinationConfig{
				BaseConfig:        BaseConfig{DataPath: "/data"},
				ListenAddr:        ":50051",
				MaxStreamBufferMB: 256,
			},
			wantErr: "",
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

func TestLoadSource(t *testing.T) {
	t.Parallel()

	t.Run("loads config from viper", func(t *testing.T) {
		t.Parallel()
		v := viper.New()
		v.Set("data", "/data/path")
		v.Set("qb-url", "http://qb:8080")
		v.Set("qb-username", "admin")
		v.Set("qb-password", "secret")
		v.Set("destination-addr", "cold:50051")
		v.Set("min-space", 100)
		v.Set("min-seeding-time", 7200)
		v.Set("dry-run", true)
		v.Set("sleep", 60)
		v.Set("rate-limit", int64(1000000))
		v.Set("synced-tag", "my-synced")
		v.Set("min-connections", 3)
		v.Set("max-connections", 6)
		v.Set("num-senders", 8)
		v.Set("source-removed-tag", "custom-removed")
		v.Set("drain-annotation", "my/drain")
		v.Set("drain-timeout", 480)

		cfg, err := LoadSource(v)
		require.NoError(t, err)

		assert.Equal(t, "/data/path", cfg.DataPath)
		assert.Equal(t, "http://qb:8080", cfg.QBURL)
		assert.Equal(t, "admin", cfg.QBUsername)
		assert.Equal(t, "secret", cfg.QBPassword)
		assert.Equal(t, "cold:50051", cfg.DestinationAddr)
		assert.Equal(t, int64(100), cfg.MinSpaceGB)
		assert.Equal(t, 7200, int(cfg.MinSeedingTime.Seconds()))
		assert.True(t, cfg.DryRun)
		assert.Equal(t, 60, int(cfg.SleepInterval.Seconds()))
		assert.Equal(t, int64(1000000), cfg.MaxBytesPerSec)
		assert.Equal(t, "my-synced", cfg.SyncedTag)
		assert.Equal(t, 3, cfg.MinGRPCConnections)
		assert.Equal(t, 6, cfg.MaxGRPCConnections)
		assert.Equal(t, 8, cfg.NumSenders)
		assert.Equal(t, "custom-removed", cfg.SourceRemovedTag)
		assert.Equal(t, "my/drain", cfg.DrainAnnotation)
		assert.Equal(t, 480, int(cfg.DrainTimeout.Seconds()))
	})

	t.Run("source-removed-tag is empty when not set in viper", func(t *testing.T) {
		t.Parallel()
		v := viper.New()
		v.Set("data", "/data/path")
		v.Set("qb-url", "http://qb:8080")
		v.Set("destination-addr", "cold:50051")
		// Default "source-removed" is applied at flag level, not viper level.
		// LoadSource without the flag binding sees empty string.

		cfg, err := LoadSource(v)
		require.NoError(t, err)
		assert.Empty(t, cfg.SourceRemovedTag)
	})

	t.Run("source-removed-tag can be empty to disable", func(t *testing.T) {
		t.Parallel()
		v := viper.New()
		v.Set("data", "/data/path")
		v.Set("qb-url", "http://qb:8080")
		v.Set("destination-addr", "cold:50051")
		v.Set("source-removed-tag", "")

		cfg, err := LoadSource(v)
		require.NoError(t, err)
		assert.Empty(t, cfg.SourceRemovedTag)
	})

	t.Run("synced-tag can be empty to disable tagging", func(t *testing.T) {
		t.Parallel()
		v := viper.New()
		v.Set("data", "/data/path")
		v.Set("qb-url", "http://qb:8080")
		v.Set("destination-addr", "cold:50051")
		v.Set("synced-tag", "")

		cfg, err := LoadSource(v)
		require.NoError(t, err)
		assert.Empty(t, cfg.SyncedTag)
	})
}

func TestLoadDestination(t *testing.T) {
	t.Parallel()

	t.Run("loads config from viper", func(t *testing.T) {
		t.Parallel()
		v := viper.New()
		v.Set("listen", ":50051")
		v.Set("data", "/data/path")
		v.Set("save-path", "/downloads")
		v.Set("qb-url", "http://qb:8080")
		v.Set("qb-username", "admin")
		v.Set("qb-password", "secret")
		v.Set("poll-interval", 5)
		v.Set("poll-timeout", 600)
		v.Set("synced-tag", "cold-synced")
		v.Set("max-stream-buffer", 256)
		v.Set("dry-run", true)

		cfg, err := LoadDestination(v)
		require.NoError(t, err)

		assert.Equal(t, ":50051", cfg.ListenAddr)
		assert.Equal(t, "/data/path", cfg.DataPath)
		assert.Equal(t, "/downloads", cfg.SavePath)
		assert.Equal(t, "http://qb:8080", cfg.QBURL)
		assert.Equal(t, "admin", cfg.QBUsername)
		assert.Equal(t, "secret", cfg.QBPassword)
		assert.Equal(t, 5, int(cfg.PollInterval.Seconds()))
		assert.Equal(t, 600, int(cfg.PollTimeout.Seconds()))
		assert.Equal(t, 256, cfg.MaxStreamBufferMB)
		assert.Equal(t, "cold-synced", cfg.SyncedTag)
		assert.True(t, cfg.DryRun)
	})

	t.Run("save-path defaults to empty", func(t *testing.T) {
		t.Parallel()
		v := viper.New()
		v.Set("listen", ":50051")
		v.Set("data", "/data/path")

		cfg, err := LoadDestination(v)
		require.NoError(t, err)
		assert.Empty(t, cfg.SavePath, "SavePath should be empty when not explicitly set")
	})

	t.Run("synced-tag can be empty to disable tagging", func(t *testing.T) {
		t.Parallel()
		v := viper.New()
		v.Set("listen", ":50051")
		v.Set("data", "/data/path")
		v.Set("synced-tag", "")

		cfg, err := LoadDestination(v)
		require.NoError(t, err)
		assert.Empty(t, cfg.SyncedTag)
	})
}

func TestSetupSourceFlags(t *testing.T) {
	t.Parallel()

	cmd := &cobra.Command{Use: "test"}
	SetupSourceFlags(cmd)

	flags := []string{
		"data", "qb-url", "qb-username", "qb-password",
		"destination-addr", "min-space", "min-seeding-time",
		"dry-run", "sleep", "rate-limit", "synced-tag",
		"min-connections", "max-connections", "num-senders", "source-removed-tag",
		"drain-annotation", "drain-timeout",
	}

	for _, flag := range flags {
		assert.NotNil(t, cmd.Flags().Lookup(flag), "flag %s should exist", flag)
	}

	// Verify synced-tag default value
	syncedTagFlag := cmd.Flags().Lookup("synced-tag")
	assert.Equal(t, "synced", syncedTagFlag.DefValue)

	// Verify min-connections default value
	minConnsFlag := cmd.Flags().Lookup("min-connections")
	assert.Equal(t, "2", minConnsFlag.DefValue)

	// Verify max-connections default value
	maxConnsFlag := cmd.Flags().Lookup("max-connections")
	assert.Equal(t, "8", maxConnsFlag.DefValue)

	// Verify num-senders default value
	numSendersFlag := cmd.Flags().Lookup("num-senders")
	assert.Equal(t, "4", numSendersFlag.DefValue)

	// Verify source-removed-tag default value
	sourceRemovedTagFlag := cmd.Flags().Lookup("source-removed-tag")
	assert.Equal(t, "source-removed", sourceRemovedTagFlag.DefValue)
}

func TestSetupDestinationFlags(t *testing.T) {
	t.Parallel()

	cmd := &cobra.Command{Use: "test"}
	SetupDestinationFlags(cmd)

	flags := []string{
		"listen", "data", "save-path", "qb-url", "qb-username", "qb-password",
		"poll-interval", "poll-timeout", "stream-workers", "max-stream-buffer",
		"dry-run", "synced-tag",
	}

	for _, flag := range flags {
		assert.NotNil(t, cmd.Flags().Lookup(flag), "flag %s should exist", flag)
	}

	// Verify synced-tag default value
	syncedTagFlag := cmd.Flags().Lookup("synced-tag")
	assert.Equal(t, "synced", syncedTagFlag.DefValue)

	// Verify max-stream-buffer default value
	bufferFlag := cmd.Flags().Lookup("max-stream-buffer")
	assert.Equal(t, "512", bufferFlag.DefValue)
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

func TestLoadDestination_EnvFallbacks(t *testing.T) {
	t.Run("uses GRPC_PORT for listen address", func(t *testing.T) {
		t.Setenv("GRPC_PORT", "50052")

		v := viper.New()
		v.Set("listen", defaultListenAddr) // default value
		v.Set("data", "/data")

		cfg, err := LoadDestination(v)
		require.NoError(t, err)
		assert.Equal(t, ":50052", cfg.ListenAddr)
	})

	t.Run("uses HTTP_PORT for health address", func(t *testing.T) {
		t.Setenv("HTTP_PORT", "9090")

		v := viper.New()
		v.Set("listen", ":50051")
		v.Set("data", "/data")
		v.Set("health-addr", defaultHealthAddr) // default value

		cfg, err := LoadDestination(v)
		require.NoError(t, err)
		assert.Equal(t, ":9090", cfg.HealthAddr)
	})

	t.Run("explicit flag overrides env var", func(t *testing.T) {
		t.Setenv("GRPC_PORT", "50052")

		v := viper.New()
		v.Set("listen", ":60000") // explicit non-default value
		v.Set("data", "/data")

		cfg, err := LoadDestination(v)
		require.NoError(t, err)
		assert.Equal(t, ":60000", cfg.ListenAddr) // flag wins over env
	})
}

func TestLoadSource_EnvFallbacks(t *testing.T) {
	t.Run("uses HTTP_PORT for health address", func(t *testing.T) {
		t.Setenv("HTTP_PORT", "9090")

		v := viper.New()
		v.Set("data", "/data")
		v.Set("qb-url", "http://qb:8080")
		v.Set("destination-addr", "cold:50051")
		v.Set("health-addr", defaultHealthAddr) // default value

		cfg, err := LoadSource(v)
		require.NoError(t, err)
		assert.Equal(t, ":9090", cfg.HealthAddr)
	})
}
