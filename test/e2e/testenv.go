//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/autobrr/go-qbittorrent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/compose"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/arsac/qb-sync/internal/config"
	"github.com/arsac/qb-sync/internal/destination"
	"github.com/arsac/qb-sync/internal/source"
	"github.com/arsac/qb-sync/internal/streaming"
)

const (
	// No auth needed - home-operations image has auth disabled for local subnets.
	testUsername = ""
	testPassword = ""

	// Test torrent - Wired CD (small, legal, multi-file test torrent: 53 MB, 18 files).
	testTorrentURL = "https://webtorrent.io/torrents/wired-cd.torrent"
	wiredCDHash    = "a88fda5954e89178c372716a6a78b8180ed4dad3"

	// Sintel torrent - larger, used only by perf tests.
	sintelTorrentURL = "https://webtorrent.io/torrents/sintel.torrent"
	sintelHash       = "08ada5a7a6183aae1e09d831df6748d566095a10"

	// Shared test constants used by both testenv.go and test files.
	torrentAppearTimeout = 30 * time.Second
	progressTolerance    = 0.001 // For float comparisons
)

// TestEnv holds the complete test environment for source/destination e2e tests.
type TestEnv struct {
	compose          compose.ComposeStack
	sourceURL        string
	destinationURL   string
	sourcePath       string
	destinationPath  string
	sourceClient     *qbittorrent.Client
	destinationClient *qbittorrent.Client
	destinationServer *destination.Server
	grpcAddr          string
	t                 *testing.T
	logger            *slog.Logger

	// Cancellation for destination server
	destinationCancel    context.CancelFunc
	destinationDone      chan error
	destinationServerCtx context.Context

	// Destination server config for restart
	destinationServerConfig destination.ServerConfig
}

// SetupOption configures the test environment.
type SetupOption func(*setupConfig)

type setupConfig struct {
	sourceTempPath bool
}

// WithSourceTempPath enables temp_path on the source qBittorrent instance.
// This mounts the same host directory at an additional container path (/incomplete)
// and configures qBittorrent to use it as the temp/incomplete download directory.
func WithSourceTempPath() SetupOption {
	return func(cfg *setupConfig) {
		cfg.sourceTempPath = true
	}
}

// SetupTestEnv creates and starts the test environment using docker-compose.
func SetupTestEnv(t *testing.T, opts ...SetupOption) *TestEnv {
	t.Helper()
	ctx := context.Background()

	var sc setupConfig
	for _, opt := range opts {
		opt(&sc)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create temp directories for test data.
	// Source directory names match the container mount points (/downloads, /incomplete)
	// so the orchestrator's relative-path computation (Rel(save_path, temp_path)
	// applied to dataPath) resolves to the correct host directory.
	tmpDir := t.TempDir()
	sourcePath := filepath.Join(tmpDir, "downloads")
	sourceIncompletePath := filepath.Join(tmpDir, "incomplete")
	destinationPath := filepath.Join(tmpDir, "destination")
	sourceConfig := filepath.Join(tmpDir, "qb-source")
	destinationConfig := filepath.Join(tmpDir, "qb-destination")

	require.NoError(t, os.MkdirAll(sourcePath, 0o755))
	require.NoError(t, os.MkdirAll(sourceIncompletePath, 0o755))
	require.NoError(t, os.MkdirAll(destinationPath, 0o755))
	require.NoError(t, os.MkdirAll(sourceConfig, 0o755))
	require.NoError(t, os.MkdirAll(destinationConfig, 0o755))

	sourceExtraVolumes := ""
	if sc.sourceTempPath {
		// Mount a separate host dir at /incomplete inside the container.
		// The orchestrator's resolveQBDir computes a local tempDataPath from
		// the relative offset between save_path and temp_path, so host paths
		// must mirror the container layout (separate directories).
		sourceExtraVolumes = fmt.Sprintf("\n      - %s:/incomplete", sourceIncompletePath)
	}

	// Let Docker assign host ports dynamically to avoid TOCTOU port conflicts.
	// Ports stay stable across container stop/start (same container instance).
	composeContent := fmt.Sprintf(`
services:
  qb-source:
    image: ghcr.io/home-operations/qbittorrent:5.1.4
    volumes:
      - %s:/downloads
      - %s:/config%s
    ports:
      - "8080"
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://localhost:8080/api/v2/app/version"]
      interval: 2s
      timeout: 5s
      retries: 30
      start_period: 10s

  qb-destination:
    image: ghcr.io/home-operations/qbittorrent:5.1.4
    volumes:
      - %s:/destination-data
      - %s:/config
    ports:
      - "8080"
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://localhost:8080/api/v2/app/version"]
      interval: 2s
      timeout: 5s
      retries: 30
      start_period: 10s
`, sourcePath, sourceConfig, sourceExtraVolumes, destinationPath, destinationConfig)

	// Create compose stack — use test name for a deterministic, unique identifier
	// that won't collide when tests run in parallel.
	identifier := sanitizeComposeIdentifier(t.Name())
	stack, err := compose.NewDockerComposeWith(
		compose.StackIdentifier(identifier),
		compose.WithStackReaders(strings.NewReader(composeContent)),
	)
	require.NoError(t, err)

	// Start the stack and wait for services to be ready
	err = stack.
		WaitForService("qb-source", wait.ForHTTP("/api/v2/app/version").
			WithPort("8080/tcp").
			WithStartupTimeout(120*time.Second)).
		WaitForService("qb-destination", wait.ForHTTP("/api/v2/app/version").
			WithPort("8080/tcp").
			WithStartupTimeout(120*time.Second)).
		Up(ctx, compose.Wait(true))
	require.NoError(t, err)

	// Query the actual mapped ports from the running containers.
	sourceURL := serviceURL(t, ctx, stack, "qb-source")
	destinationURL := serviceURL(t, ctx, stack, "qb-destination")

	t.Logf("Source qBittorrent: %s", sourceURL)
	t.Logf("Destination qBittorrent: %s", destinationURL)

	// Create qBittorrent clients
	sourceClient := qbittorrent.NewClient(qbittorrent.Config{
		Host:     sourceURL,
		Username: testUsername,
		Password: testPassword,
	})

	destinationClient := qbittorrent.NewClient(qbittorrent.Config{
		Host:     destinationURL,
		Username: testUsername,
		Password: testPassword,
	})

	// Wait for clients to be ready and login
	require.Eventually(t, func() bool {
		return sourceClient.LoginCtx(ctx) == nil
	}, 60*time.Second, 2*time.Second, "source qBittorrent should be ready")

	require.Eventually(t, func() bool {
		return destinationClient.LoginCtx(ctx) == nil
	}, 60*time.Second, 2*time.Second, "destination qBittorrent should be ready")

	// Configure qBittorrent save paths to match volume mounts.
	// The home-operations image defaults to /config/Downloads, but we mount data at /downloads.
	sourcePrefs := map[string]any{
		"save_path": "/downloads",
	}
	if sc.sourceTempPath {
		sourcePrefs["temp_path_enabled"] = true
		sourcePrefs["temp_path"] = "/incomplete"
	}
	require.NoError(t, sourceClient.SetPreferencesCtx(ctx, sourcePrefs))
	require.NoError(t, destinationClient.SetPreferencesCtx(ctx, map[string]any{
		"save_path": "/destination-data",
	}))

	// Find a free port for gRPC server
	grpcAddr := findFreePort(t)

	// Start destination gRPC server
	destinationServerConfig := destination.ServerConfig{
		ListenAddr: grpcAddr,
		BasePath:   destinationPath,
		SavePath:   "/destination-data",
		SyncedTag:  "synced",
		QB: &destination.QBConfig{
			URL:      destinationURL,
			Username: testUsername,
			Password: testPassword,
		},
	}
	destinationServer := destination.NewServer(destinationServerConfig, logger)

	// Start destination server in background
	destinationServerCtx, destinationServerCancel := context.WithCancel(context.Background())
	destinationServerDone := make(chan error, 1)
	go func() {
		destinationServerDone <- destinationServer.Run(destinationServerCtx)
	}()

	// Wait for gRPC server to be ready
	require.Eventually(t, func() bool {
		conn, dialErr := net.Dial("tcp", grpcAddr)
		if dialErr != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, 10*time.Second, 100*time.Millisecond, "gRPC server should be ready")

	env := &TestEnv{
		compose:                 stack,
		sourceURL:               sourceURL,
		destinationURL:          destinationURL,
		sourcePath:              sourcePath,
		destinationPath:         destinationPath,
		sourceClient:            sourceClient,
		destinationClient:       destinationClient,
		destinationServer:       destinationServer,
		grpcAddr:                grpcAddr,
		t:                       t,
		logger:                  logger,
		destinationCancel:       destinationServerCancel,
		destinationDone:         destinationServerDone,
		destinationServerCtx:    destinationServerCtx,
		destinationServerConfig: destinationServerConfig,
	}

	t.Cleanup(env.Cleanup)

	return env
}

// Cleanup tears down the test environment.
func (env *TestEnv) Cleanup() {
	env.destinationCancel()
	<-env.destinationDone
	_ = env.compose.Down(context.Background(),
		compose.RemoveOrphans(true),
		compose.RemoveVolumes(true),
	)
}

// sanitizeComposeIdentifier converts a Go test name into a valid Docker Compose
// project name (lowercase alphanumeric + hyphens, max 63 chars).
func sanitizeComposeIdentifier(name string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(name) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteRune('-')
		}
	}
	s := b.String()
	if len(s) > 63 {
		s = s[:63]
	}
	return s
}

// findFreePort finds an available TCP port and returns the address string.
func findFreePort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())
	return addr
}

// serviceURL returns the HTTP URL for a compose service by querying its mapped port.
func serviceURL(t *testing.T, ctx context.Context, stack compose.ComposeStack, service string) string {
	t.Helper()
	container, err := stack.ServiceContainer(ctx, service)
	require.NoError(t, err, "getting %s container", service)
	mappedPort, err := container.MappedPort(ctx, "8080/tcp")
	require.NoError(t, err, "getting mapped port for %s", service)
	return fmt.Sprintf("http://localhost:%s", mappedPort.Port())
}

// SourceURL returns the source qBittorrent URL.
func (env *TestEnv) SourceURL() string {
	return env.sourceURL
}

// DestinationURL returns the destination qBittorrent URL.
func (env *TestEnv) DestinationURL() string {
	return env.destinationURL
}

// SourcePath returns the source data path.
func (env *TestEnv) SourcePath() string {
	return env.sourcePath
}

// DestinationPath returns the destination data path.
func (env *TestEnv) DestinationPath() string {
	return env.destinationPath
}

// SourceClient returns the source qBittorrent client.
func (env *TestEnv) SourceClient() *qbittorrent.Client {
	return env.sourceClient
}

// DestinationClient returns the destination qBittorrent client.
func (env *TestEnv) DestinationClient() *qbittorrent.Client {
	return env.destinationClient
}

// GRPCAddr returns the destination gRPC server address.
func (env *TestEnv) GRPCAddr() string {
	return env.grpcAddr
}

// Logger returns the test logger.
func (env *TestEnv) Logger() *slog.Logger {
	return env.logger
}

// CreateTestFile creates a test file with deterministic content.
func (env *TestEnv) CreateTestFile(relativePath string, size int) string {
	env.t.Helper()
	fullPath := filepath.Join(env.sourcePath, relativePath)
	dir := filepath.Dir(fullPath)
	require.NoError(env.t, os.MkdirAll(dir, 0o755))

	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}
	require.NoError(env.t, os.WriteFile(fullPath, data, 0o644))
	return fullPath
}

// AddTorrentToSource adds a test torrent to source qBittorrent from URL.
func (env *TestEnv) AddTorrentToSource(ctx context.Context, url string, opts map[string]string) error {
	return env.addTorrent(ctx, env.sourceClient, url, opts)
}

// AddTorrentToDestination adds a test torrent to destination qBittorrent from URL.
func (env *TestEnv) AddTorrentToDestination(ctx context.Context, url string, opts map[string]string) error {
	if opts == nil {
		opts = make(map[string]string)
	}
	if _, ok := opts["savepath"]; !ok {
		opts["savepath"] = "/destination-data"
	}
	return env.destinationClient.AddTorrentFromUrlCtx(ctx, url, opts)
}

func (env *TestEnv) addTorrent(
	ctx context.Context,
	client *qbittorrent.Client,
	url string,
	opts map[string]string,
) error {
	if opts == nil {
		opts = make(map[string]string)
	}
	if _, ok := opts["savepath"]; !ok {
		opts["savepath"] = "/downloads"
	}
	return client.AddTorrentFromUrlCtx(ctx, url, opts)
}

// WaitForTorrent waits for a torrent to appear in qBittorrent.
func (env *TestEnv) WaitForTorrent(
	client *qbittorrent.Client,
	hash string,
	timeout time.Duration,
) *qbittorrent.Torrent {
	env.t.Helper()
	ctx := context.Background()

	var torrent *qbittorrent.Torrent
	require.Eventually(env.t, func() bool {
		torrents, err := client.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
			Hashes: []string{hash},
		})
		if err != nil || len(torrents) == 0 {
			return false
		}
		torrent = &torrents[0]
		return true
	}, timeout, time.Second, "torrent should appear")

	return torrent
}

// CleanupTorrent removes a torrent if it exists and waits for deletion to complete.
func (env *TestEnv) CleanupTorrent(ctx context.Context, client *qbittorrent.Client, hash string) {
	env.t.Helper()
	_ = client.DeleteTorrentsCtx(ctx, []string{hash}, true)
	// Wait for deletion to complete (best effort, don't fail on timeout)
	env.waitForTorrentAbsent(ctx, client, hash, 5*time.Second)
}

// waitForTorrentAbsent waits for a torrent to be removed from qBittorrent.
func (env *TestEnv) waitForTorrentAbsent(
	ctx context.Context,
	client *qbittorrent.Client,
	hash string,
	timeout time.Duration,
) bool {
	env.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		torrents, err := client.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
			Hashes: []string{hash},
		})
		if err == nil && len(torrents) == 0 {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// WaitForTorrentDeleted waits for a torrent to be deleted, failing if it's still present.
func (env *TestEnv) WaitForTorrentDeleted(
	ctx context.Context,
	client *qbittorrent.Client,
	hash string,
	timeout time.Duration,
	msg string,
) {
	env.t.Helper()
	require.Eventually(env.t, func() bool {
		torrents, err := client.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
			Hashes: []string{hash},
		})
		return err == nil && len(torrents) == 0
	}, timeout, 200*time.Millisecond, msg)
}

// WaitForTorrentComplete waits for a torrent to finish downloading.
func (env *TestEnv) WaitForTorrentComplete(
	client *qbittorrent.Client,
	hash string,
	timeout time.Duration,
) {
	env.t.Helper()
	ctx := context.Background()

	require.Eventually(env.t, func() bool {
		torrents, err := client.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
			Hashes: []string{hash},
		})
		if err != nil || len(torrents) == 0 {
			return false
		}
		torrent := torrents[0]
		if torrent.Progress >= 1.0 || env.isTorrentComplete(torrent.State) {
			return true
		}
		env.t.Logf("Torrent progress: %.2f%% (state: %s)", torrent.Progress*100, torrent.State)
		return false
	}, timeout, 5*time.Second, "torrent should complete downloading")
}

func (env *TestEnv) isTorrentComplete(state qbittorrent.TorrentState) bool {
	switch state {
	case qbittorrent.TorrentStateUploading,
		qbittorrent.TorrentStateStalledUp,
		qbittorrent.TorrentStateForcedUp,
		qbittorrent.TorrentStatePausedUp,
		qbittorrent.TorrentStateStoppedUp:
		return true
	default:
		return false
	}
}

// IsTorrentCompleteOnDestination checks if a torrent is complete on destination qBittorrent.
// This is the new way to verify sync completion - checking destination qB status
// instead of the old "synced" tag approach.
func (env *TestEnv) IsTorrentCompleteOnDestination(ctx context.Context, hash string) bool {
	torrents, err := env.destinationClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{hash},
	})
	if err != nil || len(torrents) == 0 {
		return false
	}
	torrent := torrents[0]
	return torrent.Progress >= 1.0 || env.isTorrentComplete(torrent.State)
}

// WaitForTorrentCompleteOnDestination waits for a torrent to be complete on destination qBittorrent.
func (env *TestEnv) WaitForTorrentCompleteOnDestination(
	ctx context.Context,
	hash string,
	timeout time.Duration,
	msg string,
) {
	env.t.Helper()
	require.Eventually(env.t, func() bool {
		return env.IsTorrentCompleteOnDestination(ctx, hash)
	}, timeout, 2*time.Second, msg)
}

// WaitForSyncedTagOnDestination waits for the synced tag to appear on a destination torrent.
// The synced tag is applied in background finalization after addAndVerifyTorrent completes.
func (env *TestEnv) WaitForSyncedTagOnDestination(ctx context.Context, hash string, timeout time.Duration, msg string) {
	env.t.Helper()
	require.Eventually(env.t, func() bool {
		torrents, err := env.destinationClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
			Hashes: []string{hash},
		})
		if err != nil || len(torrents) == 0 {
			return false
		}
		return strings.Contains(torrents[0].Tags, "synced")
	}, timeout, time.Second, msg)
}

// IsTorrentStopped checks if a torrent is in a stopped/paused state.
func (env *TestEnv) IsTorrentStopped(ctx context.Context, client *qbittorrent.Client, hash string) bool {
	torrents, err := client.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{hash},
	})
	if err != nil || len(torrents) == 0 {
		return false
	}
	switch torrents[0].State {
	case qbittorrent.TorrentStateStoppedUp,
		qbittorrent.TorrentStatePausedUp,
		qbittorrent.TorrentStateStoppedDl,
		qbittorrent.TorrentStatePausedDl:
		return true
	default:
		return false
	}
}

// CreateGRPCDestination creates a gRPC destination for testing with 2 connections
// (matching the default MinGRPCConnections value used in production).
func (env *TestEnv) CreateGRPCDestination() (*streaming.GRPCDestination, error) {
	return streaming.NewGRPCDestination(env.grpcAddr, 2, 8)
}

// CreateSourceConfig creates a source config for testing.
func (env *TestEnv) CreateSourceConfig(opts ...SourceConfigOption) *config.SourceConfig {
	cfg := &config.SourceConfig{
		BaseConfig: config.BaseConfig{
			QBURL:      env.sourceURL,
			QBUsername: testUsername,
			QBPassword: testPassword,
			DataPath:   env.sourcePath,
			SyncedTag:  "synced",
		},
		MinSpaceGB:         1,
		MinSeedingTime:     0,
		SleepInterval:      time.Second,
		MinGRPCConnections: 2,
		MaxGRPCConnections: 8,
		NumSenders:         4,
		SourceRemovedTag:   "source-removed",
	}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// SourceConfigOption is a functional option for source config.
type SourceConfigOption func(*config.SourceConfig)

// WithDryRun sets dry run mode.
func WithDryRun(dryRun bool) SourceConfigOption {
	return func(cfg *config.SourceConfig) {
		cfg.DryRun = dryRun
	}
}

// WithMinSeedingTime sets minimum seeding time.
func WithMinSeedingTime(d time.Duration) SourceConfigOption {
	return func(cfg *config.SourceConfig) {
		cfg.MinSeedingTime = d
	}
}

// WithGRPCConnections sets the min and max gRPC connections.
func WithGRPCConnections(min, max int) SourceConfigOption {
	return func(cfg *config.SourceConfig) {
		cfg.MinGRPCConnections = min
		cfg.MaxGRPCConnections = max
	}
}

// WithNumSenders sets the number of sender goroutines.
func WithNumSenders(n int) SourceConfigOption {
	return func(cfg *config.SourceConfig) {
		cfg.NumSenders = n
	}
}

// WithSourceRemovedTag sets the tag applied on destination when torrent is removed from source.
func WithSourceRemovedTag(tag string) SourceConfigOption {
	return func(cfg *config.SourceConfig) {
		cfg.SourceRemovedTag = tag
	}
}

// CreateSourceTask creates a QBTask for testing.
func (env *TestEnv) CreateSourceTask(cfg *config.SourceConfig) (*source.QBTask, *streaming.GRPCDestination, error) {
	dest, err := env.CreateGRPCDestination()
	if err != nil {
		return nil, nil, fmt.Errorf("creating gRPC destination: %w", err)
	}

	task, err := source.NewQBTask(cfg, dest, env.logger)
	if err != nil {
		dest.Close()
		return nil, nil, fmt.Errorf("creating source task: %w", err)
	}

	return task, dest, nil
}

// StopDestinationServer stops the destination gRPC server.
func (env *TestEnv) StopDestinationServer() {
	env.t.Helper()
	env.destinationCancel()
	<-env.destinationDone
	env.t.Log("Destination gRPC server stopped")
}

// StartDestinationServer starts (or restarts) the destination gRPC server.
func (env *TestEnv) StartDestinationServer() {
	env.t.Helper()
	ctx := context.Background()

	// Create new server with same config
	env.destinationServer = destination.NewServer(env.destinationServerConfig, env.logger)

	// Start in background
	env.destinationServerCtx, env.destinationCancel = context.WithCancel(context.Background())
	env.destinationDone = make(chan error, 1)
	go func() {
		env.destinationDone <- env.destinationServer.Run(env.destinationServerCtx)
	}()

	// Wait for gRPC server to be ready
	require.Eventually(env.t, func() bool {
		conn, dialErr := net.Dial("tcp", env.grpcAddr)
		if dialErr != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, 10*time.Second, 100*time.Millisecond, "gRPC server should be ready after restart")

	// Re-login to destination qBittorrent
	require.Eventually(env.t, func() bool {
		return env.destinationClient.LoginCtx(ctx) == nil
	}, 30*time.Second, time.Second, "destination qBittorrent should be accessible after destination server restart")

	env.t.Log("Destination gRPC server started")
}

// StopSourceQBittorrent stops the source qBittorrent container.
func (env *TestEnv) StopSourceQBittorrent(ctx context.Context) error {
	env.t.Helper()
	return env.stopContainer(ctx, "qb-source", "Source")
}

// StartSourceQBittorrent starts the source qBittorrent container.
func (env *TestEnv) StartSourceQBittorrent(ctx context.Context) error {
	env.t.Helper()
	newURL, err := env.startContainer(ctx, "qb-source", "Source")
	if err != nil {
		return err
	}

	env.sourceURL = newURL
	env.sourceClient = qbittorrent.NewClient(qbittorrent.Config{
		Host:     newURL,
		Username: testUsername,
		Password: testPassword,
	})
	env.waitForClientReady(ctx, env.sourceClient, "source")

	env.t.Log("Source qBittorrent container started and healthy")
	return nil
}

// StopDestinationQBittorrent stops the destination qBittorrent container.
func (env *TestEnv) StopDestinationQBittorrent(ctx context.Context) error {
	env.t.Helper()
	return env.stopContainer(ctx, "qb-destination", "Destination")
}

// StartDestinationQBittorrent starts the destination qBittorrent container.
func (env *TestEnv) StartDestinationQBittorrent(ctx context.Context) error {
	env.t.Helper()
	newURL, err := env.startContainer(ctx, "qb-destination", "Destination")
	if err != nil {
		return err
	}

	env.destinationURL = newURL
	env.destinationClient = qbittorrent.NewClient(qbittorrent.Config{
		Host:     newURL,
		Username: testUsername,
		Password: testPassword,
	})
	env.waitForClientReady(ctx, env.destinationClient, "destination")

	// Update the destination server config with the new URL and restart.
	env.destinationServerConfig.QB.URL = newURL
	env.StopDestinationServer()
	env.StartDestinationServer()

	env.t.Log("Destination qBittorrent container started and healthy")
	return nil
}

func (env *TestEnv) stopContainer(ctx context.Context, service, name string) error {
	container, err := env.compose.ServiceContainer(ctx, service)
	if err != nil {
		return fmt.Errorf("getting %s container: %w", name, err)
	}
	timeout := 10 * time.Second
	if stopErr := container.Stop(ctx, &timeout); stopErr != nil {
		return fmt.Errorf("stopping %s container: %w", name, stopErr)
	}
	env.t.Logf("%s qBittorrent container stopped", name)
	return nil
}

func (env *TestEnv) startContainer(
	ctx context.Context,
	service, name string,
) (string, error) {
	container, err := env.compose.ServiceContainer(ctx, service)
	if err != nil {
		return "", fmt.Errorf("getting %s container: %w", name, err)
	}
	if startErr := container.Start(ctx); startErr != nil {
		return "", fmt.Errorf("starting %s container: %w", name, startErr)
	}

	env.t.Logf("Waiting for %s qBittorrent to be healthy...", name)
	waitStrategy := wait.ForHealthCheck().WithStartupTimeout(120 * time.Second)
	if waitErr := waitStrategy.WaitUntilReady(ctx, container); waitErr != nil {
		return "", fmt.Errorf("waiting for %s container health: %w", name, waitErr)
	}

	// Re-query mapped port after restart (testcontainers best practice).
	mappedPort, portErr := container.MappedPort(ctx, "8080/tcp")
	if portErr != nil {
		return "", fmt.Errorf("getting mapped port for %s after restart: %w", name, portErr)
	}

	return fmt.Sprintf("http://localhost:%s", mappedPort.Port()), nil
}

// waitForClientReady waits for a qBittorrent client to login and be fully
// operational (BT_backup loaded). Use after creating a fresh client.
func (env *TestEnv) waitForClientReady(ctx context.Context, client *qbittorrent.Client, name string) {
	env.t.Helper()
	require.Eventually(env.t, func() bool {
		return client.LoginCtx(ctx) == nil
	}, 30*time.Second, time.Second, "%s qBittorrent should accept login after restart", name)

	require.Eventually(env.t, func() bool {
		_, listErr := client.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{})
		return listErr == nil
	}, 30*time.Second, time.Second, "%s qBittorrent API should be ready after restart", name)
}

// RestartSourceQBittorrent restarts the source qBittorrent container.
func (env *TestEnv) RestartSourceQBittorrent(ctx context.Context) error {
	env.t.Helper()
	if err := env.StopSourceQBittorrent(ctx); err != nil {
		return err
	}
	return env.StartSourceQBittorrent(ctx)
}

// RestartDestinationQBittorrent restarts the destination qBittorrent container.
func (env *TestEnv) RestartDestinationQBittorrent(ctx context.Context) error {
	env.t.Helper()
	if err := env.StopDestinationQBittorrent(ctx); err != nil {
		return err
	}
	return env.StartDestinationQBittorrent(ctx)
}

// RestartDestinationServer restarts the destination gRPC server.
func (env *TestEnv) RestartDestinationServer() {
	env.t.Helper()
	env.StopDestinationServer()
	env.StartDestinationServer()
}

// CleanupBothSides removes torrents from both source and destination qBittorrent instances.
func (env *TestEnv) CleanupBothSides(ctx context.Context, hashes ...string) {
	env.t.Helper()
	for _, hash := range hashes {
		env.CleanupTorrent(ctx, env.sourceClient, hash)
		env.CleanupTorrent(ctx, env.destinationClient, hash)
	}
}

// DownloadTorrentOnSource adds a torrent to source, waits for it to appear, and waits
// for it to finish downloading. Returns the torrent metadata.
func (env *TestEnv) DownloadTorrentOnSource(
	ctx context.Context,
	url, hash string,
	downloadTimeout time.Duration,
) *qbittorrent.Torrent {
	env.t.Helper()
	err := env.AddTorrentToSource(ctx, url, nil)
	require.NoError(env.t, err)

	torrent := env.WaitForTorrent(env.sourceClient, hash, torrentAppearTimeout)
	require.NotNil(env.t, torrent)

	env.t.Logf("Torrent added: %s, waiting for download...", torrent.Name)
	env.WaitForTorrentComplete(env.sourceClient, hash, downloadTimeout)
	env.t.Log("Torrent download complete")

	return torrent
}

// AssertTorrentCompleteOnDestination verifies the torrent exists on destination qBittorrent
// and is 100% complete.
func (env *TestEnv) AssertTorrentCompleteOnDestination(ctx context.Context, hash string) {
	env.t.Helper()
	destTorrents, err := env.destinationClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{hash},
	})
	require.NoError(env.t, err)
	require.Len(env.t, destTorrents, 1, "torrent should exist on destination qBittorrent")
	assert.InDelta(env.t, 1.0, destTorrents[0].Progress, progressTolerance,
		"torrent should be 100% complete on destination")
}
