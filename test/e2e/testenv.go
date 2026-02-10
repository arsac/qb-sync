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

	"github.com/arsac/qb-sync/internal/cold"
	"github.com/arsac/qb-sync/internal/config"
	"github.com/arsac/qb-sync/internal/hot"
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

// TestEnv holds the complete test environment for hot/cold e2e tests.
type TestEnv struct {
	compose    compose.ComposeStack
	hotURL     string
	coldURL    string
	hotPath    string
	coldPath   string
	hotClient  *qbittorrent.Client
	coldClient *qbittorrent.Client
	coldServer *cold.Server
	grpcAddr   string
	t          *testing.T
	logger     *slog.Logger

	// Cancellation for cold server
	coldCancel    context.CancelFunc
	coldDone      chan error
	coldServerCtx context.Context

	// Cold server config for restart
	coldServerConfig cold.ServerConfig
}

// SetupOption configures the test environment.
type SetupOption func(*setupConfig)

type setupConfig struct {
	hotTempPath bool
}

// WithHotTempPath enables temp_path on the hot qBittorrent instance.
// This mounts the same host directory at an additional container path (/incomplete)
// and configures qBittorrent to use it as the temp/incomplete download directory.
func WithHotTempPath() SetupOption {
	return func(cfg *setupConfig) {
		cfg.hotTempPath = true
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
	// Hot directory names match the container mount points (/downloads, /incomplete)
	// so the orchestrator's relative-path computation (Rel(save_path, temp_path)
	// applied to dataPath) resolves to the correct host directory.
	tmpDir := t.TempDir()
	hotPath := filepath.Join(tmpDir, "downloads")
	hotIncompletePath := filepath.Join(tmpDir, "incomplete")
	coldPath := filepath.Join(tmpDir, "cold")
	hotConfig := filepath.Join(tmpDir, "qb-hot")
	coldConfig := filepath.Join(tmpDir, "qb-cold")

	require.NoError(t, os.MkdirAll(hotPath, 0o755))
	require.NoError(t, os.MkdirAll(hotIncompletePath, 0o755))
	require.NoError(t, os.MkdirAll(coldPath, 0o755))
	require.NoError(t, os.MkdirAll(hotConfig, 0o755))
	require.NoError(t, os.MkdirAll(coldConfig, 0o755))

	// Find free ports for fixed binding - ports stay stable across container restarts
	hotPortNum := findFreePortNum(t)
	coldPortNum := findFreePortNum(t)

	// Docker compose content for e2e tests
	// Use fixed host port binding so URLs remain stable across container restarts.
	// This is important for testing orchestrator recovery after qBittorrent restarts.
	hotExtraVolumes := ""
	if sc.hotTempPath {
		// Mount a separate host dir at /incomplete inside the container.
		// The orchestrator's resolveQBDir computes a local tempDataPath from
		// the relative offset between save_path and temp_path, so host paths
		// must mirror the container layout (separate directories).
		hotExtraVolumes = fmt.Sprintf("\n      - %s:/incomplete", hotIncompletePath)
	}

	composeContent := fmt.Sprintf(`
services:
  qb-hot:
    image: ghcr.io/home-operations/qbittorrent:5.1.4
    volumes:
      - %s:/downloads
      - %s:/config%s
    ports:
      - "%d:8080"
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://localhost:8080/api/v2/app/version"]
      interval: 2s
      timeout: 5s
      retries: 30
      start_period: 10s

  qb-cold:
    image: ghcr.io/home-operations/qbittorrent:5.1.4
    volumes:
      - %s:/cold-data
      - %s:/config
    ports:
      - "%d:8080"
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://localhost:8080/api/v2/app/version"]
      interval: 2s
      timeout: 5s
      retries: 30
      start_period: 10s
`, hotPath, hotConfig, hotExtraVolumes, hotPortNum, coldPath, coldConfig, coldPortNum)

	// Create compose stack
	identifier := fmt.Sprintf("qbsync-e2e-%d", time.Now().UnixNano())
	stack, err := compose.NewDockerComposeWith(
		compose.StackIdentifier(identifier),
		compose.WithStackReaders(strings.NewReader(composeContent)),
	)
	require.NoError(t, err)

	// Start the stack and wait for services to be ready
	err = stack.
		WaitForService("qb-hot", wait.ForHTTP("/api/v2/app/version").
			WithPort("8080/tcp").
			WithStartupTimeout(120*time.Second)).
		WaitForService("qb-cold", wait.ForHTTP("/api/v2/app/version").
			WithPort("8080/tcp").
			WithStartupTimeout(120*time.Second)).
		Up(ctx, compose.Wait(true))
	require.NoError(t, err)

	// Use the fixed ports we allocated (stable across container restarts)
	hotURL := fmt.Sprintf("http://localhost:%d", hotPortNum)
	coldURL := fmt.Sprintf("http://localhost:%d", coldPortNum)

	t.Logf("Hot qBittorrent: %s", hotURL)
	t.Logf("Cold qBittorrent: %s", coldURL)

	// Create qBittorrent clients
	hotClient := qbittorrent.NewClient(qbittorrent.Config{
		Host:     hotURL,
		Username: testUsername,
		Password: testPassword,
	})

	coldClient := qbittorrent.NewClient(qbittorrent.Config{
		Host:     coldURL,
		Username: testUsername,
		Password: testPassword,
	})

	// Wait for clients to be ready and login
	require.Eventually(t, func() bool {
		return hotClient.LoginCtx(ctx) == nil
	}, 60*time.Second, 2*time.Second, "hot qBittorrent should be ready")

	require.Eventually(t, func() bool {
		return coldClient.LoginCtx(ctx) == nil
	}, 60*time.Second, 2*time.Second, "cold qBittorrent should be ready")

	// Configure qBittorrent save paths to match volume mounts.
	// The home-operations image defaults to /config/Downloads, but we mount data at /downloads.
	hotPrefs := map[string]interface{}{
		"save_path": "/downloads",
	}
	if sc.hotTempPath {
		hotPrefs["temp_path_enabled"] = true
		hotPrefs["temp_path"] = "/incomplete"
	}
	require.NoError(t, hotClient.SetPreferencesCtx(ctx, hotPrefs))
	require.NoError(t, coldClient.SetPreferencesCtx(ctx, map[string]interface{}{
		"save_path": "/cold-data",
	}))

	// Find a free port for gRPC server
	grpcAddr := findFreePort(t)

	// Start cold gRPC server
	coldServerConfig := cold.ServerConfig{
		ListenAddr: grpcAddr,
		BasePath:   coldPath,
		SavePath:   "/cold-data",
		SyncedTag:  "synced",
		ColdQB: &cold.QBConfig{
			URL:      coldURL,
			Username: testUsername,
			Password: testPassword,
		},
	}
	coldServer := cold.NewServer(coldServerConfig, logger)

	// Start cold server in background
	coldServerCtx, coldServerCancel := context.WithCancel(context.Background())
	coldServerDone := make(chan error, 1)
	go func() {
		coldServerDone <- coldServer.Run(coldServerCtx)
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
		compose:          stack,
		hotURL:           hotURL,
		coldURL:          coldURL,
		hotPath:          hotPath,
		coldPath:         coldPath,
		hotClient:        hotClient,
		coldClient:       coldClient,
		coldServer:       coldServer,
		grpcAddr:         grpcAddr,
		t:                t,
		logger:           logger,
		coldCancel:       coldServerCancel,
		coldDone:         coldServerDone,
		coldServerCtx:    coldServerCtx,
		coldServerConfig: coldServerConfig,
	}

	t.Cleanup(env.Cleanup)

	return env
}

// Cleanup tears down the test environment.
func (env *TestEnv) Cleanup() {
	env.coldCancel()
	<-env.coldDone
	_ = env.compose.Down(context.Background(),
		compose.RemoveOrphans(true),
		compose.RemoveVolumes(true),
	)
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

// findFreePortNum finds an available TCP port and returns the port number.
func findFreePortNum(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	return port
}

// HotURL returns the hot qBittorrent URL.
func (env *TestEnv) HotURL() string {
	return env.hotURL
}

// ColdURL returns the cold qBittorrent URL.
func (env *TestEnv) ColdURL() string {
	return env.coldURL
}

// HotPath returns the hot data path.
func (env *TestEnv) HotPath() string {
	return env.hotPath
}

// ColdPath returns the cold data path.
func (env *TestEnv) ColdPath() string {
	return env.coldPath
}

// HotClient returns the hot qBittorrent client.
func (env *TestEnv) HotClient() *qbittorrent.Client {
	return env.hotClient
}

// ColdClient returns the cold qBittorrent client.
func (env *TestEnv) ColdClient() *qbittorrent.Client {
	return env.coldClient
}

// GRPCAddr returns the cold gRPC server address.
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
	fullPath := filepath.Join(env.hotPath, relativePath)
	dir := filepath.Dir(fullPath)
	require.NoError(env.t, os.MkdirAll(dir, 0o755))

	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}
	require.NoError(env.t, os.WriteFile(fullPath, data, 0o644))
	return fullPath
}

// AddTorrentToHot adds a test torrent to hot qBittorrent from URL.
func (env *TestEnv) AddTorrentToHot(ctx context.Context, url string, opts map[string]string) error {
	return env.addTorrent(ctx, env.hotClient, url, opts)
}

// AddTorrentToCold adds a test torrent to cold qBittorrent from URL.
func (env *TestEnv) AddTorrentToCold(ctx context.Context, url string, opts map[string]string) error {
	if opts == nil {
		opts = make(map[string]string)
	}
	if _, ok := opts["savepath"]; !ok {
		opts["savepath"] = "/cold-data"
	}
	return env.coldClient.AddTorrentFromUrlCtx(ctx, url, opts)
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

// IsTorrentCompleteOnCold checks if a torrent is complete on cold qBittorrent.
// This is the new way to verify sync completion - checking cold qB status
// instead of the old "synced" tag approach.
func (env *TestEnv) IsTorrentCompleteOnCold(ctx context.Context, hash string) bool {
	torrents, err := env.coldClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{hash},
	})
	if err != nil || len(torrents) == 0 {
		return false
	}
	torrent := torrents[0]
	return torrent.Progress >= 1.0 || env.isTorrentComplete(torrent.State)
}

// WaitForTorrentCompleteOnCold waits for a torrent to be complete on cold qBittorrent.
func (env *TestEnv) WaitForTorrentCompleteOnCold(
	ctx context.Context,
	hash string,
	timeout time.Duration,
	msg string,
) {
	env.t.Helper()
	require.Eventually(env.t, func() bool {
		return env.IsTorrentCompleteOnCold(ctx, hash)
	}, timeout, 2*time.Second, msg)
}

// IsTorrentStopped checks if a torrent on hot qBittorrent is in a stopped/paused state.
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
// (matching the default GRPCConnections value used in production).
func (env *TestEnv) CreateGRPCDestination() (*streaming.GRPCDestination, error) {
	return streaming.NewGRPCDestination(env.grpcAddr, 2)
}

// CreateHotConfig creates a hot config for testing.
func (env *TestEnv) CreateHotConfig(opts ...HotConfigOption) *config.HotConfig {
	cfg := &config.HotConfig{
		BaseConfig: config.BaseConfig{
			QBURL:      env.hotURL,
			QBUsername:  testUsername,
			QBPassword: testPassword,
			DataPath:   env.hotPath,
			SyncedTag:  "synced",
		},
		MinSpaceGB:      1,
		MinSeedingTime:  0,
		SleepInterval:   time.Second,
		Force:           false,
		GRPCConnections: 2,
		NumSenders:      4,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// HotConfigOption is a functional option for hot config.
type HotConfigOption func(*config.HotConfig)

// WithDryRun sets dry run mode.
func WithDryRun(dryRun bool) HotConfigOption {
	return func(cfg *config.HotConfig) {
		cfg.DryRun = dryRun
	}
}

// WithForce sets force mode.
func WithForce(force bool) HotConfigOption {
	return func(cfg *config.HotConfig) {
		cfg.Force = force
	}
}

// WithMinSeedingTime sets minimum seeding time.
func WithMinSeedingTime(d time.Duration) HotConfigOption {
	return func(cfg *config.HotConfig) {
		cfg.MinSeedingTime = d
	}
}

// WithGRPCConnections sets the number of gRPC connections.
func WithGRPCConnections(n int) HotConfigOption {
	return func(cfg *config.HotConfig) {
		cfg.GRPCConnections = n
	}
}

// WithNumSenders sets the number of sender goroutines.
func WithNumSenders(n int) HotConfigOption {
	return func(cfg *config.HotConfig) {
		cfg.NumSenders = n
	}
}

// CreateHotTask creates a QBTask for testing.
func (env *TestEnv) CreateHotTask(cfg *config.HotConfig) (*hot.QBTask, *streaming.GRPCDestination, error) {
	dest, err := env.CreateGRPCDestination()
	if err != nil {
		return nil, nil, fmt.Errorf("creating gRPC destination: %w", err)
	}

	task, err := hot.NewQBTask(cfg, dest, env.logger)
	if err != nil {
		dest.Close()
		return nil, nil, fmt.Errorf("creating hot task: %w", err)
	}

	return task, dest, nil
}

// StopColdServer stops the cold gRPC server.
func (env *TestEnv) StopColdServer() {
	env.t.Helper()
	env.coldCancel()
	<-env.coldDone
	env.t.Log("Cold gRPC server stopped")
}

// StartColdServer starts (or restarts) the cold gRPC server.
func (env *TestEnv) StartColdServer() {
	env.t.Helper()
	ctx := context.Background()

	// Create new server with same config
	env.coldServer = cold.NewServer(env.coldServerConfig, env.logger)

	// Start in background
	env.coldServerCtx, env.coldCancel = context.WithCancel(context.Background())
	env.coldDone = make(chan error, 1)
	go func() {
		env.coldDone <- env.coldServer.Run(env.coldServerCtx)
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

	// Re-login to cold qBittorrent
	require.Eventually(env.t, func() bool {
		return env.coldClient.LoginCtx(ctx) == nil
	}, 30*time.Second, time.Second, "cold qBittorrent should be accessible after cold server restart")

	env.t.Log("Cold gRPC server started")
}

// StopHotQBittorrent stops the hot qBittorrent container.
func (env *TestEnv) StopHotQBittorrent(ctx context.Context) error {
	env.t.Helper()
	return env.stopContainer(ctx, "qb-hot", "Hot")
}

// StartHotQBittorrent starts the hot qBittorrent container.
func (env *TestEnv) StartHotQBittorrent(ctx context.Context) error {
	env.t.Helper()
	if err := env.startContainer(ctx, "qb-hot", "Hot", env.hotClient); err != nil {
		return err
	}
	env.t.Log("Hot qBittorrent container started and healthy")
	return nil
}

// StopColdQBittorrent stops the cold qBittorrent container.
func (env *TestEnv) StopColdQBittorrent(ctx context.Context) error {
	env.t.Helper()
	return env.stopContainer(ctx, "qb-cold", "Cold")
}

// StartColdQBittorrent starts the cold qBittorrent container.
func (env *TestEnv) StartColdQBittorrent(ctx context.Context) error {
	env.t.Helper()
	if err := env.startContainer(ctx, "qb-cold", "Cold", env.coldClient); err != nil {
		return err
	}

	env.t.Log("Restarting cold gRPC server...")
	env.StopColdServer()
	env.StartColdServer()

	env.t.Log("Cold qBittorrent container started and healthy")
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
	client *qbittorrent.Client,
) error {
	container, err := env.compose.ServiceContainer(ctx, service)
	if err != nil {
		return fmt.Errorf("getting %s container: %w", name, err)
	}
	if startErr := container.Start(ctx); startErr != nil {
		return fmt.Errorf("starting %s container: %w", name, startErr)
	}

	env.t.Logf("Waiting for %s qBittorrent to be healthy...", name)
	waitStrategy := wait.ForHealthCheck().WithStartupTimeout(120 * time.Second)
	if waitErr := waitStrategy.WaitUntilReady(ctx, container); waitErr != nil {
		return fmt.Errorf("waiting for %s container health: %w", name, waitErr)
	}

	require.Eventually(env.t, func() bool {
		return client.LoginCtx(ctx) == nil
	}, 30*time.Second, time.Second, "%s qBittorrent should accept login after restart", name)

	return nil
}

// RestartHotQBittorrent restarts the hot qBittorrent container.
func (env *TestEnv) RestartHotQBittorrent(ctx context.Context) error {
	env.t.Helper()
	if err := env.StopHotQBittorrent(ctx); err != nil {
		return err
	}
	return env.StartHotQBittorrent(ctx)
}

// RestartColdQBittorrent restarts the cold qBittorrent container.
func (env *TestEnv) RestartColdQBittorrent(ctx context.Context) error {
	env.t.Helper()
	if err := env.StopColdQBittorrent(ctx); err != nil {
		return err
	}
	return env.StartColdQBittorrent(ctx)
}

// RestartColdServer restarts the cold gRPC server.
func (env *TestEnv) RestartColdServer() {
	env.t.Helper()
	env.StopColdServer()
	env.StartColdServer()
}

// CleanupBothSides removes torrents from both hot and cold qBittorrent instances.
func (env *TestEnv) CleanupBothSides(ctx context.Context, hashes ...string) {
	env.t.Helper()
	for _, hash := range hashes {
		env.CleanupTorrent(ctx, env.hotClient, hash)
		env.CleanupTorrent(ctx, env.coldClient, hash)
	}
}

// DownloadTorrentOnHot adds a torrent to hot, waits for it to appear, and waits
// for it to finish downloading. Returns the torrent metadata.
func (env *TestEnv) DownloadTorrentOnHot(ctx context.Context, url, hash string, downloadTimeout time.Duration) *qbittorrent.Torrent {
	env.t.Helper()
	err := env.AddTorrentToHot(ctx, url, nil)
	require.NoError(env.t, err)

	torrent := env.WaitForTorrent(env.hotClient, hash, torrentAppearTimeout)
	require.NotNil(env.t, torrent)

	env.t.Logf("Torrent added: %s, waiting for download...", torrent.Name)
	env.WaitForTorrentComplete(env.hotClient, hash, downloadTimeout)
	env.t.Log("Torrent download complete")

	return torrent
}

// AssertTorrentCompleteOnCold verifies the torrent exists on cold qBittorrent
// and is 100% complete.
func (env *TestEnv) AssertTorrentCompleteOnCold(ctx context.Context, hash string) {
	env.t.Helper()
	coldTorrents, err := env.coldClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{hash},
	})
	require.NoError(env.t, err)
	require.Len(env.t, coldTorrents, 1, "torrent should exist on cold qBittorrent")
	assert.InDelta(env.t, 1.0, coldTorrents[0].Progress, progressTolerance,
		"torrent should be 100% complete on cold")
}
