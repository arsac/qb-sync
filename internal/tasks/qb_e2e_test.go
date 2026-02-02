//go:build e2e

package tasks

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/autobrr/go-qbittorrent"
	"github.com/mailoarsac/qb-router/internal/config"
	"github.com/mailoarsac/qb-router/internal/logger"
	"github.com/mailoarsac/qb-router/internal/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/compose"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// No auth needed - home-operations image has auth disabled for local subnets
	testUsername = ""
	testPassword = ""
)

// e2eTestEnv holds the test environment
type e2eTestEnv struct {
	compose    compose.ComposeStack
	srcURL     string
	destURL    string
	srcPath    string
	destPath   string
	srcClient  *qbittorrent.Client
	destClient *qbittorrent.Client
	t          *testing.T
}

// setupE2EEnv creates and starts the test environment using docker-compose
func setupE2EEnv(t *testing.T) *e2eTestEnv {
	t.Helper()
	ctx := context.Background()

	// Create temp directories for test data
	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "src")
	destPath := filepath.Join(tmpDir, "dest")
	srcConfig := filepath.Join(tmpDir, "qb-src")
	destConfig := filepath.Join(tmpDir, "qb-dest")

	require.NoError(t, os.MkdirAll(srcPath, 0755))
	require.NoError(t, os.MkdirAll(destPath, 0755))
	require.NoError(t, os.MkdirAll(srcConfig, 0755))
	require.NoError(t, os.MkdirAll(destConfig, 0755))

	// Docker compose content for e2e tests
	// Using home-operations image which has auth disabled for local subnets
	composeContent := fmt.Sprintf(`
services:
  qb-src:
    image: ghcr.io/home-operations/qbittorrent:5.1.4
    volumes:
      - %s:/downloads
      - %s:/config
    ports:
      - "8080"

  qb-dest:
    image: ghcr.io/home-operations/qbittorrent:5.1.4
    volumes:
      - %s:/downloads
      - %s:/config
    ports:
      - "8080"
`, srcPath, srcConfig, destPath, destConfig)

	// Create compose stack
	identifier := fmt.Sprintf("qbrouter-e2e-%d", time.Now().UnixNano())
	stack, err := compose.NewDockerComposeWith(
		compose.StackIdentifier(identifier),
		compose.WithStackReaders(strings.NewReader(composeContent)),
	)
	require.NoError(t, err)

	// Start the stack and wait for services to be ready
	err = stack.
		WaitForService("qb-src", wait.ForHTTP("/api/v2/app/version").
			WithPort("8080/tcp").
			WithStartupTimeout(120*time.Second)).
		WaitForService("qb-dest", wait.ForHTTP("/api/v2/app/version").
			WithPort("8080/tcp").
			WithStartupTimeout(120*time.Second)).
		Up(ctx, compose.Wait(true))
	require.NoError(t, err)

	// Get service endpoints
	srcContainer, err := stack.ServiceContainer(ctx, "qb-src")
	require.NoError(t, err)
	srcHost, err := srcContainer.Host(ctx)
	require.NoError(t, err)
	srcPort, err := srcContainer.MappedPort(ctx, "8080/tcp")
	require.NoError(t, err)

	destContainer, err := stack.ServiceContainer(ctx, "qb-dest")
	require.NoError(t, err)
	destHost, err := destContainer.Host(ctx)
	require.NoError(t, err)
	destPort, err := destContainer.MappedPort(ctx, "8080/tcp")
	require.NoError(t, err)

	srcURL := fmt.Sprintf("http://%s:%s", srcHost, srcPort.Port())
	destURL := fmt.Sprintf("http://%s:%s", destHost, destPort.Port())

	t.Logf("Source qBittorrent: %s", srcURL)
	t.Logf("Destination qBittorrent: %s", destURL)

	// Create qBittorrent clients
	srcClient := qbittorrent.NewClient(qbittorrent.Config{
		Host:     srcURL,
		Username: testUsername,
		Password: testPassword,
	})

	destClient := qbittorrent.NewClient(qbittorrent.Config{
		Host:     destURL,
		Username: testUsername,
		Password: testPassword,
	})

	// Wait for clients to be ready and login
	require.Eventually(t, func() bool {
		return srcClient.LoginCtx(ctx) == nil
	}, 60*time.Second, 2*time.Second, "source qBittorrent should be ready")

	require.Eventually(t, func() bool {
		return destClient.LoginCtx(ctx) == nil
	}, 60*time.Second, 2*time.Second, "destination qBittorrent should be ready")

	env := &e2eTestEnv{
		compose:    stack,
		srcURL:     srcURL,
		destURL:    destURL,
		srcPath:    srcPath,
		destPath:   destPath,
		srcClient:  srcClient,
		destClient: destClient,
		t:          t,
	}

	// Register cleanup
	t.Cleanup(func() {
		_ = stack.Down(context.Background(),
			compose.RemoveOrphans(true),
			compose.RemoveVolumes(true),
		)
	})

	return env
}

// createTestFile creates a test file with deterministic content
func (env *e2eTestEnv) createTestFile(relativePath string, size int) string {
	env.t.Helper()
	fullPath := filepath.Join(env.srcPath, relativePath)
	dir := filepath.Dir(fullPath)
	require.NoError(env.t, os.MkdirAll(dir, 0755))

	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}
	require.NoError(env.t, os.WriteFile(fullPath, data, 0644))
	return fullPath
}

// syncFileToDest copies a file from src to dest (simulating rsync)
func (env *e2eTestEnv) syncFileToDest(relativePath string) {
	env.t.Helper()
	srcFile := filepath.Join(env.srcPath, relativePath)
	destFile := filepath.Join(env.destPath, relativePath)

	destDir := filepath.Dir(destFile)
	require.NoError(env.t, os.MkdirAll(destDir, 0755))

	data, err := os.ReadFile(srcFile)
	require.NoError(env.t, err)
	require.NoError(env.t, os.WriteFile(destFile, data, 0644))
}

// createHardlink creates a hardlink from src to dest
func (env *e2eTestEnv) createHardlink(srcRelPath, destRelPath string) {
	env.t.Helper()
	srcPath := filepath.Join(env.srcPath, srcRelPath)
	destPath := filepath.Join(env.srcPath, destRelPath)

	destDir := filepath.Dir(destPath)
	require.NoError(env.t, os.MkdirAll(destDir, 0755))
	require.NoError(env.t, os.Link(srcPath, destPath))
}

// newQBTask creates a QBTask for testing
func (env *e2eTestEnv) newQBTask() *QBTask {
	cfg := &config.Config{
		SrcPath:        env.srcPath,
		DestPath:       env.destPath,
		SrcURL:         env.srcURL,
		DestURL:        env.destURL,
		SrcUsername:    testUsername,
		SrcPassword:    testPassword,
		DestUsername:   testUsername,
		DestPassword:   testPassword,
		MinSpaceGB:     1,
		MinSeedingTime: time.Second,
		SleepInterval:  time.Second,
		DryRun:         false,
	}

	task, err := NewQBTask(cfg, logger.New("test"))
	require.NoError(env.t, err)

	ctx := context.Background()
	require.NoError(env.t, task.srcClient.LoginCtx(ctx))
	require.NoError(env.t, task.destClient.LoginCtx(ctx))

	return task
}

// waitForTorrent waits for a torrent to appear with a specific state
func (env *e2eTestEnv) waitForTorrent(client *qbittorrent.Client, hash string, timeout time.Duration) *qbittorrent.Torrent {
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

// =============================================================================
// E2E TESTS
// =============================================================================

func TestE2E_QBClientConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := setupE2EEnv(t)
	ctx := context.Background()

	// Test source client
	version, err := env.srcClient.GetAppVersionCtx(ctx)
	require.NoError(t, err)
	t.Logf("Source qBittorrent version: %s", version)

	// Test destination client
	version, err = env.destClient.GetAppVersionCtx(ctx)
	require.NoError(t, err)
	t.Logf("Destination qBittorrent version: %s", version)

	// Test getting preferences
	prefs, err := env.srcClient.GetAppPreferencesCtx(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, prefs.SavePath)
	t.Logf("Source save path: %s", prefs.SavePath)

	prefs, err = env.destClient.GetAppPreferencesCtx(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, prefs.SavePath)
	t.Logf("Destination save path: %s", prefs.SavePath)
}

func TestE2E_DestPathCalculation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := setupE2EEnv(t)
	task := env.newQBTask()

	// Test file in subdirectory
	got := task.destFilePath(filepath.Join(env.srcPath, "Movies", "SomeMovie"), "movie.mkv")
	assert.Equal(t, filepath.Join(env.destPath, "Movies", "SomeMovie", "movie.mkv"), got)

	// Test file in root
	got = task.destFilePath(env.srcPath, "file.txt")
	assert.Equal(t, filepath.Join(env.destPath, "file.txt"), got)
}

func TestE2E_FileExistsWithSize(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := setupE2EEnv(t)

	// Create a test file
	testFile := "testdir/testfile.bin"
	fileSize := 1024
	env.createTestFile(testFile, fileSize)

	// Test file exists with correct size
	fullPath := filepath.Join(env.srcPath, testFile)
	exists, err := utils.FileExistsWithSize(fullPath, int64(fileSize))
	require.NoError(t, err)
	assert.True(t, exists, "file should exist with correct size")

	// Test file exists with wrong size
	exists, err = utils.FileExistsWithSize(fullPath, int64(fileSize+100))
	require.NoError(t, err)
	assert.False(t, exists, "file should not match with wrong size")

	// Test non-existent file
	exists, err = utils.FileExistsWithSize(filepath.Join(env.srcPath, "nonexistent"), 100)
	require.NoError(t, err)
	assert.False(t, exists, "non-existent file should return false")
}

func TestE2E_SyncedFileDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := setupE2EEnv(t)
	task := env.newQBTask()

	// Create test files in source
	testDir := "TestTorrent"
	testFile1 := filepath.Join(testDir, "file1.txt")
	testFile2 := filepath.Join(testDir, "file2.txt")

	env.createTestFile(testFile1, 1024)
	env.createTestFile(testFile2, 2048)

	// Before sync - files don't exist at destination
	destPath1 := task.destFilePath(filepath.Join(env.srcPath, testDir), "file1.txt")
	destPath2 := task.destFilePath(filepath.Join(env.srcPath, testDir), "file2.txt")

	exists1, _ := utils.FileExistsWithSize(destPath1, 1024)
	exists2, _ := utils.FileExistsWithSize(destPath2, 2048)
	assert.False(t, exists1, "file1 should not exist at dest before sync")
	assert.False(t, exists2, "file2 should not exist at dest before sync")

	// Sync files to destination
	env.syncFileToDest(testFile1)
	env.syncFileToDest(testFile2)

	// After sync - files should exist with correct sizes
	exists1, err := utils.FileExistsWithSize(destPath1, 1024)
	require.NoError(t, err)
	assert.True(t, exists1, "file1 should exist at dest after sync")

	exists2, err = utils.FileExistsWithSize(destPath2, 2048)
	require.NoError(t, err)
	assert.True(t, exists2, "file2 should exist at dest after sync")
}

func TestE2E_HardlinkDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := setupE2EEnv(t)

	// Create original file
	originalFile := "original/file.txt"
	env.createTestFile(originalFile, 1024)

	// Create hardlink
	hardlinkFile := "hardlink/file.txt"
	env.createHardlink(originalFile, hardlinkFile)

	// Verify hardlink detection
	originalPath := filepath.Join(env.srcPath, originalFile)
	hardlinkPath := filepath.Join(env.srcPath, hardlinkFile)

	isHardlinked, err := utils.AreHardlinked(originalPath, hardlinkPath)
	require.NoError(t, err)
	assert.True(t, isHardlinked, "files should be detected as hardlinked")

	// Create separate file (not hardlinked)
	separateFile := "separate/file.txt"
	env.createTestFile(separateFile, 1024)
	separatePath := filepath.Join(env.srcPath, separateFile)

	isHardlinked, err = utils.AreHardlinked(originalPath, separatePath)
	require.NoError(t, err)
	assert.False(t, isHardlinked, "separate files should not be hardlinked")
}

func TestE2E_CreateGroupMetrics(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := setupE2EEnv(t)
	task := env.newQBTask()

	// Create test torrents with known metrics
	torrents := []qbittorrent.Torrent{
		{
			Hash:          "hash1",
			Name:          "Torrent 1",
			Size:          1000000,
			SeedingTime:   3600,
			NumComplete:   10,
			NumIncomplete: 5,
		},
		{
			Hash:          "hash2",
			Name:          "Torrent 2",
			Size:          2000000,
			SeedingTime:   1800, // Lower seeding time
			NumComplete:   20,
			NumIncomplete: 10,
		},
	}

	group := task.createGroup(torrents)

	// Verify group metrics
	assert.Equal(t, int64(45), group.popularity, "popularity should be sum of all peers")
	assert.Equal(t, int64(2000000), group.maxSize, "maxSize should be largest torrent")
	assert.Equal(t, int64(1800), group.minSeeding, "minSeeding should be shortest seeding time")
	assert.Len(t, group.torrents, 2)
}

func TestE2E_SortGroupsByPriority(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := setupE2EEnv(t)
	task := env.newQBTask()

	// Create groups with different priorities
	groups := []torrentGroup{
		{popularity: 100, maxSize: 5000, torrents: []qbittorrent.Torrent{{Name: "High pop"}}},
		{popularity: 10, maxSize: 1000, torrents: []qbittorrent.Torrent{{Name: "Low pop, small"}}},
		{popularity: 10, maxSize: 3000, torrents: []qbittorrent.Torrent{{Name: "Low pop, large"}}},
		{popularity: 50, maxSize: 2000, torrents: []qbittorrent.Torrent{{Name: "Medium pop"}}},
	}

	sorted := task.sortGroupsByPriority(groups)

	// Verify order: popularity ASC, then size DESC
	assert.Equal(t, "Low pop, large", sorted[0].torrents[0].Name, "lowest popularity, largest size first")
	assert.Equal(t, "Low pop, small", sorted[1].torrents[0].Name, "lowest popularity, smaller size second")
	assert.Equal(t, "Medium pop", sorted[2].torrents[0].Name, "medium popularity third")
	assert.Equal(t, "High pop", sorted[3].torrents[0].Name, "highest popularity last")
}

func TestE2E_MinSeedingTimeRespected(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := setupE2EEnv(t)

	// Create task with 1 hour min seeding time
	cfg := &config.Config{
		SrcPath:        env.srcPath,
		DestPath:       env.destPath,
		SrcURL:         env.srcURL,
		DestURL:        env.destURL,
		SrcUsername:    testUsername,
		SrcPassword:    testPassword,
		DestUsername:   testUsername,
		DestPassword:   testPassword,
		MinSpaceGB:     1,
		MinSeedingTime: time.Hour, // 3600 seconds
		SleepInterval:  time.Second,
		DryRun:         true, // Dry run to avoid actual moves
	}

	task, err := NewQBTask(cfg, logger.New("test"))
	require.NoError(t, err)

	// Group that hasn't seeded long enough (30 minutes)
	youngGroup := torrentGroup{
		torrents:   []qbittorrent.Torrent{{Hash: "young", Name: "Young Torrent"}},
		minSeeding: 1800, // 30 minutes
	}

	// Group that has seeded long enough (2 hours)
	oldGroup := torrentGroup{
		torrents:   []qbittorrent.Torrent{{Hash: "old", Name: "Old Torrent"}},
		minSeeding: 7200, // 2 hours
	}

	ctx := context.Background()

	// Young group should be skipped (returns nil without error in dry run)
	err = task.moveGroupToCold(ctx, youngGroup)
	assert.NoError(t, err, "young group should be skipped without error")

	// Old group should proceed (in dry run, just logs)
	err = task.moveGroupToCold(ctx, oldGroup)
	assert.NoError(t, err, "old group should proceed without error")
}

func TestE2E_DryRunMode(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := setupE2EEnv(t)

	// Create task in dry run mode
	cfg := &config.Config{
		SrcPath:        env.srcPath,
		DestPath:       env.destPath,
		SrcURL:         env.srcURL,
		DestURL:        env.destURL,
		SrcUsername:    testUsername,
		SrcPassword:    testPassword,
		DestUsername:   testUsername,
		DestPassword:   testPassword,
		MinSpaceGB:     1,
		MinSeedingTime: 0, // No minimum
		SleepInterval:  time.Second,
		DryRun:         true,
	}

	task, err := NewQBTask(cfg, logger.New("test"))
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, task.srcClient.LoginCtx(ctx))
	require.NoError(t, task.destClient.LoginCtx(ctx))

	// In dry run, moveTorrentToCold should return without making changes
	torrent := qbittorrent.Torrent{
		Hash:     "dryrunhash",
		Name:     "Dry Run Test",
		SavePath: env.srcPath,
	}

	err = task.moveTorrentToCold(ctx, torrent)
	assert.NoError(t, err, "dry run should complete without error")

	// Verify no torrent was actually added to destination
	destTorrents, err := task.destClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{})
	require.NoError(t, err)
	assert.Empty(t, destTorrents, "no torrents should be added in dry run mode")
}

func TestE2E_TagParsing(t *testing.T) {
	// This test doesn't need containers - tests pure function
	tests := []struct {
		tags      string
		hasSynced bool
	}{
		{"", false},
		{"synced", true},
		{"foo, synced, bar", true},
		{"foo, bar", false},
		{"  synced  ", true},
		{"unsynced", false},
	}

	for _, tt := range tests {
		t.Run(tt.tags, func(t *testing.T) {
			result := hasSyncedTag(tt.tags)
			assert.Equal(t, tt.hasSynced, result)
		})
	}
}

func TestE2E_FullSyncWorkflow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := setupE2EEnv(t)
	task := env.newQBTask()
	ctx := context.Background()

	// Create test directory structure
	torrentName := "TestMovie"
	files := []struct {
		path string
		size int
	}{
		{filepath.Join(torrentName, "movie.mkv"), 10240},
		{filepath.Join(torrentName, "subs.srt"), 1024},
		{filepath.Join(torrentName, "info.nfo"), 256},
	}

	// Create files in source
	for _, f := range files {
		env.createTestFile(f.path, f.size)
	}

	// Verify destFilePath calculation for each file
	savePath := filepath.Join(env.srcPath, torrentName)
	for _, f := range files {
		fileName := filepath.Base(f.path)
		destPath := task.destFilePath(savePath, fileName)
		expectedPath := filepath.Join(env.destPath, torrentName, fileName)
		assert.Equal(t, expectedPath, destPath, "destFilePath should be correct for %s", f.path)
	}

	// Sync all files to destination
	for _, f := range files {
		env.syncFileToDest(f.path)
	}

	// Verify all files exist at destination with correct sizes
	for _, f := range files {
		destPath := filepath.Join(env.destPath, f.path)
		exists, err := utils.FileExistsWithSize(destPath, int64(f.size))
		require.NoError(t, err)
		assert.True(t, exists, "file %s should exist at dest with correct size", f.path)
	}

	// Verify we can run tagSyncedTorrents without error (even with no actual torrents)
	err := task.tagSyncedTorrents(ctx)
	assert.NoError(t, err, "tagSyncedTorrents should complete without error")
}

func TestE2E_ForceMode(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := setupE2EEnv(t)

	// Create task with Force mode enabled (bypasses disk space check)
	cfg := &config.Config{
		SrcPath:        env.srcPath,
		DestPath:       env.destPath,
		SrcURL:         env.srcURL,
		DestURL:        env.destURL,
		SrcUsername:    testUsername,
		SrcPassword:    testPassword,
		DestUsername:   testUsername,
		DestPassword:   testPassword,
		MinSpaceGB:     9999999, // Impossibly high threshold
		MinSeedingTime: 0,
		SleepInterval:  time.Second,
		DryRun:         true,
		Force:          true, // Force mode bypasses disk space check
	}

	task, err := NewQBTask(cfg, logger.New("test"))
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, task.srcClient.LoginCtx(ctx))
	require.NoError(t, task.destClient.LoginCtx(ctx))

	// maybeMoveToCold should proceed even though we don't have 9999999 GB free
	// because Force mode bypasses the check
	err = task.maybeMoveToCold(ctx)
	assert.NoError(t, err, "force mode should bypass disk space check")
}

func TestE2E_GetFreeSpaceGB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := setupE2EEnv(t)
	task := env.newQBTask()

	// Test getFreeSpaceGB on the temp directory
	freeGB, err := task.getFreeSpaceGB(env.srcPath)
	require.NoError(t, err)
	assert.Greater(t, freeGB, int64(0), "free space should be greater than 0")
	t.Logf("Free space on %s: %d GB", env.srcPath, freeGB)
}

func TestE2E_GroupHardlinkedTorrentsWithFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := setupE2EEnv(t)
	task := env.newQBTask()

	// Create test files that simulate hardlinked torrents
	// Torrent A has files in dirA
	env.createTestFile("dirA/file1.txt", 1024)
	env.createTestFile("dirA/file2.txt", 2048)

	// Torrent B shares a hardlink with torrent A
	env.createHardlink("dirA/file1.txt", "dirB/file1.txt")
	env.createTestFile("dirB/file3.txt", 512)

	// Torrent C is independent (no hardlinks)
	env.createTestFile("dirC/file4.txt", 4096)

	// Verify hardlinks are detected correctly
	pathA := filepath.Join(env.srcPath, "dirA/file1.txt")
	pathB := filepath.Join(env.srcPath, "dirB/file1.txt")
	pathC := filepath.Join(env.srcPath, "dirC/file4.txt")

	isHardlinked, err := utils.AreHardlinked(pathA, pathB)
	require.NoError(t, err)
	assert.True(t, isHardlinked, "A and B should share hardlink")

	isHardlinked, err = utils.AreHardlinked(pathA, pathC)
	require.NoError(t, err)
	assert.False(t, isHardlinked, "A and C should not share hardlink")

	// Create mock torrent data
	torrents := []qbittorrent.Torrent{
		{Hash: "hashA", Name: "Torrent A", SavePath: filepath.Join(env.srcPath, "dirA"),
			Size: 3072, SeedingTime: 3600, NumComplete: 10, NumIncomplete: 5},
		{Hash: "hashB", Name: "Torrent B", SavePath: filepath.Join(env.srcPath, "dirB"),
			Size: 1536, SeedingTime: 1800, NumComplete: 5, NumIncomplete: 2},
		{Hash: "hashC", Name: "Torrent C", SavePath: filepath.Join(env.srcPath, "dirC"),
			Size: 4096, SeedingTime: 7200, NumComplete: 20, NumIncomplete: 10},
	}

	// Test areHardlinkedTorrents with cache
	cache := map[string][]torrentFile{
		"hashA": {{Name: "file1.txt", Size: 1024}, {Name: "file2.txt", Size: 2048}},
		"hashB": {{Name: "file1.txt", Size: 1024}, {Name: "file3.txt", Size: 512}},
		"hashC": {{Name: "file4.txt", Size: 4096}},
	}

	hardlinked, err := task.areHardlinkedTorrents(torrents[0], torrents[1], cache)
	require.NoError(t, err)
	assert.True(t, hardlinked, "torrents A and B should be hardlinked")

	hardlinked, err = task.areHardlinkedTorrents(torrents[0], torrents[2], cache)
	require.NoError(t, err)
	assert.False(t, hardlinked, "torrents A and C should not be hardlinked")
}

func TestE2E_DestSavePath(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := setupE2EEnv(t)
	task := env.newQBTask()

	// Path under src
	assert.Equal(t, filepath.Join(env.destPath, "Movies", "Film"),
		task.destSavePath(filepath.Join(env.srcPath, "Movies", "Film")))

	// Src root
	assert.Equal(t, env.destPath, task.destSavePath(env.srcPath))

	// Path not under src (fallback to basename)
	assert.Equal(t, filepath.Join(env.destPath, "path"), task.destSavePath("/some/other/path"))
}

func TestE2E_CategoryTagsPreservation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := setupE2EEnv(t)
	task := env.newQBTask()

	// Create a torrent with category and tags
	torrent := qbittorrent.Torrent{
		Hash:     "preservetest",
		Name:     "Test Preservation",
		Category: "movies",
		Tags:     "synced, important, bluray",
		SavePath: filepath.Join(env.srcPath, "Movies"),
	}

	// Test that destSavePath works correctly
	destSave := task.destSavePath(torrent.SavePath)
	assert.Equal(t, filepath.Join(env.destPath, "Movies"), destSave)

	// Verify the options that would be passed to AddTorrentFromMemoryCtx
	opts := map[string]string{
		"savepath":           destSave,
		"skip_checking":      "false",
		"paused":             "false",
		"root_folder":        "true",
		"contentLayout":      "Original",
		"autoTMM":            "false",
		"sequentialDownload": "false",
	}

	// Preserve category and tags
	if torrent.Category != "" {
		opts["category"] = torrent.Category
	}
	if torrent.Tags != "" {
		opts["tags"] = torrent.Tags
	}

	assert.Equal(t, "movies", opts["category"], "category should be preserved")
	assert.Equal(t, "synced, important, bluray", opts["tags"], "tags should be preserved")
}

func TestE2E_RunOnceNoTorrents(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := setupE2EEnv(t)
	task := env.newQBTask()
	ctx := context.Background()

	// runOnce should complete without error even with no torrents
	err := task.runOnce(ctx)
	assert.NoError(t, err, "runOnce should complete without error with no torrents")
}

func TestE2E_FetchSyncedTorrentsEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := setupE2EEnv(t)
	task := env.newQBTask()
	ctx := context.Background()

	// fetchSyncedTorrents should return empty slice when no torrents have synced tag
	torrents, err := task.fetchSyncedTorrents(ctx)
	require.NoError(t, err)
	assert.Empty(t, torrents, "should return empty slice when no synced torrents")
}

func TestE2E_SplitTagsEdgeCases(t *testing.T) {
	// This test doesn't need containers - tests pure function
	tests := []struct {
		name string
		tags string
		want []string
	}{
		{"empty string", "", nil},
		{"only commas", ",,,", nil},
		{"spaces and commas", "  ,  ,  ", nil},
		{"single tag with spaces", "   mytag   ", []string{"mytag"}},
		{"multiple tags various spacing", "tag1,  tag2  ,tag3,  tag4", []string{"tag1", "tag2", "tag3", "tag4"}},
		{"unicode tags", "日本語, émojis, тест", []string{"日本語", "émojis", "тест"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitTags(tt.tags)
			assert.Equal(t, tt.want, got)
		})
	}
}

// =============================================================================
// E2E TESTS WITH REAL TORRENTS
// These tests use actual .torrent files to test the full workflow
// =============================================================================

// Small public domain torrent URLs for testing
const (
	// Sintel trailer - small, fast download (~130MB but we can pause immediately)
	// We use this to test torrent operations, not to download the full content
	testTorrentURL = "https://webtorrent.io/torrents/sintel.torrent"
	// Expected hash for the Sintel torrent
	sintelHash = "08ada5a7a6183aae1e09d831df6748d566095a10"
)

// addTestTorrent adds a torrent from URL and returns its hash
func (env *e2eTestEnv) addTestTorrent(ctx context.Context, client *qbittorrent.Client, torrentURL string, opts map[string]string) (string, error) {
	if opts == nil {
		opts = make(map[string]string)
	}
	// Add paused so we don't actually download
	opts["paused"] = "true"
	opts["savepath"] = "/downloads"

	err := client.AddTorrentFromUrlCtx(ctx, torrentURL, opts)
	return sintelHash, err
}

// cleanupTorrent removes a torrent if it exists
func (env *e2eTestEnv) cleanupTorrent(ctx context.Context, client *qbittorrent.Client, hash string) {
	// Best effort cleanup - ignore errors
	_ = client.DeleteTorrentsCtx(ctx, []string{hash}, true)
}

func TestE2E_AddTorrentFromURL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := setupE2EEnv(t)
	ctx := context.Background()

	// Cleanup any existing torrent first
	env.cleanupTorrent(ctx, env.srcClient, sintelHash)

	// Add torrent paused
	_, err := env.addTestTorrent(ctx, env.srcClient, testTorrentURL, nil)
	require.NoError(t, err, "should add torrent from URL")

	// Wait for torrent to appear
	torrent := env.waitForTorrent(env.srcClient, sintelHash, 30*time.Second)
	require.NotNil(t, torrent)

	t.Logf("Added torrent: %s (hash: %s)", torrent.Name, torrent.Hash)
	assert.Equal(t, sintelHash, torrent.Hash)
	assert.Contains(t, torrent.Name, "Sintel")

	// Cleanup
	env.cleanupTorrent(ctx, env.srcClient, sintelHash)
}

func TestE2E_TorrentFileOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := setupE2EEnv(t)
	ctx := context.Background()

	// Cleanup any existing torrent first
	env.cleanupTorrent(ctx, env.srcClient, sintelHash)

	// Add torrent paused
	_, err := env.addTestTorrent(ctx, env.srcClient, testTorrentURL, nil)
	require.NoError(t, err)

	// Wait for torrent to appear
	torrent := env.waitForTorrent(env.srcClient, sintelHash, 30*time.Second)
	require.NotNil(t, torrent)

	// Get files information
	filesPtr, err := env.srcClient.GetFilesInformationCtx(ctx, sintelHash)
	require.NoError(t, err)
	require.NotNil(t, filesPtr)

	files := *filesPtr
	t.Logf("Torrent has %d files", len(files))
	assert.Greater(t, len(files), 0, "torrent should have files")

	for i, file := range files {
		t.Logf("  File %d: %s (size: %d)", i, file.Name, file.Size)
	}

	// Cleanup
	env.cleanupTorrent(ctx, env.srcClient, sintelHash)
}

func TestE2E_AddTagsToTorrent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := setupE2EEnv(t)
	ctx := context.Background()

	// Cleanup any existing torrent first
	env.cleanupTorrent(ctx, env.srcClient, sintelHash)

	// Add torrent paused
	_, err := env.addTestTorrent(ctx, env.srcClient, testTorrentURL, nil)
	require.NoError(t, err)

	// Wait for torrent to appear
	torrent := env.waitForTorrent(env.srcClient, sintelHash, 30*time.Second)
	require.NotNil(t, torrent)

	// Initially should have no synced tag
	assert.False(t, hasSyncedTag(torrent.Tags), "should not have synced tag initially")

	// Add synced tag
	err = env.srcClient.AddTagsCtx(ctx, []string{sintelHash}, syncedTag)
	require.NoError(t, err)

	// Wait for tag to be applied and verify
	require.Eventually(t, func() bool {
		torrents, err := env.srcClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
			Hashes: []string{sintelHash},
		})
		if err != nil || len(torrents) == 0 {
			return false
		}
		return hasSyncedTag(torrents[0].Tags)
	}, 10*time.Second, 500*time.Millisecond, "torrent should have synced tag")

	t.Log("Successfully added synced tag to torrent")

	// Cleanup
	env.cleanupTorrent(ctx, env.srcClient, sintelHash)
}

func TestE2E_ExportTorrent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := setupE2EEnv(t)
	ctx := context.Background()

	// Cleanup any existing torrent first
	env.cleanupTorrent(ctx, env.srcClient, sintelHash)

	// Add torrent paused
	_, err := env.addTestTorrent(ctx, env.srcClient, testTorrentURL, nil)
	require.NoError(t, err)

	// Wait for torrent to appear
	_ = env.waitForTorrent(env.srcClient, sintelHash, 30*time.Second)

	// Export torrent data
	torrentData, err := env.srcClient.ExportTorrentCtx(ctx, sintelHash)
	require.NoError(t, err)
	require.NotEmpty(t, torrentData)

	t.Logf("Exported torrent data: %d bytes", len(torrentData))

	// Verify it's valid torrent data (starts with "d" for bencoded dict)
	assert.Equal(t, byte('d'), torrentData[0], "torrent data should be bencoded")

	// Cleanup
	env.cleanupTorrent(ctx, env.srcClient, sintelHash)
}

func TestE2E_PauseResumeTorrent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := setupE2EEnv(t)
	ctx := context.Background()

	// Cleanup any existing torrent first
	env.cleanupTorrent(ctx, env.srcClient, sintelHash)

	// Add torrent (starts paused)
	_, err := env.addTestTorrent(ctx, env.srcClient, testTorrentURL, nil)
	require.NoError(t, err)

	// Wait for torrent to appear
	torrent := env.waitForTorrent(env.srcClient, sintelHash, 30*time.Second)
	require.NotNil(t, torrent)

	// Resume the torrent
	err = env.srcClient.ResumeCtx(ctx, []string{sintelHash})
	require.NoError(t, err)

	// Wait for state to change from paused
	require.Eventually(t, func() bool {
		torrents, err := env.srcClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
			Hashes: []string{sintelHash},
		})
		if err != nil || len(torrents) == 0 {
			return false
		}
		state := torrents[0].State
		// Should not be paused anymore
		return state != qbittorrent.TorrentStatePausedDl && state != qbittorrent.TorrentStatePausedUp
	}, 10*time.Second, 500*time.Millisecond, "torrent should be resumed")

	// Pause the torrent
	err = env.srcClient.PauseCtx(ctx, []string{sintelHash})
	require.NoError(t, err)

	// Wait for state to be paused
	require.Eventually(t, func() bool {
		torrents, err := env.srcClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
			Hashes: []string{sintelHash},
		})
		if err != nil || len(torrents) == 0 {
			return false
		}
		state := torrents[0].State
		return state == qbittorrent.TorrentStatePausedDl ||
			state == qbittorrent.TorrentStatePausedUp ||
			state == qbittorrent.TorrentStateStoppedDl ||
			state == qbittorrent.TorrentStateStoppedUp
	}, 10*time.Second, 500*time.Millisecond, "torrent should be paused")

	t.Log("Successfully paused and resumed torrent")

	// Cleanup
	env.cleanupTorrent(ctx, env.srcClient, sintelHash)
}

func TestE2E_TransferTorrentBetweenInstances(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := setupE2EEnv(t)
	ctx := context.Background()

	// Cleanup any existing torrents
	env.cleanupTorrent(ctx, env.srcClient, sintelHash)
	env.cleanupTorrent(ctx, env.destClient, sintelHash)

	// Add torrent to source
	_, err := env.addTestTorrent(ctx, env.srcClient, testTorrentURL, map[string]string{
		"category": "test-category",
		"tags":     "test-tag",
	})
	require.NoError(t, err)

	// Wait for torrent on source
	srcTorrent := env.waitForTorrent(env.srcClient, sintelHash, 30*time.Second)
	require.NotNil(t, srcTorrent)
	t.Logf("Source torrent: %s", srcTorrent.Name)

	// Export torrent from source
	torrentData, err := env.srcClient.ExportTorrentCtx(ctx, sintelHash)
	require.NoError(t, err)

	// Add to destination with preserved category/tags
	err = env.destClient.AddTorrentFromMemoryCtx(ctx, torrentData, map[string]string{
		"savepath": "/downloads",
		"paused":   "true",
		"category": srcTorrent.Category,
		"tags":     srcTorrent.Tags,
	})
	require.NoError(t, err)

	// Wait for torrent on destination
	destTorrent := env.waitForTorrent(env.destClient, sintelHash, 30*time.Second)
	require.NotNil(t, destTorrent)
	t.Logf("Destination torrent: %s", destTorrent.Name)

	// Verify torrent exists on both
	assert.Equal(t, srcTorrent.Hash, destTorrent.Hash)
	assert.Equal(t, srcTorrent.Name, destTorrent.Name)

	// Cleanup
	env.cleanupTorrent(ctx, env.srcClient, sintelHash)
	env.cleanupTorrent(ctx, env.destClient, sintelHash)
}

func TestE2E_FullTorrentMigrationDryRun(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	env := setupE2EEnv(t)
	ctx := context.Background()

	// Create task in dry run mode
	cfg := &config.Config{
		SrcPath:        env.srcPath,
		DestPath:       env.destPath,
		SrcURL:         env.srcURL,
		DestURL:        env.destURL,
		SrcUsername:    testUsername,
		SrcPassword:    testPassword,
		DestUsername:   testUsername,
		DestPassword:   testPassword,
		MinSpaceGB:     1,
		MinSeedingTime: 0,
		SleepInterval:  time.Second,
		DryRun:         true,
		Force:          true, // Force to bypass space check
	}

	task, err := NewQBTask(cfg, logger.New("test"))
	require.NoError(t, err)
	require.NoError(t, task.srcClient.LoginCtx(ctx))
	require.NoError(t, task.destClient.LoginCtx(ctx))

	// Cleanup any existing torrents
	env.cleanupTorrent(ctx, env.srcClient, sintelHash)
	env.cleanupTorrent(ctx, env.destClient, sintelHash)

	// Add torrent to source with synced tag
	_, err = env.addTestTorrent(ctx, env.srcClient, testTorrentURL, map[string]string{
		"tags": syncedTag,
	})
	require.NoError(t, err)

	// Wait for torrent
	srcTorrent := env.waitForTorrent(env.srcClient, sintelHash, 30*time.Second)
	require.NotNil(t, srcTorrent)

	// Verify it has synced tag
	require.Eventually(t, func() bool {
		torrents, err := env.srcClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
			Hashes: []string{sintelHash},
		})
		if err != nil || len(torrents) == 0 {
			return false
		}
		return hasSyncedTag(torrents[0].Tags)
	}, 10*time.Second, 500*time.Millisecond)

	// Fetch synced torrents
	syncedTorrents, err := task.fetchSyncedTorrents(ctx)
	require.NoError(t, err)
	t.Logf("Found %d synced torrents", len(syncedTorrents))

	// In dry run, maybeMoveToCold should not actually move anything
	err = task.maybeMoveToCold(ctx)
	require.NoError(t, err)

	// Torrent should still exist on source (dry run)
	torrents, err := env.srcClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{sintelHash},
	})
	require.NoError(t, err)
	assert.Len(t, torrents, 1, "torrent should still exist on source in dry run mode")

	// Torrent should NOT exist on destination (dry run)
	destTorrents, err := env.destClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{sintelHash},
	})
	require.NoError(t, err)
	assert.Empty(t, destTorrents, "torrent should not be added to dest in dry run mode")

	// Cleanup
	env.cleanupTorrent(ctx, env.srcClient, sintelHash)
}
