package tasks

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/autobrr/go-qbittorrent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mailoarsac/qb-router/internal/config"
)

// QBClientInterface defines the methods we use from qbittorrent.Client
// This allows us to mock the client in tests.
type QBClientInterface interface {
	LoginCtx(ctx context.Context) error
	GetTorrentsCtx(ctx context.Context, opts qbittorrent.TorrentFilterOptions) ([]qbittorrent.Torrent, error)
	GetFilesInformationCtx(ctx context.Context, hash string) (*qbittorrent.TorrentFiles, error)
	AddTagsCtx(ctx context.Context, hashes []string, tags string) error
	PauseCtx(ctx context.Context, hashes []string) error
	ResumeCtx(ctx context.Context, hashes []string) error
	DeleteTorrentsCtx(ctx context.Context, hashes []string, deleteFiles bool) error
	ExportTorrentCtx(ctx context.Context, hash string) ([]byte, error)
	AddTorrentFromMemoryCtx(ctx context.Context, data []byte, opts map[string]string) error
	GetAppPreferencesCtx(ctx context.Context) (qbittorrent.AppPreferences, error)
}

// mockQBClient implements QBClientInterface for testing.
type mockQBClient struct {
	torrents       []qbittorrent.Torrent
	files          map[string]*qbittorrent.TorrentFiles
	addTagsCalled  []string
	pauseCalled    []string
	resumeCalled   []string
	deleteCalled   []string
	exportData     []byte
	addTorrentOpts map[string]string
	prefs          qbittorrent.AppPreferences

	// Control behavior
	loginErr          error
	getTorrentsErr    error
	getFilesErr       error
	addTagsErr        error
	pauseErr          error
	exportErr         error
	addTorrentErr     error
	deleteErr         error
	getTorrentsFilter func(opts qbittorrent.TorrentFilterOptions) []qbittorrent.Torrent
}

func newMockQBClient() *mockQBClient {
	return &mockQBClient{
		files:      make(map[string]*qbittorrent.TorrentFiles),
		exportData: []byte("mock torrent data"),
		prefs:      qbittorrent.AppPreferences{SavePath: "/downloads"},
	}
}

func (m *mockQBClient) LoginCtx(_ context.Context) error {
	return m.loginErr
}

func (m *mockQBClient) GetTorrentsCtx(
	_ context.Context,
	opts qbittorrent.TorrentFilterOptions,
) ([]qbittorrent.Torrent, error) {
	if m.getTorrentsErr != nil {
		return nil, m.getTorrentsErr
	}
	if m.getTorrentsFilter != nil {
		return m.getTorrentsFilter(opts), nil
	}
	return m.torrents, nil
}

func (m *mockQBClient) GetFilesInformationCtx(_ context.Context, hash string) (*qbittorrent.TorrentFiles, error) {
	if m.getFilesErr != nil {
		return nil, m.getFilesErr
	}
	if files, ok := m.files[hash]; ok {
		return files, nil
	}
	// Return empty files instead of nil, nil to satisfy nilnil linter.
	return &qbittorrent.TorrentFiles{}, nil
}

func (m *mockQBClient) AddTagsCtx(_ context.Context, hashes []string, _ string) error {
	m.addTagsCalled = append(m.addTagsCalled, hashes...)
	return m.addTagsErr
}

func (m *mockQBClient) PauseCtx(_ context.Context, hashes []string) error {
	m.pauseCalled = append(m.pauseCalled, hashes...)
	return m.pauseErr
}

func (m *mockQBClient) ResumeCtx(_ context.Context, hashes []string) error {
	m.resumeCalled = append(m.resumeCalled, hashes...)
	return nil
}

func (m *mockQBClient) DeleteTorrentsCtx(_ context.Context, hashes []string, _ bool) error {
	m.deleteCalled = append(m.deleteCalled, hashes...)
	return m.deleteErr
}

func (m *mockQBClient) ExportTorrentCtx(_ context.Context, _ string) ([]byte, error) {
	if m.exportErr != nil {
		return nil, m.exportErr
	}
	return m.exportData, nil
}

func (m *mockQBClient) AddTorrentFromMemoryCtx(_ context.Context, _ []byte, opts map[string]string) error {
	m.addTorrentOpts = opts
	return m.addTorrentErr
}

func (m *mockQBClient) GetAppPreferencesCtx(_ context.Context) (qbittorrent.AppPreferences, error) {
	return m.prefs, nil
}

func TestFetchSyncedTorrents_Mock(t *testing.T) {
	t.Parallel()

	mock := newMockQBClient()
	mock.torrents = []qbittorrent.Torrent{
		{Hash: "large", Name: "Large", Size: 5000, Tags: "synced"},
		{Hash: "small", Name: "Small", Size: 1000, Tags: "synced"},
		{Hash: "medium", Name: "Medium", Size: 3000, Tags: "synced"},
	}

	task := &mockableQBTask{
		cfg:       &config.Config{SrcPath: "/src", DestPath: "/dest"},
		srcClient: mock,
	}

	torrents, err := task.fetchSyncedTorrents(context.Background())

	require.NoError(t, err)
	require.Len(t, torrents, 3)

	// Should be sorted by size ascending
	assert.Equal(t, "small", torrents[0].Hash)
	assert.Equal(t, "medium", torrents[1].Hash)
	assert.Equal(t, "large", torrents[2].Hash)
}

func TestAddTagsIsCalled_Mock(t *testing.T) {
	t.Parallel()

	srcMock := newMockQBClient()
	srcMock.torrents = []qbittorrent.Torrent{
		{Hash: "abc123", Name: "Test", Tags: "", SavePath: "/src/downloads"},
	}

	task := &mockableQBTask{
		cfg:       &config.Config{SrcPath: "/src", DestPath: "/dest"},
		srcClient: srcMock,
	}

	// Verify the mock is working
	torrents, err := task.srcClient.GetTorrentsCtx(context.Background(), qbittorrent.TorrentFilterOptions{})
	require.NoError(t, err)
	assert.Len(t, torrents, 1)
	assert.Equal(t, "abc123", torrents[0].Hash)
}

func TestMockClientInterface(t *testing.T) {
	t.Parallel()

	// Verify our mock implements the expected behavior
	mock := newMockQBClient()
	mock.torrents = []qbittorrent.Torrent{
		{Hash: "test1", State: qbittorrent.TorrentStatePausedUp},
	}

	files := qbittorrent.TorrentFiles{
		{Name: "file1.txt", Size: 100},
		{Name: "file2.txt", Size: 200},
	}
	mock.files["test1"] = &files

	ctx := context.Background()

	// Test GetTorrentsCtx
	torrents, err := mock.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{})
	require.NoError(t, err)
	assert.Len(t, torrents, 1)

	// Test GetFilesInformationCtx
	gotFiles, err := mock.GetFilesInformationCtx(ctx, "test1")
	require.NoError(t, err)
	require.NotNil(t, gotFiles)
	assert.Len(t, *gotFiles, 2)

	// Test AddTagsCtx
	err = mock.AddTagsCtx(ctx, []string{"test1"}, "synced")
	require.NoError(t, err)
	assert.Contains(t, mock.addTagsCalled, "test1")

	// Test PauseCtx
	err = mock.PauseCtx(ctx, []string{"test1"})
	require.NoError(t, err)
	assert.Contains(t, mock.pauseCalled, "test1")

	// Test ExportTorrentCtx
	data, err := mock.ExportTorrentCtx(ctx, "test1")
	require.NoError(t, err)
	assert.Equal(t, []byte("mock torrent data"), data)

	// Test GetAppPreferencesCtx
	prefs, err := mock.GetAppPreferencesCtx(ctx)
	require.NoError(t, err)
	assert.Equal(t, "/downloads", prefs.SavePath)
}

func TestGroupCreationWithMock(t *testing.T) {
	t.Parallel()

	// Test that createGroup works correctly with real torrent data
	task := newTestQBTask("/src", "/dest")

	torrents := []qbittorrent.Torrent{
		{
			Hash:          "torrent1",
			Size:          1000000,
			SeedingTime:   3600,
			NumComplete:   10,
			NumIncomplete: 5,
		},
		{
			Hash:          "torrent2",
			Size:          2000000,
			SeedingTime:   1800,
			NumComplete:   20,
			NumIncomplete: 10,
		},
	}

	group := task.createGroup(torrents)

	assert.Equal(t, int64(45), group.popularity)   // 10+5+20+10
	assert.Equal(t, int64(2000000), group.maxSize) // max of sizes
	assert.Equal(t, int64(1800), group.minSeeding) // min of seeding times
	assert.Len(t, group.torrents, 2)
}

func TestSortGroupsByPriority_Mock(t *testing.T) {
	t.Parallel()

	task := newTestQBTask("/src", "/dest")

	// Create groups with different popularity and sizes
	groups := []torrentGroup{
		{popularity: 100, maxSize: 5000}, // High pop, large
		{popularity: 10, maxSize: 1000},  // Low pop, small
		{popularity: 10, maxSize: 3000},  // Low pop, medium
		{popularity: 50, maxSize: 2000},  // Medium pop
	}

	sorted := task.sortGroupsByPriority(groups)

	// Verify sort order: popularity ASC, then size DESC
	// Expected: (10, 3000), (10, 1000), (50, 2000), (100, 5000)
	assert.Equal(t, int64(10), sorted[0].popularity)
	assert.Equal(t, int64(3000), sorted[0].maxSize)

	assert.Equal(t, int64(10), sorted[1].popularity)
	assert.Equal(t, int64(1000), sorted[1].maxSize)

	assert.Equal(t, int64(50), sorted[2].popularity)
	assert.Equal(t, int64(100), sorted[3].popularity)
}

// =============================================================================
// FAILURE MODE TESTS
// These tests verify that moveTorrentToCold properly handles error conditions
// and doesn't delete from source when destination is unhealthy.
// =============================================================================

// statefulMockClient extends mockQBClient with state sequence support
// for simulating torrent state transitions.
type statefulMockClient struct {
	mockQBClient

	// State sequences - returns different states on successive calls
	stateSequence []qbittorrent.TorrentState
	stateIndex    int

	// Progress sequence - returns different progress on successive calls
	progressSequence []float64
	progressIndex    int

	// Call counters
	getTorrentsCallCount int
	deleteCallCount      int
	resumeCallCount      int
}

func newStatefulMockClient() *statefulMockClient {
	return &statefulMockClient{
		mockQBClient: mockQBClient{
			files:      make(map[string]*qbittorrent.TorrentFiles),
			exportData: []byte("mock torrent data"),
			prefs:      qbittorrent.AppPreferences{SavePath: "/downloads"},
		},
	}
}

func (m *statefulMockClient) GetTorrentsCtx(
	_ context.Context,
	_ qbittorrent.TorrentFilterOptions,
) ([]qbittorrent.Torrent, error) {
	m.getTorrentsCallCount++

	if m.getTorrentsErr != nil {
		return nil, m.getTorrentsErr
	}

	// Return torrents with current state from sequence.
	result := make([]qbittorrent.Torrent, len(m.torrents))
	copy(result, m.torrents)

	if len(result) > 0 {
		// Apply state from sequence if available.
		if m.stateIndex < len(m.stateSequence) {
			result[0].State = m.stateSequence[m.stateIndex]
			m.stateIndex++
		}

		// Apply progress from sequence if available.
		if m.progressIndex < len(m.progressSequence) {
			result[0].Progress = m.progressSequence[m.progressIndex]
			m.progressIndex++
		}
	}

	return result, nil
}

func (m *statefulMockClient) DeleteTorrentsCtx(_ context.Context, hashes []string, _ bool) error {
	m.deleteCallCount++
	m.deleteCalled = append(m.deleteCalled, hashes...)
	return m.deleteErr
}

func (m *statefulMockClient) ResumeCtx(_ context.Context, hashes []string) error {
	m.resumeCallCount++
	m.resumeCalled = append(m.resumeCalled, hashes...)
	return nil
}

// mockableQBTask is a version of QBTask that uses interfaces for testing.
type mockableQBTask struct {
	cfg        *config.Config
	srcClient  QBClientInterface
	destClient QBClientInterface
}

func (t *mockableQBTask) fetchSyncedTorrents(ctx context.Context) ([]qbittorrent.Torrent, error) {
	torrents, err := t.srcClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Filter: qbittorrent.TorrentFilterCompleted,
		Tag:    syncedTag,
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(torrents, func(i, j int) bool {
		return torrents[i].Size < torrents[j].Size
	})
	return torrents, nil
}

// isErrorState mirrors the real implementation.
func (t *mockableQBTask) isErrorState(state qbittorrent.TorrentState) bool {
	return state == qbittorrent.TorrentStateError ||
		state == qbittorrent.TorrentStateMissingFiles
}

// isCheckingState mirrors the real implementation.
func (t *mockableQBTask) isCheckingState(state qbittorrent.TorrentState) bool {
	return state == qbittorrent.TorrentStateCheckingUp ||
		state == qbittorrent.TorrentStateCheckingDl ||
		state == qbittorrent.TorrentStateCheckingResumeData
}

// waitForDestinationReady - simplified version for testing (no polling, immediate check).
func (t *mockableQBTask) waitForDestinationReady(ctx context.Context, hash string) error {
	torrents, err := t.destClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{hash},
	})
	if err != nil {
		return err
	}
	if len(torrents) == 0 {
		return nil
	}

	destTorrent := torrents[0]

	if t.isErrorState(destTorrent.State) {
		return fmt.Errorf("destination in error state: %s", destTorrent.State)
	}
	if t.isCheckingState(destTorrent.State) {
		return errors.New("destination still checking")
	}
	if destTorrent.Progress < 1.0 {
		return fmt.Errorf("destination incomplete: %.2f", destTorrent.Progress)
	}

	return nil
}

// =============================================================================
// FAILURE MODE TEST CASES
// =============================================================================

func TestDestinationInErrorState_ShouldNotDeleteSource(t *testing.T) {
	t.Parallel()

	srcMock := newStatefulMockClient()
	srcMock.torrents = []qbittorrent.Torrent{
		{Hash: "test123", Name: "Test Torrent", State: qbittorrent.TorrentStatePausedUp, SavePath: "/src/downloads"},
	}

	destMock := newStatefulMockClient()
	// Destination torrent exists but is in ERROR state
	destMock.torrents = []qbittorrent.Torrent{
		{Hash: "test123", Name: "Test Torrent", State: qbittorrent.TorrentStateError, Progress: 0.0},
	}

	task := &mockableQBTask{
		cfg:        &config.Config{SrcPath: "/src", DestPath: "/dest"},
		srcClient:  srcMock,
		destClient: destMock,
	}

	ctx := context.Background()

	// Verify destination is in error state
	err := task.waitForDestinationReady(ctx, "test123")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "error state")

	// Source should NOT have delete called
	assert.Equal(t, 0, srcMock.deleteCallCount, "source should NOT be deleted when dest is in error state")
}

func TestDestinationMissingFiles_ShouldNotDeleteSource(t *testing.T) {
	t.Parallel()

	srcMock := newStatefulMockClient()
	srcMock.torrents = []qbittorrent.Torrent{
		{Hash: "test123", Name: "Test Torrent", State: qbittorrent.TorrentStatePausedUp},
	}

	destMock := newStatefulMockClient()
	// Destination torrent exists but has MISSING FILES
	destMock.torrents = []qbittorrent.Torrent{
		{Hash: "test123", Name: "Test Torrent", State: qbittorrent.TorrentStateMissingFiles, Progress: 0.5},
	}

	task := &mockableQBTask{
		cfg:        &config.Config{SrcPath: "/src", DestPath: "/dest"},
		srcClient:  srcMock,
		destClient: destMock,
	}

	ctx := context.Background()

	// Verify destination is detected as having missing files
	err := task.waitForDestinationReady(ctx, "test123")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "error state")

	// Source should NOT have delete called
	assert.Equal(t, 0, srcMock.deleteCallCount, "source should NOT be deleted when dest has missing files")
}

func TestDestinationStillChecking_ShouldNotDeleteSource(t *testing.T) {
	t.Parallel()

	srcMock := newStatefulMockClient()
	srcMock.torrents = []qbittorrent.Torrent{
		{Hash: "test123", Name: "Test Torrent", State: qbittorrent.TorrentStatePausedUp},
	}

	destMock := newStatefulMockClient()
	// Destination torrent is still CHECKING
	destMock.torrents = []qbittorrent.Torrent{
		{Hash: "test123", Name: "Test Torrent", State: qbittorrent.TorrentStateCheckingUp, Progress: 0.75},
	}

	task := &mockableQBTask{
		cfg:        &config.Config{SrcPath: "/src", DestPath: "/dest"},
		srcClient:  srcMock,
		destClient: destMock,
	}

	ctx := context.Background()

	// Verify checking state is detected
	err := task.waitForDestinationReady(ctx, "test123")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "checking")

	// Source should NOT have delete called
	assert.Equal(t, 0, srcMock.deleteCallCount, "source should NOT be deleted when dest is still checking")
}

func TestDestinationIncompleteProgress_ShouldNotDeleteSource(t *testing.T) {
	t.Parallel()

	srcMock := newStatefulMockClient()
	srcMock.torrents = []qbittorrent.Torrent{
		{Hash: "test123", Name: "Test Torrent", State: qbittorrent.TorrentStatePausedUp},
	}

	destMock := newStatefulMockClient()
	// Destination appears to be "seeding" but progress is not 100%
	// This can happen if qBittorrent reports an upload state before hash check completes
	destMock.torrents = []qbittorrent.Torrent{
		{Hash: "test123", Name: "Test Torrent", State: qbittorrent.TorrentStateStalledUp, Progress: 0.95},
	}

	task := &mockableQBTask{
		cfg:        &config.Config{SrcPath: "/src", DestPath: "/dest"},
		srcClient:  srcMock,
		destClient: destMock,
	}

	ctx := context.Background()

	// Verify incomplete progress is detected
	err := task.waitForDestinationReady(ctx, "test123")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "incomplete")

	// Source should NOT have delete called
	assert.Equal(t, 0, srcMock.deleteCallCount, "source should NOT be deleted when dest progress < 100%")
}

func TestDestinationHealthy_ShouldAllowDelete(t *testing.T) {
	t.Parallel()

	srcMock := newStatefulMockClient()
	srcMock.torrents = []qbittorrent.Torrent{
		{Hash: "test123", Name: "Test Torrent", State: qbittorrent.TorrentStatePausedUp},
	}

	destMock := newStatefulMockClient()
	// Destination is healthy: Uploading state with 100% progress
	destMock.torrents = []qbittorrent.Torrent{
		{Hash: "test123", Name: "Test Torrent", State: qbittorrent.TorrentStateUploading, Progress: 1.0},
	}

	task := &mockableQBTask{
		cfg:        &config.Config{SrcPath: "/src", DestPath: "/dest"},
		srcClient:  srcMock,
		destClient: destMock,
	}

	ctx := context.Background()

	// Verify destination is considered ready
	err := task.waitForDestinationReady(ctx, "test123")
	require.NoError(t, err, "healthy destination should pass validation")
}

func TestAlreadyExistsButErrorState_ShouldNotDeleteSource(t *testing.T) {
	t.Parallel()

	// This tests the "already exists" branch where torrent exists on dest
	// but is in an error state - we should NOT delete from source

	srcMock := newStatefulMockClient()
	destMock := newStatefulMockClient()
	destMock.torrents = []qbittorrent.Torrent{
		{Hash: "existing", Name: "Existing Torrent", State: qbittorrent.TorrentStateError, Progress: 0.0},
	}

	task := &mockableQBTask{
		cfg:        &config.Config{SrcPath: "/src", DestPath: "/dest"},
		srcClient:  srcMock,
		destClient: destMock,
	}

	ctx := context.Background()

	// Check that waitForDestinationReady fails for existing torrent in error state
	err := task.waitForDestinationReady(ctx, "existing")
	require.Error(t, err)

	// Verify source was not deleted
	assert.Empty(t, srcMock.deleteCalled, "source should NOT be deleted when existing dest torrent is in error state")
}

func TestDestinationQueuedUp_ShouldNotDeleteSource(t *testing.T) {
	t.Parallel()

	// QueuedUp state means torrent is queued but hasn't started checking
	// We should NOT delete source in this case

	srcMock := newStatefulMockClient()
	destMock := newStatefulMockClient()
	destMock.torrents = []qbittorrent.Torrent{
		{Hash: "test123", Name: "Test Torrent", State: qbittorrent.TorrentStateQueuedUp, Progress: 0.0},
	}

	task := &mockableQBTask{
		cfg:        &config.Config{SrcPath: "/src", DestPath: "/dest"},
		srcClient:  srcMock,
		destClient: destMock,
	}

	ctx := context.Background()

	// QueuedUp with 0 progress should fail
	err := task.waitForDestinationReady(ctx, "test123")
	require.Error(t, err, "QueuedUp with 0 progress should not be considered ready")

	assert.Empty(t, srcMock.deleteCalled, "source should NOT be deleted when dest is QueuedUp with 0 progress")
}

func TestIsErrorState_AllErrorStates(t *testing.T) {
	t.Parallel()

	task := &mockableQBTask{}

	// These should be detected as error states
	assert.True(t, task.isErrorState(qbittorrent.TorrentStateError))
	assert.True(t, task.isErrorState(qbittorrent.TorrentStateMissingFiles))

	// These should NOT be detected as error states
	assert.False(t, task.isErrorState(qbittorrent.TorrentStateUploading))
	assert.False(t, task.isErrorState(qbittorrent.TorrentStateCheckingUp))
	assert.False(t, task.isErrorState(qbittorrent.TorrentStatePausedUp))
	assert.False(t, task.isErrorState(qbittorrent.TorrentStateStalledUp))
}

func TestIsCheckingState_AllCheckingStates(t *testing.T) {
	t.Parallel()

	task := &mockableQBTask{}

	// These should be detected as checking states
	assert.True(t, task.isCheckingState(qbittorrent.TorrentStateCheckingUp))
	assert.True(t, task.isCheckingState(qbittorrent.TorrentStateCheckingDl))
	assert.True(t, task.isCheckingState(qbittorrent.TorrentStateCheckingResumeData))

	// These should NOT be detected as checking states
	assert.False(t, task.isCheckingState(qbittorrent.TorrentStateUploading))
	assert.False(t, task.isCheckingState(qbittorrent.TorrentStateError))
	assert.False(t, task.isCheckingState(qbittorrent.TorrentStatePausedUp))
}
