package destination

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/autobrr/go-qbittorrent"

	"github.com/arsac/qb-sync/internal/qbclient"
	"github.com/arsac/qb-sync/internal/utils"
	pb "github.com/arsac/qb-sync/proto"
)

var _ qbclient.Client = (*mockQBClient)(nil)

// mockQBClient implements qbclient.Client for StartTorrent tests.
type mockQBClient struct {
	loginErr    error
	torrents    []qbittorrent.Torrent
	getTorrErr  error
	resumeErr   error
	addTagsErr  error
	resumeHash  []string
	addTagsArgs struct {
		hashes []string
		tags   string
	}

	deleteCalled      bool
	deleteHashes      []string
	deleteDeleteFiles bool
	deleteErr         error

	stopCalled bool
	stopHashes []string
	stopErr    error

	categories           map[string]qbittorrent.Category
	getCategoriesErr     error
	createCategoryCalled bool
	createCategoryName   string
	createCategoryPath   string
	createCategoryErr    error

	upLimitCalled bool
	upLimitHashes []string
	upLimit       int64
	upLimitErr    error

	dlLimitCalled bool
	dlLimitHashes []string
	dlLimit       int64
	dlLimitErr    error

	setFilePriorityCalls int
	setFilePriorityIDs   string
	setFilePriority      int
	setFilePriorityErr   error

	// filesByCall lets tests script qB's view evolving over successive
	// GetFilesInformationCtx calls — needed to simulate the silent-drop-then-
	// persist sequence the verify-and-retry loop is built to handle. Calls past
	// the last entry reuse it so tests only specify what changes.
	filesByCall  []qbittorrent.TorrentFiles
	getFilesCall int

	// recheckCalled records that RecheckCtx fired and (when set) swaps the
	// torrent list to torrentsAfterRecheck so subsequent GetTorrentsCtx polls
	// see the post-recheck state. Used by post-add error-state recovery tests.
	recheckCalled        bool
	recheckHashes        []string
	torrentsAfterRecheck []qbittorrent.Torrent

	// torrentsAfterAdd, when non-nil, swaps the torrent list on the next
	// AddTorrentFromMemoryCtx call. Lets tests model "torrent not present →
	// add → torrent now in some state" without manual mid-test mutation.
	addCalled        bool
	torrentsAfterAdd []qbittorrent.Torrent
}

func (m *mockQBClient) LoginCtx(context.Context) error { return m.loginErr }

func (m *mockQBClient) GetTorrentsCtx(
	_ context.Context,
	_ qbittorrent.TorrentFilterOptions,
) ([]qbittorrent.Torrent, error) {
	return m.torrents, m.getTorrErr
}
func (m *mockQBClient) ResumeCtx(_ context.Context, hashes []string) error {
	m.resumeHash = hashes
	return m.resumeErr
}
func (m *mockQBClient) AddTagsCtx(_ context.Context, hashes []string, tags string) error {
	m.addTagsArgs.hashes = hashes
	m.addTagsArgs.tags = tags
	return m.addTagsErr
}

func (m *mockQBClient) RemoveTagsCtx(context.Context, []string, string) error { return nil }

// Unused methods — satisfy the interface.
func (m *mockQBClient) GetAppPreferencesCtx(context.Context) (qbittorrent.AppPreferences, error) {
	return qbittorrent.AppPreferences{}, nil
}
func (m *mockQBClient) GetTorrentPieceStatesCtx(context.Context, string) ([]qbittorrent.PieceState, error) {
	return nil, nil
}
func (m *mockQBClient) GetTorrentPieceHashesCtx(context.Context, string) ([]string, error) {
	return nil, nil
}
func (m *mockQBClient) GetTorrentPropertiesCtx(context.Context, string) (qbittorrent.TorrentProperties, error) {
	return qbittorrent.TorrentProperties{}, nil
}
func (m *mockQBClient) GetFilesInformationCtx(context.Context, string) (*qbittorrent.TorrentFiles, error) {
	if len(m.filesByCall) == 0 {
		return &qbittorrent.TorrentFiles{}, nil
	}
	idx := min(m.getFilesCall, len(m.filesByCall)-1)
	m.getFilesCall++
	files := m.filesByCall[idx]
	return &files, nil
}
func (m *mockQBClient) ExportTorrentCtx(context.Context, string) ([]byte, error) { return nil, nil }
func (m *mockQBClient) DeleteTorrentsCtx(_ context.Context, hashes []string, deleteFiles bool) error {
	m.deleteCalled = true
	m.deleteHashes = hashes
	m.deleteDeleteFiles = deleteFiles
	if m.deleteErr != nil {
		return m.deleteErr
	}
	// Actually remove deleted hashes so subsequent GetTorrentsCtx reflects the deletion.
	deleteSet := make(map[string]struct{}, len(hashes))
	for _, h := range hashes {
		deleteSet[h] = struct{}{}
	}
	filtered := m.torrents[:0]
	for _, t := range m.torrents {
		if _, ok := deleteSet[t.Hash]; !ok {
			filtered = append(filtered, t)
		}
	}
	m.torrents = filtered
	return nil
}
func (m *mockQBClient) StopCtx(_ context.Context, hashes []string) error {
	m.stopCalled = true
	m.stopHashes = hashes
	return m.stopErr
}

func (m *mockQBClient) GetCategoriesCtx(context.Context) (map[string]qbittorrent.Category, error) {
	return m.categories, m.getCategoriesErr
}
func (m *mockQBClient) SetTorrentUploadLimitCtx(_ context.Context, hashes []string, limit int64) error {
	m.upLimitCalled = true
	m.upLimitHashes = hashes
	m.upLimit = limit
	return m.upLimitErr
}
func (m *mockQBClient) SetTorrentDownloadLimitCtx(_ context.Context, hashes []string, limit int64) error {
	m.dlLimitCalled = true
	m.dlLimitHashes = hashes
	m.dlLimit = limit
	return m.dlLimitErr
}
func (m *mockQBClient) CreateCategoryCtx(_ context.Context, category, path string) error {
	m.createCategoryCalled = true
	m.createCategoryName = category
	m.createCategoryPath = path
	if m.categories == nil {
		m.categories = map[string]qbittorrent.Category{}
	}
	m.categories[category] = qbittorrent.Category{Name: category, SavePath: path}
	return m.createCategoryErr
}
func (m *mockQBClient) AddTorrentFromMemoryCtx(context.Context, []byte, map[string]string) error {
	m.addCalled = true
	if m.torrentsAfterAdd != nil {
		m.torrents = m.torrentsAfterAdd
	}
	return nil
}
func (m *mockQBClient) SetFilePriorityCtx(_ context.Context, _, ids string, priority int) error {
	m.setFilePriorityCalls++
	m.setFilePriorityIDs = ids
	m.setFilePriority = priority
	return m.setFilePriorityErr
}
func (m *mockQBClient) RecheckCtx(_ context.Context, hashes []string) error {
	m.recheckCalled = true
	m.recheckHashes = hashes
	if m.torrentsAfterRecheck != nil {
		m.torrents = m.torrentsAfterRecheck
	}
	return nil
}
func (m *mockQBClient) GetFreeSpaceOnDiskCtx(context.Context) (int64, error) { return 0, nil }

func newTestServerWithQB(t *testing.T, mock *mockQBClient) *Server {
	t.Helper()
	tmpDir := t.TempDir()
	return &Server{
		config:   ServerConfig{BasePath: tmpDir},
		logger:   testLogger(t),
		store:    newTorrentStore(tmpDir, testLogger(t)),
		qbClient: mock,
	}
}

func TestStartTorrent(t *testing.T) {
	t.Parallel()

	t.Run("resumes torrent and applies tag", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{
			torrents: []qbittorrent.Torrent{{Hash: "abc123"}},
		}
		s := newTestServerWithQB(t, mock)

		resp, err := s.StartTorrent(context.Background(), &pb.StartTorrentRequest{
			TorrentHash: "abc123",
			Tag:         "source-removed",
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !resp.GetSuccess() {
			t.Fatalf("expected success, got error: %s", resp.GetError())
		}
		if len(mock.resumeHash) != 1 || mock.resumeHash[0] != "abc123" {
			t.Fatalf("expected ResumeCtx called with [abc123], got %v", mock.resumeHash)
		}
		if mock.addTagsArgs.tags != "source-removed" {
			t.Fatalf("expected AddTagsCtx called with tag 'source-removed', got %q", mock.addTagsArgs.tags)
		}
		if len(mock.addTagsArgs.hashes) != 1 || mock.addTagsArgs.hashes[0] != "abc123" {
			t.Fatalf("expected AddTagsCtx called with hash [abc123], got %v", mock.addTagsArgs.hashes)
		}
	})

	t.Run("skips tag when empty", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{
			torrents: []qbittorrent.Torrent{{Hash: "abc123"}},
		}
		s := newTestServerWithQB(t, mock)

		resp, err := s.StartTorrent(context.Background(), &pb.StartTorrentRequest{
			TorrentHash: "abc123",
			Tag:         "",
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !resp.GetSuccess() {
			t.Fatalf("expected success, got error: %s", resp.GetError())
		}
		if mock.addTagsArgs.tags != "" {
			t.Fatalf("expected AddTagsCtx NOT called, but was called with tag %q", mock.addTagsArgs.tags)
		}
	})

	t.Run("tag failure is non-fatal", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{
			torrents:   []qbittorrent.Torrent{{Hash: "abc123"}},
			addTagsErr: errors.New("qBittorrent API error"),
		}
		s := newTestServerWithQB(t, mock)

		resp, err := s.StartTorrent(context.Background(), &pb.StartTorrentRequest{
			TorrentHash: "abc123",
			Tag:         "source-removed",
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !resp.GetSuccess() {
			t.Fatalf("expected success despite tag failure, got error: %s", resp.GetError())
		}
		// Tag was attempted but failed — still returns success
		if mock.addTagsArgs.tags != "source-removed" {
			t.Fatalf("expected AddTagsCtx attempted, got tag %q", mock.addTagsArgs.tags)
		}
	})

	t.Run("returns error when qbClient is nil", func(t *testing.T) {
		t.Parallel()
		logger := testLogger(t)
		s := &Server{
			config: ServerConfig{BasePath: "/tmp"},
			logger: logger,
			store:  newTorrentStore("/tmp", logger),
		}

		resp, err := s.StartTorrent(context.Background(), &pb.StartTorrentRequest{
			TorrentHash: "abc123",
			Tag:         "source-removed",
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.GetSuccess() {
			t.Fatal("expected failure when qbClient is nil")
		}
		if resp.GetError() != "destination qBittorrent not configured" {
			t.Fatalf("unexpected error message: %s", resp.GetError())
		}
	})

	t.Run("returns error when torrent not found", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{
			torrents: nil, // empty — torrent not found
		}
		s := newTestServerWithQB(t, mock)

		resp, err := s.StartTorrent(context.Background(), &pb.StartTorrentRequest{
			TorrentHash: "missing",
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.GetSuccess() {
			t.Fatal("expected failure when torrent not found")
		}
		if resp.GetError() != "torrent does not exist on destination qBittorrent" {
			t.Fatalf("unexpected error message: %s", resp.GetError())
		}
	})

	t.Run("returns error when resume fails", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{
			torrents:  []qbittorrent.Torrent{{Hash: "abc123"}},
			resumeErr: errors.New("resume failed"),
		}
		s := newTestServerWithQB(t, mock)

		resp, err := s.StartTorrent(context.Background(), &pb.StartTorrentRequest{
			TorrentHash: "abc123",
			Tag:         "source-removed",
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.GetSuccess() {
			t.Fatal("expected failure when resume fails")
		}
		if mock.addTagsArgs.tags != "" {
			t.Fatal("AddTagsCtx should not be called when resume fails")
		}
	})
}

// TestStartTorrent_ClearsRateLimits pins the autobrr-pattern fix that closes
// the brief announce-before-stop window during AddTorrent. Pre-fix, qB v5
// could occasionally announce on the brief gap between AddTorrent (with
// stopped=true) and our explicit StopCtx. Post-fix, AddTorrent sets
// upLimit/dlLimit to 0; StartTorrent clears the limits (-1) so transfer
// resumes at full speed.
func TestStartTorrent_ClearsRateLimits(t *testing.T) {
	t.Parallel()
	mock := &mockQBClient{
		torrents: []qbittorrent.Torrent{{Hash: "abc"}},
	}
	s := newTestServerWithQB(t, mock)

	resp, err := s.StartTorrent(context.Background(), &pb.StartTorrentRequest{
		TorrentHash: "abc",
	})
	if err != nil || !resp.GetSuccess() {
		t.Fatalf("StartTorrent failed: err=%v resp=%v", err, resp)
	}

	if !mock.upLimitCalled || mock.upLimit != -1 {
		t.Errorf("upload limit must be cleared (-1), got called=%v limit=%d",
			mock.upLimitCalled, mock.upLimit)
	}
	if !mock.dlLimitCalled || mock.dlLimit != -1 {
		t.Errorf("download limit must be cleared (-1), got called=%v limit=%d",
			mock.dlLimitCalled, mock.dlLimit)
	}
}

// TestEnsureCategoryExists pins the autobrr-known fix for qBittorrent silently
// dropping the category field of AddTorrent when the category doesn't already
// exist on the destination instance. Without ensureCategoryExists the user's
// "movies" category on source would silently never apply on destination,
// breaking the directory-layout invariant.
func TestEnsureCategoryExists(t *testing.T) {
	t.Parallel()

	t.Run("creates category when missing", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{
			categories: map[string]qbittorrent.Category{}, // empty
		}
		s := newTestServerWithQB(t, mock)
		s.ensureCategoryExists(context.Background(), "movies")

		if !mock.createCategoryCalled {
			t.Fatal("CreateCategoryCtx must be called when the category doesn't exist")
		}
		if mock.createCategoryName != "movies" {
			t.Errorf("created name = %q, want %q", mock.createCategoryName, "movies")
		}
	})

	t.Run("skips creation when category already exists", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{
			categories: map[string]qbittorrent.Category{
				"movies": {Name: "movies"},
			},
		}
		s := newTestServerWithQB(t, mock)
		s.ensureCategoryExists(context.Background(), "movies")

		if mock.createCategoryCalled {
			t.Fatal("CreateCategoryCtx must NOT be called when the category already exists")
		}
	})

	t.Run("GetCategories error is best-effort: logs but doesn't fail", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{
			getCategoriesErr: errors.New("transient API error"),
		}
		s := newTestServerWithQB(t, mock)
		// Should not panic or return.
		s.ensureCategoryExists(context.Background(), "movies")

		if mock.createCategoryCalled {
			t.Error("CreateCategoryCtx must not be called when GetCategories fails")
		}
	})

	t.Run("CreateCategory error is best-effort: logs but doesn't fail", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{
			categories:        map[string]qbittorrent.Category{},
			createCategoryErr: errors.New("category creation forbidden"),
		}
		s := newTestServerWithQB(t, mock)
		s.ensureCategoryExists(context.Background(), "movies")

		if !mock.createCategoryCalled {
			t.Fatal("CreateCategoryCtx should still be attempted")
		}
	})
}

// TestComputePollTimeout pins the size-scaling contract for the qB readiness
// poll timeout. The historical 5-minute fixed budget failed on multi-TB
// torrents (qB recheck on spinning rust takes hours); the scaled timeout is
// (base + per-GB * size_in_GB), capped to a hard maximum.
func TestComputePollTimeout(t *testing.T) {
	t.Parallel()

	const oneGB = int64(1024 * 1024 * 1024)

	cases := []struct {
		name    string
		bytes   int64
		minWant time.Duration // lower bound (inclusive) for the returned timeout
		maxWant time.Duration // upper bound (inclusive) for the returned timeout
	}{
		{
			name:    "tiny torrent: floor governs below 1GB",
			bytes:   100 * 1024 * 1024,
			minWant: defaultQBPollTimeoutBase,
			maxWant: defaultQBPollTimeoutBase,
		},
		{
			name:    "100GB torrent: linear scaling kicks in",
			bytes:   100 * oneGB,
			minWant: defaultQBPollTimeoutBase + 100*defaultQBPollTimeoutPerGB,
			maxWant: defaultQBPollTimeoutBase + 100*defaultQBPollTimeoutPerGB,
		},
		{
			// 1TB at 1min/GB pre-cap is 17h, well past the 6h max — the cap fires.
			name:    "1TB torrent: scaling exceeds cap, gets clamped",
			bytes:   1024 * oneGB,
			minWant: defaultQBPollTimeoutMax,
			maxWant: defaultQBPollTimeoutMax,
		},
		{
			name:    "10TB torrent: hits the hard cap, not unbounded",
			bytes:   10 * 1024 * oneGB,
			minWant: defaultQBPollTimeoutMax,
			maxWant: defaultQBPollTimeoutMax,
		},
		{
			name:    "negative size (defensive): doesn't panic, stays at/under base",
			bytes:   -1,
			minWant: 0,
			maxWant: defaultQBPollTimeoutBase,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := computePollTimeout(tc.bytes)
			if got < tc.minWant || got > tc.maxWant {
				t.Errorf("computePollTimeout(%d) = %v, want within [%v, %v]",
					tc.bytes, got, tc.minWant, tc.maxWant)
			}
		})
	}
}

// TestCheckTorrentInQB_OnlyAcceptsReadyStateAt100 pins the contract introduced
// in commit b35a5da. The pre-fix code returned qbCheckComplete for any
// non-error state at progress=1.0, including pausedDL/stoppedDL. With partial
// selection on qB v5 (or after a user manually deletes unselected file dirs),
// progress can hit 1.0 in a download-side state — and trusting it would let
// source mark the torrent synced and delete its data with nothing actually
// seeding. Post-fix, only seeding-side states (uploading, stalledUp, forcedUp,
// pausedUp, stoppedUp) at 100% return COMPLETE.
func TestCheckTorrentInQB_OnlyAcceptsReadyStateAt100(t *testing.T) {
	t.Parallel()

	cases := []struct {
		state   qbittorrent.TorrentState
		want    qbCheckResult
		comment string
	}{
		// Seeding-side states at 100% — must return COMPLETE.
		{qbittorrent.TorrentStateUploading, qbCheckComplete, "actively seeding"},
		{qbittorrent.TorrentStateStalledUp, qbCheckComplete, "seeding without peers"},
		{qbittorrent.TorrentStateForcedUp, qbCheckComplete, "force-uploading"},
		{qbittorrent.TorrentStatePausedUp, qbCheckComplete, "paused while seeding (qB v4)"},
		{qbittorrent.TorrentStateStoppedUp, qbCheckComplete, "stopped while seeding (qB v5)"},

		// Download-side states at 100% — must NOT return COMPLETE even though
		// progress is 1.0. This is the data-loss path the fix closes: with
		// partial selection or external file deletion, qB can report 1.0
		// progress in these states.
		{qbittorrent.TorrentStateStoppedDl, qbCheckNotFound, "stopped mid-download must not be trusted as complete"},
		{qbittorrent.TorrentStatePausedDl, qbCheckNotFound, "paused mid-download must not be trusted as complete"},
		{qbittorrent.TorrentStateDownloading, qbCheckNotFound, "actively downloading"},
		{qbittorrent.TorrentStateStalledDl, qbCheckNotFound, "stalled while downloading"},
		{qbittorrent.TorrentStateQueuedDl, qbCheckNotFound, "queued for download"},

		// Error states — must NOT return COMPLETE.
		{qbittorrent.TorrentStateError, qbCheckNotFound, "error state"},
		{qbittorrent.TorrentStateMissingFiles, qbCheckNotFound, "files missing on disk"},
	}

	for _, tc := range cases {
		t.Run(string(tc.state)+"/"+tc.comment, func(t *testing.T) {
			t.Parallel()
			mock := &mockQBClient{
				torrents: []qbittorrent.Torrent{{
					Hash:     "abc",
					State:    tc.state,
					Progress: 1.0,
				}},
			}
			s := newTestServerWithQB(t, mock)
			got := s.checkTorrentInQB(context.Background(), "abc")
			if got != tc.want {
				t.Errorf("state=%s progress=1.0: got %v, want %v (%s)",
					tc.state, got, tc.want, tc.comment)
			}
		})
	}
}

// TestCheckTorrentInQB_RejectsLessThan100Progress documents that even
// seeding-side states at <100% progress are not treated as COMPLETE — the
// progress floor is load-bearing.
func TestCheckTorrentInQB_RejectsLessThan100Progress(t *testing.T) {
	t.Parallel()
	mock := &mockQBClient{
		torrents: []qbittorrent.Torrent{{
			Hash:     "abc",
			State:    qbittorrent.TorrentStateUploading,
			Progress: 0.99,
		}},
	}
	s := newTestServerWithQB(t, mock)
	if got := s.checkTorrentInQB(context.Background(), "abc"); got != qbCheckNotFound {
		t.Errorf("state=uploading progress=0.99: got %v, want qbCheckNotFound", got)
	}
}

// TestCheckTorrentInQB_VerifyingDuringChecking maps qB's checking states to
// qbCheckVerifying so the source side waits instead of writing concurrently.
func TestCheckTorrentInQB_VerifyingDuringChecking(t *testing.T) {
	t.Parallel()
	checkingStates := []qbittorrent.TorrentState{
		qbittorrent.TorrentStateCheckingUp,
		qbittorrent.TorrentStateCheckingDl,
		qbittorrent.TorrentStateCheckingResumeData,
	}
	for _, state := range checkingStates {
		t.Run(string(state), func(t *testing.T) {
			t.Parallel()
			mock := &mockQBClient{
				torrents: []qbittorrent.Torrent{{
					Hash:     "abc",
					State:    state,
					Progress: 1.0,
				}},
			}
			s := newTestServerWithQB(t, mock)
			if got := s.checkTorrentInQB(context.Background(), "abc"); got != qbCheckVerifying {
				t.Errorf("state=%s: got %v, want qbCheckVerifying", state, got)
			}
		})
	}
}

// TestWaitForTorrentReady_MissingFilesIsTerminal regression-tests the
// finalize-hang fix: when qB parks the torrent in missingFiles (priority-0
// update lost a race, files genuinely absent, etc.) the poll loop must fail
// fast instead of treating it as transient and waiting out the multi-hour
// budget. The source converts the resulting error into FINALIZE_ERROR_INCOMPLETE
// and re-syncs.
func TestWaitForTorrentReady_MissingFilesIsTerminal(t *testing.T) {
	t.Parallel()

	mock := &mockQBClient{
		torrents: []qbittorrent.Torrent{{
			Hash:  "abc123",
			State: qbittorrent.TorrentStateMissingFiles,
		}},
	}
	s := newTestServerWithQB(t, mock)
	s.config.QB = &QBConfig{
		PollInterval: 10 * time.Millisecond,
		PollTimeout:  5 * time.Second,
	}

	start := time.Now()
	state, err := s.waitForTorrentReady(context.Background(), "abc123", 0)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("waitForTorrentReady must return an error for missingFiles, not poll until timeout")
	}
	if state != qbittorrent.TorrentStateMissingFiles {
		t.Errorf("returned state = %v, want missingFiles", state)
	}
	// Must fail well before the 5s budget; the first poll should catch it.
	if elapsed > 1*time.Second {
		t.Errorf("waitForTorrentReady took %v — should fail-fast on missingFiles, not wait out the budget", elapsed)
	}
}

// TestAddAndVerifyTorrent_StopsFoundReadyTorrent regression-tests the autobrr
// Tier-1 fix: when addAndVerifyTorrent finds the torrent already in qB at 100%
// in a ready state (the path hit during recovery from a destination crash mid-
// finalization), it must stop the torrent before returning. Without this, the
// post-restart finalization completes with the torrent already running on
// destination qB while source still believes itself canonical seeder — a
// dual-seeding window against the tracker.
func TestAddAndVerifyTorrent_StopsFoundReadyTorrent(t *testing.T) {
	t.Parallel()

	t.Run("stops torrent when found in stoppedUp at 100%", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{
			torrents: []qbittorrent.Torrent{{
				Hash:     "abc123",
				State:    qbittorrent.TorrentStateStoppedUp,
				Progress: 1.0,
			}},
		}
		s := newTestServerWithQB(t, mock)

		state := &serverTorrentState{}
		_, err := s.addAndVerifyTorrent(context.Background(), "abc123", state,
			&pb.FinalizeTorrentRequest{TorrentHash: "abc123"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !mock.stopCalled {
			t.Fatal("StopCtx must be called even when torrent is found in a ready state — " +
				"otherwise dual-seeding against source after dest crash recovery")
		}
		if len(mock.stopHashes) != 1 || mock.stopHashes[0] != "abc123" {
			t.Errorf("StopCtx hashes = %v, want [abc123]", mock.stopHashes)
		}
	})

	t.Run("stops torrent when found in stalledUp at 100%", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{
			torrents: []qbittorrent.Torrent{{
				Hash:     "abc123",
				State:    qbittorrent.TorrentStateStalledUp,
				Progress: 1.0,
			}},
		}
		s := newTestServerWithQB(t, mock)

		state := &serverTorrentState{}
		_, err := s.addAndVerifyTorrent(context.Background(), "abc123", state,
			&pb.FinalizeTorrentRequest{TorrentHash: "abc123"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !mock.stopCalled {
			t.Fatal("StopCtx must be called for an actively-uploading found torrent")
		}
	})

	t.Run("propagates stop failure as a warn, returns success", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{
			torrents: []qbittorrent.Torrent{{
				Hash:     "abc123",
				State:    qbittorrent.TorrentStateStoppedUp,
				Progress: 1.0,
			}},
			stopErr: errors.New("transient qB error"),
		}
		s := newTestServerWithQB(t, mock)

		state := &serverTorrentState{}
		finalState, err := s.addAndVerifyTorrent(context.Background(), "abc123", state,
			&pb.FinalizeTorrentRequest{TorrentHash: "abc123"})

		if err != nil {
			t.Fatalf("stop failure must not propagate as error (best-effort): %v", err)
		}
		if finalState != qbittorrent.TorrentStateStoppedUp {
			t.Errorf("finalState = %v, want stoppedUp", finalState)
		}
		if !mock.stopCalled {
			t.Fatal("StopCtx must be attempted even though it returned an error")
		}
	})
}

// TestApplyAndVerifyDeselectedPriorities regression-tests the verify-and-retry
// flow that catches qB's silent-drop quirk on freshly-added stopped torrents.
// SetFilePriorityCtx returns 200 OK whether qB persists the change or not, so
// without verification a partial-selection torrent stays in stoppedDl with
// default priorities and the downstream wait times out.
func TestApplyAndVerifyDeselectedPriorities(t *testing.T) {
	t.Parallel()

	// files: [selected, deselected, selected, deselected]
	state := &serverTorrentState{
		torrentMeta: torrentMeta{files: []*serverFileInfo{
			{selected: true},
			{selected: false},
			{selected: true},
			{selected: false},
		}},
	}

	allDeselectedAtZero := qbittorrent.TorrentFiles{
		{Index: 0, Priority: 1},
		{Index: 1, Priority: 0},
		{Index: 2, Priority: 1},
		{Index: 3, Priority: 0},
	}
	allDeselectedAtDefault := qbittorrent.TorrentFiles{
		{Index: 0, Priority: 1},
		{Index: 1, Priority: 1},
		{Index: 2, Priority: 1},
		{Index: 3, Priority: 1},
	}

	t.Run("priorities apply on first try, resume called once", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{
			filesByCall: []qbittorrent.TorrentFiles{allDeselectedAtZero},
		}
		s := newTestServerWithQB(t, mock)

		err := s.applyAndVerifyDeselectedPriorities(context.Background(), "abc123", state.files)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if mock.setFilePriorityCalls != 1 {
			t.Errorf("SetFilePriority calls = %d, want 1", mock.setFilePriorityCalls)
		}
		if mock.setFilePriorityIDs != "1|3" {
			t.Errorf("setFilePriorityIDs = %q, want %q", mock.setFilePriorityIDs, "1|3")
		}
		if len(mock.resumeHash) != 1 || mock.resumeHash[0] != "abc123" {
			t.Errorf("Resume must be called once with the torrent hash; got %v", mock.resumeHash)
		}
	})

	t.Run("priorities silently dropped on first try, applied on second", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{
			filesByCall: []qbittorrent.TorrentFiles{
				allDeselectedAtDefault, // first call: qB silently dropped
				allDeselectedAtZero,    // second call: persisted
			},
		}
		s := newTestServerWithQB(t, mock)
		s.config.QB = &QBConfig{
			PriorityVerifyInterval: 5 * time.Millisecond,
			PriorityVerifyTimeout:  500 * time.Millisecond,
		}

		err := s.applyAndVerifyDeselectedPriorities(context.Background(), "abc123", state.files)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if mock.setFilePriorityCalls < 2 {
			t.Errorf("expected at least 2 SetFilePriority calls (retry); got %d", mock.setFilePriorityCalls)
		}
		if len(mock.resumeHash) != 1 {
			t.Errorf("Resume must be called exactly once after verification succeeds; got %v", mock.resumeHash)
		}
	})

	t.Run("priorities never persist, returns error and no resume", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{
			filesByCall: []qbittorrent.TorrentFiles{allDeselectedAtDefault},
		}
		s := newTestServerWithQB(t, mock)
		s.config.QB = &QBConfig{
			PriorityVerifyInterval: 5 * time.Millisecond,
			PriorityVerifyTimeout:  50 * time.Millisecond,
		}

		err := s.applyAndVerifyDeselectedPriorities(context.Background(), "abc123", state.files)
		if err == nil {
			t.Fatal("expected error when priorities never persist; got nil")
		}
		if !strings.Contains(err.Error(), "never persisted") {
			t.Errorf("error should mention persistence failure; got %q", err.Error())
		}
		if len(mock.resumeHash) != 0 {
			t.Error("Resume must NOT be called when verification never succeeds")
		}
	})
}

// TestNeedsPartialSelectionRecovery covers the predicate that gates
// re-application of priorities on existing-but-stuck torrents. The threshold
// (Progress < 0.001 in stoppedDl/pausedDl) is what protects mid-recheck and
// operator-paused torrents from being disrupted — Skeptic findings #1, #2, #5.
func TestNeedsPartialSelectionRecovery(t *testing.T) {
	t.Parallel()

	withDeselected := []*serverFileInfo{
		{selected: true},
		{selected: false},
	}
	allSelected := []*serverFileInfo{
		{selected: true},
		{selected: true},
	}

	tests := []struct {
		name  string
		found bool
		t     *qbittorrent.Torrent
		files []*serverFileInfo
		want  bool
	}{
		{
			name:  "fresh add with deselected files: apply",
			t:     nil,
			files: withDeselected,
			want:  true,
		},
		{
			name:  "fresh add with no deselected files: skip",
			t:     nil,
			files: allSelected,
			want:  false,
		},
		{
			name:  "existing stoppedDl at 0% with deselected files: recover",
			t:     &qbittorrent.Torrent{State: qbittorrent.TorrentStateStoppedDl, Progress: 0},
			files: withDeselected,
			want:  true,
		},
		{
			name:  "existing pausedDl at 0% with deselected files: recover",
			t:     &qbittorrent.Torrent{State: qbittorrent.TorrentStatePausedDl, Progress: 0},
			files: withDeselected,
			want:  true,
		},
		{
			// Skeptic #1 regression: don't disrupt mid-recheck torrents.
			name:  "existing stoppedDl at 50%: skip (mid-recheck)",
			t:     &qbittorrent.Torrent{State: qbittorrent.TorrentStateStoppedDl, Progress: 0.5},
			files: withDeselected,
			want:  false,
		},
		{
			// Skeptic #2 regression: don't disrupt actively checking torrents.
			name:  "existing checking at 0%: skip",
			t:     &qbittorrent.Torrent{State: qbittorrent.TorrentStateCheckingDl, Progress: 0},
			files: withDeselected,
			want:  false,
		},
		{
			// Guardian #6 add: full-selection stuck torrents shouldn't trigger recovery.
			name:  "existing stoppedDl at 0% with all files selected: skip",
			t:     &qbittorrent.Torrent{State: qbittorrent.TorrentStateStoppedDl, Progress: 0},
			files: allSelected,
			want:  false,
		},
		{
			name:  "existing seeding at 100%: skip (already done)",
			t:     &qbittorrent.Torrent{State: qbittorrent.TorrentStateUploading, Progress: 1.0},
			files: withDeselected,
			want:  false,
		},
		{
			// Boundary: exactly at the 0.001 threshold is excluded (qB has begun progressing).
			name:  "existing stoppedDl at 0.001%: skip (FP threshold boundary)",
			t:     &qbittorrent.Torrent{State: qbittorrent.TorrentStateStoppedDl, Progress: 0.001},
			files: withDeselected,
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := needsPartialSelectionRecovery(tt.t, tt.files)
			if got != tt.want {
				t.Errorf("needsPartialSelectionRecovery() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestAddAndVerifyTorrent_RecoveryErrorPathStopsAndWraps regression-tests the
// Arbiter precondition: when applyAndVerifyDeselectedPriorities returns an
// error from the recovery branch, addAndVerifyTorrent must call
// stopTorrentBestEffort and return the wrapped error so the source-side cap
// can mark the torrent sync-failed.
func TestAddAndVerifyTorrent_RecoveryErrorPathStopsAndWraps(t *testing.T) {
	t.Parallel()

	// Existing torrent stuck at 0% in stoppedDl with deselected files —
	// triggers recovery. GetFilesInformation returns wrong priorities forever,
	// so applyAndVerifyDeselectedPriorities fails after the budget.
	mock := &mockQBClient{
		torrents: []qbittorrent.Torrent{{
			Hash:     "abc123",
			State:    qbittorrent.TorrentStateStoppedDl,
			Progress: 0,
		}},
		filesByCall: []qbittorrent.TorrentFiles{
			{
				{Index: 0, Priority: 1},
				{Index: 1, Priority: 1}, // deselected file stuck at default
			},
		},
	}
	s := newTestServerWithQB(t, mock)
	s.config.QB = &QBConfig{
		PriorityVerifyInterval: 5 * time.Millisecond,
		PriorityVerifyTimeout:  50 * time.Millisecond,
	}

	state := &serverTorrentState{
		torrentMeta: torrentMeta{files: []*serverFileInfo{
			{selected: true},
			{selected: false},
		}},
	}

	_, err := s.addAndVerifyTorrent(context.Background(), "abc123", state,
		&pb.FinalizeTorrentRequest{TorrentHash: "abc123"})

	if err == nil {
		t.Fatal("expected error from recovery failure, got nil")
	}
	if !strings.Contains(err.Error(), "applying deselected priorities") {
		t.Errorf("error must wrap with 'applying deselected priorities' so source can correlate; got %q", err.Error())
	}
	if !mock.stopCalled {
		t.Error("stopTorrentBestEffort must run on recovery failure to keep the dest qB torrent quiescent")
	}
}

// TestAddAndVerifyTorrent_AutoRechecksOnErrorState regression-tests the NFS
// attribute-cache recovery path: when destination qB's mount serves a stale
// directory listing at AddTorrent time and the torrent lands in an error
// state, addAndVerifyTorrent must call RecheckCtx and re-poll once before
// surfacing the failure. The user-visible workaround is "click Force recheck";
// without this automation, every NFS-cache hiccup needs manual intervention.
func TestAddAndVerifyTorrent_AutoRechecksOnErrorState(t *testing.T) {
	t.Parallel()

	t.Run("recheck recovers torrent into ready state", func(t *testing.T) {
		t.Parallel()
		// Pre-add: torrent not in qB. Add: lands in error state (the NFS-cache
		// symptom we automate around). Recheck: forces qB to re-walk savepath,
		// finds correct files, transitions to uploading.
		mock := &mockQBClient{
			torrentsAfterAdd: []qbittorrent.Torrent{{
				Hash:  "abc123",
				State: qbittorrent.TorrentStateError,
			}},
			torrentsAfterRecheck: []qbittorrent.Torrent{{
				Hash:     "abc123",
				State:    qbittorrent.TorrentStateUploading,
				Progress: 1.0,
			}},
		}
		s := newTestServerWithQB(t, mock)
		s.config.QB = &QBConfig{
			PollInterval: 5 * time.Millisecond,
			PollTimeout:  2 * time.Second,
		}

		state := &serverTorrentState{torrentMeta: torrentMeta{
			files: []*serverFileInfo{{selected: true}},
		}, torrentFile: []byte("fake")}
		finalState, err := s.addAndVerifyTorrent(context.Background(), "abc123", state,
			&pb.FinalizeTorrentRequest{TorrentHash: "abc123"})
		if err != nil {
			t.Fatalf("expected recheck to recover, got error: %v", err)
		}
		if !mock.recheckCalled {
			t.Fatal("RecheckCtx must be called when initial wait lands in error state")
		}
		if len(mock.recheckHashes) != 1 || mock.recheckHashes[0] != "abc123" {
			t.Errorf("RecheckCtx hashes = %v, want [abc123]", mock.recheckHashes)
		}
		if finalState != qbittorrent.TorrentStateUploading {
			t.Errorf("final state = %v, want uploading after recheck", finalState)
		}
	})

	t.Run("retry is bounded — second error state surfaces failure", func(t *testing.T) {
		t.Parallel()
		// Recheck doesn't clear the error: real failure, must not loop forever.
		mock := &mockQBClient{
			torrentsAfterAdd: []qbittorrent.Torrent{{
				Hash:  "abc123",
				State: qbittorrent.TorrentStateError,
			}},
			torrentsAfterRecheck: []qbittorrent.Torrent{{
				Hash:  "abc123",
				State: qbittorrent.TorrentStateError,
			}},
		}
		s := newTestServerWithQB(t, mock)
		s.config.QB = &QBConfig{
			PollInterval: 5 * time.Millisecond,
			PollTimeout:  2 * time.Second,
		}

		state := &serverTorrentState{torrentMeta: torrentMeta{
			files: []*serverFileInfo{{selected: true}},
		}, torrentFile: []byte("fake")}
		_, err := s.addAndVerifyTorrent(context.Background(), "abc123", state,
			&pb.FinalizeTorrentRequest{TorrentHash: "abc123"})
		if err == nil {
			t.Fatal("expected error when recheck doesn't clear error state — must not loop forever")
		}
		if !mock.recheckCalled {
			t.Error("RecheckCtx should still have been attempted once")
		}
		if !mock.stopCalled {
			t.Error("stopTorrentBestEffort must run before returning to keep the torrent quiescent")
		}
	})
}

// A torrent destination qB already holds in a download-side stopped state is
// the "data was already on the destination" case: something else (an *arr,
// cross-seed automation, a hand add, or an earlier attempt that stopped the
// torrent mid-recheck) added it pointing at existing files. checkTorrentInQB
// reports it as absent, AddTorrent is skipped because qB has it, and nothing
// else in the qB stage moves it - so without a recheck the wait times out
// identically on every retry until the source tags the torrent sync-failed.
func TestAddAndVerifyTorrent_RechecksParkedTorrent(t *testing.T) {
	t.Parallel()

	parked := []struct {
		name     string
		state    qbittorrent.TorrentState
		progress float64
	}{
		{"stoppedDL at 100%", qbittorrent.TorrentStateStoppedDl, 1.0},
		{"stoppedDL at 0%", qbittorrent.TorrentStateStoppedDl, 0.0},
		{"pausedDL at 100%", qbittorrent.TorrentStatePausedDl, 1.0},
		{"queuedDL", qbittorrent.TorrentStateQueuedDl, 0.5},
	}

	for _, tc := range parked {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mock := &mockQBClient{
				torrents: []qbittorrent.Torrent{
					{Hash: "abc123", State: tc.state, Progress: tc.progress},
				},
				torrentsAfterRecheck: []qbittorrent.Torrent{
					{Hash: "abc123", State: qbittorrent.TorrentStateStoppedUp, Progress: 1.0},
				},
			}
			s := newTestServerWithQB(t, mock)
			s.config.QB = &QBConfig{PollInterval: 5 * time.Millisecond, PollTimeout: 2 * time.Second}

			state := &serverTorrentState{torrentMeta: torrentMeta{
				files: []*serverFileInfo{{selected: true}},
			}, torrentFile: []byte("fake")}

			finalState, err := s.addAndVerifyTorrent(context.Background(), "abc123", state,
				&pb.FinalizeTorrentRequest{TorrentHash: "abc123"})
			if err != nil {
				t.Fatalf("expected recheck to recover parked torrent, got: %v", err)
			}
			if !mock.recheckCalled {
				t.Fatal("RecheckCtx must be called for a torrent parked in a non-ready state")
			}
			if mock.addCalled {
				t.Error("AddTorrent must not be issued for a torrent qB already has")
			}
			if finalState != qbittorrent.TorrentStateStoppedUp {
				t.Errorf("final state = %v, want stoppedUP after recheck", finalState)
			}
			if !mock.stopCalled {
				t.Error("torrent must be left stopped: source is canonical seeder until handoff")
			}
		})
	}

	t.Run("a torrent we added is left checking", func(t *testing.T) {
		t.Parallel()
		// We added it with stopped=true, so the check completes into a stopped
		// state and cannot seed. Stopping it mid-pass is what parks it in
		// stoppedDL and sends the next attempt into the same dead end.
		mock := &mockQBClient{
			torrents: nil, // qB does not have it: we add it
			torrentsAfterAdd: []qbittorrent.Torrent{
				{Hash: "abc123", State: qbittorrent.TorrentStateCheckingUp, Progress: 0.4},
			},
		}
		s := newTestServerWithQB(t, mock)
		s.config.QB = &QBConfig{PollInterval: 5 * time.Millisecond, PollTimeout: 50 * time.Millisecond}

		state := &serverTorrentState{torrentMeta: torrentMeta{
			files: []*serverFileInfo{{selected: true}},
		}, torrentFile: []byte("fake")}

		finalState, err := s.addAndVerifyTorrent(context.Background(), "abc123", state,
			&pb.FinalizeTorrentRequest{TorrentHash: "abc123"})
		if !mock.addCalled {
			t.Fatal("expected the torrent to be added")
		}
		if !isBusyWaitError(finalState, err) {
			t.Errorf("still-checking at budget expiry should classify as BUSY, got state=%v err=%v",
				finalState, err)
		}
		if mock.stopCalled {
			t.Error("must not stop a torrent we added mid-check")
		}
	})

	t.Run("a torrent qB already held is stopped even mid-check", func(t *testing.T) {
		t.Parallel()
		// We never saw how this one was configured. Its check can complete into
		// uploading behind our back, and the source is the canonical seeder
		// until handoff, so the dual-seed window has to be closed.
		mock := &mockQBClient{
			torrents: []qbittorrent.Torrent{
				{Hash: "abc123", State: qbittorrent.TorrentStateCheckingUp, Progress: 0.4},
			},
		}
		s := newTestServerWithQB(t, mock)
		s.config.QB = &QBConfig{PollInterval: 5 * time.Millisecond, PollTimeout: 50 * time.Millisecond}

		state := &serverTorrentState{torrentMeta: torrentMeta{
			files: []*serverFileInfo{{selected: true}},
		}, torrentFile: []byte("fake")}

		_, _ = s.addAndVerifyTorrent(context.Background(), "abc123", state,
			&pb.FinalizeTorrentRequest{TorrentHash: "abc123"})
		if mock.recheckCalled {
			t.Fatal("must not recheck a torrent qB is already checking")
		}
		if !mock.stopCalled {
			t.Error("a torrent qB already held must be stopped: we cannot vouch for how it was added")
		}
	})

	t.Run("ready torrent skips recheck", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{
			torrents: []qbittorrent.Torrent{
				{Hash: "abc123", State: qbittorrent.TorrentStateStoppedUp, Progress: 1.0},
			},
		}
		s := newTestServerWithQB(t, mock)

		state := &serverTorrentState{torrentMeta: torrentMeta{
			files: []*serverFileInfo{{selected: true}},
		}, torrentFile: []byte("fake")}

		if _, err := s.addAndVerifyTorrent(context.Background(), "abc123", state,
			&pb.FinalizeTorrentRequest{TorrentHash: "abc123"}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if mock.recheckCalled {
			t.Error("a seeding-ready torrent needs no recheck")
		}
		if !mock.stopCalled {
			t.Error("fast path must still leave the torrent stopped")
		}
	})
}

func TestQBStageTimeout(t *testing.T) {
	const gb = int64(1024 * 1024 * 1024)

	tests := []struct {
		name      string
		totalSize int64
		qbCfg     *QBConfig
		want      time.Duration
	}{
		{
			name:      "small torrent uses base floor plus per-GB, doubled, plus margin",
			totalSize: 1 * gb,
			want:      2*(defaultQBPollTimeoutBase+1*defaultQBPollTimeoutPerGB) + defaultQBStageTimeoutMargin,
		},
		{
			name:      "huge torrent capped at 2x max plus margin",
			totalSize: 1000 * gb,
			want:      2*defaultQBPollTimeoutMax + defaultQBStageTimeoutMargin,
		},
		{
			name:      "explicit PollTimeout override is doubled plus margin",
			totalSize: 1000 * gb,
			qbCfg:     &QBConfig{PollTimeout: 10 * time.Minute},
			want:      2*(10*time.Minute) + defaultQBStageTimeoutMargin,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{config: ServerConfig{QB: tt.qbCfg}}
			if got := s.qbStageTimeout(tt.totalSize); got != tt.want {
				t.Errorf("qbStageTimeout(%d) = %v, want %v", tt.totalSize, got, tt.want)
			}
		})
	}
}

func TestIsBusyWaitError(t *testing.T) {
	tests := []struct {
		name       string
		finalState qbittorrent.TorrentState
		err        error
		want       bool
	}{
		{"timeout while checking is busy", qbittorrent.TorrentStateCheckingUp, utils.ErrTimeout, true},
		{"deadline while checking is busy", qbittorrent.TorrentStateCheckingDl, context.DeadlineExceeded, true},
		{"timeout in error state is not busy", qbittorrent.TorrentStateMissingFiles, utils.ErrTimeout, false},
		{
			"error-state failure is not busy",
			qbittorrent.TorrentStateError,
			errors.New("torrent in error state: error"),
			false,
		},
		{"timeout in stalled state is not busy", qbittorrent.TorrentStateStalledDl, utils.ErrTimeout, false},
		{"nil error is not busy", qbittorrent.TorrentStateCheckingUp, nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isBusyWaitError(tt.finalState, tt.err); got != tt.want {
				t.Errorf("isBusyWaitError(%q, %v) = %v, want %v", tt.finalState, tt.err, got, tt.want)
			}
		})
	}
}

func TestQBFinalizeConcurrencyValidation(t *testing.T) {
	base := ServerConfig{BasePath: "/tmp/x", ListenAddr: ":1"}

	cfg := base
	cfg.QBFinalizeConcurrency = 9
	if err := cfg.Validate(); err == nil {
		t.Error("concurrency above the cap must fail validation")
	}

	cfg = base
	cfg.QBFinalizeConcurrency = -1
	if err := cfg.Validate(); err == nil {
		t.Error("negative concurrency must fail validation")
	}

	cfg = base
	cfg.QBFinalizeConcurrency = 0
	if err := cfg.Validate(); err != nil {
		t.Errorf("zero (default) must validate: %v", err)
	}
	if got := cfg.GetQBFinalizeConcurrency(); got != 1 {
		t.Errorf("zero must normalize to 1, got %d", got)
	}

	// Defensive clamp: ServerConfig.Validate is not on the startup path, so an
	// out-of-range value must never reach the semaphore.
	cfg = base
	cfg.QBFinalizeConcurrency = 99
	if got := cfg.GetQBFinalizeConcurrency(); got != maxQBFinalizeConcurrency {
		t.Errorf("out-of-range value must clamp to %d, got %d", maxQBFinalizeConcurrency, got)
	}
}

// TestIsTorrentParked pins the allow-list. "Not checking and not ready" would
// also catch states that advance on their own, and throwing a recheck at those
// re-hashes a torrent for nothing.
func TestIsTorrentParked(t *testing.T) {
	t.Parallel()

	parked := []qbittorrent.TorrentState{
		qbittorrent.TorrentStateStoppedDl,
		qbittorrent.TorrentStatePausedDl,
		qbittorrent.TorrentStateQueuedDl,
	}
	selfAdvancing := []qbittorrent.TorrentState{
		qbittorrent.TorrentStateDownloading,
		qbittorrent.TorrentStateStalledDl,
		qbittorrent.TorrentStateMetaDl,
		qbittorrent.TorrentStateAllocating,
		qbittorrent.TorrentStateMoving,
		qbittorrent.TorrentStateCheckingDl,
		qbittorrent.TorrentStateStoppedUp,
	}

	for _, st := range parked {
		if !isTorrentParked(&qbittorrent.Torrent{State: st}) {
			t.Errorf("%s should be parked: nothing in the qB stage moves it", st)
		}
	}
	for _, st := range selfAdvancing {
		if isTorrentParked(&qbittorrent.Torrent{State: st}) {
			t.Errorf("%s advances on its own or is terminal, a recheck would be wasted work", st)
		}
	}
	if isTorrentParked(nil) {
		t.Error("a torrent qB does not have is added, not rechecked")
	}
}

// TestClaimParkedRecheck pins the latch, which is keyed on the parked state
// rather than set once. Repeating a recheck that already answered re-hashes the
// whole torrent on every finalize attempt, but a torrent that has moved to a
// different parked state has not been answered yet - and stopping a started
// torrent mid-check leaves it in a state its earlier recheck never saw.
func TestClaimParkedRecheck(t *testing.T) {
	t.Parallel()

	state := &serverTorrentState{}
	if !state.claimParkedRecheck(qbittorrent.TorrentStateQueuedDl) {
		t.Fatal("the first attempt owns the recheck")
	}
	if state.claimParkedRecheck(qbittorrent.TorrentStateQueuedDl) {
		t.Error("the same parked state was already answered, re-issuing re-hashes for nothing")
	}
	if !state.claimParkedRecheck(qbittorrent.TorrentStateStoppedDl) {
		t.Error("a different parked state has not been answered and deserves its own recheck")
	}
}

// TestAddAndVerifyTorrent_SlowRecheckSurvivesRetry pins the interaction between
// the parked recheck and the mid-check stop, which together once reopened the
// wedge they were each written to close.
//
// A torrent qB already held in stoppedDL gets a recheck. If that recheck outlasts
// the qB-stage budget, the pass ends mid-check. Stopping it there returns it to
// stoppedDL, and if the recheck were latched outright the next attempt would find
// it parked with no recheck left to issue, wait for a state nothing moves, and
// time out until the source's guard tagged it sync-failed.
func TestAddAndVerifyTorrent_SlowRecheckSurvivesRetry(t *testing.T) {
	t.Parallel()

	newAttempt := func(t *testing.T, mock *mockQBClient) *Server {
		t.Helper()
		s := newTestServerWithQB(t, mock)
		s.config.QB = &QBConfig{PollInterval: 5 * time.Millisecond, PollTimeout: 50 * time.Millisecond}
		return s
	}
	newState := func() *serverTorrentState {
		return &serverTorrentState{
			torrentMeta: torrentMeta{files: []*serverFileInfo{{selected: true}}},
			torrentFile: []byte("fake"),
		}
	}

	t.Run("a stopped torrent is left checking so the recheck can finish", func(t *testing.T) {
		t.Parallel()
		mock := &mockQBClient{
			torrents: []qbittorrent.Torrent{
				{Hash: "abc123", State: qbittorrent.TorrentStateStoppedDl, Progress: 1.0},
			},
			// The recheck starts but does not finish inside the budget.
			torrentsAfterRecheck: []qbittorrent.Torrent{
				{Hash: "abc123", State: qbittorrent.TorrentStateCheckingDl, Progress: 1.0},
			},
		}
		s := newAttempt(t, mock)

		_, _ = s.addAndVerifyTorrent(context.Background(), "abc123", newState(),
			&pb.FinalizeTorrentRequest{TorrentHash: "abc123"})

		if !mock.recheckCalled {
			t.Fatal("a parked torrent should have been rechecked")
		}
		if mock.stopCalled {
			t.Error("stopping mid-check returns it to stoppedDL and wastes the recheck")
		}
	})

	t.Run("the latch does not lock out a torrent that moved parked states", func(t *testing.T) {
		t.Parallel()
		state := newState()

		// Attempt 1: queuedDL. Started, so it is stopped after the check, which
		// leaves it in stoppedDL - a state its recheck never covered.
		queued := &mockQBClient{
			torrents: []qbittorrent.Torrent{
				{Hash: "abc123", State: qbittorrent.TorrentStateQueuedDl, Progress: 1.0},
			},
			torrentsAfterRecheck: []qbittorrent.Torrent{
				{Hash: "abc123", State: qbittorrent.TorrentStateCheckingDl, Progress: 1.0},
			},
		}
		_, _ = newAttempt(t, queued).addAndVerifyTorrent(context.Background(), "abc123", state,
			&pb.FinalizeTorrentRequest{TorrentHash: "abc123"})
		if !queued.recheckCalled {
			t.Fatal("queuedDL is parked and should have been rechecked")
		}
		if !queued.stopCalled {
			t.Error("a started torrent must not be left checking: it completes into uploading")
		}

		// Attempt 2, same state object: now stoppedDL.
		stopped := &mockQBClient{
			torrents: []qbittorrent.Torrent{
				{Hash: "abc123", State: qbittorrent.TorrentStateStoppedDl, Progress: 1.0},
			},
			torrentsAfterRecheck: []qbittorrent.Torrent{
				{Hash: "abc123", State: qbittorrent.TorrentStateStoppedUp, Progress: 1.0},
			},
		}
		_, err := newAttempt(t, stopped).addAndVerifyTorrent(context.Background(), "abc123", state,
			&pb.FinalizeTorrentRequest{TorrentHash: "abc123"})

		if !stopped.recheckCalled {
			t.Fatal("the torrent is parked in a state no earlier recheck answered: it must get one")
		}
		if err != nil {
			t.Errorf("the retry should recover the torrent, got %v", err)
		}
	})

	t.Run("the same parked state is not rechecked twice", func(t *testing.T) {
		t.Parallel()
		state := newState()

		for attempt := range 2 {
			mock := &mockQBClient{
				torrents: []qbittorrent.Torrent{
					{Hash: "abc123", State: qbittorrent.TorrentStateStoppedDl, Progress: 0.5},
				},
			}
			_, _ = newAttempt(t, mock).addAndVerifyTorrent(context.Background(), "abc123", state,
				&pb.FinalizeTorrentRequest{TorrentHash: "abc123"})

			if attempt == 1 && mock.recheckCalled {
				t.Error("qB already answered for this state, re-hashing the torrent again buys nothing")
			}
		}
	})
}

// TestSafeToLeaveChecking pins the arbiter of the dual-seed window. Leaving a
// torrent mid-check is only safe when it cannot seed on completion, and the
// entry state alone does not answer that: applyAndVerifyDeselectedPriorities
// resumes the torrent once priorities verify, so a partial-selection add we
// observed as stopped completes its check into uploading.
func TestSafeToLeaveChecking(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		existing *qbittorrent.Torrent
		resumed  bool
		want     bool
	}{
		{
			name:     "we added it, still stopped",
			existing: nil,
			want:     true,
		},
		{
			name:     "qB held it stopped",
			existing: &qbittorrent.Torrent{State: qbittorrent.TorrentStateStoppedDl},
			want:     true,
		},
		{
			name:     "qB held it paused",
			existing: &qbittorrent.Torrent{State: qbittorrent.TorrentStatePausedDl},
			want:     true,
		},
		{
			name:     "queuedDL is started and completes into uploading",
			existing: &qbittorrent.Torrent{State: qbittorrent.TorrentStateQueuedDl},
			want:     false,
		},
		{
			name:     "we added it but resumed it for partial selection",
			existing: nil,
			resumed:  true,
			want:     false,
		},
		{
			name:     "qB held it stopped but we resumed it",
			existing: &qbittorrent.Torrent{State: qbittorrent.TorrentStateStoppedDl},
			resumed:  true,
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := safeToLeaveChecking(tt.existing, tt.resumed); got != tt.want {
				t.Errorf("safeToLeaveChecking() = %v, want %v", got, tt.want)
			}
		})
	}
}
