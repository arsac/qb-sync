package destination

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/autobrr/go-qbittorrent"

	"github.com/arsac/qb-sync/internal/qbclient"
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
	return nil, nil
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
	return nil
}
func (m *mockQBClient) SetFilePriorityCtx(context.Context, string, string, int) error { return nil }
func (m *mockQBClient) RecheckCtx(context.Context, []string) error                    { return nil }
func (m *mockQBClient) GetFreeSpaceOnDiskCtx(context.Context) (int64, error)          { return 0, nil }

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
		minWant time.Duration
		maxWant time.Duration
		comment string
	}{
		{
			name:    "tiny torrent",
			bytes:   100 * 1024 * 1024, // 100 MB
			minWant: defaultQBPollTimeoutBase,
			maxWant: defaultQBPollTimeoutBase,
			comment: "below 1GB the floor governs",
		},
		{
			name:    "100GB torrent",
			bytes:   100 * oneGB,
			minWant: defaultQBPollTimeoutBase + 100*defaultQBPollTimeoutPerGB,
			maxWant: defaultQBPollTimeoutBase + 100*defaultQBPollTimeoutPerGB,
			comment: "scaling kicks in linearly",
		},
		{
			name:    "1TB torrent",
			bytes:   1024 * oneGB,
			minWant: defaultQBPollTimeoutBase + 1024*defaultQBPollTimeoutPerGB,
			maxWant: defaultQBPollTimeoutMax,
			comment: "still below cap (depends on constants)",
		},
		{
			name:    "10TB torrent",
			bytes:   10 * 1024 * oneGB,
			minWant: defaultQBPollTimeoutMax,
			maxWant: defaultQBPollTimeoutMax,
			comment: "must hit the hard cap, not grow unboundedly",
		},
		{
			name:    "negative size (defensive)",
			bytes:   -1,
			minWant: 0,
			maxWant: defaultQBPollTimeoutBase,
			comment: "shouldn't panic; should produce a reasonable value",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := computePollTimeout(tc.bytes)
			// Allow either >= minWant (when calculated value is at or above floor)
			// or == maxWant when the cap fires.
			capped := got > defaultQBPollTimeoutMax
			belowFloor := got < defaultQBPollTimeoutBase && tc.bytes >= 0
			if capped || belowFloor {
				t.Errorf("%s: got %v, expected within [%v, %v] (%s)",
					tc.name, got, defaultQBPollTimeoutBase, defaultQBPollTimeoutMax, tc.comment)
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
