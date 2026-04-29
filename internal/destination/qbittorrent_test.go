package destination

import (
	"context"
	"errors"
	"testing"

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
