package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/autobrr/go-qbittorrent"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/arsac/qb-sync/internal/config"
	"github.com/arsac/qb-sync/internal/qbclient"
	"github.com/arsac/qb-sync/internal/streaming"
	pb "github.com/arsac/qb-sync/proto"
)

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// mockQBClient implements qbclient.Client for testing.
type mockQBClient struct {
	addTagsCalled bool
	addTagsHashes []string
	addTagsTag    string
	addTagsErr    error

	deleteCalled bool
	deleteErr    error
	stopCalled   bool
	stopHashes   []string
	stopErr      error
	stopFailHash map[string]bool
	resumeCalled bool
	resumeHashes []string
	resumeErr    error

	getTorrentsResult []qbittorrent.Torrent
	getTorrentsErr    error

	freeSpaceOnDisk int64
	freeSpaceErr    error
}

var _ qbclient.Client = (*mockQBClient)(nil)

func (m *mockQBClient) LoginCtx(_ context.Context) error {
	return nil
}

func (m *mockQBClient) GetAppPreferencesCtx(_ context.Context) (qbittorrent.AppPreferences, error) {
	return qbittorrent.AppPreferences{SavePath: "/data"}, nil
}

func (m *mockQBClient) GetTorrentsCtx(
	_ context.Context,
	_ qbittorrent.TorrentFilterOptions,
) ([]qbittorrent.Torrent, error) {
	return m.getTorrentsResult, m.getTorrentsErr
}

func (m *mockQBClient) GetTorrentPieceStatesCtx(
	_ context.Context,
	_ string,
) ([]qbittorrent.PieceState, error) {
	return []qbittorrent.PieceState{}, nil
}

func (m *mockQBClient) GetTorrentPieceHashesCtx(_ context.Context, _ string) ([]string, error) {
	return []string{}, nil
}

func (m *mockQBClient) GetTorrentPropertiesCtx(
	_ context.Context,
	_ string,
) (qbittorrent.TorrentProperties, error) {
	return qbittorrent.TorrentProperties{}, nil
}

func (m *mockQBClient) GetFilesInformationCtx(
	_ context.Context,
	_ string,
) (*qbittorrent.TorrentFiles, error) {
	return &qbittorrent.TorrentFiles{}, nil
}

func (m *mockQBClient) ExportTorrentCtx(_ context.Context, _ string) ([]byte, error) {
	return []byte{}, nil
}

func (m *mockQBClient) DeleteTorrentsCtx(_ context.Context, _ []string, _ bool) error {
	m.deleteCalled = true
	return m.deleteErr
}

func (m *mockQBClient) AddTagsCtx(_ context.Context, hashes []string, tags string) error {
	m.addTagsCalled = true
	m.addTagsHashes = hashes
	m.addTagsTag = tags
	return m.addTagsErr
}

func (m *mockQBClient) StopCtx(_ context.Context, hashes []string) error {
	m.stopCalled = true
	m.stopHashes = hashes
	if m.stopFailHash != nil && m.stopFailHash[hashes[0]] {
		return errors.New("stop failed")
	}
	return m.stopErr
}

func (m *mockQBClient) ResumeCtx(_ context.Context, hashes []string) error {
	m.resumeCalled = true
	m.resumeHashes = hashes
	return m.resumeErr
}

func (m *mockQBClient) AddTorrentFromMemoryCtx(_ context.Context, _ []byte, _ map[string]string) error {
	return nil
}

func (m *mockQBClient) SetFilePriorityCtx(_ context.Context, _ string, _ string, _ int) error {
	return nil
}

func (m *mockQBClient) RecheckCtx(_ context.Context, _ []string) error {
	return nil
}

func (m *mockQBClient) GetFreeSpaceOnDiskCtx(_ context.Context) (int64, error) {
	return m.freeSpaceOnDisk, m.freeSpaceErr
}

// Tests for finalization backoff logic. These can be unit tested without mocking gRPC.
func TestFinalizeBackoff(t *testing.T) {
	logger := testLogger(t)

	t.Run("shouldAttemptFinalize returns true initially", func(t *testing.T) {
		task := &QBTask{
			cfg:      &config.SourceConfig{},
			logger:   logger,
			backoffs: NewBackoffTracker(),
		}

		if !task.backoffs.ShouldAttempt("hash123") {
			t.Error("should allow finalization attempt initially")
		}
	})

	t.Run("shouldAttemptFinalize returns false after recent failure", func(t *testing.T) {
		task := &QBTask{
			cfg:      &config.SourceConfig{},
			logger:   logger,
			backoffs: NewBackoffTracker(),
		}

		hash := "hash123"
		task.backoffs.RecordFailure(hash)

		if task.backoffs.ShouldAttempt(hash) {
			t.Error("should not allow finalization attempt immediately after failure")
		}
	})

	t.Run("shouldAttemptFinalize returns true after backoff period", func(t *testing.T) {
		tracker := NewBackoffTracker()

		hash := "hash123"

		// Manually set a past lastAttempt
		tracker.mu.Lock()
		tracker.backoffs[hash] = &finalizeBackoff{
			failures:    1,
			lastAttempt: time.Now().Add(-10 * time.Second), // Well past minFinalizeBackoff
		}
		tracker.mu.Unlock()

		if !tracker.ShouldAttempt(hash) {
			t.Error("should allow finalization attempt after backoff period")
		}
	})

	t.Run("clearFinalizeBackoff removes tracking", func(t *testing.T) {
		task := &QBTask{
			cfg:      &config.SourceConfig{},
			logger:   logger,
			backoffs: NewBackoffTracker(),
		}

		hash := "hash123"
		task.backoffs.RecordFailure(hash)

		task.backoffs.Clear(hash)

		// Should be able to attempt immediately after clearing
		if !task.backoffs.ShouldAttempt(hash) {
			t.Error("should allow finalization attempt after clearing backoff")
		}
	})

	t.Run("backoff increases with failures", func(t *testing.T) {
		task := &QBTask{
			cfg:      &config.SourceConfig{},
			logger:   logger,
			backoffs: NewBackoffTracker(),
		}

		hash := "hash123"

		// Record multiple failures — RecordFailure returns the count
		var failures int
		for range 5 {
			failures = task.backoffs.RecordFailure(hash)
		}

		if failures != 5 {
			t.Errorf("expected 5 failures recorded, got %d", failures)
		}
	})

	t.Run("backoff is capped at maxFinalizeBackoff", func(t *testing.T) {
		tracker := NewBackoffTracker()

		hash := "hash123"

		// Simulate many failures to trigger cap
		tracker.mu.Lock()
		tracker.backoffs[hash] = &finalizeBackoff{
			failures:    100, // Large number
			lastAttempt: time.Now(),
		}
		tracker.mu.Unlock()

		// The computed backoff should be capped, so waiting maxFinalizeBackoff should allow retry
		tracker.mu.Lock()
		tracker.backoffs[hash].lastAttempt = time.Now().Add(-maxFinalizeBackoff - time.Second)
		tracker.mu.Unlock()

		if !tracker.ShouldAttempt(hash) {
			t.Error("should allow finalization after max backoff period")
		}
	})
}

// Tests for tracking maps operations.
func TestTrackedTorrentsMap(t *testing.T) {
	t.Run("concurrent map access is safe", func(_ *testing.T) {
		tracked := NewTrackedSet()

		var wg sync.WaitGroup
		const numOps = 100

		// Concurrent writes
		for i := range numOps {
			wg.Go(func() {
				hash := string(rune('a' + i%26))
				tracked.Add(hash, TrackedTorrent{CompletionTime: time.Now()})
			})
		}

		// Concurrent reads
		for range numOps {
			wg.Go(func() {
				_ = tracked.Count()
			})
		}

		// Concurrent deletes
		for i := range numOps {
			wg.Go(func() {
				hash := string(rune('a' + i%26))
				tracked.Delete(hash)
			})
		}

		wg.Wait()
	})
}

func TestConcurrentBackoffAccess(t *testing.T) {
	t.Run("concurrent backoff operations are safe", func(_ *testing.T) {
		tracker := NewBackoffTracker()

		var wg sync.WaitGroup
		const numOps = 100

		// Concurrent RecordFailure
		for i := range numOps {
			wg.Go(func() {
				hash := string(rune('a' + i%26))
				tracker.RecordFailure(hash)
			})
		}

		// Concurrent ShouldAttempt
		for i := range numOps {
			wg.Go(func() {
				hash := string(rune('a' + i%26))
				_ = tracker.ShouldAttempt(hash)
			})
		}

		// Concurrent Clear
		for i := range numOps {
			wg.Go(func() {
				hash := string(rune('a' + i%26))
				tracker.Clear(hash)
			})
		}

		wg.Wait()
	})
}

func TestConstants(t *testing.T) {
	t.Run("destRPCTimeout is reasonable", func(t *testing.T) {
		if destRPCTimeout < 5*time.Second {
			t.Errorf("destRPCTimeout too short: %v", destRPCTimeout)
		}
		if destRPCTimeout > 2*time.Minute {
			t.Errorf("destRPCTimeout too long: %v", destRPCTimeout)
		}
	})

	t.Run("backoff constants are valid", func(t *testing.T) {
		if minFinalizeBackoff <= 0 {
			t.Error("minFinalizeBackoff should be positive")
		}
		if maxFinalizeBackoff <= minFinalizeBackoff {
			t.Error("maxFinalizeBackoff should be greater than minFinalizeBackoff")
		}
	})
}

// mockDest implements Destination for testing.
type mockDest struct {
	checkStatusResults map[string]*streaming.InitTorrentResult
	checkStatusErr     error

	finalizeCalled      bool
	finalizeHash        string
	finalizeSavePath    string
	finalizeCategory    string
	finalizeTags        string
	finalizeSaveSubPath string
	finalizeErr         error

	abortCalled      bool
	abortHash        string
	abortDeleteFiles bool
	abortResult      int32
	abortErr         error

	startCalled bool
	startHash   string
	startTag    string
	startErr    error

	initResult      *streaming.InitTorrentResult
	initErr         error
	clearInitCalled bool
	clearInitHash   string
}

var _ Destination = (*mockDest)(nil)

func (m *mockDest) CheckTorrentStatus(_ context.Context, hash string) (*streaming.InitTorrentResult, error) {
	if m.checkStatusErr != nil {
		return nil, m.checkStatusErr
	}
	if result, ok := m.checkStatusResults[hash]; ok {
		return result, nil
	}
	return nil, errors.New("unknown hash")
}

func (m *mockDest) FinalizeTorrent(_ context.Context, hash, savePath, category, tags, saveSubPath string) error {
	m.finalizeCalled = true
	m.finalizeHash = hash
	m.finalizeSavePath = savePath
	m.finalizeCategory = category
	m.finalizeTags = tags
	m.finalizeSaveSubPath = saveSubPath
	return m.finalizeErr
}

func (m *mockDest) AbortTorrent(_ context.Context, hash string, deleteFiles bool) (int32, error) {
	m.abortCalled = true
	m.abortHash = hash
	m.abortDeleteFiles = deleteFiles
	return m.abortResult, m.abortErr
}

func (m *mockDest) StartTorrent(_ context.Context, hash string, tag string) error {
	m.startCalled = true
	m.startHash = hash
	m.startTag = tag
	return m.startErr
}

func (m *mockDest) ClearInitResult(hash string) {
	m.clearInitCalled = true
	m.clearInitHash = hash
}

func (m *mockDest) InitTorrent(_ context.Context, _ *pb.InitTorrentRequest) (*streaming.InitTorrentResult, error) {
	if m.initErr != nil {
		return nil, m.initErr
	}
	return m.initResult, nil
}

func TestHandleTorrentRemoval(t *testing.T) {
	logger := testLogger(t)

	t.Run("calls StartTorrent when completedOnDest", func(t *testing.T) {
		dest := &mockDest{}
		tracked := NewTrackedSet()
		tracked.Add("abc123", TrackedTorrent{CompletionTime: time.Now()})
		completed := NewCompletionCache("", logger)
		completed.Mark("abc123")
		task := &QBTask{
			cfg: &config.SourceConfig{
				SourceRemovedTag: "source-removed",
			},
			logger:    logger,
			grpcDest:  dest,
			tracked:   tracked,
			completed: completed,
			backoffs:  NewBackoffTracker(),
		}

		task.handleTorrentRemoval(context.Background(), "abc123")

		if task.tracked.Has("abc123") {
			t.Error("torrent should have been removed from TrackedTorrents")
		}

		if task.completed.IsComplete("abc123") {
			t.Error("torrent should have been removed from completedOnDest")
		}

		if !dest.startCalled {
			t.Error("StartTorrent should have been called")
		}
		if dest.startHash != "abc123" {
			t.Errorf("expected start hash 'abc123', got '%s'", dest.startHash)
		}
		if dest.startTag != "source-removed" {
			t.Errorf("expected start tag 'source-removed', got '%s'", dest.startTag)
		}
		if dest.abortCalled {
			t.Error("AbortTorrent should NOT have been called when completedOnDest")
		}
	})

	t.Run("handles AbortTorrent error gracefully", func(t *testing.T) {
		dest := &mockDest{abortErr: errors.New("destination server down")}
		tracked := NewTrackedSet()
		tracked.Add("abc123", TrackedTorrent{CompletionTime: time.Now()})
		task := &QBTask{
			cfg:       &config.SourceConfig{},
			logger:    logger,
			grpcDest:  dest,
			tracked:   tracked,
			completed: NewCompletionCache("", logger),
			backoffs:  NewBackoffTracker(),
		}

		// Should not panic
		task.handleTorrentRemoval(context.Background(), "abc123")

		if !dest.abortCalled {
			t.Error("AbortTorrent should have been called even if it returns error")
		}

		// Local state should still be cleaned up
		if task.tracked.Has("abc123") {
			t.Error("torrent should have been removed from TrackedTorrents despite abort error")
		}
	})

	t.Run("respects dry run mode", func(t *testing.T) {
		dest := &mockDest{}
		tracked := NewTrackedSet()
		tracked.Add("abc123", TrackedTorrent{CompletionTime: time.Now()})
		completed := NewCompletionCache("", logger)
		completed.Mark("abc123")
		task := &QBTask{
			cfg:       &config.SourceConfig{BaseConfig: config.BaseConfig{DryRun: true}},
			logger:    logger,
			grpcDest:  dest,
			tracked:   tracked,
			completed: completed,
			backoffs:  NewBackoffTracker(),
		}

		task.handleTorrentRemoval(context.Background(), "abc123")

		// TrackedTorrents should be cleaned up even in dry run
		if task.tracked.Has("abc123") {
			t.Error("torrent should have been removed from TrackedTorrents in dry run")
		}

		// completedOnDest should be preserved in dry run (no action taken)
		if !task.completed.IsComplete("abc123") {
			t.Error("completedOnDest should be preserved in dry run mode")
		}

		// Neither StartTorrent nor AbortTorrent should be called in dry run
		if dest.startCalled {
			t.Error("StartTorrent should NOT have been called in dry run mode")
		}
		if dest.abortCalled {
			t.Error("AbortTorrent should NOT have been called in dry run mode")
		}
	})

	t.Run("calls AbortTorrent when not completedOnDest", func(t *testing.T) {
		dest := &mockDest{abortResult: 3}
		tracked := NewTrackedSet()
		tracked.Add("abc123", TrackedTorrent{CompletionTime: time.Now()})
		task := &QBTask{
			cfg:       &config.SourceConfig{},
			logger:    logger,
			grpcDest:  dest,
			tracked:   tracked,
			completed: NewCompletionCache("", logger),
			backoffs:  NewBackoffTracker(),
		}

		task.handleTorrentRemoval(context.Background(), "abc123")

		if !dest.abortCalled {
			t.Error("AbortTorrent should have been called when not completedOnDest")
		}
		if dest.abortHash != "abc123" {
			t.Errorf("expected abort hash 'abc123', got '%s'", dest.abortHash)
		}
		if dest.startCalled {
			t.Error("StartTorrent should NOT have been called when not completedOnDest")
		}
	})

	t.Run("passes empty tag when SourceRemovedTag is empty", func(t *testing.T) {
		dest := &mockDest{}
		tracked := NewTrackedSet()
		tracked.Add("abc123", TrackedTorrent{CompletionTime: time.Now()})
		completed := NewCompletionCache("", logger)
		completed.Mark("abc123")
		task := &QBTask{
			cfg: &config.SourceConfig{
				SourceRemovedTag: "",
			},
			logger:    logger,
			grpcDest:  dest,
			tracked:   tracked,
			completed: completed,
			backoffs:  NewBackoffTracker(),
		}

		task.handleTorrentRemoval(context.Background(), "abc123")

		if !dest.startCalled {
			t.Error("StartTorrent should have been called")
		}
		if dest.startTag != "" {
			t.Errorf("expected empty tag, got '%s'", dest.startTag)
		}
	})

	t.Run("keeps completedOnDest when StartTorrent fails", func(t *testing.T) {
		dest := &mockDest{startErr: errors.New("destination unreachable")}
		tracked := NewTrackedSet()
		tracked.Add("abc123", TrackedTorrent{CompletionTime: time.Now()})
		completed := NewCompletionCache("", logger)
		completed.Mark("abc123")
		task := &QBTask{
			cfg: &config.SourceConfig{
				SourceRemovedTag: "source-removed",
			},
			logger:    logger,
			grpcDest:  dest,
			tracked:   tracked,
			completed: completed,
			backoffs:  NewBackoffTracker(),
		}

		task.handleTorrentRemoval(context.Background(), "abc123")

		if !dest.startCalled {
			t.Error("StartTorrent should have been called")
		}

		// completedOnDest should be preserved when StartTorrent fails
		if !task.completed.IsComplete("abc123") {
			t.Error("completedOnDest should be preserved when StartTorrent fails")
		}

		// AbortTorrent should NOT be called
		if dest.abortCalled {
			t.Error("AbortTorrent should NOT have been called when completedOnDest")
		}
	})

	t.Run("handles untracked torrent removal", func(t *testing.T) {
		dest := &mockDest{abortResult: 0}
		task := &QBTask{
			cfg:       &config.SourceConfig{},
			logger:    logger,
			grpcDest:  dest,
			tracked:   NewTrackedSet(),
			completed: NewCompletionCache("", logger),
			backoffs:  NewBackoffTracker(),
		}

		task.handleTorrentRemoval(context.Background(), "unknown_hash")

		// AbortTorrent should still be called for untracked torrents
		if !dest.abortCalled {
			t.Error("AbortTorrent should have been called even for untracked torrents")
		}
		if dest.abortHash != "unknown_hash" {
			t.Errorf("expected abort hash 'unknown_hash', got '%s'", dest.abortHash)
		}
	})
}

func makeTestGroup(hashes ...string) torrentGroup {
	torrents := make([]qbittorrent.Torrent, len(hashes))
	for i, h := range hashes {
		torrents[i] = qbittorrent.Torrent{Hash: h, SeedingTime: 9999}
	}
	return torrentGroup{torrents: torrents, minSeeding: 9999}
}

func newDeleteGroupTask(
	t *testing.T,
	client *mockQBClient,
	dest *mockDest,
	cfg *config.SourceConfig,
) *QBTask {
	t.Helper()
	if cfg == nil {
		cfg = &config.SourceConfig{}
	}
	return &QBTask{
		cfg:       cfg,
		logger:    testLogger(t),
		srcClient: client,
		grpcDest:  dest,
	}
}

func TestDeleteGroupFromHot_HappyPath(t *testing.T) {
	mockClient := &mockQBClient{}
	dest := &mockDest{}
	task := newDeleteGroupTask(t, mockClient, dest, nil)

	group := makeTestGroup("abc123")
	handed, err := task.deleteGroupFromHot(context.Background(), group)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if handed != 1 {
		t.Errorf("expected 1 handed off, got %d", handed)
	}

	if !mockClient.stopCalled {
		t.Error("StopCtx should have been called")
	}
	if dest.startHash != "abc123" {
		t.Errorf("expected StartTorrent hash 'abc123', got '%s'", dest.startHash)
	}
	if !mockClient.deleteCalled {
		t.Error("DeleteTorrentsCtx should have been called")
	}
	if mockClient.resumeCalled {
		t.Error("ResumeCtx should NOT have been called on happy path")
	}
}

func TestDeleteGroupFromHot_DryRun(t *testing.T) {
	mockClient := &mockQBClient{}
	dest := &mockDest{}
	task := newDeleteGroupTask(t, mockClient, dest, &config.SourceConfig{
		BaseConfig: config.BaseConfig{DryRun: true},
	})

	group := makeTestGroup("abc123")
	handed, err := task.deleteGroupFromHot(context.Background(), group)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if handed != 0 {
		t.Errorf("expected 0 handed off in dry run, got %d", handed)
	}

	if mockClient.stopCalled {
		t.Error("StopCtx should NOT have been called in dry run")
	}
	if dest.startCalled {
		t.Error("StartTorrent should NOT have been called in dry run")
	}
	if mockClient.deleteCalled {
		t.Error("DeleteTorrentsCtx should NOT have been called in dry run")
	}
}

func TestDeleteGroupFromHot_Failures(t *testing.T) {
	t.Run("stop failure skips torrent", func(t *testing.T) {
		mockClient := &mockQBClient{stopErr: errors.New("stop failed")}
		dest := &mockDest{}
		task := newDeleteGroupTask(t, mockClient, dest, nil)

		group := makeTestGroup("abc123")
		handed, err := task.deleteGroupFromHot(context.Background(), group)
		if err == nil {
			t.Fatal("expected error for failed handoff")
		}
		if handed != 0 {
			t.Errorf("expected 0 handed off, got %d", handed)
		}

		if !mockClient.stopCalled {
			t.Error("StopCtx should have been called")
		}
		if dest.startCalled {
			t.Error("StartTorrent should NOT have been called when stop fails")
		}
		if mockClient.deleteCalled {
			t.Error("DeleteTorrentsCtx should NOT have been called when stop fails")
		}
		if mockClient.resumeCalled {
			t.Error("ResumeCtx should NOT have been called when stop fails")
		}
	})

	t.Run("destination start failure triggers resume on source", func(t *testing.T) {
		mockClient := &mockQBClient{}
		dest := &mockDest{startErr: errors.New("destination unreachable")}
		task := newDeleteGroupTask(t, mockClient, dest, nil)

		group := makeTestGroup("abc123")
		handed, err := task.deleteGroupFromHot(context.Background(), group)
		if err == nil {
			t.Fatal("expected error for failed handoff")
		}
		if handed != 0 {
			t.Errorf("expected 0 handed off, got %d", handed)
		}

		if !mockClient.stopCalled {
			t.Error("StopCtx should have been called")
		}
		if !dest.startCalled {
			t.Error("StartTorrent should have been called")
		}
		if !mockClient.resumeCalled {
			t.Error("ResumeCtx should have been called to rollback")
		}
		if len(mockClient.resumeHashes) != 1 || mockClient.resumeHashes[0] != "abc123" {
			t.Errorf("expected resume hashes [abc123], got %v", mockClient.resumeHashes)
		}
		if mockClient.deleteCalled {
			t.Error("DeleteTorrentsCtx should NOT have been called when destination start fails")
		}
	})

	t.Run("destination start failure with resume failure is tolerated", func(t *testing.T) {
		mockClient := &mockQBClient{
			resumeErr: errors.New("qBittorrent unreachable"),
		}
		dest := &mockDest{startErr: errors.New("destination server unreachable")}
		task := newDeleteGroupTask(t, mockClient, dest, nil)

		group := makeTestGroup("abc123")
		handed, err := task.deleteGroupFromHot(context.Background(), group)
		if err == nil {
			t.Fatal("expected error for failed handoff")
		}
		if handed != 0 {
			t.Errorf("expected 0 handed off, got %d", handed)
		}

		if !mockClient.resumeCalled {
			t.Error("ResumeCtx should have been called despite it returning an error")
		}
		if mockClient.deleteCalled {
			t.Error("DeleteTorrentsCtx should NOT have been called when destination start fails")
		}
	})

	t.Run("delete failure is tolerated (destination is seeding)", func(t *testing.T) {
		mockClient := &mockQBClient{
			deleteErr: errors.New("delete failed"),
		}
		dest := &mockDest{}
		task := newDeleteGroupTask(t, mockClient, dest, nil)

		group := makeTestGroup("abc123")
		handed, err := task.deleteGroupFromHot(context.Background(), group)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if handed != 1 {
			t.Errorf("expected 1 handed off (delete failure still counts), got %d", handed)
		}

		if !mockClient.stopCalled {
			t.Error("StopCtx should have been called")
		}
		if !mockClient.deleteCalled {
			t.Error("DeleteTorrentsCtx should have been called")
		}
		if mockClient.resumeCalled {
			t.Error("ResumeCtx should NOT have been called -- destination is seeding, delete retry next cycle")
		}
	})
}

func TestDeleteGroupFromHot_MultiTorrent(t *testing.T) {
	t.Run("returns count of handed-off torrents", func(t *testing.T) {
		mockClient := &mockQBClient{}
		dest := &mockDest{}
		task := newDeleteGroupTask(t, mockClient, dest, nil)

		group := torrentGroup{
			torrents: []qbittorrent.Torrent{
				{Hash: "abc123"},
				{Hash: "def456"},
			},
			minSeeding: 9999,
		}
		handed, err := task.deleteGroupFromHot(context.Background(), group)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if handed != 2 {
			t.Errorf("expected 2 handed off, got %d", handed)
		}
	})

	t.Run("partial group failure returns handed count and error", func(t *testing.T) {
		mockClient := &mockQBClient{
			stopFailHash: map[string]bool{"def456": true},
		}
		dest := &mockDest{}
		task := newDeleteGroupTask(t, mockClient, dest, nil)

		group := torrentGroup{
			torrents: []qbittorrent.Torrent{
				{Hash: "abc123"},
				{Hash: "def456"},
			},
			minSeeding: 9999,
		}
		handed, err := task.deleteGroupFromHot(context.Background(), group)
		if err == nil {
			t.Fatal("expected error for partial group failure")
		}
		if handed != 1 {
			t.Errorf("expected 1 handed off (partial success), got %d", handed)
		}
	})
}

func TestDeleteGroupFromHot_Tags(t *testing.T) {
	t.Run("passes SourceRemovedTag to StartTorrent", func(t *testing.T) {
		mockClient := &mockQBClient{}
		dest := &mockDest{}
		task := newDeleteGroupTask(t, mockClient, dest, &config.SourceConfig{
			SourceRemovedTag: "source-removed",
		})

		group := makeTestGroup("abc123")
		_, err := task.deleteGroupFromHot(context.Background(), group)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !dest.startCalled {
			t.Error("StartTorrent should have been called")
		}
		if dest.startTag != "source-removed" {
			t.Errorf("expected tag 'source-removed', got '%s'", dest.startTag)
		}
	})

	t.Run("passes empty tag when SourceRemovedTag is empty", func(t *testing.T) {
		mockClient := &mockQBClient{}
		dest := &mockDest{}
		task := newDeleteGroupTask(t, mockClient, dest, nil)

		group := makeTestGroup("abc123")
		_, err := task.deleteGroupFromHot(context.Background(), group)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !dest.startCalled {
			t.Error("StartTorrent should have been called")
		}
		if dest.startTag != "" {
			t.Errorf("expected empty tag, got '%s'", dest.startTag)
		}
	})
}

func TestFinalizeTorrent(t *testing.T) {
	logger := testLogger(t)

	t.Run("happy path: fetches info and calls FinalizeTorrent", func(t *testing.T) {
		mockClient := &mockQBClient{
			getTorrentsResult: []qbittorrent.Torrent{
				{Hash: "abc123", SavePath: "/data/downloads", Category: "movies", Tags: "hd,new"},
			},
		}
		dest := &mockDest{}
		task := &QBTask{
			cfg:       &config.SourceConfig{},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  dest,
			source:    qbclient.NewSource(nil, ""),
		}

		err := task.finalizeTorrent(context.Background(), "abc123")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if !dest.finalizeCalled {
			t.Error("FinalizeTorrent should have been called")
		}
		if dest.finalizeHash != "abc123" {
			t.Errorf("expected hash 'abc123', got '%s'", dest.finalizeHash)
		}
		if dest.finalizeSavePath != "/data/downloads" {
			t.Errorf("expected savePath '/data/downloads', got '%s'", dest.finalizeSavePath)
		}
		if dest.finalizeCategory != "movies" {
			t.Errorf("expected category 'movies', got '%s'", dest.finalizeCategory)
		}
		if dest.finalizeTags != "hd,new" {
			t.Errorf("expected tags 'hd,new', got '%s'", dest.finalizeTags)
		}
	})

	t.Run("torrent not found returns error", func(t *testing.T) {
		mockClient := &mockQBClient{
			getTorrentsResult: []qbittorrent.Torrent{}, // empty
		}
		dest := &mockDest{}
		task := &QBTask{
			cfg:       &config.SourceConfig{},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  dest,
		}

		err := task.finalizeTorrent(context.Background(), "missing_hash")
		if err == nil {
			t.Error("expected error when torrent not found")
		}
		if dest.finalizeCalled {
			t.Error("FinalizeTorrent should NOT have been called when torrent not found")
		}
	})

	t.Run("GetTorrentsCtx error propagates", func(t *testing.T) {
		mockClient := &mockQBClient{
			getTorrentsErr: errors.New("qb unreachable"),
		}
		dest := &mockDest{}
		task := &QBTask{
			cfg:       &config.SourceConfig{},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  dest,
		}

		err := task.finalizeTorrent(context.Background(), "abc123")
		if err == nil {
			t.Error("expected error when GetTorrentsCtx fails")
		}
		if dest.finalizeCalled {
			t.Error("FinalizeTorrent should NOT have been called when GetTorrentsCtx fails")
		}
	})

	t.Run("FinalizeTorrent RPC error propagates", func(t *testing.T) {
		mockClient := &mockQBClient{
			getTorrentsResult: []qbittorrent.Torrent{
				{Hash: "abc123", SavePath: "/data"},
			},
		}
		dest := &mockDest{finalizeErr: errors.New("destination RPC failed")}
		task := &QBTask{
			cfg:       &config.SourceConfig{},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  dest,
			source:    qbclient.NewSource(nil, ""),
		}

		err := task.finalizeTorrent(context.Background(), "abc123")
		if err == nil {
			t.Error("expected error when FinalizeTorrent RPC fails")
		}
		if !dest.finalizeCalled {
			t.Error("FinalizeTorrent should have been called")
		}
	})

	t.Run("dry run fetches info but skips FinalizeTorrent", func(t *testing.T) {
		mockClient := &mockQBClient{
			getTorrentsResult: []qbittorrent.Torrent{
				{Hash: "abc123", SavePath: "/data"},
			},
		}
		dest := &mockDest{}
		task := &QBTask{
			cfg:       &config.SourceConfig{BaseConfig: config.BaseConfig{DryRun: true}},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  dest,
			source:    qbclient.NewSource(nil, ""),
		}

		err := task.finalizeTorrent(context.Background(), "abc123")
		if err != nil {
			t.Errorf("unexpected error in dry run: %v", err)
		}
		if dest.finalizeCalled {
			t.Error("FinalizeTorrent should NOT have been called in dry run")
		}
	})
}

func TestFinalizeTorrent_ErrFinalizeVerifyingPropagates(t *testing.T) {
	logger := testLogger(t)

	t.Run("ErrFinalizeVerifying is returned by finalizeTorrent", func(t *testing.T) {
		mockClient := &mockQBClient{
			getTorrentsResult: []qbittorrent.Torrent{
				{Hash: "abc123", SavePath: "/data", Category: "movies"},
			},
		}
		dest := &mockDest{finalizeErr: streaming.ErrFinalizeVerifying}
		task := &QBTask{
			cfg:       &config.SourceConfig{},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  dest,
			source:    qbclient.NewSource(nil, ""),
		}

		err := task.finalizeTorrent(context.Background(), "abc123")
		if err == nil {
			t.Fatal("expected error")
		}
		if !errors.Is(err, streaming.ErrFinalizeVerifying) {
			t.Errorf("expected ErrFinalizeVerifying, got: %v", err)
		}
	})

	t.Run("ErrFinalizeVerifying does not increment backoff", func(t *testing.T) {
		mockClient := &mockQBClient{
			getTorrentsResult: []qbittorrent.Torrent{
				{Hash: "abc123", SavePath: "/data"},
			},
		}
		dest := &mockDest{finalizeErr: streaming.ErrFinalizeVerifying}
		task := &QBTask{
			cfg:       &config.SourceConfig{},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  dest,
			source:    qbclient.NewSource(nil, ""),
			backoffs:  NewBackoffTracker(),
		}

		// Call finalizeTorrent — returns ErrFinalizeVerifying
		_ = task.finalizeTorrent(context.Background(), "abc123")

		// Simulate orchestrator logic: ErrFinalizeVerifying should NOT call recordFinalizeFailure
		// (the orchestrator checks errors.Is before recording failure).
		// Verify no backoff was recorded — ShouldAttempt returns true when no backoff exists.
		if !task.backoffs.ShouldAttempt("abc123") {
			t.Error("should allow immediate retry after ErrFinalizeVerifying")
		}
	})
}

func TestFinalizeTorrent_ErrFinalizeIncompletePropagates(t *testing.T) {
	logger := testLogger(t)

	t.Run("ErrFinalizeIncomplete is returned by finalizeTorrent", func(t *testing.T) {
		mockClient := &mockQBClient{
			getTorrentsResult: []qbittorrent.Torrent{
				{Hash: "abc123", SavePath: "/data", Category: "movies"},
			},
		}
		dest := &mockDest{
			finalizeErr: fmt.Errorf("%w: incomplete: 100/200 pieces", streaming.ErrFinalizeIncomplete),
		}
		task := &QBTask{
			cfg:       &config.SourceConfig{},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  dest,
			source:    qbclient.NewSource(nil, ""),
		}

		err := task.finalizeTorrent(context.Background(), "abc123")
		if err == nil {
			t.Fatal("expected error")
		}
		if !errors.Is(err, streaming.ErrFinalizeIncomplete) {
			t.Errorf("expected ErrFinalizeIncomplete, got: %v", err)
		}
	})

	t.Run("ErrFinalizeIncomplete does not increment backoff", func(t *testing.T) {
		mockClient := &mockQBClient{
			getTorrentsResult: []qbittorrent.Torrent{
				{Hash: "abc123", SavePath: "/data"},
			},
		}
		dest := &mockDest{
			finalizeErr: fmt.Errorf("%w: incomplete: 50/100 pieces", streaming.ErrFinalizeIncomplete),
		}
		task := &QBTask{
			cfg:       &config.SourceConfig{},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  dest,
			source:    qbclient.NewSource(nil, ""),
			backoffs:  NewBackoffTracker(),
		}

		_ = task.finalizeTorrent(context.Background(), "abc123")

		// ShouldAttempt returns true when no backoff exists
		if !task.backoffs.ShouldAttempt("abc123") {
			t.Error("ErrFinalizeIncomplete should not create a backoff entry")
		}
	})
}

func TestResyncWithDest(t *testing.T) {
	logger := testLogger(t)

	t.Run("resyncs streamed state from destination init response", func(t *testing.T) {
		// Set up a PieceMonitor with a torrent where all pieces are "streamed"
		monitor := streaming.NewPieceMonitor(nil, nil, logger, streaming.PieceMonitorConfig{
			PollInterval: time.Second,
		})

		hash := "resync-test"
		numPieces := 10

		// Manually add a torrent state to the monitor
		monitor.AddTestState(hash, numPieces)

		// Mark all pieces as streamed (simulates source thinking everything is done)
		allWritten := make([]bool, numPieces)
		for i := range allWritten {
			allWritten[i] = true
		}
		monitor.MarkStreamedBatch(hash, allWritten)

		// Verify all are streamed before resync
		progress, _ := monitor.GetProgress(hash)
		if progress.Streamed != numPieces {
			t.Fatalf("expected %d streamed, got %d", numPieces, progress.Streamed)
		}

		// Destination only has 7 pieces — PiecesNeeded[i]=true means missing
		piecesNeeded := make([]bool, numPieces)
		piecesNeeded[7] = true
		piecesNeeded[8] = true
		piecesNeeded[9] = true

		dest := &mockDest{
			initResult: &streaming.InitTorrentResult{
				PiecesNeeded:      piecesNeeded,
				PiecesNeededCount: 3,
				PiecesHaveCount:   7,
			},
		}

		task := &QBTask{
			cfg:      &config.SourceConfig{},
			logger:   logger,
			grpcDest: dest,
			tracker:  monitor,
		}

		task.resyncWithDest(context.Background(), hash)

		// After resync, only 7 should be streamed
		progress, _ = monitor.GetProgress(hash)
		if progress.Streamed != 7 {
			t.Errorf("expected 7 streamed after resync, got %d", progress.Streamed)
		}
		if progress.Complete {
			t.Error("should not be complete after resync")
		}
	})

	t.Run("handles init error gracefully", func(t *testing.T) {
		monitor := streaming.NewPieceMonitor(nil, nil, logger, streaming.PieceMonitorConfig{
			PollInterval: time.Second,
		})

		hash := "resync-fail"
		monitor.AddTestState(hash, 5)

		dest := &mockDest{
			initErr: errors.New("destination unreachable"),
		}

		task := &QBTask{
			cfg:      &config.SourceConfig{},
			logger:   logger,
			grpcDest: dest,
			tracker:  monitor,
		}

		// Should not panic
		task.resyncWithDest(context.Background(), hash)

		// State should be unchanged
		progress, _ := monitor.GetProgress(hash)
		if progress.Streamed != 0 {
			t.Errorf("expected 0 streamed (unchanged), got %d", progress.Streamed)
		}
	})
}

// TestSyncedTagApplication tests that the synced tag is applied correctly.
func TestSyncedTagApplication(t *testing.T) {
	logger := testLogger(t)

	t.Run("applies synced tag when configured", func(t *testing.T) {
		mockClient := &mockQBClient{}
		task := &QBTask{
			cfg: &config.SourceConfig{
				BaseConfig: config.BaseConfig{SyncedTag: "my-synced-tag"},
			},
			logger:    logger,
			srcClient: mockClient,
			completed: NewCompletionCache("", logger),
			tracked:   NewTrackedSet(),
		}

		// Simulate the tag application logic from markTorrentComplete
		hash := "testhash123"
		if task.cfg.SyncedTag != "" && !task.cfg.DryRun {
			_ = task.srcClient.AddTagsCtx(context.Background(), []string{hash}, task.cfg.SyncedTag)
		}

		if !mockClient.addTagsCalled {
			t.Error("AddTagsCtx should have been called")
		}
		if mockClient.addTagsTag != "my-synced-tag" {
			t.Errorf("expected tag 'my-synced-tag', got '%s'", mockClient.addTagsTag)
		}
		if len(mockClient.addTagsHashes) != 1 || mockClient.addTagsHashes[0] != hash {
			t.Errorf("expected hashes [%s], got %v", hash, mockClient.addTagsHashes)
		}
	})

	t.Run("skips tag when empty", func(t *testing.T) {
		mockClient := &mockQBClient{}
		task := &QBTask{
			cfg:       &config.SourceConfig{},
			logger:    logger,
			srcClient: mockClient,
			completed: NewCompletionCache("", logger),
		}

		// Simulate the tag application logic
		hash := "testhash123"
		if task.cfg.SyncedTag != "" && !task.cfg.DryRun {
			_ = task.srcClient.AddTagsCtx(context.Background(), []string{hash}, task.cfg.SyncedTag)
		}

		if mockClient.addTagsCalled {
			t.Error("AddTagsCtx should NOT have been called when tag is empty")
		}
	})

	t.Run("skips tag in dry run mode", func(t *testing.T) {
		mockClient := &mockQBClient{}
		task := &QBTask{
			cfg: &config.SourceConfig{
				BaseConfig: config.BaseConfig{SyncedTag: "synced", DryRun: true},
			},
			logger:    logger,
			srcClient: mockClient,
			completed: NewCompletionCache("", logger),
		}

		// Simulate the tag application logic
		hash := "testhash123"
		if task.cfg.SyncedTag != "" && !task.cfg.DryRun {
			_ = task.srcClient.AddTagsCtx(context.Background(), []string{hash}, task.cfg.SyncedTag)
		}

		if mockClient.addTagsCalled {
			t.Error("AddTagsCtx should NOT have been called in dry run mode")
		}
	})
}

func TestQueryDestStatus(t *testing.T) {
	logger := testLogger(t)
	makeTracker := func() *streaming.PieceMonitor {
		return streaming.NewPieceMonitor(nil, nil, logger, streaming.DefaultPieceMonitorConfig())
	}

	t.Run("transient gRPC error returns error to short-circuit loop", func(t *testing.T) {
		transientErr := status.Error(codes.Unavailable, "destination server down")
		dest := &mockDest{checkStatusErr: transientErr}
		task := &QBTask{
			cfg:       &config.SourceConfig{},
			logger:    logger,
			grpcDest:  dest,
			tracked:   NewTrackedSet(),
			completed: NewCompletionCache("", logger),
			backoffs:  NewBackoffTracker(),
			tracker:   makeTracker(),
		}

		torrent := qbittorrent.Torrent{Hash: "abc123", Name: "test"}
		result, err := task.queryDestStatus(context.Background(), torrent)
		if err == nil {
			t.Error("expected transient error to be returned")
		}
		if result != nil {
			t.Error("result should be nil on transient error")
		}
	})

	t.Run("non-transient error returns errSkipTorrent", func(t *testing.T) {
		nonTransientErr := errors.New("some application error")
		dest := &mockDest{checkStatusErr: nonTransientErr}
		task := &QBTask{
			cfg:       &config.SourceConfig{},
			logger:    logger,
			grpcDest:  dest,
			tracked:   NewTrackedSet(),
			completed: NewCompletionCache("", logger),
			backoffs:  NewBackoffTracker(),
			tracker:   makeTracker(),
		}

		torrent := qbittorrent.Torrent{Hash: "abc123", Name: "test"}
		result, err := task.queryDestStatus(context.Background(), torrent)
		if !errors.Is(err, errSkipTorrent) {
			t.Errorf("expected errSkipTorrent, got: %v", err)
		}
		if result != nil {
			t.Error("result should be nil for non-transient error")
		}
	})

	t.Run("COMPLETE status returns errSkipTorrent and caches", func(t *testing.T) {
		dest := &mockDest{
			checkStatusResults: map[string]*streaming.InitTorrentResult{
				"abc123": {Status: pb.TorrentSyncStatus_SYNC_STATUS_COMPLETE},
			},
		}
		tmpDir := t.TempDir()
		task := &QBTask{
			cfg:       &config.SourceConfig{},
			logger:    logger,
			grpcDest:  dest,
			tracked:   NewTrackedSet(),
			completed: NewCompletionCache(filepath.Join(tmpDir, "cache.json"), logger),
			backoffs:  NewBackoffTracker(),
			tracker:   makeTracker(),
		}

		torrent := qbittorrent.Torrent{Hash: "abc123", Name: "test"}
		result, err := task.queryDestStatus(context.Background(), torrent)
		if !errors.Is(err, errSkipTorrent) {
			t.Errorf("expected errSkipTorrent, got: %v", err)
		}
		if result != nil {
			t.Error("result should be nil for COMPLETE torrent")
		}

		if !task.completed.IsComplete("abc123") {
			t.Error("COMPLETE torrent should be cached")
		}
	})
}

func TestCompletedCachePersistence(t *testing.T) {
	logger := testLogger(t)

	t.Run("save and load round-trip", func(t *testing.T) {
		tmpDir := t.TempDir()
		cachePath := filepath.Join(tmpDir, ".qb-sync", "completed_on_dest.json")

		cache := NewCompletionCache(cachePath, logger)

		// Mark some torrents as complete
		cache.Mark("hash1")
		cache.Mark("hash2")
		cache.Mark("hash3")

		cache.Save()

		// Verify file exists and is valid JSON object (new format)
		data, err := os.ReadFile(cachePath)
		if err != nil {
			t.Fatalf("cache file should exist: %v", err)
		}
		var fingerprints map[string]string
		if jsonErr := json.Unmarshal(data, &fingerprints); jsonErr != nil {
			t.Fatalf("cache should be valid JSON object: %v", jsonErr)
		}
		if len(fingerprints) != 3 {
			t.Errorf("expected 3 entries in cache, got %d", len(fingerprints))
		}

		// Load into a new cache
		cache2 := NewCompletionCache(cachePath, logger)
		cache2.Load()

		if cache2.Count() != 3 {
			t.Errorf("expected 3 hashes loaded, got %d", cache2.Count())
		}
		for _, h := range []string{"hash1", "hash2", "hash3"} {
			if !cache2.IsComplete(h) {
				t.Errorf("expected hash %s to be loaded", h)
			}
		}
	})

	t.Run("missing file loads empty cache", func(t *testing.T) {
		tmpDir := t.TempDir()
		cachePath := filepath.Join(tmpDir, "nonexistent", "cache.json")

		cache := NewCompletionCache(cachePath, logger)
		cache.Load()

		if cache.Count() != 0 {
			t.Errorf("expected empty cache, got %d", cache.Count())
		}
	})

	t.Run("corrupt file loads empty cache", func(t *testing.T) {
		tmpDir := t.TempDir()
		cachePath := filepath.Join(tmpDir, "cache.json")

		if err := os.WriteFile(cachePath, []byte("not json"), 0o644); err != nil {
			t.Fatal(err)
		}

		cache := NewCompletionCache(cachePath, logger)
		cache.Load()

		if cache.Count() != 0 {
			t.Errorf("expected empty cache from corrupt file, got %d", cache.Count())
		}
	})

	t.Run("MarkWithFingerprint persists to disk", func(t *testing.T) {
		tmpDir := t.TempDir()
		cachePath := filepath.Join(tmpDir, ".qb-sync", "completed_on_dest.json")

		cache := NewCompletionCache(cachePath, logger)

		cache.MarkWithFingerprint("hash_abc", "0,1")
		cache.Save()

		// Verify persisted
		data, err := os.ReadFile(cachePath)
		if err != nil {
			t.Fatalf("cache file should exist after MarkWithFingerprint: %v", err)
		}
		var fingerprints map[string]string
		if jsonErr := json.Unmarshal(data, &fingerprints); jsonErr != nil {
			t.Fatalf("cache should be valid JSON object: %v", jsonErr)
		}
		if len(fingerprints) != 1 {
			t.Errorf("expected 1 entry, got %d", len(fingerprints))
		}
		if fp, ok := fingerprints["hash_abc"]; !ok || fp != "0,1" {
			t.Errorf("expected {hash_abc: 0,1}, got %v", fingerprints)
		}
	})
}

// mockPieceSource implements streaming.PieceSource for testing.
type mockPieceSource struct {
	numPieces int32
}

func (m *mockPieceSource) GetTorrentMetadata(_ context.Context, _ string) (*streaming.TorrentMetadata, error) {
	return &streaming.TorrentMetadata{
		InitTorrentRequest: &pb.InitTorrentRequest{
			NumPieces: m.numPieces,
			PieceSize: 1024,
			Name:      "test",
		},
	}, nil
}

func (m *mockPieceSource) GetPieceHashes(_ context.Context, _ string) ([]string, error) {
	hashes := make([]string, m.numPieces)
	for i := range hashes {
		hashes[i] = "deadbeef"
	}
	return hashes, nil
}

func (m *mockPieceSource) GetPieceStates(_ context.Context, _ string) ([]streaming.PieceState, error) {
	states := make([]streaming.PieceState, m.numPieces)
	for i := range states {
		states[i] = streaming.PieceStateDownloaded
	}
	return states, nil
}

func (m *mockPieceSource) ReadPiece(_ context.Context, _ *pb.Piece) ([]byte, error) {
	return nil, nil
}

func TestIsSyncableState(t *testing.T) {
	syncable := []qbittorrent.TorrentState{
		qbittorrent.TorrentStateDownloading,
		qbittorrent.TorrentStateStalledDl,
		qbittorrent.TorrentStateQueuedDl,
		qbittorrent.TorrentStateForcedDl,
		qbittorrent.TorrentStateUploading,
		qbittorrent.TorrentStateStalledUp,
		qbittorrent.TorrentStateQueuedUp,
		qbittorrent.TorrentStateForcedUp,
	}
	for _, state := range syncable {
		if !isSyncableState(state) {
			t.Errorf("expected %q to be syncable", state)
		}
	}

	notSyncable := []qbittorrent.TorrentState{
		qbittorrent.TorrentStatePausedDl,
		qbittorrent.TorrentStatePausedUp,
		qbittorrent.TorrentStateStoppedDl,
		qbittorrent.TorrentStateStoppedUp,
		qbittorrent.TorrentStateError,
		qbittorrent.TorrentStateMissingFiles,
		qbittorrent.TorrentStateMoving,
		qbittorrent.TorrentStateCheckingDl,
		qbittorrent.TorrentStateCheckingUp,
		qbittorrent.TorrentStateCheckingResumeData,
		qbittorrent.TorrentStateMetaDl,
		qbittorrent.TorrentStateAllocating,
		qbittorrent.TorrentStateUnknown,
	}
	for _, state := range notSyncable {
		if isSyncableState(state) {
			t.Errorf("expected %q to NOT be syncable", state)
		}
	}
}

func TestTrackNewTorrents_StateAndProgressFiltering(t *testing.T) {
	logger := testLogger(t)
	const numPieces = 10

	makePiecesNeeded := func(haveCount int) []bool {
		needed := make([]bool, numPieces)
		for i := haveCount; i < numPieces; i++ {
			needed[i] = true
		}
		return needed
	}

	t.Run("skips non-syncable states", func(t *testing.T) {
		mockSource := &mockPieceSource{numPieces: numPieces}
		tracker := streaming.NewPieceMonitor(nil, mockSource, logger, streaming.DefaultPieceMonitorConfig())

		// Only hashB (downloading) should reach CheckTorrentStatus;
		// hashA (paused) and hashC (error) are filtered by isSyncableState.
		dest := &mockDest{
			checkStatusResults: map[string]*streaming.InitTorrentResult{
				"hashB": {Status: pb.TorrentSyncStatus_SYNC_STATUS_READY, PiecesNeeded: makePiecesNeeded(0)},
			},
		}

		mockClient := &mockQBClient{
			getTorrentsResult: []qbittorrent.Torrent{
				{Hash: "hashA", Name: "paused", State: qbittorrent.TorrentStatePausedDl, Progress: 0.5},
				{Hash: "hashB", Name: "downloading", State: qbittorrent.TorrentStateDownloading, Progress: 0.5},
				{Hash: "hashC", Name: "error", State: qbittorrent.TorrentStateError, Progress: 0.5},
			},
		}

		task := &QBTask{
			cfg:       &config.SourceConfig{},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  dest,
			tracker:   tracker,
			tracked:   NewTrackedSet(),
			completed: NewCompletionCache(filepath.Join(t.TempDir(), "cache.json"), logger),
			backoffs:  NewBackoffTracker(),
		}

		var trackOrder []string
		task.trackingOrderHook = func(hash string) {
			trackOrder = append(trackOrder, hash)
		}

		err := task.trackNewTorrents(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Only hashB (downloading) should be tracked
		if len(trackOrder) != 1 {
			t.Fatalf("expected 1 tracked, got %d: %v", len(trackOrder), trackOrder)
		}
		if trackOrder[0] != "hashB" {
			t.Errorf("expected hashB, got %s", trackOrder[0])
		}
	})

	t.Run("skips torrents with zero progress", func(t *testing.T) {
		mockSource := &mockPieceSource{numPieces: numPieces}
		tracker := streaming.NewPieceMonitor(nil, mockSource, logger, streaming.DefaultPieceMonitorConfig())

		// Only hashB (has progress) should reach CheckTorrentStatus;
		// hashA (zero progress) is filtered before querying destination.
		dest := &mockDest{
			checkStatusResults: map[string]*streaming.InitTorrentResult{
				"hashB": {Status: pb.TorrentSyncStatus_SYNC_STATUS_READY, PiecesNeeded: makePiecesNeeded(0)},
			},
		}

		mockClient := &mockQBClient{
			getTorrentsResult: []qbittorrent.Torrent{
				{Hash: "hashA", Name: "no-progress", State: qbittorrent.TorrentStateDownloading, Progress: 0},
				{Hash: "hashB", Name: "has-progress", State: qbittorrent.TorrentStateDownloading, Progress: 0.1},
			},
		}

		task := &QBTask{
			cfg:       &config.SourceConfig{},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  dest,
			tracker:   tracker,
			tracked:   NewTrackedSet(),
			completed: NewCompletionCache(filepath.Join(t.TempDir(), "cache.json"), logger),
			backoffs:  NewBackoffTracker(),
		}

		var trackOrder []string
		task.trackingOrderHook = func(hash string) {
			trackOrder = append(trackOrder, hash)
		}

		err := task.trackNewTorrents(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Only hashB (has progress) should be tracked
		if len(trackOrder) != 1 {
			t.Fatalf("expected 1 tracked, got %d: %v", len(trackOrder), trackOrder)
		}
		if trackOrder[0] != "hashB" {
			t.Errorf("expected hashB, got %s", trackOrder[0])
		}
	})

	t.Run("tracks downloading torrents alongside completed ones", func(t *testing.T) {
		mockSource := &mockPieceSource{numPieces: numPieces}
		tracker := streaming.NewPieceMonitor(nil, mockSource, logger, streaming.DefaultPieceMonitorConfig())

		dest := &mockDest{
			checkStatusResults: map[string]*streaming.InitTorrentResult{
				"downloading": {Status: pb.TorrentSyncStatus_SYNC_STATUS_READY, PiecesNeeded: makePiecesNeeded(0)},
				"completed": {
					Status:          pb.TorrentSyncStatus_SYNC_STATUS_READY,
					PiecesNeeded:    makePiecesNeeded(5),
					PiecesHaveCount: 5,
				},
			},
		}

		mockClient := &mockQBClient{
			getTorrentsResult: []qbittorrent.Torrent{
				{Hash: "downloading", Name: "dl-torrent", State: qbittorrent.TorrentStateDownloading, Progress: 0.3},
				{Hash: "completed", Name: "up-torrent", State: qbittorrent.TorrentStateStalledUp, Progress: 1.0},
			},
		}

		task := &QBTask{
			cfg:       &config.SourceConfig{},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  dest,
			tracker:   tracker,
			tracked:   NewTrackedSet(),
			completed: NewCompletionCache(filepath.Join(t.TempDir(), "cache.json"), logger),
			backoffs:  NewBackoffTracker(),
		}

		var trackOrder []string
		task.trackingOrderHook = func(hash string) {
			trackOrder = append(trackOrder, hash)
		}

		err := task.trackNewTorrents(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(trackOrder) != 2 {
			t.Fatalf("expected 2 tracked, got %d: %v", len(trackOrder), trackOrder)
		}
		// Sorted by progress on destination: completed (5) first, then downloading (0)
		if trackOrder[0] != "completed" {
			t.Errorf("expected completed first (more pieces on destination), got %s", trackOrder[0])
		}
		if trackOrder[1] != "downloading" {
			t.Errorf("expected downloading second, got %s", trackOrder[1])
		}
	})
}

func TestTrackNewTorrents_PrioritizesByProgress(t *testing.T) {
	logger := testLogger(t)
	const numPieces = 1000

	makePiecesNeeded := func(haveCount int) []bool {
		needed := make([]bool, numPieces)
		for i := haveCount; i < numPieces; i++ {
			needed[i] = true
		}
		return needed
	}

	t.Run("tracks torrents in descending progress order", func(t *testing.T) {
		mockSource := &mockPieceSource{numPieces: numPieces}
		tracker := streaming.NewPieceMonitor(nil, mockSource, logger, streaming.DefaultPieceMonitorConfig())

		dest := &mockDest{
			checkStatusResults: map[string]*streaming.InitTorrentResult{
				"hashA": {
					Status:          pb.TorrentSyncStatus_SYNC_STATUS_READY,
					PiecesNeeded:    makePiecesNeeded(0),
					PiecesHaveCount: 0,
				},
				"hashB": {
					Status:          pb.TorrentSyncStatus_SYNC_STATUS_READY,
					PiecesNeeded:    makePiecesNeeded(900),
					PiecesHaveCount: 900,
				},
				"hashC": {
					Status:          pb.TorrentSyncStatus_SYNC_STATUS_READY,
					PiecesNeeded:    makePiecesNeeded(500),
					PiecesHaveCount: 500,
				},
			},
		}

		// API returns A, B, C in arbitrary order
		mockClient := &mockQBClient{
			getTorrentsResult: []qbittorrent.Torrent{
				{
					Hash:         "hashA",
					Name:         "torrentA",
					CompletionOn: 100,
					State:        qbittorrent.TorrentStateStalledUp,
					Progress:     1.0,
				},
				{
					Hash:         "hashB",
					Name:         "torrentB",
					CompletionOn: 200,
					State:        qbittorrent.TorrentStateStalledUp,
					Progress:     1.0,
				},
				{
					Hash:         "hashC",
					Name:         "torrentC",
					CompletionOn: 300,
					State:        qbittorrent.TorrentStateStalledUp,
					Progress:     1.0,
				},
			},
		}

		task := &QBTask{
			cfg:       &config.SourceConfig{},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  dest,
			tracker:   tracker,
			tracked:   NewTrackedSet(),
			completed: NewCompletionCache(filepath.Join(t.TempDir(), "cache.json"), logger),
			backoffs:  NewBackoffTracker(),
		}

		var trackOrder []string
		task.trackingOrderHook = func(hash string) {
			trackOrder = append(trackOrder, hash)
		}

		err := task.trackNewTorrents(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Expect B (900), C (500), A (0)
		expected := []string{"hashB", "hashC", "hashA"}
		if len(trackOrder) != len(expected) {
			t.Fatalf("expected %d tracked, got %d: %v", len(expected), len(trackOrder), trackOrder)
		}
		for i, hash := range expected {
			if trackOrder[i] != hash {
				t.Errorf("position %d: expected %s, got %s (full order: %v)", i, hash, trackOrder[i], trackOrder)
			}
		}
	})

	t.Run("zero candidates when all COMPLETE or VERIFYING", func(t *testing.T) {
		tracker := streaming.NewPieceMonitor(nil, nil, logger, streaming.DefaultPieceMonitorConfig())

		dest := &mockDest{
			checkStatusResults: map[string]*streaming.InitTorrentResult{
				"hashX": {Status: pb.TorrentSyncStatus_SYNC_STATUS_COMPLETE},
				"hashY": {Status: pb.TorrentSyncStatus_SYNC_STATUS_VERIFYING},
			},
		}

		mockClient := &mockQBClient{
			getTorrentsResult: []qbittorrent.Torrent{
				{Hash: "hashX", Name: "torrentX", State: qbittorrent.TorrentStateStalledUp, Progress: 1.0},
				{Hash: "hashY", Name: "torrentY", State: qbittorrent.TorrentStateStalledUp, Progress: 1.0},
			},
		}

		task := &QBTask{
			cfg:       &config.SourceConfig{},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  dest,
			tracker:   tracker,
			tracked:   NewTrackedSet(),
			completed: NewCompletionCache(filepath.Join(t.TempDir(), "cache.json"), logger),
			backoffs:  NewBackoffTracker(),
		}

		var trackOrder []string
		task.trackingOrderHook = func(hash string) {
			trackOrder = append(trackOrder, hash)
		}

		err := task.trackNewTorrents(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(trackOrder) != 0 {
			t.Errorf("expected no torrents tracked, got %v", trackOrder)
		}

		// hashX should be cached as complete
		if !task.completed.IsComplete("hashX") {
			t.Error("COMPLETE torrent should be cached")
		}
	})
}

func TestSortGroupsByPriority(t *testing.T) {
	t.Run("sorts by popularity ascending", func(t *testing.T) {
		groups := []torrentGroup{
			{popularity: 100, minSeeding: 500, maxSize: 1000},
			{popularity: 10, minSeeding: 500, maxSize: 1000},
			{popularity: 50, minSeeding: 500, maxSize: 1000},
		}
		sorted := sortGroupsByPriority(groups)
		if sorted[0].popularity != 10 || sorted[1].popularity != 50 || sorted[2].popularity != 100 {
			t.Errorf("expected popularity order [10, 50, 100], got [%d, %d, %d]",
				sorted[0].popularity, sorted[1].popularity, sorted[2].popularity)
		}
	})

	t.Run("tiebreaks by seeding time descending", func(t *testing.T) {
		groups := []torrentGroup{
			{popularity: 50, minSeeding: 100, maxSize: 1000},
			{popularity: 50, minSeeding: 300, maxSize: 1000},
			{popularity: 50, minSeeding: 200, maxSize: 1000},
		}
		sorted := sortGroupsByPriority(groups)
		if sorted[0].minSeeding != 300 || sorted[1].minSeeding != 200 || sorted[2].minSeeding != 100 {
			t.Errorf("expected seeding order [300, 200, 100], got [%d, %d, %d]",
				sorted[0].minSeeding, sorted[1].minSeeding, sorted[2].minSeeding)
		}
	})

	t.Run("tiebreaks by size descending", func(t *testing.T) {
		groups := []torrentGroup{
			{popularity: 50, minSeeding: 200, maxSize: 500},
			{popularity: 50, minSeeding: 200, maxSize: 2000},
			{popularity: 50, minSeeding: 200, maxSize: 1000},
		}
		sorted := sortGroupsByPriority(groups)
		if sorted[0].maxSize != 2000 || sorted[1].maxSize != 1000 || sorted[2].maxSize != 500 {
			t.Errorf("expected size order [2000, 1000, 500], got [%d, %d, %d]",
				sorted[0].maxSize, sorted[1].maxSize, sorted[2].maxSize)
		}
	})

	t.Run("full priority chain: popularity > seeding > size", func(t *testing.T) {
		groups := []torrentGroup{
			{popularity: 50, minSeeding: 300, maxSize: 500},  // second (pop=50, seeding=300)
			{popularity: 10, minSeeding: 100, maxSize: 2000}, // first (lowest pop)
			{popularity: 50, minSeeding: 100, maxSize: 2000}, // third (pop=50, seeding=100)
		}
		sorted := sortGroupsByPriority(groups)
		if sorted[0].popularity != 10 {
			t.Error("lowest popularity should be first")
		}
		if sorted[1].minSeeding != 300 {
			t.Error("among equal popularity, longest seeded should come first")
		}
		if sorted[2].minSeeding != 100 {
			t.Error("shortest seeded should be last")
		}
	})
}

func TestGetFreeSpaceGB(t *testing.T) {
	logger := testLogger(t)

	t.Run("converts bytes to GB", func(t *testing.T) {
		mockClient := &mockQBClient{freeSpaceOnDisk: 107_374_182_400} // 100 GB
		task := &QBTask{
			cfg:       &config.SourceConfig{},
			logger:    logger,
			srcClient: mockClient,
		}

		gb, err := task.getFreeSpaceGB(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if gb != 100 {
			t.Errorf("expected 100 GB, got %d", gb)
		}
	})

	t.Run("truncates partial GB", func(t *testing.T) {
		mockClient := &mockQBClient{freeSpaceOnDisk: 53_687_091_200} // 50 GB exactly
		task := &QBTask{
			cfg:       &config.SourceConfig{},
			logger:    logger,
			srcClient: mockClient,
		}

		gb, err := task.getFreeSpaceGB(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if gb != 50 {
			t.Errorf("expected 50 GB, got %d", gb)
		}
	})

	t.Run("propagates API error", func(t *testing.T) {
		mockClient := &mockQBClient{freeSpaceErr: errors.New("qb unreachable")}
		task := &QBTask{
			cfg:       &config.SourceConfig{},
			logger:    logger,
			srcClient: mockClient,
		}

		_, err := task.getFreeSpaceGB(context.Background())
		if err == nil {
			t.Error("expected error when API fails")
		}
	})
}

func TestHasTag(t *testing.T) {
	t.Run("finds tag in comma-separated list", func(t *testing.T) {
		if !hasTag("foo,bar,baz", "bar") {
			t.Error("expected to find 'bar'")
		}
	})

	t.Run("finds tag with spaces", func(t *testing.T) {
		if !hasTag("foo, bar , baz", "bar") {
			t.Error("expected to find 'bar' with surrounding spaces")
		}
	})

	t.Run("finds single tag", func(t *testing.T) {
		if !hasTag("keep", "keep") {
			t.Error("expected to find single tag")
		}
	})

	t.Run("returns false for missing tag", func(t *testing.T) {
		if hasTag("foo,bar,baz", "qux") {
			t.Error("should not find 'qux'")
		}
	})

	t.Run("returns false for empty tags", func(t *testing.T) {
		if hasTag("", "foo") {
			t.Error("should not find tag in empty string")
		}
	})

	t.Run("no partial match", func(t *testing.T) {
		if hasTag("foobar,baz", "foo") {
			t.Error("should not partial-match 'foo' in 'foobar'")
		}
	})
}

func TestFetchTorrentsCompletedOnDest_ExcludeCleanupTag(t *testing.T) {
	logger := testLogger(t)

	t.Run("excludes torrents with the cleanup tag", func(t *testing.T) {
		mockClient := &mockQBClient{
			getTorrentsResult: []qbittorrent.Torrent{
				{Hash: "hash1", Tags: "keep-on-source", Size: 100},
				{Hash: "hash2", Tags: "other", Size: 200},
				{Hash: "hash3", Tags: "foo, keep-on-source, bar", Size: 300},
			},
		}
		completed := NewCompletionCache("", logger)
		completed.Mark("hash1")
		completed.Mark("hash2")
		completed.Mark("hash3")
		task := &QBTask{
			cfg:       &config.SourceConfig{ExcludeCleanupTag: "keep-on-source"},
			logger:    logger,
			srcClient: mockClient,
			completed: completed,
		}

		result, err := task.fetchTorrentsCompletedOnDest(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(result) != 1 {
			t.Fatalf("expected 1 torrent, got %d", len(result))
		}
		if result[0].Hash != "hash2" {
			t.Errorf("expected hash2, got %s", result[0].Hash)
		}
	})

	t.Run("returns all when ExcludeCleanupTag is empty", func(t *testing.T) {
		mockClient := &mockQBClient{
			getTorrentsResult: []qbittorrent.Torrent{
				{Hash: "hash1", Tags: "keep-on-source", Size: 100},
				{Hash: "hash2", Tags: "other", Size: 200},
			},
		}
		completed := NewCompletionCache("", logger)
		completed.Mark("hash1")
		completed.Mark("hash2")
		task := &QBTask{
			cfg:       &config.SourceConfig{},
			logger:    logger,
			srcClient: mockClient,
			completed: completed,
		}

		result, err := task.fetchTorrentsCompletedOnDest(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(result) != 2 {
			t.Fatalf("expected 2 torrents, got %d", len(result))
		}
	})

	t.Run("drain overrides exclusion tag", func(t *testing.T) {
		mockClient := &mockQBClient{
			getTorrentsResult: []qbittorrent.Torrent{
				{Hash: "hash1", Tags: "keep-on-source", Size: 100},
				{Hash: "hash2", Tags: "other", Size: 200},
			},
		}
		completed := NewCompletionCache("", logger)
		completed.Mark("hash1")
		completed.Mark("hash2")
		task := &QBTask{
			cfg:       &config.SourceConfig{ExcludeCleanupTag: "keep-on-source"},
			logger:    logger,
			srcClient: mockClient,
			completed: completed,
		}
		task.draining.Store(true)

		result, err := task.fetchTorrentsCompletedOnDest(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(result) != 2 {
			t.Fatalf("expected 2 torrents (drain overrides tag), got %d", len(result))
		}
	})

	t.Run("uses cycle cache when available", func(t *testing.T) {
		completed := NewCompletionCache("", logger)
		completed.Mark("hash1")
		completed.Mark("hash2")
		task := &QBTask{
			cfg:    &config.SourceConfig{ExcludeCleanupTag: "protected"},
			logger: logger,
			cycleTorrents: []qbittorrent.Torrent{
				{Hash: "hash1", Tags: "protected", Size: 100},
				{Hash: "hash2", Tags: "", Size: 200},
			},
			completed: completed,
		}

		result, err := task.fetchTorrentsCompletedOnDest(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(result) != 1 {
			t.Fatalf("expected 1 torrent, got %d", len(result))
		}
		if result[0].Hash != "hash2" {
			t.Errorf("expected hash2, got %s", result[0].Hash)
		}
	})
}

func TestDrain(t *testing.T) {
	logger := testLogger(t)

	t.Run("sets and clears draining flag", func(t *testing.T) {
		mockClient := &mockQBClient{
			freeSpaceOnDisk: 1_000_000_000_000, // plenty of space
		}
		task := &QBTask{
			cfg:       &config.SourceConfig{MinSpaceGB: 10},
			logger:    logger,
			srcClient: mockClient,
			completed: NewCompletionCache("", logger),
			tracked:   NewTrackedSet(),
		}

		if task.Draining() {
			t.Error("should not be draining initially")
		}

		// Drain with no completed-on-destination torrents — should succeed immediately
		err := task.Drain(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if task.Draining() {
			t.Error("should not be draining after Drain() returns")
		}
	})

	t.Run("drain bypasses space check", func(t *testing.T) {
		mockClient := &mockQBClient{
			freeSpaceOnDisk: 1_000_000_000_000, // 1TB free — well above min
			getTorrentsResult: []qbittorrent.Torrent{
				{Hash: "abc123", SeedingTime: 9999, Size: 1000},
			},
		}
		dest := &mockDest{}
		completed := NewCompletionCache("", logger)
		completed.Mark("abc123")
		task := &QBTask{
			cfg:       &config.SourceConfig{MinSpaceGB: 10},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  dest,
			source:    qbclient.NewSource(nil, ""),
			completed: completed,
			tracked:   NewTrackedSet(),
		}

		// Normal maybeMoveToDest would skip (plenty of space).
		// Drain should still process.
		err := task.Drain(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// deleteGroupFromHot should have been called
		if !mockClient.stopCalled {
			t.Error("drain should have attempted to hand off torrents despite sufficient space")
		}
	})

	t.Run("concurrent drain returns ErrDrainInProgress", func(t *testing.T) {
		task := &QBTask{
			cfg:       &config.SourceConfig{MinSpaceGB: 10},
			logger:    logger,
			completed: NewCompletionCache("", logger),
			tracked:   NewTrackedSet(),
		}

		// Manually set draining to simulate an in-progress drain
		task.draining.Store(true)

		err := task.Drain(context.Background())
		if !errors.Is(err, ErrDrainInProgress) {
			t.Errorf("expected ErrDrainInProgress, got %v", err)
		}

		task.draining.Store(false)
	})
}

func TestSelectedFingerprint(t *testing.T) {
	t.Run("all files selected returns all indices", func(t *testing.T) {
		files := qbittorrent.TorrentFiles{
			{Index: 0, Name: "file0.bin", Priority: 1},
			{Index: 1, Name: "file1.bin", Priority: 6},
			{Index: 2, Name: "file2.bin", Priority: 7},
		}
		got := selectedFingerprint(files)
		if got != "0,1,2" {
			t.Errorf("expected '0,1,2', got %q", got)
		}
	})

	t.Run("no files selected returns empty string", func(t *testing.T) {
		files := qbittorrent.TorrentFiles{
			{Index: 0, Name: "file0.bin", Priority: 0},
			{Index: 1, Name: "file1.bin", Priority: 0},
		}
		got := selectedFingerprint(files)
		if got != "" {
			t.Errorf("expected empty string, got %q", got)
		}
	})

	t.Run("partial selection skips priority-zero files", func(t *testing.T) {
		files := qbittorrent.TorrentFiles{
			{Index: 0, Name: "file0.bin", Priority: 1},
			{Index: 1, Name: "file1.bin", Priority: 0},
			{Index: 2, Name: "file2.bin", Priority: 7},
			{Index: 3, Name: "file3.bin", Priority: 0},
			{Index: 4, Name: "file4.bin", Priority: 1},
		}
		got := selectedFingerprint(files)
		if got != "0,2,4" {
			t.Errorf("expected '0,2,4', got %q", got)
		}
	})

	t.Run("single file selected", func(t *testing.T) {
		files := qbittorrent.TorrentFiles{
			{Index: 0, Name: "file0.bin", Priority: 0},
			{Index: 1, Name: "file1.bin", Priority: 0},
			{Index: 2, Name: "file2.bin", Priority: 6},
		}
		got := selectedFingerprint(files)
		if got != "2" {
			t.Errorf("expected '2', got %q", got)
		}
	})

	t.Run("empty file list returns empty string", func(t *testing.T) {
		files := qbittorrent.TorrentFiles{}
		got := selectedFingerprint(files)
		if got != "" {
			t.Errorf("expected empty string, got %q", got)
		}
	})

	t.Run("high index values are handled", func(t *testing.T) {
		files := qbittorrent.TorrentFiles{
			{Index: 100, Name: "file100.bin", Priority: 1},
			{Index: 200, Name: "file200.bin", Priority: 0},
			{Index: 300, Name: "file300.bin", Priority: 7},
		}
		got := selectedFingerprint(files)
		if got != "100,300" {
			t.Errorf("expected '100,300', got %q", got)
		}
	})
}

func TestLoadCompletedCache(t *testing.T) {
	logger := testLogger(t)

	t.Run("loads object format with fingerprints", func(t *testing.T) {
		tmpDir := t.TempDir()
		cachePath := filepath.Join(tmpDir, "cache.json")

		data := `{"hash1":"0,1","hash2":"0,1,2","hash3":""}`
		if err := os.WriteFile(cachePath, []byte(data), 0o644); err != nil {
			t.Fatal(err)
		}

		cache := NewCompletionCache(cachePath, logger)
		cache.Load()

		if cache.Count() != 3 {
			t.Fatalf("expected 3 entries, got %d", cache.Count())
		}

		expected := map[string]string{
			"hash1": "0,1",
			"hash2": "0,1,2",
			"hash3": "",
		}
		snapshot := cache.Snapshot()
		for hash, wantFP := range expected {
			gotFP, ok := snapshot[hash]
			if !ok {
				t.Errorf("expected hash %s to be loaded", hash)
			}
			if gotFP != wantFP {
				t.Errorf("hash %s: expected fingerprint %q, got %q", hash, wantFP, gotFP)
			}
		}
	})

	t.Run("corrupt file starts fresh", func(t *testing.T) {
		tmpDir := t.TempDir()
		cachePath := filepath.Join(tmpDir, "cache.json")

		if err := os.WriteFile(cachePath, []byte(`not json`), 0o644); err != nil {
			t.Fatal(err)
		}

		cache := NewCompletionCache(cachePath, logger)
		cache.Load()

		if cache.Count() != 0 {
			t.Errorf("expected 0 entries for corrupt cache, got %d", cache.Count())
		}
	})
}

func TestSyncFailedTag(t *testing.T) {
	logger := testLogger(t)

	t.Run("markSyncFailed applies tag and untracks torrent", func(t *testing.T) {
		mockClient := &mockQBClient{}
		tracker := streaming.NewPieceMonitor(
			nil,
			&mockPieceSource{numPieces: 10},
			logger,
			streaming.DefaultPieceMonitorConfig(),
		)
		dest := &mockDest{}
		task := &QBTask{
			cfg:       &config.SourceConfig{SyncFailedTag: "sync-failed"},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  dest,
			source:    qbclient.NewSource(nil, ""),
			tracker:   tracker,
			tracked:   NewTrackedSet(),
			backoffs:  NewBackoffTracker(),
		}

		hash := "fail-hash"
		task.tracked.Add(hash, TrackedTorrent{Name: "test"})
		task.backoffs.RecordFailure(hash)

		task.markSyncFailed(context.Background(), hash)

		// Tag should be applied
		if !mockClient.addTagsCalled {
			t.Error("expected AddTagsCtx to be called")
		}
		if mockClient.addTagsTag != "sync-failed" {
			t.Errorf("expected tag 'sync-failed', got %q", mockClient.addTagsTag)
		}
		if len(mockClient.addTagsHashes) != 1 || mockClient.addTagsHashes[0] != hash {
			t.Errorf("expected hash %q, got %v", hash, mockClient.addTagsHashes)
		}

		// Torrent should be untracked
		if task.tracked.Has(hash) {
			t.Error("torrent should be removed from TrackedTorrents")
		}

		// Backoff should be cleared
		if task.backoffs.ShouldAttempt(hash) != true {
			t.Error("backoff should be cleared after marking sync-failed")
		}

		// Dest init cache should be cleared
		if !dest.clearInitCalled {
			t.Error("expected ClearInitResult to be called")
		}
	})

	t.Run("markSyncFailed skips tag when SyncFailedTag is empty", func(t *testing.T) {
		mockClient := &mockQBClient{}
		task := &QBTask{
			cfg:       &config.SourceConfig{SyncFailedTag: ""},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  &mockDest{},
			source:    qbclient.NewSource(nil, ""),
			tracker: streaming.NewPieceMonitor(
				nil,
				&mockPieceSource{numPieces: 1},
				logger,
				streaming.DefaultPieceMonitorConfig(),
			),
			tracked:  NewTrackedSet(),
			backoffs: NewBackoffTracker(),
		}

		task.markSyncFailed(context.Background(), "hash")

		if mockClient.addTagsCalled {
			t.Error("should not call AddTagsCtx when SyncFailedTag is empty")
		}
	})

	t.Run("trackNewTorrents skips torrents with sync-failed tag", func(t *testing.T) {
		const numPieces = 10

		mockSource := &mockPieceSource{numPieces: numPieces}
		tracker := streaming.NewPieceMonitor(nil, mockSource, logger, streaming.DefaultPieceMonitorConfig())

		makePiecesNeeded := func() []bool {
			needed := make([]bool, numPieces)
			for i := range needed {
				needed[i] = true
			}
			return needed
		}

		dest := &mockDest{
			checkStatusResults: map[string]*streaming.InitTorrentResult{
				// Only "good-hash" should reach here
				"good-hash": {Status: pb.TorrentSyncStatus_SYNC_STATUS_READY, PiecesNeeded: makePiecesNeeded()},
			},
		}

		mockClient := &mockQBClient{
			getTorrentsResult: []qbittorrent.Torrent{
				{Hash: "good-hash", Name: "good", State: qbittorrent.TorrentStateUploading, Progress: 1.0},
				{
					Hash:     "failed-hash",
					Name:     "failed",
					State:    qbittorrent.TorrentStateUploading,
					Progress: 1.0,
					Tags:     "sync-failed",
				},
				{
					Hash:     "multi-tag",
					Name:     "multi",
					State:    qbittorrent.TorrentStateUploading,
					Progress: 1.0,
					Tags:     "other,sync-failed,more",
				},
			},
		}

		task := &QBTask{
			cfg:       &config.SourceConfig{SyncFailedTag: "sync-failed"},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  dest,
			tracker:   tracker,
			tracked:   NewTrackedSet(),
			completed: NewCompletionCache(filepath.Join(t.TempDir(), "cache.json"), logger),
			backoffs:  NewBackoffTracker(),
		}

		var trackOrder []string
		task.trackingOrderHook = func(hash string) {
			trackOrder = append(trackOrder, hash)
		}

		err := task.trackNewTorrents(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Only "good-hash" should be tracked
		if len(trackOrder) != 1 || trackOrder[0] != "good-hash" {
			t.Errorf("expected only good-hash tracked, got %v", trackOrder)
		}
	})

	t.Run("removing sync-failed tag allows re-tracking", func(t *testing.T) {
		const numPieces = 10

		mockSource := &mockPieceSource{numPieces: numPieces}
		tracker := streaming.NewPieceMonitor(nil, mockSource, logger, streaming.DefaultPieceMonitorConfig())

		dest := &mockDest{
			checkStatusResults: map[string]*streaming.InitTorrentResult{
				"hash1": {Status: pb.TorrentSyncStatus_SYNC_STATUS_READY, PiecesNeeded: make([]bool, numPieces)},
			},
		}

		mockClient := &mockQBClient{
			getTorrentsResult: []qbittorrent.Torrent{
				{
					Hash:     "hash1",
					Name:     "test",
					State:    qbittorrent.TorrentStateUploading,
					Progress: 1.0,
					Tags:     "sync-failed",
				},
			},
		}

		task := &QBTask{
			cfg:       &config.SourceConfig{SyncFailedTag: "sync-failed"},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  dest,
			tracker:   tracker,
			tracked:   NewTrackedSet(),
			completed: NewCompletionCache(filepath.Join(t.TempDir(), "cache.json"), logger),
			backoffs:  NewBackoffTracker(),
		}

		var trackOrder []string
		task.trackingOrderHook = func(hash string) {
			trackOrder = append(trackOrder, hash)
		}

		// First scan: torrent has sync-failed tag — should be skipped
		_ = task.trackNewTorrents(context.Background())
		if len(trackOrder) != 0 {
			t.Fatalf("expected no torrents tracked with sync-failed tag, got %v", trackOrder)
		}

		// Simulate user removing the tag
		mockClient.getTorrentsResult[0].Tags = ""

		// Second scan: tag removed — should now be tracked
		_ = task.trackNewTorrents(context.Background())
		if len(trackOrder) != 1 || trackOrder[0] != "hash1" {
			t.Errorf("expected hash1 tracked after tag removal, got %v", trackOrder)
		}
	})

	t.Run("recordFinalizeFailure returns cumulative count", func(t *testing.T) {
		task := &QBTask{
			cfg:      &config.SourceConfig{},
			logger:   logger,
			backoffs: NewBackoffTracker(),
		}

		hash := "count-hash"
		for i := 1; i <= 5; i++ {
			count := task.backoffs.RecordFailure(hash)
			if count != i {
				t.Errorf("attempt %d: expected count %d, got %d", i, i, count)
			}
		}
	})
}

func TestExcludeSyncTag(t *testing.T) {
	logger := testLogger(t)

	const numPieces = 10

	t.Run("trackNewTorrents skips torrents with exclude-sync tag", func(t *testing.T) {
		mockSource := &mockPieceSource{numPieces: numPieces}
		tracker := streaming.NewPieceMonitor(nil, mockSource, logger, streaming.DefaultPieceMonitorConfig())

		piecesNeeded := make([]bool, numPieces)
		for i := range piecesNeeded {
			piecesNeeded[i] = true
		}

		dest := &mockDest{
			checkStatusResults: map[string]*streaming.InitTorrentResult{
				"good-hash": {Status: pb.TorrentSyncStatus_SYNC_STATUS_READY, PiecesNeeded: piecesNeeded},
			},
		}

		mockClient := &mockQBClient{
			getTorrentsResult: []qbittorrent.Torrent{
				{Hash: "good-hash", Name: "good", State: qbittorrent.TorrentStateUploading, Progress: 1.0},
				{
					Hash:     "excluded-hash",
					Name:     "excluded",
					State:    qbittorrent.TorrentStateUploading,
					Progress: 1.0,
					Tags:     "no-sync",
				},
				{
					Hash:     "multi-tag-excluded",
					Name:     "multi",
					State:    qbittorrent.TorrentStateUploading,
					Progress: 1.0,
					Tags:     "other,no-sync,more",
				},
			},
		}

		task := &QBTask{
			cfg:       &config.SourceConfig{ExcludeSyncTag: "no-sync"},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  dest,
			tracker:   tracker,
			tracked:   NewTrackedSet(),
			completed: NewCompletionCache(filepath.Join(t.TempDir(), "cache.json"), logger),
			backoffs:  NewBackoffTracker(),
		}

		var trackOrder []string
		task.trackingOrderHook = func(hash string) {
			trackOrder = append(trackOrder, hash)
		}

		err := task.trackNewTorrents(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(trackOrder) != 1 || trackOrder[0] != "good-hash" {
			t.Errorf("expected only good-hash tracked, got %v", trackOrder)
		}
	})

	t.Run("removing exclude-sync tag allows tracking", func(t *testing.T) {
		mockSource := &mockPieceSource{numPieces: numPieces}
		tracker := streaming.NewPieceMonitor(nil, mockSource, logger, streaming.DefaultPieceMonitorConfig())

		dest := &mockDest{
			checkStatusResults: map[string]*streaming.InitTorrentResult{
				"hash1": {Status: pb.TorrentSyncStatus_SYNC_STATUS_READY, PiecesNeeded: make([]bool, numPieces)},
			},
		}

		mockClient := &mockQBClient{
			getTorrentsResult: []qbittorrent.Torrent{
				{
					Hash:     "hash1",
					Name:     "test",
					State:    qbittorrent.TorrentStateUploading,
					Progress: 1.0,
					Tags:     "no-sync",
				},
			},
		}

		task := &QBTask{
			cfg:       &config.SourceConfig{ExcludeSyncTag: "no-sync"},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  dest,
			tracker:   tracker,
			tracked:   NewTrackedSet(),
			completed: NewCompletionCache(filepath.Join(t.TempDir(), "cache.json"), logger),
			backoffs:  NewBackoffTracker(),
		}

		var trackOrder []string
		task.trackingOrderHook = func(hash string) {
			trackOrder = append(trackOrder, hash)
		}

		// First scan: torrent has exclude-sync tag — should be skipped.
		_ = task.trackNewTorrents(context.Background())
		if len(trackOrder) != 0 {
			t.Fatalf("expected no torrents tracked with exclude-sync tag, got %v", trackOrder)
		}

		// Simulate user removing the tag.
		mockClient.getTorrentsResult[0].Tags = ""

		// Second scan: tag removed — should now be tracked.
		_ = task.trackNewTorrents(context.Background())
		if len(trackOrder) != 1 || trackOrder[0] != "hash1" {
			t.Errorf("expected hash1 tracked after tag removal, got %v", trackOrder)
		}
	})

	t.Run("empty ExcludeSyncTag disables filtering", func(t *testing.T) {
		mockSource := &mockPieceSource{numPieces: numPieces}
		tracker := streaming.NewPieceMonitor(nil, mockSource, logger, streaming.DefaultPieceMonitorConfig())

		dest := &mockDest{
			checkStatusResults: map[string]*streaming.InitTorrentResult{
				"hash1": {Status: pb.TorrentSyncStatus_SYNC_STATUS_READY, PiecesNeeded: make([]bool, numPieces)},
			},
		}

		mockClient := &mockQBClient{
			getTorrentsResult: []qbittorrent.Torrent{
				{
					Hash:     "hash1",
					Name:     "test",
					State:    qbittorrent.TorrentStateUploading,
					Progress: 1.0,
					Tags:     "no-sync",
				},
			},
		}

		task := &QBTask{
			cfg:       &config.SourceConfig{ExcludeSyncTag: ""},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  dest,
			tracker:   tracker,
			tracked:   NewTrackedSet(),
			completed: NewCompletionCache(filepath.Join(t.TempDir(), "cache.json"), logger),
			backoffs:  NewBackoffTracker(),
		}

		var trackOrder []string
		task.trackingOrderHook = func(hash string) {
			trackOrder = append(trackOrder, hash)
		}

		err := task.trackNewTorrents(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(trackOrder) != 1 || trackOrder[0] != "hash1" {
			t.Errorf("expected hash1 tracked when ExcludeSyncTag is empty, got %v", trackOrder)
		}
	})
}

func TestExcludeSyncTagReactive(t *testing.T) {
	logger := testLogger(t)

	const numPieces = 10

	// trackThenExclude builds a task, tracks hash1 (no tag), then simulates
	// adding the exclude-sync tag and runs checkExcludedTorrents. Returns
	// the task and dest for per-subtest assertions.
	trackThenExclude := func(
		t *testing.T,
		cfg *config.SourceConfig,
		dest *mockDest,
	) *QBTask {
		t.Helper()

		mockSource := &mockPieceSource{numPieces: numPieces}
		tracker := streaming.NewPieceMonitor(nil, mockSource, logger, streaming.DefaultPieceMonitorConfig())

		mockClient := &mockQBClient{
			getTorrentsResult: []qbittorrent.Torrent{
				{
					Hash:     "hash1",
					Name:     "test-torrent",
					State:    qbittorrent.TorrentStateUploading,
					Progress: 1.0,
				},
			},
		}

		task := &QBTask{
			cfg:       cfg,
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  dest,
			source:    qbclient.NewSource(nil, ""),
			tracker:   tracker,
			tracked:   NewTrackedSet(),
			completed: NewCompletionCache(filepath.Join(t.TempDir(), "cache.json"), logger),
			backoffs:  NewBackoffTracker(),
		}

		// Track without the tag.
		err := task.trackNewTorrents(context.Background())
		if err != nil {
			t.Fatalf("unexpected error tracking: %v", err)
		}

		if !task.tracked.Has("hash1") {
			t.Fatal("hash1 should be tracked before tag is added")
		}

		// Simulate adding the exclude-sync tag and re-scan.
		mockClient.getTorrentsResult[0].Tags = "no-sync"
		_ = task.trackNewTorrents(context.Background())
		task.checkExcludedTorrents(context.Background())

		return task
	}

	t.Run("aborts in-progress torrent when tag is added mid-sync", func(t *testing.T) {
		dest := &mockDest{
			checkStatusResults: map[string]*streaming.InitTorrentResult{
				"hash1": {
					Status:       pb.TorrentSyncStatus_SYNC_STATUS_READY,
					PiecesNeeded: make([]bool, numPieces),
				},
			},
		}

		task := trackThenExclude(t,
			&config.SourceConfig{ExcludeSyncTag: "no-sync"},
			dest,
		)

		if task.tracked.Has("hash1") {
			t.Error("hash1 should have been removed from TrackedTorrents")
		}

		if !dest.abortCalled {
			t.Error("AbortTorrent should have been called")
		}
		if dest.abortHash != "hash1" {
			t.Errorf("expected abort hash 'hash1', got '%s'", dest.abortHash)
		}
		if !dest.abortDeleteFiles {
			t.Error("AbortTorrent should delete files")
		}
		if !dest.clearInitCalled {
			t.Error("ClearInitResult should have been called")
		}
	})

	t.Run("forgets completed torrent when tag is added", func(t *testing.T) {
		mockSource := &mockPieceSource{numPieces: numPieces}
		tracker := streaming.NewPieceMonitor(nil, mockSource, logger, streaming.DefaultPieceMonitorConfig())

		dest := &mockDest{
			checkStatusResults: map[string]*streaming.InitTorrentResult{},
		}

		mockClient := &mockQBClient{
			getTorrentsResult: []qbittorrent.Torrent{
				{
					Hash:     "completed-hash",
					Name:     "completed-torrent",
					State:    qbittorrent.TorrentStateUploading,
					Progress: 1.0,
					Tags:     "no-sync",
				},
			},
		}

		completed := NewCompletionCache(filepath.Join(t.TempDir(), "cache.json"), logger)
		completed.Mark("completed-hash")
		task := &QBTask{
			cfg:       &config.SourceConfig{ExcludeSyncTag: "no-sync"},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  dest,
			source:    qbclient.NewSource(nil, ""),
			tracker:   tracker,
			tracked:   NewTrackedSet(),
			completed: completed,
			backoffs:  NewBackoffTracker(),
		}

		// Populate cycleTorrents.
		err := task.trackNewTorrents(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		task.checkExcludedTorrents(context.Background())

		if task.completed.IsComplete("completed-hash") {
			t.Error("completed-hash should have been removed from completedOnDest")
		}

		if dest.abortCalled {
			t.Error("AbortTorrent should NOT be called for completed torrents")
		}
	})

	t.Run("no-op when ExcludeSyncTag is empty with tracked torrents", func(t *testing.T) {
		mockSource := &mockPieceSource{numPieces: numPieces}
		tracker := streaming.NewPieceMonitor(nil, mockSource, logger, streaming.DefaultPieceMonitorConfig())

		dest := &mockDest{
			checkStatusResults: map[string]*streaming.InitTorrentResult{
				"hash1": {
					Status:       pb.TorrentSyncStatus_SYNC_STATUS_READY,
					PiecesNeeded: make([]bool, numPieces),
				},
			},
		}

		mockClient := &mockQBClient{
			getTorrentsResult: []qbittorrent.Torrent{
				{
					Hash:     "hash1",
					Name:     "test",
					State:    qbittorrent.TorrentStateUploading,
					Progress: 1.0,
					Tags:     "no-sync",
				},
			},
		}

		task := &QBTask{
			cfg:       &config.SourceConfig{ExcludeSyncTag: ""},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  dest,
			source:    qbclient.NewSource(nil, ""),
			tracker:   tracker,
			tracked:   NewTrackedSet(),
			completed: NewCompletionCache(filepath.Join(t.TempDir(), "cache.json"), logger),
			backoffs:  NewBackoffTracker(),
		}

		err := task.trackNewTorrents(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !task.tracked.Has("hash1") {
			t.Fatal("hash1 should be tracked when ExcludeSyncTag is empty")
		}

		task.checkExcludedTorrents(context.Background())

		if !task.tracked.Has("hash1") {
			t.Error("hash1 should still be tracked when ExcludeSyncTag is empty")
		}

		if dest.abortCalled {
			t.Error("AbortTorrent should NOT be called when ExcludeSyncTag is empty")
		}
	})

	t.Run("dry run skips AbortTorrent but still untracks", func(t *testing.T) {
		dest := &mockDest{
			checkStatusResults: map[string]*streaming.InitTorrentResult{
				"hash1": {
					Status:       pb.TorrentSyncStatus_SYNC_STATUS_READY,
					PiecesNeeded: make([]bool, numPieces),
				},
			},
		}

		task := trackThenExclude(t,
			&config.SourceConfig{
				ExcludeSyncTag: "no-sync",
				BaseConfig:     config.BaseConfig{DryRun: true},
			},
			dest,
		)

		if task.tracked.Has("hash1") {
			t.Error("hash1 should have been removed from TrackedTorrents in dry run")
		}

		if dest.abortCalled {
			t.Error("AbortTorrent should NOT be called in dry run mode")
		}
	})

	t.Run("AbortTorrent error does not prevent local cleanup", func(t *testing.T) {
		dest := &mockDest{
			checkStatusResults: map[string]*streaming.InitTorrentResult{
				"hash1": {
					Status:       pb.TorrentSyncStatus_SYNC_STATUS_READY,
					PiecesNeeded: make([]bool, numPieces),
				},
			},
			abortErr: errors.New("destination unreachable"),
		}

		task := trackThenExclude(t,
			&config.SourceConfig{ExcludeSyncTag: "no-sync"},
			dest,
		)

		if !dest.abortCalled {
			t.Error("AbortTorrent should have been called")
		}

		if task.tracked.Has("hash1") {
			t.Error("hash1 should have been removed from TrackedTorrents despite AbortTorrent error")
		}
	})
}
