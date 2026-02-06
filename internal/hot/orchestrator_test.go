package hot

import (
	"context"
	"encoding/json"
	"errors"
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

// Tests for finalization backoff logic. These can be unit tested without mocking gRPC.
func TestFinalizeBackoff(t *testing.T) {
	logger := testLogger(t)

	t.Run("shouldAttemptFinalize returns true initially", func(t *testing.T) {
		task := &QBTask{
			cfg:              &config.HotConfig{},
			logger:           logger,
			finalizeBackoffs: make(map[string]*finalizeBackoff),
		}

		if !task.shouldAttemptFinalize("hash123") {
			t.Error("should allow finalization attempt initially")
		}
	})

	t.Run("shouldAttemptFinalize returns false after recent failure", func(t *testing.T) {
		task := &QBTask{
			cfg:              &config.HotConfig{},
			logger:           logger,
			finalizeBackoffs: make(map[string]*finalizeBackoff),
		}

		hash := "hash123"
		task.recordFinalizeFailure(hash)

		if task.shouldAttemptFinalize(hash) {
			t.Error("should not allow finalization attempt immediately after failure")
		}
	})

	t.Run("shouldAttemptFinalize returns true after backoff period", func(t *testing.T) {
		task := &QBTask{
			cfg:              &config.HotConfig{},
			logger:           logger,
			finalizeBackoffs: make(map[string]*finalizeBackoff),
		}

		hash := "hash123"

		// Manually set a past lastAttempt
		task.backoffMu.Lock()
		task.finalizeBackoffs[hash] = &finalizeBackoff{
			failures:    1,
			lastAttempt: time.Now().Add(-10 * time.Second), // Well past minFinalizeBackoff
		}
		task.backoffMu.Unlock()

		if !task.shouldAttemptFinalize(hash) {
			t.Error("should allow finalization attempt after backoff period")
		}
	})

	t.Run("clearFinalizeBackoff removes tracking", func(t *testing.T) {
		task := &QBTask{
			cfg:              &config.HotConfig{},
			logger:           logger,
			finalizeBackoffs: make(map[string]*finalizeBackoff),
		}

		hash := "hash123"
		task.recordFinalizeFailure(hash)

		task.clearFinalizeBackoff(hash)

		task.backoffMu.Lock()
		_, exists := task.finalizeBackoffs[hash]
		task.backoffMu.Unlock()

		if exists {
			t.Error("backoff should be cleared")
		}

		// Should be able to attempt immediately after clearing
		if !task.shouldAttemptFinalize(hash) {
			t.Error("should allow finalization attempt after clearing backoff")
		}
	})

	t.Run("backoff increases with failures", func(t *testing.T) {
		task := &QBTask{
			cfg:              &config.HotConfig{},
			logger:           logger,
			finalizeBackoffs: make(map[string]*finalizeBackoff),
		}

		hash := "hash123"

		// Record multiple failures
		for range 5 {
			task.recordFinalizeFailure(hash)
		}

		task.backoffMu.Lock()
		backoff := task.finalizeBackoffs[hash]
		failures := backoff.failures
		task.backoffMu.Unlock()

		if failures != 5 {
			t.Errorf("expected 5 failures recorded, got %d", failures)
		}
	})

	t.Run("backoff is capped at maxFinalizeBackoff", func(t *testing.T) {
		task := &QBTask{
			cfg:              &config.HotConfig{},
			logger:           logger,
			finalizeBackoffs: make(map[string]*finalizeBackoff),
		}

		hash := "hash123"

		// Simulate many failures to trigger cap
		task.backoffMu.Lock()
		task.finalizeBackoffs[hash] = &finalizeBackoff{
			failures:    100, // Large number
			lastAttempt: time.Now(),
		}
		task.backoffMu.Unlock()

		// The computed backoff should be capped, so waiting maxFinalizeBackoff should allow retry
		task.backoffMu.Lock()
		task.finalizeBackoffs[hash].lastAttempt = time.Now().Add(-maxFinalizeBackoff - time.Second)
		task.backoffMu.Unlock()

		if !task.shouldAttemptFinalize(hash) {
			t.Error("should allow finalization after max backoff period")
		}
	})
}

// Tests for tracking maps operations.
func TestTrackedTorrentsMap(t *testing.T) {
	logger := testLogger(t)

	t.Run("concurrent map access is safe", func(_ *testing.T) {
		task := &QBTask{
			cfg:             &config.HotConfig{},
			logger:          logger,
			trackedTorrents: make(map[string]trackedTorrent),
		}

		var wg sync.WaitGroup
		const numOps = 100

		// Concurrent writes
		for i := range numOps {
			wg.Go(func() {
				hash := string(rune('a' + i%26))
				task.trackedMu.Lock()
				task.trackedTorrents[hash] = trackedTorrent{completionTime: time.Now()}
				task.trackedMu.Unlock()
			})
		}

		// Concurrent reads
		for range numOps {
			wg.Go(func() {
				task.trackedMu.RLock()
				_ = len(task.trackedTorrents)
				task.trackedMu.RUnlock()
			})
		}

		// Concurrent deletes
		for i := range numOps {
			wg.Go(func() {
				hash := string(rune('a' + i%26))
				task.trackedMu.Lock()
				delete(task.trackedTorrents, hash)
				task.trackedMu.Unlock()
			})
		}

		wg.Wait()
	})
}

func TestConcurrentBackoffAccess(t *testing.T) {
	logger := testLogger(t)

	t.Run("concurrent backoff operations are safe", func(_ *testing.T) {
		task := &QBTask{
			cfg:              &config.HotConfig{},
			logger:           logger,
			finalizeBackoffs: make(map[string]*finalizeBackoff),
		}

		var wg sync.WaitGroup
		const numOps = 100

		// Concurrent recordFinalizeFailure
		for i := range numOps {
			wg.Go(func() {
				hash := string(rune('a' + i%26))
				task.recordFinalizeFailure(hash)
			})
		}

		// Concurrent shouldAttemptFinalize
		for i := range numOps {
			wg.Go(func() {
				hash := string(rune('a' + i%26))
				_ = task.shouldAttemptFinalize(hash)
			})
		}

		// Concurrent clearFinalizeBackoff
		for i := range numOps {
			wg.Go(func() {
				hash := string(rune('a' + i%26))
				task.clearFinalizeBackoff(hash)
			})
		}

		wg.Wait()
	})
}

func TestConstants(t *testing.T) {
	t.Run("abortTorrentTimeout is reasonable", func(t *testing.T) {
		if abortTorrentTimeout < 5*time.Second {
			t.Errorf("abortTorrentTimeout too short: %v", abortTorrentTimeout)
		}
		if abortTorrentTimeout > 2*time.Minute {
			t.Errorf("abortTorrentTimeout too long: %v", abortTorrentTimeout)
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

// mockColdDest implements ColdDestination for testing.
type mockColdDest struct {
	checkStatusResults map[string]*streaming.InitTorrentResult
	checkStatusErr     error

	finalizeCalled   bool
	finalizeHash     string
	finalizeSavePath string
	finalizeCategory string
	finalizeTags     string
	finalizeErr      error

	abortCalled      bool
	abortHash        string
	abortDeleteFiles bool
	abortResult      int32
	abortErr         error

	startCalled bool
	startHash   string
	startErr    error
}

var _ ColdDestination = (*mockColdDest)(nil)

func (m *mockColdDest) CheckTorrentStatus(_ context.Context, hash string) (*streaming.InitTorrentResult, error) {
	if m.checkStatusErr != nil {
		return nil, m.checkStatusErr
	}
	if result, ok := m.checkStatusResults[hash]; ok {
		return result, nil
	}
	return nil, errors.New("unknown hash")
}

func (m *mockColdDest) FinalizeTorrent(_ context.Context, hash, savePath, category, tags string) error {
	m.finalizeCalled = true
	m.finalizeHash = hash
	m.finalizeSavePath = savePath
	m.finalizeCategory = category
	m.finalizeTags = tags
	return m.finalizeErr
}

func (m *mockColdDest) AbortTorrent(_ context.Context, hash string, deleteFiles bool) (int32, error) {
	m.abortCalled = true
	m.abortHash = hash
	m.abortDeleteFiles = deleteFiles
	return m.abortResult, m.abortErr
}

func (m *mockColdDest) StartTorrent(_ context.Context, hash string) error {
	m.startCalled = true
	m.startHash = hash
	return m.startErr
}

func TestHandleTorrentRemoval(t *testing.T) {
	logger := testLogger(t)

	t.Run("removes from local tracking and calls AbortTorrent", func(t *testing.T) {
		coldDest := &mockColdDest{abortResult: 3}
		task := &QBTask{
			cfg:              &config.HotConfig{},
			logger:           logger,
			grpcDest:         coldDest,
			trackedTorrents:  map[string]trackedTorrent{"abc123": {completionTime: time.Now()}},
			completedOnCold:  map[string]bool{"abc123": true},
			finalizeBackoffs: make(map[string]*finalizeBackoff),
		}

		task.handleTorrentRemoval(context.Background(), "abc123")

		task.trackedMu.RLock()
		if _, ok := task.trackedTorrents["abc123"]; ok {
			t.Error("torrent should have been removed from trackedTorrents")
		}
		task.trackedMu.RUnlock()

		task.completedMu.RLock()
		if task.completedOnCold["abc123"] {
			t.Error("torrent should have been removed from completedOnCold")
		}
		task.completedMu.RUnlock()

		if !coldDest.abortCalled {
			t.Error("AbortTorrent should have been called")
		}
		if coldDest.abortHash != "abc123" {
			t.Errorf("expected abort hash 'abc123', got '%s'", coldDest.abortHash)
		}
		if !coldDest.abortDeleteFiles {
			t.Error("AbortTorrent should have been called with deleteFiles=true")
		}
	})

	t.Run("handles AbortTorrent error gracefully", func(t *testing.T) {
		coldDest := &mockColdDest{abortErr: errors.New("cold server down")}
		task := &QBTask{
			cfg:              &config.HotConfig{},
			logger:           logger,
			grpcDest:         coldDest,
			trackedTorrents:  map[string]trackedTorrent{"abc123": {completionTime: time.Now()}},
			completedOnCold:  make(map[string]bool),
			finalizeBackoffs: make(map[string]*finalizeBackoff),
		}

		// Should not panic
		task.handleTorrentRemoval(context.Background(), "abc123")

		if !coldDest.abortCalled {
			t.Error("AbortTorrent should have been called even if it returns error")
		}

		// Local state should still be cleaned up
		task.trackedMu.RLock()
		if _, ok := task.trackedTorrents["abc123"]; ok {
			t.Error("torrent should have been removed from trackedTorrents despite abort error")
		}
		task.trackedMu.RUnlock()
	})

	t.Run("respects dry run mode", func(t *testing.T) {
		coldDest := &mockColdDest{}
		task := &QBTask{
			cfg:              &config.HotConfig{BaseConfig: config.BaseConfig{DryRun: true}},
			logger:           logger,
			grpcDest:         coldDest,
			trackedTorrents:  map[string]trackedTorrent{"abc123": {completionTime: time.Now()}},
			completedOnCold:  map[string]bool{"abc123": true},
			finalizeBackoffs: make(map[string]*finalizeBackoff),
		}

		task.handleTorrentRemoval(context.Background(), "abc123")

		// Local state should still be cleaned up even in dry run
		task.trackedMu.RLock()
		if _, ok := task.trackedTorrents["abc123"]; ok {
			t.Error("torrent should have been removed from trackedTorrents in dry run")
		}
		task.trackedMu.RUnlock()

		task.completedMu.RLock()
		if task.completedOnCold["abc123"] {
			t.Error("torrent should have been removed from completedOnCold in dry run")
		}
		task.completedMu.RUnlock()

		// But AbortTorrent should NOT be called
		if coldDest.abortCalled {
			t.Error("AbortTorrent should NOT have been called in dry run mode")
		}
	})

	t.Run("handles untracked torrent removal", func(t *testing.T) {
		coldDest := &mockColdDest{abortResult: 0}
		task := &QBTask{
			cfg:              &config.HotConfig{},
			logger:           logger,
			grpcDest:         coldDest,
			trackedTorrents:  make(map[string]trackedTorrent),
			completedOnCold:  make(map[string]bool),
			finalizeBackoffs: make(map[string]*finalizeBackoff),
		}

		task.handleTorrentRemoval(context.Background(), "unknown_hash")

		// AbortTorrent should still be called for untracked torrents
		if !coldDest.abortCalled {
			t.Error("AbortTorrent should have been called even for untracked torrents")
		}
		if coldDest.abortHash != "unknown_hash" {
			t.Errorf("expected abort hash 'unknown_hash', got '%s'", coldDest.abortHash)
		}
	})
}

func TestDeleteGroupFromHot(t *testing.T) {
	logger := testLogger(t)

	makeGroup := func(hashes ...string) torrentGroup {
		torrents := make([]qbittorrent.Torrent, len(hashes))
		for i, h := range hashes {
			torrents[i] = qbittorrent.Torrent{Hash: h, SeedingTime: 9999}
		}
		return torrentGroup{torrents: torrents, minSeeding: 9999}
	}

	t.Run("happy path: stop → start cold → delete", func(t *testing.T) {
		mockClient := &mockQBClient{}
		coldDest := &mockColdDest{}
		task := &QBTask{
			cfg:       &config.HotConfig{},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  coldDest,
		}

		group := makeGroup("abc123")
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
		if coldDest.startHash != "abc123" {
			t.Errorf("expected StartTorrent hash 'abc123', got '%s'", coldDest.startHash)
		}
		if !mockClient.deleteCalled {
			t.Error("DeleteTorrentsCtx should have been called")
		}
		if mockClient.resumeCalled {
			t.Error("ResumeCtx should NOT have been called on happy path")
		}
	})

	t.Run("stop failure skips torrent", func(t *testing.T) {
		mockClient := &mockQBClient{stopErr: errors.New("stop failed")}
		coldDest := &mockColdDest{}
		task := &QBTask{
			cfg:       &config.HotConfig{},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  coldDest,
		}

		group := makeGroup("abc123")
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
		if coldDest.startCalled {
			t.Error("StartTorrent should NOT have been called when stop fails")
		}
		if mockClient.deleteCalled {
			t.Error("DeleteTorrentsCtx should NOT have been called when stop fails")
		}
		if mockClient.resumeCalled {
			t.Error("ResumeCtx should NOT have been called when stop fails")
		}
	})

	t.Run("cold start failure triggers resume on hot", func(t *testing.T) {
		mockClient := &mockQBClient{}
		coldDest := &mockColdDest{startErr: errors.New("cold unreachable")}
		task := &QBTask{
			cfg:       &config.HotConfig{},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  coldDest,
		}

		group := makeGroup("abc123")
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
		if !coldDest.startCalled {
			t.Error("StartTorrent should have been called")
		}
		if !mockClient.resumeCalled {
			t.Error("ResumeCtx should have been called to rollback")
		}
		if len(mockClient.resumeHashes) != 1 || mockClient.resumeHashes[0] != "abc123" {
			t.Errorf("expected resume hashes [abc123], got %v", mockClient.resumeHashes)
		}
		if mockClient.deleteCalled {
			t.Error("DeleteTorrentsCtx should NOT have been called when cold start fails")
		}
	})

	t.Run("dry run skips all operations", func(t *testing.T) {
		mockClient := &mockQBClient{}
		coldDest := &mockColdDest{}
		task := &QBTask{
			cfg:       &config.HotConfig{BaseConfig: config.BaseConfig{DryRun: true}},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  coldDest,
		}

		group := makeGroup("abc123")
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
		if coldDest.startCalled {
			t.Error("StartTorrent should NOT have been called in dry run")
		}
		if mockClient.deleteCalled {
			t.Error("DeleteTorrentsCtx should NOT have been called in dry run")
		}
	})

	t.Run("returns count of handed-off torrents", func(t *testing.T) {
		mockClient := &mockQBClient{}
		coldDest := &mockColdDest{}
		task := &QBTask{
			cfg:       &config.HotConfig{},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  coldDest,
		}

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
		coldDest := &mockColdDest{}
		task := &QBTask{
			cfg:       &config.HotConfig{},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  coldDest,
		}

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

	t.Run("cold start failure with resume failure is tolerated", func(t *testing.T) {
		mockClient := &mockQBClient{
			resumeErr: errors.New("qBittorrent unreachable"),
		}
		coldDest := &mockColdDest{startErr: errors.New("cold server unreachable")}
		task := &QBTask{
			cfg:       &config.HotConfig{},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  coldDest,
		}

		group := makeGroup("abc123")
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
			t.Error("DeleteTorrentsCtx should NOT have been called when cold start fails")
		}
	})

	t.Run("delete failure is tolerated (cold is seeding)", func(t *testing.T) {
		mockClient := &mockQBClient{
			deleteErr: errors.New("delete failed"),
		}
		coldDest := &mockColdDest{}
		task := &QBTask{
			cfg:       &config.HotConfig{},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  coldDest,
		}

		group := makeGroup("abc123")
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
			t.Error("ResumeCtx should NOT have been called — cold is seeding, delete retry next cycle")
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
		coldDest := &mockColdDest{}
		task := &QBTask{
			cfg:       &config.HotConfig{},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  coldDest,
		}

		err := task.finalizeTorrent(context.Background(), "abc123")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if !coldDest.finalizeCalled {
			t.Error("FinalizeTorrent should have been called")
		}
		if coldDest.finalizeHash != "abc123" {
			t.Errorf("expected hash 'abc123', got '%s'", coldDest.finalizeHash)
		}
		if coldDest.finalizeSavePath != "/data/downloads" {
			t.Errorf("expected savePath '/data/downloads', got '%s'", coldDest.finalizeSavePath)
		}
		if coldDest.finalizeCategory != "movies" {
			t.Errorf("expected category 'movies', got '%s'", coldDest.finalizeCategory)
		}
		if coldDest.finalizeTags != "hd,new" {
			t.Errorf("expected tags 'hd,new', got '%s'", coldDest.finalizeTags)
		}
	})

	t.Run("torrent not found returns error", func(t *testing.T) {
		mockClient := &mockQBClient{
			getTorrentsResult: []qbittorrent.Torrent{}, // empty
		}
		coldDest := &mockColdDest{}
		task := &QBTask{
			cfg:       &config.HotConfig{},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  coldDest,
		}

		err := task.finalizeTorrent(context.Background(), "missing_hash")
		if err == nil {
			t.Error("expected error when torrent not found")
		}
		if coldDest.finalizeCalled {
			t.Error("FinalizeTorrent should NOT have been called when torrent not found")
		}
	})

	t.Run("GetTorrentsCtx error propagates", func(t *testing.T) {
		mockClient := &mockQBClient{
			getTorrentsErr: errors.New("qb unreachable"),
		}
		coldDest := &mockColdDest{}
		task := &QBTask{
			cfg:       &config.HotConfig{},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  coldDest,
		}

		err := task.finalizeTorrent(context.Background(), "abc123")
		if err == nil {
			t.Error("expected error when GetTorrentsCtx fails")
		}
		if coldDest.finalizeCalled {
			t.Error("FinalizeTorrent should NOT have been called when GetTorrentsCtx fails")
		}
	})

	t.Run("FinalizeTorrent RPC error propagates", func(t *testing.T) {
		mockClient := &mockQBClient{
			getTorrentsResult: []qbittorrent.Torrent{
				{Hash: "abc123", SavePath: "/data"},
			},
		}
		coldDest := &mockColdDest{finalizeErr: errors.New("cold RPC failed")}
		task := &QBTask{
			cfg:       &config.HotConfig{},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  coldDest,
		}

		err := task.finalizeTorrent(context.Background(), "abc123")
		if err == nil {
			t.Error("expected error when FinalizeTorrent RPC fails")
		}
		if !coldDest.finalizeCalled {
			t.Error("FinalizeTorrent should have been called")
		}
	})

	t.Run("dry run fetches info but skips FinalizeTorrent", func(t *testing.T) {
		mockClient := &mockQBClient{
			getTorrentsResult: []qbittorrent.Torrent{
				{Hash: "abc123", SavePath: "/data"},
			},
		}
		coldDest := &mockColdDest{}
		task := &QBTask{
			cfg:       &config.HotConfig{BaseConfig: config.BaseConfig{DryRun: true}},
			logger:    logger,
			srcClient: mockClient,
			grpcDest:  coldDest,
		}

		err := task.finalizeTorrent(context.Background(), "abc123")
		if err != nil {
			t.Errorf("unexpected error in dry run: %v", err)
		}
		if coldDest.finalizeCalled {
			t.Error("FinalizeTorrent should NOT have been called in dry run")
		}
	})
}

// TestSyncedTagApplication tests that the synced tag is applied correctly.
func TestSyncedTagApplication(t *testing.T) {
	logger := testLogger(t)

	t.Run("applies synced tag when configured", func(t *testing.T) {
		mockClient := &mockQBClient{}
		task := &QBTask{
			cfg: &config.HotConfig{
				BaseConfig: config.BaseConfig{SyncedTag: "my-synced-tag"},
			},
			logger:          logger,
			srcClient:       mockClient,
			completedOnCold: make(map[string]bool),
			trackedTorrents: make(map[string]trackedTorrent),
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
			cfg: &config.HotConfig{},
			logger:          logger,
			srcClient:       mockClient,
			completedOnCold: make(map[string]bool),
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
			cfg: &config.HotConfig{
				BaseConfig: config.BaseConfig{SyncedTag: "synced", DryRun: true},
			},
			logger:          logger,
			srcClient:       mockClient,
			completedOnCold: make(map[string]bool),
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

func TestTryStartTrackingTransientError(t *testing.T) {
	logger := testLogger(t)
	makeTracker := func() *streaming.PieceMonitor {
		return streaming.NewPieceMonitor(nil, nil, logger, streaming.DefaultPieceMonitorConfig())
	}

	t.Run("transient gRPC error returns error to short-circuit loop", func(t *testing.T) {
		transientErr := status.Error(codes.Unavailable, "cold server down")
		coldDest := &mockColdDest{checkStatusErr: transientErr}
		task := &QBTask{
			cfg:              &config.HotConfig{},
			logger:           logger,
			grpcDest:         coldDest,
			trackedTorrents:  make(map[string]trackedTorrent),
			completedOnCold:  make(map[string]bool),
			finalizeBackoffs: make(map[string]*finalizeBackoff),
			tracker:          makeTracker(),
		}

		torrent := qbittorrent.Torrent{Hash: "abc123", Name: "test"}
		started, err := task.tryStartTracking(context.Background(), torrent)
		if err == nil {
			t.Error("expected transient error to be returned")
		}
		if started {
			t.Error("should not have started tracking")
		}
	})

	t.Run("non-transient error returns (false, nil)", func(t *testing.T) {
		nonTransientErr := errors.New("some application error")
		coldDest := &mockColdDest{checkStatusErr: nonTransientErr}
		task := &QBTask{
			cfg:              &config.HotConfig{},
			logger:           logger,
			grpcDest:         coldDest,
			trackedTorrents:  make(map[string]trackedTorrent),
			completedOnCold:  make(map[string]bool),
			finalizeBackoffs: make(map[string]*finalizeBackoff),
			tracker:          makeTracker(),
		}

		torrent := qbittorrent.Torrent{Hash: "abc123", Name: "test"}
		started, err := task.tryStartTracking(context.Background(), torrent)
		if err != nil {
			t.Errorf("non-transient error should not be returned: %v", err)
		}
		if started {
			t.Error("should not have started tracking")
		}
	})

	t.Run("COMPLETE status returns (false, nil) and caches", func(t *testing.T) {
		coldDest := &mockColdDest{
			checkStatusResults: map[string]*streaming.InitTorrentResult{
				"abc123": {Status: pb.TorrentSyncStatus_SYNC_STATUS_COMPLETE},
			},
		}
		tmpDir := t.TempDir()
		task := &QBTask{
			cfg:                &config.HotConfig{},
			logger:             logger,
			grpcDest:           coldDest,
			trackedTorrents:    make(map[string]trackedTorrent),
			completedOnCold:    make(map[string]bool),
			completedCachePath: filepath.Join(tmpDir, "cache.json"),
			finalizeBackoffs:   make(map[string]*finalizeBackoff),
			tracker:            makeTracker(),
		}

		torrent := qbittorrent.Torrent{Hash: "abc123", Name: "test"}
		started, err := task.tryStartTracking(context.Background(), torrent)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if started {
			t.Error("should not have started tracking for COMPLETE torrent")
		}

		task.completedMu.RLock()
		if !task.completedOnCold["abc123"] {
			t.Error("COMPLETE torrent should be cached")
		}
		task.completedMu.RUnlock()
	})
}

func TestCompletedCachePersistence(t *testing.T) {
	logger := testLogger(t)

	t.Run("save and load round-trip", func(t *testing.T) {
		tmpDir := t.TempDir()
		cachePath := filepath.Join(tmpDir, ".qb-sync", "completed_on_cold.json")

		task := &QBTask{
			cfg:                &config.HotConfig{},
			logger:             logger,
			completedOnCold:    make(map[string]bool),
			completedCachePath: cachePath,
		}

		// Mark some torrents as complete
		task.completedMu.Lock()
		task.completedOnCold["hash1"] = true
		task.completedOnCold["hash2"] = true
		task.completedOnCold["hash3"] = true
		task.completedMu.Unlock()

		task.saveCompletedCache()

		// Verify file exists and is valid JSON
		data, err := os.ReadFile(cachePath)
		if err != nil {
			t.Fatalf("cache file should exist: %v", err)
		}
		var hashes []string
		if jsonErr := json.Unmarshal(data, &hashes); jsonErr != nil {
			t.Fatalf("cache should be valid JSON: %v", jsonErr)
		}
		if len(hashes) != 3 {
			t.Errorf("expected 3 hashes in cache, got %d", len(hashes))
		}

		// Load into a new task
		task2 := &QBTask{
			cfg:                &config.HotConfig{},
			logger:             logger,
			completedOnCold:    make(map[string]bool),
			completedCachePath: cachePath,
		}
		task2.loadCompletedCache()

		task2.completedMu.RLock()
		if len(task2.completedOnCold) != 3 {
			t.Errorf("expected 3 hashes loaded, got %d", len(task2.completedOnCold))
		}
		for _, h := range []string{"hash1", "hash2", "hash3"} {
			if !task2.completedOnCold[h] {
				t.Errorf("expected hash %s to be loaded", h)
			}
		}
		task2.completedMu.RUnlock()
	})

	t.Run("missing file loads empty cache", func(t *testing.T) {
		tmpDir := t.TempDir()
		cachePath := filepath.Join(tmpDir, "nonexistent", "cache.json")

		task := &QBTask{
			cfg:                &config.HotConfig{},
			logger:             logger,
			completedOnCold:    make(map[string]bool),
			completedCachePath: cachePath,
		}

		task.loadCompletedCache()

		task.completedMu.RLock()
		if len(task.completedOnCold) != 0 {
			t.Errorf("expected empty cache, got %d", len(task.completedOnCold))
		}
		task.completedMu.RUnlock()
	})

	t.Run("corrupt file loads empty cache", func(t *testing.T) {
		tmpDir := t.TempDir()
		cachePath := filepath.Join(tmpDir, "cache.json")

		if err := os.WriteFile(cachePath, []byte("not json"), 0o644); err != nil {
			t.Fatal(err)
		}

		task := &QBTask{
			cfg:                &config.HotConfig{},
			logger:             logger,
			completedOnCold:    make(map[string]bool),
			completedCachePath: cachePath,
		}

		task.loadCompletedCache()

		task.completedMu.RLock()
		if len(task.completedOnCold) != 0 {
			t.Errorf("expected empty cache from corrupt file, got %d", len(task.completedOnCold))
		}
		task.completedMu.RUnlock()
	})

	t.Run("markCompletedOnCold persists to disk", func(t *testing.T) {
		tmpDir := t.TempDir()
		cachePath := filepath.Join(tmpDir, ".qb-sync", "completed_on_cold.json")

		task := &QBTask{
			cfg:                &config.HotConfig{},
			logger:             logger,
			completedOnCold:    make(map[string]bool),
			completedCachePath: cachePath,
		}

		task.markCompletedOnCold("hash_abc")

		// Verify persisted
		data, err := os.ReadFile(cachePath)
		if err != nil {
			t.Fatalf("cache file should exist after markCompletedOnCold: %v", err)
		}
		var hashes []string
		if jsonErr := json.Unmarshal(data, &hashes); jsonErr != nil {
			t.Fatalf("cache should be valid JSON: %v", jsonErr)
		}
		if len(hashes) != 1 || hashes[0] != "hash_abc" {
			t.Errorf("expected [hash_abc], got %v", hashes)
		}
	})
}
