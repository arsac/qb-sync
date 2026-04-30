package source

import (
	"context"
	"log/slog"
	"testing"

	"github.com/autobrr/go-qbittorrent"

	"github.com/arsac/qb-sync/internal/arr"
	"github.com/arsac/qb-sync/internal/config"
	"github.com/arsac/qb-sync/internal/qbclient"
	"github.com/arsac/qb-sync/internal/streaming"
)

// stubFilter returns a fixed Decision regardless of input.
type stubFilter struct{ d arr.Decision }

func (s stubFilter) ShouldSync(_ context.Context, _, _ string) arr.Decision { return s.d }

func TestApplyArrSkippedTagSkipsWhenDisabled(t *testing.T) {
	mock := &mockQBClient{}
	task := &QBTask{
		cfg:       &config.SourceConfig{},
		srcClient: mock,
	}
	task.applyArrSkippedTag(context.Background(), "abc", arr.ReasonIgnored)
	if mock.addTagCalls != 0 {
		t.Fatalf("expected no tag calls when ArrSkippedTag is empty, got %d", mock.addTagCalls)
	}
}

func TestApplyArrSkippedTagAppliesWhenEnabled(t *testing.T) {
	mock := &mockQBClient{}
	cfg := &config.SourceConfig{}
	cfg.ArrSkippedTag = "arr-skipped"
	task := &QBTask{cfg: cfg, srcClient: mock}

	task.applyArrSkippedTag(context.Background(), "abc", arr.ReasonIgnored)
	if mock.addTagCalls != 1 {
		t.Fatalf("expected 1 AddTagsCtx call, got %d", mock.addTagCalls)
	}
}

func TestApplyArrSkippedTagSkipsInDryRun(t *testing.T) {
	mock := &mockQBClient{}
	cfg := &config.SourceConfig{}
	cfg.ArrSkippedTag = "arr-skipped"
	cfg.DryRun = true
	task := &QBTask{cfg: cfg, srcClient: mock}

	task.applyArrSkippedTag(context.Background(), "abc", arr.ReasonIgnored)
	if mock.addTagCalls != 0 {
		t.Fatalf("expected no tag calls in dry-run, got %d", mock.addTagCalls)
	}
}

func TestRemoveArrSkippedTagIfPresent(t *testing.T) {
	mock := &mockQBClient{}
	cfg := &config.SourceConfig{}
	cfg.ArrSkippedTag = "arr-skipped"
	task := &QBTask{cfg: cfg, srcClient: mock}

	// Tag absent — no call.
	task.removeArrSkippedTagIfPresent(context.Background(), "abc", "synced")
	if mock.removeTagCalls != 0 {
		t.Fatalf("expected no remove calls when tag absent")
	}

	// Tag present — call once.
	task.removeArrSkippedTagIfPresent(context.Background(), "abc", "arr-skipped,synced")
	if mock.removeTagCalls != 1 {
		t.Fatalf("expected 1 remove call when tag present, got %d", mock.removeTagCalls)
	}
}

func TestIsExcludedFromTrackingSkipsOnArrFilter(t *testing.T) {
	mock := &mockQBClient{}
	cfg := &config.SourceConfig{}
	cfg.ArrSkippedTag = "arr-skipped"
	logger := slog.Default()

	task := &QBTask{
		cfg:       cfg,
		srcClient: mock,
		arrFilter: stubFilter{d: arr.Decision{Sync: false, Reason: arr.ReasonIgnored}},
		tracked:   NewTrackedSet(),
		completed: NewCompletionCache("", logger),
		logger:    logger,
	}
	tor := qbittorrent.Torrent{
		Hash:     "abc",
		Category: "radarr",
		State:    qbittorrent.TorrentStateDownloading,
		Progress: 0.5,
	}

	excluded := task.isExcludedFromTracking(context.Background(), tor)
	if !excluded {
		t.Fatalf("expected exclusion when arr filter says SKIP")
	}
	if mock.addTagCalls != 1 {
		t.Fatalf("expected arr-skipped tag to be applied, got %d calls", mock.addTagCalls)
	}
}

func TestRecheckArrRejectedTorrentsAbortsFlippedVerdict(t *testing.T) {
	logger := slog.Default()
	mockSrc := &mockQBClient{}
	mockDestination := &mockDest{}
	cfg := &config.SourceConfig{}
	cfg.ArrSkippedTag = "arr-skipped"

	tracker := streaming.NewPieceMonitor(nil, nil, logger, streaming.DefaultPieceMonitorConfig())

	task := &QBTask{
		cfg:       cfg,
		srcClient: mockSrc,
		grpcDest:  mockDestination,
		arrFilter: stubFilter{d: arr.Decision{Sync: false, Reason: arr.ReasonIgnored}},
		tracked:   NewTrackedSet(),
		tracker:   tracker,
		source:    qbclient.NewSource(nil, ""),
		backoffs:  NewBackoffTracker(),
		logger:    logger,
	}
	task.tracked.Add("abc", TrackedTorrent{Name: "Movie.2026"})
	task.cycleTorrents = []qbittorrent.Torrent{
		{Hash: "abc", Category: "radarr"},
	}

	task.recheckArrRejectedTorrents(context.Background())

	if mockDestination.abortCalls != 1 {
		t.Fatalf("expected 1 AbortTorrent call, got %d", mockDestination.abortCalls)
	}
	if !mockDestination.lastAbortDeleteFiles {
		t.Fatalf("expected AbortTorrent to be called with deleteFiles=true")
	}
	if mockSrc.addTagCalls != 1 {
		t.Fatalf("expected arr-skipped tag to be applied, got %d calls", mockSrc.addTagCalls)
	}
}
