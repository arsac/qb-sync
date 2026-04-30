package source

import (
	"context"
	"log/slog"
	"testing"

	"github.com/autobrr/go-qbittorrent"

	"github.com/arsac/qb-sync/internal/arr"
	"github.com/arsac/qb-sync/internal/config"
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
