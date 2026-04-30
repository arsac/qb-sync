package source

import (
	"context"
	"testing"

	"github.com/arsac/qb-sync/internal/arr"
	"github.com/arsac/qb-sync/internal/config"
)

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
