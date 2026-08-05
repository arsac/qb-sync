package source

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/autobrr/go-qbittorrent"

	"github.com/arsac/qb-sync/internal/arr"
	"github.com/arsac/qb-sync/internal/config"
)

// stubFilter returns a fixed decision and counts how often it was consulted.
// The count is what pins the ordering property: a lookup is a network call, so
// "was it asked at all" matters as much as what it answered.
type stubFilter struct {
	decision arr.Decision
	calls    atomic.Int32
}

func (s *stubFilter) ShouldSync(_ context.Context, _, _ string) arr.Decision {
	s.calls.Add(1)
	return s.decision
}

func (s *stubFilter) ShouldSyncAll(_ context.Context, items []arr.CheckItem) []arr.Decision {
	s.calls.Add(int32(len(items)))
	decisions := make([]arr.Decision, len(items))
	for i := range decisions {
		decisions[i] = s.decision
	}
	return decisions
}

func arrTask(t *testing.T, cfg *config.SourceConfig, filter arr.Filter) (*QBTask, *mockQBClient) {
	t.Helper()
	mock := &mockQBClient{}
	return &QBTask{
		cfg:       cfg,
		srcClient: mock,
		logger:    testStoreLogger(),
		store:     newTorrentStore("", 0, testStoreLogger()),
		arrFilter: filter,
	}, mock
}

func TestApplyArrSkippedTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tag     string
		dryRun  bool
		wantTag bool
	}{
		{name: "applies when configured", tag: "arr-skipped", wantTag: true},
		{name: "empty tag disables tagging", tag: "", wantTag: false},
		{name: "dry run does not mutate qBittorrent", tag: "arr-skipped", dryRun: true, wantTag: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := &config.SourceConfig{ArrSkippedTag: tc.tag}
			cfg.DryRun = tc.dryRun
			task, mock := arrTask(t, cfg, arr.NoopFilter())

			task.applyArrSkippedTag(context.Background(), "abc", arr.ReasonIgnored)

			if mock.addTagsCalled != tc.wantTag {
				t.Errorf("addTagsCalled = %v, want %v", mock.addTagsCalled, tc.wantTag)
			}
			if tc.wantTag && mock.addTagsTag != tc.tag {
				t.Errorf("tag applied = %q, want %q", mock.addTagsTag, tc.tag)
			}
		})
	}
}

func TestRemoveArrSkippedTagIfPresent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tags       string
		dryRun     bool
		wantRemove bool
	}{
		{name: "removes when the torrent carries it", tags: "arr-skipped,synced", wantRemove: true},
		{name: "no call when the tag is absent", tags: "synced", wantRemove: false},
		{name: "dry run does not mutate qBittorrent", tags: "arr-skipped", dryRun: true, wantRemove: false},
		{
			// Whole-tag match: a torrent tagged arr-skipped-manual keeps its tag.
			name:       "does not match a tag that merely contains it",
			tags:       "arr-skipped-manual",
			wantRemove: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := &config.SourceConfig{ArrSkippedTag: "arr-skipped"}
			cfg.DryRun = tc.dryRun
			task, mock := arrTask(t, cfg, arr.NoopFilter())

			task.removeArrSkippedTagIfPresent(context.Background(), "abc", tc.tags)

			if mock.removeTagsCalled != tc.wantRemove {
				t.Errorf("removeTagsCalled = %v, want %v", mock.removeTagsCalled, tc.wantRemove)
			}
		})
	}
}

func TestArrRejects(t *testing.T) {
	t.Parallel()

	t.Run("skip verdict excludes the torrent and marks it", func(t *testing.T) {
		t.Parallel()
		filter := &stubFilter{decision: arr.Decision{Sync: false, Reason: arr.ReasonIgnored}}
		task, mock := arrTask(t, &config.SourceConfig{ArrSkippedTag: "arr-skipped"}, filter)

		if !task.arrRejects(context.Background(), qbittorrent.Torrent{Hash: "abc", Category: "radarr"}) {
			t.Fatal("a rejected torrent must be excluded")
		}
		if !mock.addTagsCalled {
			t.Error("a rejected torrent must be tagged for the operator")
		}
	})

	t.Run("sync verdict clears a stale marker", func(t *testing.T) {
		t.Parallel()
		filter := &stubFilter{decision: arr.Decision{Sync: true, Reason: arr.ReasonNotRejected}}
		task, mock := arrTask(t, &config.SourceConfig{ArrSkippedTag: "arr-skipped"}, filter)

		torrent := qbittorrent.Torrent{Hash: "abc", Category: "radarr", Tags: "arr-skipped"}
		if task.arrRejects(context.Background(), torrent) {
			t.Fatal("a torrent arr accepts must not be excluded")
		}
		if !mock.removeTagsCalled {
			t.Error("the marker must be cleared once the verdict flips back to sync")
		}
	})

	t.Run("the noop filter never excludes", func(t *testing.T) {
		t.Parallel()
		// What an unconfigured deployment actually holds: NewQBTask substitutes
		// the noop filter, so nil never reaches these paths in production.
		task, _ := arrTask(t, &config.SourceConfig{}, arr.NoopFilter())

		if task.arrRejects(context.Background(), qbittorrent.Torrent{Hash: "abc"}) {
			t.Error("an unconfigured deployment must behave as though the filter did not exist")
		}
	})
}

// TestIsExcludedFromTrackingConsultsArrLast pins the ordering. The filter makes
// a network call, so a torrent already ruled out by a local check must never
// reach it: otherwise every quarantined or already-synced torrent would cost an
// *arr request on every cycle.
func TestIsExcludedFromTrackingConsultsArrLast(t *testing.T) {
	t.Parallel()

	syncable := qbittorrent.Torrent{
		Hash:     "abc",
		State:    qbittorrent.TorrentStateUploading,
		Progress: 1,
		Category: "radarr",
	}

	tests := []struct {
		name      string
		mutate    func(*qbittorrent.Torrent)
		setup     func(*QBTask)
		wantCalls int32
	}{
		{
			name:      "eligible torrent is asked",
			wantCalls: 1,
		},
		{
			name:      "non-syncable state short-circuits",
			mutate:    func(tr *qbittorrent.Torrent) { tr.State = qbittorrent.TorrentStateError },
			wantCalls: 0,
		},
		{
			name:      "quarantined short-circuits",
			mutate:    func(tr *qbittorrent.Torrent) { tr.Tags = "sync-failed" },
			wantCalls: 0,
		},
		{
			name:      "already synced short-circuits",
			setup:     func(task *QBTask) { task.store.MarkComplete("abc", "") },
			wantCalls: 0,
		},
		{
			name:      "already tracked short-circuits",
			setup:     func(task *QBTask) { task.store.Track("abc", TrackedTorrent{}) },
			wantCalls: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			filter := &stubFilter{decision: arr.Decision{Sync: true, Reason: arr.ReasonNotRejected}}
			task, _ := arrTask(t, &config.SourceConfig{SyncFailedTag: "sync-failed"}, filter)
			if tc.setup != nil {
				tc.setup(task)
			}
			torrent := syncable
			if tc.mutate != nil {
				tc.mutate(&torrent)
			}

			task.isExcludedFromTracking(context.Background(), torrent)

			if got := filter.calls.Load(); got != tc.wantCalls {
				t.Errorf("arr filter consulted %d times, want %d", got, tc.wantCalls)
			}
		})
	}
}
