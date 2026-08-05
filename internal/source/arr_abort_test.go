package source

import (
	"context"
	"testing"
	"time"

	"github.com/autobrr/go-qbittorrent"

	"github.com/arsac/qb-sync/internal/arr"
	"github.com/arsac/qb-sync/internal/config"
	"github.com/arsac/qb-sync/internal/qbclient"
	"github.com/arsac/qb-sync/internal/streaming"
)

// The mid-flight abort is the only place qb-sync deletes a user's data on the
// strength of a third party's opinion. Quarantine, by deliberate contrast,
// keeps the bytes. So these tests care less about the happy path than about
// every way the filter could delete something it should not have.

// batchFilter returns a fixed decision per hash and records what it was asked.
type batchFilter struct {
	byHash map[string]arr.Decision
	asked  []arr.CheckItem
}

func (b *batchFilter) ShouldSync(ctx context.Context, hash, category string) arr.Decision {
	return b.ShouldSyncAll(ctx, []arr.CheckItem{{Hash: hash, Category: category}})[0]
}

func (b *batchFilter) ShouldSyncAll(_ context.Context, items []arr.CheckItem) []arr.Decision {
	b.asked = append(b.asked, items...)
	decisions := make([]arr.Decision, len(items))
	for i, item := range items {
		d, ok := b.byHash[item.Hash]
		if !ok {
			d = arr.Decision{Sync: true, Reason: arr.ReasonNotRejected}
		}
		decisions[i] = d
	}
	return decisions
}

func recheckTask(t *testing.T, filter arr.Filter) (*QBTask, *mockDest, *mockQBClient) {
	t.Helper()
	dest := &mockDest{}
	client := &mockQBClient{}
	cfg := &config.SourceConfig{ArrSkippedTag: "arr-skipped", SleepInterval: time.Second}

	return &QBTask{
		cfg:       cfg,
		logger:    testStoreLogger(),
		srcClient: client,
		grpcDest:  dest,
		source:    qbclient.NewSource(nil, ""),
		tracker: streaming.NewPieceMonitor(
			nil, &mockPieceSource{numPieces: 1}, testStoreLogger(), streaming.DefaultPieceMonitorConfig(),
		),
		store:     newTorrentStore("", 0, testStoreLogger()),
		arrFilter: filter,
	}, dest, client
}

func TestRecheckArrRejectedTorrents(t *testing.T) {
	t.Parallel()

	const hash = "abc"

	t.Run("a flipped verdict aborts the sync and deletes the partial data", func(t *testing.T) {
		t.Parallel()
		filter := &batchFilter{byHash: map[string]arr.Decision{
			hash: {Sync: false, Reason: arr.ReasonIgnored, Instance: "radarr"},
		}}
		task, dest, client := recheckTask(t, filter)
		task.store.Track(hash, TrackedTorrent{Name: "a movie"})
		task.cycleTorrents = []qbittorrent.Torrent{{Hash: hash, Category: "radarr"}}

		task.recheckArrRejectedTorrents(context.Background())

		if !dest.abortCalled {
			t.Fatal("a rejected torrent must have its in-flight sync aborted")
		}
		if dest.abortHash != hash {
			t.Errorf("aborted %q, want %q", dest.abortHash, hash)
		}
		if !dest.abortDeleteFiles {
			t.Error("the partial data must be deleted: the torrent was never wanted, " +
				"unlike quarantine where the bytes are kept for a retry")
		}
		if !client.addTagsCalled {
			t.Error("the torrent must be tagged so the operator can see why it stopped")
		}
		if task.store.IsTracked(hash) {
			t.Error("an aborted torrent must be released, or it would be re-streamed next cycle")
		}
	})

	t.Run("a sync verdict leaves the transfer alone", func(t *testing.T) {
		t.Parallel()
		filter := &batchFilter{byHash: map[string]arr.Decision{
			hash: {Sync: true, Reason: arr.ReasonNotRejected},
		}}
		task, dest, client := recheckTask(t, filter)
		task.store.Track(hash, TrackedTorrent{Name: "a movie"})
		task.cycleTorrents = []qbittorrent.Torrent{{Hash: hash, Category: "radarr"}}

		task.recheckArrRejectedTorrents(context.Background())

		if dest.abortCalled {
			t.Fatal("an accepted torrent must not be aborted")
		}
		if client.addTagsCalled {
			t.Error("an accepted torrent must not be tagged")
		}
		if !task.store.IsTracked(hash) {
			t.Error("an accepted torrent must stay tracked")
		}
	})

	// Every fail-open reason has to reach here as sync. If any of them were
	// treated as a rejection, an unreachable *arr would delete data instead of
	// merely failing to filter.
	t.Run("fail-open verdicts never delete anything", func(t *testing.T) {
		t.Parallel()

		for _, reason := range []arr.Reason{
			arr.ReasonLookupFailed,
			arr.ReasonCircuitOpen,
			arr.ReasonBudgetExceeded,
			arr.ReasonRelayFailed,
			arr.ReasonNoCategory,
		} {
			t.Run(string(reason), func(t *testing.T) {
				t.Parallel()
				filter := &batchFilter{byHash: map[string]arr.Decision{
					hash: {Sync: true, Reason: reason},
				}}
				task, dest, _ := recheckTask(t, filter)
				task.store.Track(hash, TrackedTorrent{Name: "a movie"})
				task.cycleTorrents = []qbittorrent.Torrent{{Hash: hash, Category: "radarr"}}

				task.recheckArrRejectedTorrents(context.Background())

				if dest.abortCalled {
					t.Errorf("reason %q is fail-open and must never delete data", reason)
				}
			})
		}
	})

	// A torrent that vanished from qBittorrent this cycle has no category, so it
	// cannot be routed. Asking anyway would send an empty category and risk a
	// verdict meant for nothing in particular.
	t.Run("a torrent missing from this cycle is not asked about", func(t *testing.T) {
		t.Parallel()
		filter := &batchFilter{byHash: map[string]arr.Decision{
			hash: {Sync: false, Reason: arr.ReasonIgnored},
		}}
		task, dest, _ := recheckTask(t, filter)
		task.store.Track(hash, TrackedTorrent{Name: "a movie"})
		task.cycleTorrents = nil

		task.recheckArrRejectedTorrents(context.Background())

		if len(filter.asked) != 0 {
			t.Errorf("asked about %d torrents, want 0", len(filter.asked))
		}
		if dest.abortCalled {
			t.Error("a torrent with no known category must not be aborted")
		}
	})

	t.Run("an untracked torrent is never aborted", func(t *testing.T) {
		t.Parallel()
		filter := &batchFilter{byHash: map[string]arr.Decision{
			hash: {Sync: false, Reason: arr.ReasonIgnored},
		}}
		task, dest, _ := recheckTask(t, filter)
		task.cycleTorrents = []qbittorrent.Torrent{{Hash: hash, Category: "radarr"}}

		task.recheckArrRejectedTorrents(context.Background())

		if dest.abortCalled {
			t.Error("nothing is in flight, so there is nothing to abort")
		}
	})

	t.Run("dry run decides but does not act", func(t *testing.T) {
		t.Parallel()
		filter := &batchFilter{byHash: map[string]arr.Decision{
			hash: {Sync: false, Reason: arr.ReasonIgnored},
		}}
		task, dest, client := recheckTask(t, filter)
		task.cfg.DryRun = true
		task.store.Track(hash, TrackedTorrent{Name: "a movie"})
		task.cycleTorrents = []qbittorrent.Torrent{{Hash: hash, Category: "radarr"}}

		task.recheckArrRejectedTorrents(context.Background())

		if dest.abortCalled {
			t.Error("dry run must not delete destination data")
		}
		if client.addTagsCalled {
			t.Error("dry run must not tag the source torrent")
		}
	})
}

// TestRecheckArrRejectedTorrentsIgnoresMismatchedVerdicts guards the worst
// failure this path could have. Verdicts are matched to torrents by index, so a
// response of a different length cannot be attributed safely: acting on it
// would abort whichever torrents happened to line up.
func TestRecheckArrRejectedTorrentsIgnoresMismatchedVerdicts(t *testing.T) {
	t.Parallel()

	task, dest, _ := recheckTask(t, shortFilter{})
	task.store.Track("aaa", TrackedTorrent{Name: "one"})
	task.store.Track("bbb", TrackedTorrent{Name: "two"})
	task.cycleTorrents = []qbittorrent.Torrent{
		{Hash: "aaa", Category: "radarr"},
		{Hash: "bbb", Category: "radarr"},
	}

	task.recheckArrRejectedTorrents(context.Background())

	if dest.abortCalled {
		t.Error("a mismatched verdict list must abort nothing: the verdicts cannot be attributed")
	}
	if !task.store.IsTracked("aaa") || !task.store.IsTracked("bbb") {
		t.Error("both torrents must stay tracked when the verdicts cannot be trusted")
	}
}

// shortFilter returns fewer decisions than it was asked about.
type shortFilter struct{}

func (shortFilter) ShouldSync(context.Context, string, string) arr.Decision {
	return arr.Decision{Sync: true}
}

func (shortFilter) ShouldSyncAll(context.Context, []arr.CheckItem) []arr.Decision {
	return []arr.Decision{{Sync: false, Reason: arr.ReasonIgnored}}
}
