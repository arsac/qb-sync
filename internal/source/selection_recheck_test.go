package source

import (
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/autobrr/go-qbittorrent"

	"github.com/arsac/qb-sync/internal/config"
)

// newRecheckFixture builds a completed set of n torrents whose stored
// fingerprints already match qBittorrent, so recheckFileSelections probes every
// torrent it visits and resyncs none. probed records the hashes each pass asked
// qBittorrent about.
func newRecheckFixture(t *testing.T, n int) (*QBTask, []string, *[]string) {
	t.Helper()

	filesByHash := make(map[string]qbittorrent.TorrentFiles, n)
	hashes := make([]string, 0, n)
	store := newTorrentStore("", 0, testLogger(t))
	for i := range n {
		hash := fmt.Sprintf("%040x", i)
		files := qbittorrent.TorrentFiles{{Index: 0, Name: hash + "/a.bin", Priority: 1}}
		filesByHash[hash] = files
		store.MarkComplete(hash, selectedFingerprint(files))
		hashes = append(hashes, hash)
	}

	var mu sync.Mutex
	var seen []string
	client := &mockQBClient{
		filesByHash: filesByHash,
		filesHook: func(hash string) {
			mu.Lock()
			seen = append(seen, hash)
			mu.Unlock()
		},
	}

	return &QBTask{
		cfg:       &config.SourceConfig{},
		logger:    testLogger(t),
		srcClient: client,
		store:     store,
	}, hashes, &seen
}

func TestRecheckFileSelections_ShardsAcrossOnePruneInterval(t *testing.T) {
	t.Parallel()

	const torrents = 200

	t.Run("every torrent is probed exactly once per interval", func(t *testing.T) {
		t.Parallel()

		task, hashes, probed := newRecheckFixture(t, torrents)

		perShard := make([]int, pruneCycleInterval)
		for shard := range pruneCycleInterval {
			before := len(*probed)
			task.resetCycleFiles()
			task.recheckFileSelections(t.Context(), shard)
			perShard[shard] = len(*probed) - before
		}

		got := slices.Clone(*probed)
		slices.Sort(got)
		want := slices.Clone(hashes)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Fatalf("probed set over one interval = %d hashes, want the %d completed torrents exactly once each",
				len(got), len(want))
		}

		// A shard is a fraction of the library, which is the whole point: the
		// pass previously spent one qBittorrent round-trip per completed torrent
		// in a single cycle.
		if peak := slices.Max(perShard); peak > torrents/10 {
			t.Fatalf("busiest shard probed %d of %d torrents, want a small fraction", peak, torrents)
		}
	})

	t.Run("allShards covers the whole completed set in one pass", func(t *testing.T) {
		t.Parallel()

		task, hashes, probed := newRecheckFixture(t, torrents)

		task.recheckFileSelections(t.Context(), allShards)

		got := slices.Clone(*probed)
		slices.Sort(got)
		want := slices.Clone(hashes)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Fatalf("allShards probed %d hashes, want all %d", len(got), len(want))
		}
	})
}

func TestSelectionShard_IsStableAndInRange(t *testing.T) {
	t.Parallel()

	for i := range 500 {
		hash := fmt.Sprintf("%040x", i)
		shard := selectionShard(hash)
		if shard < 0 || shard >= pruneCycleInterval {
			t.Fatalf("selectionShard(%s) = %d, want [0,%d)", hash, shard, pruneCycleInterval)
		}
		if again := selectionShard(hash); again != shard {
			t.Fatalf("selectionShard(%s) returned %d then %d", hash, shard, again)
		}
	}
}
