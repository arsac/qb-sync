//go:build e2e

package e2e

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/autobrr/go-qbittorrent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/arsac/qb-sync/internal/arr"
	"github.com/arsac/qb-sync/internal/destination"
)

const (
	arrTestCategory = "radarr-test"
	arrTestAPIKey   = "test-api-key"
	arrTagTimeout   = 90 * time.Second
)

// fakeArr mimics the two Radarr endpoints the filter uses.
//
// It stores rejected hashes uppercase and matches downloadId with an exact,
// case-sensitive comparison, because that is what the real thing does: every
// *arr download client records DownloadId as torrent.Hash.ToUpper(), and the
// history filter is SQL equality against a binary-collated column.
//
// That detail is the whole point of the fake. A server that matched
// case-insensitively would answer correctly no matter what case the client
// sent, so it would pass just as happily against a client that queries with a
// lowercase hash and therefore never matches anything in production.
type fakeArr struct {
	mu       sync.RWMutex
	rejected map[string]bool
	queries  []string
}

func newFakeArr(t *testing.T) (*httptest.Server, *fakeArr) {
	t.Helper()

	arr := &fakeArr{rejected: make(map[string]bool)}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/system/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"appName":"Radarr","version":"5.0.0"}`))
	})
	mux.HandleFunc("/api/v3/history", func(w http.ResponseWriter, r *http.Request) {
		downloadID := r.URL.Query().Get("downloadId")

		arr.mu.Lock()
		arr.queries = append(arr.queries, downloadID)
		isRejected := arr.rejected[downloadID]
		arr.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		if !isRejected {
			_, _ = w.Write([]byte(`{"records":[]}`))
			return
		}
		_, _ = w.Write([]byte(`{"records":[{"eventType":"downloadIgnored","downloadId":"` +
			downloadID + `","date":"2026-04-29T10:00:00Z"}]}`))
	})

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server, arr
}

// reject marks a hash as rejected, stored the way *arr stores it.
func (f *fakeArr) reject(hash string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rejected[strings.ToUpper(hash)] = true
}

// queried reports every downloadId the client asked about.
func (f *fakeArr) queried() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return append([]string(nil), f.queries...)
}

// TestE2E_ArrFilterSkipsRejectedTorrent covers the filter end to end: a torrent
// Radarr has rejected is tagged on the source and never reaches the destination.
//
// The fake Radarr is an [httptest.Server] on 127.0.0.1, reachable from the
// in-process orchestrator without any container networking. Both qBittorrent
// instances are the usual Docker stack.
func TestE2E_ArrFilterSkipsRejectedTorrent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	ctx := context.Background()
	radarr, fake := newFakeArr(t)

	// *arr is configured on the destination: that is where the lookup runs, and
	// the source only relays. The destination server is in-process here, so the
	// httptest server on 127.0.0.1 is reachable with no container networking.
	env := SetupTestEnv(t, WithDestinationServerConfig(func(cfg *destination.ServerConfig) {
		cfg.Arr = arr.Config{
			Radarr: arr.InstanceConfig{
				URL:        radarr.URL,
				APIKey:     arrTestAPIKey,
				Categories: []string{arrTestCategory},
			},
		}
	}))

	env.CleanupBothSides(ctx, wiredCDHash)

	// Reject before the orchestrator starts, so the very first lookup sees it
	// and there is no race between tracking and the verdict.
	fake.reject(wiredCDHash)

	t.Logf("Adding torrent to source in category %q...", arrTestCategory)
	require.NoError(t, env.AddTorrentToSource(ctx, testTorrentURL, map[string]string{
		"category": arrTestCategory,
	}), "adding torrent to source")

	torrent := env.WaitForTorrent(env.SourceClient(), wiredCDHash, torrentAppearTimeout)
	require.NotNil(t, torrent, "torrent should appear in source qBittorrent")

	// The filter is consulted only for torrents that are otherwise eligible, and
	// a torrent at zero progress is excluded before that point.
	t.Log("Waiting for the torrent to reach non-zero progress...")
	require.Eventually(t, func() bool {
		torrents, listErr := env.SourceClient().GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
			Hashes: []string{wiredCDHash},
		})
		return listErr == nil && len(torrents) == 1 && torrents[0].Progress > 0
	}, 2*time.Minute, 2*time.Second, "torrent should reach non-zero progress")

	cfg := env.CreateSourceConfig(
		WithArrSkippedTag(defaultArrSkippedTag),
		WithMinSeedingTime(0),
	)

	task, dest, err := env.CreateSourceTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	orchestratorCtx, cancelOrchestrator := context.WithCancel(ctx)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() { orchestratorDone <- task.Run(orchestratorCtx) }()

	t.Log("Waiting for the arr-skipped tag on the source torrent...")
	env.WaitForSourceTag(ctx, wiredCDHash, defaultArrSkippedTag, arrTagTimeout,
		"a torrent Radarr rejected must be tagged on the source")

	assert.False(t, env.DestinationHasTorrent(ctx, wiredCDHash),
		"a torrent Radarr rejected must never reach the destination")

	// Pin the wire format. Without this the test would still pass against a
	// client that queried with a lowercase hash, because an unmatched hash
	// yields empty history, which fails open to sync - and the assertions above
	// would then fail for a reason that looks nothing like a case mismatch.
	queries := fake.queried()
	require.NotEmpty(t, queries, "the filter should have queried Radarr")
	for _, q := range queries {
		assert.Equal(t, strings.ToUpper(wiredCDHash), q,
			"downloadId must be uppercase: arr stores torrent.Hash.ToUpper() and compares exactly")
	}

	cancelOrchestrator()
	select {
	case <-orchestratorDone:
	case <-time.After(15 * time.Second):
		t.Log("orchestrator shutdown timed out (non-fatal)")
	}

	env.CleanupBothSides(ctx, wiredCDHash)
}
