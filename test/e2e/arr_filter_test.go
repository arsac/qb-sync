//go:build e2e

package e2e

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/autobrr/go-qbittorrent"
	"github.com/stretchr/testify/require"
)

// fakeArrServer starts an [httptest.Server] that mimics Radarr's /api/v3 endpoints.
//
// rejectHashes is a concurrency-safe store: call setRejected(hash, true) to make
// the server return a "downloadIgnored" history record for that hash. All other
// hashes return an empty history (accepted — no skip).
//
// The server is closed automatically via t.Cleanup.
func fakeArrServer(t *testing.T) (*httptest.Server, func(hash string, reject bool)) {
	t.Helper()

	var mu sync.RWMutex
	rejected := make(map[string]bool)

	setRejected := func(hash string, reject bool) {
		mu.Lock()
		rejected[hash] = reject
		mu.Unlock()
	}

	mux := http.NewServeMux()

	// GET /api/v3/system/status — Radarr identity probe (Ping).
	mux.HandleFunc("/api/v3/system/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"appName":"Radarr","version":"5.0.0"}`))
	})

	// GET /api/v3/history?downloadId=<hash> — verdict lookup.
	mux.HandleFunc("/api/v3/history", func(w http.ResponseWriter, r *http.Request) {
		hash := r.URL.Query().Get("downloadId")

		mu.RLock()
		isRejected := rejected[hash]
		mu.RUnlock()

		w.Header().Set("Content-Type", "application/json")

		if !isRejected {
			// Empty history → arr filter treats as "not rejected" → SYNC.
			_, _ = w.Write([]byte(`{"records":[]}`))
			return
		}

		// downloadIgnored → arr filter returns SKIP.
		_, _ = w.Write(
			[]byte(
				`{"records":[{"eventType":"downloadIgnored","downloadId":"` + hash + `","date":"2026-04-29T10:00:00Z"}]}`,
			),
		)
	})

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server, setRejected
}

// TestE2E_ArrFilterSkipsRejectedTorrent verifies that a torrent rejected by Radarr
// (downloadIgnored event) is:
//   - never synced to the destination qBittorrent, AND
//   - tagged with "arr-skipped" on the source.
//
// The arr filter and both qBittorrent instances run in-process or in Docker containers
// reachable from the test process. The fake Radarr is an [httptest.Server] (127.0.0.1)
// reachable from the in-process arr filter without any container networking.
func TestE2E_ArrFilterSkipsRejectedTorrent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	// Start the fake Radarr server. It starts with empty history (no rejection),
	// then we flip the hash to "rejected" after the torrent is added so the arr
	// filter sees the rejection on its first lookup.
	radarr, setRejected := fakeArrServer(t)

	// Spin up the docker-compose stack (source qB + destination qB + destination gRPC server).
	env := SetupTestEnv(t)

	// Pre-register the known test torrent hash as rejected so the filter skips it
	// immediately on the first tracking cycle (no race with the lookup).
	setRejected(wiredCDHash, true)

	// Build the source config with the Radarr filter wired to our fake server.
	// Category "radarr-test" will be used for the torrent add; it must be listed
	// in the Radarr categories so the filter routes it to the fake Radarr.
	const (
		testCategory   = "radarr-test"
		arrSkippedTag  = "arr-skipped"
		arrAPIKey      = "test-api-key"
		waitForTagTime = 90 * time.Second
	)

	cfg := env.CreateSourceConfig(
		WithRadarrConfig(radarr.URL, arrAPIKey, []string{testCategory}),
		WithArrSkippedTag(arrSkippedTag),
		// Fast sleep so the filter runs quickly.
		WithMinSeedingTime(0),
	)

	// Cleanup any leftover torrent from a previous failed run.
	env.CleanupBothSides(ctx, wiredCDHash)

	// Add the torrent to source qBittorrent under the radarr-test category.
	t.Logf("Adding torrent %s to source in category %q...", wiredCDHash, testCategory)
	err := env.AddTorrentToSource(ctx, testTorrentURL, map[string]string{
		"category": testCategory,
	})
	if err != nil {
		t.Fatalf("adding torrent to source: %v", err)
	}

	// Wait for the torrent to appear in source qBittorrent.
	torrent := env.WaitForTorrent(env.SourceClient(), wiredCDHash, torrentAppearTimeout)
	if torrent == nil {
		t.Fatal("torrent did not appear in source qBittorrent within timeout")
	}
	t.Logf("Torrent appeared: %s (state=%s progress=%.2f%%)", torrent.Name, torrent.State, torrent.Progress*100)

	// Resume so the torrent transitions to a syncable state (downloading).
	// isExcludedFromTracking skips torrents with Progress <= 0; the torrent must
	// have at least one piece before the orchestrator will consult the arr filter.
	if forceErr := env.SourceClient().ResumeCtx(ctx, []string{wiredCDHash}); forceErr != nil {
		t.Logf("resume torrent: %v (non-fatal, torrent may already be running)", forceErr)
	}

	// Wait for at least one piece to download (progress > 0 is the arr-filter gate).
	// This is best-effort: if there are no seeders the tag wait below still passes
	// because the arr filter fires on progress==0 torrents in later code paths.
	t.Log("Waiting for torrent to reach non-zero progress...")
	require.Eventually(t, func() bool {
		torrents, listErr := env.SourceClient().GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
			Hashes: []string{wiredCDHash},
		})
		if listErr != nil || len(torrents) == 0 {
			return false
		}
		if torrents[0].Progress > 0 {
			t.Logf("Torrent has progress: %.4f%%", torrents[0].Progress*100)
			return true
		}
		return false
	}, 2*time.Minute, 2*time.Second, "torrent should reach non-zero progress")

	// Create and start the source orchestrator (in-process, uses arr filter).
	task, dest, createErr := env.CreateSourceTask(cfg)
	if createErr != nil {
		t.Fatalf("creating source task: %v", createErr)
	}
	defer dest.Close()

	orchestratorCtx, cancelOrchestrator := context.WithCancel(ctx)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- task.Run(orchestratorCtx)
	}()

	// Assert: the arr-skipped tag appears on the source torrent.
	// The orchestrator consults the arr filter on every tracking cycle (SleepInterval = 1s).
	t.Log("Waiting for arr-skipped tag on source torrent...")
	env.WaitForSourceTag(ctx, wiredCDHash, arrSkippedTag, waitForTagTime,
		"arr filter should apply arr-skipped tag to rejected torrent")

	t.Log("arr-skipped tag confirmed on source")

	// Assert: destination qBittorrent never received the torrent.
	if env.DestinationHasTorrent(ctx, wiredCDHash) {
		t.Fatal("destination qBittorrent should NOT have the torrent that was rejected by arr filter")
	}

	t.Log("Confirmed: destination does not have the rejected torrent")

	// Graceful shutdown.
	cancelOrchestrator()
	select {
	case <-orchestratorDone:
	case <-time.After(15 * time.Second):
		t.Log("orchestrator shutdown timed out (non-fatal)")
	}

	env.CleanupBothSides(ctx, wiredCDHash)
}
