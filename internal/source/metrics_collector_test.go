package source

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/arsac/qb-sync/internal/streaming"
)

// TestMetricsCollector_GatherSurface confirms the collector emits the metrics
// it advertises. Uses prometheus.testutil.GatherAndCount (which counts
// emitted samples per metric name) — that's the test boundary that matters
// for canonical Prometheus collectors: do dashboards see the names they
// expect on a scrape? Empty state still advertises the global gauges
// (active_torrents, completed_on_dest_cache_size, active_finalization_backoffs).
func TestMetricsCollector_GatherSurface(t *testing.T) {
	t.Parallel()

	logger := testLogger(t)
	tracker := streaming.NewPieceMonitor(
		nil, &mockPieceSource{numPieces: 1}, logger, streaming.DefaultPieceMonitorConfig(),
	)
	task := &QBTask{
		logger:    logger,
		tracker:   tracker,
		tracked:   NewTrackedSet(),
		completed: NewCompletionCache("", logger),
		backoffs:  NewBackoffTracker(),
	}

	registry := prometheus.NewRegistry()
	registry.MustRegister(NewMetricsCollector(task))

	// On empty state, only the three label-less gauges emit a sample.
	for _, metric := range []string{
		"qbsync_active_torrents",
		"qbsync_completed_on_dest_cache_size",
		"qbsync_active_finalization_backoffs",
	} {
		if got := testutil.CollectAndCount(NewMetricsCollector(task), metric); got != 1 {
			t.Errorf("%s sample count = %d, want 1 on empty state", metric, got)
		}
	}

	// Per-torrent metrics emit zero samples when nothing is tracked.
	for _, metric := range []string{
		"qbsync_oldest_pending_sync_seconds",
		"qbsync_torrent_pieces",
		"qbsync_torrent_pieces_streamed",
		"qbsync_torrent_size_bytes",
		"qbsync_torrent_progress_ratio",
		"qbsync_torrent_bytes_streamed",
	} {
		if got := testutil.CollectAndCount(NewMetricsCollector(task), metric); got != 0 {
			t.Errorf("%s sample count = %d, want 0 with empty tracked", metric, got)
		}
	}

	// Sanity: the gather doesn't error out and we see the expected metric set.
	mfs, err := registry.Gather()
	if err != nil {
		t.Fatalf("registry.Gather: %v", err)
	}
	names := make([]string, 0, len(mfs))
	for _, mf := range mfs {
		names = append(names, mf.GetName())
	}
	wantPresent := "qbsync_active_torrents"
	if !contains(names, wantPresent) {
		t.Errorf("Gather() produced %v, want at least %q", names, wantPresent)
	}
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if strings.EqualFold(s, needle) {
			return true
		}
	}
	return false
}
