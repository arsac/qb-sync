//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/autobrr/go-qbittorrent"
	"github.com/stretchr/testify/require"

	"github.com/arsac/qb-sync/internal/hot"
)

// PerfMetrics holds performance measurements during a sync test.
type PerfMetrics struct {
	Timestamp       time.Time
	Elapsed         time.Duration
	HotPieces       int     // Completed pieces on hot (download progress)
	ColdPieces      int     // Streamed pieces to cold
	SyncLag         int     // HotPieces - ColdPieces
	SyncLagPercent  float64 // Lag as percentage of hot progress
	InFlight        int     // Pieces currently in flight
	BytesSent       int64   // Total bytes sent
	ThroughputMBps  float64 // MB/s throughput (instantaneous)
	DownloadPercent float64 // Download progress on hot (0-1)
}

// PerfReport summarizes the performance test results.
type PerfReport struct {
	TorrentName    string
	TorrentSize    int64
	TotalPieces    int
	Duration       time.Duration
	Samples        []PerfMetrics
	MaxSyncLag     int
	AvgSyncLag     float64
	MaxSyncLagPct  float64
	AvgThroughput  float64
	PeakThroughput float64
	FinalSyncLag   int
	SyncCompleteAt time.Duration // When sync lag hit zero
}

// TestE2E_PerfActiveTorrent measures sync performance during an active download.
// It downloads a torrent on hot while streaming to cold and measures the lag.
//
// Key metrics:
// - Sync Lag: How many pieces behind is cold compared to hot download progress
// - Throughput: How fast are we streaming pieces to cold
// - Window utilization: How well is the adaptive window performing.
func TestE2E_PerfActiveTorrent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping perf test in short mode")
	}

	env := SetupTestEnv(t)
	ctx := context.Background()

	// Use Sintel torrent - larger than Big Buck Bunny for better measurements
	torrentURL := sintelTorrentURL
	torrentHash := sintelHash

	// Cleanup any existing
	env.CleanupTorrent(ctx, env.HotClient(), torrentHash)
	env.CleanupTorrent(ctx, env.ColdClient(), torrentHash)

	// Add torrent to hot (will start downloading immediately)
	t.Log("Adding torrent to hot (download will start)...")
	err := env.AddTorrentToHot(ctx, torrentURL, nil)
	require.NoError(t, err)

	torrent := env.WaitForTorrent(env.HotClient(), torrentHash, 30*time.Second)
	require.NotNil(t, torrent)

	// Get torrent properties for piece count
	props, err := env.HotClient().GetTorrentPropertiesCtx(ctx, torrentHash)
	require.NoError(t, err)

	t.Logf("Torrent: %s (%.2f MB, %d pieces)",
		torrent.Name,
		float64(torrent.Size)/(1024*1024),
		props.PiecesNum,
	)

	// Start orchestrator
	cfg := env.CreateHotConfig()
	task, dest, err := env.CreateHotTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	orchestratorCtx, cancelOrchestrator := context.WithTimeout(ctx, 10*time.Minute)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- task.Run(orchestratorCtx)
	}()

	// Collect metrics
	report := &PerfReport{
		TorrentName: torrent.Name,
		TorrentSize: torrent.Size,
		TotalPieces: props.PiecesNum,
		Samples:     make([]PerfMetrics, 0),
	}

	startTime := time.Now()
	sampleInterval := 2 * time.Second
	var lastBytesSent int64

	t.Log("Collecting performance metrics...")
	t.Log("Time\t\tDL%\tHot\tCold\tLag\tLag%\tInFlt\tMB/s")
	t.Log("----\t\t---\t---\t----\t---\t----\t-----\t----")

	ticker := time.NewTicker(sampleInterval)
	defer ticker.Stop()

loop:
	for {
		select {
		case <-orchestratorCtx.Done():
			t.Log("Orchestrator context cancelled")
			break loop

		case err := <-orchestratorDone:
			if err != nil && ctx.Err() == nil {
				t.Logf("Orchestrator finished: %v", err)
			}
			break loop

		case <-ticker.C:
			metrics := collectPerfMetrics(ctx, env, task, torrentHash, report.TotalPieces, startTime, lastBytesSent, sampleInterval)
			if metrics == nil {
				continue
			}

			lastBytesSent = metrics.BytesSent
			report.Samples = append(report.Samples, *metrics)

			// Print progress
			t.Logf("%s\t\t%.0f%%\t%d\t%d\t%d\t%.1f%%\t%d\t%.2f",
				metrics.Elapsed.Truncate(time.Second),
				metrics.DownloadPercent*100,
				metrics.HotPieces,
				metrics.ColdPieces,
				metrics.SyncLag,
				metrics.SyncLagPercent,
				metrics.InFlight,
				metrics.ThroughputMBps,
			)

			// Check if sync is complete
			if metrics.HotPieces == report.TotalPieces && metrics.SyncLag == 0 {
				report.SyncCompleteAt = time.Since(startTime)
				break loop
			}

			// Check if torrent is complete on cold (streaming complete)
			if env.IsTorrentCompleteOnCold(ctx, torrentHash) {
				break loop
			}
		}
	}

	report.Duration = time.Since(startTime)
	cancelOrchestrator()
	<-orchestratorDone

	// Calculate summary stats
	calculateReportStats(report)
	printPerfReport(t, report)

	// Cleanup
	env.CleanupTorrent(ctx, env.HotClient(), torrentHash)
	env.CleanupTorrent(ctx, env.ColdClient(), torrentHash)
}

// TestE2E_PerfPreDownloaded measures sync performance on an already-downloaded torrent.
// This isolates streaming performance from download speed.
func TestE2E_PerfPreDownloaded(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping perf test in short mode")
	}

	env := SetupTestEnv(t)
	ctx := context.Background()

	torrentHash := sintelHash

	// Cleanup any existing
	env.CleanupTorrent(ctx, env.HotClient(), torrentHash)
	env.CleanupTorrent(ctx, env.ColdClient(), torrentHash)

	// Add torrent and wait for complete download
	t.Log("Adding torrent and waiting for download to complete...")
	err := env.AddTorrentToHot(ctx, sintelTorrentURL, nil)
	require.NoError(t, err)

	torrent := env.WaitForTorrent(env.HotClient(), torrentHash, 30*time.Second)
	require.NotNil(t, torrent)

	env.WaitForTorrentComplete(env.HotClient(), torrentHash, 10*time.Minute)
	t.Log("Download complete, starting streaming performance test...")

	// Get torrent properties for piece count
	props, err := env.HotClient().GetTorrentPropertiesCtx(ctx, torrentHash)
	require.NoError(t, err)

	t.Logf("Torrent: %s (%.2f MB, %d pieces)",
		torrent.Name,
		float64(torrent.Size)/(1024*1024),
		props.PiecesNum,
	)

	// Start orchestrator
	cfg := env.CreateHotConfig()
	task, dest, err := env.CreateHotTask(cfg)
	require.NoError(t, err)
	defer dest.Close()

	startTime := time.Now()

	orchestratorCtx, cancelOrchestrator := context.WithTimeout(ctx, 5*time.Minute)
	defer cancelOrchestrator()

	orchestratorDone := make(chan error, 1)
	go func() {
		orchestratorDone <- task.Run(orchestratorCtx)
	}()

	// Collect metrics
	report := &PerfReport{
		TorrentName: torrent.Name,
		TorrentSize: torrent.Size,
		TotalPieces: props.PiecesNum,
		Samples:     make([]PerfMetrics, 0),
	}

	sampleInterval := 1 * time.Second
	var lastBytesSent int64

	t.Log("Time\t\tStreamed\tInFlight\tMB/s")
	t.Log("----\t\t--------\t--------\t----")

	ticker := time.NewTicker(sampleInterval)
	defer ticker.Stop()

loop:
	for {
		select {
		case <-orchestratorCtx.Done():
			break loop

		case <-orchestratorDone:
			break loop

		case <-ticker.C:
			progress, err := task.Progress(ctx, torrentHash)
			if err != nil {
				continue
			}

			elapsed := time.Since(startTime)
			bytesSent := progress.BytesSent
			throughput := float64(bytesSent-lastBytesSent) / sampleInterval.Seconds() / (1024 * 1024)
			lastBytesSent = bytesSent

			metrics := PerfMetrics{
				Timestamp:      time.Now(),
				Elapsed:        elapsed,
				HotPieces:      report.TotalPieces,
				ColdPieces:     progress.Streamed,
				SyncLag:        report.TotalPieces - progress.Streamed,
				InFlight:       progress.InFlight,
				BytesSent:      bytesSent,
				ThroughputMBps: throughput,
			}
			if metrics.HotPieces > 0 {
				metrics.SyncLagPercent = float64(metrics.SyncLag) / float64(metrics.HotPieces) * 100
			}
			report.Samples = append(report.Samples, metrics)

			t.Logf("%s\t\t%d/%d\t\t%d\t\t%.2f",
				elapsed.Truncate(time.Second),
				progress.Streamed,
				report.TotalPieces,
				progress.InFlight,
				throughput,
			)

			// Check if torrent is complete on cold (streaming complete)
			if env.IsTorrentCompleteOnCold(ctx, torrentHash) {
				report.SyncCompleteAt = elapsed
				break loop
			}
		}
	}

	report.Duration = time.Since(startTime)
	cancelOrchestrator()
	<-orchestratorDone

	calculateReportStats(report)
	printPerfReport(t, report)

	// Cleanup
	env.CleanupTorrent(ctx, env.HotClient(), torrentHash)
	env.CleanupTorrent(ctx, env.ColdClient(), torrentHash)
}

func collectPerfMetrics(
	ctx context.Context,
	env *TestEnv,
	task *hot.QBTask,
	hash string,
	totalPieces int,
	startTime time.Time,
	lastBytes int64,
	interval time.Duration,
) *PerfMetrics {
	// Get hot progress (download)
	torrents, err := env.HotClient().GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{hash},
	})
	if err != nil || len(torrents) == 0 {
		return nil
	}
	hotTorrent := torrents[0]

	// Calculate hot pieces from progress
	hotPieces := int(float64(totalPieces) * hotTorrent.Progress)

	// Get cold progress (streamed)
	var coldPieces int
	var inFlight int
	var bytesSent int64

	progress, err := task.Progress(ctx, hash)
	if err == nil {
		coldPieces = progress.Streamed
		inFlight = progress.InFlight
		bytesSent = progress.BytesSent
	}

	elapsed := time.Since(startTime)

	metrics := &PerfMetrics{
		Timestamp:       time.Now(),
		Elapsed:         elapsed,
		HotPieces:       hotPieces,
		ColdPieces:      coldPieces,
		SyncLag:         hotPieces - coldPieces,
		InFlight:        inFlight,
		BytesSent:       bytesSent,
		DownloadPercent: hotTorrent.Progress,
	}

	if hotPieces > 0 {
		metrics.SyncLagPercent = float64(metrics.SyncLag) / float64(hotPieces) * 100
	}

	// Calculate throughput
	if interval > 0 {
		metrics.ThroughputMBps = float64(bytesSent-lastBytes) / interval.Seconds() / (1024 * 1024)
	}

	return metrics
}

func calculateReportStats(report *PerfReport) {
	if len(report.Samples) == 0 {
		return
	}

	var totalLag float64
	var totalThroughput float64
	throughputSamples := 0

	for _, s := range report.Samples {
		totalLag += float64(s.SyncLag)

		if s.SyncLag > report.MaxSyncLag {
			report.MaxSyncLag = s.SyncLag
		}
		if s.SyncLagPercent > report.MaxSyncLagPct {
			report.MaxSyncLagPct = s.SyncLagPercent
		}
		if s.ThroughputMBps > report.PeakThroughput {
			report.PeakThroughput = s.ThroughputMBps
		}
		if s.ThroughputMBps > 0 {
			totalThroughput += s.ThroughputMBps
			throughputSamples++
		}
	}

	report.AvgSyncLag = totalLag / float64(len(report.Samples))
	if throughputSamples > 0 {
		report.AvgThroughput = totalThroughput / float64(throughputSamples)
	}

	// Final lag is from last sample
	report.FinalSyncLag = report.Samples[len(report.Samples)-1].SyncLag
}

func printPerfReport(t *testing.T, report *PerfReport) {
	t.Log("")
	t.Log("=== PERFORMANCE REPORT ===")
	t.Logf("Torrent: %s", report.TorrentName)
	t.Logf("Size: %.2f MB, Pieces: %d", float64(report.TorrentSize)/(1024*1024), report.TotalPieces)
	t.Logf("Duration: %s", report.Duration.Truncate(time.Second))
	t.Log("")
	t.Log("Sync Lag Metrics:")
	t.Logf("  Max Lag: %d pieces (%.1f%%)", report.MaxSyncLag, report.MaxSyncLagPct)
	t.Logf("  Avg Lag: %.1f pieces", report.AvgSyncLag)
	t.Logf("  Final Lag: %d pieces", report.FinalSyncLag)
	if report.SyncCompleteAt > 0 {
		t.Logf("  Sync Complete At: %s", report.SyncCompleteAt.Truncate(time.Second))
	}
	t.Log("")
	t.Log("Throughput Metrics:")
	t.Logf("  Avg Throughput: %.2f MB/s", report.AvgThroughput)
	t.Logf("  Peak Throughput: %.2f MB/s", report.PeakThroughput)
	if report.Duration.Seconds() > 0 {
		t.Logf("  Effective Rate: %.2f MB/s", float64(report.TorrentSize)/(1024*1024)/report.Duration.Seconds())
	}
	t.Log("===========================")
}
