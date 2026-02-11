package hot

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/autobrr/go-qbittorrent"
	"golang.org/x/sync/errgroup"

	"github.com/arsac/qb-sync/internal/config"
	"github.com/arsac/qb-sync/internal/congestion"
	"github.com/arsac/qb-sync/internal/metrics"
	"github.com/arsac/qb-sync/internal/qbclient"
	"github.com/arsac/qb-sync/internal/streaming"
	"github.com/arsac/qb-sync/internal/utils"
	pb "github.com/arsac/qb-sync/proto"
)

// ErrDrainInProgress is returned when Drain is called while another drain is running.
var ErrDrainInProgress = errors.New("drain already in progress")

// errSkipTorrent is returned by queryColdStatus when a torrent is not eligible
// for finalization (already tracked, complete, verifying, or non-transient error).
var errSkipTorrent = errors.New("torrent not eligible for finalization")

const (
	bytesPerGB = int64(1024 * 1024 * 1024)

	// Streaming queue configuration defaults.
	defaultRetryDelay = 5 * time.Second

	// Finalization retry settings - exponential backoff.
	minFinalizeBackoff = 2 * time.Second
	maxFinalizeBackoff = 30 * time.Second

	// Timeout for unary RPCs to cold server during removal/handoff.
	coldRPCTimeout = 30 * time.Second
)

// finalizeBackoff tracks exponential backoff state for finalization retries.
type finalizeBackoff struct {
	failures    int
	lastAttempt time.Time
}

// trackedTorrent holds metadata for a torrent being synced from hot to cold.
type trackedTorrent struct {
	completionTime time.Time // when the torrent finished downloading on hot (from qbittorrent CompletionOn)
	name           string    // torrent name for metric labels
	size           int64     // torrent size in bytes for TorrentBytesSyncedTotal metric
}

// completionTimeOrNow converts a qBittorrent CompletionOn unix timestamp to time.Time.
// Returns time.Now() as fallback when the value is invalid (qBittorrent uses -1 for
// torrents that were never tracked for completion).
func completionTimeOrNow(completionOn int64) time.Time {
	if completionOn > 0 {
		return time.Unix(completionOn, 0)
	}
	return time.Now()
}

// QBTask orchestrates torrent streaming from hot to cold.
type QBTask struct {
	cfg       *config.HotConfig
	logger    *slog.Logger
	srcClient qbclient.Client
	grpcDest  ColdDestination

	// Streaming components
	source  *qbclient.Source
	tracker *streaming.PieceMonitor
	queue   *streaming.BidiQueue

	// Tracked torrents currently being streamed
	trackedTorrents map[string]trackedTorrent
	trackedMu       sync.RWMutex

	// Torrents known to be complete on cold (persisted to disk)
	// Cold qBittorrent is the source of truth; synced tag is for visibility only
	completedOnCold    map[string]bool
	completedMu        sync.RWMutex
	completedCachePath string

	// Finalization backoff tracking per torrent
	finalizeBackoffs map[string]*finalizeBackoff
	backoffMu        sync.Mutex

	// Cycle counter for periodic pruning of completedOnCold
	pruneCycleCount int

	// Per-cycle cache of torrents to avoid redundant GetTorrentsCtx calls.
	// Set by trackNewTorrents, consumed by fetchTorrentsCompletedOnCold, reset each cycle.
	// nil means not yet fetched this cycle; non-nil (even empty) means cached.
	cycleTorrents []qbittorrent.Torrent

	// trackingOrderHook is called with each hash when tracking starts. Test-only.
	trackingOrderHook func(hash string)

	// draining is set by Drain() to bypass space/seeding checks in maybeMoveToCold.
	draining atomic.Bool
}

// NewQBTask creates a new QBTask with streaming integration.
func NewQBTask(
	cfg *config.HotConfig,
	dest *streaming.GRPCDestination,
	logger *slog.Logger,
) (*QBTask, error) {
	rawClient := qbittorrent.NewClient(qbittorrent.Config{
		Host:     cfg.QBURL,
		Username: cfg.QBUsername,
		Password: cfg.QBPassword,
	})

	// Wrap with resilient client for automatic retry and circuit breaker
	qbConfig := qbclient.DefaultConfig()
	srcClient := qbclient.NewResilientClient(
		rawClient,
		qbConfig,
		logger.With("component", "qb-client"),
		metrics.ModeHot,
	)

	// Create QBSource with resilient client
	source := qbclient.NewSource(srcClient, cfg.DataPath)

	// Create PieceMonitor (uses raw client for MainData.Update, but has internal retry)
	monitorConfig := streaming.DefaultPieceMonitorConfig()
	tracker := streaming.NewPieceMonitor(rawClient, source, logger, monitorConfig)

	// Create streaming queue with adaptive congestion control and multi-stream pooling
	windowConfig := congestion.DefaultConfig()
	if cfg.PieceTimeout > 0 {
		windowConfig.PieceTimeout = cfg.PieceTimeout
	}
	queueConfig := streaming.DefaultBidiQueueConfig()
	queueConfig.MaxBytesPerSec = cfg.MaxBytesPerSec
	queueConfig.RetryDelay = defaultRetryDelay
	queueConfig.AdaptiveWindow = windowConfig
	if cfg.ReconnectMaxDelay > 0 {
		queueConfig.ReconnectMaxDelay = cfg.ReconnectMaxDelay
	}
	if cfg.NumSenders > 0 {
		queueConfig.NumSenders = cfg.NumSenders
	}
	queue := streaming.NewBidiQueue(source, dest, tracker, logger, queueConfig)

	t := &QBTask{
		cfg:                cfg,
		logger:             logger,
		srcClient:          srcClient,
		grpcDest:           dest,
		source:             source,
		tracker:            tracker,
		queue:              queue,
		trackedTorrents:    make(map[string]trackedTorrent),
		completedOnCold:    make(map[string]bool),
		completedCachePath: filepath.Join(cfg.DataPath, ".qb-sync", "completed_on_cold.json"),
		finalizeBackoffs:   make(map[string]*finalizeBackoff),
	}

	t.loadCompletedCache()

	return t, nil
}

// Login authenticates with the source qBittorrent instance and initializes
// path resolution by querying qBittorrent's default save path.
// Uses resilient client with automatic retry for transient errors.
func (t *QBTask) Login(ctx context.Context) error {
	if err := t.srcClient.LoginCtx(ctx); err != nil {
		return err
	}
	return t.source.Init(ctx)
}

// QBLogin is an alias for Login, used for health checks.
func (t *QBTask) QBLogin(ctx context.Context) error {
	return t.Login(ctx)
}

// RunOnce executes a single iteration of the main loop.
// Exported for testing (used by E2E tests in test/e2e/).
func (t *QBTask) RunOnce(ctx context.Context) {
	t.runOnce(ctx)
}

// MaybeMoveToCold is the exported version of maybeMoveToCold for testing.
func (t *QBTask) MaybeMoveToCold(ctx context.Context) error {
	return t.maybeMoveToCold(ctx)
}

// Drain triggers a one-shot evacuation: hands off ALL synced torrents to cold,
// ignoring MinSpaceGB and MinSeedingTime. Blocks until the cycle completes.
// Returns ErrDrainInProgress if a drain is already running.
func (t *QBTask) Drain(ctx context.Context) error {
	if !t.draining.CompareAndSwap(false, true) {
		return ErrDrainInProgress
	}
	defer func() {
		t.draining.Store(false)
		metrics.Draining.Set(0)
	}()
	metrics.Draining.Set(1)
	t.logger.InfoContext(ctx, "drain started")
	err := t.maybeMoveToCold(ctx)
	if err != nil {
		t.logger.ErrorContext(ctx, "drain failed", "error", err)
	} else {
		t.logger.InfoContext(ctx, "drain complete")
	}
	return err
}

// Draining reports whether a drain is in progress.
func (t *QBTask) Draining() bool {
	return t.draining.Load()
}

// Progress returns the streaming progress for a torrent.
// Exported for testing (used by E2E tests).
func (t *QBTask) Progress(_ context.Context, hash string) (streaming.StreamProgress, error) {
	return t.tracker.GetProgress(hash)
}

// FetchCompletedOnCold returns torrents known to be complete on cold.
// Exported for testing (used by E2E tests).
func (t *QBTask) FetchCompletedOnCold() []string {
	t.completedMu.RLock()
	defer t.completedMu.RUnlock()
	result := make([]string, 0, len(t.completedOnCold))
	for hash := range t.completedOnCold {
		result = append(result, hash)
	}
	return result
}

// MarkCompletedOnCold marks a torrent as complete on cold.
// Exported for testing only - allows tests to simulate synced state.
func (t *QBTask) MarkCompletedOnCold(hash string) {
	t.completedMu.Lock()
	t.completedOnCold[hash] = true
	t.completedMu.Unlock()
}

// loadCompletedCache reads the persisted completed-on-cold cache from disk.
// Missing or corrupt file is non-fatal — starts with empty cache.
func (t *QBTask) loadCompletedCache() {
	data, err := os.ReadFile(t.completedCachePath)
	if err != nil {
		if !os.IsNotExist(err) {
			t.logger.Warn("failed to read completed cache, starting fresh",
				"path", t.completedCachePath,
				"error", err,
			)
		}
		return
	}

	var hashes []string
	if jsonErr := json.Unmarshal(data, &hashes); jsonErr != nil {
		t.logger.Warn("failed to parse completed cache, starting fresh",
			"path", t.completedCachePath,
			"error", jsonErr,
		)
		return
	}

	t.completedMu.Lock()
	for _, hash := range hashes {
		t.completedOnCold[hash] = true
	}
	metrics.CompletedOnColdCacheSize.Set(float64(len(t.completedOnCold)))
	t.completedMu.Unlock()

	t.logger.Info("loaded completed-on-cold cache",
		"count", len(hashes),
		"path", t.completedCachePath,
	)
}

// saveCompletedCache atomically persists the completed-on-cold cache to disk.
// Caller must NOT hold completedMu.
func (t *QBTask) saveCompletedCache() {
	t.completedMu.RLock()
	hashes := make([]string, 0, len(t.completedOnCold))
	for hash := range t.completedOnCold {
		hashes = append(hashes, hash)
	}
	t.completedMu.RUnlock()

	data, err := json.Marshal(hashes)
	if err != nil {
		t.logger.Warn("failed to marshal completed cache", "error", err)
		return
	}

	dir := filepath.Dir(t.completedCachePath)
	if mkErr := os.MkdirAll(dir, 0o750); mkErr != nil {
		t.logger.Warn("failed to create cache directory", "path", dir, "error", mkErr)
		return
	}

	tmp := t.completedCachePath + ".tmp"
	f, createErr := os.Create(tmp)
	if createErr != nil {
		t.logger.Warn("failed to create temp cache file", "error", createErr)
		return
	}

	if _, writeErr := f.Write(data); writeErr != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		t.logger.Warn("failed to write completed cache", "error", writeErr)
		return
	}
	if syncErr := f.Sync(); syncErr != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		t.logger.Warn("failed to fsync completed cache", "error", syncErr)
		return
	}
	_ = f.Close()

	if renameErr := os.Rename(tmp, t.completedCachePath); renameErr != nil {
		_ = os.Remove(tmp)
		t.logger.Warn("failed to rename completed cache", "error", renameErr)
	}
}

// markCompletedOnCold marks a torrent as complete on cold, updates the metric,
// and persists the cache to disk.
func (t *QBTask) markCompletedOnCold(hash string) {
	t.completedMu.Lock()
	t.completedOnCold[hash] = true
	metrics.CompletedOnColdCacheSize.Set(float64(len(t.completedOnCold)))
	t.completedMu.Unlock()

	t.saveCompletedCache()
}

// pruneCompletedOnCold removes entries from the completedOnCold cache that are
// no longer present in hot qBittorrent. This prevents unbounded growth when
// torrents are deleted from hot after being synced to cold.
func (t *QBTask) pruneCompletedOnCold(ctx context.Context) {
	torrents, err := t.srcClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{})
	if err != nil {
		t.logger.WarnContext(ctx, "failed to fetch torrents for cache pruning", "error", err)
		return
	}

	hotHashes := make(map[string]struct{}, len(torrents))
	for _, torrent := range torrents {
		hotHashes[torrent.Hash] = struct{}{}
	}

	t.completedMu.Lock()
	var pruned int
	for hash := range t.completedOnCold {
		if _, exists := hotHashes[hash]; !exists {
			delete(t.completedOnCold, hash)
			pruned++
		}
	}
	remaining := len(t.completedOnCold)
	metrics.CompletedOnColdCacheSize.Set(float64(remaining))
	t.completedMu.Unlock()

	if pruned > 0 {
		t.logger.InfoContext(ctx, "pruned completed-on-cold cache",
			"pruned", pruned,
			"remaining", remaining,
		)
		t.saveCompletedCache()
	}
}

// Run executes the QBTask main loop.
func (t *QBTask) Run(ctx context.Context) error {
	if err := t.Login(ctx); err != nil {
		return fmt.Errorf("logging into source: %w", err)
	}

	// Use errgroup for proper goroutine lifecycle management
	g, gCtx := errgroup.WithContext(ctx)

	// Start the piece monitor in background
	g.Go(func() error {
		if err := t.tracker.Run(gCtx); err != nil && gCtx.Err() == nil {
			t.logger.ErrorContext(gCtx, "piece monitor error", "error", err)
			return fmt.Errorf("piece monitor failed: %w", err)
		}
		return nil
	})

	// Start the streaming queue in background
	g.Go(func() error {
		if err := t.queue.Run(gCtx); err != nil && gCtx.Err() == nil {
			t.logger.ErrorContext(gCtx, "streaming queue error", "error", err)
			return fmt.Errorf("streaming queue failed: %w", err)
		}
		return nil
	})

	// Main orchestration loop
	g.Go(func() error {
		ticker := time.NewTicker(t.cfg.SleepInterval)
		defer ticker.Stop()

		for {
			t.runOnce(gCtx)

			select {
			case <-gCtx.Done():
				return gCtx.Err()
			case <-ticker.C:
			}
		}
	})

	// Listen for torrent removals and clean up cold server
	g.Go(func() error {
		return t.listenForRemovals(gCtx)
	})

	// Wait for all goroutines to complete
	return g.Wait()
}

const pruneCycleInterval = 50

func (t *QBTask) runOnce(ctx context.Context) {
	t.cycleTorrents = nil

	if err := t.trackNewTorrents(ctx); err != nil {
		t.logger.ErrorContext(ctx, "failed to track torrents", "error", err)
	}
	if err := t.finalizeCompletedStreams(ctx); err != nil {
		t.logger.ErrorContext(ctx, "failed to finalize streams", "error", err)
	}
	if !t.Draining() {
		if err := t.maybeMoveToCold(ctx); err != nil {
			t.logger.ErrorContext(ctx, "failed to move torrents", "error", err)
		}
	}
	t.trackedMu.RLock()
	metrics.ActiveTorrents.WithLabelValues(metrics.ModeHot).Set(float64(len(t.trackedTorrents)))
	t.trackedMu.RUnlock()
	t.updateSyncAgeGauge()
	t.updateTorrentProgressGauges()

	t.pruneCycleCount++
	if t.pruneCycleCount >= pruneCycleInterval {
		t.pruneCycleCount = 0
		t.pruneCompletedOnCold(ctx)
	}
}

// updateSyncAgeGauge sets the oldest_pending_sync_seconds gauge per tracked torrent.
// Resets first to clear stale labels from previously finalized/removed torrents.
func (t *QBTask) updateSyncAgeGauge() {
	t.trackedMu.RLock()
	defer t.trackedMu.RUnlock()
	metrics.OldestPendingSyncSeconds.Reset()
	for hash, tt := range t.trackedTorrents {
		age := time.Since(tt.completionTime).Seconds()
		metrics.OldestPendingSyncSeconds.WithLabelValues(hash, tt.name).Set(age)
	}
}

// updateTorrentProgressGauges sets per-torrent progress gauges for Grafana dashboards.
// Resets first to clear stale labels from previously finalized/removed torrents.
func (t *QBTask) updateTorrentProgressGauges() {
	t.trackedMu.RLock()
	defer t.trackedMu.RUnlock()

	metrics.TorrentPieces.Reset()
	metrics.TorrentPiecesStreamed.Reset()
	metrics.TorrentSizeBytes.Reset()

	for hash, tt := range t.trackedTorrents {
		progress, err := t.tracker.GetProgress(hash)
		if err != nil {
			continue
		}
		metrics.TorrentPieces.WithLabelValues(hash, tt.name).Set(float64(progress.TotalPieces))
		metrics.TorrentPiecesStreamed.WithLabelValues(hash, tt.name).Set(float64(progress.Streamed))
		metrics.TorrentSizeBytes.WithLabelValues(hash, tt.name).Set(float64(tt.size))
	}
}

// isSyncableState returns true for torrent states where pieces can be read and synced.
func isSyncableState(state qbittorrent.TorrentState) bool {
	switch state { //nolint:exhaustive // Only positive matches matter; all other states are non-syncable.
	case qbittorrent.TorrentStateDownloading,
		qbittorrent.TorrentStateStalledDl,
		qbittorrent.TorrentStateQueuedDl,
		qbittorrent.TorrentStateForcedDl,
		qbittorrent.TorrentStateUploading,
		qbittorrent.TorrentStateStalledUp,
		qbittorrent.TorrentStateQueuedUp,
		qbittorrent.TorrentStateForcedUp:
		return true
	default:
		return false
	}
}

// candidateTorrent pairs a torrent with its cold status for priority sorting.
type candidateTorrent struct {
	torrent    qbittorrent.Torrent
	coldResult *streaming.InitTorrentResult
}

// trackNewTorrents starts tracking new syncable torrents (downloading or completed).
// Uses a two-pass approach: first queries cold for status, then sorts candidates
// by progress (most pieces on cold first) before tracking. This ensures
// partially-streamed torrents are prioritized after restart.
func (t *QBTask) trackNewTorrents(ctx context.Context) error {
	torrents, err := t.srcClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{})
	if err != nil {
		return fmt.Errorf("fetching torrents: %w", err)
	}

	// Cache for reuse by fetchTorrentsCompletedOnCold in the same cycle
	t.cycleTorrents = torrents

	// Pass 1: Query cold status for each untracked torrent, collect READY candidates.
	var candidates []candidateTorrent
	for _, torrent := range torrents {
		// Skip torrents that aren't in a syncable state
		if !isSyncableState(torrent.State) {
			continue
		}

		// Skip torrents with no downloaded pieces yet
		if torrent.Progress <= 0 {
			continue
		}

		// Check local cache first (fast path)
		t.completedMu.RLock()
		knownComplete := t.completedOnCold[torrent.Hash]
		t.completedMu.RUnlock()
		if knownComplete {
			continue
		}

		// Check if already tracking
		t.trackedMu.RLock()
		_, alreadyTracked := t.trackedTorrents[torrent.Hash]
		t.trackedMu.RUnlock()
		if alreadyTracked {
			continue
		}

		result, coldErr := t.queryColdStatus(ctx, torrent)
		if coldErr != nil {
			if errors.Is(coldErr, errSkipTorrent) {
				continue
			}
			t.logger.WarnContext(ctx, "cold server unreachable, skipping remaining torrents",
				"error", coldErr,
			)
			break
		}
		candidates = append(candidates, candidateTorrent{torrent: torrent, coldResult: result})
	}

	// Pass 2: Sort candidates by progress on cold (most complete first), then track.
	slices.SortFunc(candidates, func(a, b candidateTorrent) int {
		return cmp.Compare(b.coldResult.PiecesHaveCount, a.coldResult.PiecesHaveCount)
	})

	for _, c := range candidates {
		if t.startTrackingReady(ctx, c.torrent, c.coldResult) {
			t.logger.InfoContext(ctx, "started tracking torrent",
				"name", c.torrent.Name,
				"hash", c.torrent.Hash,
				"piecesOnCold", c.coldResult.PiecesHaveCount,
			)
		}
	}

	return nil
}

// queryColdStatus checks a torrent's status on cold without starting tracking.
// Returns:
//   - (result, nil): READY status — caller should collect as candidate
//   - (nil, err): transient error — caller should short-circuit
//   - (nil, errSkipTorrent): non-transient error, already tracking, COMPLETE, or VERIFYING — skip
func (t *QBTask) queryColdStatus(
	ctx context.Context,
	torrent qbittorrent.Torrent,
) (*streaming.InitTorrentResult, error) {
	// Check if the tracker already knows about it
	if t.tracker.IsTracking(torrent.Hash) {
		t.trackedMu.Lock()
		t.trackedTorrents[torrent.Hash] = trackedTorrent{
			completionTime: completionTimeOrNow(torrent.CompletionOn),
			name:           torrent.Name,
			size:           torrent.Size,
		}
		t.trackedMu.Unlock()
		t.logger.DebugContext(ctx, "synced tracker state to orchestrator",
			"name", torrent.Name,
			"hash", torrent.Hash,
		)
		return nil, errSkipTorrent
	}

	// Query cold via CheckTorrentStatus - this is the source of truth
	initResp, err := t.grpcDest.CheckTorrentStatus(ctx, torrent.Hash)
	if err != nil {
		if streaming.IsTransientError(err) {
			return nil, err
		}
		t.logger.WarnContext(ctx, "failed to check torrent status on cold",
			"name", torrent.Name,
			"hash", torrent.Hash,
			"error", err,
		)
		return nil, errSkipTorrent
	}

	switch initResp.Status {
	case pb.TorrentSyncStatus_SYNC_STATUS_COMPLETE:
		t.markCompletedOnCold(torrent.Hash)
		t.logger.InfoContext(ctx, "torrent already complete on cold",
			"name", torrent.Name,
			"hash", torrent.Hash,
		)
		return nil, errSkipTorrent

	case pb.TorrentSyncStatus_SYNC_STATUS_VERIFYING:
		t.logger.InfoContext(ctx, "torrent verifying on cold, will retry",
			"name", torrent.Name,
			"hash", torrent.Hash,
		)
		return nil, errSkipTorrent

	case pb.TorrentSyncStatus_SYNC_STATUS_READY:
		return initResp, nil

	default:
		t.logger.WarnContext(ctx, "unknown status from cold CheckTorrentStatus",
			"name", torrent.Name,
			"hash", torrent.Hash,
			"status", initResp.Status,
		)
		return nil, errSkipTorrent
	}
}

// startTrackingReady handles the READY status: converts cold's pieces_needed
// into resume data and starts tracking the torrent for streaming.
func (t *QBTask) startTrackingReady(
	ctx context.Context,
	torrent qbittorrent.Torrent,
	resp *streaming.InitTorrentResult,
) bool {
	if t.trackingOrderHook != nil {
		t.trackingOrderHook(torrent.Hash)
	}

	switch resp.PiecesNeededCount {
	case -1:
		t.logger.DebugContext(ctx, "torrent not initialized on cold, queuing for streaming",
			"name", torrent.Name,
			"hash", torrent.Hash,
		)
	case 0:
		t.logger.InfoContext(ctx, "all pieces already on cold, will finalize",
			"name", torrent.Name,
			"hash", torrent.Hash,
		)
	}

	alreadyWritten := invertPiecesNeeded(resp.PiecesNeeded)

	if trackErr := t.tracker.TrackTorrentWithResume(ctx, torrent.Hash, alreadyWritten); trackErr != nil {
		t.logger.WarnContext(ctx, "failed to track torrent",
			"name", torrent.Name,
			"hash", torrent.Hash,
			"error", trackErr,
		)
		return false
	}

	t.trackedMu.Lock()
	if _, exists := t.trackedTorrents[torrent.Hash]; exists {
		t.trackedMu.Unlock()
		return false
	}
	t.trackedTorrents[torrent.Hash] = trackedTorrent{
		completionTime: completionTimeOrNow(torrent.CompletionOn),
		name:           torrent.Name,
		size:           torrent.Size,
	}
	t.trackedMu.Unlock()

	return true
}

// finalizeCompletedStreams checks for streams where all pieces are streamed
// and calls FinalizeTorrent on the cold server.
//
//nolint:unparam,gocognit // error return kept for interface consistency; errors handled internally
func (t *QBTask) finalizeCompletedStreams(ctx context.Context) error {
	t.trackedMu.RLock()
	tracked := make(map[string]trackedTorrent, len(t.trackedTorrents))
	maps.Copy(tracked, t.trackedTorrents)
	t.trackedMu.RUnlock()

	for hash := range tracked {
		progress, err := t.tracker.GetProgress(hash)
		if err != nil {
			t.logger.DebugContext(ctx, "GetProgress failed",
				"hash", hash,
				"error", err,
			)
			continue
		}

		t.logger.DebugContext(ctx, "checking stream progress",
			"hash", hash,
			"streamed", progress.Streamed,
			"total", progress.TotalPieces,
			"complete", progress.Complete,
		)

		if !progress.Complete {
			continue
		}

		// Check if we should wait due to backoff from previous failures
		if !t.shouldAttemptFinalize(hash) {
			continue
		}

		// All pieces streamed - finalize on cold server
		if finalizeErr := t.finalizeTorrent(ctx, hash); finalizeErr != nil {
			// Cold server is still verifying — not an error, just poll again next cycle.
			if errors.Is(finalizeErr, streaming.ErrFinalizeVerifying) {
				t.logger.InfoContext(ctx, "cold server still verifying, will poll again",
					"hash", hash,
				)
				continue
			}

			// Cold says pieces are missing — hot's streamed state diverged from
			// cold's written state (e.g., cold restarted with stale flush).
			// Re-sync: clear init cache, re-init to get actual PiecesNeeded,
			// and reset tracker so missing pieces get re-streamed.
			if errors.Is(finalizeErr, streaming.ErrFinalizeIncomplete) {
				t.logger.WarnContext(ctx, "cold reports incomplete, re-syncing streamed state",
					"hash", hash,
					"error", finalizeErr,
				)
				t.resyncWithCold(ctx, hash)
				continue
			}

			metrics.FinalizationErrorsTotal.WithLabelValues(metrics.ModeHot).Inc()
			t.logger.ErrorContext(ctx, "finalize failed",
				"hash", hash,
				"error", finalizeErr,
			)
			t.recordFinalizeFailure(hash)
			if streaming.IsTransientError(finalizeErr) {
				t.logger.WarnContext(ctx, "cold server unreachable, skipping remaining finalizations",
					"error", finalizeErr,
				)
				break
			}
			continue
		}

		// Success - clear backoff and mark as complete
		t.clearFinalizeBackoff(hash)

		// Record sync latency (time from download completion to finalization)
		metrics.TorrentSyncLatencySeconds.Observe(time.Since(tracked[hash].completionTime).Seconds())

		// Mark as complete on cold (persisted cache)
		t.markCompletedOnCold(hash)

		// Stop tracking and evict cached metadata
		name := tracked[hash].name
		t.tracker.Untrack(hash)
		t.source.EvictCache(hash)
		t.trackedMu.Lock()
		delete(t.trackedTorrents, hash)
		t.trackedMu.Unlock()

		metrics.TorrentsSyncedTotal.WithLabelValues(metrics.ModeHot, hash, name).Inc()
		metrics.TorrentBytesSyncedTotal.WithLabelValues(hash, name).Add(float64(tracked[hash].size))
		metrics.OldestPendingSyncSeconds.DeleteLabelValues(hash, name)

		t.logger.InfoContext(ctx, "torrent synced successfully", "hash", hash)

		// Apply synced tag for visibility (not used as source of truth)
		if t.cfg.SyncedTag != "" && !t.cfg.DryRun {
			if tagErr := t.srcClient.AddTagsCtx(ctx, []string{hash}, t.cfg.SyncedTag); tagErr != nil {
				metrics.TagApplicationErrorsTotal.WithLabelValues(metrics.ModeHot).Inc()
				t.logger.ErrorContext(ctx, "failed to add synced tag",
					"hash", hash,
					"tag", t.cfg.SyncedTag,
					"error", tagErr,
				)
			}
		}
	}

	return nil
}

// finalizeTorrent calls the cold server to finalize the torrent.
func (t *QBTask) finalizeTorrent(ctx context.Context, hash string) error {
	// Get torrent info for save path
	torrents, err := t.srcClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{hash},
	})
	if err != nil {
		return fmt.Errorf("getting torrent info: %w", err)
	}
	if len(torrents) == 0 {
		return fmt.Errorf("torrent not found: %s", hash)
	}

	torrent := torrents[0]
	savePath := torrent.SavePath
	saveSubPath := t.source.ResolveSubPath(torrent.SavePath)

	t.logger.InfoContext(ctx, "finalizing torrent on cold",
		"name", torrent.Name,
		"hash", hash,
		"savePath", savePath,
		"saveSubPath", saveSubPath,
	)

	if t.cfg.DryRun {
		return nil
	}

	return t.grpcDest.FinalizeTorrent(ctx, hash, savePath, torrent.Category, torrent.Tags, saveSubPath)
}

// maybeMoveToCold deletes torrents known to be complete on cold when space is low.
// During a drain (t.draining is true), bypasses space and seeding checks to
// evacuate all synced torrents.
//
//nolint:gocognit
func (t *QBTask) maybeMoveToCold(ctx context.Context) error {
	isDraining := t.draining.Load()

	if !isDraining {
		freeSpaceGB, err := t.getFreeSpaceGB(ctx)
		if err != nil {
			return fmt.Errorf("getting free space: %w", err)
		}

		t.logger.InfoContext(ctx, "checking free space",
			"freeGB", freeSpaceGB,
			"minGB", t.cfg.MinSpaceGB,
		)

		if freeSpaceGB >= t.cfg.MinSpaceGB {
			return nil
		}
	}

	torrents, err := t.fetchTorrentsCompletedOnCold(ctx)
	if err != nil {
		return fmt.Errorf("fetching torrents completed on cold: %w", err)
	}

	if len(torrents) == 0 {
		return nil
	}

	groups := t.groupHardlinkedTorrents(ctx, torrents)
	sortedGroups := t.sortGroupsByPriority(groups)

	var freeSpaceBefore int64
	if !isDraining {
		var spaceErr error
		freeSpaceBefore, spaceErr = t.getFreeSpaceGB(ctx)
		if spaceErr != nil {
			return fmt.Errorf("getting free space before cleanup: %w", spaceErr)
		}
	}

	var groupsDeleted, groupsSkippedSeeding, groupsFailed, torrentsHandedOff int
	minSeedingSeconds := int64(t.cfg.MinSeedingTime.Seconds())

	for _, group := range sortedGroups {
		if !isDraining && group.minSeeding < minSeedingSeconds {
			t.logger.InfoContext(ctx, "group has not seeded long enough",
				"minSeeding", group.minSeeding,
				"required", minSeedingSeconds,
			)
			groupsSkippedSeeding++
			continue
		}

		handed, moveErr := t.deleteGroupFromHot(ctx, group)
		torrentsHandedOff += handed
		if moveErr != nil {
			t.logger.ErrorContext(ctx, "failed to delete group", "error", moveErr)
			groupsFailed++
		} else {
			groupsDeleted++
		}

		if !isDraining {
			currentSpaceGB, spaceErr := t.getFreeSpaceGB(ctx)
			if spaceErr != nil {
				return fmt.Errorf("getting free space: %w", spaceErr)
			}
			if currentSpaceGB >= t.cfg.MinSpaceGB {
				t.logger.InfoContext(ctx, "reached minimum free space, stopping")
				break
			}
		}
	}

	t.recordCleanupMetrics(ctx, cleanupStats{
		groupsEvaluated:   len(sortedGroups),
		groupsDeleted:     groupsDeleted,
		groupsSkippedSeed: groupsSkippedSeeding,
		groupsFailed:      groupsFailed,
		torrentsHandedOff: torrentsHandedOff,
		isDraining:        isDraining,
		freeSpaceBefore:   freeSpaceBefore,
	})

	return nil
}

// cleanupStats holds counters from a single hot cleanup cycle for metrics and logging.
type cleanupStats struct {
	groupsEvaluated   int
	groupsDeleted     int
	groupsSkippedSeed int
	groupsFailed      int
	torrentsHandedOff int
	isDraining        bool
	freeSpaceBefore   int64
}

// recordCleanupMetrics records Prometheus counters and logs a summary for a cleanup cycle.
func (t *QBTask) recordCleanupMetrics(ctx context.Context, s cleanupStats) {
	metrics.HotCleanupGroupsTotal.WithLabelValues(metrics.ResultSuccess).Add(float64(s.groupsDeleted))
	metrics.HotCleanupGroupsTotal.WithLabelValues(metrics.ResultSkippedSeeding).Add(float64(s.groupsSkippedSeed))
	metrics.HotCleanupGroupsTotal.WithLabelValues(metrics.ResultFailure).Add(float64(s.groupsFailed))
	metrics.HotCleanupTorrentsHandedOffTotal.Add(float64(s.torrentsHandedOff))

	logAttrs := []any{
		"groupsEvaluated", s.groupsEvaluated,
		"groupsDeleted", s.groupsDeleted,
		"groupsSkippedSeeding", s.groupsSkippedSeed,
		"groupsFailed", s.groupsFailed,
		"torrentsHandedOff", s.torrentsHandedOff,
	}
	if !s.isDraining {
		freeSpaceAfter, spaceErr := t.getFreeSpaceGB(ctx)
		if spaceErr == nil {
			logAttrs = append(logAttrs, "spaceFreedGB", freeSpaceAfter-s.freeSpaceBefore)
		}
	}
	t.logger.InfoContext(ctx, "hot cleanup cycle complete", logAttrs...)
}

// hasTag reports whether the comma-separated tag list contains the target tag.
func hasTag(tags, target string) bool {
	for tag := range strings.SplitSeq(tags, ",") {
		if strings.TrimSpace(tag) == target {
			return true
		}
	}
	return false
}

// fetchTorrentsCompletedOnCold returns hot torrents that are known to be complete on cold.
func (t *QBTask) fetchTorrentsCompletedOnCold(ctx context.Context) ([]qbittorrent.Torrent, error) {
	// Reuse the torrents list from trackNewTorrents if available
	allTorrents := t.cycleTorrents
	if allTorrents != nil {
		metrics.CycleCacheHitsTotal.Inc()
	} else {
		var err error
		allTorrents, err = t.srcClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{})
		if err != nil {
			return nil, err
		}
	}

	// Filter to those known complete on cold
	t.completedMu.RLock()
	defer t.completedMu.RUnlock()

	var result []qbittorrent.Torrent
	for _, torrent := range allTorrents {
		if !t.completedOnCold[torrent.Hash] {
			continue
		}
		if !t.draining.Load() && t.cfg.ExcludeCleanupTag != "" &&
			hasTag(torrent.Tags, t.cfg.ExcludeCleanupTag) {
			continue
		}
		result = append(result, torrent)
	}

	slices.SortFunc(result, func(a, b qbittorrent.Torrent) int {
		return cmp.Compare(a.Size, b.Size)
	})

	return result, nil
}

type torrentGroup struct {
	torrents   []qbittorrent.Torrent
	popularity int64
	maxSize    int64
	minSeeding int64
}

// unionFind implements a disjoint-set data structure for grouping torrents.
type unionFind struct {
	parent map[string]string
	rank   map[string]int
}

func newUnionFind() *unionFind {
	return &unionFind{
		parent: make(map[string]string),
		rank:   make(map[string]int),
	}
}

func (uf *unionFind) find(x string) string {
	if _, ok := uf.parent[x]; !ok {
		uf.parent[x] = x
	}
	if uf.parent[x] != x {
		uf.parent[x] = uf.find(uf.parent[x]) // path compression
	}
	return uf.parent[x]
}

func (uf *unionFind) union(x, y string) {
	rx, ry := uf.find(x), uf.find(y)
	if rx == ry {
		return
	}
	// union by rank
	switch {
	case uf.rank[rx] < uf.rank[ry]:
		uf.parent[rx] = ry
	case uf.rank[rx] > uf.rank[ry]:
		uf.parent[ry] = rx
	default:
		uf.parent[ry] = rx
		uf.rank[rx]++
	}
}

func (t *QBTask) groupHardlinkedTorrents(ctx context.Context, torrents []qbittorrent.Torrent) []torrentGroup {
	if len(torrents) == 0 {
		return nil
	}

	// Phase 1: Single pass — stat each file, build inode → []torrentHash map
	inodeToHashes := make(map[uint64][]string)
	torrentMap := make(map[string]qbittorrent.Torrent, len(torrents))
	for _, torrent := range torrents {
		torrentMap[torrent.Hash] = torrent

		filesPtr, err := t.srcClient.GetFilesInformationCtx(ctx, torrent.Hash)
		if err != nil {
			t.logger.WarnContext(ctx, "failed to get files", "hash", torrent.Hash, "error", err)
			continue
		}
		if filesPtr == nil {
			continue
		}

		contentDir := t.source.ResolveContentDir(torrent.SavePath)
		for _, f := range *filesPtr {
			path := filepath.Join(contentDir, f.Name)
			inode, statErr := utils.GetInode(path)
			if statErr != nil || inode == 0 {
				continue
			}
			inodeToHashes[inode] = append(inodeToHashes[inode], torrent.Hash)
		}
	}

	// Phase 2: Union-find — for each inode shared by multiple torrents, union their groups
	uf := newUnionFind()
	for _, hashes := range inodeToHashes {
		if len(hashes) < 2 { //nolint:mnd // minimum count for a shared inode
			continue
		}
		for i := 1; i < len(hashes); i++ {
			uf.union(hashes[0], hashes[i])
		}
	}

	// Phase 3: Collect groups from union-find roots
	rootToTorrents := make(map[string][]qbittorrent.Torrent)
	for _, torrent := range torrents {
		root := uf.find(torrent.Hash)
		rootToTorrents[root] = append(rootToTorrents[root], torrent)
	}

	groups := make([]torrentGroup, 0, len(rootToTorrents))
	for _, group := range rootToTorrents {
		groups = append(groups, t.createGroup(group))
	}

	return groups
}

func (t *QBTask) createGroup(torrents []qbittorrent.Torrent) torrentGroup {
	group := torrentGroup{
		torrents:   torrents,
		minSeeding: torrents[0].SeedingTime,
	}

	for _, torrent := range torrents {
		group.popularity += torrent.NumComplete + torrent.NumIncomplete
		if torrent.Size > group.maxSize {
			group.maxSize = torrent.Size
		}
		if torrent.SeedingTime < group.minSeeding {
			group.minSeeding = torrent.SeedingTime
		}
	}

	return group
}

func (t *QBTask) sortGroupsByPriority(groups []torrentGroup) []torrentGroup {
	slices.SortFunc(groups, func(a, b torrentGroup) int {
		if a.popularity != b.popularity {
			return cmp.Compare(a.popularity, b.popularity)
		}
		// Longest seeded first (already contributed most to the swarm)
		if a.minSeeding != b.minSeeding {
			return cmp.Compare(b.minSeeding, a.minSeeding)
		}
		// Largest first (reclaim more space)
		return cmp.Compare(b.maxSize, a.maxSize)
	})
	return groups
}

// deleteGroupFromHot deletes a group of torrents complete on cold from hot storage.
// Returns the number of torrents successfully handed off.
// Uses a 3-step handoff to prevent dual seeding:
//  1. Stop on hot → fails? skip torrent (hot keeps seeding, cold stays stopped)
//  2. Start on cold → fails? resume on hot (rollback, nobody left seeding otherwise)
//  3. Delete from hot → fails? log it (cold is seeding, next cycle retries)
func (t *QBTask) deleteGroupFromHot(ctx context.Context, group torrentGroup) (int, error) {
	var handed, failed int
	for _, torrent := range group.torrents {
		t.logger.InfoContext(ctx, "handing off torrent from hot to cold",
			"name", torrent.Name,
			"hash", torrent.Hash,
		)

		if t.cfg.DryRun {
			continue
		}

		// Step 1: Stop seeding on hot
		if stopErr := t.srcClient.StopCtx(ctx, []string{torrent.Hash}); stopErr != nil {
			metrics.TorrentStopErrorsTotal.WithLabelValues(metrics.ModeHot).Inc()
			t.logger.WarnContext(ctx, "failed to stop torrent on hot, skipping handoff",
				"hash", torrent.Hash, "error", stopErr)
			failed++
			continue // Hot keeps seeding, cold stays stopped — safe
		}

		// Step 2: Start on cold (cold has the torrent stopped from finalization)
		if startErr := t.grpcDest.StartTorrent(ctx, torrent.Hash, t.cfg.SourceRemovedTag); startErr != nil {
			t.logger.ErrorContext(ctx, "failed to start torrent on cold, resuming on hot",
				"hash", torrent.Hash, "error", startErr)
			// Rollback: resume on hot so somebody is seeding
			if resumeErr := t.srcClient.ResumeCtx(ctx, []string{torrent.Hash}); resumeErr != nil {
				metrics.TorrentResumeErrorsTotal.WithLabelValues(metrics.ModeHot).Inc()
				t.logger.WarnContext(ctx, "failed to resume torrent on hot after cold start failure",
					"hash", torrent.Hash, "error", resumeErr)
			}
			failed++
			continue
		}

		// Step 3: Delete from hot (cold is now seeding)
		if deleteErr := t.srcClient.DeleteTorrentsCtx(ctx, []string{torrent.Hash}, true); deleteErr != nil {
			t.logger.ErrorContext(ctx, "failed to delete torrent from hot (cold is seeding, will retry)",
				"hash", torrent.Hash, "error", deleteErr)
			// Cold is seeding, next cleanup cycle will retry the delete
		}

		handed++
	}

	if failed > 0 {
		return handed, fmt.Errorf("%d of %d torrents failed handoff", failed, len(group.torrents))
	}
	return handed, nil
}

func (t *QBTask) getFreeSpaceGB(ctx context.Context) (int64, error) {
	freeBytes, err := t.srcClient.GetFreeSpaceOnDiskCtx(ctx)
	if err != nil {
		return 0, err
	}
	return freeBytes / bytesPerGB, nil
}

// shouldAttemptFinalize checks if enough time has passed since the last failed
// finalization attempt. Returns true if we should try again.
func (t *QBTask) shouldAttemptFinalize(hash string) bool {
	t.backoffMu.Lock()
	defer t.backoffMu.Unlock()

	backoff, exists := t.finalizeBackoffs[hash]
	if !exists {
		return true
	}

	// Calculate backoff duration based on failure count (exponential)
	backoffDuration := min(minFinalizeBackoff*time.Duration(1<<uint(backoff.failures-1)), maxFinalizeBackoff)

	return time.Since(backoff.lastAttempt) >= backoffDuration
}

// recordFinalizeFailure records a finalization failure for backoff tracking.
func (t *QBTask) recordFinalizeFailure(hash string) {
	t.backoffMu.Lock()
	defer t.backoffMu.Unlock()

	backoff, exists := t.finalizeBackoffs[hash]
	if !exists {
		backoff = &finalizeBackoff{}
		t.finalizeBackoffs[hash] = backoff
	}

	backoff.failures++
	backoff.lastAttempt = time.Now()
	metrics.ActiveFinalizationBackoffs.Set(float64(len(t.finalizeBackoffs)))
}

// clearFinalizeBackoff removes backoff tracking for a successfully finalized torrent.
func (t *QBTask) clearFinalizeBackoff(hash string) {
	t.backoffMu.Lock()
	defer t.backoffMu.Unlock()
	delete(t.finalizeBackoffs, hash)
	metrics.ActiveFinalizationBackoffs.Set(float64(len(t.finalizeBackoffs)))
}

// invertPiecesNeeded converts PiecesNeeded (true=missing) to written (true=have).
func invertPiecesNeeded(piecesNeeded []bool) []bool {
	written := make([]bool, len(piecesNeeded))
	for i, needed := range piecesNeeded {
		written[i] = !needed
	}
	return written
}

// resyncWithCold re-initializes a torrent on cold to discover which pieces are
// actually written, then resets the tracker's streamed state to match. This
// recovers from divergence after a cold restart where flushed state was stale.
func (t *QBTask) resyncWithCold(ctx context.Context, hash string) {
	// Clear cached init result so InitTorrent actually calls cold
	t.grpcDest.ClearInitResult(hash)

	meta, ok := t.tracker.GetTorrentMetadata(hash)
	if !ok {
		t.logger.ErrorContext(ctx, "resync failed: torrent metadata not found",
			"hash", hash,
		)
		return
	}

	result, initErr := t.grpcDest.InitTorrent(ctx, meta.InitTorrentRequest)
	if initErr != nil {
		t.logger.ErrorContext(ctx, "resync failed: InitTorrent error",
			"hash", hash,
			"error", initErr,
		)
		return
	}

	if result == nil || len(result.PiecesNeeded) == 0 {
		t.logger.InfoContext(ctx, "resync: cold reports all pieces written",
			"hash", hash,
		)
		return
	}

	writtenOnCold := invertPiecesNeeded(result.PiecesNeeded)
	reset := t.tracker.ResyncStreamed(hash, writtenOnCold)

	t.logger.InfoContext(ctx, "resync complete, pieces will be re-streamed",
		"hash", hash,
		"piecesReset", reset,
		"coldHas", result.PiecesHaveCount,
		"coldNeeds", result.PiecesNeededCount,
	)
}

// listenForRemovals watches for torrents removed from hot qBittorrent
// and triggers cleanup on the cold server.
func (t *QBTask) listenForRemovals(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case hash, ok := <-t.tracker.Removed():
			if !ok {
				// Channel closed, tracker shutting down
				return nil
			}
			t.handleTorrentRemoval(ctx, hash)
		}
	}
}

// handleTorrentRemoval cleans up when a torrent is removed from hot qBittorrent.
// It notifies the cold server to abort the in-progress transfer and delete partial files.
func (t *QBTask) handleTorrentRemoval(ctx context.Context, hash string) {
	t.logger.InfoContext(ctx, "torrent removed from hot, cleaning up cold",
		"hash", hash,
	)

	// Remove from local tracking first
	t.trackedMu.Lock()
	tt, wasTracked := t.trackedTorrents[hash]
	delete(t.trackedTorrents, hash)
	t.trackedMu.Unlock()

	// Check completedOnCold (determines StartTorrent vs AbortTorrent).
	// Don't delete yet — only remove after StartTorrent succeeds so
	// pruneCompletedOnCold can clean up if the RPC fails.
	t.completedMu.RLock()
	wasCompletedOnCold := t.completedOnCold[hash]
	t.completedMu.RUnlock()

	// Clear any finalize backoff state
	t.clearFinalizeBackoff(hash)

	if wasTracked {
		metrics.OldestPendingSyncSeconds.DeleteLabelValues(hash, tt.name)
	} else {
		t.logger.DebugContext(ctx, "removed torrent was not in tracked list",
			"hash", hash,
		)
	}

	if t.cfg.DryRun {
		if wasCompletedOnCold {
			t.logger.InfoContext(ctx, "[dry-run] would start/tag torrent on cold",
				"hash", hash, "tag", t.cfg.SourceRemovedTag,
			)
		} else {
			t.logger.InfoContext(ctx, "[dry-run] would abort torrent on cold",
				"hash", hash,
			)
		}
		return
	}

	// Torrent was complete on cold: start seeding and apply tag
	if wasCompletedOnCold {
		startCtx, cancel := context.WithTimeout(ctx, coldRPCTimeout)
		defer cancel()
		if startErr := t.grpcDest.StartTorrent(startCtx, hash, t.cfg.SourceRemovedTag); startErr != nil {
			t.logger.WarnContext(ctx, "failed to start/tag torrent on cold after removal (will retry via prune)",
				"hash", hash, "error", startErr,
			)
			return // Keep in completedOnCold — pruneCompletedOnCold will clean up
		}

		// StartTorrent succeeded — remove from completedOnCold cache
		t.completedMu.Lock()
		delete(t.completedOnCold, hash)
		metrics.CompletedOnColdCacheSize.Set(float64(len(t.completedOnCold)))
		t.completedMu.Unlock()
		t.saveCompletedCache()

		t.logger.InfoContext(ctx, "started and tagged torrent on cold after hot removal",
			"hash", hash, "tag", t.cfg.SourceRemovedTag,
		)
		return
	}

	// Torrent was not complete on cold: abort and clean up partial files
	abortCtx, cancel := context.WithTimeout(ctx, coldRPCTimeout)
	defer cancel()

	filesDeleted, err := t.grpcDest.AbortTorrent(abortCtx, hash, true)
	if err != nil {
		t.logger.WarnContext(ctx, "failed to abort torrent on cold (periodic cleanup will handle)",
			"hash", hash,
			"error", err,
		)
		return
	}

	t.logger.InfoContext(ctx, "aborted torrent on cold",
		"hash", hash,
		"filesDeleted", filesDeleted,
	)
}
