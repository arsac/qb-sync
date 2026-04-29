package source

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/autobrr/go-qbittorrent"
	"golang.org/x/sync/errgroup"

	"github.com/arsac/qb-sync/internal/config"
	"github.com/arsac/qb-sync/internal/congestion"
	"github.com/arsac/qb-sync/internal/metrics"
	"github.com/arsac/qb-sync/internal/qbclient"
	"github.com/arsac/qb-sync/internal/streaming"
)

// ErrDrainInProgress is returned when Drain is called while another drain is running.
var ErrDrainInProgress = errors.New("drain already in progress")

// errSkipTorrent is returned by queryDestStatus when a torrent is not eligible
// for finalization (already tracked, complete, verifying, or non-transient error).
var errSkipTorrent = errors.New("torrent not eligible for finalization")

const (
	bytesPerGB = int64(1024 * 1024 * 1024)

	// Streaming queue configuration defaults.
	defaultRetryDelay = 5 * time.Second

	// maxVerificationRetries is the number of consecutive verification failures
	// before the torrent is tagged as sync-failed and excluded from future syncs.
	maxVerificationRetries = 3

	// Timeout for unary RPCs to destination server during removal/handoff.
	destRPCTimeout = 30 * time.Second

	cacheFilePermissions = 0o644

	// pruneCycleInterval controls how often runOnce runs the periodic
	// prune/recheck/cleanup pass (every Nth cycle).
	pruneCycleInterval = 50
)

// TrackedTorrent holds metadata for a torrent being synced from source to destination.
type TrackedTorrent struct {
	CompletionTime time.Time // when the torrent finished downloading on source (from qbittorrent CompletionOn)
	Name           string    // torrent name for metric labels
	Size           int64     // torrent size in bytes for TorrentBytesSyncedTotal metric
}

// QBTask orchestrates torrent streaming from source to destination.
type QBTask struct {
	cfg       *config.SourceConfig
	logger    *slog.Logger
	srcClient qbclient.Client
	grpcDest  Destination

	// Streaming components
	source  *qbclient.Source
	tracker *streaming.PieceMonitor
	queue   *streaming.BidiQueue

	// Extracted sub-components with internal locking
	tracked   *TrackedSet      // torrents currently being streamed
	completed *CompletionCache // torrents known to be complete on destination
	backoffs  *BackoffTracker  // finalization retry backoff per torrent

	// Cycle counter for periodic pruning of completedOnDest
	pruneCycleCount int

	// Per-cycle cache of torrents to avoid redundant GetTorrentsCtx calls.
	// Set by trackNewTorrents, consumed by fetchTorrentsCompletedOnDest, reset each cycle.
	// nil means not yet fetched this cycle; non-nil (even empty) means cached.
	cycleTorrents []qbittorrent.Torrent

	// trackingOrderHook is called with each hash when tracking starts. Test-only.
	trackingOrderHook func(hash string)

	// draining is set by Drain() to bypass space/seeding checks in maybeMoveToDest.
	draining atomic.Bool
}

// NewQBTask creates a new QBTask with streaming integration.
func NewQBTask(
	cfg *config.SourceConfig,
	dest *streaming.GRPCDestination,
	logger *slog.Logger,
) (*QBTask, error) {
	rawClient := qbittorrent.NewClient(qbittorrent.Config{
		Host:     cfg.QBURL,
		Username: cfg.QBUsername,
		Password: cfg.QBPassword,
	})

	qbConfig := qbclient.DefaultConfig()
	srcClient := qbclient.NewResilientClient(
		rawClient,
		qbConfig,
		logger.With("component", "qb-client"),
		metrics.ModeSource,
	)

	source := qbclient.NewSource(srcClient, cfg.DataPath)

	monitorConfig := streaming.DefaultPieceMonitorConfig()
	tracker := streaming.NewPieceMonitor(rawClient, source, logger, monitorConfig)

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

	cachePath := filepath.Join(cfg.DataPath, ".qb-sync", "completed_on_dest.json")

	t := &QBTask{
		cfg:       cfg,
		logger:    logger,
		srcClient: srcClient,
		grpcDest:  dest,
		source:    source,
		tracker:   tracker,
		queue:     queue,
		tracked:   NewTrackedSet(),
		completed: NewCompletionCache(cachePath, logger),
		backoffs:  NewBackoffTracker(),
	}

	t.completed.Load()

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

// Run executes the QBTask main loop.
func (t *QBTask) Run(ctx context.Context) error {
	if err := t.Login(ctx); err != nil {
		return fmt.Errorf("logging into source: %w", err)
	}

	g, gCtx := errgroup.WithContext(ctx)

	// runComponent wraps a long-running subsystem so cancellation is silent
	// (errors raised after gCtx is done are noise from the shutdown cascade).
	runComponent := func(name string, fn func(context.Context) error) func() error {
		return func() error {
			if err := fn(gCtx); err != nil && gCtx.Err() == nil {
				t.logger.ErrorContext(gCtx, name+" error", "error", err)
				return fmt.Errorf("%s failed: %w", name, err)
			}
			return nil
		}
	}

	g.Go(runComponent("piece monitor", t.tracker.Run))
	g.Go(runComponent("streaming queue", t.queue.Run))

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

	g.Go(func() error {
		return t.listenForRemovals(gCtx)
	})

	return g.Wait()
}

func (t *QBTask) runOnce(ctx context.Context) {
	t.cycleTorrents = nil

	if err := t.trackNewTorrents(ctx); err != nil {
		t.logger.ErrorContext(ctx, "failed to track torrents", "error", err)
	}
	t.checkExcludedTorrents(ctx)
	if err := t.finalizeCompletedStreams(ctx); err != nil {
		t.logger.ErrorContext(ctx, "failed to finalize streams", "error", err)
	}
	if !t.Draining() {
		if err := t.maybeMoveToDest(ctx); err != nil {
			t.logger.ErrorContext(ctx, "failed to move torrents", "error", err)
		}
	}
	metrics.ActiveTorrents.WithLabelValues(metrics.ModeSource).Set(float64(t.tracked.Count()))
	t.updateSyncAgeGauge()
	t.updateTorrentProgressGauges()

	t.pruneCycleCount++
	if t.pruneCycleCount >= pruneCycleInterval {
		t.pruneCycleCount = 0
		t.pruneCompletedOnDest(ctx)
		t.recheckFileSelections(ctx)
		t.pruneStaleMonitorEntries(ctx)
	}
}

// FetchCompletedOnDestination returns torrents known to be complete on destination.
// Exported for testing (used by E2E tests).
func (t *QBTask) FetchCompletedOnDestination() []string {
	return t.completed.Keys()
}

// MarkCompletedOnDestination marks a torrent as complete on destination.
// Exported for testing only - allows tests to simulate synced state.
func (t *QBTask) MarkCompletedOnDestination(hash string) {
	t.completed.Mark(hash)
}

// withDestRPCTimeout derives a context with destRPCTimeout from the parent
// and returns it alongside its cancel func. Used for unary destination RPCs
// to bound blocking time during cleanup/handoff paths.
func withDestRPCTimeout(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, destRPCTimeout)
}

// PruneCompletedOnDest is the exported version of pruneCompletedOnDest for testing.
func (t *QBTask) PruneCompletedOnDest(ctx context.Context) {
	t.pruneCompletedOnDest(ctx)
}

// RecheckFileSelections is the exported version of recheckFileSelections for testing.
func (t *QBTask) RecheckFileSelections(ctx context.Context) {
	t.recheckFileSelections(ctx)
}

// pruneCompletedOnDest hands off completed torrents whose source-side copy has
// been removed (so the destination picks up seeding with the source-removed
// tag) and removes their entries from the completed cache.
//
// PieceMonitor.removeAndNotify only fires the tracker.Removed channel for
// *tracked* torrents; once a torrent finishes syncing it gets untracked, so a
// later removal from source qB never reaches handleTorrentRemoval. Without
// this path the destination would silently stay stopped indefinitely, even
// though the user clearly expects it to take over seeding once source is gone.
//
// On handoff failure the cache entry is left in place so the next cycle
// retries; on success (or in dry-run) the entry is pruned to bound cache size.
func (t *QBTask) pruneCompletedOnDest(ctx context.Context) {
	torrents, err := t.srcClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{})
	if err != nil {
		t.logger.WarnContext(ctx, "failed to fetch torrents for cache pruning", "error", err)
		return
	}

	sourceHashes := make(map[string]struct{}, len(torrents))
	for _, torrent := range torrents {
		sourceHashes[torrent.Hash] = struct{}{}
	}

	var prunable []string
	for hash := range t.completed.Snapshot() {
		if _, stillInSource := sourceHashes[hash]; stillInSource {
			continue
		}
		if t.cfg.DryRun {
			t.logger.InfoContext(ctx, "[dry-run] would hand off removed-from-source torrent",
				"hash", hash, "tag", t.cfg.SourceRemovedTag)
			prunable = append(prunable, hash)
			continue
		}
		if t.handoffRemovedCompleted(ctx, hash) {
			prunable = append(prunable, hash)
		}
	}

	if len(prunable) == 0 {
		return
	}

	t.completed.RemoveAll(prunable)
	t.completed.Save()
	t.logger.InfoContext(ctx, "pruned completed-on-destination cache",
		"pruned", len(prunable),
		"remaining", t.completed.Count(),
	)
}

// handoffRemovedCompleted starts the torrent on destination qBittorrent with
// the source-removed tag. Returns true on success (caller should prune the
// cache entry); false on failure (caller should leave the entry so the next
// cycle retries).
func (t *QBTask) handoffRemovedCompleted(ctx context.Context, hash string) bool {
	startCtx, cancel := withDestRPCTimeout(ctx)
	defer cancel()
	if startErr := t.grpcDest.StartTorrent(startCtx, hash, t.cfg.SourceRemovedTag); startErr != nil {
		t.logger.WarnContext(ctx, "failed to hand off removed-from-source torrent to destination, will retry",
			"hash", hash, "error", startErr,
		)
		return false
	}
	t.logger.InfoContext(ctx, "handed off removed-from-source torrent to destination",
		"hash", hash, "tag", t.cfg.SourceRemovedTag,
	)
	return true
}

// pruneStaleMonitorEntries removes PieceMonitor entries for torrents that the
// orchestrator no longer tracks. This is a safety net for cases where Untrack
// was missed due to an error path.
func (t *QBTask) pruneStaleMonitorEntries(ctx context.Context) {
	pruned := t.tracker.PruneStale(t.tracked.Hashes())
	if pruned > 0 {
		t.logger.WarnContext(ctx, "pruned stale PieceMonitor entries",
			"pruned", pruned,
		)
	}
}
