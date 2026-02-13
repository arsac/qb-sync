package source

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
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

	// Finalization retry settings - exponential backoff.
	minFinalizeBackoff = 2 * time.Second
	maxFinalizeBackoff = 30 * time.Second

	// maxVerificationRetries is the number of consecutive verification failures
	// before the torrent is tagged as sync-failed and excluded from future syncs.
	maxVerificationRetries = 3

	// Timeout for unary RPCs to destination server during removal/handoff.
	destRPCTimeout = 30 * time.Second

	cacheFilePermissions = 0o644
)

// finalizeBackoff tracks exponential backoff state for finalization retries.
type finalizeBackoff struct {
	failures    int
	lastAttempt time.Time
}

// trackedTorrent holds metadata for a torrent being synced from source to destination.
type trackedTorrent struct {
	completionTime time.Time // when the torrent finished downloading on source (from qbittorrent CompletionOn)
	name           string    // torrent name for metric labels
	size           int64     // torrent size in bytes for TorrentBytesSyncedTotal metric
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

	// Tracked torrents currently being streamed
	trackedTorrents map[string]trackedTorrent
	trackedMu       sync.RWMutex

	// Torrents known to be complete on destination (persisted to disk).
	// Value is the selection fingerprint (sorted selected file indices, e.g. "0,1,3").
	// Destination qBittorrent is the source of truth; synced tag is for visibility only.
	completedOnDest    map[string]string
	completedMu        sync.RWMutex
	completedCachePath string

	// Finalization backoff tracking per torrent
	finalizeBackoffs map[string]*finalizeBackoff
	backoffMu        sync.Mutex

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

	t := &QBTask{
		cfg:                cfg,
		logger:             logger,
		srcClient:          srcClient,
		grpcDest:           dest,
		source:             source,
		tracker:            tracker,
		queue:              queue,
		trackedTorrents:    make(map[string]trackedTorrent),
		completedOnDest:    make(map[string]string),
		completedCachePath: filepath.Join(cfg.DataPath, ".qb-sync", "completed_on_dest.json"),
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

// Run executes the QBTask main loop.
func (t *QBTask) Run(ctx context.Context) error {
	if err := t.Login(ctx); err != nil {
		return fmt.Errorf("logging into source: %w", err)
	}

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		if err := t.tracker.Run(gCtx); err != nil && gCtx.Err() == nil {
			t.logger.ErrorContext(gCtx, "piece monitor error", "error", err)
			return fmt.Errorf("piece monitor failed: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		if err := t.queue.Run(gCtx); err != nil && gCtx.Err() == nil {
			t.logger.ErrorContext(gCtx, "streaming queue error", "error", err)
			return fmt.Errorf("streaming queue failed: %w", err)
		}
		return nil
	})

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
		if err := t.maybeMoveToDest(ctx); err != nil {
			t.logger.ErrorContext(ctx, "failed to move torrents", "error", err)
		}
	}
	t.trackedMu.RLock()
	metrics.ActiveTorrents.WithLabelValues(metrics.ModeSource).Set(float64(len(t.trackedTorrents)))
	t.trackedMu.RUnlock()
	t.updateSyncAgeGauge()
	t.updateTorrentProgressGauges()

	t.pruneCycleCount++
	if t.pruneCycleCount >= pruneCycleInterval {
		t.pruneCycleCount = 0
		t.pruneCompletedOnDest(ctx)
		t.recheckFileSelections(ctx)
	}
}
