package hot

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"math"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/autobrr/go-qbittorrent"
	"golang.org/x/sync/errgroup"

	"github.com/arsac/qb-sync/internal/config"
	"github.com/arsac/qb-sync/internal/congestion"
	"github.com/arsac/qb-sync/internal/metrics"
	"github.com/arsac/qb-sync/internal/qbclient"
	"github.com/arsac/qb-sync/internal/streaming"
	"github.com/arsac/qb-sync/internal/utils"
)

const (
	syncedTag           = "synced"
	pollInterval        = 2 * time.Second
	pollTimeout         = 5 * time.Minute
	bytesPerGB          = int64(1024 * 1024 * 1024)
	maxConcurrentChecks = 10

	// Streaming queue configuration defaults.
	defaultRetryDelaySeconds = 5

	// Finalization retry settings - exponential backoff.
	minFinalizeBackoff = 2 * time.Second
	maxFinalizeBackoff = 30 * time.Second

	// Abort torrent settings.
	abortTorrentTimeout = 30 * time.Second
)

type torrentFile struct {
	Name string
	Size int64
}

// finalizeBackoff tracks exponential backoff state for finalization retries.
type finalizeBackoff struct {
	failures    int
	lastAttempt time.Time
}

// QBTask orchestrates torrent streaming from hot to cold.
type QBTask struct {
	cfg       *config.HotConfig
	logger    *slog.Logger
	srcClient *qbclient.ResilientClient
	grpcDest  *streaming.GRPCDestination

	// Streaming components
	source  *qbclient.Source
	tracker *streaming.PieceMonitor
	queue   *streaming.BidiQueue

	// Tracked torrents currently being streamed
	trackedTorrents map[string]bool
	trackedMu       sync.RWMutex

	// Finalization backoff tracking per torrent
	finalizeBackoffs map[string]*finalizeBackoff
	backoffMu        sync.Mutex
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
	srcClient := qbclient.NewResilientClient(rawClient, qbConfig, logger.With("component", "qb-client"))

	// Create QBSource with resilient client
	source := qbclient.NewSource(srcClient, cfg.DataPath)

	// Create PieceMonitor (uses raw client for MainData.Update, but has internal retry)
	monitorConfig := streaming.DefaultPieceMonitorConfig()
	tracker := streaming.NewPieceMonitor(rawClient, source, logger, monitorConfig)

	// Create streaming queue with adaptive congestion control and multi-stream pooling
	queueConfig := streaming.BidiQueueConfig{
		MaxBytesPerSec: cfg.MaxBytesPerSec,
		RetryDelay:     defaultRetryDelaySeconds * time.Second,
		AdaptiveWindow: congestion.DefaultConfig(),
	}
	queue := streaming.NewBidiQueue(source, dest, tracker, logger, queueConfig)

	return &QBTask{
		cfg:              cfg,
		logger:           logger,
		srcClient:        srcClient,
		grpcDest:         dest,
		source:           source,
		tracker:          tracker,
		queue:            queue,
		trackedTorrents:  make(map[string]bool),
		finalizeBackoffs: make(map[string]*finalizeBackoff),
	}, nil
}

// Login authenticates with the source qBittorrent instance.
// Uses resilient client with automatic retry for transient errors.
func (t *QBTask) Login(ctx context.Context) error {
	return t.srcClient.LoginCtx(ctx)
}

// QBLogin is an alias for Login, used for health checks.
func (t *QBTask) QBLogin(ctx context.Context) error {
	return t.Login(ctx)
}

// RunOnce executes a single iteration of the main loop.
// Useful for testing without starting background goroutines.
func (t *QBTask) RunOnce(ctx context.Context) {
	t.runOnce(ctx)
}

// MaybeMoveToCold is the exported version of maybeMoveToCold for testing.
func (t *QBTask) MaybeMoveToCold(ctx context.Context) error {
	return t.maybeMoveToCold(ctx)
}

// Progress returns the streaming progress for a torrent.
// Useful for testing to monitor streaming state.
func (t *QBTask) Progress(_ context.Context, hash string) (streaming.StreamProgress, error) {
	return t.tracker.GetProgress(hash)
}

// FetchSyncedTorrents is the exported version of fetchSyncedTorrents for testing.
func (t *QBTask) FetchSyncedTorrents(ctx context.Context) ([]qbittorrent.Torrent, error) {
	return t.fetchSyncedTorrents(ctx)
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

func (t *QBTask) runOnce(ctx context.Context) {
	// 1. Track new completed torrents for streaming
	if err := t.trackNewTorrents(ctx); err != nil {
		t.logger.ErrorContext(ctx, "failed to track torrents", "error", err)
	}

	// 2. Check for completed streams and finalize them
	if err := t.finalizeCompletedStreams(ctx); err != nil {
		t.logger.ErrorContext(ctx, "failed to finalize streams", "error", err)
	}

	// 3. Maybe delete synced torrents from hot to free space
	if err := t.maybeMoveToCold(ctx); err != nil {
		t.logger.ErrorContext(ctx, "failed to move torrents", "error", err)
	}
}

// trackNewTorrents starts tracking new completed torrents.
func (t *QBTask) trackNewTorrents(ctx context.Context) error {
	torrents, err := t.srcClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Filter: qbittorrent.TorrentFilterCompleted,
	})
	if err != nil {
		return fmt.Errorf("fetching completed torrents: %w", err)
	}

	for _, torrent := range torrents {
		if hasSyncedTag(torrent.Tags) {
			continue
		}

		// Check if already tracking - use atomic check-and-set to prevent race
		if !t.tryStartTracking(ctx, torrent) {
			continue
		}

		metrics.ActiveTorrents.WithLabelValues("hot").Inc()

		t.logger.InfoContext(ctx, "started tracking torrent",
			"name", torrent.Name,
			"hash", torrent.Hash,
		)
	}

	return nil
}

// tryStartTracking checks if a torrent is already tracked and starts tracking if not.
// Returns true if tracking was started, false if already tracked or failed.
// Note: This method releases the lock before making external calls to avoid lock inversion.
func (t *QBTask) tryStartTracking(ctx context.Context, torrent qbittorrent.Torrent) bool {
	// Phase 1: Check if already tracked (with lock)
	t.trackedMu.RLock()
	alreadyTracked := t.trackedTorrents[torrent.Hash]
	t.trackedMu.RUnlock()

	if alreadyTracked {
		return false
	}

	// Check if the tracker already knows about it (no lock needed - tracker has its own sync)
	if t.tracker.IsTracking(torrent.Hash) {
		// Sync our local map with tracker state
		t.trackedMu.Lock()
		t.trackedTorrents[torrent.Hash] = true
		t.trackedMu.Unlock()
		t.logger.DebugContext(ctx, "synced tracker state to orchestrator",
			"name", torrent.Name,
			"hash", torrent.Hash,
		)
		return false
	}

	// Phase 2: Query cold for already-written pieces BEFORE tracking.
	// This data is passed to TrackTorrentWithResume so pieces are marked as
	// streamed before queueCompletedPieces runs, preventing re-transfer.
	alreadyWritten := t.getWrittenPiecesFromCold(ctx, torrent.Hash)

	// Phase 3: Start tracking with resume data (external call - no lock held)
	if trackErr := t.tracker.TrackTorrentWithResume(ctx, torrent.Hash, alreadyWritten); trackErr != nil {
		t.logger.WarnContext(ctx, "failed to track torrent",
			"name", torrent.Name,
			"hash", torrent.Hash,
			"error", trackErr,
		)
		return false
	}

	// Phase 4: Update local state (with lock)
	// Re-check to handle race where another goroutine added it while we were tracking
	t.trackedMu.Lock()
	if t.trackedTorrents[torrent.Hash] {
		// Another goroutine beat us - that's fine, tracking succeeded
		t.trackedMu.Unlock()
		return false
	}
	t.trackedTorrents[torrent.Hash] = true
	t.trackedMu.Unlock()

	return true
}

// getWrittenPiecesFromCold queries the cold server for already-written pieces.
// Returns nil if cold doesn't have the torrent or on error (non-fatal).
func (t *QBTask) getWrittenPiecesFromCold(ctx context.Context, hash string) []bool {
	written, err := t.grpcDest.GetWrittenPieces(ctx, hash)
	if err != nil {
		// Not fatal - cold may not have this torrent yet (first time streaming)
		t.logger.DebugContext(ctx, "could not get written pieces from cold",
			"hash", hash,
			"error", err,
		)
		return nil
	}
	return written
}

// finalizeCompletedStreams checks for streams where all pieces are streamed
// and calls FinalizeTorrent on the cold server.
//
//nolint:unparam // error return kept for interface consistency; errors handled internally
func (t *QBTask) finalizeCompletedStreams(ctx context.Context) error {
	t.trackedMu.RLock()
	hashes := make([]string, 0, len(t.trackedTorrents))
	for hash := range t.trackedTorrents {
		hashes = append(hashes, hash)
	}
	t.trackedMu.RUnlock()

	for _, hash := range hashes {
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
			metrics.FinalizationErrorsTotal.WithLabelValues("hot").Inc()
			t.logger.ErrorContext(ctx, "finalize failed",
				"hash", hash,
				"error", finalizeErr,
			)
			t.recordFinalizeFailure(hash)
			continue
		}

		// Success - clear any backoff state
		t.clearFinalizeBackoff(hash)

		// Mark as synced on hot with retry logic
		tagSuccess := t.cfg.DryRun // In dry-run, consider it success
		if !t.cfg.DryRun {
			tagSuccess = t.addSyncedTagWithRetry(ctx, hash)
		}

		// Always stop tracking - if tagging failed, we'll retry next cycle
		// when trackNewTorrents picks it up again (finalize is idempotent)
		t.tracker.Untrack(hash)
		t.trackedMu.Lock()
		delete(t.trackedTorrents, hash)
		t.trackedMu.Unlock()

		metrics.ActiveTorrents.WithLabelValues("hot").Dec()

		if tagSuccess {
			metrics.TorrentsSyncedTotal.WithLabelValues("hot").Inc()
			t.logger.InfoContext(ctx, "torrent synced successfully", "hash", hash)
		} else {
			t.logger.WarnContext(ctx, "torrent finalized but tagging failed, will retry next cycle",
				"hash", hash,
			)
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

	t.logger.InfoContext(ctx, "finalizing torrent on cold",
		"name", torrent.Name,
		"hash", hash,
		"savePath", savePath,
	)

	if t.cfg.DryRun {
		return nil
	}

	return t.grpcDest.FinalizeTorrent(ctx, hash, savePath, torrent.Category, torrent.Tags)
}

// addSyncedTagWithRetry attempts to add the synced tag.
// Uses resilient client which handles retry internally.
func (t *QBTask) addSyncedTagWithRetry(ctx context.Context, hash string) bool {
	if err := t.srcClient.AddTagsCtx(ctx, []string{hash}, syncedTag); err != nil {
		t.logger.WarnContext(ctx, "failed to add synced tag after retries",
			"hash", hash,
			"error", err,
		)
		return false
	}
	return true
}

// maybeMoveToCold deletes synced torrents from hot when space is low.
// The torrent data is already verified on cold, so we just delete from hot.
func (t *QBTask) maybeMoveToCold(ctx context.Context) error {
	if !t.cfg.Force {
		freeSpaceGB, err := t.getFreeSpaceGB(t.cfg.DataPath)
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

	torrents, err := t.fetchSyncedTorrents(ctx)
	if err != nil {
		return fmt.Errorf("fetching synced torrents: %w", err)
	}

	if len(torrents) == 0 {
		return nil
	}

	groups := t.groupHardlinkedTorrents(ctx, torrents)
	sortedGroups := t.sortGroupsByPriority(groups)

	for _, group := range sortedGroups {
		if moveErr := t.deleteGroupFromHot(ctx, group); moveErr != nil {
			t.logger.ErrorContext(ctx, "failed to delete group", "error", moveErr)
			continue
		}

		if !t.cfg.Force {
			currentSpaceGB, spaceErr := t.getFreeSpaceGB(t.cfg.DataPath)
			if spaceErr != nil {
				return fmt.Errorf("getting free space: %w", spaceErr)
			}
			if currentSpaceGB >= t.cfg.MinSpaceGB {
				t.logger.InfoContext(ctx, "reached minimum free space, stopping")
				return nil
			}
		}
	}

	return nil
}

func (t *QBTask) fetchSyncedTorrents(ctx context.Context) ([]qbittorrent.Torrent, error) {
	torrents, err := t.srcClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Filter: qbittorrent.TorrentFilterCompleted,
		Tag:    syncedTag,
	})
	if err != nil {
		return nil, err
	}

	slices.SortFunc(torrents, func(a, b qbittorrent.Torrent) int {
		return cmp.Compare(a.Size, b.Size)
	})

	return torrents, nil
}

type torrentGroup struct {
	torrents   []qbittorrent.Torrent
	popularity int64
	maxSize    int64
	minSeeding int64
}

func (t *QBTask) groupHardlinkedTorrents(ctx context.Context, torrents []qbittorrent.Torrent) []torrentGroup {
	if len(torrents) == 0 {
		return nil
	}

	visited := make(map[string]bool)
	var groups []torrentGroup

	fileCache := make(map[string][]torrentFile)
	for _, torrent := range torrents {
		filesPtr, err := t.srcClient.GetFilesInformationCtx(ctx, torrent.Hash)
		if err != nil {
			t.logger.WarnContext(ctx, "failed to get files", "hash", torrent.Hash, "error", err)
			continue
		}
		if filesPtr != nil {
			files := make([]torrentFile, len(*filesPtr))
			for i, f := range *filesPtr {
				files[i] = torrentFile{Name: f.Name, Size: f.Size}
			}
			fileCache[torrent.Hash] = files
		}
	}

	for _, torrent := range torrents {
		if visited[torrent.Hash] {
			continue
		}

		group := []qbittorrent.Torrent{torrent}
		visited[torrent.Hash] = true

		for _, other := range torrents {
			if visited[other.Hash] {
				continue
			}

			if t.areHardlinkedTorrents(torrent, other, fileCache) {
				group = append(group, other)
				visited[other.Hash] = true
			}
		}

		groups = append(groups, t.createGroup(group))
	}

	return groups
}

func (t *QBTask) areHardlinkedTorrents(a, b qbittorrent.Torrent, cache map[string][]torrentFile) bool {
	filesA := cache[a.Hash]
	filesB := cache[b.Hash]

	for _, fileA := range filesA {
		pathA := filepath.Join(a.SavePath, fileA.Name)
		for _, fileB := range filesB {
			pathB := filepath.Join(b.SavePath, fileB.Name)
			hardlinked, err := utils.AreHardlinked(pathA, pathB)
			if err != nil {
				continue
			}
			if hardlinked {
				return true
			}
		}
	}

	return false
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
		// Reverse order for size (larger first)
		return cmp.Compare(b.maxSize, a.maxSize)
	})
	return groups
}

// deleteGroupFromHot deletes a group of synced torrents from hot storage.
//
//nolint:unparam // error return kept for interface consistency; errors handled internally
func (t *QBTask) deleteGroupFromHot(ctx context.Context, group torrentGroup) error {
	minSeedingSeconds := int64(t.cfg.MinSeedingTime.Seconds())
	if group.minSeeding < minSeedingSeconds {
		t.logger.InfoContext(ctx, "group has not seeded long enough",
			"minSeeding", group.minSeeding,
			"required", minSeedingSeconds,
		)
		return nil
	}

	for _, torrent := range group.torrents {
		t.logger.InfoContext(ctx, "deleting synced torrent from hot",
			"name", torrent.Name,
			"hash", torrent.Hash,
		)

		if t.cfg.DryRun {
			continue
		}

		if err := t.srcClient.DeleteTorrentsCtx(ctx, []string{torrent.Hash}, true); err != nil {
			t.logger.ErrorContext(ctx, "failed to delete torrent",
				"hash", torrent.Hash,
				"error", err,
			)
		}
	}

	return nil
}

func (t *QBTask) getFreeSpaceGB(path string) (int64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}
	freeBytes := stat.Bavail * uint64(stat.Bsize)
	freeGB := freeBytes / uint64(bytesPerGB)
	return int64(min(freeGB, uint64(math.MaxInt64))), nil
}

func hasSyncedTag(tags string) bool {
	return slices.Contains(splitTags(tags), syncedTag)
}

func splitTags(tags string) []string {
	if tags == "" {
		return nil
	}
	parts := strings.Split(tags, ",")
	var result []string
	for _, part := range parts {
		tag := strings.TrimSpace(part)
		if tag != "" {
			result = append(result, tag)
		}
	}
	return result
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
}

// clearFinalizeBackoff removes backoff tracking for a successfully finalized torrent.
func (t *QBTask) clearFinalizeBackoff(hash string) {
	t.backoffMu.Lock()
	defer t.backoffMu.Unlock()
	delete(t.finalizeBackoffs, hash)
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
	wasTracked := t.trackedTorrents[hash]
	delete(t.trackedTorrents, hash)
	t.trackedMu.Unlock()

	// Clear any finalize backoff state
	t.clearFinalizeBackoff(hash)

	if wasTracked {
		metrics.ActiveTorrents.WithLabelValues("hot").Dec()
	} else {
		t.logger.DebugContext(ctx, "removed torrent was not in tracked list",
			"hash", hash,
		)
	}

	// Notify cold server to abort and clean up partial files
	if t.cfg.DryRun {
		t.logger.InfoContext(ctx, "[dry-run] would abort torrent on cold",
			"hash", hash,
		)
		return
	}

	// Use timeout to prevent blocking indefinitely if cold server is unresponsive
	abortCtx, cancel := context.WithTimeout(ctx, abortTorrentTimeout)
	defer cancel()

	filesDeleted, err := t.grpcDest.AbortTorrent(abortCtx, hash, true)
	if err != nil {
		// Log but don't fail - periodic orphan cleanup on cold will handle it
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
