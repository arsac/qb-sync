package streaming

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/autobrr/go-qbittorrent"

	"github.com/arsac/qb-sync/internal/metrics"
	"github.com/arsac/qb-sync/internal/utils"
	pb "github.com/arsac/qb-sync/proto"
)

var (
	ErrTorrentNotTracked = errors.New("torrent not being tracked")
	ErrPieceNotReady     = errors.New("piece not ready for streaming")
)

const (
	defaultPollInterval     = 2 * time.Second
	completedChannelBufSize = 1000
	removedChannelBufSize   = 100
	completeProgress        = 1.0

	idleThreshold  = 5 // consecutive polls with no new pieces before slowing down
	idleSlowFactor = 5 // poll idle torrents every Nth tick (10s instead of 2s)

	queueFullLogInterval = 30 * time.Second // minimum interval between "queue full" warnings
)

// PieceMonitorConfig configures the piece tracker.
type PieceMonitorConfig struct {
	PollInterval time.Duration // How often to poll for piece state changes
}

// DefaultPieceMonitorConfig returns sensible defaults.
func DefaultPieceMonitorConfig() PieceMonitorConfig {
	return PieceMonitorConfig{
		PollInterval: defaultPollInterval,
	}
}

// torrentState tracks the streaming state for a single torrent.
type torrentState struct {
	meta       *TorrentMetadata
	hashes     []string // SHA1 hash per piece
	lastStates []PieceState
	streamed   []bool // pieces successfully streamed
	failed     []bool // pieces that failed (for retry logic)
	idleTicks  int    // consecutive polls with no new pieces completed
	mu         sync.RWMutex
}

// PieceMonitor monitors piece completion and queues pieces for streaming.
// Uses sync/maindata for efficient polling, only fetching piece states
// for actively downloading torrents.
type PieceMonitor struct {
	client      *qbittorrent.Client // Raw client for MainData.Update
	source      PieceSource
	logger      *slog.Logger
	config      PieceMonitorConfig
	retryConfig utils.RetryConfig     // Retry config for MainData.Update
	mainData    *qbittorrent.MainData // Cached maindata with rid for incremental updates

	torrents  map[string]*torrentState
	mu        sync.RWMutex
	tickCount uint64 // monotonic counter for idle skip modulo

	// Channel to signal newly completed pieces
	completed chan *pb.Piece

	// Channel to signal torrents removed from qBittorrent
	// This allows the orchestrator to clean up partial data on cold
	removed chan string

	closed    atomic.Bool // Prevents sends after channel close
	closeOnce sync.Once   // Ensures channel is closed exactly once

	queueFullLogNano atomic.Int64 // last time we logged "piece queue full" (UnixNano)
}

// NewPieceMonitor creates a new piece tracker.
func NewPieceMonitor(
	client *qbittorrent.Client,
	source PieceSource,
	logger *slog.Logger,
	config PieceMonitorConfig,
) *PieceMonitor {
	return &PieceMonitor{
		client:      client,
		source:      source,
		logger:      logger,
		config:      config,
		retryConfig: utils.DefaultRetryConfig(),
		mainData:    &qbittorrent.MainData{},
		torrents:    make(map[string]*torrentState),
		completed:   make(chan *pb.Piece, completedChannelBufSize),
		removed:     make(chan string, removedChannelBufSize),
	}
}

// Completed returns the channel of pieces ready to stream.
func (t *PieceMonitor) Completed() <-chan *pb.Piece {
	return t.completed
}

// Removed returns the channel of torrent hashes that have been removed from qBittorrent.
// The orchestrator should listen to this channel and call AbortTorrent on cold to clean up.
func (t *PieceMonitor) Removed() <-chan string {
	return t.removed
}

// TrackTorrent explicitly adds a torrent to track (for manual control).
func (t *PieceMonitor) TrackTorrent(ctx context.Context, hash string) error {
	return t.startTracking(ctx, hash, nil)
}

// TrackTorrentWithResume adds a torrent to track, marking pieces in alreadyWritten
// as already streamed. This prevents re-queuing pieces that cold already has.
func (t *PieceMonitor) TrackTorrentWithResume(ctx context.Context, hash string, alreadyWritten []bool) error {
	return t.startTracking(ctx, hash, alreadyWritten)
}

// Untrack stops tracking a torrent.
func (t *PieceMonitor) Untrack(hash string) {
	t.mu.Lock()
	delete(t.torrents, hash)
	t.mu.Unlock()
}

// MarkStreamed marks a piece as successfully streamed.
func (t *PieceMonitor) MarkStreamed(hash string, pieceIndex int) {
	t.mu.RLock()
	state, ok := t.torrents[hash]
	t.mu.RUnlock()

	if !ok {
		return
	}

	state.mu.Lock()
	state.streamed[pieceIndex] = true
	state.failed[pieceIndex] = false
	state.mu.Unlock()
}

// MarkStreamedBatch marks multiple pieces as already streamed.
// Used to resume after restart - pieces already written to cold don't need re-streaming.
func (t *PieceMonitor) MarkStreamedBatch(hash string, written []bool) int {
	t.mu.RLock()
	state, ok := t.torrents[hash]
	t.mu.RUnlock()

	if !ok {
		return 0
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	count := 0
	for i, isWritten := range written {
		if isWritten && i < len(state.streamed) {
			state.streamed[i] = true
			state.failed[i] = false
			count++
		}
	}

	return count
}

// MarkFailed marks a piece as failed (for retry).
func (t *PieceMonitor) MarkFailed(hash string, pieceIndex int) {
	t.mu.RLock()
	state, ok := t.torrents[hash]
	t.mu.RUnlock()

	if !ok {
		return
	}

	state.mu.Lock()
	state.failed[pieceIndex] = true
	state.mu.Unlock()
}

// GetProgress returns streaming progress for a torrent.
func (t *PieceMonitor) GetProgress(hash string) (StreamProgress, error) {
	t.mu.RLock()
	state, ok := t.torrents[hash]
	t.mu.RUnlock()

	if !ok {
		return StreamProgress{}, ErrTorrentNotTracked
	}

	state.mu.RLock()
	defer state.mu.RUnlock()

	var streamed, failed int
	for i := range state.streamed {
		if state.streamed[i] {
			streamed++
		}
		if state.failed[i] {
			failed++
		}
	}

	numPieces := int(state.meta.GetNumPieces())

	return StreamProgress{
		TorrentHash: hash,
		TotalPieces: numPieces,
		Streamed:    streamed,
		Failed:      failed,
		Complete:    streamed == numPieces,
	}, nil
}

// Run starts the tracker polling loop.
func (t *PieceMonitor) Run(ctx context.Context) error {
	ticker := time.NewTicker(t.config.PollInterval)
	defer ticker.Stop()

	// Initial full sync
	if err := t.pollMainData(ctx); err != nil {
		metrics.StreamOpenErrorsTotal.WithLabelValues(metrics.ModeHot).Inc()
		t.logger.WarnContext(ctx, "initial maindata poll failed", "error", err)
	}

	for {
		select {
		case <-ctx.Done():
			t.closeChannels()
			return ctx.Err()
		case <-ticker.C:
			if err := t.poll(ctx); err != nil {
				metrics.StreamOpenErrorsTotal.WithLabelValues(metrics.ModeHot).Inc()
				t.logger.WarnContext(ctx, "poll cycle failed", "error", err)
			}
		}
	}
}

// closeChannels safely closes all output channels exactly once.
func (t *PieceMonitor) closeChannels() {
	t.closeOnce.Do(func() {
		t.closed.Store(true)
		close(t.completed)
		close(t.removed)
	})
}

// trySend safely sends a value to a channel with optional blocking behavior.
// Returns true if sent successfully, false if channel is closed, full (non-blocking), or context cancelled.
//
//nolint:nonamedreturns // Named return required for panic recovery
func trySend[T any](ctx context.Context, closed *atomic.Bool, ch chan<- T, val T, blocking bool) (sent bool) {
	if closed.Load() {
		return false
	}

	defer func() {
		if r := recover(); r != nil {
			sent = false
		}
	}()

	if blocking {
		select {
		case ch <- val:
			return true
		case <-ctx.Done():
			return false
		}
	}

	select {
	case ch <- val:
		return true
	case <-ctx.Done():
		return false
	default:
		return false
	}
}

// trySendCompleted safely sends a piece to the completed channel (blocking).
func (t *PieceMonitor) trySendCompleted(ctx context.Context, piece *pb.Piece) bool {
	return trySend(ctx, &t.closed, t.completed, piece, true)
}

// trySendCompletedNonBlocking safely sends a piece without blocking.
func (t *PieceMonitor) trySendCompletedNonBlocking(ctx context.Context, piece *pb.Piece) bool {
	return trySend(ctx, &t.closed, t.completed, piece, false)
}

// trySendRemoved safely sends a hash to the removed channel (blocking).
func (t *PieceMonitor) trySendRemoved(ctx context.Context, hash string) bool {
	return trySend(ctx, &t.closed, t.removed, hash, true)
}

// poll performs a two-tier polling cycle.
func (t *PieceMonitor) poll(ctx context.Context) error {
	// Tier 1: Efficient incremental maindata update
	if err := t.pollMainData(ctx); err != nil {
		return err
	}

	// Tier 2: Poll piece states only for active downloads
	t.pollActiveTorrents(ctx)

	return nil
}

// pollMainData uses sync/maindata with rid for incremental updates.
// Uses retry logic for transient network errors.
func (t *PieceMonitor) pollMainData(ctx context.Context) error {
	// MainData.Update requires raw client; wrap with retry for resilience
	_, err := utils.Retry(ctx, t.retryConfig, t.logger, "MainData.Update",
		func(ctx context.Context) (struct{}, error) {
			return struct{}{}, t.mainData.Update(ctx, t.client)
		})
	if err != nil {
		return err
	}

	// Process torrent changes
	for hash, torrent := range t.mainData.Torrents {
		t.processTorrentUpdate(ctx, hash, torrent)
	}

	// Handle removed torrents
	for _, hash := range t.mainData.TorrentsRemoved {
		t.mu.Lock()
		_, wasTracked := t.torrents[hash]
		if wasTracked {
			t.logger.InfoContext(ctx, "torrent removed from qBittorrent, stopping tracking", "hash", hash)
			delete(t.torrents, hash)
		}
		t.mu.Unlock()

		// Notify orchestrator about the removal so it can clean up cold
		if wasTracked {
			if t.trySendRemoved(ctx, hash) {
				t.logger.InfoContext(ctx, "notified orchestrator of torrent removal", "hash", hash)
			} else if ctx.Err() != nil {
				return ctx.Err()
			}
		}
	}

	return nil
}

// processTorrentUpdate handles a torrent update from maindata.
func (t *PieceMonitor) processTorrentUpdate(ctx context.Context, hash string, torrent qbittorrent.Torrent) {
	t.mu.RLock()
	_, isTracked := t.torrents[hash]
	t.mu.RUnlock()

	// Check if torrent is actively downloading
	isDownloading := torrent.Progress < completeProgress && t.isDownloadingState(torrent.State)

	if isDownloading && !isTracked {
		// New downloading torrent - start tracking
		// Note: nil for alreadyWritten since this is auto-discovery, not orchestrator-controlled
		if err := t.startTracking(ctx, hash, nil); err != nil {
			t.logger.WarnContext(ctx, "failed to start tracking torrent",
				"hash", hash,
				"error", err,
			)
		}
	} else if !isDownloading && isTracked {
		// Torrent complete or stopped - log progress but DON'T auto-delete.
		// The orchestrator manages the lifecycle: it checks GetProgress(),
		// calls FinalizeTorrent, then Untrack(). Auto-deletion here would
		// cause the orchestrator to miss the completion.
		progress, err := t.GetProgress(hash)
		if err == nil && progress.Complete {
			t.logger.DebugContext(ctx, "torrent fully streamed, awaiting finalization",
				"hash", hash,
				"streamed", progress.Streamed,
			)
		}
	}
}

// isDownloadingState returns true if the torrent state indicates active downloading.
func (t *PieceMonitor) isDownloadingState(state qbittorrent.TorrentState) bool {
	switch state {
	case qbittorrent.TorrentStateDownloading,
		qbittorrent.TorrentStateStalledDl,
		qbittorrent.TorrentStateQueuedDl,
		qbittorrent.TorrentStateForcedDl,
		qbittorrent.TorrentStateAllocating,
		qbittorrent.TorrentStateMetaDl:
		return true
	case qbittorrent.TorrentStateError,
		qbittorrent.TorrentStateMissingFiles,
		qbittorrent.TorrentStateUploading,
		qbittorrent.TorrentStatePausedUp,
		qbittorrent.TorrentStateStoppedUp,
		qbittorrent.TorrentStateQueuedUp,
		qbittorrent.TorrentStateStalledUp,
		qbittorrent.TorrentStateCheckingUp,
		qbittorrent.TorrentStateForcedUp,
		qbittorrent.TorrentStatePausedDl,
		qbittorrent.TorrentStateStoppedDl,
		qbittorrent.TorrentStateCheckingDl,
		qbittorrent.TorrentStateCheckingResumeData,
		qbittorrent.TorrentStateMoving,
		qbittorrent.TorrentStateUnknown:
		return false
	}
	return false
}

// startTracking begins tracking a torrent's piece completion.
// If alreadyWritten is provided, those pieces are marked as streamed before
// queuing, preventing re-transfer of data that cold already has.
func (t *PieceMonitor) startTracking(ctx context.Context, hash string, alreadyWritten []bool) error {
	meta, err := t.source.GetTorrentMetadata(ctx, hash)
	if err != nil {
		return err
	}

	hashes, err := t.source.GetPieceHashes(ctx, hash)
	if err != nil {
		return err
	}

	states, err := t.source.GetPieceStates(ctx, hash)
	if err != nil {
		return err
	}

	numPieces := int(meta.GetNumPieces())

	state := &torrentState{
		meta:       meta,
		hashes:     hashes,
		lastStates: states,
		streamed:   make([]bool, numPieces),
		failed:     make([]bool, numPieces),
	}

	// Apply already-written pieces BEFORE adding to map and queuing.
	// This prevents queueCompletedPieces from sending pieces that cold already has.
	resumedCount := 0
	for i, written := range alreadyWritten {
		if written && i < numPieces {
			state.streamed[i] = true
			resumedCount++
		}
	}

	t.mu.Lock()
	t.torrents[hash] = state
	t.mu.Unlock()

	// Queue any already-completed pieces (skips those marked as streamed above)
	t.queueCompletedPieces(ctx, state, states)

	t.logger.InfoContext(ctx, "started tracking torrent",
		"hash", hash,
		"name", meta.GetName(),
		"pieces", numPieces,
		"pieceSize", meta.GetPieceSize(),
		"resumed", resumedCount,
	)

	return nil
}

// pollActiveTorrents fetches piece states for all tracked torrents.
func (t *PieceMonitor) pollActiveTorrents(ctx context.Context) {
	t.tickCount++

	t.mu.RLock()
	hashes := make([]string, 0, len(t.torrents))
	for hash := range t.torrents {
		hashes = append(hashes, hash)
	}
	t.mu.RUnlock()

	for _, hash := range hashes {
		if t.shouldSkipIdleTorrent(hash) {
			metrics.IdlePollSkipsTotal.Inc()
			continue
		}

		if err := t.pollTorrentPieces(ctx, hash); err != nil {
			// If the torrent is no longer found, treat it as removed.
			// This handles cases where the torrent was deleted between maindata syncs.
			if errors.Is(err, ErrTorrentNotFound) {
				t.handleTorrentNotFound(ctx, hash)
				continue
			}
			t.logger.WarnContext(ctx, "failed to poll torrent pieces",
				"hash", hash,
				"error", err,
			)
		}
	}
}

// shouldSkipIdleTorrent returns true if the torrent has been idle long enough
// to skip this poll tick. Idle torrents are polled every idleSlowFactor ticks.
func (t *PieceMonitor) shouldSkipIdleTorrent(hash string) bool {
	t.mu.RLock()
	state, ok := t.torrents[hash]
	t.mu.RUnlock()
	if !ok {
		return false
	}

	state.mu.RLock()
	idle := state.idleTicks
	state.mu.RUnlock()

	return idle >= idleThreshold && t.tickCount%idleSlowFactor != 0
}

// handleTorrentNotFound handles detection of a removed torrent via 404 error.
// This is a fallback detection path when maindata sync misses the removal.
func (t *PieceMonitor) handleTorrentNotFound(ctx context.Context, hash string) {
	t.mu.Lock()
	_, wasTracked := t.torrents[hash]
	if wasTracked {
		t.logger.InfoContext(ctx, "torrent not found (404), treating as removed", "hash", hash)
		delete(t.torrents, hash)
	}
	t.mu.Unlock()

	// Notify orchestrator about the removal so it can clean up cold
	if wasTracked {
		if t.trySendRemoved(ctx, hash) {
			t.logger.InfoContext(ctx, "notified orchestrator of torrent removal (via 404)", "hash", hash)
		}
	}
}

func (t *PieceMonitor) pollTorrentPieces(ctx context.Context, hash string) error {
	states, err := t.source.GetPieceStates(ctx, hash)
	if err != nil {
		return err
	}

	t.mu.RLock()
	state, ok := t.torrents[hash]
	t.mu.RUnlock()

	if !ok {
		return ErrTorrentNotTracked
	}

	newPieces := t.queueCompletedPieces(ctx, state, states)

	state.mu.Lock()
	state.lastStates = states
	if newPieces > 0 {
		state.idleTicks = 0
	} else {
		state.idleTicks++
	}
	state.mu.Unlock()

	return nil
}

func (t *PieceMonitor) queueCompletedPieces(ctx context.Context, state *torrentState, current []PieceState) int {
	if t.closed.Load() {
		return 0
	}

	state.mu.RLock()
	defer state.mu.RUnlock()

	var queued, skipped int
	for i, pieceState := range current {
		if pieceState != PieceStateDownloaded {
			continue
		}
		if state.streamed[i] {
			continue
		}

		piece := t.buildPiece(state, i)

		switch {
		case t.trySendCompletedNonBlocking(ctx, piece):
			queued++
			t.logger.DebugContext(ctx, "queued piece",
				"hash", state.meta.GetTorrentHash(),
				"piece", i,
			)
		case ctx.Err() != nil:
			return queued
		case !t.closed.Load():
			skipped++
		}
	}

	if skipped > 0 {
		lastLog := t.queueFullLogNano.Load()
		if time.Duration(time.Now().UnixNano()-lastLog) >= queueFullLogInterval {
			t.logger.WarnContext(ctx, "piece queue full, will retry",
				"hash", state.meta.GetTorrentHash(),
				"skipped", skipped,
			)
			t.queueFullLogNano.Store(time.Now().UnixNano())
		}
	}

	return queued
}

func (t *PieceMonitor) buildPiece(state *torrentState, index int) *pb.Piece {
	pieceSize := state.meta.GetPieceSize()
	offset := int64(index) * pieceSize

	// Calculate actual size (last piece may be smaller)
	remaining := state.meta.GetTotalSize() - offset
	size := min(pieceSize, remaining)

	var hash string
	if index < len(state.hashes) {
		hash = state.hashes[index]
	}

	return &pb.Piece{
		TorrentHash: state.meta.GetTorrentHash(),
		Index:       int32(index),
		Offset:      offset,
		Size:        size,
		Hash:        hash,
	}
}

// RetryFailed re-queues failed pieces for retry.
func (t *PieceMonitor) RetryFailed(ctx context.Context, hash string) error {
	if t.closed.Load() {
		return ctx.Err()
	}

	t.mu.RLock()
	state, ok := t.torrents[hash]
	t.mu.RUnlock()

	if !ok {
		return ErrTorrentNotTracked
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	for i, failed := range state.failed {
		if !failed {
			continue
		}

		state.failed[i] = false
		piece := t.buildPiece(state, i)

		if !t.trySendCompleted(ctx, piece) {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Channel closed, stop retrying
			return nil
		}
	}

	return nil
}

// TrackedCount returns the number of torrents being tracked.
func (t *PieceMonitor) TrackedCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.torrents)
}

// IsTracking returns true if the given torrent is being tracked.
func (t *PieceMonitor) IsTracking(hash string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.torrents[hash]
	return ok
}

// IsPieceStreamed returns true if the given piece has been streamed.
func (t *PieceMonitor) IsPieceStreamed(hash string, pieceIndex int) bool {
	t.mu.RLock()
	state, ok := t.torrents[hash]
	t.mu.RUnlock()

	if !ok {
		return false
	}

	state.mu.RLock()
	defer state.mu.RUnlock()

	if pieceIndex >= len(state.streamed) {
		return false
	}
	return state.streamed[pieceIndex]
}

// GetTorrentMetadata returns metadata for a tracked torrent.
func (t *PieceMonitor) GetTorrentMetadata(hash string) (*TorrentMetadata, bool) {
	t.mu.RLock()
	state, ok := t.torrents[hash]
	t.mu.RUnlock()

	if !ok {
		return nil, false
	}

	return state.meta, true
}
