package destination

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/arsac/qb-sync/internal/metrics"
	"github.com/arsac/qb-sync/internal/utils"
	pb "github.com/arsac/qb-sync/proto"
)

// FinalizeTorrent completes the torrent transfer by renaming partial files,
// verifying piece integrity, adding to qBittorrent, and confirming.
//
// Verification runs in the background with a progress-based idle timeout
// (not tied to the RPC context) so it survives client-side deadline cancellation.
// If the source side calls FinalizeTorrent while verification is already running,
// the response indicates "in progress" without error so the source side can poll.
func (s *Server) FinalizeTorrent(
	ctx context.Context,
	req *pb.FinalizeTorrentRequest,
) (*pb.FinalizeTorrentResponse, error) {
	startTime := time.Now()
	hash := req.GetTorrentHash()

	state, stateErr := s.getOrRecoverState(ctx, hash)
	if stateErr != nil {
		//nolint:nilerr // gRPC returns errors in response body
		return &pb.FinalizeTorrentResponse{Success: false, Error: stateErr.Error()}, nil
	}

	// Check if finalization is already in progress or completed.
	state.mu.Lock()
	if state.finalizing {
		result, done := state.finalizeResult, state.finalizeDone
		state.mu.Unlock()
		return s.handleExistingFinalization(hash, state, result, done)
	}
	// Create finalizeDone immediately so concurrent polls always see it.
	done := state.startFinalization()
	writtenCount := state.writtenCount
	state.mu.Unlock()

	// Helper to clear finalizing state, close the done channel, record failure
	// metrics, and return error response. Used on all early exit paths before
	// the background goroutine is launched.
	failureResponse := func(errMsg string, code pb.FinalizeErrorCode) *pb.FinalizeTorrentResponse {
		close(done)
		state.mu.Lock()
		state.resetFinalization()
		state.mu.Unlock()
		metrics.FinalizationDuration.WithLabelValues(metrics.ResultFailure).Observe(time.Since(startTime).Seconds())
		metrics.FinalizationErrorsTotal.WithLabelValues(metrics.ModeDestination).Inc()
		return &pb.FinalizeTorrentResponse{Success: false, Error: errMsg, ErrorCode: code}
	}

	// Relocate files if save_sub_path changed (e.g., source moved torrent to different category).
	// Only relocate when request carries a non-empty sub-path to avoid accidental relocation
	// from an old source version that doesn't send this field.
	if newSubPath := req.GetSaveSubPath(); newSubPath != "" && newSubPath != state.saveSubPath {
		if relocErr := s.relocateForSubPathChange(ctx, hash, state, newSubPath); relocErr != nil {
			return failureResponse(relocErr.Error(), pb.FinalizeErrorCode_FINALIZE_ERROR_NONE), nil
		}
	}

	// Verify all selected pieces are written.
	// For partial selection, only pieces overlapping selected files must be written.
	selectedPiecesTotal := state.countSelectedPiecesTotal()
	if writtenCount < selectedPiecesTotal {
		msg := fmt.Sprintf("incomplete: %d/%d selected pieces", writtenCount, selectedPiecesTotal)
		return failureResponse(msg, pb.FinalizeErrorCode_FINALIZE_ERROR_INCOMPLETE), nil
	}

	// Finalize files (sync, close, rename .partial -> final).
	// This is idempotent — already renamed files are detected and skipped.
	if finalizeErr := s.finalizeFiles(ctx, hash, state); finalizeErr != nil {
		msg := fmt.Sprintf("finalizing files: %v", finalizeErr)
		return failureResponse(msg, pb.FinalizeErrorCode_FINALIZE_ERROR_NONE), nil
	}

	// Launch verification and post-verification steps in the background.
	// This decouples from the RPC context so verification survives source-side
	// deadline cancellation. The finalizing flag and finalizeDone channel were
	// set upfront (under lock) so concurrent polls see "verifying" immediately.
	go s.runBackgroundFinalization(hash, state, req, startTime, done)

	// Return "verifying" to the source side. Source should poll via subsequent
	// FinalizeTorrent calls until it gets the final result.
	return &pb.FinalizeTorrentResponse{
		Success: true,
		State:   "verifying",
	}, nil
}

// runBackgroundFinalization runs piece verification and post-verification steps
// (inode registration, qBittorrent integration) independently of the RPC context.
// On completion, it stores the result in state.finalizeResult and closes done.
func (s *Server) runBackgroundFinalization(
	hash string,
	state *serverTorrentState,
	req *pb.FinalizeTorrentRequest,
	startTime time.Time,
	done chan struct{},
) {
	defer close(done)

	// storeFailure records failure metrics and stores the error for the next poll.
	// errorCode is included in the result so source can make retry decisions.
	storeFailure := func(errMsg string, errorCode pb.FinalizeErrorCode) {
		metrics.FinalizationDuration.WithLabelValues(metrics.ResultFailure).Observe(time.Since(startTime).Seconds())
		metrics.FinalizationErrorsTotal.WithLabelValues(metrics.ModeDestination).Inc()
		state.mu.Lock()
		state.finalizeResult = &finalizeResult{err: errMsg, errorCode: errorCode}
		state.mu.Unlock()
	}

	// Serialize background finalizations to prevent disk I/O and qBittorrent
	// API saturation when many torrents complete around the same time.
	// Use a generous wait context — queue time doesn't count against actual work.
	waitCtx, waitCancel := context.WithTimeout(context.Background(), finalizeQueueTimeout)
	if acquireErr := s.finalizeSem.Acquire(waitCtx, 1); acquireErr != nil {
		waitCancel()
		s.logger.WarnContext(context.Background(), "finalization queue timeout",
			"hash", hash,
			"error", acquireErr,
		)
		storeFailure(
			fmt.Sprintf("finalization queue timeout: %v", acquireErr),
			pb.FinalizeErrorCode_FINALIZE_ERROR_NONE,
		)
		return
	}
	waitCancel()
	defer s.finalizeSem.Release(1)

	s.logger.InfoContext(context.Background(), "acquired finalization slot",
		"hash", hash,
		"queueWait", time.Since(startTime).Round(time.Millisecond),
	)

	// Work timeout starts after acquiring the semaphore — queue wait doesn't
	// eat into the time budget for verification and qBittorrent operations.
	ctx, cancel := context.WithTimeout(context.Background(), backgroundFinalizeTimeout)
	defer cancel()

	// Verify all pieces by reading back from finalized files.
	failedPieces, verifyErr := s.verifyFinalizedPieces(ctx, hash, state)
	if verifyErr != nil {
		// System-level error (context cancel, idle timeout)
		s.logger.ErrorContext(ctx, "background verification failed",
			"hash", hash,
			"error", verifyErr,
		)
		storeFailure(
			fmt.Sprintf("verification failed: %v", verifyErr),
			pb.FinalizeErrorCode_FINALIZE_ERROR_NONE,
		)
		return
	}
	if len(failedPieces) > 0 {
		// Piece corruption — recover and signal incomplete to source.
		s.recoverVerificationFailure(ctx, hash, state, failedPieces)
		metrics.VerificationRecoveriesTotal.Inc()
		storeFailure(
			fmt.Sprintf("verification failed: %d pieces corrupted, will re-stream", len(failedPieces)),
			pb.FinalizeErrorCode_FINALIZE_ERROR_INCOMPLETE,
		)
		return
	}

	// Register inodes for files we wrote (not hardlinked) and signal waiters.
	s.registerFinalizedInodes(ctx, hash, state)

	// Add to qBittorrent if configured and not in dry-run mode.
	if s.qbClient != nil && !s.config.DryRun {
		finalState, qbErr := s.addAndVerifyTorrent(ctx, hash, state, req)
		if qbErr != nil {
			s.logger.ErrorContext(ctx, "background qBittorrent integration failed",
				"hash", hash,
				"error", qbErr,
			)
			storeFailure(
				fmt.Sprintf("qBittorrent: %v", qbErr),
				pb.FinalizeErrorCode_FINALIZE_ERROR_NONE,
			)
			return
		}

		// Apply synced tag for visibility (not used as source of truth).
		if s.config.SyncedTag != "" {
			if tagErr := s.qbClient.AddTagsCtx(ctx, []string{hash}, s.config.SyncedTag); tagErr != nil {
				metrics.TagApplicationErrorsTotal.WithLabelValues(metrics.ModeDestination).Inc()
				s.logger.ErrorContext(ctx, "failed to add synced tag",
					"hash", hash,
					"tag", s.config.SyncedTag,
					"error", tagErr,
				)
			}
		}

		s.storeSuccessResult(ctx, hash, state, string(finalState), startTime)
		return
	}

	s.storeSuccessResult(ctx, hash, state, "finalized", startTime)
}

// storeSuccessResult records success metrics and stores the result for the next poll.
func (s *Server) storeSuccessResult(
	ctx context.Context,
	hash string,
	state *serverTorrentState,
	stateStr string,
	startTime time.Time,
) {
	metrics.FinalizationDuration.WithLabelValues(metrics.ResultSuccess).Observe(time.Since(startTime).Seconds())
	name := strings.TrimSuffix(filepath.Base(state.torrentPath), ".torrent")
	metrics.TorrentsSyncedTotal.WithLabelValues(metrics.ModeDestination, hash, name).Inc()
	s.logger.InfoContext(ctx, "torrent finalized (background)", "hash", hash, "state", stateStr)

	state.mu.Lock()
	state.finalizeResult = &finalizeResult{success: true, state: stateStr}
	state.mu.Unlock()
}

// cleanupFinalizedTorrent removes a successfully finalized torrent from tracking
// and cleans up its metadata directory.
func (s *Server) cleanupFinalizedTorrent(hash string) {
	s.mu.Lock()
	delete(s.torrents, hash)
	s.mu.Unlock()

	// Remove metadata directory (.qbsync/{hash}/) — no longer needed after finalization.
	metaDir := filepath.Join(s.config.BasePath, metaDirName, hash)
	if err := os.RemoveAll(metaDir); err != nil && !os.IsNotExist(err) {
		s.logger.Warn("failed to clean up metadata directory after finalization",
			"hash", hash,
			"path", metaDir,
			"error", err,
		)
	}
}

// relocateForSubPathChange moves files when save_sub_path changed between init and finalize.
func (s *Server) relocateForSubPathChange(
	ctx context.Context,
	hash string,
	state *serverTorrentState,
	newSubPath string,
) error {
	oldSubPath := state.saveSubPath
	oldBase := filepath.Join(s.config.BasePath, oldSubPath)

	relPaths := make([]string, len(state.files))
	for i, fi := range state.files {
		rel, relErr := filepath.Rel(oldBase, strings.TrimSuffix(fi.path, partialSuffix))
		if relErr != nil {
			return fmt.Errorf("computing relative path: %w", relErr)
		}
		relPaths[i] = rel
	}

	if _, relocErr := s.relocateFiles(ctx, hash, relPaths, oldSubPath, newSubPath); relocErr != nil {
		return fmt.Errorf("relocating files: %w", relocErr)
	}

	updateStateAfterRelocate(state, s.config.BasePath, oldSubPath, newSubPath)
	metaDir := filepath.Join(s.config.BasePath, metaDirName, hash)
	if subPathErr := saveSubPathFile(metaDir, newSubPath); subPathErr != nil {
		return fmt.Errorf("persisting sub-path after relocation: %w", subPathErr)
	}

	return nil
}

// getOrRecoverState gets the torrent state from memory, or recovers it from disk.
func (s *Server) getOrRecoverState(ctx context.Context, hash string) (*serverTorrentState, error) {
	s.mu.RLock()
	state, exists := s.torrents[hash]
	initializing := exists && state.initializing
	s.mu.RUnlock()

	if exists {
		if initializing {
			return nil, errors.New("torrent initialization in progress")
		}
		return state, nil
	}

	// Try to recover state from disk (after server restart)
	recoveredState, recoverErr := s.recoverTorrentState(ctx, hash)
	if recoverErr != nil {
		s.logger.DebugContext(ctx, "failed to recover torrent state",
			"hash", hash,
			"error", recoverErr,
		)
		return nil, errors.New("torrent not found")
	}

	// Check-and-insert atomically to prevent TOCTOU race — another concurrent
	// FinalizeTorrent could have recovered and inserted state between our
	// RUnlock above and this Lock.
	s.mu.Lock()
	if existing, alreadyInserted := s.torrents[hash]; alreadyInserted {
		s.mu.Unlock()
		return existing, nil
	}
	s.torrents[hash] = recoveredState
	s.mu.Unlock()

	s.logger.InfoContext(ctx, "recovered torrent state from disk",
		"hash", hash,
		"pieces", len(recoveredState.written),
		"files", len(recoveredState.files),
	)

	return recoveredState, nil
}

// finalizeFiles syncs all file handles, closes them, and renames from .partial to final.
// Also resolves pending hardlinks by waiting for source files to complete.
//
//nolint:gocognit
func (s *Server) finalizeFiles(ctx context.Context, hash string, state *serverTorrentState) error {
	// Phase 1: Resolve pending hardlinks without holding state.mu.
	// Waiting on hardlink channels can block for up to defaultHardlinkWaitTimeout,
	// and holding the lock would block all WritePiece calls for that duration.
	// Safe because: files slice is immutable after init, and finalizing=true prevents writes.
	for _, fi := range state.files {
		if fi.hlState != hlStatePending {
			continue
		}

		s.logger.DebugContext(ctx, "waiting for pending hardlink source",
			"hash", hash,
			"target", fi.path,
			"source", fi.hardlinkSource,
		)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(defaultHardlinkWaitTimeout):
			return fmt.Errorf("timeout waiting for pending hardlink source %s (waited %v)",
				fi.hardlinkSource, defaultHardlinkWaitTimeout)
		case <-fi.hardlinkDoneCh:
			// Source is ready
		}

		sourcePath := filepath.Join(s.config.BasePath, fi.hardlinkSource)
		if linkErr := os.Link(sourcePath, fi.path); linkErr != nil {
			if os.IsExist(linkErr) {
				s.logger.DebugContext(ctx, "pending hardlink target already exists",
					"hash", hash,
					"target", fi.path,
				)
			} else {
				return fmt.Errorf("creating pending hardlink %s -> %s: %w",
					sourcePath, fi.path, linkErr)
			}
		} else {
			metrics.HardlinksCreatedTotal.Inc()
			s.logger.InfoContext(ctx, "created pending hardlink",
				"hash", hash,
				"source", sourcePath,
				"target", fi.path,
			)
		}

		fi.hlState = hlStateComplete
	}

	// Phase 2: Sync, close, and rename under lock.
	state.mu.Lock()
	defer state.mu.Unlock()

	// Sync and close all file handles before rename.
	// Fail early if any file can't be flushed — renaming unflushed files
	// risks data loss, especially on NFS where sync is less reliable.
	for _, fi := range state.files {
		if fi.hlState != hlStateComplete {
			if err := s.closeFileHandle(ctx, hash, fi); err != nil {
				return fmt.Errorf("flushing before rename: %w", err)
			}
		}
	}

	// Then rename partial files
	for _, fi := range state.files {
		if fi.hlState == hlStateComplete {
			continue
		}
		if err := s.renamePartialFile(ctx, hash, fi); err != nil {
			return err
		}
	}

	s.saveStatePath(ctx, hash, state)
	return nil
}

// closeFileHandle syncs and closes an open file handle.
// Returns an error if sync or close fails (data may not be durable).
func (s *Server) closeFileHandle(ctx context.Context, hash string, fi *serverFileInfo) error {
	if fi.file == nil {
		return nil
	}

	var syncErr error
	if syncErr = fi.file.Sync(); syncErr != nil {
		metrics.FileSyncErrorsTotal.WithLabelValues(metrics.ModeDestination).Inc()
		s.logger.WarnContext(ctx, "failed to sync file",
			"hash", hash,
			"path", fi.path,
			"error", syncErr,
		)
	}

	closeErr := fi.file.Close()
	fi.file = nil
	if closeErr != nil {
		metrics.FileSyncErrorsTotal.WithLabelValues(metrics.ModeDestination).Inc()
		s.logger.WarnContext(ctx, "failed to close file",
			"hash", hash,
			"path", fi.path,
			"error", closeErr,
		)
		return fmt.Errorf("closing %s: %w", fi.path, closeErr)
	}

	if syncErr != nil {
		return fmt.Errorf("syncing %s: %w", fi.path, syncErr)
	}
	return nil
}

// renamePartialFile renames a .partial file to its final path.
func (s *Server) renamePartialFile(ctx context.Context, hash string, fi *serverFileInfo) error {
	if !strings.HasSuffix(fi.path, partialSuffix) {
		return nil
	}

	finalPath := strings.TrimSuffix(fi.path, partialSuffix)

	// Check if final path already exists (idempotent)
	if _, statErr := os.Stat(finalPath); statErr == nil {
		s.logger.DebugContext(ctx, "final file already exists",
			"hash", hash,
			"path", finalPath,
		)
		return nil
	}

	if renameErr := os.Rename(fi.path, finalPath); renameErr != nil {
		// Race condition: another rename may have completed
		if os.IsNotExist(renameErr) {
			if _, statErr := os.Stat(finalPath); statErr == nil {
				return nil
			}
		}
		return fmt.Errorf("renaming %s: %w", fi.path, renameErr)
	}

	s.logger.DebugContext(ctx, "renamed file",
		"hash", hash,
		"from", fi.path,
		"to", finalPath,
	)
	return nil
}

// saveStatePath saves the torrent state to disk.
func (s *Server) saveStatePath(ctx context.Context, hash string, state *serverTorrentState) {
	if state.statePath == "" {
		return
	}

	if saveErr := s.saveState(state.statePath, state.written); saveErr != nil {
		s.logger.WarnContext(ctx, "failed to save final state",
			"hash", hash,
			"error", saveErr,
		)
	}
}

// registerFinalizedInodes registers inodes for files we wrote (not hardlinked)
// and signals any waiting torrents that the files are ready for hardlinking.
func (s *Server) registerFinalizedInodes(ctx context.Context, hash string, state *serverTorrentState) {
	state.mu.Lock()
	defer state.mu.Unlock()

	var registered int
	for _, f := range state.files {
		// Skip files that need no data (hardlinked/pending/unselected) or have no inode
		if f.skipForWriteData() || f.sourceInode == 0 {
			continue
		}

		// Get final path (remove .partial suffix if present)
		finalPath, _ := strings.CutSuffix(f.path, partialSuffix)

		// Get relative path for storage
		relPath, relErr := filepath.Rel(s.config.BasePath, finalPath)
		if relErr != nil {
			s.logger.WarnContext(ctx, "failed to get relative path for inode registration",
				"hash", hash,
				"path", finalPath,
				"error", relErr,
			)
			continue
		}

		// Register in persistent map and signal waiters
		s.inodes.Register(f.sourceInode, relPath)
		s.inodes.CompleteInProgress(f.sourceInode, hash)
		registered++
	}

	if registered > 0 {
		s.logger.InfoContext(ctx, "registered finalized inodes",
			"hash", hash,
			"count", registered,
		)

		// Persist inode map
		if saveErr := s.inodes.Save(); saveErr != nil {
			s.logger.WarnContext(ctx, "failed to save inode map", "error", saveErr)
		}
	}
}

// verifyFinalizedPieces reads back all pieces from finalized files and verifies their hashes.
// Uses a progress-based idle timeout: as long as pieces keep being verified, it continues.
// Only aborts if no piece is verified within verifyIdleTimeout.
//
// Return semantics:
//   - (nil, nil) — all pieces verified OK
//   - (failedPieces, nil) — piece-level corruption, recovery needed
//   - (nil, err) — system error (context cancel, idle timeout)
//
//nolint:gocognit
func (s *Server) verifyFinalizedPieces(
	ctx context.Context,
	hash string,
	state *serverTorrentState,
) ([]int, error) {
	if len(state.pieceHashes) == 0 {
		return nil, errors.New(
			"no piece hashes available for verification — refusing to finalize without integrity check",
		)
	}
	if state.pieceLength <= 0 || state.totalSize <= 0 {
		return nil, errors.New(
			"missing piece size or total size metadata — refusing to finalize without integrity check",
		)
	}

	pieceSize := state.pieceLength
	totalSize := state.totalSize
	numPieces := len(state.pieceHashes)

	s.logger.InfoContext(ctx, "verifying finalized pieces",
		"hash", hash,
		"pieces", numPieces,
		"pieceSize", pieceSize,
	)

	// Create a context with progress-based idle timeout.
	// The cancel func is called by the idle watchdog if no progress is made.
	idleCtx, idleCancel := context.WithCancel(ctx)
	defer idleCancel()

	// Verify pieces in parallel — all accessed state fields (pieceHashes,
	// pieceLength, totalSize, files) are immutable at this point (finalizing=true).
	g, gCtx := errgroup.WithContext(idleCtx)
	g.SetLimit(maxVerifyConcurrency)

	var verified atomic.Int64
	var lastProgress atomic.Value // stores time.Time of last verified piece
	lastProgress.Store(time.Now())

	// Collect failed piece indices under a mutex (multiple goroutines may append).
	var failedMu sync.Mutex
	var failedPieces []int

	go s.verifyIdleWatchdog(ctx, gCtx, hash, numPieces, &verified, &lastProgress, idleCancel)

	for i, expectedHash := range state.pieceHashes {
		if expectedHash == "" {
			continue
		}

		// Skip pieces not fully covered by selected files — boundary pieces
		// (spanning selected + unselected) can't be read back because the
		// unselected file's data doesn't exist on disk. Those were hash-verified
		// at write time.
		if state.classifyPiece(i) != pieceFullySelected {
			continue
		}

		g.Go(func() error {
			if gCtx.Err() != nil {
				return gCtx.Err()
			}

			// Calculate piece offset and size
			offset := int64(i) * pieceSize
			size := pieceSize
			if offset+size > totalSize {
				size = totalSize - offset
			}

			// Read and verify piece hash. Failures are collected (not fail-fast)
			// so all corrupted pieces are found in a single pass.
			if !s.verifyOnePiece(ctx, hash, state, i, offset, size, expectedHash) {
				failedMu.Lock()
				failedPieces = append(failedPieces, i)
				failedMu.Unlock()
			}

			// Record progress for idle watchdog.
			lastProgress.Store(time.Now())

			if count := verified.Add(1); count%50 == 0 || count == int64(numPieces) {
				s.logger.InfoContext(ctx, "verification progress",
					"hash", hash,
					"verified", count,
					"total", numPieces,
				)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	if len(failedPieces) > 0 {
		s.logger.ErrorContext(ctx, "verification found corrupted pieces",
			"hash", hash,
			"failedCount", len(failedPieces),
			"failedPieces", failedPieces,
		)
		return failedPieces, nil
	}

	s.logger.InfoContext(ctx, "all pieces verified successfully", "hash", hash, "pieces", numPieces)
	return nil, nil
}

// verifyOnePiece reads a single piece from finalized files and verifies its hash.
// Returns true if the piece is valid, false if it's corrupted or unreadable.
func (s *Server) verifyOnePiece(
	ctx context.Context,
	hash string,
	state *serverTorrentState,
	pieceIdx int,
	offset, size int64,
	expectedHash string,
) bool {
	data, readErr := s.readPieceFromFinalizedFiles(state, offset, size)
	if readErr != nil {
		metrics.VerificationErrorsTotal.WithLabelValues(metrics.ModeDestination).Inc()
		s.logger.WarnContext(ctx, "piece read failed during verification",
			"hash", hash, "piece", pieceIdx, "error", readErr,
		)
		return false
	}
	if hashErr := utils.VerifyPieceHash(data, expectedHash); hashErr != nil {
		metrics.VerificationErrorsTotal.WithLabelValues(metrics.ModeDestination).Inc()
		s.logger.WarnContext(ctx, "piece hash mismatch during verification",
			"hash", hash, "piece", pieceIdx, "error", hashErr,
		)
		return false
	}
	return true
}

// verifyIdleWatchdog cancels verification if no progress within verifyIdleTimeout.
func (s *Server) verifyIdleWatchdog(
	ctx, gCtx context.Context,
	hash string,
	numPieces int,
	verified *atomic.Int64,
	lastProgress *atomic.Value,
	cancel context.CancelFunc,
) {
	ticker := time.NewTicker(verifyIdleTimeout / verifyIdleCheckDivisor)
	defer ticker.Stop()
	for {
		select {
		case <-gCtx.Done():
			return
		case <-ticker.C:
			last, ok := lastProgress.Load().(time.Time)
			if !ok {
				continue
			}
			if time.Since(last) > verifyIdleTimeout {
				s.logger.ErrorContext(ctx, "verification stalled, aborting",
					"hash", hash,
					"verified", verified.Load(),
					"total", numPieces,
					"idleTimeout", verifyIdleTimeout,
				)
				cancel()
				return
			}
		}
	}
}

// recoverVerificationFailure marks corrupted pieces as unwritten and renames
// affected files back to .partial so that source can re-stream them.
func (s *Server) recoverVerificationFailure(
	ctx context.Context,
	hash string,
	state *serverTorrentState,
	failedPieces []int,
) {
	state.mu.Lock()
	defer state.mu.Unlock()

	for _, p := range failedPieces {
		state.written[p] = false
		state.writtenCount--
	}

	// Find and recover affected files.
	for _, fi := range state.files {
		s.recoverAffectedFile(ctx, hash, state, fi, failedPieces)
	}

	// Persist the recovered state.
	state.dirty = true
	if saveErr := s.doSaveState(state.statePath, state.written); saveErr != nil {
		metrics.StateSaveErrorsTotal.WithLabelValues(metrics.ModeDestination).Inc()
		s.logger.ErrorContext(ctx, "failed to persist state after verification recovery",
			"hash", hash,
			"error", saveErr,
		)
	}

	s.logger.InfoContext(ctx, "recovered from verification failure",
		"hash", hash,
		"failedPieces", len(failedPieces),
		"writtenCount", state.writtenCount,
	)
}

// recoverAffectedFile renames a single file back to .partial and recalculates
// its piecesWritten if any failed piece overlaps it.
func (s *Server) recoverAffectedFile(
	ctx context.Context,
	hash string,
	state *serverTorrentState,
	fi *serverFileInfo,
	failedPieces []int,
) {
	if fi.skipForWriteData() {
		return
	}

	// Check if any failed piece overlaps this file.
	affected := slices.ContainsFunc(failedPieces, fi.overlaps)
	if !affected {
		return
	}

	// Rename final file back to .partial (skip if already .partial).
	// Even if rename fails, we still clear earlyFinalized and recalculate
	// piecesWritten so bookkeeping stays consistent with state.written.
	if !strings.HasSuffix(fi.path, partialSuffix) {
		partialPath := fi.path + partialSuffix
		if renameErr := os.Rename(fi.path, partialPath); renameErr != nil {
			s.logger.WarnContext(ctx, "failed to rename file back to partial",
				"hash", hash,
				"path", fi.path,
				"error", renameErr,
			)
		} else {
			fi.path = partialPath
		}
	}
	fi.earlyFinalized = false
	fi.recalcPiecesWritten(state.written)
}

// handleExistingFinalization handles a FinalizeTorrent call when background
// finalization is already in progress. It returns the cached result if available,
// or tells source to poll again.
func (s *Server) handleExistingFinalization(
	hash string,
	state *serverTorrentState,
	result *finalizeResult,
	done chan struct{},
) (*pb.FinalizeTorrentResponse, error) {
	// Background work is still running — tell source it's in progress
	// so it polls again without counting this as a failure.
	if result == nil {
		return &pb.FinalizeTorrentResponse{
			Success: true,
			State:   "verifying",
		}, nil
	}

	if result.success {
		// Clean up now that source has received the success response.
		s.cleanupFinalizedTorrent(hash)
		return &pb.FinalizeTorrentResponse{Success: true, State: result.state}, nil
	}

	// Background finalization failed — wait for the goroutine to fully exit
	// before clearing state, preventing concurrent background goroutines.
	<-done
	state.mu.Lock()
	state.resetFinalization()
	state.mu.Unlock()
	return &pb.FinalizeTorrentResponse{
		Success:   false,
		Error:     result.err,
		ErrorCode: result.errorCode,
	}, nil
}

// readPieceFromFinalizedFiles reads piece data from finalized (non-.partial) files.
func (s *Server) readPieceFromFinalizedFiles(state *serverTorrentState, offset, size int64) ([]byte, error) {
	regions := make([]utils.FileRegion, len(state.files))
	for i, fi := range state.files {
		path, _ := strings.CutSuffix(fi.path, partialSuffix)
		regions[i] = utils.FileRegion{Path: path, Offset: fi.offset, Size: fi.size}
	}
	return utils.ReadPieceFromFiles(regions, offset, size)
}
