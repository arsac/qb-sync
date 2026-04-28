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

	state, exists := s.store.Get(hash)
	if !exists {
		return &pb.FinalizeTorrentResponse{
			Success:   false,
			Error:     "torrent not found",
			ErrorCode: pb.FinalizeErrorCode_FINALIZE_ERROR_NOT_FOUND,
		}, nil
	}
	if state.initializing {
		return &pb.FinalizeTorrentResponse{
			Success:   false,
			Error:     "torrent initialization in progress",
			ErrorCode: pb.FinalizeErrorCode_FINALIZE_ERROR_NOT_FOUND,
		}, nil
	}

	// Check if finalization is already in progress or completed.
	state.mu.Lock()
	if state.finalization.active {
		result, done := state.finalization.result, state.finalization.done
		state.mu.Unlock()
		return s.handleExistingFinalization(hash, state, result, done)
	}
	// Create finalizeDone immediately so concurrent polls always see it.
	done := state.finalization.start()
	writtenCount := int(state.written.Count())
	state.mu.Unlock()

	// Helper to clear finalizing state, close the done channel, record failure
	// metrics, and return error response. Used on all early exit paths before
	// the background goroutine is launched.
	failureResponse := func(errMsg string, code pb.FinalizeErrorCode) *pb.FinalizeTorrentResponse {
		close(done)
		state.mu.Lock()
		state.finalization.reset()
		state.mu.Unlock()
		metrics.FinalizationDuration.WithLabelValues(metrics.ResultFailure).Observe(time.Since(startTime).Seconds())
		metrics.FinalizationErrorsTotal.WithLabelValues(metrics.ModeDestination).Inc()
		return &pb.FinalizeTorrentResponse{Success: false, Error: errMsg, ErrorCode: code}
	}

	// Relocate files if save_sub_path changed (e.g., source moved torrent to different category).
	// Only relocate when the source explicitly set save_sub_path (save_sub_path_explicit=true)
	// to avoid accidental relocation from an old source version that doesn't send this field.
	if newSubPath := req.GetSaveSubPath(); req.GetSaveSubPathExplicit() && newSubPath != state.saveSubPath {
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
	// Tracked via bgWg so shutdown waits for completion before cleanup.
	//
	// Check bgCtx before launching: during the shutdown window between
	// GracefulStop returning and bgCancel() being called, a goroutine launched
	// here would immediately fail with a cancelled context. Fail fast instead.
	if s.bgCtx.Err() != nil {
		//nolint:nilerr // bgCtx.Err is a context error, not the function's error; we return a structured gRPC failure.
		return failureResponse("server shutting down", pb.FinalizeErrorCode_FINALIZE_ERROR_NONE), nil
	}
	s.bgWg.Go(func() {
		s.runBackgroundFinalization(hash, state, req, startTime, done)
	})

	// Return "verifying" to the source side. Source should poll via subsequent
	// FinalizeTorrent calls until it gets the final result.
	return &pb.FinalizeTorrentResponse{
		Success: true,
		State:   "verifying",
	}, nil
}

// runBackgroundFinalization runs piece verification and post-verification steps
// (inode registration, qBittorrent integration) independently of the RPC context.
// On completion, it stores the result in state.finalization and closes done.
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
		state.finalization.storeResult(&finalizeResult{err: errMsg, errorCode: errorCode})
		state.mu.Unlock()
	}

	// Serialize background finalizations to prevent disk I/O and qBittorrent
	// API saturation when many torrents complete around the same time.
	// Use a generous wait context — queue time doesn't count against actual work.
	// Derived from s.bgCtx so server shutdown cancels queued goroutines.
	waitCtx, waitCancel := context.WithTimeout(s.bgCtx, finalizeQueueTimeout)
	if acquireErr := s.finalizeSem.Acquire(waitCtx, 1); acquireErr != nil {
		waitCancel()
		s.logger.WarnContext(s.bgCtx, "finalization queue timeout",
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

	s.logger.DebugContext(s.bgCtx, "acquired finalization slot",
		"hash", hash,
		"queueWait", time.Since(startTime).Round(time.Millisecond),
	)

	// Work timeout starts after acquiring the semaphore — queue wait doesn't
	// eat into the time budget for verification and qBittorrent operations.
	// Derived from s.bgCtx so server shutdown cancels in-flight work.
	ctx, cancel := context.WithTimeout(s.bgCtx, backgroundFinalizeTimeout)
	defer cancel()

	// Sync parent directories before verification to ensure NFS has flushed
	// file data and renames to the server. Without this, verification can
	// read stale data from the NFS client cache, causing false hash mismatches.
	s.syncFileParentDirs(ctx, hash, state)

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
		s.abortInProgressInodes(ctx, hash, state)
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

// storeSuccessResult records success metrics, writes the finalized marker,
// and stores the result for the next poll.
func (s *Server) storeSuccessResult(
	ctx context.Context,
	hash string,
	state *serverTorrentState,
	stateStr string,
	startTime time.Time,
) {
	metrics.FinalizationDuration.WithLabelValues(metrics.ResultSuccess).Observe(time.Since(startTime).Seconds())

	// Write finalized marker immediately so the torrent is recognized as
	// complete even if the server restarts before the source polls.
	metaDir := filepath.Join(s.config.BasePath, metaDirName, hash)
	s.markFinalized(metaDir, hash)

	state.mu.Lock()
	state.finalization.storeResult(&finalizeResult{success: true, state: stateStr})
	state.mu.Unlock()

	metrics.TorrentsSyncedTotal.WithLabelValues(metrics.ModeDestination, hash, hash).Inc()
	s.logger.InfoContext(ctx, "torrent finalized (background)", "hash", hash, "state", stateStr)
}

// markFinalized replaces the metadata directory contents with a single
// .finalized marker file. Removes .state, .torrent, and other working
// files but keeps the directory so the marker persists.
func (s *Server) markFinalized(metaDir, hash string) {
	// Remove working files but keep the directory.
	entries, err := os.ReadDir(metaDir)
	if err != nil {
		// Directory may not exist (already cleaned up). Create it for the marker.
		if mkErr := os.MkdirAll(metaDir, serverDirPermissions); mkErr != nil {
			s.logger.Warn("failed to create metadata directory for finalized marker",
				"hash", hash, "error", mkErr)
			return
		}
	} else {
		for _, e := range entries {
			if e.Name() == finalizedFileName {
				continue
			}
			_ = os.RemoveAll(filepath.Join(metaDir, e.Name()))
		}
	}

	markerPath := filepath.Join(metaDir, finalizedFileName)
	if writeErr := atomicWriteFile(markerPath, nil); writeErr != nil {
		s.logger.Warn("failed to write finalized marker",
			"hash", hash, "error", writeErr)
	}
}

// relocateForSubPathChange moves files when save_sub_path changed between init and finalize.
// Safe to read state.files and state.saveSubPath without state.mu here because
// finalization.active prevents concurrent WritePiece. However, updateStateAfterRelocate
// mutates state.files paths and state.saveSubPath, so it acquires state.mu.
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

	state.mu.Lock()
	relocErr := updateStateAfterRelocate(state, s.config.BasePath, oldSubPath, newSubPath)
	state.mu.Unlock()
	if relocErr != nil {
		return fmt.Errorf("updating state after relocation: %w", relocErr)
	}

	metaDir := filepath.Join(s.config.BasePath, metaDirName, hash)
	metaPath := filepath.Join(metaDir, metaFileName)
	if existingMeta, loadErr := loadPersistedMeta(metaPath); loadErr == nil {
		existingMeta.SaveSubPath = newSubPath
		if saveErr := savePersistedMeta(metaPath, existingMeta); saveErr != nil {
			return fmt.Errorf("persisting sub-path after relocation: %w", saveErr)
		}
	} else {
		s.logger.WarnContext(ctx, "could not update .meta after relocation",
			"hash", hash, "error", loadErr)
	}

	return nil
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
		if fi.hardlink.state != hlStatePending {
			continue
		}

		s.logger.DebugContext(ctx, "waiting for pending hardlink source",
			"hash", hash,
			"target", fi.path,
			"source", fi.hardlink.sourcePath,
		)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(defaultHardlinkWaitTimeout):
			return fmt.Errorf("timeout waiting for pending hardlink source %s (waited %v)",
				fi.hardlink.sourcePath, defaultHardlinkWaitTimeout)
		case <-fi.hardlink.doneCh:
			// Source is ready
		}

		sourcePath := filepath.Join(s.config.BasePath, fi.hardlink.sourcePath)
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

		fi.hardlink.markComplete()
	}

	// Phase 2: Sync, close, and rename under lock.
	state.mu.Lock()
	defer state.mu.Unlock()

	// Sync and close all file handles before rename.
	// Fail early if any file can't be flushed — renaming unflushed files
	// risks data loss, especially on NFS where sync is less reliable.
	for _, fi := range state.files {
		if fi.hardlink.state != hlStateComplete {
			if err := s.closeFileHandle(ctx, hash, fi); err != nil {
				return fmt.Errorf("flushing before rename: %w", err)
			}
		}
	}

	// Then rename partial files and update in-memory paths.
	for _, fi := range state.files {
		if fi.hardlink.state == hlStateComplete {
			continue
		}
		if err := s.renamePartialFile(ctx, hash, fi); err != nil {
			return err
		}
		fi.path = strings.TrimSuffix(fi.path, partialSuffix)
	}

	s.flushWrittenState(ctx, hash, state)
	return nil
}

// closeFileHandle syncs and closes an open file handle.
// Acquires fileMu to ensure in-flight writeAt calls (which hold fileMu.RLock)
// complete before the file descriptor is closed.
// Returns an error if sync or close fails (data may not be durable).
func (s *Server) closeFileHandle(ctx context.Context, hash string, fi *serverFileInfo) error {
	fi.fileMu.Lock()
	defer fi.fileMu.Unlock()

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

	if renameErr := os.Rename(fi.path, finalPath); renameErr != nil {
		// .partial is gone but final exists: already renamed (idempotent restart case).
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

// syncFileParentDirs fsyncs the parent directories of finalized files to ensure
// NFS has flushed file data and renames to the server before verification reads.
// Best-effort: sync failures are logged but do not block verification.
func (s *Server) syncFileParentDirs(ctx context.Context, hash string, state *serverTorrentState) {
	synced := make(map[string]bool)
	for _, fi := range state.files {
		if !fi.selected {
			continue
		}
		dir := filepath.Dir(fi.path)
		if synced[dir] {
			continue
		}
		synced[dir] = true

		dirFD, openErr := os.Open(dir)
		if openErr != nil {
			s.logger.DebugContext(ctx, "failed to open dir for sync",
				"hash", hash, "dir", dir, "error", openErr)
			continue
		}
		if syncErr := dirFD.Sync(); syncErr != nil {
			s.logger.DebugContext(ctx, "failed to sync dir",
				"hash", hash, "dir", dir, "error", syncErr)
		}
		_ = dirFD.Close()
	}
}

// flushWrittenState persists the written bitmap to disk.
func (s *Server) flushWrittenState(ctx context.Context, hash string, state *serverTorrentState) {
	if state.statePath == "" {
		return
	}

	if saveErr := s.saveState(state.statePath, state.written); saveErr != nil {
		s.logger.WarnContext(ctx, "failed to save final state",
			"hash", hash,
			"error", saveErr,
		)
		return
	}
	state.flushGen++
}

// registerFinalizedInodes registers inodes for files we wrote (not hardlinked)
// and signals any waiting torrents that the files are ready for hardlinking.
func (s *Server) registerFinalizedInodes(ctx context.Context, hash string, state *serverTorrentState) {
	state.mu.Lock()
	defer state.mu.Unlock()
	s.store.RegisterInodes(ctx, hash, state.files)
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
				s.logger.DebugContext(ctx, "verification progress",
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
		state.written.Clear(uint(p))
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
	} else {
		state.flushGen++
	}

	s.logger.InfoContext(ctx, "recovered from verification failure",
		"hash", hash,
		"failedPieces", len(failedPieces),
		"writtenCount", int(state.written.Count()),
	)
}

// abortInProgressInodes aborts in-progress inode entries for all files in the
// torrent so pending torrents waiting on this torrent's doneCh are unblocked
// instead of timing out.
func (s *Server) abortInProgressInodes(ctx context.Context, hash string, state *serverTorrentState) {
	for _, fi := range state.files {
		s.store.Inodes().AbortInProgress(ctx, fi.hardlink.sourceInode, hash)
	}
}

// recoverAffectedFile recovers a single file that overlaps failed pieces.
// For normal (streamed) files: renames back to .partial.
// For hardlinked/pre-existing files: deletes the file to break the hardlink
// and resets state so the file can be re-streamed from scratch.
func (s *Server) recoverAffectedFile(
	ctx context.Context,
	hash string,
	state *serverTorrentState,
	fi *serverFileInfo,
	failedPieces []int,
) {
	if !fi.selected {
		return
	}

	// Check if any failed piece overlaps this file.
	affected := slices.ContainsFunc(failedPieces, fi.overlaps)
	if !affected {
		return
	}

	// Hardlinked or pre-existing files with wrong content: break the hardlink
	// by deleting the file. Writing to a renamed hardlink would corrupt the
	// source file that other torrents still reference.
	if fi.hardlink.state == hlStateComplete || fi.hardlink.state == hlStatePending {
		s.logger.WarnContext(ctx, "breaking hardlink for file with failed pieces",
			"hash", hash, "path", fi.path, "hardlinkState", fi.hardlink.state)

		if removeErr := os.Remove(fi.path); removeErr != nil && !os.IsNotExist(removeErr) {
			s.logger.WarnContext(ctx, "failed to remove hardlinked file",
				"hash", hash, "path", fi.path, "error", removeErr)
		}

		fi.hardlink.state = hlStateNone
		finalPath := strings.TrimSuffix(fi.path, partialSuffix)
		fi.path = finalPath + partialSuffix
		fi.earlyFinalized = false
		fi.recalcPiecesWritten(state.written)
		return
	}

	// Normal (streamed) files: rename back to .partial (skip if already .partial).
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
		s.store.Remove(hash)
		return &pb.FinalizeTorrentResponse{Success: true, State: result.state}, nil
	}

	// Background finalization failed — wait for the goroutine to fully exit
	// before clearing state, preventing concurrent background goroutines.
	<-done
	state.mu.Lock()
	state.finalization.reset()
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
