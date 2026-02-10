package cold

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
// If the hot side calls FinalizeTorrent while verification is already running,
// the response indicates "in progress" without error so the hot side can poll.
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

		// If we have a cached result from a completed background finalization, return it.
		if result != nil {
			if result.success {
				// Clean up now that hot has received the success response.
				s.cleanupFinalizedTorrent(hash)
				return &pb.FinalizeTorrentResponse{Success: true, State: result.state}, nil
			}
			// Background finalization failed — wait for the goroutine to fully exit
			// before clearing state, preventing concurrent background goroutines.
			if done != nil {
				<-done
			}
			state.mu.Lock()
			state.finalizing = false
			state.finalizeResult = nil
			state.finalizeDone = nil
			state.mu.Unlock()
			return &pb.FinalizeTorrentResponse{Success: false, Error: result.err}, nil
		}

		// Background work is still running — tell hot it's in progress
		// so it polls again without counting this as a failure.
		return &pb.FinalizeTorrentResponse{
			Success: true,
			State:   "verifying",
		}, nil
	}
	// Create finalizeDone immediately so concurrent polls always see it.
	done := make(chan struct{})
	state.finalizing = true
	state.finalizeDone = done
	writtenCount := state.writtenCount
	totalPieces := len(state.written)
	state.mu.Unlock()

	// Helper to clear finalizing state, close the done channel, record failure
	// metrics, and return error response. Used on all early exit paths before
	// the background goroutine is launched.
	failureResponse := func(errMsg string) *pb.FinalizeTorrentResponse {
		close(done)
		state.mu.Lock()
		state.finalizing = false
		state.finalizeResult = nil
		state.finalizeDone = nil
		state.mu.Unlock()
		metrics.FinalizationDuration.WithLabelValues(metrics.ResultFailure).Observe(time.Since(startTime).Seconds())
		metrics.FinalizationErrorsTotal.WithLabelValues(metrics.ModeCold).Inc()
		return &pb.FinalizeTorrentResponse{Success: false, Error: errMsg}
	}

	// Relocate files if save_sub_path changed (e.g., hot moved torrent to different category).
	// Only relocate when request carries a non-empty sub-path to avoid accidental relocation
	// from an old hot version that doesn't send this field.
	if newSubPath := req.GetSaveSubPath(); newSubPath != "" && newSubPath != state.saveSubPath {
		oldSubPath := state.saveSubPath
		oldBase := filepath.Join(s.config.BasePath, oldSubPath)

		relPaths := make([]string, len(state.files))
		for i, fi := range state.files {
			rel, relErr := filepath.Rel(oldBase, strings.TrimSuffix(fi.path, partialSuffix))
			if relErr != nil {
				return failureResponse(fmt.Sprintf("computing relative path: %v", relErr)), nil
			}
			relPaths[i] = rel
		}

		if _, relocErr := s.relocateFiles(ctx, hash, relPaths, oldSubPath, newSubPath); relocErr != nil {
			return failureResponse(fmt.Sprintf("relocating files: %v", relocErr)), nil
		}

		updateStateAfterRelocate(state, s.config.BasePath, oldSubPath, newSubPath)
		metaDir := filepath.Join(s.config.BasePath, metaDirName, hash)
		if subPathErr := saveSubPathFile(metaDir, newSubPath); subPathErr != nil {
			return failureResponse(fmt.Sprintf("persisting sub-path after relocation: %v", subPathErr)), nil
		}
	}

	// Verify all pieces are written.
	if writtenCount < totalPieces {
		return failureResponse(fmt.Sprintf("incomplete: %d/%d pieces", writtenCount, totalPieces)), nil
	}

	// Finalize files (sync, close, rename .partial -> final).
	// This is idempotent — already renamed files are detected and skipped.
	if finalizeErr := s.finalizeFiles(ctx, hash, state); finalizeErr != nil {
		return failureResponse(fmt.Sprintf("finalizing files: %v", finalizeErr)), nil
	}

	// Launch verification and post-verification steps in the background.
	// This decouples from the RPC context so verification survives hot-side
	// deadline cancellation. The finalizing flag and finalizeDone channel were
	// set upfront (under lock) so concurrent polls see "verifying" immediately.
	go s.runBackgroundFinalization(hash, state, req, startTime, done)

	// Return "verifying" to the hot side. Hot should poll via subsequent
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

	// Use a background context with an upper-bound timeout — this work must not be
	// cancelled by the RPC deadline, but we cap the total wall-clock time to prevent
	// indefinite background work (e.g., qBittorrent operations hanging).
	ctx, cancel := context.WithTimeout(context.Background(), backgroundFinalizeTimeout)
	defer cancel()

	// storeFailure records failure metrics and stores the error for the next poll.
	storeFailure := func(errMsg string) {
		metrics.FinalizationDuration.WithLabelValues(metrics.ResultFailure).Observe(time.Since(startTime).Seconds())
		metrics.FinalizationErrorsTotal.WithLabelValues(metrics.ModeCold).Inc()
		state.mu.Lock()
		state.finalizeResult = &finalizeResult{err: errMsg}
		state.mu.Unlock()
	}

	// Verify all pieces by reading back from finalized files.
	if verifyErr := s.verifyFinalizedPieces(ctx, hash, state); verifyErr != nil {
		s.logger.ErrorContext(ctx, "background verification failed",
			"hash", hash,
			"error", verifyErr,
		)
		storeFailure(fmt.Sprintf("verification failed: %v", verifyErr))
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
			storeFailure(fmt.Sprintf("qBittorrent: %v", qbErr))
			return
		}

		// Apply synced tag for visibility (not used as source of truth).
		if s.config.SyncedTag != "" {
			if tagErr := s.qbClient.AddTagsCtx(ctx, []string{hash}, s.config.SyncedTag); tagErr != nil {
				metrics.TagApplicationErrorsTotal.WithLabelValues(metrics.ModeCold).Inc()
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
	metrics.TorrentsSyncedTotal.WithLabelValues(metrics.ModeCold, hash, name).Inc()
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
		metrics.FileSyncErrorsTotal.WithLabelValues(metrics.ModeCold).Inc()
		s.logger.WarnContext(ctx, "failed to sync file",
			"hash", hash,
			"path", fi.path,
			"error", syncErr,
		)
	}

	closeErr := fi.file.Close()
	fi.file = nil
	if closeErr != nil {
		metrics.FileSyncErrorsTotal.WithLabelValues(metrics.ModeCold).Inc()
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
		// Skip files that were hardlinked (either from registration or pending) or have no inode
		if f.hlState == hlStateComplete || f.sourceInode == 0 {
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
func (s *Server) verifyFinalizedPieces(ctx context.Context, hash string, state *serverTorrentState) error {
	if len(state.pieceHashes) == 0 {
		return fmt.Errorf("no piece hashes available for verification — refusing to finalize without integrity check")
	}
	if state.pieceLength <= 0 || state.totalSize <= 0 {
		return fmt.Errorf("missing piece size or total size metadata — refusing to finalize without integrity check")
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

	// Idle watchdog: cancel verification if no progress within verifyIdleTimeout.
	go func() {
		ticker := time.NewTicker(verifyIdleTimeout / 2) // Check at half the timeout interval.
		defer ticker.Stop()
		for {
			select {
			case <-gCtx.Done():
				return
			case <-ticker.C:
				last := lastProgress.Load().(time.Time)
				if time.Since(last) > verifyIdleTimeout {
					s.logger.ErrorContext(ctx, "verification stalled, aborting",
						"hash", hash,
						"verified", verified.Load(),
						"total", numPieces,
						"idleTimeout", verifyIdleTimeout,
					)
					idleCancel()
					return
				}
			}
		}
	}()

	for i, expectedHash := range state.pieceHashes {
		if expectedHash == "" {
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

			// Read piece from finalized files
			data, readErr := s.readPieceFromFinalizedFiles(state, offset, size)
			if readErr != nil {
				metrics.VerificationErrorsTotal.WithLabelValues(metrics.ModeCold).Inc()
				return fmt.Errorf("reading piece %d: %w", i, readErr)
			}

			// Verify hash
			if err := utils.VerifyPieceHash(data, expectedHash); err != nil {
				metrics.VerificationErrorsTotal.WithLabelValues(metrics.ModeCold).Inc()
				return fmt.Errorf("piece %d: %w", i, err)
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
		return err
	}

	s.logger.InfoContext(ctx, "all pieces verified successfully", "hash", hash, "pieces", numPieces)
	return nil
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
