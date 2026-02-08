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
// adding to qBittorrent, and verifying via recheck.
// This operation is idempotent: file renaming checks for existing files,
// and qB operations handle already-added torrents.
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

	// Set finalizing flag to prevent concurrent writes
	state.mu.Lock()
	if state.finalizing {
		state.mu.Unlock()
		return &pb.FinalizeTorrentResponse{
			Success: false,
			Error:   "finalization already in progress",
		}, nil
	}
	state.finalizing = true
	writtenCount := state.writtenCount
	totalPieces := len(state.written)
	state.mu.Unlock()

	// Helper to clear finalizing flag - must be called on all exit paths except success
	clearFinalizing := func() {
		state.mu.Lock()
		state.finalizing = false
		state.mu.Unlock()
	}

	// Helper to record failure metrics and return error response
	failureResponse := func(errMsg string) *pb.FinalizeTorrentResponse {
		clearFinalizing()
		metrics.FinalizationDuration.WithLabelValues(metrics.ResultFailure).Observe(time.Since(startTime).Seconds())
		metrics.FinalizationErrorsTotal.WithLabelValues(metrics.ModeCold, hash, "").Inc()
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
				clearFinalizing()
				return &pb.FinalizeTorrentResponse{
					Success: false,
					Error:   fmt.Sprintf("computing relative path: %v", relErr),
				}, nil
			}
			relPaths[i] = rel
		}

		if _, relocErr := s.relocateFiles(ctx, hash, relPaths, oldSubPath, newSubPath); relocErr != nil {
			clearFinalizing()
			return &pb.FinalizeTorrentResponse{
				Success: false,
				Error:   fmt.Sprintf("relocating files: %v", relocErr),
			}, nil
		}

		updateStateAfterRelocate(state, s.config.BasePath, oldSubPath, newSubPath)
		s.updatePersistedInfoAfterRelocate(hash, s.config.BasePath, oldSubPath, newSubPath)
	}

	// Verify all pieces are written
	if writtenCount < totalPieces {
		return failureResponse(fmt.Sprintf("incomplete: %d/%d pieces", writtenCount, totalPieces)), nil
	}

	// Finalize files (sync, close, rename .partial -> final)
	// This is idempotent - already renamed files are detected and skipped
	if finalizeErr := s.finalizeFiles(ctx, hash, state); finalizeErr != nil {
		return failureResponse(fmt.Sprintf("finalizing files: %v", finalizeErr)), nil
	}

	// Verify all pieces by reading back from finalized files
	// This ensures data integrity before we tell hot it's safe to delete
	if verifyErr := s.verifyFinalizedPieces(ctx, hash, state); verifyErr != nil {
		return failureResponse(fmt.Sprintf("verification failed: %v", verifyErr)), nil
	}

	// Register inodes for files we wrote (not hardlinked) and signal waiters
	s.registerFinalizedInodes(ctx, hash, state)

	// Helper to record success metrics and clean up
	successResponse := func(stateStr string) *pb.FinalizeTorrentResponse {
		metrics.FinalizationDuration.WithLabelValues(metrics.ResultSuccess).Observe(time.Since(startTime).Seconds())
		metrics.TorrentsSyncedTotal.WithLabelValues(metrics.ModeCold, hash, "").Inc()
		s.logger.InfoContext(ctx, "torrent finalized", "hash", hash, "state", stateStr)
		s.cleanupFinalizedTorrent(hash)
		return &pb.FinalizeTorrentResponse{Success: true, State: stateStr}
	}

	// Add to qBittorrent if configured and not in dry-run mode
	if s.qbClient != nil && !s.config.DryRun {
		finalState, qbErr := s.addAndVerifyTorrent(ctx, hash, state, req)
		if qbErr != nil {
			return failureResponse(fmt.Sprintf("qBittorrent: %v", qbErr)), nil
		}

		// Apply synced tag for visibility (not used as source of truth)
		if s.config.SyncedTag != "" && !s.config.DryRun {
			if tagErr := s.qbClient.AddTagsCtx(ctx, []string{hash}, s.config.SyncedTag); tagErr != nil {
				metrics.TagApplicationErrorsTotal.WithLabelValues(metrics.ModeCold).Inc()
				s.logger.ErrorContext(ctx, "failed to add synced tag",
					"hash", hash,
					"tag", s.config.SyncedTag,
					"error", tagErr,
				)
			}
		}

		return successResponse(string(finalState)), nil
	}

	return successResponse("finalized"), nil
}

// cleanupFinalizedTorrent removes a successfully finalized torrent from tracking.
// This prevents memory leaks from accumulating completed torrent state.
func (s *Server) cleanupFinalizedTorrent(hash string) {
	s.mu.Lock()
	delete(s.torrents, hash)
	s.mu.Unlock()
	metrics.ActiveTorrents.WithLabelValues(metrics.ModeCold).Dec()
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
// This ensures data integrity before telling hot it's safe to delete.
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

	// Verify pieces in parallel — all accessed state fields (pieceHashes,
	// pieceLength, totalSize, files) are immutable at this point (finalizing=true).
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(maxVerifyConcurrency)

	var verified atomic.Int64

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
