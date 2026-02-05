package cold

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/arsac/qb-sync/internal/metrics"
	"github.com/arsac/qb-sync/internal/utils"
	pb "github.com/arsac/qb-sync/proto"
)

// FinalizeTorrent completes the torrent transfer by renaming partial files,
// adding to qBittorrent, and verifying via recheck.
// This operation is idempotent: file renaming checks for existing files,
// and qB operations handle already-added torrents.
//
//nolint:funlen // Complex orchestration logic is clearer as single function
func (s *Server) FinalizeTorrent(
	ctx context.Context,
	req *pb.FinalizeTorrentRequest,
) (*pb.FinalizeTorrentResponse, error) {
	startTime := time.Now()
	hash := req.GetTorrentHash()

	s.mu.RLock()
	state, exists := s.torrents[hash]
	s.mu.RUnlock()

	if !exists {
		// Try to recover state from disk (after server restart)
		recoveredState, recoverErr := s.recoverTorrentState(ctx, hash)
		if recoverErr != nil {
			s.logger.DebugContext(ctx, "failed to recover torrent state",
				"hash", hash,
				"error", recoverErr,
			)
			return &pb.FinalizeTorrentResponse{
				Success: false,
				Error:   "torrent not found",
			}, nil
		}

		// Register recovered state
		s.mu.Lock()
		s.torrents[hash] = recoveredState
		s.mu.Unlock()

		state = recoveredState
		s.logger.InfoContext(ctx, "recovered torrent state from disk",
			"hash", hash,
			"pieces", len(state.written),
			"files", len(state.files),
		)
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
	writtenCount := countWritten(state.written)
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
		metrics.FinalizationDuration.WithLabelValues("failure").Observe(time.Since(startTime).Seconds())
		metrics.FinalizationErrorsTotal.WithLabelValues("cold").Inc()
		return &pb.FinalizeTorrentResponse{Success: false, Error: errMsg}
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
		metrics.FinalizationDuration.WithLabelValues("success").Observe(time.Since(startTime).Seconds())
		metrics.TorrentsSyncedTotal.WithLabelValues("cold").Inc()
		s.logger.InfoContext(ctx, "torrent finalized", "hash", hash, "state", stateStr)
		s.cleanupFinalizedTorrent(hash)
		return &pb.FinalizeTorrentResponse{Success: true, State: stateStr}
	}

	// Add to qBittorrent if configured
	if s.qbClient != nil {
		finalState, qbErr := s.addAndVerifyTorrent(ctx, hash, state, req)
		if qbErr != nil {
			return failureResponse(fmt.Sprintf("qBittorrent: %v", qbErr)), nil
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
}

// finalizeFiles syncs all file handles, closes them, and renames from .partial to final.
// Also resolves pending hardlinks by waiting for source files to complete.
func (s *Server) finalizeFiles(ctx context.Context, hash string, state *serverTorrentState) error {
	state.mu.Lock()
	defer state.mu.Unlock()

	// First, resolve pending hardlinks
	for _, fi := range state.files {
		if !fi.pendingHardlink {
			continue
		}

		// Wait for source file to be finalized with timeout
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

		// Create hardlink (hardlinkSource is relative, join with BasePath)
		sourcePath := filepath.Join(s.config.BasePath, fi.hardlinkSource)
		if linkErr := os.Link(sourcePath, fi.path); linkErr != nil {
			// Check if target already exists (idempotent)
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
			s.logger.InfoContext(ctx, "created pending hardlink",
				"hash", hash,
				"source", sourcePath,
				"target", fi.path,
			)
		}

		fi.hardlinked = true
		fi.pendingHardlink = false
	}

	// Close all file handles first to ensure no leaks even if rename fails
	for _, fi := range state.files {
		if !fi.hardlinked {
			s.closeFileHandle(ctx, hash, fi)
		}
	}

	// Then rename partial files
	for _, fi := range state.files {
		if fi.hardlinked {
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
func (s *Server) closeFileHandle(ctx context.Context, hash string, fi *serverFileInfo) {
	if fi.file == nil {
		return
	}

	if syncErr := fi.file.Sync(); syncErr != nil {
		s.logger.WarnContext(ctx, "failed to sync file",
			"hash", hash,
			"path", fi.path,
			"error", syncErr,
		)
	}

	if closeErr := fi.file.Close(); closeErr != nil {
		s.logger.WarnContext(ctx, "failed to close file",
			"hash", hash,
			"path", fi.path,
			"error", closeErr,
		)
	}
	fi.file = nil
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
		// Skip files that were hardlinked (either from registration or pending)
		if f.hardlinked || f.sourceInode == 0 {
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

		// Register in persistent map
		s.inodeMu.Lock()
		s.inodeToPath[f.sourceInode] = relPath
		s.inodeMu.Unlock()
		registered++

		// Signal waiters and clean up in-progress tracking
		s.inodeProgressMu.Lock()
		if inProgress, ok := s.inodeInProgress[f.sourceInode]; ok {
			inProgress.close() // Signal waiters (safe for double-close)
			delete(s.inodeInProgress, f.sourceInode)
		}
		s.inodeProgressMu.Unlock()
	}

	if registered > 0 {
		s.logger.InfoContext(ctx, "registered finalized inodes",
			"hash", hash,
			"count", registered,
		)

		// Persist inode map
		if saveErr := s.saveInodeMap(); saveErr != nil {
			s.logger.WarnContext(ctx, "failed to save inode map", "error", saveErr)
		}
	}
}

// verifyFinalizedPieces reads back all pieces from finalized files and verifies their hashes.
// This ensures data integrity before telling hot it's safe to delete.
func (s *Server) verifyFinalizedPieces(ctx context.Context, hash string, state *serverTorrentState) error {
	if len(state.pieceHashes) == 0 {
		s.logger.WarnContext(ctx, "no piece hashes available for verification", "hash", hash)
		return nil // Can't verify without hashes
	}

	pieceSize := state.info.GetPieceSize()
	totalSize := state.info.GetTotalSize()
	numPieces := len(state.pieceHashes)

	s.logger.InfoContext(ctx, "verifying finalized pieces",
		"hash", hash,
		"pieces", numPieces,
		"pieceSize", pieceSize,
	)

	for i, expectedHash := range state.pieceHashes {
		if expectedHash == "" {
			continue
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
			return fmt.Errorf("reading piece %d: %w", i, readErr)
		}

		// Verify hash
		if err := utils.VerifyPieceHash(data, expectedHash); err != nil {
			return fmt.Errorf("piece %d: %w", i, err)
		}
	}

	s.logger.InfoContext(ctx, "all pieces verified successfully", "hash", hash, "pieces", numPieces)
	return nil
}

// readPieceFromFinalizedFiles reads piece data from finalized (non-.partial) files.
func (s *Server) readPieceFromFinalizedFiles(state *serverTorrentState, offset, size int64) ([]byte, error) {
	data := make([]byte, 0, size)
	remaining := size
	currentOffset := offset

	for _, fi := range state.files {
		if remaining <= 0 {
			break
		}

		fileEnd := fi.offset + fi.size
		if fileEnd <= currentOffset {
			continue
		}

		// Calculate read position within this file
		fileReadOffset := max(currentOffset-fi.offset, 0)
		availableInFile := fi.size - fileReadOffset
		toRead := min(remaining, availableInFile)

		// Get final path (remove .partial suffix if present)
		filePath, _ := strings.CutSuffix(fi.path, partialSuffix)

		// Read from file
		chunk, readErr := s.readChunkFromFile(filePath, fileReadOffset, toRead)
		if readErr != nil {
			return nil, fmt.Errorf("reading from %s at offset %d: %w", filePath, fileReadOffset, readErr)
		}

		data = append(data, chunk...)
		remaining -= int64(len(chunk))
		currentOffset += int64(len(chunk))
	}

	return data, nil
}

// readChunkFromFile reads a chunk of data from a file at a specific offset.
func (s *Server) readChunkFromFile(path string, offset, size int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data := make([]byte, size)
	n, err := file.ReadAt(data, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	return data[:n], nil
}
