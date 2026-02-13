package cold

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	pb "github.com/arsac/qb-sync/proto"
)

// tryRemoveWithLog attempts to remove a file, logging and collecting errors.
func tryRemoveWithLog(
	ctx context.Context,
	logger *slog.Logger,
	path, fileType, hash string,
	deleteErrors *[]string,
) bool {
	if err := os.Remove(path); err == nil {
		logger.DebugContext(ctx, "deleted "+fileType,
			"hash", hash,
			"path", path,
		)
		return true
	} else if !os.IsNotExist(err) {
		*deleteErrors = append(*deleteErrors, fmt.Sprintf("%s %s: %v", fileType, path, err))
		logger.WarnContext(ctx, "failed to delete "+fileType,
			"hash", hash,
			"path", path,
			"error", err,
		)
	}
	return false
}

// AbortTorrent aborts an in-progress torrent transfer and optionally cleans up partial files.
// This is called when a torrent is removed from hot before streaming completes.
//
//nolint:funlen // Complex cleanup with proper lock ordering is clearer as single function
func (s *Server) AbortTorrent(
	ctx context.Context,
	req *pb.AbortTorrentRequest,
) (*pb.AbortTorrentResponse, error) {
	hash := req.GetTorrentHash()
	deleteFiles := req.GetDeleteFiles()

	s.logger.InfoContext(ctx, "aborting torrent",
		"hash", hash,
		"deleteFiles", deleteFiles,
	)

	// Register this abort to prevent concurrent InitTorrent from racing with cleanup.
	// Create a channel that InitTorrent can wait on.
	abortCh := make(chan struct{})

	// cleanupAbort ensures the abort tracking is cleaned up even on panic.
	// Must be called after registering the abort but before any code that might panic.
	cleanupAbort := func() {
		s.mu.Lock()
		delete(s.abortingHashes, hash)
		s.mu.Unlock()
		close(abortCh) // Signal waiting InitTorrent calls
	}

	s.mu.Lock()
	// Check if already aborting (shouldn't happen but be safe)
	if existingCh, alreadyAborting := s.abortingHashes[hash]; alreadyAborting {
		s.mu.Unlock()
		// Wait for existing abort to complete, then return success
		<-existingCh
		return &pb.AbortTorrentResponse{
			Success:      true,
			FilesDeleted: 0,
		}, nil
	}
	s.abortingHashes[hash] = abortCh

	state, exists := s.torrents[hash]
	if exists {
		delete(s.torrents, hash)
	}
	s.mu.Unlock()

	// Register cleanup IMMEDIATELY after releasing lock to minimize panic window.
	// This ensures the abort tracking is cleaned up even if subsequent code panics.
	defer cleanupAbort()

	if !exists {
		s.logger.InfoContext(ctx, "torrent not found for abort (may already be cleaned up)",
			"hash", hash,
		)
		return &pb.AbortTorrentResponse{
			Success:      true,
			FilesDeleted: 0,
		}, nil
	}

	filesDeleted := int32(0)
	var deleteErrors []string

	state.mu.Lock()
	defer state.mu.Unlock()

	// Clean up in-progress inode entries for this torrent's files.
	// Signal waiters so they can handle the abort (their hardlink attempt will fail).
	// AbortInProgress is idempotent: no-ops for zero inodes, completed, or pending files.
	for _, fi := range state.files {
		s.inodes.AbortInProgress(ctx, fi.sourceInode, hash)
	}

	for _, fi := range state.files {
		// closeFileHandle is idempotent (no-op if fi.file is nil).
		_ = s.closeFileHandle(ctx, hash, fi)

		if deleteFiles {
			if tryRemoveWithLog(ctx, s.logger, fi.path, "partial file", hash, &deleteErrors) {
				filesDeleted++
			}
		}
	}

	if deleteFiles {
		if state.statePath != "" {
			tryRemoveWithLog(ctx, s.logger, state.statePath, "state file", hash, &deleteErrors)
		}
		if state.torrentPath != "" {
			tryRemoveWithLog(ctx, s.logger, state.torrentPath, "torrent file", hash, &deleteErrors)
		}
		metaDir := filepath.Join(s.config.BasePath, metaDirName, hash)
		if err := os.RemoveAll(metaDir); err != nil && !os.IsNotExist(err) {
			deleteErrors = append(deleteErrors, fmt.Sprintf("meta directory: %v", err))
			s.logger.WarnContext(ctx, "failed to delete meta directory",
				"hash", hash,
				"path", metaDir,
				"error", err,
			)
		}
	}

	s.logger.InfoContext(ctx, "torrent aborted",
		"hash", hash,
		"filesDeleted", filesDeleted,
		"deleteErrors", len(deleteErrors),
	)

	// Report partial success if some deletions failed
	if len(deleteErrors) > 0 {
		return &pb.AbortTorrentResponse{
			Success:      false,
			Error:        fmt.Sprintf("partial cleanup: %d errors", len(deleteErrors)),
			FilesDeleted: filesDeleted,
		}, nil
	}

	return &pb.AbortTorrentResponse{
		Success:      true,
		FilesDeleted: filesDeleted,
	}, nil
}
