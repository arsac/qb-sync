package destination

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/arsac/qb-sync/internal/metrics"
	pb "github.com/arsac/qb-sync/proto"
)

// tryRemoveWithLog attempts to remove a file, logging and collecting errors.
// Returns true on successful removal, false if the file was missing or removal
// failed (in which case the error is appended to deleteErrors).
func tryRemoveWithLog(
	ctx context.Context,
	logger *slog.Logger,
	path, fileType, hash string,
	deleteErrors *[]string,
) bool {
	err := os.Remove(path)
	if err == nil {
		logger.DebugContext(ctx, "deleted "+fileType,
			"hash", hash,
			"path", path,
		)
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	*deleteErrors = append(*deleteErrors, fmt.Sprintf("%s %s: %v", fileType, path, err))
	logger.WarnContext(ctx, "failed to delete "+fileType,
		"hash", hash,
		"path", path,
		"error", err,
	)
	return false
}

// shouldDeleteAbortedFile reports whether AbortTorrent should remove the given
// file. Returns false (with metric incremented) for files qb-sync didn't put on
// disk: deselected files (never written) and PreExisting files (operator data
// that setupFile reused at the right size).
func (s *Server) shouldDeleteAbortedFile(
	ctx context.Context,
	hash string,
	fi *serverFileInfo,
	result *pb.HardlinkResult,
) bool {
	if !fi.selected {
		metrics.AbortFileDeletionsSkippedTotal.WithLabelValues(metrics.ReasonAbortUnselected).Inc()
		return false
	}
	if result.GetPreExisting() {
		s.logger.InfoContext(ctx, "preserving pre-existing file on abort",
			"hash", hash, "path", fi.path,
		)
		metrics.AbortFileDeletionsSkippedTotal.WithLabelValues(metrics.ReasonAbortPreExisting).Inc()
		return false
	}
	return true
}

// AbortTorrent aborts an in-progress torrent transfer and optionally cleans up partial files.
// This is called when a torrent is removed from source before streaming completes.
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

	// Safety guard: if destination qB already has the torrent, deleting its
	// files would leave qB seeing missing files. This catches the narrow
	// finalization-completion race (source's removal detection fires after
	// destination's FinalizeTorrent succeeded but before source's completion
	// cache reflects it) and any other path where source's view drifts from
	// destination's qB state. Disable file deletion; the destination becomes
	// the canonical seeder. In-memory state is still dropped via BeginAbort
	// below so this doesn't accumulate.
	if deleteFiles && s.qbClient != nil && s.isTorrentInQB(ctx, hash) {
		s.logger.WarnContext(ctx, "torrent exists in destination qBittorrent, preserving files on abort",
			"hash", hash,
		)
		metrics.AbortFileDeletionsSkippedTotal.WithLabelValues(metrics.ReasonAbortInQB).Inc()
		deleteFiles = false
	}

	// Register this abort to prevent concurrent InitTorrent from racing with cleanup.
	// Create a channel that InitTorrent can wait on.
	abortCh := make(chan struct{})
	defer func() {
		close(abortCh) // Signal waiting InitTorrent calls before deregistering
		s.store.EndCleanup(hash)
	}()

	state, existingCh := s.store.BeginAbort(hash, abortCh)
	if existingCh != nil {
		// Wait for existing abort to complete, then return success
		<-existingCh
		return &pb.AbortTorrentResponse{
			Success:      true,
			FilesDeleted: 0,
		}, nil
	}

	if state == nil {
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

	for i, fi := range state.files {
		// closeFileHandle is idempotent (no-op if fi.file is nil).
		_ = s.closeFileHandle(ctx, hash, fi)

		if !deleteFiles {
			continue
		}
		var hardlinkResult *pb.HardlinkResult
		if i < len(state.hardlinkResults) {
			hardlinkResult = state.hardlinkResults[i]
		}
		if !s.shouldDeleteAbortedFile(ctx, hash, fi, hardlinkResult) {
			continue
		}
		if tryRemoveWithLog(ctx, s.logger, fi.path, "partial file", hash, &deleteErrors) {
			filesDeleted++
		}
	}

	if deleteFiles {
		if state.statePath != "" {
			tryRemoveWithLog(ctx, s.logger, state.statePath, "state file", hash, &deleteErrors)
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
