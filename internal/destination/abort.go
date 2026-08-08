package destination

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"golang.org/x/sync/errgroup"

	"github.com/arsac/qb-sync/internal/metrics"
	pb "github.com/arsac/qb-sync/proto"
)

// tryRemoveWithLog attempts to remove a file, logging failures. Returns whether
// the file was removed and, when removal failed for a reason other than the file
// already being gone, a message describing it. Reporting the failure rather than
// accumulating it lets concurrent callers merge results in file order.
func tryRemoveWithLog(
	ctx context.Context,
	logger *slog.Logger,
	path, fileType, hash string,
) (bool, string) {
	err := os.Remove(path)
	if err == nil {
		logger.DebugContext(ctx, "deleted "+fileType,
			"hash", hash,
			"path", path,
		)
		return true, ""
	}
	if os.IsNotExist(err) {
		return false, ""
	}
	logger.WarnContext(ctx, "failed to delete "+fileType,
		"hash", hash,
		"path", path,
		"error", err,
	)
	return false, fmt.Sprintf("%s %s: %v", fileType, path, err)
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

// deletionAllowedOnAbort reports whether an abort may still delete the torrent's
// files.
//
// If destination qB already has the torrent, deleting its files would leave qB
// seeing missing files. This catches the narrow finalization-completion race
// (source's removal detection fires after destination's FinalizeTorrent
// succeeded but before source's completion cache reflects it) and any other path
// where source's view drifts from destination's qB state. The destination becomes
// the canonical seeder; in-memory state is still dropped via BeginAbort so this
// doesn't accumulate.
func (s *Server) deletionAllowedOnAbort(ctx context.Context, hash string) bool {
	if s.qbClient == nil || !s.isTorrentInQB(ctx, hash) {
		return true
	}
	s.logger.WarnContext(ctx, "torrent exists in destination qBittorrent, preserving files on abort",
		"hash", hash,
	)
	metrics.AbortFileDeletionsSkippedTotal.WithLabelValues(metrics.ReasonAbortInQB).Inc()
	return false
}

// removeTorrentMetadata deletes the bookkeeping qb-sync keeps for a torrent - its
// written-bitmap state file and its .meta directory - and returns one message per
// failure.
func (s *Server) removeTorrentMetadata(ctx context.Context, hash, statePath string) []string {
	var failures []string

	if statePath != "" {
		if _, failure := tryRemoveWithLog(ctx, s.logger, statePath, "state file", hash); failure != "" {
			failures = append(failures, failure)
		}
	}

	metaDir := filepath.Join(s.config.BasePath, metaDirName, hash)
	if err := os.RemoveAll(metaDir); err != nil && !os.IsNotExist(err) {
		failures = append(failures, fmt.Sprintf("meta directory: %v", err))
		s.logger.WarnContext(ctx, "failed to delete meta directory",
			"hash", hash,
			"path", metaDir,
			"error", err,
		)
	}

	return failures
}

// abortFileCleanup releases every handle the torrent still holds open and, when
// deleteFiles is set, removes the partial files qb-sync itself put on disk.
// Returns how many files were deleted plus one message per failed deletion, in
// file order regardless of which task finished first.
//
// Each file costs independent NFS round-trips - an fsync and a close on any
// still-open handle, plus an unlink when the file is being removed - and no
// file's outcome depends on another's, so the pass fans out at
// fileSetupConcurrency. AbortTorrent runs synchronously inside the RPC handler
// under the source's 30s destRPCTimeout, which a serial loop over a many-file
// torrent cannot meet.
//
// Caller must hold state.mu. Each task touches only its own file's fileMu, so
// the established state.mu -> fileMu order is preserved.
func (s *Server) abortFileCleanup(
	ctx context.Context,
	hash string,
	state *serverTorrentState,
	deleteFiles bool,
) (int32, []string) {
	deleted := make([]bool, len(state.files))
	failures := make([]string, len(state.files))

	g := new(errgroup.Group)
	g.SetLimit(fileSetupConcurrency)
	for i, fi := range state.files {
		g.Go(func() error {
			// closeFileHandle is idempotent (no-op if fi.file is nil).
			_ = s.closeFileHandle(ctx, hash, fi)

			if !deleteFiles {
				return nil
			}
			var hardlinkResult *pb.HardlinkResult
			if i < len(state.hardlinkResults) {
				hardlinkResult = state.hardlinkResults[i]
			}
			if !s.shouldDeleteAbortedFile(ctx, hash, fi, hardlinkResult) {
				return nil
			}
			deleted[i], failures[i] = tryRemoveWithLog(ctx, s.logger, fi.path, "partial file", hash)
			return nil
		})
	}
	_ = g.Wait()

	var (
		filesDeleted int32
		deleteErrors []string
	)
	for i := range state.files {
		if deleted[i] {
			filesDeleted++
		}
		if failures[i] != "" {
			deleteErrors = append(deleteErrors, failures[i])
		}
	}
	return filesDeleted, deleteErrors
}

// abortResponse maps a cleanup outcome onto the RPC response: any failed
// deletion is reported as partial success, with the count of files that were
// removed either way.
func abortResponse(filesDeleted int32, deleteErrors []string) *pb.AbortTorrentResponse {
	if len(deleteErrors) > 0 {
		return &pb.AbortTorrentResponse{
			Success:      false,
			Error:        fmt.Sprintf("partial cleanup: %d errors", len(deleteErrors)),
			FilesDeleted: filesDeleted,
		}
	}
	return &pb.AbortTorrentResponse{
		Success:      true,
		FilesDeleted: filesDeleted,
	}
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

	deleteFiles = deleteFiles && s.deletionAllowedOnAbort(ctx, hash)

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
		return abortResponse(0, nil), nil
	}

	if state == nil {
		s.logger.InfoContext(ctx, "torrent not found for abort (may already be cleaned up)",
			"hash", hash,
		)
		return abortResponse(0, nil), nil
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	filesDeleted, deleteErrors := s.abortFileCleanup(ctx, hash, state, deleteFiles)

	if deleteFiles {
		deleteErrors = append(deleteErrors, s.removeTorrentMetadata(ctx, hash, state.statePath)...)
	}

	s.logger.InfoContext(ctx, "torrent aborted",
		"hash", hash,
		"filesDeleted", filesDeleted,
		"deleteErrors", len(deleteErrors),
	)

	return abortResponse(filesDeleted, deleteErrors), nil
}
