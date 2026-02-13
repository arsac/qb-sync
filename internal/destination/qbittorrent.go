package destination

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/autobrr/go-qbittorrent"

	"github.com/arsac/qb-sync/internal/metrics"
	"github.com/arsac/qb-sync/internal/utils"
	pb "github.com/arsac/qb-sync/proto"
)

// getQBTorrent logs in to qBittorrent and fetches a torrent by hash.
// Returns (torrent, found, error). If the torrent does not exist, found is false with nil error.
func (s *Server) getQBTorrent(ctx context.Context, hash string) (*qbittorrent.Torrent, bool, error) {
	if loginErr := s.qbClient.LoginCtx(ctx); loginErr != nil {
		return nil, false, fmt.Errorf("login failed: %w", loginErr)
	}

	torrents, getErr := s.qbClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{hash},
	})
	if getErr != nil {
		return nil, false, fmt.Errorf("fetching torrent: %w", getErr)
	}

	if len(torrents) == 0 {
		return nil, false, nil
	}

	return &torrents[0], true, nil
}

// addAndVerifyTorrent adds the torrent to qBittorrent and waits for verification.
func (s *Server) addAndVerifyTorrent(
	ctx context.Context,
	hash string,
	state *serverTorrentState,
	req *pb.FinalizeTorrentRequest,
) (qbittorrent.TorrentState, error) {
	existingTorrent, found, getErr := s.getQBTorrent(ctx, hash)
	if getErr != nil {
		return "", fmt.Errorf("checking existing: %w", getErr)
	}

	if found {
		if isErrorState(existingTorrent.State) {
			return existingTorrent.State, fmt.Errorf("torrent in error state: %s", existingTorrent.State)
		}
		if existingTorrent.Progress >= 1.0 && isReadyState(existingTorrent.State) {
			return existingTorrent.State, nil
		}
		// Not yet ready (checking, incomplete, moving, etc.) — poll until ready.
		return s.waitForTorrentReady(ctx, hash)
	}

	// Torrent doesn't exist - add it.
	// Read torrentPath under state.mu: ensureTorrentFileWritten can write it concurrently.
	state.mu.Lock()
	torrentPath := state.torrentPath
	state.mu.Unlock()
	torrentData, readErr := os.ReadFile(torrentPath)
	if readErr != nil {
		return "", fmt.Errorf("reading torrent file: %w", readErr)
	}

	// Use the destination-side save path (container mount point, e.g., "/downloads"),
	// joined with the sub-path from init (e.g., "movies" from category).
	savePath := filepath.Join(s.config.GetSavePath(), state.saveSubPath)

	deselectedIDs := deselectedFileIDs(state.files)

	opts := map[string]string{
		"savepath":           savePath,
		"stopped":            "true", // Add stopped so source controls when destination starts seeding (qB v5+)
		"paused":             "true", // Compat alias for qB v4.x
		"autoTMM":            "false",
		"sequentialDownload": "false",
	}

	// skip_checking is safe only when all files are present on disk. With
	// partial file selection, deselected files are absent, and qBittorrent
	// reports missingFiles even with skip_checking=true. In that case, we
	// set priorities first and let qBittorrent check only selected files.
	if deselectedIDs == "" {
		opts["skip_checking"] = "true"
	}

	if req.GetCategory() != "" {
		opts["category"] = req.GetCategory()
	}
	if req.GetTags() != "" {
		opts["tags"] = req.GetTags()
	}

	if addErr := s.qbClient.AddTorrentFromMemoryCtx(ctx, torrentData, opts); addErr != nil {
		return "", fmt.Errorf("adding torrent: %w", addErr)
	}

	s.logger.InfoContext(ctx, "added torrent to qBittorrent",
		"hash", hash,
		"savePath", savePath,
		"skipChecking", deselectedIDs == "",
	)

	// For partial file selection: set deselected file priorities to 0 and
	// resume so qBittorrent checks only selected files (which exist on disk).
	if deselectedIDs != "" {
		s.applyDeselectedPriorities(ctx, hash, deselectedIDs)
	}

	finalState, waitErr := s.waitForTorrentReady(ctx, hash)

	// Always stop the torrent after adding, even if waitForTorrentReady was
	// interrupted by context cancellation. Uses a detached context because the
	// gRPC caller may cancel before the wait completes, but the stop must succeed
	// to prevent dual seeding. qBittorrent may also briefly transition through an
	// active state (e.g. stalledUP) even with stopped=true+skip_checking=true.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), stopTorrentTimeout)
	defer stopCancel()
	if stopErr := s.qbClient.StopCtx(stopCtx, []string{hash}); stopErr != nil {
		s.logger.WarnContext(ctx, "failed to stop torrent after add (may already be stopped)",
			"hash", hash, "error", stopErr)
	}

	return finalState, waitErr
}

// waitForTorrentReady polls until the torrent is verified and ready.
func (s *Server) waitForTorrentReady(ctx context.Context, hash string) (qbittorrent.TorrentState, error) {
	interval := defaultQBPollInterval
	timeout := defaultQBPollTimeout

	if s.config.QB != nil {
		if s.config.QB.PollInterval > 0 {
			interval = s.config.QB.PollInterval
		}
		if s.config.QB.PollTimeout > 0 {
			timeout = s.config.QB.PollTimeout
		}
	}

	var finalState qbittorrent.TorrentState

	waitErr := utils.Until(ctx, func(pollCtx context.Context) (bool, error) {
		torrents, getErr := s.qbClient.GetTorrentsCtx(pollCtx, qbittorrent.TorrentFilterOptions{
			Hashes: []string{hash},
		})
		if getErr != nil {
			return false, getErr
		}
		if len(torrents) == 0 {
			return false, nil // Still waiting for torrent to appear
		}

		torrent := torrents[0]
		finalState = torrent.State

		// Hard error state — fail immediately.
		if torrent.State == qbittorrent.TorrentStateError {
			return false, fmt.Errorf("torrent entered error state: %s", torrent.State)
		}

		// missingFiles may be transient after partial file selection: we set
		// deselected priorities to 0 and resume, so keep polling until
		// qBittorrent re-evaluates (or the overall timeout expires).
		if torrent.State == qbittorrent.TorrentStateMissingFiles {
			s.logger.DebugContext(pollCtx, "torrent in missingFiles state, waiting for recovery",
				"hash", hash)
			return false, nil
		}

		// Still checking - keep waiting
		if isCheckingState(torrent.State) {
			s.logger.DebugContext(pollCtx, "torrent checking",
				"hash", hash,
				"progress", torrent.Progress,
				"state", torrent.State,
			)
			return false, nil
		}

		// Verify hash check is complete: Progress must be 100%
		if torrent.Progress < 1.0 {
			s.logger.DebugContext(pollCtx, "torrent not complete",
				"hash", hash,
				"progress", torrent.Progress,
				"state", torrent.State,
			)
			return false, nil
		}

		// Torrent is ready - must be in a seeding/upload state with 100% progress
		isReady := isReadyState(torrent.State)
		if isReady {
			s.logger.InfoContext(pollCtx, "torrent verified and ready",
				"hash", hash,
				"progress", torrent.Progress,
				"state", torrent.State,
			)
		}

		return isReady, nil
	}, interval, timeout)

	return finalState, waitErr
}

// StartTorrent resumes a stopped torrent on destination qBittorrent.
func (s *Server) StartTorrent(ctx context.Context, req *pb.StartTorrentRequest) (*pb.StartTorrentResponse, error) {
	hash := req.GetTorrentHash()

	if s.qbClient == nil {
		return &pb.StartTorrentResponse{
			Success: false,
			Error:   "destination qBittorrent not configured",
		}, nil
	}

	_, found, getErr := s.getQBTorrent(ctx, hash)
	if getErr != nil {
		return &pb.StartTorrentResponse{
			Success: false,
			Error:   fmt.Sprintf("checking torrent: %v", getErr),
		}, nil
	}
	if !found {
		return &pb.StartTorrentResponse{
			Success: false,
			Error:   "torrent does not exist on destination qBittorrent",
		}, nil
	}

	if resumeErr := s.qbClient.ResumeCtx(ctx, []string{hash}); resumeErr != nil {
		return &pb.StartTorrentResponse{
			Success: false,
			Error:   fmt.Sprintf("resume failed: %v", resumeErr),
		}, nil
	}

	if tag := req.GetTag(); tag != "" {
		if tagErr := s.qbClient.AddTagsCtx(ctx, []string{hash}, tag); tagErr != nil {
			metrics.TagApplicationErrorsTotal.WithLabelValues(metrics.ModeDestination).Inc()
			s.logger.WarnContext(ctx, "failed to apply tag to started torrent",
				"hash", hash, "tag", tag, "error", tagErr,
			)
		}
	}

	s.logger.InfoContext(ctx, "started torrent on destination qBittorrent", "hash", hash)

	return &pb.StartTorrentResponse{Success: true}, nil
}

// deleteTorrentFromQB removes a torrent from destination qBittorrent without deleting files.
// Used during re-sync to clear a stale entry that reports 100% after partial finalization.
func (s *Server) deleteTorrentFromQB(ctx context.Context, hash string) error {
	if s.qbClient == nil {
		return nil
	}
	if delErr := s.qbClient.DeleteTorrentsCtx(ctx, []string{hash}, false); delErr != nil {
		s.logger.WarnContext(ctx, "failed to delete torrent from destination qBittorrent",
			"hash", hash, "error", delErr)
		return delErr
	}
	s.logger.InfoContext(ctx, "deleted stale torrent from destination qBittorrent for re-sync",
		"hash", hash)
	return nil
}

// applyDeselectedPriorities sets file priorities to 0 for deselected files
// and resumes the torrent so qBittorrent checks only selected files.
func (s *Server) applyDeselectedPriorities(ctx context.Context, hash, ids string) {
	if priorityErr := s.qbClient.SetFilePriorityCtx(ctx, hash, ids, 0); priorityErr != nil {
		s.logger.WarnContext(ctx, "failed to set deselected file priorities",
			"hash", hash, "error", priorityErr)
	}
	if resumeErr := s.qbClient.ResumeCtx(ctx, []string{hash}); resumeErr != nil {
		s.logger.WarnContext(ctx, "failed to resume for file checking",
			"hash", hash, "error", resumeErr)
	}
	s.logger.InfoContext(ctx, "set deselected file priorities and started checking",
		"hash", hash, "deselected", ids)
}

// deselectedFileIDs returns a pipe-separated string of 0-based file indices
// that are not selected (priority 0). Returns "" if all files are selected.
func deselectedFileIDs(files []*serverFileInfo) string {
	var ids []string
	for i, f := range files {
		if !f.selected {
			ids = append(ids, strconv.Itoa(i))
		}
	}
	return strings.Join(ids, "|")
}

// isErrorState returns true if the torrent state indicates an error.
func isErrorState(state qbittorrent.TorrentState) bool {
	return state == qbittorrent.TorrentStateError ||
		state == qbittorrent.TorrentStateMissingFiles
}

// isCheckingState returns true if the torrent is still checking files.
func isCheckingState(state qbittorrent.TorrentState) bool {
	return state == qbittorrent.TorrentStateCheckingUp ||
		state == qbittorrent.TorrentStateCheckingDl ||
		state == qbittorrent.TorrentStateCheckingResumeData
}

// isReadyState returns true if the torrent is in a seeding/upload state.
func isReadyState(state qbittorrent.TorrentState) bool {
	//nolint:exhaustive // default case intentionally handles all non-ready states
	switch state {
	case qbittorrent.TorrentStateUploading,
		qbittorrent.TorrentStateStalledUp,
		qbittorrent.TorrentStateForcedUp,
		qbittorrent.TorrentStatePausedUp,
		qbittorrent.TorrentStateStoppedUp:
		return true
	default:
		return false
	}
}
