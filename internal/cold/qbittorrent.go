package cold

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/autobrr/go-qbittorrent"

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
		// If it's in error state, return error
		if isErrorState(existingTorrent.State) {
			return existingTorrent.State, fmt.Errorf("torrent in error state: %s", existingTorrent.State)
		}

		// If already verified (100% progress and ready), return success
		if existingTorrent.Progress >= 1.0 && isReadyState(existingTorrent.State) {
			return existingTorrent.State, nil
		}

		// If checking, wait for completion
		if isCheckingState(existingTorrent.State) || existingTorrent.Progress < 1.0 {
			return s.waitForTorrentReady(ctx, hash)
		}

		return existingTorrent.State, nil
	}


	// Torrent doesn't exist - add it
	torrentData, readErr := os.ReadFile(state.torrentPath)
	if readErr != nil {
		return "", fmt.Errorf("reading torrent file: %w", readErr)
	}

	// Use the cold-side save path (container mount point, e.g., "/downloads").
	// The hot's save path from req is meaningless on the cold side.
	savePath := s.config.GetSavePath()

	opts := map[string]string{
		"savepath":           savePath,
		"skip_checking":      "true",  // We verified pieces on write via SHA1 hash
		"stopped":            "true",  // Add stopped so hot controls when cold starts seeding (qB v5+)
		"paused":             "true",  // Compat alias for qB v4.x
		"autoTMM":            "false",
		"sequentialDownload": "false",
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
	)

	// Wait for torrent to be ready (skip_checking=true so should be immediate)
	finalState, waitErr := s.waitForTorrentReady(ctx, hash)

	// Always stop the torrent after adding, even if waitForTorrentReady was
	// interrupted by context cancellation. Uses a detached context because the
	// gRPC caller may cancel before the wait completes, but the stop must succeed
	// to prevent dual seeding. qBittorrent may also briefly transition through an
	// active state (e.g. stalledUP) even with stopped=true+skip_checking=true.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
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

	if s.config.ColdQB != nil {
		if s.config.ColdQB.PollInterval > 0 {
			interval = s.config.ColdQB.PollInterval
		}
		if s.config.ColdQB.PollTimeout > 0 {
			timeout = s.config.ColdQB.PollTimeout
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

		// Check for error states - fail immediately
		if isErrorState(torrent.State) {
			return false, fmt.Errorf("torrent entered error state: %s", torrent.State)
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

// StartTorrent resumes a stopped torrent on cold qBittorrent.
func (s *Server) StartTorrent(ctx context.Context, req *pb.StartTorrentRequest) (*pb.StartTorrentResponse, error) {
	hash := req.GetTorrentHash()

	if s.qbClient == nil {
		return &pb.StartTorrentResponse{
			Success: false,
			Error:   "cold qBittorrent not configured",
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
			Error:   "torrent does not exist on cold qBittorrent",
		}, nil
	}

	if resumeErr := s.qbClient.ResumeCtx(ctx, []string{hash}); resumeErr != nil {
		return &pb.StartTorrentResponse{
			Success: false,
			Error:   fmt.Sprintf("resume failed: %v", resumeErr),
		}, nil
	}

	s.logger.InfoContext(ctx, "started torrent on cold qBittorrent", "hash", hash)

	return &pb.StartTorrentResponse{Success: true}, nil
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
