package destination

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

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

// isTorrentInQB checks whether the torrent exists in destination qBittorrent.
// Fail-closed: returns true on error (QB unreachable) to prevent accidental deletion.
func (s *Server) isTorrentInQB(ctx context.Context, hash string) bool {
	if s.qbClient == nil {
		return false
	}
	_, found, err := s.getQBTorrent(ctx, hash)
	if err != nil {
		s.logger.WarnContext(ctx, "qBittorrent check failed during orphan cleanup, skipping cleanup",
			"hash", hash,
			"error", err,
		)
		return true // fail-closed
	}
	return found
}

// addAndVerifyTorrent adds the torrent to qBittorrent and waits for verification.
// On exit the torrent is always stopped on destination qB (best-effort): source
// is the canonical seeder until handoff via StartTorrent. Without this, a crash
// mid-finalization followed by recovery can leave the torrent already running
// in destination qB, producing a dual-seeding window.
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
		finalState := existingTorrent.State
		var waitErr error
		// If already at 100% and seeding-side, accept the current state; otherwise
		// (checking, incomplete, moving, etc.) poll until ready.
		if existingTorrent.Progress < 1.0 || !isReadyState(existingTorrent.State) {
			finalState, waitErr = s.waitForTorrentReady(ctx, hash, state.totalSize)
		}
		s.stopTorrentBestEffort(ctx, hash)
		return finalState, waitErr
	}

	// Torrent doesn't exist - add it.
	state.mu.Lock()
	torrentData := state.torrentFile
	state.mu.Unlock()
	if len(torrentData) == 0 {
		return "", errors.New("torrent file bytes not cached on state")
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
		// Defense in depth against the brief window between AddTorrent and the
		// post-add Stop. With stopped=true, qB shouldn't announce or upload —
		// but autobrr observed qB v5 occasionally announcing during this
		// window. Pinning bandwidth to 0 prevents any traffic while the
		// torrent is technically "added" but our explicit StopCtx hasn't
		// landed yet. StartTorrent re-enables transfer by removing the limits.
		"upLimit": "0",
		"dlLimit": "0",
	}

	// skip_checking is safe only when all files are present on disk. With
	// partial file selection, deselected files are absent, and qBittorrent
	// reports missingFiles even with skip_checking=true. In that case, we
	// set priorities first and let qBittorrent check only selected files.
	if deselectedIDs == "" {
		opts["skip_checking"] = "true"
	}

	if req.GetCategory() != "" {
		// qBittorrent silently drops the category field on AddTorrent if the
		// category doesn't already exist on the destination instance — autobrr
		// learned this the hard way. Ensure the category exists first so the
		// user's directory-layout assumption (savePath joined with sub-path
		// from category folders) doesn't silently diverge from qB's view.
		s.ensureCategoryExists(ctx, req.GetCategory())
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

	finalState, waitErr := s.waitForTorrentReady(ctx, hash, state.totalSize)
	s.stopTorrentBestEffort(ctx, hash)
	return finalState, waitErr
}

// ensureCategoryExists creates the category on destination qB if it doesn't
// already exist. Best-effort: failure is logged and swallowed so AddTorrent
// can still proceed (qB will silently ignore the unknown category, matching
// pre-fix behavior). With this in place the common "user has 'movies' on
// source but never created it on dest" case works correctly.
func (s *Server) ensureCategoryExists(ctx context.Context, category string) {
	cats, err := s.qbClient.GetCategoriesCtx(ctx)
	if err != nil {
		s.logger.WarnContext(ctx, "failed to list categories on destination, will let AddTorrent attempt",
			"category", category, "error", err)
		return
	}
	if _, exists := cats[category]; exists {
		return
	}
	if createErr := s.qbClient.CreateCategoryCtx(ctx, category, ""); createErr != nil {
		s.logger.WarnContext(ctx, "failed to create category on destination",
			"category", category, "error", createErr)
		return
	}
	s.logger.InfoContext(ctx, "created category on destination qBittorrent",
		"category", category)
}

// stopTorrentBestEffort stops the torrent on destination qB. Uses a detached
// context because the gRPC caller may cancel before the stop completes, but
// the stop must run to prevent dual seeding (source still believes it is the
// canonical seeder until handoff). Failure is logged and swallowed — the
// torrent may already be stopped, or qB may be unreachable at this moment.
func (s *Server) stopTorrentBestEffort(ctx context.Context, hash string) {
	stopCtx, stopCancel := context.WithTimeout(context.Background(), stopTorrentTimeout)
	defer stopCancel()
	if stopErr := s.qbClient.StopCtx(stopCtx, []string{hash}); stopErr != nil {
		s.logger.WarnContext(ctx, "failed to stop torrent (may already be stopped)",
			"hash", hash, "error", stopErr)
	}
}

// computePollTimeout returns a reasonable wait budget for qBittorrent to
// finish verifying a torrent of the given size. qB's recheck pass is roughly
// linear in data volume, so the timeout scales by GB above a small floor and
// is capped to prevent unbounded waits. Operators can override via the
// QBConfig.PollTimeout field when they have specific knowledge.
func computePollTimeout(totalSize int64) time.Duration {
	const bytesPerGB = 1024 * 1024 * 1024
	gigabytes := totalSize / bytesPerGB
	timeout := defaultQBPollTimeoutBase + time.Duration(gigabytes)*defaultQBPollTimeoutPerGB
	if timeout > defaultQBPollTimeoutMax {
		return defaultQBPollTimeoutMax
	}
	return timeout
}

// waitForTorrentReady polls until the torrent is verified and ready.
// totalSize is used to size the poll timeout when QBConfig.PollTimeout is unset.
func (s *Server) waitForTorrentReady(
	ctx context.Context,
	hash string,
	totalSize int64,
) (qbittorrent.TorrentState, error) {
	interval := defaultQBPollInterval
	timeout := computePollTimeout(totalSize)

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

		// Terminal qB-reported failures. missingFiles persists once qB lands in
		// it (priority-0 update lost a race, files truly absent, etc.) — polling
		// until the multi-hour budget expires hides the failure. Fail fast so the
		// source can re-sync via FINALIZE_ERROR_INCOMPLETE.
		if isErrorState(torrent.State) {
			return false, fmt.Errorf("torrent in error state: %s", torrent.State)
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
		if !isReadyState(torrent.State) {
			return false, nil
		}

		s.logger.InfoContext(pollCtx, "torrent verified and ready",
			"hash", hash,
			"progress", torrent.Progress,
			"state", torrent.State,
		)
		return true, nil
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

	// Clear the dl/up bandwidth limits we set at AddTorrent time. -1 in the
	// qB API removes the per-torrent limit and defers to global settings.
	// Best-effort: a stuck-at-zero limit on a few torrents is recoverable
	// manually, so we don't fail the StartTorrent call on this.
	s.clearTorrentRateLimits(ctx, hash)

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

// clearTorrentRateLimits removes the upLimit/dlLimit set during AddTorrent
// (the autobrr-pattern defense against the brief announce-before-stop
// window). Passes -1 to the qB API which defers to global settings.
// Best-effort.
func (s *Server) clearTorrentRateLimits(ctx context.Context, hash string) {
	if upErr := s.qbClient.SetTorrentUploadLimitCtx(ctx, []string{hash}, -1); upErr != nil {
		s.logger.WarnContext(ctx, "failed to clear upload limit on start", "hash", hash, "error", upErr)
	}
	if dlErr := s.qbClient.SetTorrentDownloadLimitCtx(ctx, []string{hash}, -1); dlErr != nil {
		s.logger.WarnContext(ctx, "failed to clear download limit on start", "hash", hash, "error", dlErr)
	}
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
		qbittorrent.TorrentStateStoppedUp,
		qbittorrent.TorrentStateQueuedUp:
		return true
	default:
		return false
	}
}
