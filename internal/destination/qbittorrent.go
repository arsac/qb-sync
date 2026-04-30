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

	if found && isErrorState(existingTorrent.State) {
		return existingTorrent.State, fmt.Errorf("torrent in error state: %s", existingTorrent.State)
	}

	// Fast path: torrent is already verified and seeding-ready. Just stop
	// (source remains canonical seeder until handoff) and return.
	if found && existingTorrent.Progress >= 1.0 && isReadyState(existingTorrent.State) {
		s.stopTorrentBestEffort(ctx, hash)
		return existingTorrent.State, nil
	}

	if !found {
		if addErr := s.addTorrentToQB(ctx, hash, state, req); addErr != nil {
			return "", addErr
		}
	}

	if needsPartialSelectionRecovery(found, existingTorrent, state.files) {
		if found {
			s.logger.InfoContext(ctx, "existing torrent stuck stopped near zero progress, re-applying priorities",
				"hash", hash,
				"state", existingTorrent.State,
				"progress", existingTorrent.Progress,
			)
		}
		if priErr := s.applyAndVerifyDeselectedPriorities(ctx, hash, state.files); priErr != nil {
			s.stopTorrentBestEffort(ctx, hash)
			return "", fmt.Errorf("applying deselected priorities: %w", priErr)
		}
	}

	finalState, waitErr := s.waitForTorrentReady(ctx, hash, state.totalSize)
	s.stopTorrentBestEffort(ctx, hash)
	return finalState, waitErr
}

// addTorrentToQB issues the actual AddTorrent against destination qB with the
// options block. Stopped + bandwidth=0 + autoTMM=false is the safe-add posture
// (no announce, no upload, qB doesn't move files); skip_checking=true is only
// safe when all files exist on disk, so it's gated on full selection.
func (s *Server) addTorrentToQB(
	ctx context.Context,
	hash string,
	state *serverTorrentState,
	req *pb.FinalizeTorrentRequest,
) error {
	state.mu.Lock()
	torrentData := state.torrentFile
	state.mu.Unlock()
	if len(torrentData) == 0 {
		return errors.New("torrent file bytes not cached on state")
	}

	savePath := filepath.Join(s.config.GetSavePath(), state.saveSubPath)
	deselectedIDs := deselectedFileIDs(state.files)

	opts := map[string]string{
		"savepath":           savePath,
		"stopped":            "true", // Source controls when destination starts seeding (qB v5+)
		"paused":             "true", // Compat alias for qB v4.x
		"autoTMM":            "false",
		"sequentialDownload": "false",
		// autobrr observed qB v5 occasionally announcing during the brief
		// window between AddTorrent and the explicit Stop. Pinning bandwidth
		// to 0 prevents traffic. StartTorrent re-enables on handoff.
		"upLimit": "0",
		"dlLimit": "0",
	}

	// Partial selection: deselected files are absent, so qB reports missingFiles
	// even with skip_checking=true. Caller applies priorities + resumes after add.
	if deselectedIDs == "" {
		opts["skip_checking"] = "true"
	}

	if req.GetCategory() != "" {
		// qBittorrent silently drops the category field on AddTorrent if the
		// category doesn't already exist on the destination instance.
		s.ensureCategoryExists(ctx, req.GetCategory())
		opts["category"] = req.GetCategory()
	}
	if req.GetTags() != "" {
		opts["tags"] = req.GetTags()
	}

	if addErr := s.qbClient.AddTorrentFromMemoryCtx(ctx, torrentData, opts); addErr != nil {
		return fmt.Errorf("adding torrent: %w", addErr)
	}

	s.logger.InfoContext(ctx, "added torrent to qBittorrent",
		"hash", hash,
		"savePath", savePath,
		"skipChecking", deselectedIDs == "",
	)
	return nil
}

// needsPartialSelectionRecovery decides when to (re-)apply deselected file
// priorities. Two cases:
//
//   - Fresh add: always apply, since priorities aren't set yet.
//   - Existing torrent stuck at near-zero progress in a download-side stopped
//     state: this is the production-observed signature of qB silently dropping
//     the priority change on a prior attempt's add. Progress > 0 means qB is
//     actively making progress (mid-recheck, partial download) — don't disrupt
//     by re-issuing SetFilePriority, and don't override an operator who paused
//     a torrent mid-progress for investigation.
func needsPartialSelectionRecovery(found bool, t *qbittorrent.Torrent, files []*serverFileInfo) bool {
	if deselectedFileIDs(files) == "" {
		return false
	}
	if !found {
		return true
	}
	return t.Progress < 0.001 && isDownloadStoppedState(t.State)
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

	var (
		finalState    qbittorrent.TorrentState
		finalProgress float64
		pollCount     int
	)

	waitErr := utils.Until(ctx, func(pollCtx context.Context) (bool, error) {
		pollCount++
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
		finalProgress = torrent.Progress

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

	// Capture the qB state at the moment of timeout so operators can tell which
	// sub-cause fired (stoppedDL+<100% from a failed Resume, persistent
	// pausedUP that never reaches a ready state, torrent disappeared from qB
	// entirely, etc.) without re-running with debug logging.
	if errors.Is(waitErr, utils.ErrTimeout) {
		s.logger.WarnContext(ctx, "waitForTorrentReady timed out",
			"hash", hash,
			"lastState", finalState,
			"lastProgress", finalProgress,
			"polls", pollCount,
			"budget", timeout,
		)
	}

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

// applyAndVerifyDeselectedPriorities exists because qB returns 200 OK to
// filePrio on freshly-added stopped torrents whether it persists the change or
// not. Skipping the read-back verify lets a silently-dropped priority update
// strand the torrent in stoppedDl with default priorities until the downstream
// poll budget expires. The retry loop keeps trying until qB has settled enough
// for the change to stick; a budget-exhaustion error fails the finalize
// attempt so the source-side cap takes over instead of blocking on a doomed
// torrent.
func (s *Server) applyAndVerifyDeselectedPriorities(
	ctx context.Context,
	hash string,
	files []*serverFileInfo,
) error {
	deselectedIDs := deselectedFileIDs(files)
	expected := make(map[int]bool, len(files))
	for i, f := range files {
		if !f.selected {
			expected[i] = true
		}
	}

	interval := priorityVerifyInterval
	timeout := priorityVerifyTimeout
	if s.config.QB != nil {
		if s.config.QB.PriorityVerifyInterval > 0 {
			interval = s.config.QB.PriorityVerifyInterval
		}
		if s.config.QB.PriorityVerifyTimeout > 0 {
			timeout = s.config.QB.PriorityVerifyTimeout
		}
	}

	deadline := time.Now().Add(timeout)
	var attempts int
	var lastErr error

	for time.Now().Before(deadline) {
		attempts++
		applied, attemptErr := s.attemptDeselectedPrioritiesOnce(ctx, hash, deselectedIDs, expected, attempts)
		if applied {
			s.logger.InfoContext(ctx, "deselected priorities applied and verified",
				"hash", hash, "attempts", attempts, "deselected", deselectedIDs)
			if resumeErr := s.qbClient.ResumeCtx(ctx, []string{hash}); resumeErr != nil {
				return fmt.Errorf("resume after priorities verified: %w", resumeErr)
			}
			metrics.PartialSelectionRecoveryTotal.WithLabelValues(metrics.ResultSuccess).Inc()
			return nil
		}
		if attemptErr != nil {
			lastErr = attemptErr
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
		interval = min(interval*priorityVerifyBackoffMultiplier, priorityVerifyMaxInterval)
	}

	metrics.PartialSelectionRecoveryTotal.WithLabelValues(metrics.ResultFailure).Inc()
	if lastErr != nil {
		return fmt.Errorf("deselected priorities never persisted after %d attempts: %w", attempts, lastErr)
	}
	return fmt.Errorf("deselected priorities never persisted after %d attempts", attempts)
}

// attemptDeselectedPrioritiesOnce is split out from the retry loop to keep the
// caller under gocognit/nestif limits. The returned err is the latest transient
// API failure so the outer budget-exhaustion error can wrap it.
func (s *Server) attemptDeselectedPrioritiesOnce(
	ctx context.Context,
	hash, deselectedIDs string,
	expected map[int]bool,
	attempt int,
) (bool, error) {
	if priErr := s.qbClient.SetFilePriorityCtx(ctx, hash, deselectedIDs, 0); priErr != nil {
		s.logger.DebugContext(ctx, "set file priority failed, will retry",
			"hash", hash, "attempt", attempt, "error", priErr)
		return false, priErr
	}
	applied, verifyErr := s.deselectedPrioritiesApplied(ctx, hash, expected)
	if verifyErr != nil {
		s.logger.DebugContext(ctx, "verify priorities failed, will retry",
			"hash", hash, "attempt", attempt, "error", verifyErr)
		return false, verifyErr
	}
	return applied, nil
}

// deselectedPrioritiesApplied reads back qB's reported file priorities and
// returns true when every expected-deselected file is at priority 0.
func (s *Server) deselectedPrioritiesApplied(
	ctx context.Context,
	hash string,
	expectedDeselected map[int]bool,
) (bool, error) {
	qbFiles, err := s.qbClient.GetFilesInformationCtx(ctx, hash)
	if err != nil {
		return false, err
	}
	for _, f := range *qbFiles {
		if expectedDeselected[f.Index] && f.Priority != 0 {
			return false, nil
		}
	}
	return true, nil
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

// isDownloadStoppedState returns true for download-side stopped states.
// Distinct from pausedUp/stoppedUp (seeding-ready, see isReadyState): those
// are terminal-good, these mean "qB never finished verifying".
func isDownloadStoppedState(state qbittorrent.TorrentState) bool {
	return state == qbittorrent.TorrentStateStoppedDl ||
		state == qbittorrent.TorrentStatePausedDl
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
