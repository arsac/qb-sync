package destination

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/bits-and-blooms/bitset"

	"github.com/arsac/qb-sync/internal/metrics"
	pb "github.com/arsac/qb-sync/proto"
)

// qbCheckResult represents the result of checking a torrent in qBittorrent.
type qbCheckResult int

const (
	qbCheckNotFound qbCheckResult = iota
	qbCheckVerifying
	qbCheckComplete
)

// InitTorrent initializes or resumes a torrent sync.
// This is the primary entry point that determines what pieces need to be streamed.
//
// Decision flow:
// 1. If re-sync, clean up all prior state first (qB, memory, disk).
// 2. Check destination qBittorrent - if complete, return SYNC_STATUS_COMPLETE.
// 3. Check active sync state - return remaining pieces_needed.
// 4. Fresh torrent - initialize tracking and return all pieces_needed.
func (s *Server) InitTorrent(
	ctx context.Context,
	req *pb.InitTorrentRequest,
) (*pb.InitTorrentResponse, error) {
	hash := req.GetTorrentHash()

	// Wait for any in-progress abort to complete before initializing.
	if err := s.waitForAbortComplete(ctx, hash); err != nil {
		return initErrorResponse("abort in progress: %v", err), nil
	}

	// Re-sync: file selection changed, so we must re-initialize from scratch.
	// Clean up ALL prior state (qB entry, in-memory state, persisted .state,
	// finalized marker) in one place before any other checks.
	if req.GetResync() && len(req.GetFiles()) > 0 {
		if err := s.cleanupForResync(ctx, hash); err != nil {
			return initErrorResponse("re-sync cleanup: %v", err), nil
		}
	}

	// Fast local check: finalized marker means we already wrote, verified,
	// and added this torrent to destination qBittorrent. No QB query needed.
	if s.isFinalized(hash) {
		s.logger.InfoContext(ctx, "torrent already finalized (local marker)",
			"hash", hash,
		)
		return &pb.InitTorrentResponse{
			Success: true,
			Status:  pb.TorrentSyncStatus_SYNC_STATUS_COMPLETE,
		}, nil
	}

	// Check if torrent is already complete/verifying in destination qBittorrent.
	if resp := s.checkQBCompletion(ctx, hash); resp != nil {
		return resp, nil
	}

	// Check for existing state (resume case).
	// Use GetWithSentinel to distinguish "not found" from "initializing".
	if resp, found := s.lookupExistingState(hash, req); found {
		return resp, nil
	}

	// Don't create state for minimal requests (e.g., CheckTorrentStatus sends only hash).
	// Return -1 to indicate torrent not initialized yet - caller should do full InitTorrent.
	if len(req.GetFiles()) == 0 {
		return &pb.InitTorrentResponse{
			Success:           true,
			Status:            pb.TorrentSyncStatus_SYNC_STATUS_READY,
			PiecesNeededCount: -1,
		}, nil
	}

	// Reserve the hash while we do disk I/O.
	// This prevents concurrent InitTorrent calls for the same hash from racing.
	if reserveErr := s.store.Reserve(hash); reserveErr != nil {
		// Another goroutine beat us -- retry the check.
		if resp, found := s.lookupExistingState(hash, req); found {
			return resp, nil
		}
		return initErrorResponse("reserve failed: %v", reserveErr), nil
	}

	// Initialize new torrent (disk I/O happens here without store lock held)
	resp := s.initNewTorrent(ctx, hash, req)

	// On error, clean up the sentinel
	if !resp.GetSuccess() {
		s.store.Unreserve(hash)
	}

	return resp, nil
}

// lookupExistingState returns a response for any pre-existing entry for hash:
// an "in progress" error if a sentinel is present, or a resume response if
// real state is present. Returns (nil, false) when no entry exists.
func (s *Server) lookupExistingState(
	hash string,
	req *pb.InitTorrentRequest,
) (*pb.InitTorrentResponse, bool) {
	state, exists := s.store.GetWithSentinel(hash)
	if !exists {
		return nil, false
	}
	if state.initializing.Load() {
		return initErrorResponse("torrent initialization already in progress"), true
	}
	return s.resumeTorrent(state, req), true
}

// resumeTorrent handles resuming an existing torrent sync.
func (s *Server) resumeTorrent(
	state *serverTorrentState,
	req *pb.InitTorrentRequest,
) *pb.InitTorrentResponse {
	// Acquire state.mu to read written/hardlinkResults safely — a concurrent
	// WritePiece may be modifying state.written under this lock. Cache the
	// torrent file bytes under the same lock when missing to avoid two
	// lock/unlock cycles.
	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.torrentFile) == 0 && len(req.GetTorrentFile()) > 0 {
		state.torrentFile = req.GetTorrentFile()
	}
	return state.buildReadyResponse()
}

// setupMetadataDir creates the metadata directory and writes the .meta file
// (serialized PersistedTorrentMeta). If the directory exists without .meta
// (old format or corrupted), it is removed first.
// Skips .meta write if it already exists (idempotent for recovery path).
// Returns metaDir, statePath, and error.
func (s *Server) setupMetadataDir(
	hash string,
	req *pb.InitTorrentRequest,
) (string, string, error) {
	metaDir := filepath.Join(s.config.BasePath, metaDirName, hash)
	metaPath := filepath.Join(metaDir, metaFileName)
	statePath := filepath.Join(metaDir, stateFileName)

	// If .meta already exists, this is a recovery/idempotent call — skip all writes.
	if _, err := os.Stat(metaPath); err == nil {
		return metaDir, statePath, nil
	}

	// .meta absent: either fresh init or old-format directory.
	// Remove any stale directory contents and recreate.
	_ = os.RemoveAll(metaDir)
	if err := os.MkdirAll(metaDir, serverDirPermissions); err != nil {
		return "", "", fmt.Errorf("creating metadata directory: %w", err)
	}

	meta := buildPersistedMeta(req)
	if err := savePersistedMeta(metaPath, meta); err != nil {
		return "", "", fmt.Errorf("writing metadata: %w", err)
	}

	return metaDir, statePath, nil
}

// initNewTorrent handles initializing a fresh torrent sync.
func (s *Server) initNewTorrent(
	ctx context.Context,
	hash string,
	req *pb.InitTorrentRequest,
) *pb.InitTorrentResponse {
	name := req.GetName()

	// Create metadata directory and write .meta file.
	metaDir, statePath, err := s.setupMetadataDir(hash, req)
	if err != nil {
		return initErrorResponse("%v", err)
	}

	// Check for existing files at a different sub-path and relocate if needed.
	// This handles destination restart: persisted state may have files at the old path
	// (e.g., no category prefix), while source now sends the correct sub-path.
	saveSubPath := req.GetSaveSubPath()
	metaPath := filepath.Join(metaDir, metaFileName)
	s.maybeRelocateSubPath(ctx, hash, req, metaPath, saveSubPath)

	// Set up files and check for hardlink opportunities
	files, hardlinkResults, err := s.setupFiles(ctx, hash, req.GetFiles(), saveSubPath)
	if err != nil {
		return initErrorResponse("%v", err)
	}

	pieceSize := req.GetPieceSize()
	totalSize := req.GetTotalSize()

	// Create torrentMeta early so buildWrittenBitmap can use its methods.
	meta := torrentMeta{
		pieceHashes: req.GetPieceHashes(),
		pieceLength: pieceSize,
		totalSize:   totalSize,
		files:       files,
	}

	// Build written bitmap from persisted state + hardlink coverage.
	written := s.buildWrittenBitmap(ctx, hash, statePath, &meta)

	// Persist initial written state so pre-existing/hardlinked piece coverage
	// survives a crash before the first WritePiece triggers a periodic flush.
	if written.Count() > 0 {
		if saveErr := s.doSaveState(statePath, written); saveErr != nil {
			s.logger.WarnContext(ctx, "failed to persist initial state",
				"hash", hash, "error", saveErr)
		}
	}

	// Swap sentinel for real state under store lock.
	state := &serverTorrentState{
		torrentMeta:     meta,
		written:         written,
		verified:        bitset.New(uint(meta.numPieces())),
		torrentFile:     req.GetTorrentFile(),
		statePath:       statePath,
		saveSubPath:     saveSubPath,
		hardlinkResults: hardlinkResults,
	}
	if collisionErr := s.store.Commit(hash, state); collisionErr != nil {
		return initErrorResponse("path collision: %v", collisionErr)
	}

	// Log summary
	hardlinkedCount, pendingCount, preExistingCount := countHardlinkResults(hardlinkResults)
	_, needCount, haveCount := calculatePiecesNeeded(written)
	selectedCount := state.countSelectedFiles()

	s.logger.InfoContext(ctx, "initialized torrent",
		"hash", hash,
		"name", name,
		"pieces", meta.numPieces(),
		"files", len(files),
		"selectedFiles", selectedCount,
		"hasHashes", len(req.GetPieceHashes()) > 0,
		"hardlinked", hardlinkedCount,
		"pending", pendingCount,
		"preExisting", preExistingCount,
		"piecesNeeded", needCount,
		"piecesHave", haveCount,
	)

	return state.buildReadyResponse()
}

// maybeRelocateSubPath checks whether the persisted sub-path differs from the
// requested one and relocates data files if needed. After a successful
// relocation the .meta file is updated so recovery picks up the new path.
func (s *Server) maybeRelocateSubPath(
	ctx context.Context,
	hash string,
	req *pb.InitTorrentRequest,
	metaPath, saveSubPath string,
) {
	existingMeta, loadErr := loadPersistedMeta(metaPath)
	if loadErr != nil || existingMeta.GetSaveSubPath() == saveSubPath {
		return
	}

	relPaths := make([]string, len(req.GetFiles()))
	for i, f := range req.GetFiles() {
		relPaths[i] = f.GetPath()
	}
	if _, relocErr := s.relocateFiles(
		ctx, hash, relPaths, existingMeta.GetSaveSubPath(), saveSubPath,
	); relocErr != nil {
		s.logger.WarnContext(ctx, "failed to relocate files, continuing with fresh setup",
			"hash", hash, "error", relocErr)
	}
	// Update .meta to reflect the new sub-path so recovery uses the correct path.
	updatedMeta := buildPersistedMeta(req)
	if saveErr := savePersistedMeta(metaPath, updatedMeta); saveErr != nil {
		s.logger.WarnContext(ctx, "failed to update .meta after relocation",
			"hash", hash, "error", saveErr)
	}
}

// buildWrittenBitmap constructs the written bitmap from persisted state and hardlink
// coverage, and initializes per-file piece tracking for early finalization.
//
// When a persisted .state file claims pieces are written but the underlying data
// file is missing (externally deleted or hardlink never created), the stale pieces
// are cleared so only files that actually exist on disk keep their progress.
func (s *Server) buildWrittenBitmap(
	ctx context.Context,
	hash, statePath string,
	meta *torrentMeta,
) *bitset.BitSet {
	numPieces := meta.numPieces()
	piecesCovered := meta.calculatePiecesCovered()

	written := bitset.New(uint(numPieces))
	if existingState, loadErr := s.loadState(statePath, int(numPieces)); loadErr == nil {
		written = existingState
		s.logger.InfoContext(ctx, "resumed torrent state",
			"hash", hash,
			"written", written.Count(),
			"total", numPieces,
		)
	}

	written.InPlaceUnion(boolSliceToBitSet(piecesCovered))

	// Compute per-file piece ranges and mark files that need no streamed data as already finalized.
	meta.computeFilePieceRanges()
	for _, fi := range meta.files {
		if fi.skipForWriteData() {
			fi.earlyFinalized = true
		}
	}

	// Clear stale pieces: if a selected file's data is missing from disk,
	// the .state claim is outdated. Clear only the affected file's pieces
	// so that files which DO exist keep their progress.
	s.clearStalePieces(ctx, hash, written, meta.files)

	meta.initFilePieceCounts(written)

	return written
}

// isFinalized checks whether a .finalized marker exists for the given torrent,
// indicating it was previously synced, verified, and added to qBittorrent.
func (s *Server) isFinalized(hash string) bool {
	markerPath := filepath.Join(s.config.BasePath, metaDirName, hash, finalizedFileName)
	_, err := os.Stat(markerPath)
	return err == nil
}

// initErrorResponse creates an error response for InitTorrent.
func initErrorResponse(format string, args ...any) *pb.InitTorrentResponse {
	return &pb.InitTorrentResponse{
		Success: false,
		Error:   fmt.Sprintf(format, args...),
	}
}

// cleanupForResync removes all prior state for a torrent so it appears as
// "never initialized" to subsequent steps. Called at the top of InitTorrent
// when resync is requested.
func (s *Server) cleanupForResync(ctx context.Context, hash string) error {
	// 1. Delete from qBittorrent (any state — complete or verifying).
	if delErr := s.deleteTorrentFromQB(ctx, hash); delErr != nil {
		return fmt.Errorf("failed to delete stale qB entry: %w", delErr)
	}

	// 2. Delete in-memory state and file path ownership.
	s.store.Remove(hash)

	// 3. Delete persisted .state, .finalized marker, and .meta so the next
	// setupMetadataDir writes a fresh .meta from the new request. Without
	// the .meta removal, setupMetadataDir's idempotent skip would preserve
	// the stale file selection on disk — and a crash before the new
	// InitTorrent completes would leave recovery rebuilding state from the
	// old metadata.
	// During the first sync with partial selection, boundary pieces spanning
	// selected + unselected files were marked "written" but only the
	// selected-file portion was actually written to disk. On re-sync those
	// pieces must be re-streamed for the newly-selected portions.
	metaDir := filepath.Join(s.config.BasePath, metaDirName, hash)
	_ = os.Remove(filepath.Join(metaDir, stateFileName))
	_ = os.Remove(filepath.Join(metaDir, finalizedFileName))
	_ = os.Remove(filepath.Join(metaDir, metaFileName))

	s.logger.InfoContext(ctx, "re-sync: cleared all prior state",
		"hash", hash)
	return nil
}

// checkQBCompletion checks the torrent's state in destination qBittorrent and returns
// a response if the caller should short-circuit (complete or verifying).
// Returns nil if InitTorrent should continue to check local state.
// This is a pure check with no mutations.
func (s *Server) checkQBCompletion(ctx context.Context, hash string) *pb.InitTorrentResponse {
	switch s.checkTorrentInQB(ctx, hash) {
	case qbCheckComplete:
		return &pb.InitTorrentResponse{
			Success: true,
			Status:  pb.TorrentSyncStatus_SYNC_STATUS_COMPLETE,
		}
	case qbCheckVerifying:
		return &pb.InitTorrentResponse{
			Success: true,
			Status:  pb.TorrentSyncStatus_SYNC_STATUS_VERIFYING,
		}
	case qbCheckNotFound:
		return nil
	}
	return nil
}

// checkTorrentInQB checks if the torrent already exists in destination qBittorrent.
func (s *Server) checkTorrentInQB(ctx context.Context, hash string) qbCheckResult {
	if s.qbClient == nil {
		return qbCheckNotFound
	}

	torrent, found, err := s.getQBTorrent(ctx, hash)
	if err != nil {
		s.logger.WarnContext(ctx, "failed to check qBittorrent for torrent",
			"hash", hash,
			"error", err,
		)
		return qbCheckNotFound
	}

	if !found {
		return qbCheckNotFound
	}

	// If qBittorrent is actively checking files, report verifying so the
	// source waits — we must not write while qBittorrent reads.
	if isCheckingState(torrent.State) {
		return qbCheckVerifying
	}

	// Only return COMPLETE when the torrent is unambiguously seeding-side at
	// 100%. Accepting any non-error state at progress=1.0 is dangerous: with
	// partial file selection on qB v5 a torrent can report progress=1.0 in
	// pausedDL/stoppedDL even when files are missing on disk (e.g., the user
	// manually deleted unselected dirs). Trusting that would let source mark
	// the torrent synced and delete its data with nothing actually seeding.
	// Our own finalization path explicitly transitions to a stoppedUp/pausedUp
	// state via StopCtx after waitForTorrentReady, so the legitimate
	// already-finalized case lands in isReadyState. The local .finalized
	// marker check upstream (isFinalized) is the fast path for our own
	// finalizations; this RPC check is only for externally-added torrents.
	if torrent.Progress >= 1.0 && isReadyState(torrent.State) {
		s.logger.InfoContext(ctx, "torrent already complete in destination qBittorrent",
			"hash", hash,
			"state", torrent.State,
		)
		return qbCheckComplete
	}

	return qbCheckNotFound
}

// waitForAbortComplete waits for any in-progress abort to complete.
// This prevents a race where InitTorrent creates files that AbortTorrent then deletes.
func (s *Server) waitForAbortComplete(ctx context.Context, hash string) error {
	abortCh, aborting := s.store.AbortCh(hash)

	if !aborting {
		return nil
	}

	s.logger.DebugContext(ctx, "waiting for abort to complete before init", "hash", hash)

	select {
	case <-abortCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// hardlinkOutcome represents the result of hardlink resolution.
type hardlinkOutcome struct {
	state      hardlinkState
	sourcePath string        // Path that was hardlinked from (or will be)
	doneCh     chan struct{} // Channel to wait on for pending hardlinks
	err        error         // Error if hardlink creation failed
}

// setupFiles sets up all files for a torrent, checking for hardlink opportunities.
func (s *Server) setupFiles(
	ctx context.Context,
	hash string,
	reqFiles []*pb.FileInfo,
	saveSubPath string,
) ([]*serverFileInfo, []*pb.HardlinkResult, error) {
	files := make([]*serverFileInfo, len(reqFiles))
	results := make([]*pb.HardlinkResult, len(reqFiles))

	for i, f := range reqFiles {
		fileInfo, result, err := s.setupFile(ctx, hash, f, i, saveSubPath)
		if err != nil {
			return nil, nil, err
		}
		files[i] = fileInfo
		results[i] = result
	}

	return files, results, nil
}

// setupFile sets up a single file, checking for hardlink opportunities.
func (s *Server) setupFile(
	ctx context.Context,
	hash string,
	f *pb.FileInfo,
	fileIndex int,
	saveSubPath string,
) (*serverFileInfo, *pb.HardlinkResult, error) {
	result := &pb.HardlinkResult{FileIndex: int32(fileIndex)}
	targetPath := filepath.Join(s.config.BasePath, saveSubPath, f.GetPath())
	sourceFileID := FileID{Dev: f.GetDevice(), Ino: f.GetInode()}

	fileInfo := &serverFileInfo{
		path:     targetPath,
		size:     f.GetSize(),
		offset:   f.GetOffset(),
		hardlink: hardlinkInfo{sourceFileID: sourceFileID},
		selected: f.GetSelected(),
	}

	// Unselected files: no .partial, no directory creation, no hardlink resolution.
	// The serverFileInfo entry is still needed for offset tracking in writePieceData.
	if !f.GetSelected() {
		return fileInfo, result, nil
	}

	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(targetPath), serverDirPermissions); err != nil {
		return nil, nil, fmt.Errorf("creating directory for %s: %w", f.GetPath(), err)
	}

	// Check if file already exists with correct size (pre-existing data)
	if info, statErr := os.Stat(targetPath); statErr == nil && info.Size() == f.GetSize() {
		fileInfo.hardlink.state = hlStateComplete
		result.PreExisting = true
		return fileInfo, result, nil
	}

	// Check if .partial file exists from a previous interrupted sync.
	// Piece-level progress is recovered from .state in buildWrittenBitmap;
	// skip hardlink resolution since the file already has data on disk.
	partialPath := targetPath + partialSuffix
	fileInfo.path = partialPath
	if info, statErr := os.Stat(partialPath); statErr == nil && info.Size() == f.GetSize() {
		return fileInfo, result, nil
	}

	// Check for hardlink opportunities if file ID is provided
	if !sourceFileID.IsZero() {
		outcome := s.resolveHardlink(ctx, hash, f.GetPath(), targetPath, sourceFileID, f.GetSize())
		s.applyHardlinkOutcome(fileInfo, result, targetPath, outcome)
	}

	return fileInfo, result, nil
}

// resolveHardlink attempts to create or schedule a hardlink for the file.
func (s *Server) resolveHardlink(
	ctx context.Context,
	hash, filePath, targetPath string,
	sourceFileID FileID,
	expectedSize int64,
) hardlinkOutcome {
	// Case 1: Check if inode is already registered (completed file from previous torrent)
	if outcome, ok := s.tryHardlinkFromRegistered(ctx, hash, filePath, targetPath, sourceFileID, expectedSize); ok {
		return outcome
	}

	// Case 2: Check if another torrent is currently writing this inode
	if outcome, ok := s.tryHardlinkFromInProgress(ctx, hash, filePath, targetPath, sourceFileID); ok {
		return outcome
	}

	// Case 3: Try to register as the first writer for this inode.
	// RegisterInProgress returns false if another torrent raced us between
	// Case 2 (GetInProgress) and now. In that case, retry Case 2 to pick
	// up the winner's doneCh and become pending instead of duplicating work.
	if !s.registerInodeInProgress(ctx, hash, filePath, targetPath, sourceFileID) {
		if outcome, ok := s.tryHardlinkFromInProgress(ctx, hash, filePath, targetPath, sourceFileID); ok {
			return outcome
		}
		// Winner already completed between our two checks — write from scratch.
	}
	return hardlinkOutcome{state: hlStateInProgress}
}

// applyHardlinkOutcome applies the hardlink resolution outcome to the file info and result.
func (s *Server) applyHardlinkOutcome(
	fileInfo *serverFileInfo,
	result *pb.HardlinkResult,
	targetPath string,
	outcome hardlinkOutcome,
) {
	fileInfo.hardlink.applyOutcome(outcome)

	switch outcome.state {
	case hlStateComplete:
		result.Hardlinked = true
		result.SourcePath = outcome.sourcePath
		fileInfo.path = targetPath
	case hlStatePending:
		result.Pending = true
		result.SourcePath = outcome.sourcePath
		fileInfo.path = targetPath
	case hlStateInProgress, hlStateNone:
		// Keep default values
	}

	if outcome.err != nil {
		result.Error = outcome.err.Error()
	}
}

// tryHardlinkFromRegistered attempts to hardlink from a registered (completed) inode.
// Returns the outcome and true if a registered inode was found (even if hardlink failed).
//
// Validates that the registered file's size matches expectedSize before linking.
// Source filesystems recycle inodes after file deletion; a stale registry entry
// for a recycled inode would hardlink to a completely different file's content.
func (s *Server) tryHardlinkFromRegistered(
	ctx context.Context,
	hash, filePath, targetPath string,
	sourceFileID FileID,
	expectedSize int64,
) (hardlinkOutcome, bool) {
	existingPath, found := s.store.Inodes().GetRegistered(sourceFileID)
	if !found {
		return hardlinkOutcome{}, false
	}

	sourcePath := filepath.Join(s.config.BasePath, existingPath)

	// Guard against stale inode entries from recycled inodes.
	sourceInfo, statErr := os.Stat(sourcePath)
	if statErr != nil || sourceInfo.Size() != expectedSize {
		s.store.Inodes().Evict(sourceFileID)
		s.logger.InfoContext(ctx, "evicted stale inode registry entry",
			"torrent", hash,
			"file", filePath,
			"fileID", sourceFileID,
			"registeredPath", existingPath,
			"statErr", statErr,
		)
		metrics.StaleInodeEvictionsTotal.Inc()
		return hardlinkOutcome{}, false
	}

	// Same-filesystem guard: hardlinks only work within one filesystem.
	// On overlayfs destinations this fires for all files, safely disabling hardlinks.
	if !sameFilesystem(sourcePath, filepath.Dir(targetPath)) {
		return hardlinkOutcome{}, false
	}

	if err := os.Link(sourcePath, targetPath); err == nil {
		metrics.HardlinksCreatedTotal.Inc()
		s.logger.InfoContext(ctx, "created hardlink from registered inode",
			"torrent", hash,
			"file", filePath,
			"source", existingPath,
		)
		return hardlinkOutcome{
			state:      hlStateComplete,
			sourcePath: existingPath,
		}, true
	} else if !os.IsExist(err) {
		metrics.HardlinkErrorsTotal.WithLabelValues(metrics.ModeDestination).Inc()
		s.logger.WarnContext(ctx, "failed to create hardlink from registered inode",
			"torrent", hash,
			"file", filePath,
			"source", existingPath,
			"error", err,
		)
		return hardlinkOutcome{err: err}, true
	}

	// File already exists - not our responsibility
	return hardlinkOutcome{}, false
}

// tryHardlinkFromInProgress checks if another torrent is writing this inode.
// Returns the outcome and true if the inode is being written by another torrent.
func (s *Server) tryHardlinkFromInProgress(
	ctx context.Context,
	hash, filePath, targetPath string,
	sourceFileID FileID,
) (hardlinkOutcome, bool) {
	sourceRelPath, doneCh, sourceTorrent, found := s.store.Inodes().GetInProgress(sourceFileID)
	if !found {
		return hardlinkOutcome{}, false
	}

	// Same-filesystem guard: if the in-progress writer's target lives on a
	// different filesystem from this file's target, the eventual hardlink at
	// finalize would fail with EXDEV. Skip pending and fall through so the
	// data gets streamed independently.
	sourceDir := filepath.Dir(filepath.Join(s.config.BasePath, sourceRelPath))
	if !sameFilesystem(sourceDir, filepath.Dir(targetPath)) {
		return hardlinkOutcome{}, false
	}

	s.logger.InfoContext(ctx, "file pending hardlink from concurrent torrent",
		"torrent", hash,
		"file", filePath,
		"source_torrent", sourceTorrent,
	)

	return hardlinkOutcome{
		state:      hlStatePending,
		sourcePath: sourceRelPath,
		doneCh:     doneCh,
	}, true
}

// sameFilesystem reports whether two paths reside on the same filesystem
// by comparing the device IDs of their stat results. Returns false on any
// stat error or if device IDs cannot be obtained — callers treat that as
// "not safe to hardlink".
func sameFilesystem(pathA, pathB string) bool {
	infoA, errA := os.Stat(pathA)
	if errA != nil {
		return false
	}
	infoB, errB := os.Stat(pathB)
	if errB != nil {
		return false
	}
	statA, okA := infoA.Sys().(*syscall.Stat_t)
	statB, okB := infoB.Sys().(*syscall.Stat_t)
	if !okA || !okB {
		return false
	}
	return uint64(statA.Dev) == uint64(statB.Dev)
}

// registerInodeInProgress registers this torrent as the first writer for an inode.
// Returns true if registration succeeded, false if another torrent already registered.
func (s *Server) registerInodeInProgress(
	ctx context.Context,
	hash, filePath, targetPath string,
	sourceFileID FileID,
) bool {
	relTargetPath, err := filepath.Rel(s.config.BasePath, targetPath)
	if err != nil {
		s.logger.WarnContext(ctx, "failed to compute relative path for in-progress inode",
			"torrent", hash,
			"targetPath", targetPath,
			"basePath", s.config.BasePath,
			"error", err,
		)
		relTargetPath = filePath
	}

	return s.store.Inodes().RegisterInProgress(sourceFileID, hash, relTargetPath)
}
