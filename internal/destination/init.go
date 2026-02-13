package destination

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

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
	// Clean up ALL prior state (qB entry, in-memory state, persisted .state)
	// in one place before any other checks.
	if req.GetResync() && len(req.GetFiles()) > 0 {
		if err := s.cleanupForResync(ctx, hash); err != nil {
			return initErrorResponse("re-sync cleanup: %v", err), nil
		}
	}

	// Check if torrent is already complete/verifying in destination qBittorrent.
	if resp := s.checkQBCompletion(ctx, hash); resp != nil {
		return resp, nil
	}

	s.mu.Lock()

	// Check for existing state (resume case).
	if state, exists := s.torrents[hash]; exists {
		if state.initializing {
			s.mu.Unlock()
			return initErrorResponse("torrent initialization already in progress"), nil
		}
		s.mu.Unlock()
		return s.resumeTorrent(ctx, hash, state, req), nil
	}

	// Don't create state for minimal requests (e.g., CheckTorrentStatus sends only hash).
	// Return -1 to indicate torrent not initialized yet - caller should do full InitTorrent.
	if len(req.GetFiles()) == 0 {
		s.mu.Unlock()
		return &pb.InitTorrentResponse{
			Success:           true,
			Status:            pb.TorrentSyncStatus_SYNC_STATUS_READY,
			PiecesNeededCount: -1,
		}, nil
	}

	// Insert sentinel to reserve the hash while we do disk I/O without holding s.mu.
	// This prevents concurrent InitTorrent calls for the same hash from racing.
	s.torrents[hash] = &serverTorrentState{initializing: true}
	s.mu.Unlock()

	// Initialize new torrent (disk I/O happens here without s.mu held)
	resp := s.initNewTorrent(ctx, hash, req)

	// On error, clean up the sentinel
	if !resp.GetSuccess() {
		s.mu.Lock()
		// Only delete if still our sentinel (should always be true)
		if state, exists := s.torrents[hash]; exists && state.initializing {
			delete(s.torrents, hash)
		}
		s.mu.Unlock()
	}

	return resp, nil
}

// resumeTorrent handles resuming an existing torrent sync.
func (s *Server) resumeTorrent(
	ctx context.Context,
	hash string,
	state *serverTorrentState,
	req *pb.InitTorrentRequest,
) *pb.InitTorrentResponse {
	// Ensure torrent file is written (may have been missed by CheckTorrentStatus)
	if err := s.ensureTorrentFileWritten(ctx, hash, state, req); err != nil {
		return initErrorResponse("ensuring torrent file: %v", err)
	}

	// Acquire state.mu to read written/hardlinkResults safely — a concurrent
	// WritePiece may be modifying state.written under this lock.
	state.mu.Lock()
	resp := state.buildReadyResponse()
	state.mu.Unlock()
	return resp
}

// setupMetadataDir creates the metadata directory and writes the torrent file.
// If the directory exists with a stale or missing version, it is removed first
// to prevent loading incompatible state files.
// Returns metaDir, torrentPath, statePath, and error.
func (s *Server) setupMetadataDir(
	hash, name string,
	torrentFile []byte,
) (string, string, string, error) {
	metaDir := filepath.Join(s.config.BasePath, metaDirName, hash)

	// Nuke stale metadata directory before re-creating.
	if !checkMetaVersion(metaDir) {
		_ = os.RemoveAll(metaDir)
	}

	if err := os.MkdirAll(metaDir, serverDirPermissions); err != nil {
		return "", "", "", fmt.Errorf("creating metadata directory: %w", err)
	}

	torrentPath := filepath.Join(metaDir, filepath.Base(name)+".torrent")
	statePath := filepath.Join(metaDir, ".state")

	if len(torrentFile) > 0 {
		if err := atomicWriteFile(torrentPath, torrentFile); err != nil {
			return "", "", "", fmt.Errorf("writing .torrent file: %w", err)
		}
	}

	if err := atomicWriteFile(filepath.Join(metaDir, versionFileName), []byte(metaVersion)); err != nil {
		return "", "", "", fmt.Errorf("writing version file: %w", err)
	}

	return metaDir, torrentPath, statePath, nil
}

// initNewTorrent handles initializing a fresh torrent sync.
func (s *Server) initNewTorrent(
	ctx context.Context,
	hash string,
	req *pb.InitTorrentRequest,
) *pb.InitTorrentResponse {
	name := req.GetName()

	// Create metadata directory and write torrent file
	metaDir, torrentPath, statePath, err := s.setupMetadataDir(hash, name, req.GetTorrentFile())
	if err != nil {
		return initErrorResponse("%v", err)
	}

	// Check for existing files at a different sub-path and relocate if needed.
	// This handles destination restart: persisted state may have files at the old path
	// (e.g., no category prefix), while source now sends the correct sub-path.
	saveSubPath := req.GetSaveSubPath()
	if oldSubPath := loadSubPathFile(metaDir); oldSubPath != saveSubPath {
		relPaths := make([]string, len(req.GetFiles()))
		for i, f := range req.GetFiles() {
			relPaths[i] = f.GetPath()
		}
		if _, relocErr := s.relocateFiles(ctx, hash, relPaths, oldSubPath, saveSubPath); relocErr != nil {
			s.logger.WarnContext(ctx, "failed to relocate files, continuing with fresh setup",
				"hash", hash, "error", relocErr)
		}
	}

	// Set up files and check for hardlink opportunities
	files, hardlinkResults, err := s.setupFiles(ctx, hash, req.GetFiles(), saveSubPath)
	if err != nil {
		return initErrorResponse("%v", err)
	}

	numPieces := req.GetNumPieces()
	pieceSize := req.GetPieceSize()
	totalSize := req.GetTotalSize()

	// Build written bitmap from persisted state + hardlink coverage.
	written := s.buildWrittenBitmap(ctx, hash, statePath, files, numPieces, pieceSize, totalSize)

	// Persist initial written state so pre-existing/hardlinked piece coverage
	// survives a crash before the first WritePiece triggers a periodic flush.
	if initialHave := countWritten(written); initialHave > 0 {
		if saveErr := s.doSaveState(statePath, written); saveErr != nil {
			s.logger.WarnContext(ctx, "failed to persist initial state",
				"hash", hash, "error", saveErr)
		}
	}

	// Persist save sub-path and file selection for recovery after restart.
	// The .torrent file (already written above) provides all other metadata.
	if saveErr := saveSubPathFile(metaDir, saveSubPath); saveErr != nil {
		return initErrorResponse("failed to save sub-path for recovery: %v", saveErr)
	}
	if saveErr := saveSelectedFile(metaDir, files); saveErr != nil {
		return initErrorResponse("failed to save selection state: %v", saveErr)
	}

	// Swap sentinel for real state under s.mu
	state := &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceHashes: req.GetPieceHashes(),
			pieceLength: pieceSize,
			totalSize:   totalSize,
			files:       files,
		},
		info:            req,
		written:         written,
		writtenCount:    countWritten(written),
		torrentPath:     torrentPath,
		statePath:       statePath,
		saveSubPath:     saveSubPath,
		hardlinkResults: hardlinkResults,
	}
	s.mu.Lock()
	s.torrents[hash] = state
	s.mu.Unlock()

	// Log summary
	hardlinkedCount, pendingCount, preExistingCount := countHardlinkResults(hardlinkResults)
	_, needCount, haveCount := calculatePiecesNeeded(written)
	selectedCount := state.countSelectedFiles()

	s.logger.InfoContext(ctx, "initialized torrent",
		"hash", hash,
		"name", name,
		"pieces", numPieces,
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

// buildWrittenBitmap constructs the written bitmap from persisted state and hardlink
// coverage, and initializes per-file piece tracking for early finalization.
func (s *Server) buildWrittenBitmap(
	ctx context.Context,
	hash, statePath string,
	files []*serverFileInfo,
	numPieces int32,
	pieceSize, totalSize int64,
) []bool {
	piecesCovered := calculatePiecesCovered(files, numPieces, pieceSize, totalSize)

	written := make([]bool, numPieces)
	if existingState, loadErr := s.loadState(statePath, int(numPieces)); loadErr == nil {
		written = existingState
		_, _, haveCount := calculatePiecesNeeded(written)
		s.logger.InfoContext(ctx, "resumed torrent state",
			"hash", hash,
			"written", haveCount,
			"total", numPieces,
		)
	}

	for i, covered := range piecesCovered {
		if covered {
			written[i] = true
		}
	}

	// Compute per-file piece ranges and mark files that need no streamed data as already finalized.
	computeFilePieceRanges(files, pieceSize, totalSize)
	for _, fi := range files {
		if fi.skipForWriteData() {
			fi.earlyFinalized = true
		}
	}
	initFilePieceCounts(files, written)

	return written
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

	// 2. Delete in-memory state.
	s.mu.Lock()
	delete(s.torrents, hash)
	s.mu.Unlock()

	// 3. Delete persisted .state so buildWrittenBitmap starts fresh.
	// During the first sync with partial selection, boundary pieces spanning
	// selected + unselected files were marked "written" but only the
	// selected-file portion was actually written to disk. On re-sync those
	// pieces must be re-streamed for the newly-selected portions.
	stateFile := filepath.Join(s.config.BasePath, metaDirName, hash, ".state")
	_ = os.Remove(stateFile)

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

	if isCheckingState(torrent.State) {
		return qbCheckVerifying
	}

	if torrent.Progress >= 1.0 && isReadyState(torrent.State) {
		s.logger.InfoContext(ctx, "torrent already complete in qBittorrent",
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
	s.mu.RLock()
	abortCh, aborting := s.abortingHashes[hash]
	s.mu.RUnlock()

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

// ensureTorrentFileWritten writes the torrent file if it doesn't exist on disk.
// This handles the case where CheckTorrentStatus created state without the file.
func (s *Server) ensureTorrentFileWritten(
	ctx context.Context,
	hash string,
	state *serverTorrentState,
	req *pb.InitTorrentRequest,
) error {
	torrentFile := req.GetTorrentFile()
	if len(torrentFile) == 0 {
		return nil
	}

	// Use existing path if valid, otherwise construct from request.
	// Read under state.mu: torrentPath can be written concurrently by another
	// resumeTorrent call for the same hash.
	state.mu.Lock()
	torrentPath := state.torrentPath
	state.mu.Unlock()
	if torrentPath == "" {
		name := req.GetName()
		if name == "" {
			return nil
		}
		name = filepath.Base(name) // Sanitize to prevent path traversal
		torrentPath = filepath.Join(s.config.BasePath, metaDirName, hash, name+".torrent")
	}

	// Check if file already exists
	if _, err := os.Stat(torrentPath); err == nil {
		return nil
	}

	// Ensure directory exists and write file
	if err := os.MkdirAll(filepath.Dir(torrentPath), serverDirPermissions); err != nil {
		return fmt.Errorf("creating metadata directory: %w", err)
	}

	if err := atomicWriteFile(torrentPath, torrentFile); err != nil {
		return fmt.Errorf("writing torrent file: %w", err)
	}

	state.mu.Lock()
	state.torrentPath = torrentPath
	state.mu.Unlock()
	s.logger.InfoContext(ctx, "wrote torrent file for existing state", "hash", hash, "path", torrentPath)
	return nil
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

	// Unselected files: no .partial, no directory creation, no hardlink resolution.
	// The serverFileInfo entry is still needed for offset tracking in writePieceData.
	if !f.GetSelected() {
		return &serverFileInfo{
			path:     targetPath,
			size:     f.GetSize(),
			offset:   f.GetOffset(),
			hl:       hardlinkInfo{sourceInode: Inode(f.GetInode())},
			selected: false,
		}, result, nil
	}

	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(targetPath), serverDirPermissions); err != nil {
		return nil, nil, fmt.Errorf("creating directory for %s: %w", f.GetPath(), err)
	}

	// Check if file already exists with correct size (pre-existing data)
	if info, statErr := os.Stat(targetPath); statErr == nil && info.Size() == f.GetSize() {
		fileInfo := &serverFileInfo{
			path:     targetPath,
			size:     f.GetSize(),
			offset:   f.GetOffset(),
			hl:       hardlinkInfo{sourceInode: Inode(f.GetInode()), state: hlStateComplete},
			selected: true,
		}
		result.PreExisting = true
		return fileInfo, result, nil
	}

	// Check if .partial file exists from a previous interrupted sync.
	// Piece-level progress is recovered from .state in buildWrittenBitmap;
	// skip hardlink resolution since the file already has data on disk.
	partialPath := targetPath + ".partial"
	if info, statErr := os.Stat(partialPath); statErr == nil && info.Size() == f.GetSize() {
		return &serverFileInfo{
			path:     partialPath,
			size:     f.GetSize(),
			offset:   f.GetOffset(),
			hl:       hardlinkInfo{sourceInode: Inode(f.GetInode())},
			selected: true,
		}, result, nil
	}

	sourceInode := Inode(f.GetInode())
	fileInfo := &serverFileInfo{
		path:     partialPath,
		size:     f.GetSize(),
		offset:   f.GetOffset(),
		hl:       hardlinkInfo{sourceInode: sourceInode},
		selected: true,
	}

	// Check for hardlink opportunities if inode is provided
	if sourceInode != 0 {
		outcome := s.resolveHardlink(ctx, hash, f.GetPath(), targetPath, sourceInode)
		s.applyHardlinkOutcome(fileInfo, result, targetPath, outcome)
	}

	return fileInfo, result, nil
}

// resolveHardlink attempts to create or schedule a hardlink for the file.
func (s *Server) resolveHardlink(
	ctx context.Context,
	hash, filePath, targetPath string,
	sourceInode Inode,
) hardlinkOutcome {
	// Case 1: Check if inode is already registered (completed file from previous torrent)
	if outcome, ok := s.tryHardlinkFromRegistered(ctx, hash, filePath, targetPath, sourceInode); ok {
		return outcome
	}

	// Case 2: Check if another torrent is currently writing this inode
	if outcome, ok := s.tryHardlinkFromInProgress(ctx, hash, filePath, sourceInode); ok {
		return outcome
	}

	// Case 3: We're the first to write this inode - register as in-progress
	s.registerInodeInProgress(ctx, hash, filePath, targetPath, sourceInode)
	return hardlinkOutcome{state: hlStateInProgress}
}

// applyHardlinkOutcome applies the hardlink resolution outcome to the file info and result.
func (s *Server) applyHardlinkOutcome(
	fileInfo *serverFileInfo,
	result *pb.HardlinkResult,
	targetPath string,
	outcome hardlinkOutcome,
) {
	fileInfo.hl.state = outcome.state

	switch outcome.state {
	case hlStateComplete:
		result.Hardlinked = true
		result.SourcePath = outcome.sourcePath
		fileInfo.path = targetPath
	case hlStatePending:
		result.Pending = true
		result.SourcePath = outcome.sourcePath
		fileInfo.hl.sourcePath = outcome.sourcePath
		fileInfo.hl.doneCh = outcome.doneCh
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
func (s *Server) tryHardlinkFromRegistered(
	ctx context.Context,
	hash, filePath, targetPath string,
	sourceInode Inode,
) (hardlinkOutcome, bool) {
	existingPath, found := s.inodes.GetRegistered(sourceInode)
	if !found {
		return hardlinkOutcome{}, false
	}

	sourcePath := filepath.Join(s.config.BasePath, existingPath)
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
	hash, filePath string,
	sourceInode Inode,
) (hardlinkOutcome, bool) {
	targetPath, doneCh, sourceTorrent, found := s.inodes.GetInProgress(sourceInode)
	if !found {
		return hardlinkOutcome{}, false
	}

	s.logger.InfoContext(ctx, "file pending hardlink from concurrent torrent",
		"torrent", hash,
		"file", filePath,
		"source_torrent", sourceTorrent,
	)

	return hardlinkOutcome{
		state:      hlStatePending,
		sourcePath: targetPath,
		doneCh:     doneCh,
	}, true
}

// registerInodeInProgress registers this torrent as the first writer for an inode.
func (s *Server) registerInodeInProgress(ctx context.Context, hash, filePath, targetPath string, sourceInode Inode) {
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

	s.inodes.RegisterInProgress(sourceInode, hash, relTargetPath)
}
