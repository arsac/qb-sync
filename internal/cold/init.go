package cold

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
// 1. Check cold qBittorrent - if complete, return SYNC_STATUS_COMPLETE.
// 2. Check active sync state - return remaining pieces_needed.
// 3. Fresh torrent - initialize tracking and return all pieces_needed.
func (s *Server) InitTorrent(
	ctx context.Context,
	req *pb.InitTorrentRequest,
) (*pb.InitTorrentResponse, error) {
	hash := req.GetTorrentHash()

	// Wait for any in-progress abort to complete before initializing.
	if err := s.waitForAbortComplete(ctx, hash); err != nil {
		return initErrorResponse("abort in progress: %v", err), nil
	}

	// Check if torrent is already complete in cold qBittorrent
	switch s.checkTorrentInQB(ctx, hash) {
	case qbCheckComplete:
		return &pb.InitTorrentResponse{
			Success: true,
			Status:  pb.TorrentSyncStatus_SYNC_STATUS_COMPLETE,
		}, nil
	case qbCheckVerifying:
		return &pb.InitTorrentResponse{
			Success: true,
			Status:  pb.TorrentSyncStatus_SYNC_STATUS_VERIFYING,
		}, nil
	case qbCheckNotFound:
		// Continue to check local state
	}

	s.mu.Lock()

	// Check for existing state (resume case)
	if state, exists := s.torrents[hash]; exists {
		if state.initializing {
			s.mu.Unlock()
			return initErrorResponse("torrent initialization already in progress"), nil
		}
		resp := s.resumeTorrent(ctx, hash, state, req)
		s.mu.Unlock()
		return resp, nil
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
	resp := buildReadyResponse(state.written, state.hardlinkResults)
	state.mu.Unlock()
	return resp
}

// setupMetadataDir creates the metadata directory and writes the torrent file.
// Returns metaDir, torrentPath, statePath, and error.
func (s *Server) setupMetadataDir(
	hash, name string,
	torrentFile []byte,
) (string, string, string, error) {
	metaDir := filepath.Join(s.config.BasePath, metaDirName, hash)
	if err := os.MkdirAll(metaDir, serverDirPermissions); err != nil {
		return "", "", "", fmt.Errorf("creating metadata directory: %w", err)
	}

	torrentPath := filepath.Join(metaDir, filepath.Base(name)+".torrent")
	statePath := filepath.Join(metaDir, ".state")

	if len(torrentFile) > 0 {
		if err := atomicWriteFile(torrentPath, torrentFile, serverFilePermissions); err != nil {
			return "", "", "", fmt.Errorf("writing .torrent file: %w", err)
		}
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

	// Set up files and check for hardlink opportunities
	files, hardlinkResults, err := s.setupFiles(ctx, hash, req.GetFiles())
	if err != nil {
		return initErrorResponse("%v", err)
	}

	numPieces := req.GetNumPieces()
	pieceSize := req.GetPieceSize()
	totalSize := req.GetTotalSize()

	// Calculate which pieces are covered by hardlinks
	piecesCovered := calculatePiecesCovered(files, numPieces, pieceSize, totalSize)

	// Try to load existing state for resume, or create fresh
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

	// Persist file info for recovery after restart.
	// If this fails, the torrent would be unrecoverable after a server restart,
	// so we return an error (caller will clean up the sentinel).
	persistedInfo := buildPersistedInfo(name, numPieces, pieceSize, totalSize, files, req.GetPieceHashes())
	if saveErr := s.saveFilesInfo(metaDir, persistedInfo); saveErr != nil {
		return initErrorResponse("failed to save files info for recovery: %v", saveErr)
	}

	// Swap sentinel for real state under s.mu
	state := &serverTorrentState{
		info:            req,
		written:         written,
		writtenCount:    countWritten(written),
		pieceHashes:     req.GetPieceHashes(),
		pieceLength:     pieceSize,
		totalSize:       totalSize,
		files:           files,
		torrentPath:     torrentPath,
		statePath:       statePath,
		hardlinkResults: hardlinkResults,
	}
	s.mu.Lock()
	s.torrents[hash] = state
	s.mu.Unlock()

	// Log summary
	hardlinkedCount, pendingCount := countHardlinkResults(hardlinkResults)
	piecesNeeded, needCount, haveCount := calculatePiecesNeeded(written)

	s.logger.InfoContext(ctx, "initialized torrent",
		"hash", hash,
		"name", name,
		"pieces", numPieces,
		"files", len(files),
		"hasHashes", len(req.GetPieceHashes()) > 0,
		"hardlinked", hardlinkedCount,
		"pending", pendingCount,
		"piecesNeeded", needCount,
		"piecesHave", haveCount,
	)

	return &pb.InitTorrentResponse{
		Success:           true,
		Status:            pb.TorrentSyncStatus_SYNC_STATUS_READY,
		PiecesNeeded:      piecesNeeded,
		HardlinkResults:   hardlinkResults,
		PiecesNeededCount: needCount,
		PiecesHaveCount:   haveCount,
	}
}

// initErrorResponse creates an error response for InitTorrent.
func initErrorResponse(format string, args ...any) *pb.InitTorrentResponse {
	return &pb.InitTorrentResponse{
		Success: false,
		Error:   fmt.Sprintf(format, args...),
	}
}

// buildReadyResponse creates a successful READY response with piece information.
func buildReadyResponse(written []bool, hardlinkResults []*pb.HardlinkResult) *pb.InitTorrentResponse {
	piecesNeeded, needCount, haveCount := calculatePiecesNeeded(written)
	return &pb.InitTorrentResponse{
		Success:           true,
		Status:            pb.TorrentSyncStatus_SYNC_STATUS_READY,
		PiecesNeeded:      piecesNeeded,
		HardlinkResults:   hardlinkResults,
		PiecesNeededCount: needCount,
		PiecesHaveCount:   haveCount,
	}
}

// checkTorrentInQB checks if the torrent already exists in cold qBittorrent.
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

	// Use existing path if valid, otherwise construct from request
	torrentPath := state.torrentPath
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

	if err := atomicWriteFile(torrentPath, torrentFile, serverFilePermissions); err != nil {
		return fmt.Errorf("writing torrent file: %w", err)
	}

	state.torrentPath = torrentPath
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
) ([]*serverFileInfo, []*pb.HardlinkResult, error) {
	files := make([]*serverFileInfo, len(reqFiles))
	results := make([]*pb.HardlinkResult, len(reqFiles))

	for i, f := range reqFiles {
		fileInfo, result, err := s.setupFile(ctx, hash, f, i)
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
) (*serverFileInfo, *pb.HardlinkResult, error) {
	result := &pb.HardlinkResult{FileIndex: int32(fileIndex)}
	targetPath := filepath.Join(s.config.BasePath, f.GetPath())

	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(targetPath), serverDirPermissions); err != nil {
		return nil, nil, fmt.Errorf("creating directory for %s: %w", f.GetPath(), err)
	}

	sourceInode := Inode(f.GetInode())
	fileInfo := &serverFileInfo{
		path:        targetPath + ".partial",
		size:        f.GetSize(),
		offset:      f.GetOffset(),
		sourceInode: sourceInode,
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
	fileInfo.hlState = outcome.state

	switch outcome.state {
	case hlStateComplete:
		result.Hardlinked = true
		result.SourcePath = outcome.sourcePath
		fileInfo.path = targetPath
	case hlStatePending:
		result.Pending = true
		result.SourcePath = outcome.sourcePath
		fileInfo.hardlinkSource = outcome.sourcePath
		fileInfo.hardlinkDoneCh = outcome.doneCh
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
