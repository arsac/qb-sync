package cold

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	pb "github.com/arsac/qb-sync/proto"
)

// initErrorResponse creates an error response for InitTorrent.
func initErrorResponse(format string, args ...any) *pb.InitTorrentResponse {
	return &pb.InitTorrentResponse{
		Success: false,
		Error:   fmt.Sprintf(format, args...),
	}
}

// calculatePiecesCovered determines which pieces are fully covered by hardlinked or pending files.
func calculatePiecesCovered(files []*serverFileInfo, numPieces int32, pieceSize, totalSize int64) []bool {
	piecesCovered := make([]bool, numPieces)
	for pieceIdx := range numPieces {
		pieceStart := int64(pieceIdx) * pieceSize
		pieceEnd := min(pieceStart+pieceSize, totalSize)

		// Piece is covered if all overlapping files are hardlinked or pending
		covered := true
		for _, f := range files {
			fileStart := f.offset
			fileEnd := f.offset + f.size

			// Check if file overlaps with piece
			if fileStart < pieceEnd && fileEnd > pieceStart {
				if !f.hardlinked && !f.pendingHardlink {
					covered = false
					break
				}
			}
		}
		piecesCovered[pieceIdx] = covered
	}
	return piecesCovered
}

// setupFile sets up a single file, checking for hardlink opportunities.
// Returns the serverFileInfo and HardlinkResult for the file.
// Must be called with s.mu held (for logging context only - does not modify s.torrents).
func (s *Server) setupFile(
	ctx context.Context,
	torrentHash string,
	f *pb.FileInfo,
	fileIndex int,
) (*serverFileInfo, *pb.HardlinkResult, error) {
	result := &pb.HardlinkResult{FileIndex: int32(fileIndex)}
	sourceInode := f.GetInode()
	targetPath := filepath.Join(s.config.BasePath, f.GetPath())
	partialPath := targetPath + ".partial"

	// Ensure parent directory exists
	if mkdirErr := os.MkdirAll(filepath.Dir(targetPath), serverDirPermissions); mkdirErr != nil {
		return nil, nil, fmt.Errorf("failed to create directory for %s: %w", f.GetPath(), mkdirErr)
	}

	fileInfo := &serverFileInfo{
		path:        partialPath,
		size:        f.GetSize(),
		offset:      f.GetOffset(),
		file:        nil, // Lazy open
		sourceInode: sourceInode,
	}

	// Skip hardlink detection if no inode provided
	if sourceInode == 0 {
		return fileInfo, result, nil
	}

	// Case 1: Check if inode is already registered (completed file from previous torrent)
	s.inodeMu.RLock()
	existingPath, found := s.inodeToPath[sourceInode]
	s.inodeMu.RUnlock()

	if found {
		sourcePath := filepath.Join(s.config.BasePath, existingPath)
		if linkErr := os.Link(sourcePath, targetPath); linkErr == nil {
			result.Hardlinked = true
			result.SourcePath = existingPath
			fileInfo.hardlinked = true
			fileInfo.path = targetPath // Use final path, not .partial
			s.logger.InfoContext(ctx, "created hardlink from registered inode",
				"torrent", torrentHash,
				"file", f.GetPath(),
				"source", existingPath,
			)
			return fileInfo, result, nil
		} else if !os.IsExist(linkErr) {
			s.logger.WarnContext(ctx, "failed to create hardlink from registered inode",
				"torrent", torrentHash,
				"file", f.GetPath(),
				"source", existingPath,
				"error", linkErr,
			)
			result.Error = linkErr.Error()
		}
		// If target already exists, check if it's the same inode (idempotent case)
		// Fall through to normal handling
	}

	// Case 2: Check if another torrent is currently writing this inode
	s.inodeProgressMu.Lock()
	if inProgress, ok := s.inodeInProgress[sourceInode]; ok {
		// Another torrent is writing this file - mark as pending hardlink
		fileInfo.pendingHardlink = true
		fileInfo.hardlinkSource = inProgress.targetPath
		fileInfo.hardlinkDoneCh = inProgress.doneCh
		fileInfo.path = targetPath // Final path, not .partial
		s.inodeProgressMu.Unlock()

		result.Pending = true
		result.SourcePath = inProgress.targetPath
		s.logger.InfoContext(ctx, "file pending hardlink from concurrent torrent",
			"torrent", torrentHash,
			"file", f.GetPath(),
			"source_torrent", inProgress.torrentHash,
		)
		return fileInfo, result, nil
	}

	// Case 3: We're the first to write this inode - register in-progress
	doneCh := make(chan struct{})
	relTargetPath, relErr := filepath.Rel(s.config.BasePath, targetPath)
	if relErr != nil {
		s.logger.WarnContext(ctx, "failed to compute relative path for in-progress inode",
			"torrent", torrentHash,
			"targetPath", targetPath,
			"basePath", s.config.BasePath,
			"error", relErr,
		)
		relTargetPath = f.GetPath()
	}
	s.inodeInProgress[sourceInode] = &inProgressInode{
		targetPath:  relTargetPath,
		doneCh:      doneCh,
		torrentHash: torrentHash,
	}
	s.inodeProgressMu.Unlock()

	return fileInfo, result, nil
}

// applyPiecesCoverage marks pieces covered by hardlinks as written.
// Returns the number of pieces marked as covered.
func applyPiecesCoverage(written, piecesCovered []bool) int {
	coveredCount := 0
	for i, covered := range piecesCovered {
		if covered {
			written[i] = true
			coveredCount++
		}
	}
	return coveredCount
}

// buildPersistedInfo creates a persistedTorrentInfo from file info.
func buildPersistedInfo(
	name string,
	numPieces int32,
	pieceSize, totalSize int64,
	files []*serverFileInfo,
) *persistedTorrentInfo {
	persistedFiles := make([]persistedFileInfo, len(files))
	for i, f := range files {
		persistedFiles[i] = persistedFileInfo{
			Path:   f.path,
			Size:   f.size,
			Offset: f.offset,
		}
	}
	return &persistedTorrentInfo{
		Name:        name,
		NumPieces:   int(numPieces),
		PieceLength: pieceSize,
		TotalSize:   totalSize,
		Files:       persistedFiles,
	}
}

// countHardlinkResults counts hardlinked and pending files from results.
func countHardlinkResults(results []*pb.HardlinkResult) (int, int) {
	hardlinked, pending := 0, 0
	for _, r := range results {
		if r.GetHardlinked() {
			hardlinked++
		}
		if r.GetPending() {
			pending++
		}
	}
	return hardlinked, pending
}

// waitForAbortComplete waits for any in-progress abort of the given hash to complete.
// This prevents a race condition where InitTorrent creates files that AbortTorrent then deletes.
func (s *Server) waitForAbortComplete(ctx context.Context, hash string) error {
	s.mu.RLock()
	abortCh, aborting := s.abortingHashes[hash]
	s.mu.RUnlock()

	if !aborting {
		return nil
	}

	s.logger.DebugContext(ctx, "waiting for abort to complete before init",
		"hash", hash,
	)

	select {
	case <-abortCh:
		// Abort completed, safe to proceed
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// InitTorrent initializes tracking for a new torrent.
func (s *Server) InitTorrent(
	ctx context.Context,
	req *pb.InitTorrentRequest,
) (*pb.InitTorrentResponse, error) {
	torrentHash := req.GetTorrentHash()

	// Wait for any in-progress abort to complete before initializing.
	// This prevents a race where AbortTorrent deletes files that we just created.
	if err := s.waitForAbortComplete(ctx, torrentHash); err != nil {
		return initErrorResponse("abort in progress: %v", err), nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if state, exists := s.torrents[torrentHash]; exists {
		// Return cached results from previous initialization
		return &pb.InitTorrentResponse{
			Success:         true,
			HardlinkResults: state.hardlinkResults,
			PiecesCovered:   state.piecesCovered,
		}, nil
	}

	// Create metadata directory (separate from content to avoid polluting torrent data)
	// Content files go directly in BasePath to match qBittorrent's save_path
	metaDir := filepath.Join(s.config.BasePath, ".meta", torrentHash)
	if mkdirErr := os.MkdirAll(metaDir, serverDirPermissions); mkdirErr != nil {
		return initErrorResponse("failed to create metadata directory: %v", mkdirErr), nil
	}

	name := req.GetName()
	torrentPath := filepath.Join(metaDir, name+".torrent")
	statePath := filepath.Join(metaDir, ".state")

	// Store the .torrent file for later use by qBittorrent
	torrentFile := req.GetTorrentFile()
	if len(torrentFile) > 0 {
		if writeErr := os.WriteFile(torrentPath, torrentFile, serverFilePermissions); writeErr != nil {
			return initErrorResponse("failed to write .torrent file: %v", writeErr), nil
		}
	}

	// Set up file info for each file in the torrent, checking for hardlink opportunities
	reqFiles := req.GetFiles()
	files := make([]*serverFileInfo, len(reqFiles))
	hardlinkResults := make([]*pb.HardlinkResult, len(reqFiles))

	for i, f := range reqFiles {
		fileInfo, result, err := s.setupFile(ctx, torrentHash, f, i)
		if err != nil {
			return initErrorResponse("%v", err), nil
		}
		files[i] = fileInfo
		hardlinkResults[i] = result
	}

	numPieces := req.GetNumPieces()
	pieceHashes := req.GetPieceHashes()
	pieceSize := req.GetPieceSize()
	totalSize := req.GetTotalSize()

	// Calculate which pieces are fully covered by hardlinked or pending files
	piecesCovered := calculatePiecesCovered(files, numPieces, pieceSize, totalSize)

	// Try to load existing state for resume
	written := make([]bool, numPieces)
	if existingState, loadErr := s.loadState(statePath, int(numPieces)); loadErr == nil {
		written = existingState
		s.logger.InfoContext(ctx, "resumed torrent state",
			"hash", torrentHash,
			"written", countWritten(written),
			"total", numPieces,
		)
	}

	// Mark pieces covered by hardlinks as written
	coveredCount := applyPiecesCoverage(written, piecesCovered)

	s.torrents[torrentHash] = &serverTorrentState{
		info:            req,
		written:         written,
		pieceHashes:     pieceHashes,
		files:           files,
		torrentPath:     torrentPath,
		statePath:       statePath,
		hardlinkResults: hardlinkResults,
		piecesCovered:   piecesCovered,
	}

	// Persist file info for recovery after restart
	persistedInfo := buildPersistedInfo(name, numPieces, pieceSize, totalSize, files)
	if saveErr := s.saveFilesInfo(metaDir, persistedInfo); saveErr != nil {
		s.logger.WarnContext(ctx, "failed to save files info for recovery",
			"hash", torrentHash,
			"error", saveErr,
		)
	}

	// Count hardlinked files for logging
	hardlinkedCount, pendingCount := countHardlinkResults(hardlinkResults)

	s.logger.InfoContext(ctx, "initialized torrent",
		"hash", torrentHash,
		"name", name,
		"pieces", numPieces,
		"files", len(files),
		"hasHashes", len(pieceHashes) > 0,
		"hardlinked", hardlinkedCount,
		"pending", pendingCount,
		"piecesCovered", coveredCount,
	)

	return &pb.InitTorrentResponse{
		Success:         true,
		HardlinkResults: hardlinkResults,
		PiecesCovered:   piecesCovered,
	}, nil
}
