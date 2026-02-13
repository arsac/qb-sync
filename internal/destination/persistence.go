package destination

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/arsac/qb-sync/internal/utils"
)

// loadState loads the written pieces state from disk.
func (s *Server) loadState(path string, numPieces int) ([]bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	written := make([]bool, numPieces)
	for i := range min(numPieces, len(data)) {
		written[i] = data[i] == 1
	}
	return written, nil
}

// atomicWriteFile writes data atomically using standard server permissions.
func atomicWriteFile(path string, data []byte) error {
	return utils.AtomicWriteFile(path, data, serverFilePermissions)
}

// saveState persists the written pieces state to disk.
func (s *Server) saveState(path string, written []bool) error {
	data := make([]byte, len(written))
	for i, w := range written {
		if w {
			data[i] = 1
		}
	}
	return atomicWriteFile(path, data)
}

// doSaveState persists state using saveStateFunc (injected for tests) or the default saveState.
func (s *Server) doSaveState(path string, written []bool) error {
	if s.saveStateFunc != nil {
		return s.saveStateFunc(path, written)
	}
	return s.saveState(path, written)
}

// saveSubPathFile persists the save sub-path to a .subpath file in the metadata directory.
func saveSubPathFile(metaDir, subPath string) error {
	if subPath == "" {
		return nil
	}
	path := filepath.Join(metaDir, subPathFileName)
	return atomicWriteFile(path, []byte(subPath))
}

// loadSubPathFile reads the save sub-path from the .subpath file.
// Returns "" if the file is missing or unreadable.
func loadSubPathFile(metaDir string) string {
	path := filepath.Join(metaDir, subPathFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// saveSelectedFile persists the file selection bitmap to a .selected file.
func saveSelectedFile(metaDir string, files []*serverFileInfo) error {
	data := make([]byte, len(files))
	for i, fi := range files {
		if fi.selected {
			data[i] = 1
		}
	}
	return atomicWriteFile(filepath.Join(metaDir, selectedFileName), data)
}

// loadSelectedFile reads the file selection bitmap from the .selected file.
// Returns nil if the file is missing (callers default to all-selected).
func loadSelectedFile(metaDir string, numFiles int) []bool {
	data, err := os.ReadFile(filepath.Join(metaDir, selectedFileName))
	if err != nil {
		return nil
	}
	selected := make([]bool, numFiles)
	for i := range min(numFiles, len(data)) {
		selected[i] = data[i] == 1
	}
	return selected
}

// checkMetaVersion returns true if the metadata directory has the current version.
func checkMetaVersion(metaDir string) bool {
	data, err := os.ReadFile(filepath.Join(metaDir, versionFileName))
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(data)) == metaVersion
}

// findTorrentFile locates the .torrent file in metaDir.
func findTorrentFile(metaDir string) (string, error) {
	entries, readErr := os.ReadDir(metaDir)
	if readErr != nil {
		return "", fmt.Errorf("reading meta dir: %w", readErr)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".torrent") {
			return filepath.Join(metaDir, entry.Name()), nil
		}
	}
	return "", errors.New("torrent file not found")
}

// recoverTorrentState attempts to recover torrent state from disk after a server restart.
// This enables FinalizeTorrent to work even if the torrent was initialized in a previous
// server instance. Parses the .torrent file directly for metadata.
func (s *Server) recoverTorrentState(ctx context.Context, hash string) (*serverTorrentState, error) {
	metaDir := filepath.Join(s.config.BasePath, metaDirName, hash)

	if !checkMetaVersion(metaDir) {
		s.logger.WarnContext(ctx, "stale metadata version, nuking directory",
			"hash", hash, "expected", metaVersion)
		if removeErr := os.RemoveAll(metaDir); removeErr != nil {
			s.logger.ErrorContext(ctx, "failed to remove stale metadata",
				"hash", hash, "error", removeErr)
		}
		return nil, fmt.Errorf("stale metadata for %s (removed)", hash)
	}

	// Find and parse the .torrent file
	torrentPath, findErr := findTorrentFile(metaDir)
	if findErr != nil {
		return nil, findErr
	}

	torrentData, readErr := os.ReadFile(torrentPath)
	if readErr != nil {
		return nil, fmt.Errorf("reading torrent file: %w", readErr)
	}

	parsed, parseErr := parseTorrentFile(torrentData)
	if parseErr != nil {
		return nil, fmt.Errorf("parsing torrent file: %w", parseErr)
	}

	statePath := filepath.Join(metaDir, ".state")
	saveSubPath := loadSubPathFile(metaDir)

	files := s.reconstructFiles(parsed.Files, saveSubPath)
	restoreFileSelection(files, metaDir)

	computeFilePieceRanges(files, parsed.PieceLength, parsed.TotalSize)
	for _, fi := range files {
		alreadyRenamed := !strings.HasSuffix(fi.path, partialSuffix)
		if alreadyRenamed || !fi.selected {
			fi.earlyFinalized = true
		}
	}

	written, loadErr := s.loadOrReconstructState(ctx, hash, statePath, files, parsed.NumPieces)
	if loadErr != nil {
		return nil, loadErr
	}

	initFilePieceCounts(files, written)

	writtenCount := countWritten(written)

	s.logger.InfoContext(ctx, "recovering torrent state",
		"hash", hash,
		"written", writtenCount,
		"total", parsed.NumPieces,
		"files", len(files),
	)

	return &serverTorrentState{
		torrentMeta: torrentMeta{
			pieceHashes: parsed.PieceHashes,
			pieceLength: parsed.PieceLength,
			totalSize:   parsed.TotalSize,
			files:       files,
		},
		written:      written,
		writtenCount: writtenCount,
		torrentPath:  torrentPath,
		statePath:    statePath,
		saveSubPath:  saveSubPath,
	}, nil
}

// reconstructFiles builds serverFileInfo entries from torrent metadata.
// Uses the finalized path if the file exists on disk, otherwise assumes .partial.
func (s *Server) reconstructFiles(parsedFiles []parsedFile, saveSubPath string) []*serverFileInfo {
	files := make([]*serverFileInfo, len(parsedFiles))
	for i, f := range parsedFiles {
		finalPath := filepath.Join(s.config.BasePath, saveSubPath, f.Path)
		diskPath := finalPath + partialSuffix
		if _, err := os.Stat(finalPath); err == nil {
			diskPath = finalPath
		}
		files[i] = &serverFileInfo{
			path:     diskPath,
			size:     f.Size,
			offset:   f.Offset,
			selected: true, // Default; overridden by restoreFileSelection
		}
	}
	return files
}

// restoreFileSelection applies the persisted .selected bitmap to the files.
func restoreFileSelection(files []*serverFileInfo, metaDir string) {
	selectedBitmap := loadSelectedFile(metaDir, len(files))
	if selectedBitmap == nil {
		return
	}
	for i, fi := range files {
		fi.selected = selectedBitmap[i]
	}
}

// loadOrReconstructState loads the .state file, or reconstructs the written
// bitmap from files on disk when the state file is missing.
// Files at final path (renamed from .partial) are assumed fully written.
// .partial files are conservatively treated as having no written pieces.
func (s *Server) loadOrReconstructState(
	ctx context.Context,
	hash, statePath string,
	files []*serverFileInfo,
	numPieces int,
) ([]bool, error) {
	written, stateErr := s.loadState(statePath, numPieces)
	if stateErr == nil {
		return written, nil
	}
	if !os.IsNotExist(stateErr) {
		return nil, fmt.Errorf("loading state: %w", stateErr)
	}

	s.logger.WarnContext(ctx, "state file missing, reconstructing from disk", "hash", hash)
	written = make([]bool, numPieces)

	for _, fi := range files {
		if !fi.selected || strings.HasSuffix(fi.path, partialSuffix) {
			continue
		}
		for p := fi.firstPiece; p <= fi.lastPiece; p++ {
			written[p] = true
		}
	}

	// Persist reconstructed state to prevent repeated reconstruction.
	if saveErr := s.doSaveState(statePath, written); saveErr != nil {
		s.logger.WarnContext(ctx, "failed to persist reconstructed state",
			"hash", hash, "error", saveErr)
	}

	return written, nil
}
