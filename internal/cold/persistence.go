package cold

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
	for i, b := range data {
		if i >= numPieces {
			break
		}
		written[i] = b == 1
	}
	return written, nil
}

// atomicWriteFile delegates to utils.AtomicWriteFile with standard server permissions.
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

// doSaveState calls saveStateFunc if set (testing), otherwise saveState.
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
// Returns nil if the file is missing (treat as all-selected for backward compat).
func loadSelectedFile(metaDir string, numFiles int) []bool {
	data, err := os.ReadFile(filepath.Join(metaDir, selectedFileName))
	if err != nil {
		return nil
	}
	selected := make([]bool, numFiles)
	for i := range selected {
		if i < len(data) {
			selected[i] = data[i] == 1
		}
	}
	return selected
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

	// Load written pieces state
	statePath := filepath.Join(metaDir, ".state")
	written, stateErr := s.loadState(statePath, parsed.NumPieces)
	if stateErr != nil {
		return nil, fmt.Errorf("loading state: %w", stateErr)
	}

	// Load save sub-path
	saveSubPath := loadSubPathFile(metaDir)

	// Reconstruct file paths from torrent metadata + saveSubPath.
	// Use the finalized path if it exists, otherwise assume partial.
	files := make([]*serverFileInfo, len(parsed.Files))
	for i, f := range parsed.Files {
		finalPath := filepath.Join(s.config.BasePath, saveSubPath, f.Path)
		diskPath := finalPath + partialSuffix
		if _, err := os.Stat(finalPath); err == nil {
			diskPath = finalPath
		}
		files[i] = &serverFileInfo{
			path:     diskPath,
			size:     f.Size,
			offset:   f.Offset,
			selected: true, // Default; overridden below if .selected file exists
		}
	}

	// Restore file selection from persisted .selected file.
	selectedBitmap := loadSelectedFile(metaDir, len(files))
	if selectedBitmap != nil {
		for i, fi := range files {
			fi.selected = selectedBitmap[i]
		}
	}

	// Compute per-file piece ranges and mark files that need no streamed data as already finalized.
	// During recovery, files at final path (not .partial) or unselected files are already done.
	computeFilePieceRanges(files, parsed.PieceLength, parsed.TotalSize)
	for _, fi := range files {
		if !strings.HasSuffix(fi.path, partialSuffix) || !fi.selected {
			fi.earlyFinalized = true
		}
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
		info:         nil, // Not needed for finalization
		written:      written,
		writtenCount: writtenCount,
		pieceHashes:  parsed.PieceHashes,
		pieceLength:  parsed.PieceLength,
		totalSize:    parsed.TotalSize,
		files:        files,
		torrentPath:  torrentPath,
		statePath:    statePath,
		saveSubPath:  saveSubPath,
	}, nil
}
