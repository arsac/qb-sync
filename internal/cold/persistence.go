package cold

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

// atomicWriteFile writes data to a file atomically using write-to-temp + fsync + rename.
// This prevents corruption from crashes or NFS connection drops mid-write.
//
//nolint:unparam // perm kept as parameter for API correctness even though callers currently use the same value
func atomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	tmpPath := tmp.Name()

	// Clean up temp file on any failure path
	success := false
	defer func() {
		if !success {
			_ = tmp.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	if _, writeErr := tmp.Write(data); writeErr != nil {
		return fmt.Errorf("writing temp file: %w", writeErr)
	}
	if syncErr := tmp.Sync(); syncErr != nil {
		return fmt.Errorf("syncing temp file: %w", syncErr)
	}
	if closeErr := tmp.Close(); closeErr != nil {
		return fmt.Errorf("closing temp file: %w", closeErr)
	}
	if chmodErr := os.Chmod(tmpPath, perm); chmodErr != nil {
		return fmt.Errorf("setting permissions: %w", chmodErr)
	}
	if renameErr := os.Rename(tmpPath, path); renameErr != nil {
		return fmt.Errorf("renaming temp file: %w", renameErr)
	}

	success = true
	return nil
}

// saveState persists the written pieces state to disk.
func (s *Server) saveState(path string, written []bool) error {
	data := make([]byte, len(written))
	for i, w := range written {
		if w {
			data[i] = 1
		}
	}
	return atomicWriteFile(path, data, serverFilePermissions)
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
	return atomicWriteFile(path, []byte(subPath), serverFilePermissions)
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
			path:   diskPath,
			size:   f.Size,
			offset: f.Offset,
		}
	}

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
