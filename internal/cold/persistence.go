package cold

import (
	"context"
	"encoding/json"
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

// saveFilesInfo saves file metadata for recovery after restart.
func (s *Server) saveFilesInfo(metaDir string, info *persistedTorrentInfo) error {
	path := filepath.Join(metaDir, filesInfoFileName)
	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("marshaling files info: %w", err)
	}
	return atomicWriteFile(path, data, serverFilePermissions)
}

// loadFilesInfo loads file metadata for recovery after restart.
func (s *Server) loadFilesInfo(metaDir string) (*persistedTorrentInfo, error) {
	path := filepath.Join(metaDir, filesInfoFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var info persistedTorrentInfo
	if unmarshalErr := json.Unmarshal(data, &info); unmarshalErr != nil {
		return nil, fmt.Errorf("unmarshaling files info: %w", unmarshalErr)
	}
	return &info, nil
}

// recoverTorrentState attempts to recover torrent state from disk after a server restart.
// This enables FinalizeTorrent to work even if the torrent was initialized in a previous
// server instance.
func (s *Server) recoverTorrentState(ctx context.Context, hash string) (*serverTorrentState, error) {
	metaDir := filepath.Join(s.config.BasePath, metaDirName, hash)

	// Load persisted file info
	info, loadErr := s.loadFilesInfo(metaDir)
	if loadErr != nil {
		return nil, fmt.Errorf("loading files info: %w", loadErr)
	}

	statePath := filepath.Join(metaDir, ".state")

	// Load written pieces state
	written, stateErr := s.loadState(statePath, info.NumPieces)
	if stateErr != nil {
		return nil, fmt.Errorf("loading state: %w", stateErr)
	}

	// Find the .torrent file
	torrentPath := ""
	entries, readErr := os.ReadDir(metaDir)
	if readErr != nil {
		return nil, fmt.Errorf("reading meta dir: %w", readErr)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".torrent") {
			torrentPath = filepath.Join(metaDir, entry.Name())
			break
		}
	}
	if torrentPath == "" {
		return nil, errors.New("torrent file not found")
	}

	// Reconstruct file info
	files := make([]*serverFileInfo, len(info.Files))
	for i, f := range info.Files {
		files[i] = &serverFileInfo{
			path:   f.Path,
			size:   f.Size,
			offset: f.Offset,
		}
	}

	writtenCount := countWritten(written)

	s.logger.InfoContext(ctx, "recovering torrent state",
		"hash", hash,
		"written", writtenCount,
		"total", info.NumPieces,
		"files", len(files),
	)

	return &serverTorrentState{
		info:         nil, // Not needed for finalization
		written:      written,
		writtenCount: writtenCount,
		pieceHashes:  info.PieceHashes,
		pieceLength:  info.PieceLength,
		totalSize:    info.TotalSize,
		files:        files,
		torrentPath:  torrentPath,
		statePath:    statePath,
		saveSubPath:  info.SaveSubPath,
	}, nil
}
