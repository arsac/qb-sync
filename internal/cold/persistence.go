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

// saveState persists the written pieces state to disk.
func (s *Server) saveState(path string, written []bool) error {
	data := make([]byte, len(written))
	for i, w := range written {
		if w {
			data[i] = 1
		}
	}
	return os.WriteFile(path, data, serverFilePermissions)
}

// saveFilesInfo saves file metadata for recovery after restart.
func (s *Server) saveFilesInfo(metaDir string, info *persistedTorrentInfo) error {
	path := filepath.Join(metaDir, filesInfoFileName)
	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("marshaling files info: %w", err)
	}
	return os.WriteFile(path, data, serverFilePermissions)
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

// inodeMapPath returns the path to the inode map persistence file.
func (s *Server) inodeMapPath() string {
	return filepath.Join(s.config.BasePath, ".inode_map.json")
}

// loadInodeMap loads the persisted inode-to-path mapping from disk.
func (s *Server) loadInodeMap() error {
	data, err := os.ReadFile(s.inodeMapPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No persisted state yet
		}
		return err
	}

	// Unmarshal to temp variable first to avoid partial modification on error
	var loaded map[uint64]string
	if unmarshalErr := json.Unmarshal(data, &loaded); unmarshalErr != nil {
		return fmt.Errorf("unmarshaling inode map: %w", unmarshalErr)
	}

	s.inodeMu.Lock()
	s.inodeToPath = loaded
	s.inodeMu.Unlock()

	return nil
}

// saveInodeMap persists the inode-to-path mapping to disk.
func (s *Server) saveInodeMap() error {
	s.inodeMu.RLock()
	data, err := json.Marshal(s.inodeToPath)
	s.inodeMu.RUnlock()

	if err != nil {
		return err
	}

	// Ensure base path exists
	if mkdirErr := os.MkdirAll(s.config.BasePath, serverDirPermissions); mkdirErr != nil {
		return mkdirErr
	}

	return os.WriteFile(s.inodeMapPath(), data, serverFilePermissions)
}

// recoverTorrentState attempts to recover torrent state from disk after a server restart.
// This enables FinalizeTorrent to work even if the torrent was initialized in a previous
// server instance.
func (s *Server) recoverTorrentState(ctx context.Context, hash string) (*serverTorrentState, error) {
	metaDir := filepath.Join(s.config.BasePath, ".meta", hash)

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
			file:   nil, // Lazy open
		}
	}

	s.logger.InfoContext(ctx, "recovering torrent state",
		"hash", hash,
		"written", countWritten(written),
		"total", info.NumPieces,
		"files", len(files),
	)

	return &serverTorrentState{
		info:        nil, // Not needed for finalization
		written:     written,
		pieceHashes: nil, // Not needed for finalization
		files:       files,
		torrentPath: torrentPath,
		statePath:   statePath,
	}, nil
}

// countWritten counts the number of written pieces.
func countWritten(written []bool) int {
	count := 0
	for _, w := range written {
		if w {
			count++
		}
	}
	return count
}
