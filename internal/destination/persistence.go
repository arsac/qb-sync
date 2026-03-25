package destination

import (
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

// validateDataFiles checks that every selected file exists on disk.
// Skips unselected files and files with pending/complete hardlinks (those
// are created during finalization, not during streaming).
// Returns a non-nil error listing the first missing file if any are absent.
func validateDataFiles(files []*serverFileInfo) error {
	for _, fi := range files {
		if !fi.selected {
			continue
		}
		if fi.hardlink.state == hlStatePending || fi.hardlink.state == hlStateComplete {
			continue
		}
		if _, err := os.Stat(fi.path); err != nil {
			return fmt.Errorf("data file missing: %s", fi.path)
		}
	}
	return nil
}
