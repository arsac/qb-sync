package destination

import (
	"fmt"
	"strings"
)

// targetPath returns the canonical target path for a file (without .partial suffix).
func targetPath(fi *serverFileInfo) string {
	return strings.TrimSuffix(fi.path, partialSuffix)
}

// checkPathCollisions checks if any selected file's target path is already
// owned by another active torrent. Caller must hold the map's protecting lock.
func checkPathCollisions(filePaths map[string]string, hash string, files []*serverFileInfo) error {
	for _, fi := range files {
		if !fi.selected {
			continue
		}
		tp := targetPath(fi)
		if owner, exists := filePaths[tp]; exists && owner != hash {
			return fmt.Errorf("file path %s already owned by torrent %s", tp, owner)
		}
	}
	return nil
}

// registerFilePaths records ownership of all selected file paths for a torrent.
// Caller must hold the map's protecting lock.
func registerFilePaths(filePaths map[string]string, hash string, files []*serverFileInfo) {
	for _, fi := range files {
		if !fi.selected {
			continue
		}
		filePaths[targetPath(fi)] = hash
	}
}

// unregisterFilePaths removes ownership records for a torrent's file paths.
// Caller must hold the map's protecting lock.
func unregisterFilePaths(filePaths map[string]string, hash string, files []*serverFileInfo) {
	for _, fi := range files {
		tp := targetPath(fi)
		if owner, exists := filePaths[tp]; exists && owner == hash {
			delete(filePaths, tp)
		}
	}
}
