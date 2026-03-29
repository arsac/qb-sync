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
// owned by another active torrent. Caller must hold s.mu.
func (s *Server) checkPathCollisions(hash string, files []*serverFileInfo) error {
	if s.filePaths == nil {
		return nil
	}
	for _, fi := range files {
		if !fi.selected {
			continue
		}
		tp := targetPath(fi)
		if owner, exists := s.filePaths[tp]; exists && owner != hash {
			return fmt.Errorf("file path %s already owned by torrent %s", tp, owner)
		}
	}
	return nil
}

// registerFilePaths records ownership of all selected file paths for a torrent.
// Caller must hold s.mu.
func (s *Server) registerFilePaths(hash string, files []*serverFileInfo) {
	if s.filePaths == nil {
		return
	}
	for _, fi := range files {
		if !fi.selected {
			continue
		}
		s.filePaths[targetPath(fi)] = hash
	}
}

// unregisterFilePaths removes ownership records for a torrent's file paths.
// Caller must hold s.mu.
func (s *Server) unregisterFilePaths(hash string, files []*serverFileInfo) {
	if s.filePaths == nil {
		return
	}
	for _, fi := range files {
		tp := targetPath(fi)
		if owner, exists := s.filePaths[tp]; exists && owner == hash {
			delete(s.filePaths, tp)
		}
	}
}
