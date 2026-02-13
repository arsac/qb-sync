package destination

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// relocateFiles moves torrent files from one sub-path to another when the
// save_sub_path has changed (e.g., source moved the torrent to a different category).
// relPaths are the torrent-relative file paths (e.g., "TorrentName/file.mkv").
// Both .partial and finalized file versions are checked and moved.
// Returns the number of files moved.
func (s *Server) relocateFiles(
	ctx context.Context,
	hash string,
	relPaths []string,
	oldSubPath, newSubPath string,
) (int, error) {
	if oldSubPath == newSubPath {
		return 0, nil
	}

	var moved int
	for _, relPath := range relPaths {
		for _, suffix := range []string{partialSuffix, ""} {
			oldPath := filepath.Join(s.config.BasePath, oldSubPath, relPath) + suffix
			newPath := filepath.Join(s.config.BasePath, newSubPath, relPath) + suffix

			if _, statErr := os.Stat(oldPath); statErr != nil {
				continue
			}

			// Skip if target already exists
			if _, statErr := os.Stat(newPath); statErr == nil {
				s.logger.DebugContext(ctx, "relocation target already exists, skipping",
					"hash", hash, "path", newPath)
				continue
			}

			if mkErr := os.MkdirAll(filepath.Dir(newPath), serverDirPermissions); mkErr != nil {
				return moved, fmt.Errorf("creating directory for %s: %w", newPath, mkErr)
			}

			if renameErr := os.Rename(oldPath, newPath); renameErr != nil {
				return moved, fmt.Errorf("moving %s to %s: %w", oldPath, newPath, renameErr)
			}

			moved++
		}
	}

	if moved > 0 {
		s.logger.InfoContext(ctx, "relocated torrent files",
			"hash", hash, "moved", moved,
			"from", oldSubPath, "to", newSubPath)
	}

	return moved, nil
}

// updateStateAfterRelocate updates in-memory state file paths and saveSubPath
// after a successful relocation.
func updateStateAfterRelocate(state *serverTorrentState, basePath, oldSubPath, newSubPath string) {
	oldBase := filepath.Join(basePath, oldSubPath)
	newBase := filepath.Join(basePath, newSubPath)

	for _, fi := range state.files {
		rel, relErr := filepath.Rel(oldBase, fi.path)
		if relErr == nil {
			fi.path = filepath.Join(newBase, rel)
		}
	}

	state.saveSubPath = newSubPath
}
