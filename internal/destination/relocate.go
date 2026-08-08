package destination

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

// relocateFiles moves torrent files from one sub-path to another when the
// save_sub_path has changed (e.g., source moved the torrent to a different category).
// relPaths are the torrent-relative file paths (e.g., "TorrentName/file.mkv").
// Both .partial and finalized file versions are checked and moved.
// Returns the number of files moved.
//
// The per-file work is fanned out because each file costs up to five independent
// NFS round-trips (two stats, MkdirAll, rename) and none of them depends on
// another file's answer. Serially that put len(relPaths) x RTT in front of both
// callers: InitTorrent, ahead of the first streamed piece, and FinalizeTorrent,
// which runs this synchronously inside the RPC the source is blocking on with a
// 20s connection timeout - a season pack at 2ms RTT was the same order as that
// timeout.
//
// Short-circuits on the first failure like setupFiles: the caller abandons the
// relocation either way, so moving further files into the new sub-path only
// widens the split between the two locations.
func (s *Server) relocateFiles(
	ctx context.Context,
	hash string,
	relPaths []string,
	oldSubPath, newSubPath string,
) (int, error) {
	if oldSubPath == newSubPath {
		return 0, nil
	}

	var moved atomic.Int64
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(fileSetupConcurrency)
	for _, relPath := range relPaths {
		g.Go(func() error {
			if gctx.Err() != nil {
				return gctx.Err()
			}
			n, relocErr := s.relocateFile(gctx, hash, relPath, oldSubPath, newSubPath)
			moved.Add(int64(n))
			return relocErr
		})
	}
	waitErr := g.Wait()

	total := int(moved.Load())
	if total > 0 {
		s.logger.InfoContext(ctx, "relocated torrent files",
			"hash", hash, "moved", total,
			"from", oldSubPath, "to", newSubPath)
	}

	return total, waitErr
}

// relocateFile moves one torrent file's .partial and finalized versions from the
// old sub-path to the new one, reporting how many of the two it moved.
func (s *Server) relocateFile(
	ctx context.Context,
	hash, relPath, oldSubPath, newSubPath string,
) (int, error) {
	var moved int
	for _, suffix := range [2]string{partialSuffix, ""} {
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
	return moved, nil
}

// updateStateAfterRelocate updates in-memory state file paths and saveSubPath
// after a successful relocation. Returns an error if any file path cannot be
// rewritten — this indicates a mismatch between disk state and memory state.
func updateStateAfterRelocate(state *serverTorrentState, basePath, oldSubPath, newSubPath string) error {
	oldBase := filepath.Join(basePath, oldSubPath)
	newBase := filepath.Join(basePath, newSubPath)

	for _, fi := range state.files {
		rel, relErr := filepath.Rel(oldBase, fi.path)
		if relErr != nil {
			return fmt.Errorf("computing relative path for %s from %s: %w", fi.path, oldBase, relErr)
		}
		fi.setPath(filepath.Join(newBase, rel))
	}

	state.saveSubPath = newSubPath
	return nil
}
