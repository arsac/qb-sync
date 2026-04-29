package destination

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// recoverInFlightTorrents scans persisted metadata directories and rebuilds
// in-memory state for in-flight torrents. Called once during Server.Run
// before accepting gRPC requests.
func (s *Server) recoverInFlightTorrents(ctx context.Context) error {
	// Rebuild inode registry from all .meta files before recovering in-flight torrents.
	s.rebuildInodeMap(ctx)

	// Delete legacy .inode_map.json if it exists (superseded by .meta-based rebuild).
	oldInodeMap := filepath.Join(s.config.BasePath, metaDirName, ".inode_map.json")
	if removeErr := os.Remove(oldInodeMap); removeErr == nil {
		s.logger.InfoContext(ctx, "deleted legacy .inode_map.json")
	}

	metaRoot := filepath.Join(s.config.BasePath, metaDirName)
	entries, err := os.ReadDir(metaRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("reading metadata root: %w", err)
	}

	var recovered int
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		hash := entry.Name()
		metaDir := filepath.Join(metaRoot, hash)

		if recoverErr := s.recoverTorrent(ctx, hash, metaDir); recoverErr != nil {
			s.logger.WarnContext(ctx, "skipping recovery for torrent",
				"hash", hash, "error", recoverErr)
			continue
		}
		recovered++
	}

	if recovered > 0 {
		s.logger.InfoContext(ctx, "recovered in-flight torrents",
			"count", recovered)
	}
	return nil
}

// recoverTorrent rebuilds state for a single torrent from its persisted
// .meta file. Torrents persisted in the old format (pre-.meta) are skipped;
// they will re-download on next sync — simpler code is worth the one-time cost.
func (s *Server) recoverTorrent(ctx context.Context, hash, metaDir string) error {
	if _, err := os.Stat(filepath.Join(metaDir, finalizedFileName)); err == nil {
		return errors.New("already finalized")
	}

	metaPath := filepath.Join(metaDir, metaFileName)
	meta, err := loadPersistedMeta(metaPath)
	if err != nil {
		return fmt.Errorf("loading metadata: %w", err)
	}
	if meta.GetSchemaVersion() > currentSchemaVersion {
		return fmt.Errorf("unknown schema version: %d", meta.GetSchemaVersion())
	}

	req := persistedMetaToRequest(meta)

	if reserveErr := s.store.Reserve(hash); reserveErr != nil {
		return fmt.Errorf("reserve failed: %w", reserveErr)
	}

	resp := s.initNewTorrent(ctx, hash, req)
	if !resp.GetSuccess() {
		s.store.Unreserve(hash)
		return fmt.Errorf("init failed: %s", resp.GetError())
	}

	s.logger.InfoContext(ctx, "recovered torrent from disk",
		"hash", hash,
		"piecesHave", resp.GetPiecesHaveCount(),
	)
	return nil
}

// inodeEntry pairs a FileID with the relative path of the file on disk.
type inodeEntry struct {
	fileID  FileID
	relPath string
}

// rebuildInodeMap scans all .meta files (finalized and in-flight) and rebuilds
// the in-memory inode registry. Uses a worker pool to manage NFS latency.
func (s *Server) rebuildInodeMap(ctx context.Context) {
	metaRoot := filepath.Join(s.config.BasePath, metaDirName)
	entries, err := os.ReadDir(metaRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		s.logger.WarnContext(ctx, "failed to read metadata root for inode rebuild", "error", err)
		return
	}

	// Collect entries concurrently with bounded workers.
	var mu sync.Mutex
	var results []inodeEntry
	sem := make(chan struct{}, inodeRebuildWorkers)

	var wg sync.WaitGroup
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		metaPath := filepath.Join(metaRoot, entry.Name(), metaFileName)

		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()

			found := s.collectInodeEntries(metaPath)
			if len(found) > 0 {
				mu.Lock()
				results = append(results, found...)
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	// Register all collected entries.
	for _, e := range results {
		s.store.Inodes().Register(e.fileID, e.relPath)
	}

	if len(results) > 0 {
		s.logger.InfoContext(ctx, "rebuilt inode registry from .meta files",
			"registered", len(results))
	}
}

// collectInodeEntries reads a single .meta file and returns inode entries
// for files that exist on disk with non-zero source device+inode.
func (s *Server) collectInodeEntries(metaPath string) []inodeEntry {
	meta, loadErr := loadPersistedMeta(metaPath)
	if loadErr != nil {
		return nil
	}

	subPath := meta.GetSaveSubPath()
	var entries []inodeEntry
	for _, f := range meta.GetFiles() {
		dev := f.GetSourceDevice()
		ino := f.GetSourceInode()
		if dev == 0 && ino == 0 {
			continue
		}
		relPath := filepath.Join(subPath, f.GetPath())
		fullPath := filepath.Join(s.config.BasePath, relPath)

		// Verify file exists on disk — skip stale entries.
		if _, statErr := os.Stat(fullPath); statErr != nil {
			continue
		}

		entries = append(entries, inodeEntry{
			fileID:  FileID{Dev: dev, Ino: ino},
			relPath: relPath,
		})
	}
	return entries
}
