package destination

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// recoverInFlightTorrents scans persisted metadata directories and rebuilds
// in-memory state for in-flight torrents. Called once during Server.Run
// before accepting gRPC requests.
func (s *Server) recoverInFlightTorrents(ctx context.Context) error {
	s.store.Inodes().CleanupStale(ctx)

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
