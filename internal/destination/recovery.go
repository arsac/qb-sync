package destination

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	pb "github.com/arsac/qb-sync/proto"
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
// metadata. Tries .meta (new format) first, then falls back to old files
// (.torrent + .subpath + .selected) for migration.
func (s *Server) recoverTorrent(
	ctx context.Context,
	hash, metaDir string,
) error {
	if _, err := os.Stat(filepath.Join(metaDir, finalizedFileName)); err == nil {
		return errors.New("already finalized")
	}

	metaPath := filepath.Join(metaDir, metaFileName)
	req, recoverErr := s.loadOrMigrateMeta(ctx, hash, metaDir, metaPath)
	if recoverErr != nil {
		return recoverErr
	}

	if reserveErr := s.store.Reserve(hash); reserveErr != nil {
		return fmt.Errorf("reserve failed: %w", reserveErr)
	}

	resp := s.initNewTorrent(ctx, hash, req)
	if !resp.GetSuccess() {
		s.store.Unreserve(hash)
		return fmt.Errorf("init failed: %s", resp.GetError())
	}

	// Cache torrent file bytes on recovered state.
	if state, ok := s.store.Get(hash); ok && len(req.GetTorrentFile()) > 0 {
		state.mu.Lock()
		if len(state.torrentFile) == 0 {
			state.torrentFile = req.GetTorrentFile()
		}
		state.mu.Unlock()
	}

	s.logger.InfoContext(ctx, "recovered torrent from disk",
		"hash", hash,
		"piecesHave", resp.GetPiecesHaveCount(),
	)
	return nil
}

// loadOrMigrateMeta loads metadata from .meta (new format) or falls back to
// old files (bencode + .subpath + .selected). On successful fallback, writes
// .meta for self-healing migration.
func (s *Server) loadOrMigrateMeta(
	ctx context.Context,
	hash, metaDir, metaPath string,
) (*pb.InitTorrentRequest, error) {
	// Try new format first.
	if meta, err := loadPersistedMeta(metaPath); err == nil {
		if meta.GetSchemaVersion() < currentSchemaVersion {
			return nil, fmt.Errorf("unknown schema version: %d", meta.GetSchemaVersion())
		}
		return persistedMetaToRequest(meta), nil
	}

	// Fall back to old format (migration path).
	if !checkMetaVersion(metaDir) {
		return nil, errors.New("stale metadata version")
	}

	torrentPath, err := findTorrentFile(metaDir)
	if err != nil {
		return nil, fmt.Errorf("finding torrent file: %w", err)
	}
	torrentData, err := os.ReadFile(torrentPath)
	if err != nil {
		return nil, fmt.Errorf("reading torrent file: %w", err)
	}
	parsed, err := parseTorrentFile(torrentData)
	if err != nil {
		return nil, fmt.Errorf("parsing torrent file: %w", err)
	}

	saveSubPath := loadSubPathFile(metaDir)
	numFiles := len(parsed.Files)
	selected := loadSelectedFile(metaDir, numFiles)

	files := make([]*pb.FileInfo, numFiles)
	for i, f := range parsed.Files {
		sel := true
		if selected != nil && i < len(selected) {
			sel = selected[i]
		}
		files[i] = &pb.FileInfo{
			Path:     f.Path,
			Size:     f.Size,
			Offset:   f.Offset,
			Selected: sel,
		}
	}

	req := &pb.InitTorrentRequest{
		TorrentHash: hash,
		Name:        parsed.Name,
		PieceSize:   parsed.PieceLength,
		TotalSize:   parsed.TotalSize,
		NumPieces:   int32(parsed.NumPieces),
		Files:       files,
		PieceHashes: parsed.PieceHashes,
		SaveSubPath: saveSubPath,
		TorrentFile: torrentData,
	}

	// Self-healing: write .meta so next restart uses new path.
	meta := buildPersistedMeta(req)
	if saveErr := savePersistedMeta(metaPath, meta); saveErr != nil {
		s.logger.WarnContext(ctx, "failed to write .meta during migration",
			"hash", hash, "error", saveErr)
	}

	return req, nil
}
