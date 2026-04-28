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
// metadata.
func (s *Server) recoverTorrent(
	ctx context.Context,
	hash, metaDir string,
) error {
	if _, err := os.Stat(filepath.Join(metaDir, finalizedFileName)); err == nil {
		return errors.New("already finalized")
	}

	if !checkMetaVersion(metaDir) {
		return errors.New("stale metadata version")
	}

	torrentPath, err := findTorrentFile(metaDir)
	if err != nil {
		return fmt.Errorf("finding torrent file: %w", err)
	}
	torrentData, err := os.ReadFile(torrentPath)
	if err != nil {
		return fmt.Errorf("reading torrent file: %w", err)
	}
	parsed, err := parseTorrentFile(torrentData)
	if err != nil {
		return fmt.Errorf("parsing torrent file: %w", err)
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
	}

	// Reserve the hash before initializing to prevent concurrent access.
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
		"name", parsed.Name,
		"piecesHave", resp.GetPiecesHaveCount(),
	)
	return nil
}
