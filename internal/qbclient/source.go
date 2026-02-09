package qbclient

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/autobrr/go-qbittorrent"

	"github.com/arsac/qb-sync/internal/streaming"
	"github.com/arsac/qb-sync/internal/utils"
	pb "github.com/arsac/qb-sync/proto"
)

var _ streaming.PieceSource = (*Source)(nil)

// cachedMeta holds per-torrent cached metadata for ReadPiece.
type cachedMeta struct {
	files       []*pb.FileInfo
	contentDir  string // primary read directory for this torrent
	fallbackDir string // alternate directory to try on ENOENT (empty if none)
}

// Source implements streaming.PieceSource using qBittorrent API.
type Source struct {
	client            *ResilientClient
	dataPath          string   // Local path where torrent content is accessible
	qbDefaultSavePath string   // qBittorrent's default save path (queried at init)
	fileCache         sync.Map // hash -> *cachedMeta
}

// NewSource creates a new qBittorrent piece source with resilient client.
// dataPath is the local path where torrent content is accessible.
func NewSource(client *ResilientClient, dataPath string) *Source {
	return &Source{
		client:   client,
		dataPath: dataPath,
	}
}

// Init queries qBittorrent for its configured save path. Must be called after Login.
func (s *Source) Init(ctx context.Context) error {
	prefs, err := s.client.GetAppPreferencesCtx(ctx)
	if err != nil {
		return fmt.Errorf("getting qBittorrent preferences: %w", err)
	}
	s.qbDefaultSavePath = filepath.Clean(prefs.SavePath)
	return nil
}

// ResolveSubPath computes the relative sub-path between qBittorrent's default
// save path and a torrent's actual save path. When ATM + categories are enabled,
// this captures the category subdirectory (e.g., "movies").
// Returns "" when the torrent is at the default save path root.
func (s *Source) ResolveSubPath(torrentSavePath string) string {
	rel, err := filepath.Rel(s.qbDefaultSavePath, torrentSavePath)
	if err != nil || rel == "." || strings.HasPrefix(rel, "..") {
		return ""
	}
	return rel
}

// ResolveContentDir maps a torrent's save_path to the local content directory.
// When ATM + categories are enabled, save_path includes a category subdirectory
// (e.g., /downloads/movies instead of /downloads). This method computes the
// relative subdirectory and applies it to the local dataPath.
func (s *Source) ResolveContentDir(torrentSavePath string) string {
	if sub := s.ResolveSubPath(torrentSavePath); sub != "" {
		return filepath.Join(s.dataPath, sub)
	}
	return s.dataPath
}

// resolveReadDirs returns the primary and fallback directories for reading pieces.
// The primary is the best guess based on torrent progress; the fallback covers
// the transition window when files move between download and save paths.
func (s *Source) resolveReadDirs(torrent qbittorrent.Torrent) (primary, fallback string) {
	saveDir := s.ResolveContentDir(torrent.SavePath)
	if torrent.DownloadPath == "" {
		return saveDir, ""
	}
	dlDir := s.ResolveContentDir(torrent.DownloadPath)
	// dlDir == s.dataPath means DownloadPath is outside the qB save root
	// (ResolveContentDir couldn't compute a meaningful relative path).
	if dlDir == s.dataPath || dlDir == saveDir {
		return saveDir, ""
	}
	if torrent.Progress < 1.0 {
		return dlDir, saveDir
	}
	return saveDir, dlDir
}

// GetPieceStates returns the current state of all pieces for a torrent.
// Returns ErrTorrentNotFound if the torrent no longer exists (e.g., was deleted).
// Uses resilient client with automatic retry for transient errors.
func (s *Source) GetPieceStates(ctx context.Context, hash string) ([]streaming.PieceState, error) {
	states, err := s.client.GetTorrentPieceStatesCtx(ctx, hash)
	if err != nil {
		// Detect 404 errors which indicate the torrent was deleted.
		// The go-qbittorrent library returns "unexpected status: 404" for deleted torrents.
		// Note: 404 errors are NOT retried by ResilientClient (non-retriable).
		if strings.Contains(err.Error(), "404") {
			return nil, fmt.Errorf("getting piece states: %w", streaming.ErrTorrentNotFound)
		}
		return nil, fmt.Errorf("getting piece states: %w", err)
	}

	result := make([]streaming.PieceState, len(states))
	for i, state := range states {
		result[i] = streaming.PieceState(state)
	}
	return result, nil
}

// GetPieceHashes returns the expected SHA1 hash for each piece.
// Uses resilient client with automatic retry for transient errors.
func (s *Source) GetPieceHashes(ctx context.Context, hash string) ([]string, error) {
	hashes, err := s.client.GetTorrentPieceHashesCtx(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("getting piece hashes: %w", err)
	}
	return hashes, nil
}

// GetTorrentMetadata returns metadata needed for streaming.
// Uses resilient client with automatic retry for transient errors.
func (s *Source) GetTorrentMetadata(ctx context.Context, hash string) (*streaming.TorrentMetadata, error) {
	props, err := s.client.GetTorrentPropertiesCtx(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("getting torrent properties: %w", err)
	}

	torrents, err := s.client.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{hash},
	})
	if err != nil {
		return nil, fmt.Errorf("getting torrent info: %w", err)
	}
	if len(torrents) == 0 {
		return nil, fmt.Errorf("torrent not found: %s", hash)
	}
	torrent := torrents[0]

	contentDir, fallbackDir := s.resolveReadDirs(torrent)

	qbFiles, err := s.client.GetFilesInformationCtx(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("getting torrent files: %w", err)
	}

	// Sort files by Index to ensure correct offset calculation.
	// qBittorrent may return files in any order, but torrent piece data
	// is laid out according to the file order in the .torrent metadata.
	// Note: Using sort.Slice because TorrentFiles has anonymous struct elements.
	sortedQBFiles := make(qbittorrent.TorrentFiles, len(*qbFiles))
	copy(sortedQBFiles, *qbFiles)
	sort.Slice(sortedQBFiles, func(i, j int) bool {
		return sortedQBFiles[i].Index < sortedQBFiles[j].Index
	})

	files := make([]*pb.FileInfo, len(sortedQBFiles))
	var offset int64
	for i, f := range sortedQBFiles {
		files[i] = &pb.FileInfo{
			Path:   f.Name,
			Size:   f.Size,
			Offset: offset,
		}

		filePath := filepath.Join(contentDir, f.Name)
		if inode, inodeErr := utils.GetInode(filePath); inodeErr == nil {
			files[i].Inode = inode
		}

		offset += f.Size
	}

	pieceSize := int64(props.PieceSize)
	numPieces := props.PiecesNum
	if numPieces == 0 && pieceSize > 0 {
		numPieces = int((torrent.Size + pieceSize - 1) / pieceSize)
	}

	// Validate piece count fits in int32 (protobuf field type)
	if numPieces > math.MaxInt32 {
		return nil, fmt.Errorf("piece count %d exceeds maximum supported value", numPieces)
	}

	torrentFile, exportErr := s.client.ExportTorrentCtx(ctx, hash)
	if exportErr != nil {
		return nil, fmt.Errorf("exporting torrent: %w", exportErr)
	}

	pieceHashes, hashErr := s.client.GetTorrentPieceHashesCtx(ctx, hash)
	if hashErr != nil {
		return nil, fmt.Errorf("getting piece hashes: %w", hashErr)
	}

	return &streaming.TorrentMetadata{
		InitTorrentRequest: &pb.InitTorrentRequest{
			TorrentHash: hash,
			Name:        torrent.Name,
			PieceSize:   pieceSize,
			TotalSize:   torrent.Size,
			NumPieces:   int32(numPieces),
			Files:       files,
			TorrentFile: torrentFile,
			PieceHashes: pieceHashes,
			SaveSubPath: s.ResolveSubPath(torrent.SavePath),
		},
		ContentDir:  contentDir,
		FallbackDir: fallbackDir,
	}, nil
}

// ReadPiece reads a piece's data from disk.
// On ENOENT, tries the fallback directory (covers download ↔ save path transitions),
// then evicts cache and retries with fresh metadata (covers *arr recategorization).
func (s *Source) ReadPiece(ctx context.Context, piece *pb.Piece) ([]byte, error) {
	hash := piece.GetTorrentHash()

	cached, err := s.cachedTorrentMeta(ctx, hash)
	if err != nil {
		return nil, err
	}

	data, readErr := s.readPieceMultiFile(cached.contentDir, cached.files, piece.GetOffset(), piece.GetSize())
	if readErr == nil || !errors.Is(readErr, os.ErrNotExist) {
		return data, readErr
	}

	// ENOENT: try the fallback directory (download ↔ save path transition).
	if cached.fallbackDir != "" {
		data, readErr = s.readPieceMultiFile(cached.fallbackDir, cached.files, piece.GetOffset(), piece.GetSize())
		if readErr == nil {
			// Fallback worked — promote it to primary in the cache.
			s.fileCache.Store(hash, &cachedMeta{
				files:      cached.files,
				contentDir: cached.fallbackDir,
			})
			return data, nil
		}
	}

	// Both dirs failed (or no fallback). Evict and retry with fresh metadata
	// from qBittorrent — handles *arr recategorization where paths change entirely.
	s.fileCache.Delete(hash)
	cached, err = s.cachedTorrentMeta(ctx, hash)
	if err != nil {
		return nil, err
	}
	return s.readPieceMultiFile(cached.contentDir, cached.files, piece.GetOffset(), piece.GetSize())
}

// cachedTorrentMeta returns the cached metadata for a torrent, fetching on first access.
func (s *Source) cachedTorrentMeta(ctx context.Context, hash string) (*cachedMeta, error) {
	if cached, ok := s.fileCache.Load(hash); ok {
		return cached.(*cachedMeta), nil
	}

	meta, err := s.GetTorrentMetadata(ctx, hash)
	if err != nil {
		return nil, err
	}

	cm := &cachedMeta{
		files:       meta.GetFiles(),
		contentDir:  meta.ContentDir,
		fallbackDir: meta.FallbackDir,
	}
	s.fileCache.Store(hash, cm)
	return cm, nil
}

// EvictCache removes cached file metadata for a torrent after finalization.
func (s *Source) EvictCache(hash string) {
	s.fileCache.Delete(hash)
}

func (s *Source) readPieceMultiFile(
	basePath string,
	files []*pb.FileInfo,
	offset, size int64,
) ([]byte, error) {
	regions := make([]utils.FileRegion, len(files))
	for i, f := range files {
		regions[i] = utils.FileRegion{
			Path:   filepath.Join(basePath, f.GetPath()),
			Offset: f.GetOffset(),
			Size:   f.GetSize(),
		}
	}
	return utils.ReadPieceFromFiles(regions, offset, size)
}
