package qbclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/autobrr/go-qbittorrent"

	"github.com/arsac/qb-sync/internal/metrics"
	"github.com/arsac/qb-sync/internal/streaming"
	"github.com/arsac/qb-sync/internal/utils"
	pb "github.com/arsac/qb-sync/proto"
)

var _ streaming.PieceSource = (*Source)(nil)

// fileHandleCache caches open file handles per torrent to avoid repeated
// open/close syscalls on the hot read path. os.File.ReadAt maps to pread(2)
// which is safe for concurrent use on the same fd.
type fileHandleCache struct {
	mu     sync.Mutex
	byHash map[string]map[string]*os.File // hash -> (abs path -> *os.File)
}

// get returns a cached handle or opens the file and caches it.
// The lock is released before os.Open to avoid holding the mutex during the syscall.
// On re-acquire, a double-check handles the race where another goroutine opened the
// same file concurrently — the loser closes its duplicate fd.
func (c *fileHandleCache) get(hash, path string) (*os.File, error) {
	c.mu.Lock()
	if c.byHash != nil {
		if perHash, ok := c.byHash[hash]; ok {
			if f, found := perHash[path]; found {
				c.mu.Unlock()
				metrics.FileHandleCacheTotal.WithLabelValues(metrics.ResultHit).Inc()
				return f, nil
			}
		}
	}
	c.mu.Unlock()

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.byHash == nil {
		c.byHash = make(map[string]map[string]*os.File)
	}
	perHash, ok := c.byHash[hash]
	if !ok {
		perHash = make(map[string]*os.File)
		c.byHash[hash] = perHash
	}

	if existing, exists := perHash[path]; exists {
		// Another goroutine won the race — use its handle, close ours.
		_ = f.Close()
		metrics.FileHandleCacheTotal.WithLabelValues(metrics.ResultHit).Inc()
		return existing, nil
	}
	perHash[path] = f
	metrics.FileHandleCacheTotal.WithLabelValues(metrics.ResultMiss).Inc()
	return f, nil
}

// evict removes and closes all cached handles for a torrent hash.
func (c *fileHandleCache) evict(hash string) {
	c.mu.Lock()
	handles := c.byHash[hash]
	delete(c.byHash, hash)
	c.mu.Unlock()

	if len(handles) > 0 {
		metrics.FileHandleEvictionsTotal.Inc()
	}
	for _, f := range handles {
		_ = f.Close()
	}
}

// evictPath removes and closes a single cached handle for a torrent+path.
func (c *fileHandleCache) evictPath(hash, path string) {
	c.mu.Lock()
	var f *os.File
	if perHash, ok := c.byHash[hash]; ok {
		f = perHash[path]
		delete(perHash, path)
	}
	c.mu.Unlock()

	if f != nil {
		metrics.FileHandleEvictionsTotal.Inc()
		_ = f.Close()
	}
}

// cachedMeta holds per-torrent cached metadata for ReadPiece.
type cachedMeta struct {
	files      []*pb.FileInfo
	contentDir string // read directory for this torrent
}

// Source implements streaming.PieceSource using qBittorrent API.
type Source struct {
	client            *ResilientClient
	dataPath          string   // Local path where torrent content is accessible
	qbDefaultSavePath string   // qBittorrent's default save path (queried at init)
	qbTempPath        string   // qBittorrent's temp path for incomplete torrents (empty if disabled)
	tempDataPath      string   // Local equivalent of qbTempPath (computed from dataPath + relative offset)
	fileCache         sync.Map // hash -> *cachedMeta
	handles           fileHandleCache
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
	if prefs.TempPathEnabled && prefs.TempPath != "" {
		s.qbTempPath = filepath.Clean(prefs.TempPath)
		tempRel, relErr := filepath.Rel(s.qbDefaultSavePath, s.qbTempPath)
		if relErr != nil {
			return fmt.Errorf("computing temp path relative offset: %w", relErr)
		}
		s.tempDataPath = filepath.Clean(filepath.Join(s.dataPath, tempRel))
	}
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

// resolveQBDir maps any qBittorrent path to the corresponding local path by
// trying both the default save root and the temp root. This handles paths under
// the temp directory (incomplete torrents) as well as the save directory.
func (s *Source) resolveQBDir(qbDir string) string {
	if rel, err := filepath.Rel(s.qbDefaultSavePath, qbDir); err == nil && !strings.HasPrefix(rel, "..") {
		return filepath.Clean(filepath.Join(s.dataPath, rel))
	}
	if s.qbTempPath != "" && s.tempDataPath != "" {
		if rel, err := filepath.Rel(s.qbTempPath, qbDir); err == nil && !strings.HasPrefix(rel, "..") {
			return filepath.Clean(filepath.Join(s.tempDataPath, rel))
		}
	}
	return s.dataPath
}

// ResolveContentDir maps a torrent's save_path to the local content directory.
// When ATM + categories are enabled, save_path includes a category subdirectory
// (e.g., /downloads/movies instead of /downloads). This method computes the
// relative subdirectory and applies it to the local dataPath.
func (s *Source) ResolveContentDir(torrentSavePath string) string {
	return s.resolveQBDir(torrentSavePath)
}

// resolveReadDir returns the directory for reading pieces.
// Uses ContentPath (the actual current location on disk) as the authoritative
// source. On ENOENT (torrent moved mid-read), callers should re-query.
func (s *Source) resolveReadDir(torrent qbittorrent.Torrent) string {
	if torrent.ContentPath != "" {
		return s.resolveQBDir(filepath.Dir(torrent.ContentPath))
	}
	return s.resolveQBDir(torrent.SavePath)
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

	contentDir := s.resolveReadDir(torrent)

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
		ContentDir: contentDir,
	}, nil
}

// ReadPiece reads a piece's data from disk.
// On ENOENT, evicts cache and retries with fresh metadata from qBittorrent.
// Re-querying content_path reflects the torrent's new location after moves
// (download→save transition or *arr recategorization).
func (s *Source) ReadPiece(ctx context.Context, piece *pb.Piece) ([]byte, error) {
	hash := piece.GetTorrentHash()

	cached, err := s.cachedTorrentMeta(ctx, hash)
	if err != nil {
		return nil, err
	}

	readStart := time.Now()
	defer func() { metrics.PieceReadDuration.Observe(time.Since(readStart).Seconds()) }()

	data, readErr := s.readPieceMultiFile(hash, cached.contentDir, cached.files, piece.GetOffset(), piece.GetSize())
	if readErr == nil || !errors.Is(readErr, os.ErrNotExist) {
		return data, readErr
	}

	// ENOENT: torrent moved (download→save or *arr recategorization).
	// Re-query content_path which reflects the new location.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	s.handles.evict(hash)
	s.fileCache.Delete(hash)
	cached, err = s.cachedTorrentMeta(ctx, hash)
	if err != nil {
		return nil, err
	}
	return s.readPieceMultiFile(hash, cached.contentDir, cached.files, piece.GetOffset(), piece.GetSize())
}

// cachedTorrentMeta returns the cached metadata for a torrent, fetching on first access.
func (s *Source) cachedTorrentMeta(ctx context.Context, hash string) (*cachedMeta, error) {
	if cached, ok := s.fileCache.Load(hash); ok {
		cm, valid := cached.(*cachedMeta)
		if !valid {
			return nil, fmt.Errorf("invalid cached type for %s", hash)
		}
		return cm, nil
	}

	meta, err := s.GetTorrentMetadata(ctx, hash)
	if err != nil {
		return nil, err
	}

	cm := &cachedMeta{
		files:      meta.GetFiles(),
		contentDir: meta.ContentDir,
	}
	s.fileCache.Store(hash, cm)
	return cm, nil
}

// EvictCache removes cached file metadata and open file handles for a torrent.
func (s *Source) EvictCache(hash string) {
	s.fileCache.Delete(hash)
	s.handles.evict(hash)
}

// readChunkCached reads a chunk using a cached file handle. On ReadAt error
// (not io.EOF), it evicts the stale handle and retries once with a fresh open.
func (s *Source) readChunkCached(hash, path string, offset, size int64) ([]byte, error) {
	f, err := s.handles.get(hash, path)
	if err != nil {
		return nil, err
	}

	data := make([]byte, size)
	n, err := f.ReadAt(data, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		// Stale handle (e.g., file replaced in-place) — evict and retry once.
		s.handles.evictPath(hash, path)
		f, err = s.handles.get(hash, path)
		if err != nil {
			return nil, err
		}
		n, err = f.ReadAt(data, offset)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
	}

	return data[:n], nil
}

// readPieceFromRegions reads piece data spanning multiple files using cached handles.
func (s *Source) readPieceFromRegions(
	hash string,
	regions []utils.FileRegion,
	pieceOffset, pieceSize int64,
) ([]byte, error) {
	data := make([]byte, 0, pieceSize)
	remaining := pieceSize
	currentOffset := pieceOffset

	for _, region := range regions {
		if remaining <= 0 {
			break
		}

		fileEnd := region.Offset + region.Size
		if fileEnd <= currentOffset {
			continue
		}

		fileReadOffset := max(currentOffset-region.Offset, 0)
		availableInFile := region.Size - fileReadOffset
		toRead := min(remaining, availableInFile)

		chunk, err := s.readChunkCached(hash, region.Path, fileReadOffset, toRead)
		if err != nil {
			return nil, fmt.Errorf("reading from %s at offset %d: %w", region.Path, fileReadOffset, err)
		}

		data = append(data, chunk...)
		remaining -= int64(len(chunk))
		currentOffset += int64(len(chunk))
	}

	return data, nil
}

func (s *Source) readPieceMultiFile(
	hash string,
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
	return s.readPieceFromRegions(hash, regions, offset, size)
}
