package destination

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/bits-and-blooms/bitset"

	bencode "github.com/jackpal/bencode-go"

	pb "github.com/arsac/qb-sync/proto"
)

const testPieceLen int64 = 1024

// buildTestTorrentBytes creates a valid bencoded .torrent file for a single-file
// torrent with the given name and file size, using testPieceLen as piece length.
func buildTestTorrentBytes(t *testing.T, name string, fileSize int64) []byte {
	t.Helper()
	numPieces := (fileSize + testPieceLen - 1) / testPieceLen
	var piecesBuf bytes.Buffer
	for range numPieces {
		h := sha1.Sum([]byte("fake-piece"))
		piecesBuf.Write(h[:])
	}

	bt := bencodeTorrent{
		Info: bencodeInfo{
			Name:        name,
			PieceLength: testPieceLen,
			Length:      fileSize,
			Pieces:      piecesBuf.String(),
		},
	}

	var buf bytes.Buffer
	if err := bencode.Marshal(&buf, bt); err != nil {
		t.Fatalf("failed to encode torrent: %v", err)
	}
	return buf.Bytes()
}

// buildTestPieceHashes returns hex-encoded SHA1 hashes for the given file size,
// using testPieceLen, matching the hashes produced by buildTestTorrentBytes.
func buildTestPieceHashes(t *testing.T, fileSize int64) []string {
	t.Helper()
	numPieces := (fileSize + testPieceLen - 1) / testPieceLen
	hashes := make([]string, numPieces)
	h := sha1.Sum([]byte("fake-piece"))
	hexHash := hex.EncodeToString(h[:])
	for i := range hashes {
		hashes[i] = hexHash
	}
	return hashes
}

// setupRecoveryMetaDir creates a metadata directory with a .meta file (new format)
// and optionally a .state file for recovery tests.
func setupRecoveryMetaDir(
	t *testing.T,
	basePath, hash string,
	req *pb.InitTorrentRequest,
	written *bitset.BitSet,
) string {
	t.Helper()
	metaDir := filepath.Join(basePath, metaDirName, hash)
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Write .meta file (new format).
	meta := buildPersistedMeta(req)
	if err := savePersistedMeta(filepath.Join(metaDir, metaFileName), meta); err != nil {
		t.Fatal(err)
	}

	// Write .state file
	if written != nil {
		stateData, marshalErr := written.MarshalBinary()
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if writeErr := os.WriteFile(filepath.Join(metaDir, ".state"), stateData, 0o644); writeErr != nil {
			t.Fatal(writeErr)
		}
	}

	return metaDir
}

func TestRecoverInFlightTorrents(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)
	ctx := context.Background()

	hash := "abcdef1234567890abcdef1234567890abcdef12"
	fileSize := int64(4096)
	torrentData := buildTestTorrentBytes(t, "test-file", fileSize)
	pieceHashes := buildTestPieceHashes(t, fileSize)

	// Create written bitmap with piece 0 set
	numPieces := uint((fileSize + testPieceLen - 1) / testPieceLen) // 4
	written := bitset.New(numPieces)
	written.Set(0)

	req := &pb.InitTorrentRequest{
		TorrentHash: hash,
		Name:        "test-file",
		PieceSize:   testPieceLen,
		TotalSize:   fileSize,
		NumPieces:   int32(numPieces),
		Files: []*pb.FileInfo{{
			Path:     "test-file",
			Size:     fileSize,
			Offset:   0,
			Selected: true,
		}},
		PieceHashes: pieceHashes,
		SaveSubPath: "downloads",
		TorrentFile: torrentData,
	}

	setupRecoveryMetaDir(t, tmpDir, hash, req, written)

	// Create the .partial data file so clearStalePieces does not zero our bitmap
	partialPath := filepath.Join(tmpDir, "downloads", "test-file.partial")
	if err := os.MkdirAll(filepath.Dir(partialPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(partialPath, make([]byte, fileSize), 0o644); err != nil {
		t.Fatal(err)
	}

	// Run recovery
	if err := s.recoverInFlightTorrents(ctx); err != nil {
		t.Fatalf("recoverInFlightTorrents failed: %v", err)
	}

	// Verify torrent is tracked in store
	state, exists := s.store.Get(hash)
	if !exists {
		t.Fatal("torrent not found in store after recovery")
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	// Verify written bitmap has piece 0 set
	if !state.written.Test(0) {
		t.Error("piece 0 should be set in recovered written bitmap")
	}

	// Verify save sub-path was loaded
	if state.saveSubPath != "downloads" {
		t.Errorf("saveSubPath = %q, want %q", state.saveSubPath, "downloads")
	}

	// Verify torrent file bytes are cached on state.
	// initNewTorrent sets torrentFile directly from the request, so no
	// separate cache step in recoverTorrent is needed.
	if len(state.torrentFile) == 0 {
		t.Fatal("expected torrentFile to be cached on state after recovery")
	}
}

func TestRecoverInFlightTorrents_SkipsFinalized(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)
	ctx := context.Background()

	hash := "finalized00000000000000000000000000000000"
	torrentData := buildTestTorrentBytes(t, "done-file", 1024)
	pieceHashes := buildTestPieceHashes(t, 1024)

	req := &pb.InitTorrentRequest{
		TorrentHash: hash,
		Name:        "done-file",
		PieceSize:   1024,
		TotalSize:   1024,
		NumPieces:   1,
		Files: []*pb.FileInfo{{
			Path:     "done-file",
			Size:     1024,
			Offset:   0,
			Selected: true,
		}},
		PieceHashes: pieceHashes,
		TorrentFile: torrentData,
	}

	metaDir := setupRecoveryMetaDir(t, tmpDir, hash, req, nil)

	// Write .finalized marker
	if err := os.WriteFile(filepath.Join(metaDir, finalizedFileName), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := s.recoverInFlightTorrents(ctx); err != nil {
		t.Fatalf("recoverInFlightTorrents failed: %v", err)
	}

	// Verify torrent was NOT recovered
	if _, exists := s.store.Get(hash); exists {
		t.Error("finalized torrent should not be recovered")
	}
}

func TestRecoverInFlightTorrents_NoMetaRoot(t *testing.T) {
	t.Parallel()
	s, _ := newTestDestServer(t)
	ctx := context.Background()

	// No .qbsync directory exists — should return nil with no error
	if err := s.recoverInFlightTorrents(ctx); err != nil {
		t.Fatalf("expected nil error when meta root missing, got: %v", err)
	}
}

// TestRebuildInodeMap_SkipsZeroFileID covers the legacy-metadata path:
// .meta files written before the FileID change have source_device=0 and
// source_inode=0 (proto3 zero defaults). collectInodeEntries skips those
// so the registry stays empty rather than registering a hash-collision
// magnet at FileID{0,0}.
func TestRebuildInodeMap_SkipsZeroFileID(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)
	ctx := context.Background()

	hash := "legacy_torrent_hash"
	// Build a request with NO source device/inode (legacy default).
	req := &pb.InitTorrentRequest{
		TorrentHash: hash,
		Name:        "legacy",
		PieceSize:   testPieceLen,
		TotalSize:   2048,
		NumPieces:   2,
		Files: []*pb.FileInfo{
			{Path: "legacy.bin", Size: 2048, Selected: true /* Device=0, Inode=0 */},
		},
		PieceHashes: buildTestPieceHashes(t, 2048),
		TorrentFile: buildTestTorrentBytes(t, "legacy", 2048),
	}
	setupRecoveryMetaDir(t, tmpDir, hash, req, nil)

	// Write the file on disk so the rebuild's stat passes.
	dataPath := filepath.Join(tmpDir, "legacy.bin")
	if err := os.WriteFile(dataPath, make([]byte, 2048), 0o644); err != nil {
		t.Fatal(err)
	}

	s.rebuildInodeMap(ctx)

	if got := s.store.Inodes().Len(); got != 0 {
		t.Errorf("legacy .meta with zero source FileID must NOT register an inode entry; got %d entries", got)
	}
}

// TestRecoverInFlightTorrents_StateWithoutMeta covers the partial-corruption
// case where a torrent directory has .state but no .meta. recoverTorrent
// should skip the directory rather than crash, because there's no metadata
// to rebuild from.
func TestRecoverInFlightTorrents_StateWithoutMeta(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)
	ctx := context.Background()

	hash := "orphan_state_hash"
	metaDir := filepath.Join(tmpDir, metaDirName, hash)
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Write a .state but no .meta.
	written := bitset.New(8)
	written.Set(0)
	written.Set(3)
	stateData, _ := written.MarshalBinary()
	if err := os.WriteFile(filepath.Join(metaDir, ".state"), stateData, 0o644); err != nil {
		t.Fatal(err)
	}

	// Should not crash — skip the malformed directory and continue.
	if err := s.recoverInFlightTorrents(ctx); err != nil {
		t.Fatalf("recovery must tolerate orphan .state without .meta: %v", err)
	}
	// Torrent should NOT be in the store (nothing to recover from).
	if _, exists := s.store.GetWithSentinel(hash); exists {
		t.Error("orphan .state without .meta should not produce a recovered torrent state")
	}
}

// TestRecoverInFlightTorrents_MetaWithoutState covers the case where the
// torrent's .meta exists but no .state was ever flushed (init crashed before
// the first flush). recoverTorrent should rebuild from .meta with an empty
// written bitmap, so streaming starts fresh.
func TestRecoverInFlightTorrents_MetaWithoutState(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)
	ctx := context.Background()

	hash := "fresh_torrent_hash"
	req := &pb.InitTorrentRequest{
		TorrentHash: hash,
		Name:        "fresh",
		PieceSize:   testPieceLen,
		TotalSize:   2048,
		NumPieces:   2,
		Files: []*pb.FileInfo{
			{Path: "fresh.bin", Size: 2048, Selected: true},
		},
		PieceHashes: buildTestPieceHashes(t, 2048),
		TorrentFile: buildTestTorrentBytes(t, "fresh", 2048),
	}
	// Write .meta but no .state.
	setupRecoveryMetaDir(t, tmpDir, hash, req, nil)

	if err := s.recoverInFlightTorrents(ctx); err != nil {
		t.Fatalf("recovery must succeed with .meta but no .state: %v", err)
	}

	state, exists := s.store.GetWithSentinel(hash)
	if !exists {
		t.Fatal("torrent must be recovered into the store")
	}
	if state.written == nil {
		t.Fatal("recovered state must have a non-nil written bitset")
	}
	if state.written.Count() != 0 {
		t.Errorf("recovered written bitmap must be empty (no .state on disk), got %d bits set", state.written.Count())
	}
}
