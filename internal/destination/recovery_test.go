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
