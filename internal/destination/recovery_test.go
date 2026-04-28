package destination

import (
	"bytes"
	"context"
	"crypto/sha1"
	"os"
	"path/filepath"
	"testing"

	"github.com/bits-and-blooms/bitset"

	bencode "github.com/jackpal/bencode-go"
)

// buildTestTorrentBytes creates a valid bencoded .torrent file for a single-file
// torrent with the given name, file size, and piece length.
func buildTestTorrentBytes(t *testing.T, name string, fileSize, pieceLen int64) []byte {
	t.Helper()
	numPieces := (fileSize + pieceLen - 1) / pieceLen
	var piecesBuf bytes.Buffer
	for range numPieces {
		h := sha1.Sum([]byte("fake-piece"))
		piecesBuf.Write(h[:])
	}

	bt := bencodeTorrent{
		Info: bencodeInfo{
			Name:        name,
			PieceLength: pieceLen,
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

// setupRecoveryMetaDir creates a metadata directory with a .torrent, .state,
// .version, and optionally .subpath and .selected files for recovery tests.
func setupRecoveryMetaDir(
	t *testing.T,
	basePath, hash string,
	torrentData []byte,
	written *bitset.BitSet,
) string {
	t.Helper()
	metaDir := filepath.Join(basePath, metaDirName, hash)
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Write .torrent file
	if err := os.WriteFile(filepath.Join(metaDir, "test.torrent"), torrentData, 0o644); err != nil {
		t.Fatal(err)
	}

	// Write .version file
	if err := os.WriteFile(
		filepath.Join(metaDir, versionFileName),
		[]byte(metaVersion),
		0o644,
	); err != nil {
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
	pieceLen := int64(1024)
	torrentData := buildTestTorrentBytes(t, "test-file", fileSize, pieceLen)

	// Create written bitmap with piece 0 set
	numPieces := uint((fileSize + pieceLen - 1) / pieceLen) // 4
	written := bitset.New(numPieces)
	written.Set(0)

	metaDir := setupRecoveryMetaDir(t, tmpDir, hash, torrentData, written)

	// Write .subpath file
	if err := os.WriteFile(filepath.Join(metaDir, subPathFileName), []byte("downloads"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Write .selected file (single file, selected)
	if err := os.WriteFile(filepath.Join(metaDir, selectedFileName), []byte{1}, 0o644); err != nil {
		t.Fatal(err)
	}

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
}

func TestRecoverInFlightTorrents_SkipsFinalized(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)
	ctx := context.Background()

	hash := "finalized00000000000000000000000000000000"
	torrentData := buildTestTorrentBytes(t, "done-file", 1024, 1024)

	metaDir := setupRecoveryMetaDir(t, tmpDir, hash, torrentData, nil)

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

func TestRecoverInFlightTorrents_SkipsStaleVersion(t *testing.T) {
	t.Parallel()
	s, tmpDir := newTestDestServer(t)
	ctx := context.Background()

	hash := "staleversion000000000000000000000000000000"
	torrentData := buildTestTorrentBytes(t, "old-file", 1024, 1024)

	metaDir := filepath.Join(tmpDir, metaDirName, hash)
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Write torrent file
	if err := os.WriteFile(filepath.Join(metaDir, "test.torrent"), torrentData, 0o644); err != nil {
		t.Fatal(err)
	}

	// Write OLD version
	if err := os.WriteFile(filepath.Join(metaDir, versionFileName), []byte("1"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := s.recoverInFlightTorrents(ctx); err != nil {
		t.Fatalf("recoverInFlightTorrents failed: %v", err)
	}

	// Verify torrent was NOT recovered
	if _, exists := s.store.Get(hash); exists {
		t.Error("torrent with stale version should not be recovered")
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
