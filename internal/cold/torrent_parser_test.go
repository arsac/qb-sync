package cold

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	bencode "github.com/jackpal/bencode-go"
)

// encodeTorrent encodes a bencodeTorrent to bytes for testing.
func encodeTorrent(t *testing.T, bt bencodeTorrent) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := bencode.Marshal(&buf, bt); err != nil {
		t.Fatalf("failed to encode torrent: %v", err)
	}
	return buf.Bytes()
}

// makePieces creates a raw pieces string from the given SHA1 hashes (hex-encoded).
func makePieces(t *testing.T, hexHashes ...string) string {
	t.Helper()
	var buf bytes.Buffer
	for _, h := range hexHashes {
		b, err := hex.DecodeString(h)
		if err != nil {
			t.Fatalf("bad hex hash %q: %v", h, err)
		}
		if len(b) != sha1Size {
			t.Fatalf("hash %q is %d bytes, want %d", h, len(b), sha1Size)
		}
		buf.Write(b)
	}
	return buf.String()
}

// sha1Hex returns the hex-encoded SHA1 of the given data.
func sha1Hex(data []byte) string {
	h := sha1.Sum(data)
	return hex.EncodeToString(h[:])
}

func TestParseTorrentFile_SingleFile(t *testing.T) {
	hash1 := sha1Hex([]byte("piece0"))
	hash2 := sha1Hex([]byte("piece1"))

	bt := bencodeTorrent{
		Info: bencodeInfo{
			Name:        "testfile.bin",
			PieceLength: 262144,
			Pieces:      makePieces(t, hash1, hash2),
			Length:      500000,
		},
	}
	data := encodeTorrent(t, bt)

	parsed, err := parseTorrentFile(data)
	if err != nil {
		t.Fatalf("parseTorrentFile: %v", err)
	}

	if parsed.Name != "testfile.bin" {
		t.Errorf("Name = %q, want %q", parsed.Name, "testfile.bin")
	}
	if parsed.PieceLength != 262144 {
		t.Errorf("PieceLength = %d, want 262144", parsed.PieceLength)
	}
	if parsed.TotalSize != 500000 {
		t.Errorf("TotalSize = %d, want 500000", parsed.TotalSize)
	}
	if parsed.NumPieces != 2 {
		t.Errorf("NumPieces = %d, want 2", parsed.NumPieces)
	}
	if len(parsed.Files) != 1 {
		t.Fatalf("len(Files) = %d, want 1", len(parsed.Files))
	}
	if parsed.Files[0].Path != "testfile.bin" {
		t.Errorf("Files[0].Path = %q, want %q", parsed.Files[0].Path, "testfile.bin")
	}
	if parsed.Files[0].Size != 500000 {
		t.Errorf("Files[0].Size = %d, want 500000", parsed.Files[0].Size)
	}
	if parsed.Files[0].Offset != 0 {
		t.Errorf("Files[0].Offset = %d, want 0", parsed.Files[0].Offset)
	}
}

func TestParseTorrentFile_MultiFile(t *testing.T) {
	hash1 := sha1Hex([]byte("piece0"))
	hash2 := sha1Hex([]byte("piece1"))
	hash3 := sha1Hex([]byte("piece2"))

	bt := bencodeTorrent{
		Info: bencodeInfo{
			Name:        "MyTorrent",
			PieceLength: 131072,
			Pieces:      makePieces(t, hash1, hash2, hash3),
			Files: []bencodeFile{
				{Length: 100000, Path: []string{"dir1", "file1.txt"}},
				{Length: 200000, Path: []string{"dir2", "file2.txt"}},
				{Length: 50000, Path: []string{"readme.txt"}},
			},
		},
	}
	data := encodeTorrent(t, bt)

	parsed, err := parseTorrentFile(data)
	if err != nil {
		t.Fatalf("parseTorrentFile: %v", err)
	}

	if parsed.Name != "MyTorrent" {
		t.Errorf("Name = %q, want %q", parsed.Name, "MyTorrent")
	}
	if parsed.TotalSize != 350000 {
		t.Errorf("TotalSize = %d, want 350000", parsed.TotalSize)
	}
	if parsed.NumPieces != 3 {
		t.Errorf("NumPieces = %d, want 3", parsed.NumPieces)
	}
	if len(parsed.Files) != 3 {
		t.Fatalf("len(Files) = %d, want 3", len(parsed.Files))
	}

	// Check file paths (joined with filepath.Join, including torrent name as root dir)
	wantPath0 := filepath.Join("MyTorrent", "dir1", "file1.txt")
	if parsed.Files[0].Path != wantPath0 {
		t.Errorf("Files[0].Path = %q, want %q", parsed.Files[0].Path, wantPath0)
	}
	wantPath1 := filepath.Join("MyTorrent", "dir2", "file2.txt")
	if parsed.Files[1].Path != wantPath1 {
		t.Errorf("Files[1].Path = %q, want %q", parsed.Files[1].Path, wantPath1)
	}
	wantPath2 := filepath.Join("MyTorrent", "readme.txt")
	if parsed.Files[2].Path != wantPath2 {
		t.Errorf("Files[2].Path = %q, want %q", parsed.Files[2].Path, wantPath2)
	}

	// Check offsets
	if parsed.Files[0].Offset != 0 {
		t.Errorf("Files[0].Offset = %d, want 0", parsed.Files[0].Offset)
	}
	if parsed.Files[1].Offset != 100000 {
		t.Errorf("Files[1].Offset = %d, want 100000", parsed.Files[1].Offset)
	}
	if parsed.Files[2].Offset != 300000 {
		t.Errorf("Files[2].Offset = %d, want 300000", parsed.Files[2].Offset)
	}

	// Check sizes
	if parsed.Files[0].Size != 100000 {
		t.Errorf("Files[0].Size = %d, want 100000", parsed.Files[0].Size)
	}
	if parsed.Files[1].Size != 200000 {
		t.Errorf("Files[1].Size = %d, want 200000", parsed.Files[1].Size)
	}
	if parsed.Files[2].Size != 50000 {
		t.Errorf("Files[2].Size = %d, want 50000", parsed.Files[2].Size)
	}
}

func TestParseTorrentFile_PieceHashes(t *testing.T) {
	hash1 := sha1Hex([]byte("alpha"))
	hash2 := sha1Hex([]byte("beta"))

	bt := bencodeTorrent{
		Info: bencodeInfo{
			Name:        "test",
			PieceLength: 1024,
			Pieces:      makePieces(t, hash1, hash2),
			Length:      2000,
		},
	}
	data := encodeTorrent(t, bt)

	parsed, err := parseTorrentFile(data)
	if err != nil {
		t.Fatalf("parseTorrentFile: %v", err)
	}

	if len(parsed.PieceHashes) != 2 {
		t.Fatalf("len(PieceHashes) = %d, want 2", len(parsed.PieceHashes))
	}
	if parsed.PieceHashes[0] != hash1 {
		t.Errorf("PieceHashes[0] = %q, want %q", parsed.PieceHashes[0], hash1)
	}
	if parsed.PieceHashes[1] != hash2 {
		t.Errorf("PieceHashes[1] = %q, want %q", parsed.PieceHashes[1], hash2)
	}
}

func TestParseTorrentFile_InvalidPiecesLength(t *testing.T) {
	bt := bencodeTorrent{
		Info: bencodeInfo{
			Name:        "test",
			PieceLength: 1024,
			Pieces:      "not-a-multiple-of-20-bytes!!",
			Length:      1024,
		},
	}
	data := encodeTorrent(t, bt)

	_, err := parseTorrentFile(data)
	if err == nil {
		t.Fatal("expected error for invalid pieces length")
	}
	if !strings.Contains(err.Error(), "not a multiple") {
		t.Errorf("error = %q, want to contain 'not a multiple'", err.Error())
	}
}

func TestParseTorrentFile_NoName(t *testing.T) {
	bt := bencodeTorrent{
		Info: bencodeInfo{
			PieceLength: 1024,
			Pieces:      makePieces(t, sha1Hex([]byte("x"))),
			Length:      100,
		},
	}
	data := encodeTorrent(t, bt)

	_, err := parseTorrentFile(data)
	if err == nil {
		t.Fatal("expected error for missing name")
	}
}

// Tests using real .torrent files downloaded from public sources.
// These catch encoding quirks that synthetic round-trip tests would miss.

func TestParseTorrentFile_RealSingleFile(t *testing.T) {
	// Real torrent: Ubuntu 24.04.2 desktop ISO, created by mktorrent.
	data, err := os.ReadFile("testdata/ubuntu-24.04.2-desktop-amd64.iso.torrent")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}

	parsed, err := parseTorrentFile(data)
	if err != nil {
		t.Fatalf("parseTorrentFile: %v", err)
	}

	if parsed.Name != "ubuntu-24.04.2-desktop-amd64.iso" {
		t.Errorf("Name = %q, want %q", parsed.Name, "ubuntu-24.04.2-desktop-amd64.iso")
	}
	if parsed.PieceLength != 262144 {
		t.Errorf("PieceLength = %d, want 262144", parsed.PieceLength)
	}
	if parsed.TotalSize != 6343219200 {
		t.Errorf("TotalSize = %d, want 6343219200", parsed.TotalSize)
	}
	if parsed.NumPieces != 24198 {
		t.Errorf("NumPieces = %d, want 24198", parsed.NumPieces)
	}

	// Single-file mode: one entry using info.name
	if len(parsed.Files) != 1 {
		t.Fatalf("len(Files) = %d, want 1", len(parsed.Files))
	}
	if parsed.Files[0].Path != "ubuntu-24.04.2-desktop-amd64.iso" {
		t.Errorf("Files[0].Path = %q", parsed.Files[0].Path)
	}
	if parsed.Files[0].Size != 6343219200 {
		t.Errorf("Files[0].Size = %d", parsed.Files[0].Size)
	}
	if parsed.Files[0].Offset != 0 {
		t.Errorf("Files[0].Offset = %d, want 0", parsed.Files[0].Offset)
	}

	// All piece hashes should be 40-char hex strings
	if len(parsed.PieceHashes) != 24198 {
		t.Fatalf("len(PieceHashes) = %d, want 24198", len(parsed.PieceHashes))
	}
	for i, h := range parsed.PieceHashes {
		if len(h) != 40 {
			t.Errorf("PieceHashes[%d] len = %d, want 40", i, len(h))
			break
		}
	}
}

func TestParseTorrentFile_RealMultiFile(t *testing.T) {
	// Real torrent: Big Buck Bunny from webtorrent.io, 3 files.
	data, err := os.ReadFile("testdata/big-buck-bunny.torrent")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}

	parsed, err := parseTorrentFile(data)
	if err != nil {
		t.Fatalf("parseTorrentFile: %v", err)
	}

	if parsed.Name != "Big Buck Bunny" {
		t.Errorf("Name = %q, want %q", parsed.Name, "Big Buck Bunny")
	}
	if parsed.PieceLength != 262144 {
		t.Errorf("PieceLength = %d, want 262144", parsed.PieceLength)
	}
	if parsed.NumPieces != 1055 {
		t.Errorf("NumPieces = %d, want 1055", parsed.NumPieces)
	}

	// 3 files, total 276445467 bytes
	if parsed.TotalSize != 276445467 {
		t.Errorf("TotalSize = %d, want 276445467", parsed.TotalSize)
	}
	if len(parsed.Files) != 3 {
		t.Fatalf("len(Files) = %d, want 3", len(parsed.Files))
	}

	wantFiles := []struct {
		path   string
		size   int64
		offset int64
	}{
		{"Big Buck Bunny/Big Buck Bunny.en.srt", 140, 0},
		{"Big Buck Bunny/Big Buck Bunny.mp4", 276134947, 140},
		{"Big Buck Bunny/poster.jpg", 310380, 140 + 276134947},
	}
	for i, want := range wantFiles {
		if parsed.Files[i].Path != want.path {
			t.Errorf("Files[%d].Path = %q, want %q", i, parsed.Files[i].Path, want.path)
		}
		if parsed.Files[i].Size != want.size {
			t.Errorf("Files[%d].Size = %d, want %d", i, parsed.Files[i].Size, want.size)
		}
		if parsed.Files[i].Offset != want.offset {
			t.Errorf("Files[%d].Offset = %d, want %d", i, parsed.Files[i].Offset, want.offset)
		}
	}

	// All piece hashes should be 40-char hex strings
	for i, h := range parsed.PieceHashes {
		if len(h) != 40 {
			t.Errorf("PieceHashes[%d] len = %d, want 40", i, len(h))
			break
		}
	}
}
