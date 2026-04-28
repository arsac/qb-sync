package destination

import (
	"os"
	"path/filepath"
	"testing"

	pb "github.com/arsac/qb-sync/proto"
)

func TestSaveLoadPersistedMeta_RoundTrip(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, metaFileName)

	original := &pb.PersistedTorrentMeta{
		SchemaVersion: currentSchemaVersion,
		TorrentHash:   "abc123",
		Name:          "My Torrent",
		PieceSize:     262144,
		TotalSize:     1048576,
		NumPieces:     4,
		Files: []*pb.PersistedFileInfo{
			{Path: "file1.txt", Size: 524288, Offset: 0, Selected: true},
			{Path: "file2.txt", Size: 524288, Offset: 524288, Selected: false},
		},
		TorrentFile: []byte("fake-torrent-data"),
		PieceHashes: []string{"hash0", "hash1", "hash2", "hash3"},
		SaveSubPath: "movies",
	}

	if err := savePersistedMeta(path, original); err != nil {
		t.Fatalf("savePersistedMeta: %v", err)
	}

	loaded, err := loadPersistedMeta(path)
	if err != nil {
		t.Fatalf("loadPersistedMeta: %v", err)
	}

	if loaded.GetSchemaVersion() != original.GetSchemaVersion() {
		t.Errorf("SchemaVersion: got %d, want %d",
			loaded.GetSchemaVersion(), original.GetSchemaVersion())
	}
	if loaded.GetTorrentHash() != original.GetTorrentHash() {
		t.Errorf("TorrentHash: got %q, want %q",
			loaded.GetTorrentHash(), original.GetTorrentHash())
	}
	if loaded.GetName() != original.GetName() {
		t.Errorf("Name: got %q, want %q", loaded.GetName(), original.GetName())
	}
	if loaded.GetPieceSize() != original.GetPieceSize() {
		t.Errorf("PieceSize: got %d, want %d",
			loaded.GetPieceSize(), original.GetPieceSize())
	}
	if loaded.GetTotalSize() != original.GetTotalSize() {
		t.Errorf("TotalSize: got %d, want %d",
			loaded.GetTotalSize(), original.GetTotalSize())
	}
	if loaded.GetNumPieces() != original.GetNumPieces() {
		t.Errorf("NumPieces: got %d, want %d",
			loaded.GetNumPieces(), original.GetNumPieces())
	}
	if len(loaded.GetFiles()) != len(original.GetFiles()) {
		t.Fatalf("Files count: got %d, want %d",
			len(loaded.GetFiles()), len(original.GetFiles()))
	}
	for i, f := range loaded.GetFiles() {
		orig := original.GetFiles()[i]
		if f.GetPath() != orig.GetPath() || f.GetSize() != orig.GetSize() ||
			f.GetOffset() != orig.GetOffset() || f.GetSelected() != orig.GetSelected() {
			t.Errorf("Files[%d]: got %+v, want %+v", i, f, orig)
		}
	}
	if string(loaded.GetTorrentFile()) != string(original.GetTorrentFile()) {
		t.Errorf("TorrentFile: got %q, want %q",
			loaded.GetTorrentFile(), original.GetTorrentFile())
	}
	if len(loaded.GetPieceHashes()) != len(original.GetPieceHashes()) {
		t.Fatalf("PieceHashes count: got %d, want %d",
			len(loaded.GetPieceHashes()), len(original.GetPieceHashes()))
	}
	for i, h := range loaded.GetPieceHashes() {
		if h != original.GetPieceHashes()[i] {
			t.Errorf("PieceHashes[%d]: got %q, want %q",
				i, h, original.GetPieceHashes()[i])
		}
	}
	if loaded.GetSaveSubPath() != original.GetSaveSubPath() {
		t.Errorf("SaveSubPath: got %q, want %q",
			loaded.GetSaveSubPath(), original.GetSaveSubPath())
	}
}

func TestLoadPersistedMeta_MissingFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "nonexistent", metaFileName)
	_, err := loadPersistedMeta(path)
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
	if !os.IsNotExist(err) {
		t.Errorf("expected os.IsNotExist error, got: %v", err)
	}
}

func TestBuildPersistedMeta_OmitsInode(t *testing.T) {
	t.Parallel()

	req := &pb.InitTorrentRequest{
		TorrentHash: "deadbeef",
		Name:        "Test",
		PieceSize:   131072,
		TotalSize:   262144,
		NumPieces:   2,
		Files: []*pb.FileInfo{
			{Path: "a.txt", Size: 131072, Offset: 0, Selected: true, Inode: 999},
			{Path: "b.txt", Size: 131072, Offset: 131072, Selected: false, Inode: 888},
		},
		TorrentFile: []byte("torrent-bytes"),
		PieceHashes: []string{"h0", "h1"},
		SaveSubPath: "tv",
	}

	meta := buildPersistedMeta(req)

	if meta.GetSchemaVersion() != currentSchemaVersion {
		t.Errorf("SchemaVersion: got %d, want %d",
			meta.GetSchemaVersion(), currentSchemaVersion)
	}
	if meta.GetTorrentHash() != "deadbeef" {
		t.Errorf("TorrentHash: got %q, want %q", meta.GetTorrentHash(), "deadbeef")
	}
	if len(meta.GetFiles()) != 2 {
		t.Fatalf("Files count: got %d, want 2", len(meta.GetFiles()))
	}

	// PersistedFileInfo has no Inode field — verify the proto message
	// only contains Path, Size, Offset, Selected.
	for i, f := range meta.GetFiles() {
		orig := req.GetFiles()[i]
		if f.GetPath() != orig.GetPath() {
			t.Errorf("Files[%d].Path: got %q, want %q", i, f.GetPath(), orig.GetPath())
		}
		if f.GetSize() != orig.GetSize() {
			t.Errorf("Files[%d].Size: got %d, want %d", i, f.GetSize(), orig.GetSize())
		}
		if f.GetOffset() != orig.GetOffset() {
			t.Errorf("Files[%d].Offset: got %d, want %d", i, f.GetOffset(), orig.GetOffset())
		}
		if f.GetSelected() != orig.GetSelected() {
			t.Errorf("Files[%d].Selected: got %v, want %v",
				i, f.GetSelected(), orig.GetSelected())
		}
	}
}

func TestPersistedMetaToRequest(t *testing.T) {
	t.Parallel()

	meta := &pb.PersistedTorrentMeta{
		SchemaVersion: currentSchemaVersion,
		TorrentHash:   "abc123",
		Name:          "My Torrent",
		PieceSize:     262144,
		TotalSize:     524288,
		NumPieces:     2,
		Files: []*pb.PersistedFileInfo{
			{Path: "file1.txt", Size: 262144, Offset: 0, Selected: true},
			{Path: "file2.txt", Size: 262144, Offset: 262144, Selected: false},
		},
		TorrentFile: []byte("torrent-data"),
		PieceHashes: []string{"h0", "h1"},
		SaveSubPath: "movies",
	}

	req := persistedMetaToRequest(meta)

	if req.GetTorrentHash() != meta.GetTorrentHash() {
		t.Errorf("TorrentHash: got %q, want %q",
			req.GetTorrentHash(), meta.GetTorrentHash())
	}
	if req.GetName() != meta.GetName() {
		t.Errorf("Name: got %q, want %q", req.GetName(), meta.GetName())
	}
	if req.GetPieceSize() != meta.GetPieceSize() {
		t.Errorf("PieceSize: got %d, want %d",
			req.GetPieceSize(), meta.GetPieceSize())
	}
	if req.GetTotalSize() != meta.GetTotalSize() {
		t.Errorf("TotalSize: got %d, want %d",
			req.GetTotalSize(), meta.GetTotalSize())
	}
	if req.GetNumPieces() != meta.GetNumPieces() {
		t.Errorf("NumPieces: got %d, want %d",
			req.GetNumPieces(), meta.GetNumPieces())
	}
	if len(req.GetFiles()) != len(meta.GetFiles()) {
		t.Fatalf("Files count: got %d, want %d",
			len(req.GetFiles()), len(meta.GetFiles()))
	}
	for i, f := range req.GetFiles() {
		orig := meta.GetFiles()[i]
		if f.GetPath() != orig.GetPath() {
			t.Errorf("Files[%d].Path: got %q, want %q", i, f.GetPath(), orig.GetPath())
		}
		if f.GetSize() != orig.GetSize() {
			t.Errorf("Files[%d].Size: got %d, want %d", i, f.GetSize(), orig.GetSize())
		}
		if f.GetOffset() != orig.GetOffset() {
			t.Errorf("Files[%d].Offset: got %d, want %d", i, f.GetOffset(), orig.GetOffset())
		}
		if f.GetSelected() != orig.GetSelected() {
			t.Errorf("Files[%d].Selected: got %v, want %v",
				i, f.GetSelected(), orig.GetSelected())
		}
	}
	if string(req.GetTorrentFile()) != string(meta.GetTorrentFile()) {
		t.Errorf("TorrentFile: got %q, want %q",
			req.GetTorrentFile(), meta.GetTorrentFile())
	}
	if len(req.GetPieceHashes()) != len(meta.GetPieceHashes()) {
		t.Fatalf("PieceHashes count: got %d, want %d",
			len(req.GetPieceHashes()), len(meta.GetPieceHashes()))
	}
	for i, h := range req.GetPieceHashes() {
		if h != meta.GetPieceHashes()[i] {
			t.Errorf("PieceHashes[%d]: got %q, want %q",
				i, h, meta.GetPieceHashes()[i])
		}
	}
	if req.GetSaveSubPath() != meta.GetSaveSubPath() {
		t.Errorf("SaveSubPath: got %q, want %q",
			req.GetSaveSubPath(), meta.GetSaveSubPath())
	}
}
