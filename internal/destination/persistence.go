package destination

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bits-and-blooms/bitset"

	"google.golang.org/protobuf/proto"

	"github.com/arsac/qb-sync/internal/metrics"
	"github.com/arsac/qb-sync/internal/utils"
	pb "github.com/arsac/qb-sync/proto"
)

const currentSchemaVersion int32 = 3

// loadState loads the written pieces state from disk.
func (s *Server) loadState(path string, numPieces int) (*bitset.BitSet, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	written := bitset.New(0)
	if unmarshalErr := written.UnmarshalBinary(data); unmarshalErr != nil {
		return nil, fmt.Errorf("unmarshaling bitset: %w", unmarshalErr)
	}
	return ensureBitSetLength(written, uint(numPieces)), nil
}

// atomicWriteFile writes data atomically using standard server permissions.
func atomicWriteFile(path string, data []byte) error {
	return utils.AtomicWriteFile(path, data, serverFilePermissions)
}

// saveState persists the written pieces state to disk.
func (s *Server) saveState(path string, written *bitset.BitSet) error {
	data, marshalErr := written.MarshalBinary()
	if marshalErr != nil {
		return fmt.Errorf("marshaling bitset: %w", marshalErr)
	}
	return atomicWriteFile(path, data)
}

// doSaveState persists state using saveStateFunc (injected for tests) or the default saveState.
func (s *Server) doSaveState(path string, written *bitset.BitSet) error {
	if s.saveStateFunc != nil {
		return s.saveStateFunc(path, written)
	}
	return s.saveState(path, written)
}

// loadSubPathFile reads the save sub-path from the .subpath file.
// Returns "" if the file is missing or unreadable.
func loadSubPathFile(metaDir string) string {
	path := filepath.Join(metaDir, subPathFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// loadSelectedFile reads the file selection bitmap from the .selected file.
// Returns nil if the file is missing (callers default to all-selected).
func loadSelectedFile(metaDir string, numFiles int) []bool {
	data, err := os.ReadFile(filepath.Join(metaDir, selectedFileName))
	if err != nil {
		return nil
	}
	selected := make([]bool, numFiles)
	for i := range min(numFiles, len(data)) {
		selected[i] = data[i] == 1
	}
	return selected
}

// checkMetaVersion returns true if the metadata directory has the current version.
func checkMetaVersion(metaDir string) bool {
	data, err := os.ReadFile(filepath.Join(metaDir, versionFileName))
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(data)) == metaVersion
}

// findTorrentFile locates the .torrent file in metaDir.
func findTorrentFile(metaDir string) (string, error) {
	entries, readErr := os.ReadDir(metaDir)
	if readErr != nil {
		return "", fmt.Errorf("reading meta dir: %w", readErr)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".torrent") {
			return filepath.Join(metaDir, entry.Name()), nil
		}
	}
	return "", errors.New("torrent file not found")
}

func savePersistedMeta(path string, meta *pb.PersistedTorrentMeta) error {
	data, err := proto.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshaling metadata: %w", err)
	}
	return atomicWriteFile(path, data)
}

func loadPersistedMeta(path string) (*pb.PersistedTorrentMeta, error) {
	data, readErr := os.ReadFile(path)
	if readErr != nil {
		return nil, readErr
	}
	meta := &pb.PersistedTorrentMeta{}
	if unmarshalErr := proto.Unmarshal(data, meta); unmarshalErr != nil {
		return nil, fmt.Errorf("unmarshaling metadata: %w", unmarshalErr)
	}
	return meta, nil
}

func buildPersistedMeta(req *pb.InitTorrentRequest) *pb.PersistedTorrentMeta {
	files := make([]*pb.PersistedFileInfo, len(req.GetFiles()))
	for i, f := range req.GetFiles() {
		files[i] = &pb.PersistedFileInfo{
			Path:     f.GetPath(),
			Size:     f.GetSize(),
			Offset:   f.GetOffset(),
			Selected: f.GetSelected(),
		}
	}
	return &pb.PersistedTorrentMeta{
		SchemaVersion: currentSchemaVersion,
		TorrentHash:   req.GetTorrentHash(),
		Name:          req.GetName(),
		PieceSize:     req.GetPieceSize(),
		TotalSize:     req.GetTotalSize(),
		NumPieces:     req.GetNumPieces(),
		Files:         files,
		TorrentFile:   req.GetTorrentFile(),
		PieceHashes:   req.GetPieceHashes(),
		SaveSubPath:   req.GetSaveSubPath(),
	}
}

func persistedMetaToRequest(meta *pb.PersistedTorrentMeta) *pb.InitTorrentRequest {
	files := make([]*pb.FileInfo, len(meta.GetFiles()))
	for i, f := range meta.GetFiles() {
		files[i] = &pb.FileInfo{
			Path:     f.GetPath(),
			Size:     f.GetSize(),
			Offset:   f.GetOffset(),
			Selected: f.GetSelected(),
		}
	}
	return &pb.InitTorrentRequest{
		TorrentHash: meta.GetTorrentHash(),
		Name:        meta.GetName(),
		PieceSize:   meta.GetPieceSize(),
		TotalSize:   meta.GetTotalSize(),
		NumPieces:   meta.GetNumPieces(),
		Files:       files,
		PieceHashes: meta.GetPieceHashes(),
		SaveSubPath: meta.GetSaveSubPath(),
		TorrentFile: meta.GetTorrentFile(),
	}
}

// clearStalePieces checks each selected file for existence on disk.
// If a file is missing, all pieces in its range are cleared from the
// written bitmap. Files with pending/complete hardlinks are skipped
// since they are created during finalization, not streaming.
// This preserves progress for files that DO exist while invalidating
// only the pieces whose data was lost.
func (s *Server) clearStalePieces(
	ctx context.Context,
	hash string,
	written *bitset.BitSet,
	files []*serverFileInfo,
) {
	for _, fi := range files {
		if !fi.selected || fi.earlyFinalized {
			continue
		}
		if fi.hardlink.state == hlStatePending || fi.hardlink.state == hlStateComplete {
			continue
		}
		if _, err := os.Stat(fi.path); err == nil {
			continue
		}

		// Data file missing — clear its pieces from the bitmap.
		cleared := 0
		for p := fi.firstPiece; p <= fi.lastPiece; p++ {
			if written.Test(uint(p)) {
				written.Clear(uint(p))
				cleared++
			}
		}
		if cleared > 0 {
			metrics.StaleBitmapPiecesClearedTotal.Add(float64(cleared))
			s.logger.WarnContext(ctx, "cleared stale pieces for missing file",
				"hash", hash,
				"file", fi.path,
				"pieces", cleared,
			)
		}
	}
}
