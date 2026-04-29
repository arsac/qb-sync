package destination

import (
	"context"
	"fmt"
	"os"

	"github.com/bits-and-blooms/bitset"

	"google.golang.org/protobuf/proto"

	"github.com/arsac/qb-sync/internal/metrics"
	"github.com/arsac/qb-sync/internal/utils"
	pb "github.com/arsac/qb-sync/proto"
)

const currentSchemaVersion int32 = 3

// loadState loads the written pieces state from disk. Panics from
// bitset.UnmarshalBinary on truncated or malformed payloads (the underlying
// library indexes blindly into the byte slice on some inputs) are recovered
// as errors so a torn .state from the no-sync fast path can't crash the
// server on startup — the caller treats any error as "no prior state" and
// re-streams.
func (s *Server) loadState(path string, numPieces int) (*bitset.BitSet, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	written, unmarshalErr := safeUnmarshalBitset(data)
	if unmarshalErr != nil {
		return nil, fmt.Errorf("unmarshaling bitset: %w", unmarshalErr)
	}
	return ensureBitSetLength(written, uint(numPieces)), nil
}

// safeUnmarshalBitset wraps bitset.UnmarshalBinary with panic recovery.
// Returns nil + error on any panic or unmarshal error.
func safeUnmarshalBitset(data []byte) (*bitset.BitSet, error) {
	var (
		out    *bitset.BitSet
		outErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				out = nil
				outErr = fmt.Errorf("bitset unmarshal panic: %v", r)
			}
		}()
		b := bitset.New(0)
		if err := b.UnmarshalBinary(data); err != nil {
			outErr = err
			return
		}
		out = b
	}()
	return out, outErr
}

// atomicWriteFile writes data atomically using standard server permissions.
func atomicWriteFile(path string, data []byte) error {
	return utils.AtomicWriteFile(path, data, serverFilePermissions)
}

// saveState persists the written pieces state to disk. Uses the no-sync
// fast path: .state is a regenerable checkpoint, and a torn write at crash
// time is recovered automatically via clearStalePieces (any piece whose
// data file is missing on disk has its bit cleared on next init).
func (s *Server) saveState(path string, written *bitset.BitSet) error {
	data, marshalErr := written.MarshalBinary()
	if marshalErr != nil {
		return fmt.Errorf("marshaling bitset: %w", marshalErr)
	}
	return utils.AtomicWriteFileNoSync(path, data)
}

// doSaveState persists state using saveStateFunc (injected for tests) or the default saveState.
func (s *Server) doSaveState(path string, written *bitset.BitSet) error {
	if s.saveStateFunc != nil {
		return s.saveStateFunc(path, written)
	}
	return s.saveState(path, written)
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
		return nil, fmt.Errorf("reading %s: %w", path, readErr)
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
			Path:         f.GetPath(),
			Size:         f.GetSize(),
			Offset:       f.GetOffset(),
			Selected:     f.GetSelected(),
			SourceDevice: f.GetDevice(),
			SourceInode:  f.GetInode(),
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
			Device:   f.GetSourceDevice(),
			Inode:    f.GetSourceInode(),
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
		// skipForWriteData covers unselected files and files whose data is
		// supplied via hardlinks (pending/complete). Those are created at
		// finalize time, not by streaming, so the bitmap may legitimately
		// claim coverage even though the file isn't on disk yet.
		if fi.skipForWriteData() || fi.earlyFinalized {
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
