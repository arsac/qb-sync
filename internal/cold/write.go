package cold

import (
	"context"
	"fmt"
	"os"

	"github.com/arsac/qb-sync/internal/metrics"
	"github.com/arsac/qb-sync/internal/utils"
	pb "github.com/arsac/qb-sync/proto"
)

// openFile lazily opens a file for writing, creating and pre-allocating it if needed.
func (s *Server) openFile(fi *serverFileInfo) (*os.File, error) {
	if fi.file != nil {
		return fi.file, nil
	}

	file, err := os.OpenFile(fi.path, os.O_RDWR|os.O_CREATE, serverFilePermissions)
	if err != nil {
		return nil, err
	}

	// Pre-allocate to expected size
	if truncErr := file.Truncate(fi.size); truncErr != nil {
		_ = file.Close()
		return nil, truncErr
	}

	fi.file = file
	return file, nil
}

// writePieceData writes piece data to the correct file(s) based on offset.
// A piece may span multiple files in a multi-file torrent.
// Skips files that are hardlinked or pending hardlink.
func (s *Server) writePieceData(state *serverTorrentState, offset int64, data []byte) error {
	remaining := data
	currentOffset := offset

	for _, fi := range state.files {
		if len(remaining) == 0 {
			break
		}

		fileEnd := fi.offset + fi.size

		// Skip files before our offset
		if fileEnd <= currentOffset {
			continue
		}

		// Calculate position within this file
		fileWriteOffset := max(currentOffset-fi.offset, 0)

		// Calculate how much data belongs to this file
		availableInFile := fi.size - fileWriteOffset
		toProcess := min(int64(len(remaining)), availableInFile)

		// Skip hardlinked or pending hardlink files
		if fi.hardlinked || fi.pendingHardlink {
			remaining = remaining[toProcess:]
			currentOffset += toProcess
			continue
		}

		// Open file if needed
		file, openErr := s.openFile(fi)
		if openErr != nil {
			return fmt.Errorf("opening %s: %w", fi.path, openErr)
		}

		// Write data
		if _, writeErr := file.WriteAt(remaining[:toProcess], fileWriteOffset); writeErr != nil {
			return fmt.Errorf("writing to %s: %w", fi.path, writeErr)
		}

		remaining = remaining[toProcess:]
		currentOffset += toProcess
	}

	return nil
}

// verifyPieceHash checks the piece data against expected hash.
// Returns empty string if valid, error message if invalid.
func (s *Server) verifyPieceHash(state *serverTorrentState, pieceIndex int32, data []byte, reqHash string) string {
	// Prefer pre-stored hash from InitTorrent, fall back to request hash
	var expectedHash string
	if int(pieceIndex) < len(state.pieceHashes) && state.pieceHashes[pieceIndex] != "" {
		expectedHash = state.pieceHashes[pieceIndex]
	} else {
		expectedHash = reqHash
	}

	if err := utils.VerifyPieceHash(data, expectedHash); err != nil {
		return err.Error()
	}
	return ""
}

// markPieceWritten updates state tracking after a piece is written.
// Caller must hold state.mu.
func (s *Server) markPieceWritten(ctx context.Context, hash string, state *serverTorrentState, pieceIndex int32) {
	if int(pieceIndex) >= len(state.written) {
		return
	}

	state.written[pieceIndex] = true
	state.dirty = true
	state.piecesSinceFlush++

	flushCount := s.config.StateFlushCount
	if flushCount == 0 {
		flushCount = defaultStateFlushCount
	}

	// Count-based flush: safety net between time-based flushes
	// Also flush immediately when transfer is complete
	writtenCount := countWritten(state.written)
	shouldFlush := state.piecesSinceFlush >= flushCount || writtenCount == len(state.written)

	if shouldFlush && state.statePath != "" {
		if saveErr := s.saveState(state.statePath, state.written); saveErr != nil {
			s.logger.WarnContext(ctx, "failed to save state", "hash", hash, "error", saveErr)
		} else {
			state.dirty = false
			state.piecesSinceFlush = 0
		}
	}
}

// WritePiece receives and writes a single piece.
func (s *Server) WritePiece(
	ctx context.Context,
	req *pb.WritePieceRequest,
) (*pb.WritePieceResponse, error) {
	torrentHash := req.GetTorrentHash()

	s.mu.RLock()
	state, exists := s.torrents[torrentHash]
	s.mu.RUnlock()

	if !exists {
		return &pb.WritePieceResponse{Success: false, Error: "torrent not initialized"}, nil
	}

	pieceIndex := req.GetPieceIndex()
	data := req.GetData()

	// Early check with lock (optimization to skip hash verification in common cases).
	// This is NOT the correctness check - see double-check below after hash verification.
	state.mu.Lock()
	alreadyWritten := int(pieceIndex) < len(state.written) && state.written[pieceIndex]
	isFinalizing := state.finalizing
	state.mu.Unlock()

	if alreadyWritten {
		return &pb.WritePieceResponse{Success: true}, nil
	}

	// Early rejection during finalization (optimization to skip expensive hash verification)
	if isFinalizing {
		return &pb.WritePieceResponse{Success: false, Error: "torrent is being finalized"}, nil
	}

	// Verify piece hash outside lock (pieceHashes is immutable after init).
	// This is CPU-intensive so we don't hold the lock during verification.
	if hashErr := s.verifyPieceHash(state, pieceIndex, data, req.GetPieceHash()); hashErr != "" {
		return &pb.WritePieceResponse{Success: false, Error: hashErr}, nil
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	// CORRECTNESS CHECK: Re-verify finalizing flag under lock.
	// Even if finalization started between the early check and now, this prevents the write.
	if state.finalizing {
		return &pb.WritePieceResponse{Success: false, Error: "torrent is being finalized"}, nil
	}

	// Double-check after acquiring lock to prevent duplicate writes
	if int(pieceIndex) < len(state.written) && state.written[pieceIndex] {
		return &pb.WritePieceResponse{Success: true}, nil
	}

	// Write piece to correct file(s)
	if writeErr := s.writePieceData(state, req.GetOffset(), data); writeErr != nil {
		return &pb.WritePieceResponse{Success: false, Error: fmt.Sprintf("write failed: %v", writeErr)}, nil
	}

	s.markPieceWritten(ctx, torrentHash, state, pieceIndex)

	metrics.PiecesReceivedTotal.Inc()
	metrics.BytesReceivedTotal.Add(float64(len(data)))

	s.logger.DebugContext(ctx, "wrote piece",
		"hash", torrentHash,
		"piece", pieceIndex,
		"size", len(data),
	)

	return &pb.WritePieceResponse{Success: true}, nil
}
