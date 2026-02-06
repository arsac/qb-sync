package cold

import (
	"context"
	"fmt"
	"os"
	"time"

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

		if fileEnd <= currentOffset {
			continue
		}

		fileWriteOffset := max(currentOffset-fi.offset, 0)
		availableInFile := fi.size - fileWriteOffset
		toProcess := min(int64(len(remaining)), availableInFile)

		if fi.hlState == hlStateComplete || fi.hlState == hlStatePending {
			remaining = remaining[toProcess:]
			currentOffset += toProcess
			continue
		}

		file, openErr := s.openFile(fi)
		if openErr != nil {
			return fmt.Errorf("opening %s: %w", fi.path, openErr)
		}
		// No per-piece fsync: data integrity is guaranteed by verifyFinalizedPieces
		// which reads back and SHA1-verifies every piece before finalization.
		// Per-piece fsync would severely degrade write throughput on NFS/spinning disks.
		if _, writeErr := file.WriteAt(remaining[:toProcess], fileWriteOffset); writeErr != nil {
			return fmt.Errorf("writing to %s: %w", fi.path, writeErr)
		}

		remaining = remaining[toProcess:]
		currentOffset += toProcess
	}

	return nil
}

// writePieceError builds a failure response with the given message and error code.
func writePieceError(msg string, code pb.PieceErrorCode) *pb.WritePieceResponse {
	return &pb.WritePieceResponse{
		Success:   false,
		Error:     msg,
		ErrorCode: code,
	}
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
	state.writtenCount++
	state.dirty = true
	state.piecesSinceFlush++

	flushCount := s.config.StateFlushCount
	if flushCount == 0 {
		flushCount = defaultStateFlushCount
	}

	shouldFlush := state.piecesSinceFlush >= flushCount || state.writtenCount == len(state.written)

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
	if s.config.DryRun {
		return &pb.WritePieceResponse{Success: true}, nil
	}

	torrentHash := req.GetTorrentHash()

	s.mu.RLock()
	state, exists := s.torrents[torrentHash]
	s.mu.RUnlock()

	if !exists {
		return writePieceError("torrent not initialized", pb.PieceErrorCode_PIECE_ERROR_NOT_INITIALIZED), nil
	}

	if state.initializing {
		return writePieceError("torrent not initialized", pb.PieceErrorCode_PIECE_ERROR_NOT_INITIALIZED), nil
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
		return writePieceError("torrent is being finalized", pb.PieceErrorCode_PIECE_ERROR_FINALIZING), nil
	}

	// Verify piece hash outside lock (pieceHashes is immutable after init).
	// This is CPU-intensive so we don't hold the lock during verification.
	writeStart := time.Now()
	if hashErr := s.verifyPieceHash(state, pieceIndex, data, req.GetPieceHash()); hashErr != "" {
		metrics.PieceWriteDuration.Observe(time.Since(writeStart).Seconds())
		return writePieceError(hashErr, pb.PieceErrorCode_PIECE_ERROR_HASH_MISMATCH), nil
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	// CORRECTNESS CHECK: Re-verify finalizing flag under lock.
	// Even if finalization started between the early check and now, this prevents the write.
	if state.finalizing {
		return writePieceError("torrent is being finalized", pb.PieceErrorCode_PIECE_ERROR_FINALIZING), nil
	}

	if int(pieceIndex) < len(state.written) && state.written[pieceIndex] {
		return &pb.WritePieceResponse{Success: true}, nil
	}

	if writeErr := s.writePieceData(state, req.GetOffset(), data); writeErr != nil {
		return writePieceError(fmt.Sprintf("write failed: %v", writeErr), pb.PieceErrorCode_PIECE_ERROR_IO), nil
	}

	s.markPieceWritten(ctx, torrentHash, state, pieceIndex)
	metrics.PieceWriteDuration.Observe(time.Since(writeStart).Seconds())

	metrics.PiecesReceivedTotal.Inc()
	metrics.BytesReceivedTotal.Add(float64(len(data)))

	s.logger.DebugContext(ctx, "wrote piece",
		"hash", torrentHash,
		"piece", pieceIndex,
		"size", len(data),
	)

	return &pb.WritePieceResponse{Success: true}, nil
}
