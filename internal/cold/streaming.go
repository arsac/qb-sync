package cold

import (
	"context"
	"errors"
	"io"
	"sync"

	pb "github.com/arsac/qb-sync/proto"
)

// StreamPiecesBidi provides full-duplex streaming for maximum throughput.
// Uses a worker pool to process pieces concurrently while maintaining
// reliable acknowledgment delivery.
func (s *Server) StreamPiecesBidi(stream pb.QBSyncService_StreamPiecesBidiServer) error {
	ctx := stream.Context()

	numWorkers := s.config.StreamWorkers
	if numWorkers <= 0 {
		numWorkers = defaultStreamWorkers
	}

	// Channels for coordination
	workCh := make(chan streamWork, streamWorkQueueSize)
	ackCh := make(chan *pb.PieceAck, streamWorkQueueSize)
	errCh := make(chan error, 1)

	var wg sync.WaitGroup

	// Start worker pool
	for range numWorkers {
		wg.Go(func() {
			s.streamWorker(ctx, workCh, ackCh)
		})
	}

	// Start ack sender goroutine (separate from worker WaitGroup to avoid deadlock)
	ackDone := make(chan struct{})
	go func() {
		defer close(ackDone)
		s.streamAckSender(ctx, stream, ackCh, errCh)
	}()

	// Receive pieces and dispatch to workers
	recvErr := s.streamReceiver(ctx, stream, workCh)

	// Close work channel to signal workers to finish
	close(workCh)

	// Wait for workers to drain their pending work
	wg.Wait()

	// Close ack channel after workers are done sending acks
	close(ackCh)

	// Wait for ack sender to finish sending remaining acks
	<-ackDone

	// Check for ack sender error
	select {
	case sendErr := <-errCh:
		if sendErr != nil {
			return sendErr
		}
	default:
	}

	return recvErr
}

// streamReceiver reads pieces from the stream and dispatches them to workers.
func (s *Server) streamReceiver(
	ctx context.Context,
	stream pb.QBSyncService_StreamPiecesBidiServer,
	workCh chan<- streamWork,
) error {
	for {
		req, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return nil // Client finished sending
		}
		if recvErr != nil {
			return recvErr
		}

		select {
		case workCh <- streamWork{req: req}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// streamWorker processes pieces from the work channel.
func (s *Server) streamWorker(
	ctx context.Context,
	workCh <-chan streamWork,
	ackCh chan<- *pb.PieceAck,
) {
	for work := range workCh {
		req := work.req

		// Process the piece
		resp, writeErr := s.WritePiece(ctx, req)

		// Build acknowledgment
		ack := &pb.PieceAck{
			TorrentHash: req.GetTorrentHash(),
			PieceIndex:  req.GetPieceIndex(),
		}

		switch {
		case writeErr != nil:
			ack.Success = false
			ack.Error = writeErr.Error()
		case !resp.GetSuccess():
			ack.Success = false
			ack.Error = resp.GetError()
		default:
			ack.Success = true
		}

		select {
		case ackCh <- ack:
		case <-ctx.Done():
			return
		}
	}
}

// streamAckSender sends acknowledgments back over the stream.
func (s *Server) streamAckSender(
	_ context.Context,
	stream pb.QBSyncService_StreamPiecesBidiServer,
	ackCh <-chan *pb.PieceAck,
	errCh chan<- error,
) {
	for ack := range ackCh {
		if sendErr := stream.Send(ack); sendErr != nil {
			select {
			case errCh <- sendErr:
			default:
			}
			// Drain remaining acks to let workers finish
			//nolint:revive // intentionally empty - just draining channel
			for range ackCh {
			}
			return
		}
	}
}
