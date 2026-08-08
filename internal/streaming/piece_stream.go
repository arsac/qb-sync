package streaming

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/arsac/qb-sync/internal/metrics"
	pb "github.com/arsac/qb-sync/proto"
)

// sendRequest is a message passed to the sendLoop goroutine.
type sendRequest struct {
	msg   *pb.WritePieceRequest
	errCh chan<- error // Caller waits on this for the result
}

// ackSink consumes everything a stream's receive loop produces. StreamPool is
// the production implementation: it routes acks and errors straight into its
// aggregated channels, so an ack crosses one channel between gRPC and the ack
// processor rather than being relayed by a second per-stream goroutine.
type ackSink interface {
	// deliverAck hands one ack to the sink, blocking until it is accepted or
	// ctx (the stream's own context) is cancelled. Returning false stops the
	// receive loop and so tears the stream down; the sink records why, since
	// only it knows whether the consumer went away or simply stopped keeping
	// up.
	deliverAck(ctx context.Context, ack *pb.PieceAck) bool

	// streamEnded reports that the receive loop exited. err is the stream
	// error, or nil for a clean EOF or a cancelled stream context - a stream
	// that ends without an error still ends, which the sink must notice.
	streamEnded(err error)
}

// PieceStream manages a bidirectional streaming connection for piece transfer.
// This is a thin wrapper around the gRPC stream - in-flight tracking is handled
// by AdaptiveWindow in BidiQueue for congestion control.
//
// Each PieceStream owns a cancellable context derived from the parent. When the
// receive loop detects stream death, it cancels the context to unblock any Send()
// stuck on HTTP/2 flow control.
//
// A dedicated sendLoop goroutine serializes all stream.Send() calls via a channel,
// following the gRPC best practice of a single sender per stream.
type PieceStream struct {
	connIdx int // Index of the gRPC connection this stream uses

	ctx    context.Context
	cancel context.CancelFunc // Cancels stream context; unblocks stuck Send()
	stream pb.QBSyncService_StreamPiecesBidiClient
	logger *slog.Logger

	done chan struct{} // Closed when receive goroutine exits

	sendCh   chan *sendRequest // Fan-in channel for Send requests → sendLoop
	stopSend chan struct{}     // Closed by CloseSend to signal sendLoop to exit
	sendDone chan struct{}     // Closed when sendLoop exits

	closeSendOnce sync.Once // Protects CloseSend from multiple calls
	closeSendErr  error     // Result of the first CloseSend call

	// Test-overridable timeout. Zero means use the package-level const.
	sendTimeoutOverride time.Duration
}

func (ps *PieceStream) effectiveSendTimeout() time.Duration {
	if ps.sendTimeoutOverride > 0 {
		return ps.sendTimeoutOverride
	}
	return sendTimeout
}

// receiveAcks reads acknowledgments from the stream and hands each one to sink.
// Exits when the context is cancelled, the sink stops accepting, or the stream
// ends. On exit it reports the outcome to the sink and then cancels the stream
// context to unblock any Send() stuck on HTTP/2 flow control - this is the
// primary mechanism that breaks the deadlock when the receiver stops consuming
// data, and the only way stream death is detected at all, which is why a sink
// that cannot keep up must stop the loop rather than block it.
//
// The sink is notified before ps.done closes, so anything that waits on Done()
// sees the reason the stream ended already published.
func (ps *PieceStream) receiveAcks(sink ackSink) {
	var exitErr error

	defer close(ps.done)
	defer ps.cancel() // Unblock stuck Send() by cancelling the stream context
	defer func() { sink.streamEnded(exitErr) }()
	defer func() {
		if r := recover(); r != nil {
			exitErr = fmt.Errorf("panic in receiveAcks: %v", r)
			ps.logger.Error("panic in receiveAcks",
				"panic", r,
				"stack", string(debug.Stack()),
			)
		}
	}()

	for {
		// Check for context cancellation before blocking on Recv
		select {
		case <-ps.ctx.Done():
			metrics.ReceiveAcksExitTotal.WithLabelValues(metrics.ReasonContextCancel).Inc()
			return
		default:
		}

		ack, err := ps.stream.Recv()
		switch {
		case errors.Is(err, io.EOF):
			metrics.ReceiveAcksExitTotal.WithLabelValues(metrics.ReasonEOF).Inc()
			return
		case err != nil && ps.ctx.Err() != nil:
			metrics.ReceiveAcksExitTotal.WithLabelValues(metrics.ReasonContextCancel).Inc()
			return
		case err != nil:
			metrics.ReceiveAcksExitTotal.WithLabelValues(metrics.ReasonStreamError).Inc()
			exitErr = err
			return
		}

		if !sink.deliverAck(ps.ctx, ack) {
			return // Sink recorded why; defer ps.cancel() unblocks stuck Send()
		}
	}
}

// sendLoop is the sole goroutine that calls stream.Send(). All Send() callers
// submit requests via sendCh; sendLoop serializes them and responds on errCh.
// Exits when ctx is cancelled or stopSend is closed.
//
// On exit, drains any pending requests from sendCh and responds with a context
// error so callers waiting on errCh don't leak.
func (ps *PieceStream) sendLoop() {
	defer close(ps.sendDone)
	defer func() {
		// Drain pending requests so callers blocked on errCh don't leak.
		// Use ctx.Err() when available (context cancel path), otherwise
		// fall back to a concrete error for the CloseSend path where
		// the context may still be active.
		drainErr := ps.ctx.Err()
		if drainErr == nil {
			drainErr = errors.New("stream send closed")
		}
		for {
			select {
			case req := <-ps.sendCh:
				req.errCh <- drainErr
			default:
				return
			}
		}
	}()

	for {
		select {
		case <-ps.ctx.Done():
			return
		case <-ps.stopSend:
			return
		case req := <-ps.sendCh:
			err := ps.stream.Send(req.msg)
			req.errCh <- err
		}
	}
}

// Send sends a piece over the stream with a timeout.
// Submits the request to the sendLoop goroutine via a channel and waits for the
// result. If the send doesn't complete within the timeout, the stream context is
// cancelled to unblock sendLoop's stream.Send() call.
// This method is safe for concurrent use.
func (ps *PieceStream) Send(req *pb.WritePieceRequest) error {
	// Fast path: stream already cancelled.
	if err := ps.ctx.Err(); err != nil {
		return err
	}

	errCh := make(chan error, 1)
	sr := &sendRequest{msg: req, errCh: errCh}

	// Submit to sendLoop. Unblocks if ctx is cancelled or CloseSend was called.
	select {
	case ps.sendCh <- sr:
	case <-ps.ctx.Done():
		return ps.ctx.Err()
	case <-ps.stopSend:
		return errors.New("stream send closed")
	}

	// Wait for result with timeout.
	timer := time.NewTimer(ps.effectiveSendTimeout())
	defer timer.Stop()

	select {
	case err := <-errCh:
		return err
	case <-timer.C:
		metrics.SendTimeoutTotal.Inc()
		ps.cancel() // Unblock sendLoop's stream.Send via context cancel
		return <-errCh
	case <-ps.ctx.Done():
		return <-errCh
	}
}

// Done returns a channel that's closed when the stream ends.
func (ps *PieceStream) Done() <-chan struct{} {
	return ps.done
}

// CloseSend signals that no more pieces will be sent.
// It signals sendLoop to stop via stopSend, waits for it to finish any
// in-progress send, then calls stream.CloseSend(). Safe for concurrent
// and repeated calls via [sync.Once]. Does not cancel the stream context,
// so receiveAcks continues to drain acks after the send side closes.
func (ps *PieceStream) CloseSend() error {
	ps.closeSendOnce.Do(func() {
		close(ps.stopSend)
		<-ps.sendDone
		ps.closeSendErr = ps.stream.CloseSend()
	})
	return ps.closeSendErr
}

// Close closes the stream and waits for all goroutines to exit.
// This should be called when the stream is no longer needed to ensure clean shutdown.
func (ps *PieceStream) Close() {
	// Cancel stream context first to unblock sendLoop if it's stuck in
	// stream.Send() due to HTTP/2 flow control. This ensures CloseSend's
	// <-ps.sendDone doesn't block indefinitely.
	ps.cancel()

	_ = ps.CloseSend()

	// Wait for receiver goroutine to exit (it will exit when stream ends or errors)
	<-ps.done
}
