package streaming

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/arsac/qb-sync/internal/metrics"
	pb "github.com/arsac/qb-sync/proto"
)

const (
	// gRPC keepalive parameters.
	keepaliveTime    = 30 * time.Second // Send pings every 30 seconds if no activity
	keepaliveTimeout = 10 * time.Second // Wait 10 seconds for ping ack

	// maxGRPCMessageSize is the maximum gRPC message size for piece transfers.
	// Torrent pieces are commonly 1–16 MB; the default gRPC limit of 4 MB is too small.
	maxGRPCMessageSize = 32 * 1024 * 1024 // 32 MB

	// HTTP/2 flow control window sizes. The default 64 KB initial window requires
	// 262 RTTs to transfer a 16 MB piece, capping first-piece throughput at ~61 MB/s
	// on a 1 ms LAN. Larger windows allow the sender to push data without waiting
	// for WINDOW_UPDATE frames, closing the gap with rsync on fast links.
	initialStreamWindowSize = 16 * 1024 * 1024 // 16 MB per-stream flow control window
	initialConnWindowSize   = 64 * 1024 * 1024 // 64 MB connection-level flow control window

	// finalizeConnTimeout is how long FinalizeTorrent waits for the gRPC
	// connection to become READY before giving up. This prevents fail-fast
	// behavior on unary RPCs when the cold server was recently restarted.
	finalizeConnTimeout = 20 * time.Second

	// maxReconnectBackoff caps gRPC's exponential reconnection backoff.
	// The default (120s) causes long gaps between reconnect attempts, which
	// can exceed finalizeConnTimeout and prevent recovery after a server restart.
	maxReconnectBackoff = 5 * time.Second

	// ackWriteTimeout is how long receiveAcks waits to write an ack to the channel
	// before treating the stream as stuck. If forwardAcks is slow draining the channel,
	// this prevents receiveAcks from blocking forever and never calling Recv() again
	// (which is the only way to detect stream death and trigger ps.cancel()).
	ackWriteTimeout = 30 * time.Second

	// sendTimeout is how long Send waits for the gRPC stream.Send to complete.
	// gRPC Send() blocks on HTTP/2 flow control when the receiver stops consuming.
	// Since Send doesn't accept a context, the caller-side timer cancels the stream
	// context if the sendLoop's stream.Send doesn't return in time.
	sendTimeout = 30 * time.Second
)

// ErrFinalizeVerifying is returned by FinalizeTorrent when the cold server is
// still verifying pieces in the background. The caller should retry later
// without counting this as a failure.
var ErrFinalizeVerifying = errors.New("finalization in progress: cold server is verifying pieces")

// successResponse is implemented by gRPC response types that have Success/Error fields.
type successResponse interface {
	GetSuccess() bool
	GetError() string
}

// checkRPCResponse validates a gRPC response with success/error fields.
func checkRPCResponse(resp successResponse, operation string) error {
	if !resp.GetSuccess() {
		return fmt.Errorf("%s failed: %s", operation, resp.GetError())
	}
	return nil
}

var _ PieceDestination = (*GRPCDestination)(nil)
var _ HardlinkDestination = (*GRPCDestination)(nil)

// GRPCDestination sends pieces to a remote gRPC server.
// It provides PieceDestination-like functionality plus additional features
// for hardlink deduplication, streaming, and torrent lifecycle management.
//
// Multiple TCP connections can be opened to the same server to avoid the
// single-TCP-flow bandwidth ceiling imposed by HTTP/2 multiplexing.
// Streaming RPCs are distributed across connections via round-robin;
// unary RPCs always use the first connection.
type GRPCDestination struct {
	conns       []*grpc.ClientConn
	clients     []pb.QBSyncServiceClient
	streamIdx   atomic.Uint32                 // Lock-free round-robin for OpenStream
	initResults map[string]*InitTorrentResult // Cached init results
	initGroup   singleflight.Group            // Deduplicates concurrent InitTorrent calls
	mu          sync.RWMutex
	closeOnce   sync.Once
	closeErr    error
}

// NewGRPCDestination creates a new gRPC destination client with numConns
// independent TCP connections. Each connection gets its own kernel CUBIC
// window, avoiding the single-TCP-flow bandwidth ceiling. Streaming RPCs
// are distributed across connections via round-robin.
func NewGRPCDestination(addr string, numConns int) (*GRPCDestination, error) {
	if numConns <= 0 {
		numConns = 1
	}

	kaParams := keepalive.ClientParameters{
		Time:                keepaliveTime,
		Timeout:             keepaliveTimeout,
		PermitWithoutStream: true,
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(kaParams),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  1 * time.Second,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   maxReconnectBackoff,
			},
		}),
		grpc.WithInitialWindowSize(initialStreamWindowSize),
		grpc.WithInitialConnWindowSize(initialConnWindowSize),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxGRPCMessageSize),
			grpc.MaxCallSendMsgSize(maxGRPCMessageSize),
		),
	}

	d := &GRPCDestination{
		conns:       make([]*grpc.ClientConn, 0, numConns),
		clients:     make([]pb.QBSyncServiceClient, 0, numConns),
		initResults: make(map[string]*InitTorrentResult),
	}

	for i := range numConns {
		conn, err := grpc.NewClient(addr, opts...)
		if err != nil {
			// Clean up already-opened connections
			for _, c := range d.conns {
				_ = c.Close()
			}
			return nil, fmt.Errorf("failed to connect (conn %d): %w", i, err)
		}
		d.conns = append(d.conns, conn)
		d.clients = append(d.clients, pb.NewQBSyncServiceClient(conn))
	}

	return d, nil
}

// client returns the primary client for unary RPCs (always the first connection).
func (d *GRPCDestination) client() pb.QBSyncServiceClient {
	return d.clients[0]
}

// streamClient returns the next client for streaming RPCs (round-robin across all connections).
func (d *GRPCDestination) streamClient() pb.QBSyncServiceClient {
	if len(d.clients) == 1 {
		return d.clients[0]
	}
	idx := d.streamIdx.Add(1) - 1
	return d.clients[idx%uint32(len(d.clients))]
}

// ValidateConnection checks that the cold server is reachable on all
// connections using the standard gRPC health check protocol (grpc.health.v1.Health).
func (d *GRPCDestination) ValidateConnection(ctx context.Context) error {
	for i, conn := range d.conns {
		healthClient := healthpb.NewHealthClient(conn)
		_, err := healthClient.Check(ctx, &healthpb.HealthCheckRequest{})
		if err != nil {
			return fmt.Errorf("cold server not reachable (conn %d): %w", i, err)
		}
	}
	return nil
}

// InitTorrentResult contains the result of InitTorrent including sync status and hardlink information.
type InitTorrentResult struct {
	Status            pb.TorrentSyncStatus
	PiecesNeeded      []bool
	PiecesNeededCount int32
	PiecesHaveCount   int32
	HardlinkResults   []*pb.HardlinkResult
}

// initTorrentResultFromProto converts a proto response to InitTorrentResult.
func initTorrentResultFromProto(resp *pb.InitTorrentResponse) *InitTorrentResult {
	return &InitTorrentResult{
		Status:            resp.GetStatus(),
		PiecesNeeded:      resp.GetPiecesNeeded(),
		PiecesNeededCount: resp.GetPiecesNeededCount(),
		PiecesHaveCount:   resp.GetPiecesHaveCount(),
		HardlinkResults:   resp.GetHardlinkResults(),
	}
}

// InitTorrent initializes a torrent on the remote server.
// Returns sync status, pieces needed for streaming, and hardlink results.
// Results are cached and returned on subsequent calls for the same torrent.
// Uses singleflight to deduplicate concurrent requests for the same torrent.
func (d *GRPCDestination) InitTorrent(ctx context.Context, req *pb.InitTorrentRequest) (*InitTorrentResult, error) {
	hash := req.GetTorrentHash()

	// Check cache first (fast path)
	d.mu.RLock()
	if result, ok := d.initResults[hash]; ok {
		d.mu.RUnlock()
		return result, nil
	}
	d.mu.RUnlock()

	// Use singleflight to deduplicate concurrent requests for same hash.
	// This avoids holding the lock during RPC which could cause deadlocks.
	v, err, _ := d.initGroup.Do(hash, func() (any, error) {
		// Double-check cache inside singleflight
		d.mu.RLock()
		if result, ok := d.initResults[hash]; ok {
			d.mu.RUnlock()
			return result, nil
		}
		d.mu.RUnlock()

		resp, rpcErr := d.client().InitTorrent(ctx, req)
		if rpcErr != nil {
			return nil, fmt.Errorf("init torrent RPC failed: %w", rpcErr)
		}

		if err := checkRPCResponse(resp, "init torrent"); err != nil {
			return nil, err
		}

		result := initTorrentResultFromProto(resp)

		// Cache the result
		d.mu.Lock()
		d.initResults[hash] = result
		d.mu.Unlock()

		return result, nil
	})

	if err != nil {
		return nil, err
	}
	result, ok := v.(*InitTorrentResult)
	if !ok {
		return nil, fmt.Errorf("unexpected type from singleflight: %T", v)
	}
	return result, nil
}

// IsInitialized returns whether a torrent has been initialized.
func (d *GRPCDestination) IsInitialized(hash string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	_, ok := d.initResults[hash]
	return ok
}

// CheckTorrentStatus queries the cold server for a torrent's current sync status.
// Unlike InitTorrent, this does NOT cache results and does NOT require full torrent info.
// Use this for status checking (is torrent complete/verifying/ready on cold?).
// For full initialization with file tracking and hardlink detection, use InitTorrent.
//
// Note: This sends a minimal request with just the hash. Cold server will check:
// 1. qBittorrent status (returns COMPLETE/VERIFYING if found).
// 2. Existing tracking state (returns READY with pieces_needed if found).
// 3. If neither, returns READY with empty state (caller should do full InitTorrent).
func (d *GRPCDestination) CheckTorrentStatus(ctx context.Context, hash string) (*InitTorrentResult, error) {
	// Create minimal request with just the hash
	req := &pb.InitTorrentRequest{
		TorrentHash: hash,
	}

	resp, err := d.client().InitTorrent(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("check status RPC failed: %w", err)
	}

	if respErr := checkRPCResponse(resp, "check status"); respErr != nil {
		return nil, respErr
	}

	// Return result WITHOUT caching - BidiQueue should still do full InitTorrent
	return initTorrentResultFromProto(resp), nil
}

// WritePiece sends a piece to the remote server (unary, for simple cases).
func (d *GRPCDestination) WritePiece(ctx context.Context, req *pb.WritePieceRequest) error {
	resp, err := d.client().WritePiece(ctx, req)
	if err != nil {
		return fmt.Errorf("write piece RPC failed: %w", err)
	}
	return checkRPCResponse(resp, "write piece")
}

// OpenStream opens a bidirectional stream for high-throughput piece transfer.
// Each stream gets its own cancellable context so a stuck Send() can be
// unblocked without tearing down the entire pool.
func (d *GRPCDestination) OpenStream(ctx context.Context, logger *slog.Logger) (*PieceStream, error) {
	streamCtx, streamCancel := context.WithCancel(ctx)

	stream, err := d.streamClient().StreamPiecesBidi(streamCtx)
	if err != nil {
		streamCancel()
		return nil, fmt.Errorf("failed to open stream: %w", err)
	}

	ps := &PieceStream{
		ctx:      streamCtx,
		cancel:   streamCancel,
		stream:   stream,
		logger:   logger,
		acks:     make(chan *pb.PieceAck, DefaultAckChannelSize),
		ackReady: make(chan struct{}, 1), // Signal when acks are processed
		done:     make(chan struct{}),
		errors:   make(chan error, 1),
		sendCh:   make(chan *sendRequest), // unbuffered: natural backpressure
		stopSend: make(chan struct{}),
		sendDone: make(chan struct{}),
	}

	go ps.receiveAcks()
	go ps.sendLoop()

	return ps, nil
}

// GetFileByInode checks if a file with the given inode exists on the receiver.
func (d *GRPCDestination) GetFileByInode(ctx context.Context, inode uint64) (string, bool, error) {
	resp, err := d.client().GetFileByInode(ctx, &pb.GetFileByInodeRequest{
		Inode: inode,
	})
	if err != nil {
		return "", false, fmt.Errorf("get file by inode RPC failed: %w", err)
	}

	return resp.GetPath(), resp.GetFound(), nil
}

// CreateHardlink creates a hardlink on the receiver from source to target path.
func (d *GRPCDestination) CreateHardlink(ctx context.Context, sourcePath, targetPath string) error {
	resp, err := d.client().CreateHardlink(ctx, &pb.CreateHardlinkRequest{
		SourcePath: sourcePath,
		TargetPath: targetPath,
	})
	if err != nil {
		return fmt.Errorf("create hardlink RPC failed: %w", err)
	}
	return checkRPCResponse(resp, "create hardlink")
}

// RegisterFile registers a completed file for hardlink tracking on the receiver.
func (d *GRPCDestination) RegisterFile(ctx context.Context, inode uint64, path string, size int64) error {
	resp, err := d.client().RegisterFile(ctx, &pb.RegisterFileRequest{
		Inode: inode,
		Path:  path,
		Size:  size,
	})
	if err != nil {
		return fmt.Errorf("register file RPC failed: %w", err)
	}
	return checkRPCResponse(resp, "register file")
}

// FinalizeTorrent requests the cold server to finalize a torrent:
// rename .partial files, add to qBittorrent, verify, and confirm.
// On success, clears the cached init result to prevent memory leaks.
//
// Returns ErrFinalizeVerifying if the cold server is still verifying pieces
// in the background. The caller should retry later without penalty.
func (d *GRPCDestination) FinalizeTorrent(
	ctx context.Context,
	hash, savePath, category, tags, saveSubPath string,
) error {
	// Use WaitForReady so the RPC waits for the connection to recover
	// after a cold server restart instead of failing fast with "connection refused".
	// The per-call timeout prevents blocking the orchestrator loop indefinitely.
	callCtx, cancel := context.WithTimeout(ctx, finalizeConnTimeout)
	defer cancel()

	resp, err := d.client().FinalizeTorrent(callCtx, &pb.FinalizeTorrentRequest{
		TorrentHash: hash,
		SavePath:    savePath,
		Category:    category,
		Tags:        tags,
		SaveSubPath: saveSubPath,
	}, grpc.WaitForReady(true))
	if err != nil {
		return fmt.Errorf("finalize torrent RPC failed: %w", err)
	}
	if respErr := checkRPCResponse(resp, "finalize"); respErr != nil {
		return respErr
	}

	// Cold returns state="verifying" when background verification is still running.
	// Return a sentinel error so the orchestrator can retry without penalty.
	if resp.GetState() == "verifying" {
		return ErrFinalizeVerifying
	}

	// Clean up cached init result to prevent memory leak
	d.ClearInitResult(hash)
	return nil
}

// AbortTorrent requests the cold server to abort an in-progress torrent
// and optionally delete partial files. Called when a torrent is removed
// from hot before streaming completes.
func (d *GRPCDestination) AbortTorrent(ctx context.Context, hash string, deleteFiles bool) (int32, error) {
	// Always clear init results - the server removes tracking regardless of deletion success,
	// and if RPC fails the torrent needs re-initialization anyway
	defer d.ClearInitResult(hash)

	resp, err := d.client().AbortTorrent(ctx, &pb.AbortTorrentRequest{
		TorrentHash: hash,
		DeleteFiles: deleteFiles,
	})
	if err != nil {
		return 0, fmt.Errorf("abort torrent RPC failed: %w", err)
	}
	if respErr := checkRPCResponse(resp, "abort"); respErr != nil {
		return resp.GetFilesDeleted(), respErr
	}

	return resp.GetFilesDeleted(), nil
}

// StartTorrent resumes a stopped torrent on the cold server.
// Called during disk pressure cleanup after hot stops seeding,
// to ensure cold takes over before hot deletes.
func (d *GRPCDestination) StartTorrent(ctx context.Context, hash string) error {
	resp, err := d.client().StartTorrent(ctx, &pb.StartTorrentRequest{
		TorrentHash: hash,
	})
	if err != nil {
		return fmt.Errorf("start torrent RPC failed: %w", err)
	}
	return checkRPCResponse(resp, "start torrent")
}

// ClearInitResult removes a cached init result for a torrent hash.
// Use this when a piece ack indicates the torrent is not initialized on cold,
// so the next send triggers re-initialization.
func (d *GRPCDestination) ClearInitResult(hash string) {
	d.mu.Lock()
	delete(d.initResults, hash)
	d.mu.Unlock()
}

// Close closes all gRPC connections. Safe for repeated calls.
func (d *GRPCDestination) Close() error {
	d.closeOnce.Do(func() {
		var errs []error
		for _, conn := range d.conns {
			if err := conn.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		d.closeErr = errors.Join(errs...)
	})
	return d.closeErr
}

// CleanupStaleEntries removes cached init results for torrents that are no longer active.
// Call this periodically with the set of active torrent hashes to prevent memory leaks
// for transfers that were interrupted without proper finalization or abort.
func (d *GRPCDestination) CleanupStaleEntries(activeHashes map[string]struct{}) int {
	d.mu.Lock()
	defer d.mu.Unlock()

	removed := 0
	for hash := range d.initResults {
		if _, active := activeHashes[hash]; !active {
			delete(d.initResults, hash)
			removed++
		}
	}
	return removed
}

// grpcStatusProvider is implemented by errors that wrap a gRPC status
// (e.g., fmt.Errorf("...: %w", statusErr)). status.FromError only checks
// the outermost error, so we fall back to errors.As for wrapped errors.
type grpcStatusProvider interface {
	GRPCStatus() *status.Status
}

// IsTransientError returns true if the error is a transient gRPC error that may
// succeed on retry (e.g., network issues, server overload).
func IsTransientError(err error) bool {
	s, ok := status.FromError(err)
	if !ok {
		// Fall back to unwrapping wrapped gRPC errors
		var provider grpcStatusProvider
		if errors.As(err, &provider) {
			s = provider.GRPCStatus()
		} else {
			return false
		}
	}
	//nolint:exhaustive // Only specific transient codes are relevant
	switch s.Code() {
	case codes.Unavailable, codes.ResourceExhausted, codes.Aborted, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

// GRPCErrorCode extracts the gRPC status code from an error, if present.
// Returns codes.Unknown if the error is not a gRPC status error.
func GRPCErrorCode(err error) codes.Code {
	if s, ok := status.FromError(err); ok {
		return s.Code()
	}
	// Fall back to unwrapping wrapped gRPC errors
	var provider grpcStatusProvider
	if errors.As(err, &provider) {
		return provider.GRPCStatus().Code()
	}
	return codes.Unknown
}

// sendRequest is a message passed to the sendLoop goroutine.
type sendRequest struct {
	msg   *pb.WritePieceRequest
	errCh chan<- error // Caller waits on this for the result
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
	ctx    context.Context
	cancel context.CancelFunc // Cancels stream context; unblocks stuck Send()
	stream pb.QBSyncService_StreamPiecesBidiClient
	logger *slog.Logger

	acks     chan *pb.PieceAck // Incoming acknowledgments
	ackReady chan struct{}     // Signals when acks have been processed (for backpressure)
	done     chan struct{}     // Closed when receive goroutine exits
	errors   chan error        // Stream errors

	sendCh   chan *sendRequest // Fan-in channel for Send requests → sendLoop
	stopSend chan struct{}     // Closed by CloseSend to signal sendLoop to exit
	sendDone chan struct{}     // Closed when sendLoop exits

	closeSendOnce sync.Once // Protects CloseSend from multiple calls
	closeSendErr  error     // Result of the first CloseSend call

	// Test-overridable timeouts. Zero means use the package-level const.
	ackWriteTimeoutOverride time.Duration
	sendTimeoutOverride     time.Duration
}

func (ps *PieceStream) effectiveAckWriteTimeout() time.Duration {
	if ps.ackWriteTimeoutOverride > 0 {
		return ps.ackWriteTimeoutOverride
	}
	return ackWriteTimeout
}

func (ps *PieceStream) effectiveSendTimeout() time.Duration {
	if ps.sendTimeoutOverride > 0 {
		return ps.sendTimeoutOverride
	}
	return sendTimeout
}

// pieceKey creates a unique key for tracking in-flight pieces.
func pieceKey(hash string, index int32) string {
	return fmt.Sprintf("%s:%d", hash, index)
}

// receiveAcks reads acknowledgments from the stream.
// Exits when context is cancelled or stream ends.
// On exit, cancels the stream context to unblock any Send() stuck on
// HTTP/2 flow control — this is the primary mechanism that breaks the
// deadlock when the receiver stops consuming data.
func (ps *PieceStream) receiveAcks() { //nolint:gocognit // complexity from panic recovery + timeout
	defer close(ps.done)
	defer ps.cancel() // Unblock stuck Send() by cancelling the stream context
	defer func() {
		if r := recover(); r != nil {
			ps.logger.Error("panic in receiveAcks",
				"panic", r,
				"stack", string(debug.Stack()),
			)
			select {
			case ps.errors <- fmt.Errorf("panic in receiveAcks: %v", r):
			default:
			}
		}
	}()

	// Reusable timer for ack channel write timeout.
	// time.NewTimer + Reset avoids per-iteration allocation of time.After.
	timeout := ps.effectiveAckWriteTimeout()
	ackTimer := time.NewTimer(timeout)
	defer ackTimer.Stop()

	for {
		// Check for context cancellation before blocking on Recv
		select {
		case <-ps.ctx.Done():
			metrics.ReceiveAcksExitTotal.WithLabelValues(metrics.ReasonContextCancel).Inc()
			return
		default:
		}

		ack, err := ps.stream.Recv()
		if errors.Is(err, io.EOF) {
			metrics.ReceiveAcksExitTotal.WithLabelValues(metrics.ReasonEOF).Inc()
			return
		}
		if err != nil {
			// Check if this is due to context cancellation
			if ps.ctx.Err() != nil {
				metrics.ReceiveAcksExitTotal.WithLabelValues(metrics.ReasonContextCancel).Inc()
				return
			}
			metrics.ReceiveAcksExitTotal.WithLabelValues(metrics.ReasonStreamError).Inc()
			select {
			case ps.errors <- err:
			default:
			}
			return
		}

		// Signal that an ack was processed (for backpressure relief)
		select {
		case ps.ackReady <- struct{}{}:
		default:
		}

		// Send ack to consumer. If the channel is full for too long, the ack
		// consumer is stuck — exit so defer ps.cancel() fires and unblocks
		// any Send() blocked on HTTP/2 flow control.
		ackTimer.Reset(timeout)
		select {
		case ps.acks <- ack:
			// In Go 1.23+, Stop guarantees the channel is drained.
			ackTimer.Stop()
		case <-ps.ctx.Done():
			metrics.ReceiveAcksExitTotal.WithLabelValues(metrics.ReasonContextCancel).Inc()
			return
		case <-ackTimer.C:
			metrics.ReceiveAcksExitTotal.WithLabelValues(metrics.ReasonAckChannelBlocked).Inc()
			metrics.AckChannelBlockedTotal.Inc()
			ps.logger.Warn("ack channel blocked, closing stream",
				"timeout", timeout,
			)
			return // defer ps.cancel() fires, unblocks stuck Send()
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

// Acks returns the channel of incoming acknowledgments.
func (ps *PieceStream) Acks() <-chan *pb.PieceAck {
	return ps.acks
}

// AckReady returns a channel that signals when acks have been processed.
// Use this for efficient backpressure instead of polling.
func (ps *PieceStream) AckReady() <-chan struct{} {
	return ps.ackReady
}

// Errors returns the channel for stream errors.
func (ps *PieceStream) Errors() <-chan error {
	return ps.errors
}

// Done returns a channel that's closed when the stream ends.
func (ps *PieceStream) Done() <-chan struct{} {
	return ps.done
}

// CloseSend signals that no more pieces will be sent.
// It signals sendLoop to stop via stopSend, waits for it to finish any
// in-progress send, then calls stream.CloseSend(). Safe for concurrent
// and repeated calls via sync.Once. Does not cancel the stream context,
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

// ParsePieceKey extracts the torrent hash and piece index from a piece key.
func ParsePieceKey(key string) (string, int32, bool) {
	// Format is "hash:index"
	lastColon := strings.LastIndexByte(key, ':')
	if lastColon == -1 {
		return "", 0, false
	}

	idx, err := strconv.ParseInt(key[lastColon+1:], 10, 32)
	if err != nil {
		return "", 0, false
	}

	return key[:lastColon], int32(idx), true
}
