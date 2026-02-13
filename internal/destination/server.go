// Package destination implements the destination server for the qb-sync system.
// It receives torrent pieces over gRPC, writes them to disk, and manages the
// full torrent lifecycle including initialization, piece verification,
// hardlink deduplication, finalization, and qBittorrent integration.
package destination

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/autobrr/go-qbittorrent"
	"golang.org/x/sync/semaphore"

	"github.com/arsac/qb-sync/internal/health"
	"github.com/arsac/qb-sync/internal/metrics"
	"github.com/arsac/qb-sync/internal/qbclient"
	pb "github.com/arsac/qb-sync/proto"

	"google.golang.org/grpc"
	grpchealth "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
)

const (
	// healthCheckCacheTTL is how long a qBittorrent health check result is cached
	// before re-running the actual check. Prevents excessive login calls on every
	// K8s probe request.
	healthCheckCacheTTL = 30 * time.Second

	// maxGRPCMessageSize is the maximum gRPC message size for piece transfers.
	// Torrent pieces are commonly 1-16 MB; the default gRPC limit of 4 MB is too small.
	maxGRPCMessageSize = 32 * 1024 * 1024 // 32 MB

	// HTTP/2 flow control window sizes. Must match the client (source) side.
	// The default 64 KB window throttles bulk piece transfers — the receiver
	// can only buffer 64 KB before sending WINDOW_UPDATE, forcing the sender
	// to stall on every round trip. Larger windows let the sender push data
	// continuously without waiting for per-RTT acknowledgments.
	initialStreamWindowSize = 16 * 1024 * 1024 // 16 MB per-stream flow control window
	initialConnWindowSize   = 64 * 1024 * 1024 // 64 MB connection-level flow control window

	// gracefulShutdownTimeout is how long GracefulStop waits for active streams
	// to finish before force-stopping. Long-lived bidirectional streams (piece
	// streaming) can block shutdown indefinitely without this timeout.
	gracefulShutdownTimeout = 10 * time.Second

	// gRPC keepalive parameters for the destination server.
	keepalivePingInterval = 30 * time.Second // Send pings if no activity
	keepalivePingTimeout  = 10 * time.Second // Wait for ping ack before closing
	keepaliveMinPingTime  = 15 * time.Second // Minimum allowed client ping frequency
)

// Server receives pieces over gRPC and writes them to disk.
//
// Lock ordering (to prevent deadlocks):
//  1. s.mu - server-level lock for torrents map and abortingHashes
//  2. state.mu - per-torrent lock for torrent state
//  3. s.inodes.registeredMu - lock for inode-to-path mapping
//  4. s.inodes.inProgressMu - lock for in-progress inode tracking
//
// Always acquire locks in the order above. Release s.mu before acquiring
// state.mu when possible to reduce contention. The inode locks (3, 4) may
// be acquired independently when s.mu and state.mu are not held.
type Server struct {
	pb.UnimplementedQBSyncServiceServer

	config   ServerConfig
	logger   *slog.Logger
	server   *grpc.Server
	torrents map[string]*serverTorrentState
	mu       sync.RWMutex // Protects torrents and abortingHashes

	// Abort tracking: prevents race between AbortTorrent and InitTorrent.
	// When a hash is being aborted, the channel is present and open.
	// InitTorrent waits for the channel to close before proceeding.
	// AbortTorrent closes the channel when cleanup is complete.
	abortingHashes map[string]chan struct{}

	// Inode registry for hardlink deduplication
	inodes *InodeRegistry

	// qBittorrent client for adding verified torrents (destination server only)
	qbClient qbclient.Client

	// Global memory budget for buffered piece data
	memBudget *semaphore.Weighted

	// finalizeSem serializes background finalizations so only one torrent
	// is verified/added to qBittorrent at a time. This prevents disk I/O
	// and qBittorrent API saturation when many torrents complete together.
	finalizeSem *semaphore.Weighted

	// Health server for K8s probes
	healthServer *health.Server

	// saveStateFunc overrides saveState for testing. nil in production.
	saveStateFunc func(path string, written []bool) error
}

// NewServer creates a new gRPC piece receiver server.
func NewServer(config ServerConfig, logger *slog.Logger) *Server {
	bufferBytes := config.MaxStreamBufferBytes
	if bufferBytes <= 0 {
		bufferBytes = defaultMaxStreamBufferMB * bytesPerMB
	}

	s := &Server{
		config:         config,
		logger:         logger,
		torrents:       make(map[string]*serverTorrentState),
		abortingHashes: make(map[string]chan struct{}),
		inodes:         NewInodeRegistry(config.BasePath, logger),
		memBudget:      semaphore.NewWeighted(bufferBytes),
		finalizeSem:    semaphore.NewWeighted(1),
	}

	if config.QB != nil && config.QB.URL != "" {
		rawClient := qbittorrent.NewClient(qbittorrent.Config{
			Host:     config.QB.URL,
			Username: config.QB.Username,
			Password: config.QB.Password,
		})
		qbConfig := qbclient.DefaultConfig()
		s.qbClient = qbclient.NewResilientClient(
			rawClient,
			qbConfig,
			logger.With("component", "destination-qb-client"),
			metrics.ModeDestination,
		)
	}

	if loadErr := s.inodes.Load(); loadErr != nil {
		logger.Warn("failed to load inode map, starting fresh", "error", loadErr)
	}

	return s
}

// collectTorrents returns a snapshot of all torrents for safe iteration.
// The caller should acquire s.mu.RLock() or s.mu.Lock() before calling.
// Returns a slice that can be iterated after releasing s.mu.
func (s *Server) collectTorrents() []torrentRef {
	refs := make([]torrentRef, 0, len(s.torrents))
	for hash, state := range s.torrents {
		refs = append(refs, torrentRef{hash: hash, state: state})
	}
	return refs
}

// SetHealthServer sets the health server for registering health checks.
func (s *Server) SetHealthServer(hs *health.Server) {
	s.healthServer = hs
}

// Run starts the gRPC server and blocks until context is cancelled.
func (s *Server) Run(ctx context.Context) error {
	lc := net.ListenConfig{}
	listener, err := lc.Listen(ctx, "tcp", s.config.ListenAddr)
	if err != nil {
		return err
	}

	s.server = grpc.NewServer(
		grpc.MaxRecvMsgSize(maxGRPCMessageSize),
		grpc.MaxSendMsgSize(maxGRPCMessageSize),
		grpc.InitialWindowSize(initialStreamWindowSize),
		grpc.InitialConnWindowSize(initialConnWindowSize),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    keepalivePingInterval, // Send pings every 30s if no activity
			Timeout: keepalivePingTimeout,  // Wait 10s for ping ack
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             keepaliveMinPingTime, // Allow client pings as frequent as 15s
			PermitWithoutStream: true,                 // Allow pings even when no active streams
		}),
	)
	pb.RegisterQBSyncServiceServer(s.server, s)

	// Register standard gRPC health service
	grpcHealthServer := grpchealth.NewServer()
	healthpb.RegisterHealthServer(s.server, grpcHealthServer)
	grpcHealthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	// Start background state flusher
	go s.runStateFlusher(ctx)

	// Start background orphan cleanup
	go s.runOrphanCleaner(ctx)

	// Start background inode cleanup
	go s.runInodeCleaner(ctx)

	// Register health checks if health server is configured
	if s.healthServer != nil {
		if s.qbClient != nil {
			s.healthServer.RegisterCheck("qbittorrent",
				health.CachedCheck(health.QBHealthCheck(s.qbClient.LoginCtx), healthCheckCacheTTL))
		}
		s.healthServer.SetReady(true)
	}

	s.logger.InfoContext(ctx, "starting gRPC server", "addr", s.config.ListenAddr)

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.server.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		s.logger.InfoContext(ctx, "shutting down gRPC server")

		// Try graceful shutdown first, then force-stop after timeout.
		// GracefulStop blocks until all active RPCs finish — long-lived
		// bidirectional streams (piece streaming) can block indefinitely.
		stopped := make(chan struct{})
		go func() {
			s.server.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(gracefulShutdownTimeout):
			s.logger.WarnContext(ctx, "graceful shutdown timed out, forcing stop")
			s.server.Stop()
			<-stopped // Wait for GracefulStop to return after Stop
		}

		s.cleanup()
		return ctx.Err()
	case serveErr := <-errCh:
		return serveErr
	}
}

// cleanup closes all file handles and saves state before shutdown.
func (s *Server) cleanup() {
	s.mu.Lock()
	torrents := s.collectTorrents()
	s.torrents = make(map[string]*serverTorrentState)
	s.mu.Unlock()

	for _, t := range torrents {
		t.state.mu.Lock()
		if t.state.dirty && t.state.statePath != "" {
			if saveErr := s.saveState(t.state.statePath, t.state.written); saveErr != nil {
				s.logger.Warn("failed to save state on cleanup",
					"hash", t.hash,
					"error", saveErr,
				)
			}
		}
		for _, fi := range t.state.files {
			_ = s.closeFileHandle(context.Background(), t.hash, fi)
		}
		t.state.mu.Unlock()
	}

	if saveErr := s.inodes.Save(); saveErr != nil {
		s.logger.Warn("failed to save inode map on cleanup", "error", saveErr)
	}
}
