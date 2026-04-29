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
	"github.com/bits-and-blooms/bitset"
	"golang.org/x/sync/semaphore"

	"github.com/arsac/qb-sync/internal/grpcutil"
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
//  1. store.mu - store-level lock for torrents map, aborting, filePaths
//  2. state.mu - per-torrent lock for torrent state
//  3. fi.fileMu - per-file lock for file handle open/close/write
//  4. store.inodes.registeredMu - lock for inode-to-path mapping
//  5. store.inodes.inProgressMu - lock for in-progress inode tracking
//
// Always acquire locks in the order above. Release store.mu before acquiring
// state.mu when possible to reduce contention. The inode locks (4, 5) may
// be acquired independently when store.mu and state.mu are not held.
// fileMu (3) may be acquired with or without state.mu held.
type Server struct {
	pb.UnimplementedQBSyncServiceServer

	config ServerConfig
	logger *slog.Logger
	server *grpc.Server
	store  *torrentStore

	// qBittorrent client for adding verified torrents (destination server only)
	qbClient qbclient.Client

	// Global memory budget for buffered piece data
	memBudget *semaphore.Weighted

	// finalizeSem serializes background finalizations so only one torrent
	// is verified/added to qBittorrent at a time. This prevents disk I/O
	// and qBittorrent API saturation when many torrents complete together.
	finalizeSem *semaphore.Weighted

	// bgCtx is cancelled during shutdown to interrupt in-flight background
	// finalizations. bgWg tracks all running background finalization goroutines
	// so cleanup() waits for them to exit before saving state and closing handles.
	bgCtx    context.Context
	bgCancel context.CancelFunc
	bgWg     sync.WaitGroup

	// Health server for K8s probes
	healthServer *health.Server

	// saveStateFunc overrides saveState for testing. nil in production.
	saveStateFunc func(path string, written *bitset.BitSet) error
}

// NewServer creates a new gRPC piece receiver server.
func NewServer(config ServerConfig, logger *slog.Logger) *Server {
	bufferBytes := config.MaxStreamBufferBytes
	if bufferBytes <= 0 {
		bufferBytes = defaultMaxStreamBufferMB * grpcutil.BytesPerMB
	}

	bgCtx, bgCancel := context.WithCancel(
		context.Background(),
	)

	s := &Server{
		config:      config,
		logger:      logger,
		store:       newTorrentStore(config.BasePath, logger),
		memBudget:   semaphore.NewWeighted(bufferBytes),
		finalizeSem: semaphore.NewWeighted(1),
		bgCtx:       bgCtx,
		bgCancel:    bgCancel,
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

	return s
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

	// Recover in-flight torrents from persisted metadata before accepting
	// any gRPC requests or starting background goroutines.
	if recoverErr := s.recoverInFlightTorrents(ctx); recoverErr != nil {
		s.logger.ErrorContext(ctx, "failed to recover torrents", "error", recoverErr)
	}

	s.server = grpc.NewServer(
		grpc.MaxRecvMsgSize(grpcutil.MaxGRPCMessageSize),
		grpc.MaxSendMsgSize(grpcutil.MaxGRPCMessageSize),
		grpc.InitialWindowSize(grpcutil.InitialStreamWindowSize),
		grpc.InitialConnWindowSize(grpcutil.InitialConnWindowSize),
		grpc.ReadBufferSize(grpcutil.TransportBufferSize),
		grpc.WriteBufferSize(grpcutil.TransportBufferSize),
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

		// Cancel background finalizations and wait for them to exit before
		// cleanup. This prevents races where cleanup saves state or closes
		// file handles while a finalization goroutine is still running.
		s.bgCancel()
		s.bgWg.Wait()

		s.cleanup()
		return ctx.Err()
	case serveErr := <-errCh:
		return serveErr
	}
}

// cleanup closes all file handles and saves state before shutdown.
func (s *Server) cleanup() {
	torrents := s.store.Drain()

	for hash, state := range torrents {
		state.mu.Lock()
		if state.dirty && state.statePath != "" {
			if saveErr := s.saveState(state.statePath, state.written); saveErr != nil {
				s.logger.Warn("failed to save state on cleanup",
					"hash", hash,
					"error", saveErr,
				)
			} else {
				state.flushGen++
			}
		}
		for _, fi := range state.files {
			_ = s.closeFileHandle(context.Background(), hash, fi)
		}
		state.mu.Unlock()
	}
}
