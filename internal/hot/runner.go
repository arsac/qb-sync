// Package hot implements the hot (source) server for the qb-sync system.
// It monitors qBittorrent for completed pieces, coordinates streaming to
// the cold server, handles torrent lifecycle events (addition, removal),
// and manages the overall synchronization workflow.
package hot

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/arsac/qb-sync/internal/config"
	"github.com/arsac/qb-sync/internal/health"
	"github.com/arsac/qb-sync/internal/streaming"
)

// Runner orchestrates the hot server tasks.
type Runner struct {
	cfg          *config.HotConfig
	logger       *slog.Logger
	healthServer *health.Server
}

// NewRunner creates a new hot server runner.
func NewRunner(cfg *config.HotConfig, logger *slog.Logger) *Runner {
	return &Runner{
		cfg:    cfg,
		logger: logger,
	}
}

// SetHealthServer sets the health server for registering health checks.
func (r *Runner) SetHealthServer(hs *health.Server) {
	r.healthServer = hs
}

// Run starts the hot server orchestration.
func (r *Runner) Run(ctx context.Context) error {
	// Connect to cold server
	dest, err := streaming.NewGRPCDestination(r.cfg.ColdAddr)
	if err != nil {
		return fmt.Errorf("connecting to cold server: %w", err)
	}
	defer dest.Close()

	// Validate connection before starting - fail fast if cold server is unreachable
	if validateErr := dest.ValidateConnection(ctx); validateErr != nil {
		return fmt.Errorf("cold server connection validation failed: %w", validateErr)
	}

	r.logger.InfoContext(ctx, "connected to cold server", "addr", r.cfg.ColdAddr)

	// Create QBTask with streaming destination
	qbTask, taskErr := NewQBTask(r.cfg, dest, r.logger.With("task", "qb"))
	if taskErr != nil {
		return fmt.Errorf("creating qb task: %w", taskErr)
	}

	// Register health checks if health server is configured
	if r.healthServer != nil {
		r.healthServer.RegisterCheck("cold", health.GRPCHealthCheck(dest.ValidateConnection))
		r.healthServer.RegisterCheck("qbittorrent", health.QBHealthCheck(qbTask.QBLogin))
		r.healthServer.SetReady(true)
	}

	// Run the task - it handles both orchestration and streaming
	r.logger.InfoContext(ctx, "starting qbittorrent task")
	return qbTask.Run(ctx)
}
