// Package source implements the source server for the qb-sync system.
// It monitors qBittorrent for completed pieces, coordinates streaming to
// the destination server, handles torrent lifecycle events (addition, removal),
// and manages the overall synchronization workflow.
package source

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/arsac/qb-sync/internal/arr"
	"github.com/arsac/qb-sync/internal/config"
	"github.com/arsac/qb-sync/internal/health"
	"github.com/arsac/qb-sync/internal/metrics"
	"github.com/arsac/qb-sync/internal/streaming"
)

const (
	// arrPerCallTimeout is the per-HTTP-call timeout for arr API requests.
	arrPerCallTimeout = 3 * time.Second
	// arrCacheDivisor halves the sleep interval to derive the arr verdict cache TTL.
	arrCacheDivisor = 2
	// arrBreakerMaxFailures is the consecutive-failure threshold before the circuit opens.
	arrBreakerMaxFailures = 5
	// arrBreakerResetTimeout is the cool-down period before the circuit moves to half-open.
	arrBreakerResetTimeout = 60 * time.Second
)

// Runner orchestrates the source server tasks.
type Runner struct {
	cfg          *config.SourceConfig
	logger       *slog.Logger
	healthServer *health.Server

	// checkAnnotation checks whether the drain annotation allows draining.
	checkAnnotation func(ctx context.Context, annotationKey string) (bool, error)
}

// NewRunner creates a new source server runner.
func NewRunner(cfg *config.SourceConfig, logger *slog.Logger) *Runner {
	return &Runner{
		cfg:             cfg,
		logger:          logger,
		checkAnnotation: checkDrainAnnotation,
	}
}

// SetHealthServer sets the health server for registering health checks.
func (r *Runner) SetHealthServer(hs *health.Server) {
	r.healthServer = hs
}

// Run starts the source server orchestration.
func (r *Runner) Run(ctx context.Context) error {
	// Connect to destination server
	minConns := r.cfg.MinGRPCConnections
	if minConns <= 0 {
		minConns = 1
	}
	maxConns := max(r.cfg.MaxGRPCConnections, minConns)
	dest, err := streaming.NewGRPCDestination(r.cfg.DestinationAddr, minConns, maxConns)
	if err != nil {
		return fmt.Errorf("connecting to destination server: %w", err)
	}
	defer dest.Close()

	// Validate connection before starting - fail fast if destination server is unreachable
	if validateErr := dest.ValidateConnection(ctx); validateErr != nil {
		return fmt.Errorf("destination server connection validation failed: %w", validateErr)
	}

	metrics.GRPCConnectionsConfigured.Set(float64(maxConns))
	metrics.GRPCConnectionsActive.Set(float64(minConns))
	r.logger.InfoContext(ctx, "connected to destination server",
		"addr", r.cfg.DestinationAddr,
		"minConnections", minConns,
		"maxConnections", maxConns,
	)

	arrCfg := arr.Config{
		Radarr: arr.InstanceConfig{
			URL:        r.cfg.Radarr.URL,
			APIKey:     r.cfg.Radarr.APIKey,
			Categories: r.cfg.Radarr.Categories,
		},
		Sonarr: arr.InstanceConfig{
			URL:        r.cfg.Sonarr.URL,
			APIKey:     r.cfg.Sonarr.APIKey,
			Categories: r.cfg.Sonarr.Categories,
		},
		PerCallTimeout:      arrPerCallTimeout,
		CacheTTL:            r.cfg.SleepInterval / arrCacheDivisor,
		BreakerMaxFailures:  arrBreakerMaxFailures,
		BreakerResetTimeout: arrBreakerResetTimeout,
	}
	arrFilter, arrErr := arr.New(arrCfg, r.logger.With("component", "arr"))
	if arrErr != nil {
		return fmt.Errorf("constructing arr filter: %w", arrErr)
	}

	// Ping results are logged for operator visibility; failures do not gate startup.
	for name, pingErr := range arr.PingAll(ctx, arrFilter) {
		if pingErr != nil {
			r.logger.WarnContext(ctx, "arr instance ping failed at startup",
				"instance", name, "error", pingErr)
		} else {
			r.logger.InfoContext(ctx, "arr instance reachable at startup", "instance", name)
		}
	}

	// Create QBTask with streaming destination
	qbTask, taskErr := NewQBTask(r.cfg, dest, arrFilter, r.logger.With("task", "qb"))
	if taskErr != nil {
		return fmt.Errorf("creating qb task: %w", taskErr)
	}

	prometheus.MustRegister(NewMetricsCollector(qbTask))

	// Register health checks if health server is configured
	if r.healthServer != nil {
		r.healthServer.RegisterCheck("destination", health.GRPCHealthCheck(dest.ValidateConnection))
		r.healthServer.RegisterCheck("qbittorrent", health.QBHealthCheck(qbTask.QBLogin))
		r.healthServer.SetReady(true)
	}

	// Run the task - it handles both orchestration and streaming
	r.logger.InfoContext(ctx, "starting qbittorrent task")
	runErr := qbTask.Run(ctx)

	// On shutdown (SIGTERM), check if we should drain before exiting.
	// Uses a fresh context since ctx is already cancelled.
	if ctx.Err() != nil {
		r.shutdownDrain(qbTask)
	}

	return runErr
}

func (r *Runner) shutdownDrain(task *QBTask) {
	timeout := r.cfg.DrainTimeout
	if timeout == 0 {
		timeout = time.Duration(config.DefaultDrainTimeoutSec) * time.Second
	}
	drainCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if r.cfg.DrainAnnotation != "" {
		allowed, err := r.checkAnnotation(drainCtx, r.cfg.DrainAnnotation)
		if err != nil {
			r.logger.Warn("drain skipped: annotation check failed", "error", err)
			return
		}
		if !allowed {
			r.logger.Info("drain skipped: annotation not set")
			return
		}
	}

	if err := task.Drain(drainCtx); err != nil {
		r.logger.Error("shutdown drain failed", "error", err)
	}
}
