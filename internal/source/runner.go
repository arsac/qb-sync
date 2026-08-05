// Package source implements the source server for the qb-sync system.
// It monitors qBittorrent for completed pieces, coordinates streaming to
// the destination server, handles torrent lifecycle events (addition, removal),
// and manages the overall synchronization workflow.
package source

import (
	"context"
	"errors"
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

	// Create QBTask with streaming destination
	qbTask, taskErr := NewQBTask(r.cfg, dest, r.buildArrFilter(ctx, dest), r.logger.With("task", "qb"))
	if taskErr != nil {
		return fmt.Errorf("creating qb task: %w", taskErr)
	}

	// See destination.Server.Run for the registration rationale: production has
	// one Runner per process so AlreadyRegisteredError never fires; e2e tests
	// run multiple Runners in one process and the first registration wins.
	if regErr := prometheus.Register(NewMetricsCollector(qbTask)); regErr != nil {
		if !errors.As(regErr, new(prometheus.AlreadyRegisteredError)) {
			return fmt.Errorf("registering metrics collector: %w", regErr)
		}
	}

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
			// Error, not warning: the gate could not be evaluated, so a drain
			// the operator asked for silently did not happen. Common causes are
			// a deployment missing POD_NAME/POD_NAMESPACE and an unreachable
			// API server, and both leave synced torrents stranded on the source.
			metrics.ShutdownDrainOutcomesTotal.WithLabelValues(metrics.ResultDrainSkippedFailed).Inc()
			r.logger.Error("drain skipped: annotation check failed",
				"annotation", r.cfg.DrainAnnotation,
				"error", err,
			)
			return
		}
		if !allowed {
			metrics.ShutdownDrainOutcomesTotal.WithLabelValues(metrics.ResultDrainSkippedNotAllow).Inc()
			r.logger.Info("drain skipped: annotation not set", "annotation", r.cfg.DrainAnnotation)
			return
		}
	}

	metrics.ShutdownDrainOutcomesTotal.WithLabelValues(metrics.ResultDrainStarted).Inc()

	if err := task.Drain(drainCtx); err != nil {
		r.logger.Error("shutdown drain failed", "error", err)
	}
}

// buildArrFilter picks how this process obtains *arr verdicts.
//
// Where the instances are configured is the mode, so there is no separate
// switch to set or to disagree with reality. Configured here means they are
// reachable from the source, so query them directly. Not configured means they
// sit with the destination, so relay: the verdict is one bit where the history
// behind it can be a hundred records, and the gRPC connection is already open.
//
// Configuring both sides is not an error, but the local instances win, because
// a round trip to ask someone else about a service this process can already
// reach is pure cost.
func (r *Runner) buildArrFilter(ctx context.Context, dest *streaming.GRPCDestination) arr.Filter {
	logger := r.logger.With("component", "arr")

	if r.cfg.Radarr.IsZero() && r.cfg.Sonarr.IsZero() {
		// Cache in front of the relay. The destination caches its own *arr
		// lookups, but that is on the far side of the link, so without this a
		// rejected torrent costs a round trip every cycle for as long as it
		// exists: it is never tracked, and its tag deliberately does not exclude
		// it from being re-checked.
		return arr.Cached(newRemoteArrFilter(dest.CheckArrRejections, logger), relayVerdictTTL)
	}

	arrCfg := r.cfg.ArrConfig()
	arrCfg.PerCallTimeout = arrPerCallTimeout
	arrCfg.CacheTTL = r.cfg.SleepInterval / arrCacheDivisor
	arrCfg.BreakerMaxFailures = arrBreakerMaxFailures
	arrCfg.BreakerResetTimeout = arrBreakerResetTimeout

	filter, err := arr.New(arrCfg, logger)
	if err != nil {
		// Validation already rejected the shapes worth rejecting, so this is
		// unexpected. Degrade to relaying rather than failing startup: the
		// filter only ever saves work.
		r.logger.ErrorContext(ctx, "arr filter falling back to the destination: local construction failed",
			"error", err)
		return newRemoteArrFilter(dest.CheckArrRejections, logger)
	}

	r.logger.InfoContext(ctx, "arr filter querying locally configured instances")
	for name, pingErr := range arr.PingAll(ctx, filter) {
		if pingErr != nil {
			r.logger.WarnContext(ctx, "arr instance ping failed at startup",
				"instance", name, "error", pingErr)
			continue
		}
		r.logger.InfoContext(ctx, "arr instance reachable at startup", "instance", name)
	}
	return filter
}

// arr tuning for locally configured instances.
const (
	// arrPerCallTimeout bounds a single *arr HTTP request. The filter runs
	// inside the tracking loop, so a hung *arr must not hold up the cycle.
	arrPerCallTimeout = 3 * time.Second

	// arrCacheDivisor derives the SYNC verdict TTL from the cycle interval, so
	// a torrent is re-checked every couple of cycles rather than every one.
	arrCacheDivisor = 2

	arrBreakerMaxFailures  = 5
	arrBreakerResetTimeout = 60 * time.Second

	// relayVerdictTTL bounds how long a relayed verdict is reused. Short on
	// purpose: this cache sits in series with the destination's, so the two
	// staleness windows add, and the cost of being wrong is a torrent that
	// stays skipped a few minutes after *arr changed its mind.
	relayVerdictTTL = 5 * time.Minute
)
