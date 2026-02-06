package health

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/arsac/qb-sync/internal/metrics"
)

const (
	readHeaderTimeout = 5 * time.Second
	shutdownTimeout   = 5 * time.Second
	checkTimeout      = 5 * time.Second
)

// CheckFunc is a function that checks a component's health.
// It should return nil if healthy, or an error describing the issue.
type CheckFunc func(ctx context.Context) error

// Server provides HTTP health endpoints for Kubernetes probes.
type Server struct {
	addr   string
	logger *slog.Logger
	server *http.Server

	ready   atomic.Bool
	checks  map[string]CheckFunc
	checkMu sync.RWMutex
}

// Config configures the health server.
type Config struct {
	Addr string // Listen address (e.g., ":8080")
}

// NewServer creates a new health server.
func NewServer(cfg Config, logger *slog.Logger) *Server {
	s := &Server{
		addr:   cfg.Addr,
		logger: logger,
		checks: make(map[string]CheckFunc),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/livez", s.handleLivez)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.Handle("/metrics", promhttp.Handler())

	s.server = &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
	}

	return s
}

// RegisterCheck adds a named health check.
func (s *Server) RegisterCheck(name string, check CheckFunc) {
	s.checkMu.Lock()
	defer s.checkMu.Unlock()
	s.checks[name] = check
}

// SetReady marks the server as ready to receive traffic.
func (s *Server) SetReady(ready bool) {
	s.ready.Store(ready)
}

// Run starts the health server and blocks until context is cancelled.
func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		s.logger.InfoContext(ctx, "starting health server", "addr", s.addr)
		if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := s.server.Shutdown(shutdownCtx); err != nil {
			s.logger.WarnContext(ctx, "health server shutdown error", "error", err)
		}
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

// Response is the JSON response for health endpoints.
type Response struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}

// handleHealthz is a basic health check - returns 200 if the process is alive.
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	s.writeJSON(w, http.StatusOK, Response{Status: "ok"})
}

// handleLivez checks if the process is alive and not deadlocked.
// Runs all registered health checks.
func (s *Server) handleLivez(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), checkTimeout)
	defer cancel()

	checkResults, allHealthy := s.runChecks(ctx)

	resp := Response{
		Status: "ok",
		Checks: checkResults,
	}

	if !allHealthy {
		resp.Status = "unhealthy"
		s.writeJSON(w, http.StatusServiceUnavailable, resp)
		return
	}

	s.writeJSON(w, http.StatusOK, resp)
}

// handleReadyz checks if the server is ready to receive traffic.
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if !s.ready.Load() {
		s.writeJSON(w, http.StatusServiceUnavailable, Response{Status: "not ready"})
		return
	}

	// Also run liveness checks for readiness
	ctx, cancel := context.WithTimeout(r.Context(), checkTimeout)
	defer cancel()

	checkResults, allHealthy := s.runChecks(ctx)

	resp := Response{
		Status: "ok",
		Checks: checkResults,
	}

	if !allHealthy {
		resp.Status = "not ready"
		s.writeJSON(w, http.StatusServiceUnavailable, resp)
		return
	}

	s.writeJSON(w, http.StatusOK, resp)
}

// runChecks executes all registered health checks and returns results.
// Returns a map of check names to their status strings, and whether all checks passed.
func (s *Server) runChecks(ctx context.Context) (map[string]string, bool) {
	checks := s.copyChecks()
	results := make(map[string]string, len(checks))
	allHealthy := true

	for name, check := range checks {
		if err := check(ctx); err != nil {
			results[name] = err.Error()
			allHealthy = false
		} else {
			results[name] = "ok"
		}
	}

	return results, allHealthy
}

// copyChecks returns a copy of the checks map for safe iteration.
func (s *Server) copyChecks() map[string]CheckFunc {
	s.checkMu.RLock()
	defer s.checkMu.RUnlock()
	checks := make(map[string]CheckFunc, len(s.checks))
	maps.Copy(checks, s.checks)
	return checks
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, resp Response) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.logger.Warn("failed to write health response", "error", err)
	}
}

// CachedCheck wraps a CheckFunc with time-based caching. Repeated calls within
// the TTL return the cached result without invoking the underlying check.
func CachedCheck(check CheckFunc, ttl time.Duration) CheckFunc {
	var (
		mu         sync.Mutex
		lastCheck  time.Time
		lastResult error
	)
	return func(ctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		if time.Since(lastCheck) < ttl {
			metrics.HealthCheckCacheTotal.WithLabelValues(metrics.ResultHit).Inc()
			return lastResult
		}
		metrics.HealthCheckCacheTotal.WithLabelValues(metrics.ResultMiss).Inc()
		lastResult = check(ctx)
		lastCheck = time.Now()
		return lastResult
	}
}

// QBHealthCheck returns a check function that verifies qBittorrent connectivity.
func QBHealthCheck(login func(ctx context.Context) error) CheckFunc {
	return func(ctx context.Context) error {
		if err := login(ctx); err != nil {
			return fmt.Errorf("qbittorrent: %w", err)
		}
		return nil
	}
}

// GRPCHealthCheck returns a check function that verifies gRPC connectivity.
func GRPCHealthCheck(validate func(ctx context.Context) error) CheckFunc {
	return func(ctx context.Context) error {
		if err := validate(ctx); err != nil {
			return fmt.Errorf("grpc: %w", err)
		}
		return nil
	}
}
