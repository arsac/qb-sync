package qbclient

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/autobrr/go-qbittorrent"
	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/circuitbreaker"

	"github.com/arsac/qb-sync/internal/metrics"
	"github.com/arsac/qb-sync/internal/utils"
)

// Client defines the interface for qBittorrent client operations.
// This interface allows for mocking in tests.
type Client interface {
	LoginCtx(ctx context.Context) error
	GetAppPreferencesCtx(ctx context.Context) (qbittorrent.AppPreferences, error)
	GetTorrentsCtx(ctx context.Context, opts qbittorrent.TorrentFilterOptions) ([]qbittorrent.Torrent, error)
	GetTorrentPieceStatesCtx(ctx context.Context, hash string) ([]qbittorrent.PieceState, error)
	GetTorrentPieceHashesCtx(ctx context.Context, hash string) ([]string, error)
	GetTorrentPropertiesCtx(ctx context.Context, hash string) (qbittorrent.TorrentProperties, error)
	GetFilesInformationCtx(ctx context.Context, hash string) (*qbittorrent.TorrentFiles, error)
	ExportTorrentCtx(ctx context.Context, hash string) ([]byte, error)
	DeleteTorrentsCtx(ctx context.Context, hashes []string, deleteFiles bool) error
	AddTagsCtx(ctx context.Context, hashes []string, tags string) error
	StopCtx(ctx context.Context, hashes []string) error
	ResumeCtx(ctx context.Context, hashes []string) error
	AddTorrentFromMemoryCtx(ctx context.Context, buf []byte, options map[string]string) error
}

// Ensure ResilientClient implements Client interface.
var _ Client = (*ResilientClient)(nil)

// Config configures the resilient qBittorrent client.
type Config struct {
	Retry          utils.RetryConfig
	CircuitBreaker *utils.CircuitBreakerConfig // nil to disable circuit breaker
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		Retry: utils.DefaultRetryConfig(),
		CircuitBreaker: &utils.CircuitBreakerConfig{
			MaxFailures:  5,
			ResetTimeout: 30 * time.Second,
		},
	}
}

// ResilientClient wraps a qBittorrent client with retry and circuit breaker logic.
type ResilientClient struct {
	client   *qbittorrent.Client
	logger   *slog.Logger
	executor failsafe.Executor[any]
	cb       circuitbreaker.CircuitBreaker[any] // nil if CB disabled; for state inspection
	mode     string                             // metrics label: "hot" or "cold"
}

// NewResilientClient creates a new resilient qBittorrent client.
// mode is the metrics label identifying the caller ("hot" or "cold").
func NewResilientClient(
	client *qbittorrent.Client,
	config Config,
	logger *slog.Logger,
	mode string,
) *ResilientClient {
	// Wrap the retriable checker to explicitly reject circuitbreaker.ErrOpen,
	// so retry aborts immediately when the circuit breaker is open.
	retryConfig := config.Retry
	originalChecker := retryConfig.RetriableChecker
	if originalChecker == nil {
		originalChecker = utils.IsRetriableError
	}
	retryConfig.RetriableChecker = func(err error) bool {
		if errors.Is(err, circuitbreaker.ErrOpen) {
			return false
		}
		return originalChecker(err)
	}

	retryPolicy := utils.NewRetryPolicy(retryConfig, logger, func() {
		metrics.QBClientRetriesTotal.Inc()
	})

	var cb circuitbreaker.CircuitBreaker[any]
	var policies []failsafe.Policy[any]

	if config.CircuitBreaker != nil {
		cb = circuitbreaker.NewBuilder[any]().
			WithFailureThreshold(uint(config.CircuitBreaker.MaxFailures)).
			WithSuccessThreshold(1).
			WithDelay(config.CircuitBreaker.ResetTimeout).
			HandleIf(func(_ any, err error) bool {
				return utils.IsCircuitBreakerFailure(err)
			}).
			OnOpen(func(e circuitbreaker.StateChangedEvent) {
				logger.Warn("circuit breaker opened")
			}).
			OnHalfOpen(func(e circuitbreaker.StateChangedEvent) {
				logger.Info("circuit breaker half-open, probing")
			}).
			OnClose(func(e circuitbreaker.StateChangedEvent) {
				logger.Info("circuit breaker closed")
			}).
			Build()
		// Retry wraps CircuitBreaker: each retry attempt checks CB first.
		// If CB is open, circuitbreaker.ErrOpen is returned, and the
		// retry HandleIf rejects it immediately (no pointless retries).
		policies = []failsafe.Policy[any]{retryPolicy, cb}
	} else {
		policies = []failsafe.Policy[any]{retryPolicy}
	}

	return &ResilientClient{
		client:   client,
		logger:   logger,
		executor: failsafe.With(policies...),
		cb:       cb,
		mode:     mode,
	}
}

// Client returns the underlying qBittorrent client for operations that don't need retry.
func (r *ResilientClient) Client() *qbittorrent.Client {
	return r.client
}

// LoginCtx logs in to qBittorrent with retry.
func (r *ResilientClient) LoginCtx(ctx context.Context) error {
	return r.runVoid(ctx, "Login", func(ctx context.Context) error {
		return r.client.LoginCtx(ctx)
	})
}

// GetAppPreferencesCtx gets qBittorrent application preferences with retry.
func (r *ResilientClient) GetAppPreferencesCtx(ctx context.Context) (qbittorrent.AppPreferences, error) {
	return runWithResult(ctx, r.executor, r.mode, "GetAppPreferences",
		func(ctx context.Context) (qbittorrent.AppPreferences, error) {
			return r.client.GetAppPreferencesCtx(ctx)
		})
}

// GetTorrentsCtx gets torrents with retry.
func (r *ResilientClient) GetTorrentsCtx(
	ctx context.Context,
	opts qbittorrent.TorrentFilterOptions,
) ([]qbittorrent.Torrent, error) {
	return runWithResult(ctx, r.executor, r.mode, "GetTorrents",
		func(ctx context.Context) ([]qbittorrent.Torrent, error) {
			return r.client.GetTorrentsCtx(ctx, opts)
		})
}

// GetTorrentPieceStatesCtx gets piece states with retry.
func (r *ResilientClient) GetTorrentPieceStatesCtx(
	ctx context.Context,
	hash string,
) ([]qbittorrent.PieceState, error) {
	return runWithResult(ctx, r.executor, r.mode, "GetTorrentPieceStates",
		func(ctx context.Context) ([]qbittorrent.PieceState, error) {
			return r.client.GetTorrentPieceStatesCtx(ctx, hash)
		})
}

// GetTorrentPieceHashesCtx gets piece hashes with retry.
func (r *ResilientClient) GetTorrentPieceHashesCtx(
	ctx context.Context,
	hash string,
) ([]string, error) {
	return runWithResult(ctx, r.executor, r.mode, "GetTorrentPieceHashes",
		func(ctx context.Context) ([]string, error) {
			return r.client.GetTorrentPieceHashesCtx(ctx, hash)
		})
}

// GetTorrentPropertiesCtx gets torrent properties with retry.
func (r *ResilientClient) GetTorrentPropertiesCtx(
	ctx context.Context,
	hash string,
) (qbittorrent.TorrentProperties, error) {
	return runWithResult(ctx, r.executor, r.mode, "GetTorrentProperties",
		func(ctx context.Context) (qbittorrent.TorrentProperties, error) {
			return r.client.GetTorrentPropertiesCtx(ctx, hash)
		})
}

// GetFilesInformationCtx gets file information with retry.
func (r *ResilientClient) GetFilesInformationCtx(
	ctx context.Context,
	hash string,
) (*qbittorrent.TorrentFiles, error) {
	return runWithResult(ctx, r.executor, r.mode, "GetFilesInformation",
		func(ctx context.Context) (*qbittorrent.TorrentFiles, error) {
			return r.client.GetFilesInformationCtx(ctx, hash)
		})
}

// ExportTorrentCtx exports a torrent file with retry.
func (r *ResilientClient) ExportTorrentCtx(
	ctx context.Context,
	hash string,
) ([]byte, error) {
	return runWithResult(ctx, r.executor, r.mode, "ExportTorrent",
		func(ctx context.Context) ([]byte, error) {
			return r.client.ExportTorrentCtx(ctx, hash)
		})
}

// DeleteTorrentsCtx deletes torrents with retry.
func (r *ResilientClient) DeleteTorrentsCtx(
	ctx context.Context,
	hashes []string,
	deleteFiles bool,
) error {
	return r.runVoid(ctx, "DeleteTorrents", func(ctx context.Context) error {
		return r.client.DeleteTorrentsCtx(ctx, hashes, deleteFiles)
	})
}

// AddTagsCtx adds tags to torrents with retry.
func (r *ResilientClient) AddTagsCtx(
	ctx context.Context,
	hashes []string,
	tags string,
) error {
	return r.runVoid(ctx, "AddTags", func(ctx context.Context) error {
		return r.client.AddTagsCtx(ctx, hashes, tags)
	})
}

// StopCtx pauses torrents with retry.
func (r *ResilientClient) StopCtx(
	ctx context.Context,
	hashes []string,
) error {
	return r.runVoid(ctx, "Stop", func(ctx context.Context) error {
		return r.client.StopCtx(ctx, hashes)
	})
}

// ResumeCtx resumes torrents with retry.
func (r *ResilientClient) ResumeCtx(
	ctx context.Context,
	hashes []string,
) error {
	return r.runVoid(ctx, "Resume", func(ctx context.Context) error {
		return r.client.ResumeCtx(ctx, hashes)
	})
}

// AddTorrentFromMemoryCtx adds a torrent from memory with retry.
func (r *ResilientClient) AddTorrentFromMemoryCtx(
	ctx context.Context,
	buf []byte,
	options map[string]string,
) error {
	return r.runVoid(ctx, "AddTorrentFromMemory", func(ctx context.Context) error {
		return r.client.AddTorrentFromMemoryCtx(ctx, buf, options)
	})
}

// runVoid executes a void operation through the executor.
func (r *ResilientClient) runVoid(
	ctx context.Context,
	operation string,
	fn func(ctx context.Context) error,
) error {
	metrics.QBAPICallsTotal.WithLabelValues(r.mode, operation).Inc()
	start := time.Now()
	err := r.executor.WithContext(ctx).Run(func() error {
		return fn(ctx)
	})
	metrics.QBAPICallDuration.WithLabelValues(r.mode, operation).Observe(time.Since(start).Seconds())
	return err
}

// runWithResult executes a typed operation through the executor.
func runWithResult[T any](
	ctx context.Context,
	executor failsafe.Executor[any],
	mode string,
	operation string,
	fn func(ctx context.Context) (T, error),
) (T, error) {
	metrics.QBAPICallsTotal.WithLabelValues(mode, operation).Inc()
	start := time.Now()
	result, err := executor.WithContext(ctx).Get(func() (any, error) {
		return fn(ctx)
	})
	metrics.QBAPICallDuration.WithLabelValues(mode, operation).Observe(time.Since(start).Seconds())
	if err != nil {
		var zero T
		return zero, err
	}
	return result.(T), nil
}

// CircuitBreakerState returns the current circuit breaker state, or "disabled" if not configured.
func (r *ResilientClient) CircuitBreakerState() string {
	if r.cb == nil {
		return "disabled"
	}
	switch {
	case r.cb.IsClosed():
		return "closed"
	case r.cb.IsOpen():
		return "open"
	case r.cb.IsHalfOpen():
		return "half-open"
	default:
		return "unknown"
	}
}
