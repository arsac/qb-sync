package qbclient

import (
	"context"
	"log/slog"

	"github.com/autobrr/go-qbittorrent"

	"github.com/arsac/qb-sync/internal/utils"
)

// Config configures the resilient qBittorrent client.
type Config struct {
	Retry          utils.RetryConfig
	CircuitBreaker *utils.CircuitBreakerConfig // nil to disable circuit breaker
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	cbConfig := utils.DefaultCircuitBreakerConfig()
	return Config{
		Retry: utils.RetryConfig{
			MaxAttempts:      3,
			InitialDelay:     500 * 1e6, // 500ms in nanoseconds for time.Duration
			MaxDelay:         5 * 1e9,   // 5s
			Multiplier:       2.0,
			RetriableChecker: IsRetriableError,
		},
		CircuitBreaker: &cbConfig,
	}
}

// IsRetriableError determines if a qBittorrent error is retriable.
func IsRetriableError(err error) bool {
	if err == nil {
		return false
	}

	// Use the generic check first
	if utils.IsRetriableError(err) {
		return true
	}

	// qBittorrent-specific: 404 on piece states means torrent was deleted
	// This is NOT retriable - it's a signal that we should handle elsewhere
	// Already handled by IsRetriableError returning false for 404

	return false
}

// ResilientClient wraps a qBittorrent client with retry and circuit breaker logic.
type ResilientClient struct {
	client *qbittorrent.Client
	config Config
	logger *slog.Logger
	cb     *utils.CircuitBreaker
}

// NewResilientClient creates a new resilient qBittorrent client.
func NewResilientClient(
	client *qbittorrent.Client,
	config Config,
	logger *slog.Logger,
) *ResilientClient {
	var cb *utils.CircuitBreaker
	if config.CircuitBreaker != nil {
		cb = utils.NewCircuitBreaker(*config.CircuitBreaker)
	}

	return &ResilientClient{
		client: client,
		config: config,
		logger: logger,
		cb:     cb,
	}
}

// Client returns the underlying qBittorrent client for operations that don't need retry.
func (r *ResilientClient) Client() *qbittorrent.Client {
	return r.client
}

// LoginCtx logs in to qBittorrent with retry.
func (r *ResilientClient) LoginCtx(ctx context.Context) error {
	return r.doVoid(ctx, "Login", func(ctx context.Context) error {
		return r.client.LoginCtx(ctx)
	})
}

// GetTorrentsCtx gets torrents with retry.
func (r *ResilientClient) GetTorrentsCtx(
	ctx context.Context,
	opts qbittorrent.TorrentFilterOptions,
) ([]qbittorrent.Torrent, error) {
	return doWithRetry(ctx, r.cb, r.config.Retry, r.logger, "GetTorrents",
		func(ctx context.Context) ([]qbittorrent.Torrent, error) {
			return r.client.GetTorrentsCtx(ctx, opts)
		})
}

// GetTorrentPieceStatesCtx gets piece states with retry.
func (r *ResilientClient) GetTorrentPieceStatesCtx(
	ctx context.Context,
	hash string,
) ([]qbittorrent.PieceState, error) {
	return doWithRetry(ctx, r.cb, r.config.Retry, r.logger, "GetTorrentPieceStates",
		func(ctx context.Context) ([]qbittorrent.PieceState, error) {
			return r.client.GetTorrentPieceStatesCtx(ctx, hash)
		})
}

// GetTorrentPieceHashesCtx gets piece hashes with retry.
func (r *ResilientClient) GetTorrentPieceHashesCtx(
	ctx context.Context,
	hash string,
) ([]string, error) {
	return doWithRetry(ctx, r.cb, r.config.Retry, r.logger, "GetTorrentPieceHashes",
		func(ctx context.Context) ([]string, error) {
			return r.client.GetTorrentPieceHashesCtx(ctx, hash)
		})
}

// GetTorrentPropertiesCtx gets torrent properties with retry.
func (r *ResilientClient) GetTorrentPropertiesCtx(
	ctx context.Context,
	hash string,
) (qbittorrent.TorrentProperties, error) {
	return doWithRetry(ctx, r.cb, r.config.Retry, r.logger, "GetTorrentProperties",
		func(ctx context.Context) (qbittorrent.TorrentProperties, error) {
			return r.client.GetTorrentPropertiesCtx(ctx, hash)
		})
}

// GetFilesInformationCtx gets file information with retry.
func (r *ResilientClient) GetFilesInformationCtx(
	ctx context.Context,
	hash string,
) (*qbittorrent.TorrentFiles, error) {
	return doWithRetry(ctx, r.cb, r.config.Retry, r.logger, "GetFilesInformation",
		func(ctx context.Context) (*qbittorrent.TorrentFiles, error) {
			return r.client.GetFilesInformationCtx(ctx, hash)
		})
}

// ExportTorrentCtx exports a torrent file with retry.
func (r *ResilientClient) ExportTorrentCtx(
	ctx context.Context,
	hash string,
) ([]byte, error) {
	return doWithRetry(ctx, r.cb, r.config.Retry, r.logger, "ExportTorrent",
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
	return r.doVoid(ctx, "DeleteTorrents", func(ctx context.Context) error {
		return r.client.DeleteTorrentsCtx(ctx, hashes, deleteFiles)
	})
}

// AddTagsCtx adds tags to torrents with retry.
func (r *ResilientClient) AddTagsCtx(
	ctx context.Context,
	hashes []string,
	tags string,
) error {
	return r.doVoid(ctx, "AddTags", func(ctx context.Context) error {
		return r.client.AddTagsCtx(ctx, hashes, tags)
	})
}

// doVoid executes a void operation with retry and optional circuit breaker.
func (r *ResilientClient) doVoid(
	ctx context.Context,
	operation string,
	fn func(ctx context.Context) error,
) error {
	wrappedFn := func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	}
	_, err := doWithRetry(ctx, r.cb, r.config.Retry, r.logger, operation, wrappedFn)
	return err
}

// doWithRetry is a package-level generic function for retry with optional circuit breaker.
func doWithRetry[T any](
	ctx context.Context,
	cb *utils.CircuitBreaker,
	config utils.RetryConfig,
	logger *slog.Logger,
	operation string,
	fn func(ctx context.Context) (T, error),
) (T, error) {
	if cb != nil {
		return utils.RetryWithCircuitBreaker(ctx, cb, config, logger, operation, fn)
	}
	return utils.Retry(ctx, config, logger, operation, fn)
}

// CircuitBreakerState returns the current circuit breaker state, or "disabled" if not configured.
func (r *ResilientClient) CircuitBreakerState() string {
	if r.cb == nil {
		return "disabled"
	}
	return r.cb.State()
}
