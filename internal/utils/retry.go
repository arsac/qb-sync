package utils

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/retrypolicy"
)

// Default retry configuration values.
const (
	defaultMaxAttempts  = 3
	defaultInitialDelay = 500 * time.Millisecond
	defaultMaxDelay     = 5 * time.Second
)

// RetryConfig configures retry behavior with exponential backoff.
type RetryConfig struct {
	MaxAttempts      int           // Maximum number of attempts (including first try)
	InitialDelay     time.Duration // Initial delay between retries
	MaxDelay         time.Duration // Maximum delay between retries
	RetriableChecker func(error) bool
}

// DefaultRetryConfig returns sensible defaults for retry configuration.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:      defaultMaxAttempts,
		InitialDelay:     defaultInitialDelay,
		MaxDelay:         defaultMaxDelay,
		RetriableChecker: IsRetriableError,
	}
}

// CircuitBreakerConfig configures the circuit breaker.
type CircuitBreakerConfig struct {
	MaxFailures  int           // Failures before opening circuit
	ResetTimeout time.Duration // Time before trying again after opening
}

// NewRetryPolicy builds a failsafe-go retry policy from a RetryConfig.
// Optional onRetry callbacks are invoked on each retry attempt (e.g., for metrics).
func NewRetryPolicy(config RetryConfig, logger *slog.Logger, onRetry ...func()) retrypolicy.RetryPolicy[any] {
	checker := config.RetriableChecker
	if checker == nil {
		checker = IsRetriableError
	}

	return retrypolicy.NewBuilder[any]().
		WithMaxAttempts(config.MaxAttempts).
		WithBackoff(config.InitialDelay, config.MaxDelay).
		WithJitterFactor(0.1).
		HandleIf(func(_ any, err error) bool {
			return checker(err)
		}).
		OnRetry(func(e failsafe.ExecutionEvent[any]) {
			logger.Warn("operation failed, retrying",
				"attempt", e.Attempts(),
				"error", e.LastError(),
			)
			for _, fn := range onRetry {
				fn()
			}
		}).
		ReturnLastFailure().
		Build()
}

// isRetriableNetworkError checks if the error string indicates a network issue worth retrying.
func isRetriableNetworkError(errStr string) bool {
	networkPatterns := []string{
		"connection refused",
		"connection reset",
		"connection timed out",
		"no such host",
		"network is unreachable",
		"i/o timeout",
		"eof",
		"broken pipe",
		"temporary failure",
		"dns",
	}
	return slices.ContainsFunc(networkPatterns, func(pattern string) bool {
		return strings.Contains(errStr, pattern)
	})
}

// isRetriableHTTPError checks if the error string indicates a retriable HTTP status.
func isRetriableHTTPError(errStr string) bool {
	httpPatterns := []string{
		"status: 502", "status 502", // Bad Gateway
		"status: 503", "status 503", // Service Unavailable
		"status: 504", "status 504", // Gateway Timeout
		"status: 429", "status 429", // Too Many Requests
	}
	return slices.ContainsFunc(httpPatterns, func(pattern string) bool {
		return strings.Contains(errStr, pattern)
	})
}

// IsRetriableError determines if an error is transient and worth retrying.
// This is the default checker - callers can provide custom checkers.
func IsRetriableError(err error) bool {
	if err == nil {
		return false
	}

	// Context errors are NOT retriable (caller cancelled)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	errStr := strings.ToLower(err.Error())

	// Network/connection errors - always retry
	if isRetriableNetworkError(errStr) {
		return true
	}

	// HTTP errors that are retriable
	if isRetriableHTTPError(errStr) {
		return true
	}

	// 404 errors are NOT retriable (resource doesn't exist)
	if strings.Contains(errStr, "404") || strings.Contains(errStr, "not found") {
		return false
	}

	// 401/403 errors are NOT retriable (auth issues)
	if strings.Contains(errStr, "401") || strings.Contains(errStr, "403") ||
		strings.Contains(errStr, "unauthorized") || strings.Contains(errStr, "forbidden") {
		return false
	}

	return false
}

// IsCircuitBreakerFailure determines if an error should count against the circuit breaker.
// Some errors (like 404 Not Found) indicate the resource doesn't exist, not that the
// service is unavailable. These should NOT trip the circuit breaker.
func IsCircuitBreakerFailure(err error) bool {
	if err == nil {
		return false
	}

	// Context errors indicate caller cancellation, not service failure
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	errStr := strings.ToLower(err.Error())

	// 404 errors indicate resource not found (e.g., deleted torrent).
	// This is expected behavior, not a service availability issue.
	if strings.Contains(errStr, "404") || strings.Contains(errStr, "not found") {
		return false
	}

	// qBittorrent returns "Not Found" as plain text for missing torrents.
	// The go-qbittorrent library tries to unmarshal this as JSON, resulting in:
	// "could not unmarshal body: invalid character 'n' looking for beginning of value"
	// (Note: lowercase 'n' after strings.ToLower normalization)
	if strings.Contains(errStr, "could not unmarshal body: invalid character 'n'") {
		return false
	}

	// All other errors are considered circuit breaker failures
	return true
}
