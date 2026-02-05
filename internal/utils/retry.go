package utils

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"
)

// Default retry configuration values.
const (
	defaultMaxAttempts  = 3
	defaultInitialDelay = 500 * time.Millisecond
	defaultMaxDelay     = 5 * time.Second
	defaultMultiplier   = 2.0
)

// Default circuit breaker configuration values.
const (
	defaultCBMaxFailures         = 5
	defaultCBResetTimeout        = 30 * time.Second
	defaultCBHalfOpenMaxAttempts = 2
)

// RetryConfig configures retry behavior with exponential backoff.
type RetryConfig struct {
	MaxAttempts      int           // Maximum number of attempts (including first try)
	InitialDelay     time.Duration // Initial delay between retries
	MaxDelay         time.Duration // Maximum delay between retries
	Multiplier       float64       // Multiplier for exponential backoff
	RetriableChecker func(error) bool
}

// DefaultRetryConfig returns sensible defaults for retry configuration.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:      defaultMaxAttempts,
		InitialDelay:     defaultInitialDelay,
		MaxDelay:         defaultMaxDelay,
		Multiplier:       defaultMultiplier,
		RetriableChecker: IsRetriableError,
	}
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

// Retry executes the given function with exponential backoff retry logic.
// Returns the result and final error after all retries are exhausted.
func Retry[T any](
	ctx context.Context,
	config RetryConfig,
	logger *slog.Logger,
	operation string,
	fn func(ctx context.Context) (T, error),
) (T, error) {
	var zero T
	var lastErr error
	delay := config.InitialDelay

	checker := config.RetriableChecker
	if checker == nil {
		checker = IsRetriableError
	}

	for attempt := 1; attempt <= config.MaxAttempts; attempt++ {
		result, err := fn(ctx)
		if err == nil {
			if attempt > 1 && logger != nil {
				logger.DebugContext(ctx, "operation succeeded after retry",
					"operation", operation,
					"attempt", attempt,
				)
			}
			return result, nil
		}

		lastErr = err

		// Check if we should retry
		if !checker(err) {
			// Non-retriable error, fail immediately
			return zero, err
		}

		// Check if this was the last attempt
		if attempt >= config.MaxAttempts {
			break
		}

		// Log retry attempt
		if logger != nil {
			logger.WarnContext(ctx, "operation failed, retrying",
				"operation", operation,
				"attempt", attempt,
				"maxAttempts", config.MaxAttempts,
				"error", err,
				"nextDelay", delay,
			)
		}

		// Wait before retrying
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(delay):
		}

		// Increase delay for next iteration (exponential backoff)
		delay = min(time.Duration(float64(delay)*config.Multiplier), config.MaxDelay)
	}

	return zero, lastErr
}

// CircuitBreaker provides circuit breaker functionality to prevent
// hammering a failing service.
type CircuitBreaker struct {
	mu                  sync.Mutex
	failures            int
	lastFailure         time.Time
	state               circuitState
	maxFailures         int
	resetTimeout        time.Duration
	halfOpenMaxAttempts int
	halfOpenAttempts    int
}

type circuitState int

const (
	circuitClosed circuitState = iota
	circuitOpen
	circuitHalfOpen
)

// CircuitBreakerConfig configures the circuit breaker.
type CircuitBreakerConfig struct {
	MaxFailures         int           // Failures before opening circuit
	ResetTimeout        time.Duration // Time before trying again after opening
	HalfOpenMaxAttempts int           // Max attempts in half-open state before closing
}

// DefaultCircuitBreakerConfig returns sensible defaults.
func DefaultCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		MaxFailures:         defaultCBMaxFailures,
		ResetTimeout:        defaultCBResetTimeout,
		HalfOpenMaxAttempts: defaultCBHalfOpenMaxAttempts,
	}
}

// NewCircuitBreaker creates a new circuit breaker.
func NewCircuitBreaker(config CircuitBreakerConfig) *CircuitBreaker {
	return &CircuitBreaker{
		state:               circuitClosed,
		maxFailures:         config.MaxFailures,
		resetTimeout:        config.ResetTimeout,
		halfOpenMaxAttempts: config.HalfOpenMaxAttempts,
	}
}

// ErrCircuitOpen is returned when the circuit breaker is open.
var ErrCircuitOpen = errors.New("circuit breaker is open")

// Allow checks if a request should be allowed through.
// Returns true if the request can proceed, false if circuit is open.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case circuitClosed:
		return true
	case circuitOpen:
		// Check if reset timeout has passed
		if time.Since(cb.lastFailure) > cb.resetTimeout {
			cb.state = circuitHalfOpen
			cb.halfOpenAttempts = 0
			return true
		}
		return false
	case circuitHalfOpen:
		// Allow limited attempts in half-open state
		if cb.halfOpenAttempts < cb.halfOpenMaxAttempts {
			cb.halfOpenAttempts++
			return true
		}
		return false
	}
	return false
}

// RecordSuccess records a successful request.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures = 0
	cb.state = circuitClosed
}

// RecordFailure records a failed request.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures++
	cb.lastFailure = time.Now()

	if cb.state == circuitHalfOpen {
		// Any failure in half-open state opens the circuit
		cb.state = circuitOpen
		return
	}

	if cb.failures >= cb.maxFailures {
		cb.state = circuitOpen
	}
}

// State returns the current circuit state as a string.
func (cb *CircuitBreaker) State() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case circuitClosed:
		return "closed"
	case circuitOpen:
		return "open"
	case circuitHalfOpen:
		return "half-open"
	}
	return "unknown"
}

// RetryWithCircuitBreaker combines retry logic with circuit breaker.
func RetryWithCircuitBreaker[T any](
	ctx context.Context,
	cb *CircuitBreaker,
	config RetryConfig,
	logger *slog.Logger,
	operation string,
	fn func(ctx context.Context) (T, error),
) (T, error) {
	var zero T

	if !cb.Allow() {
		if logger != nil {
			logger.WarnContext(ctx, "circuit breaker open, skipping operation",
				"operation", operation,
				"state", cb.State(),
			)
		}
		return zero, ErrCircuitOpen
	}

	result, err := Retry(ctx, config, logger, operation, fn)
	if err != nil {
		cb.RecordFailure()
		return zero, err
	}

	cb.RecordSuccess()
	return result, nil
}
