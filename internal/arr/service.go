package arr

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/circuitbreaker"
)

// instanceState holds per-instance config and runtime state.
type instanceState struct {
	name       string
	client     *Client
	categories []string
	breaker    circuitbreaker.CircuitBreaker[any] // nil if disabled
	executor   failsafe.Executor[any]             // nil if disabled
}

// Service routes torrents to the right *arr instance, queries history,
// and produces a Decision. Always returned via the Filter interface so
// callers don't depend on concrete type.
type Service struct {
	instances map[string]*instanceState
	routes    map[string]string // category -> instance name
	cache     *verdictCache
	logger    *slog.Logger
	mu        sync.Mutex //nolint:unused // reserved for circuit-breaker state added in Task 10
}

// Compile-time interface assertion.
var _ Filter = (*Service)(nil)

// ShouldSync is the entry point for the source package. It is total: errors,
// breaker-open, and budget exhaustion all map to a Decision (fail-open).
func (s *Service) ShouldSync(ctx context.Context, hash, category string) Decision {
	instanceName, ok := s.routes[category]
	if !ok {
		return Decision{Sync: true, Reason: ReasonNoCategory}
	}
	inst, ok := s.instances[instanceName]
	if !ok {
		// Configuration drift — should be caught at construction. Fail open.
		return Decision{Sync: true, Reason: ReasonNoCategory}
	}

	key := verdictKey{instance: instanceName, hash: hash}
	if d, hit := s.cache.Get(key); hit {
		return d
	}

	d := s.lookup(ctx, inst, hash)
	s.cache.Set(key, d)
	return d
}

// breakerConfig configures a per-instance circuit breaker.
type breakerConfig struct {
	MaxFailures  int
	ResetTimeout time.Duration
}

// attachBreaker installs a failsafe circuit breaker on inst with the given config.
// MaxFailures<=0 disables the breaker.
func attachBreaker(inst *instanceState, cfg breakerConfig) {
	if cfg.MaxFailures <= 0 {
		return
	}
	cb := circuitbreaker.NewBuilder[any]().
		WithFailureThreshold(uint(cfg.MaxFailures)).
		WithSuccessThreshold(1).
		WithDelay(cfg.ResetTimeout).
		HandleIf(func(_ any, err error) bool { return err != nil }).
		Build()
	inst.breaker = cb
	inst.executor = failsafe.With[any](cb)
}

// lookup performs the network call and interprets the response.
// All error paths return a fail-open Decision; the typed error is consumed
// internally for metrics.
func (s *Service) lookup(ctx context.Context, inst *instanceState, hash string) Decision {
	if inst.executor == nil {
		return s.lookupDirect(ctx, inst, hash)
	}
	var records []HistoryRecord
	err := inst.executor.RunWithExecution(func(_ failsafe.Execution[any]) error {
		var lookupErr error
		records, lookupErr = inst.client.GetHistoryByDownloadID(ctx, hash)
		return lookupErr
	})
	if err != nil {
		if errors.Is(err, circuitbreaker.ErrOpen) {
			return Decision{Sync: true, Reason: ReasonCircuitOpen}
		}
		var arrErr *Error
		if errors.As(err, &arrErr) {
			s.logger.WarnContext(ctx, "arr lookup error",
				"instance", inst.name,
				"hash", hash,
				"kind", arrErr.Kind,
				"error", arrErr.Cause,
			)
		}
		return Decision{Sync: true, Reason: ReasonLookupFailed}
	}
	if len(records) == 0 {
		return Decision{Sync: true, Reason: ReasonEmptyHistory}
	}
	return interpretHistory(records)
}

// lookupDirect is the no-breaker variant used by tests that don't attach one.
func (s *Service) lookupDirect(ctx context.Context, inst *instanceState, hash string) Decision {
	records, err := inst.client.GetHistoryByDownloadID(ctx, hash)
	if err != nil {
		var arrErr *Error
		if errors.As(err, &arrErr) {
			s.logger.WarnContext(ctx, "arr lookup error",
				"instance", inst.name,
				"hash", hash,
				"kind", arrErr.Kind,
				"error", arrErr.Cause,
			)
		}
		return Decision{Sync: true, Reason: ReasonLookupFailed}
	}
	if len(records) == 0 {
		return Decision{Sync: true, Reason: ReasonEmptyHistory}
	}
	return interpretHistory(records)
}

// interpretHistory finds the most recent terminal event and maps it to a Decision.
// Falls through to ReasonNotRejected if no terminal event is present.
// records is mutated (sorted in-place); callers pass a freshly allocated slice.
func interpretHistory(records []HistoryRecord) Decision {
	slices.SortStableFunc(records, func(a, b HistoryRecord) int {
		return cmp.Compare(b.Date.UnixNano(), a.Date.UnixNano()) // desc
	})

	for _, r := range records {
		switch r.EventType {
		case eventTypeIgnored:
			return Decision{Sync: false, Reason: ReasonIgnored}
		case eventTypeFailed:
			return Decision{Sync: false, Reason: ReasonFailed}
		}
	}
	return Decision{Sync: true, Reason: ReasonNotRejected}
}

// matchesHash reports whether two infohashes refer to the same torrent.
// *arr stores uppercase, qB stores lowercase; we compare case-insensitively.
//
//nolint:unused // required by Tasks 10-12 for case-insensitive hash comparison
func matchesHash(a, b string) bool {
	return strings.EqualFold(a, b)
}

// timeNow is a small indirection for tests; defaults to [time.Now].
//
//nolint:unused,gochecknoglobals // test-injectable time; used in Tasks 10-12
var timeNow = time.Now
