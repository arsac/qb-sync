package arr

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"slices"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/circuitbreaker"
	"golang.org/x/sync/errgroup"

	"github.com/arsac/qb-sync/internal/metrics"
	"github.com/arsac/qb-sync/internal/utils"
)

// instanceState holds per-instance config and runtime state.
type instanceState struct {
	name       string
	client     *Client
	categories []string
	// executor is nil when no breaker is attached, and selects the lookup path.
	executor failsafe.Executor[any]
}

// verdictTerminalTTL is the cache lifetime for terminal SKIP decisions
// (ReasonIgnored, ReasonFailed). These states are effectively permanent in
// *arr and won't reverse without manual user intervention.
const verdictTerminalTTL = time.Hour

// Service routes torrents to the right *arr instance, queries history,
// and produces a Decision. Always returned via the Filter interface so
// callers don't depend on concrete type.
type Service struct {
	instances map[string]*instanceState
	routes    map[string]string // category -> instance name
	cache     *verdictCache
	cacheTTL  time.Duration // "sync" TTL for non-terminal decisions
	logger    *slog.Logger

	now func() time.Time // injectable for tests
}

// Compile-time interface assertion.
var _ Filter = (*Service)(nil)

// ShouldSync is the entry point for the source package. It is total: errors,
// breaker-open, and budget exhaustion all map to a Decision (fail-open).
func (s *Service) ShouldSync(ctx context.Context, hash, category string) Decision {
	d := s.decide(ctx, hash, category)
	// Stamped here rather than inside decide so it is set on every path,
	// including a cache hit, where the cached Decision predates this call.
	d.Instance = s.routes[category]
	s.recordDecision(d)
	return d
}

// decide is the pure decision logic without metric side-effects.
func (s *Service) decide(ctx context.Context, hash, category string) Decision {
	instanceName, ok := s.routes[category]
	if !ok {
		return Decision{Sync: true, Reason: ReasonNoCategory}
	}
	inst, ok := s.instances[instanceName]
	if !ok {
		// Configuration drift - should be caught at construction. Fail open.
		return Decision{Sync: true, Reason: ReasonNoCategory}
	}

	key := verdictKey{instance: instanceName, hash: hash}
	if d, hit := s.cache.Get(key); hit {
		return d
	}

	start := s.now()
	d := s.lookup(ctx, inst, hash)
	metrics.ArrLookupSeconds.WithLabelValues(instanceName).Observe(s.now().Sub(start).Seconds())
	if ttl := ttlFor(d.Reason, s.cacheTTL); ttl > 0 {
		s.cache.Set(key, d, ttl)
	}
	return d
}

// recordDecision emits decision-level metrics for the verdict. The instance is
// taken from the Decision, which ShouldSync has already resolved.
func (s *Service) recordDecision(d Decision) {
	if d.Reason == ReasonNoCategory {
		return // not a real decision; never count routing-skips
	}
	instanceName := d.Instance

	switch {
	case !d.Sync:
		metrics.ArrDecisionsTotal.WithLabelValues(instanceName, metrics.OutcomeArrSkipped).Inc()
		metrics.ArrSkipTotal.WithLabelValues(instanceName, string(d.Reason)).Inc()
	case d.Reason == ReasonLookupFailed || d.Reason == ReasonCircuitOpen || d.Reason == ReasonBudgetExceeded:
		metrics.ArrDecisionsTotal.WithLabelValues(instanceName, metrics.OutcomeArrFailedOpen).Inc()
	default:
		metrics.ArrDecisionsTotal.WithLabelValues(instanceName, metrics.OutcomeArrSynced).Inc()
	}
}

// attachBreaker installs a failsafe circuit breaker on inst with the given config.
// MaxFailures<=0 disables the breaker.
func attachBreaker(inst *instanceState, cfg utils.CircuitBreakerConfig) {
	if cfg.MaxFailures <= 0 {
		return
	}
	cb := circuitbreaker.NewBuilder[any]().
		WithFailureThreshold(uint(cfg.MaxFailures)).
		WithSuccessThreshold(1).
		WithDelay(cfg.ResetTimeout).
		HandleIf(func(_ any, err error) bool { return err != nil }).
		OnOpen(func(_ circuitbreaker.StateChangedEvent) {
			metrics.ArrCircuitBreakerState.WithLabelValues(inst.name).Set(metrics.CircuitStateOpen)
		}).
		OnHalfOpen(func(_ circuitbreaker.StateChangedEvent) {
			metrics.ArrCircuitBreakerState.WithLabelValues(inst.name).Set(metrics.CircuitStateHalfOpen)
		}).
		OnClose(func(_ circuitbreaker.StateChangedEvent) {
			metrics.ArrCircuitBreakerState.WithLabelValues(inst.name).Set(metrics.CircuitStateClosed)
		}).
		Build()
	inst.executor = failsafe.With[any](cb)
	// Initialize gauge so the label appears in /metrics output from the start.
	metrics.ArrCircuitBreakerState.WithLabelValues(inst.name).Set(metrics.CircuitStateClosed)
}

// lookup performs the network call and interprets the response.
// All error paths return a fail-open Decision; the typed error is consumed
// internally for metrics.
func (s *Service) lookup(ctx context.Context, inst *instanceState, hash string) Decision {
	var (
		records []HistoryRecord
		err     error
	)
	if inst.executor == nil {
		records, err = inst.client.GetHistoryByDownloadID(ctx, hash)
	} else {
		err = inst.executor.RunWithExecution(func(_ failsafe.Execution[any]) error {
			var lookupErr error
			records, lookupErr = inst.client.GetHistoryByDownloadID(ctx, hash)
			return lookupErr
		})
	}

	switch {
	case errors.Is(err, circuitbreaker.ErrOpen):
		return Decision{Sync: true, Reason: ReasonCircuitOpen}
	case err != nil:
		s.logArrError(ctx, inst, hash, err)
		return Decision{Sync: true, Reason: ReasonLookupFailed}
	}
	return decideFromRecords(records)
}

// logArrError logs typed *Error details; silently ignores non-*Error values.
func (s *Service) logArrError(ctx context.Context, inst *instanceState, hash string, err error) {
	var arrErr *Error
	if errors.As(err, &arrErr) {
		s.logger.WarnContext(ctx, "arr lookup error",
			"instance", inst.name,
			"hash", hash,
			"kind", arrErr.Kind,
			"error", arrErr.Cause,
		)
		metrics.ArrLookupErrorsTotal.WithLabelValues(inst.name, string(arrErr.Kind)).Inc()
	}
}

// decideFromRecords maps a (possibly empty) history slice to a Decision.
func decideFromRecords(records []HistoryRecord) Decision {
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

// ttlFor returns the cache TTL for a given Reason.
// Terminal SKIP reasons get a long TTL since *arr won't reverse them without
// user intervention. SYNC reasons get the configured short TTL since *arr may
// fire a terminal event after the initial grab. Fail-open reasons return 0 so
// they are NOT cached - every cycle should retry.
func ttlFor(r Reason, syncTTL time.Duration) time.Duration {
	switch r {
	case ReasonIgnored, ReasonFailed:
		return verdictTerminalTTL
	case ReasonNotRejected, ReasonEmptyHistory:
		return syncTTL
	case ReasonLookupFailed, ReasonCircuitOpen, ReasonBudgetExceeded:
		return 0 // retry next cycle
	case ReasonRelayFailed:
		// Produced on the source when the destination could not be reached, so
		// it never arrives here. Listed so adding a reason cannot silently
		// inherit a caching policy nobody chose.
		return 0
	case ReasonNoCategory:
		return 0 // short-circuited before the cache is ever consulted
	}
	return 0
}

// batchConcurrency bounds how many lookups run at once for one batch. The
// instances are local to this process, but *arr is single-threaded enough that
// a wide fan-out buys nothing and risks tripping the breaker on load.
const batchConcurrency = 4

// ShouldSyncAll resolves a batch of torrents, returning one Decision per item
// in the same order.
//
// Honours ctx cancellation: anything unresolved when the deadline passes comes
// back as a fail-open budget verdict rather than blocking the caller. That is
// the same contract a single lookup has, applied to the whole batch.
func (s *Service) ShouldSyncAll(ctx context.Context, items []CheckItem) []Decision {
	decisions := make([]Decision, len(items))
	for i := range decisions {
		decisions[i] = Decision{Sync: true, Reason: ReasonBudgetExceeded}
	}

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(batchConcurrency)

	for i, item := range items {
		g.Go(func() error {
			select {
			case <-gCtx.Done():
				// Leave the pre-seeded budget verdict in place.
				return nil
			default:
			}
			decisions[i] = s.ShouldSync(gCtx, item.Hash, item.Category)
			return nil
		})
	}
	// Each goroutine writes only its own index and never errors, so Wait is a
	// barrier rather than an error check.
	_ = g.Wait()

	return decisions
}
