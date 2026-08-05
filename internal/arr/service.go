package arr

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/circuitbreaker"
	"golang.org/x/sync/errgroup"

	"github.com/arsac/qb-sync/internal/metrics"
	"github.com/arsac/qb-sync/internal/utils"
)

// instanceState holds per-instance config and runtime state.
type instanceState struct {
	name   string
	client *Client
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
	cache     *verdictCache
	cacheTTL  time.Duration // "sync" TTL for non-terminal decisions
	logger    *slog.Logger

	// routes maps a qBittorrent category to the instance that claims it, and is
	// discovered from *arr rather than configured. *arr is the authority on
	// which category it assigns, so asking it removes a list that would
	// otherwise silently stop matching the day someone renames one.
	//
	// Empty until the first refresh succeeds, which leaves the filter inert and
	// everything syncing. That is the only behaviour available without a
	// mapping - there is no instance to ask - and it is the fail-open direction.
	// A failed refresh keeps the previous map rather than clearing it, so a
	// transient *arr outage does not silently switch filtering off.
	routesMu sync.RWMutex
	routes   map[string]string

	now func() time.Time // injectable for tests
}

// RefreshCategories rediscovers which categories each instance claims.
//
// Callers schedule this; the Service does not own a goroutine. Returns the
// first error encountered, having still applied whatever it did learn: one
// unreachable instance should not discard the other's routing.
func (s *Service) RefreshCategories(ctx context.Context) error {
	discovered := make(map[string]string)
	var firstErr error

	s.routesMu.RLock()
	previous := maps.Clone(s.routes)
	s.routesMu.RUnlock()

	// Deterministic order. Two instances can legitimately claim the same
	// category - someone points Radarr and Sonarr at one qBittorrent category -
	// and map iteration would otherwise hand it to a different owner on each
	// refresh, so a torrent would be checked against whichever won that round.
	for _, name := range slices.Sorted(maps.Keys(s.instances)) {
		inst := s.instances[name]
		categories, err := inst.client.DownloadClientCategories(ctx)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("%s: %w", name, err)
			}
			metrics.ArrCategoryRefreshErrorsTotal.WithLabelValues(name).Inc()
			// Carry this instance's previous routes forward, so one instance
			// failing does not unroute torrents the other never owned.
			for category, owner := range previous {
				if owner == name {
					discovered[category] = owner
				}
			}
			continue
		}
		for _, category := range categories {
			if owner, taken := discovered[category]; taken && owner != name {
				// First owner wins, deterministically. Logged because the
				// verdict a torrent gets now depends on which instance is
				// asked, and only the operator can resolve that.
				s.logger.WarnContext(ctx, "category claimed by more than one arr instance",
					"category", category, "using", owner, "ignoring", name)
				continue
			}
			discovered[category] = name
		}
	}

	s.publishRoutedCounts(discovered)

	s.routesMu.Lock()
	s.routes = discovered
	s.routesMu.Unlock()
	return firstErr
}

// publishRoutedCounts reports how many categories each instance actually owns.
//
// Counted from the routing rather than from what each instance reported: a
// category lost to a conflict stays in the loser's reply but routes nowhere,
// and counting it would overstate a filter that is inert for that instance.
// Every instance is published, including ones whose refresh failed, so a zero
// shows up as a zero rather than as a gap in the series.
func (s *Service) publishRoutedCounts(routes map[string]string) {
	counts := make(map[string]int, len(s.instances))
	for name := range s.instances {
		counts[name] = 0
	}
	for _, owner := range routes {
		counts[owner]++
	}
	for name, count := range counts {
		metrics.ArrRoutedCategories.WithLabelValues(name).Set(float64(count))
	}
}

// RoutedCategories returns every category currently routed to an instance.
func (s *Service) RoutedCategories() []string {
	s.routesMu.RLock()
	defer s.routesMu.RUnlock()
	out := make([]string, 0, len(s.routes))
	for category := range s.routes {
		out = append(out, category)
	}
	slices.Sort(out) // stable output for logs and the relayed response
	return out
}

// instanceFor returns the instance claiming a category, if any.
func (s *Service) instanceFor(category string) (string, bool) {
	s.routesMu.RLock()
	defer s.routesMu.RUnlock()
	name, ok := s.routes[category]
	return name, ok
}

// Compile-time interface assertion.
var _ Filter = (*Service)(nil)

// ShouldSync is the entry point for the source package. It is total: errors,
// breaker-open, and budget exhaustion all map to a Decision (fail-open).
func (s *Service) ShouldSync(ctx context.Context, hash, category string) Decision {
	d := s.decide(ctx, hash, category)
	// Stamped here rather than inside decide so it is set on every path,
	// including a cache hit, where the cached Decision predates this call.
	d.Instance, _ = s.instanceFor(category)
	s.recordDecision(d)
	return d
}

// decide is the pure decision logic without metric side-effects.
func (s *Service) decide(ctx context.Context, hash, category string) Decision {
	instanceName, ok := s.instanceFor(category)
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
