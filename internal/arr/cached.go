package arr

import (
	"context"
	"time"
)

// cachedFilter memoises another Filter's verdicts for a short window.
//
// Service already caches, but on the machine that talks to *arr. A relayed
// filter sits on the other side of that cache, so without this every check
// costs a round trip. That matters more than it sounds: a rejected torrent is
// never tracked, and the arr-skipped tag deliberately does not exclude it, so
// it re-enters the check on every single cycle for as long as it exists on the
// source. The cost never converges.
//
// The TTL here is deliberately much shorter than Service's terminal one. Both
// caches are consulted in series, so their staleness adds up; a short window
// collapses the round trips while keeping the delay before an un-ignored
// torrent starts syncing to minutes rather than hours.
type cachedFilter struct {
	inner Filter
	cache *verdictCache
	ttl   time.Duration
}

// Compile-time interface assertion.
var _ Filter = (*cachedFilter)(nil)

// Cached wraps f so repeated questions about the same torrent are answered
// locally. A non-positive ttl returns f unchanged.
func Cached(f Filter, ttl time.Duration) Filter {
	if ttl <= 0 {
		return f
	}
	return &cachedFilter{inner: f, cache: newVerdictCache(), ttl: ttl}
}

func (c *cachedFilter) RefreshCategories(ctx context.Context) error {
	return c.inner.RefreshCategories(ctx)
}

func (c *cachedFilter) RoutedCategories() []string { return c.inner.RoutedCategories() }

func (c *cachedFilter) ShouldSync(ctx context.Context, hash, category string) Decision {
	return c.ShouldSyncAll(ctx, []CheckItem{{Hash: hash, Category: category}})[0]
}

// ShouldSyncAll answers what it can from the cache and asks the inner filter
// only about the rest, preserving index alignment with items.
func (c *cachedFilter) ShouldSyncAll(ctx context.Context, items []CheckItem) []Decision {
	decisions := make([]Decision, len(items))
	misses := make([]CheckItem, 0, len(items))
	missIndex := make([]int, 0, len(items))

	for i, item := range items {
		if d, hit := c.cache.Get(cacheKeyFor(item)); hit {
			decisions[i] = d
			continue
		}
		misses = append(misses, item)
		missIndex = append(missIndex, i)
	}

	if len(misses) == 0 {
		return decisions
	}

	fresh := c.inner.ShouldSyncAll(ctx, misses)
	if len(fresh) != len(misses) {
		// Cannot attribute the answers, so cache none of them and fail open
		// rather than risk pairing a verdict with the wrong torrent.
		for _, i := range missIndex {
			decisions[i] = Decision{Sync: true, Reason: ReasonRelayFailed}
		}
		return decisions
	}

	for j, d := range fresh {
		decisions[missIndex[j]] = d
		if cacheableVerdict(d.Reason) {
			c.cache.Set(cacheKeyFor(misses[j]), d, c.ttl)
		}
	}
	return decisions
}

// cacheKeyFor keys on the category rather than the instance, because a relayed
// filter never learns which instance answered until the verdict comes back.
func cacheKeyFor(item CheckItem) verdictKey {
	return verdictKey{instance: item.Category, hash: item.Hash}
}

// cacheableVerdict reports whether a verdict represents a real answer.
//
// Fail-open reasons are never cached: they mean the question could not be
// answered, and caching them would extend an outage past its own duration.
func cacheableVerdict(r Reason) bool {
	switch r {
	case ReasonLookupFailed, ReasonCircuitOpen, ReasonBudgetExceeded, ReasonRelayFailed:
		return false
	case ReasonIgnored, ReasonFailed, ReasonNotRejected, ReasonEmptyHistory, ReasonNoCategory:
		return true
	}
	return false
}
