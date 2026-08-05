package arr

import (
	"context"
	"testing"
	"time"
)

// countingFilter records how many items it was asked about.
type countingFilter struct {
	calls    int
	items    int
	decision Decision
}

func (c *countingFilter) RefreshCategories(context.Context) error { return nil }

// routed is everything: these stubs answer for whatever they are asked.
func (c *countingFilter) RoutedCategories() []string { return nil }

func (c *countingFilter) ShouldSync(ctx context.Context, hash, category string) Decision {
	return c.ShouldSyncAll(ctx, []CheckItem{{Hash: hash, Category: category}})[0]
}

func (c *countingFilter) ShouldSyncAll(_ context.Context, items []CheckItem) []Decision {
	c.calls++
	c.items += len(items)
	out := make([]Decision, len(items))
	for i := range out {
		out[i] = c.decision
	}
	return out
}

// TestCachedCollapsesRepeatQuestions is the point of the decorator. A rejected
// torrent is never tracked and its tag deliberately does not exclude it, so it
// is re-checked every cycle forever. Over a relayed link each of those is a
// round trip.
func TestCachedCollapsesRepeatQuestions(t *testing.T) {
	t.Parallel()

	inner := &countingFilter{decision: Decision{Sync: false, Reason: ReasonIgnored}}
	filter := Cached(inner, time.Minute)
	item := []CheckItem{{Hash: "abc", Category: "radarr"}}

	for range 10 {
		if d := filter.ShouldSyncAll(context.Background(), item); d[0].Sync {
			t.Fatal("a cached skip must stay a skip")
		}
	}

	if inner.calls != 1 {
		t.Errorf("asked the inner filter %d times, want 1", inner.calls)
	}
}

// TestCachedDoesNotCacheFailOpen guards the direction that matters. Caching a
// fail-open verdict would extend an *arr or link outage past its own duration,
// leaving torrents unfiltered long after the problem cleared.
func TestCachedDoesNotCacheFailOpen(t *testing.T) {
	t.Parallel()

	for _, reason := range []Reason{
		ReasonLookupFailed, ReasonCircuitOpen, ReasonBudgetExceeded, ReasonRelayFailed,
	} {
		t.Run(string(reason), func(t *testing.T) {
			t.Parallel()
			inner := &countingFilter{decision: Decision{Sync: true, Reason: reason}}
			filter := Cached(inner, time.Minute)
			item := []CheckItem{{Hash: "abc", Category: "radarr"}}

			for range 3 {
				filter.ShouldSyncAll(context.Background(), item)
			}

			if inner.calls != 3 {
				t.Errorf("asked %d times, want 3: %q means no answer was obtained", inner.calls, reason)
			}
		})
	}
}

// TestCachedAsksOnlyAboutMisses checks a partially warm batch still lines up:
// verdicts are matched to torrents by index, so a mixed hit/miss batch is where
// misalignment would show.
func TestCachedAsksOnlyAboutMisses(t *testing.T) {
	t.Parallel()

	inner := &countingFilter{decision: Decision{Sync: false, Reason: ReasonIgnored}}
	filter := Cached(inner, time.Minute)

	filter.ShouldSyncAll(context.Background(), []CheckItem{{Hash: "aaa", Category: "radarr"}})

	decisions := filter.ShouldSyncAll(context.Background(), []CheckItem{
		{Hash: "aaa", Category: "radarr"}, // cached
		{Hash: "bbb", Category: "radarr"}, // miss
	})

	if len(decisions) != 2 {
		t.Fatalf("got %d decisions, want 2", len(decisions))
	}
	for i, d := range decisions {
		if d.Sync {
			t.Errorf("decision %d should be a skip", i)
		}
	}
	if inner.items != 2 {
		t.Errorf("inner filter saw %d items total, want 2 (one per miss)", inner.items)
	}
}

func TestCachedExpires(t *testing.T) {
	t.Parallel()

	inner := &countingFilter{decision: Decision{Sync: false, Reason: ReasonIgnored}}
	filter := Cached(inner, time.Millisecond)
	item := []CheckItem{{Hash: "abc", Category: "radarr"}}

	filter.ShouldSyncAll(context.Background(), item)
	time.Sleep(10 * time.Millisecond)
	filter.ShouldSyncAll(context.Background(), item)

	if inner.calls != 2 {
		t.Errorf("asked %d times, want 2: the entry should have expired", inner.calls)
	}
}

func TestCachedIsOptional(t *testing.T) {
	t.Parallel()

	inner := &countingFilter{decision: Decision{Sync: true, Reason: ReasonNotRejected}}
	if got := Cached(inner, 0); got != Filter(inner) {
		t.Error("a non-positive ttl must return the filter unwrapped")
	}
}
