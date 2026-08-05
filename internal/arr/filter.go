package arr

import "context"

// Filter is the public contract that the source package consumes. The source
// package always holds a Filter, never a *Service - so it never has to nil-check.
type Filter interface {
	// ShouldSync returns the Decision for a single torrent.
	ShouldSync(ctx context.Context, hash, category string) Decision

	// ShouldSyncAll returns one Decision per item, index-aligned with items.
	// Batched because the periodic re-check asks about every tracked torrent at
	// once, and over a relayed link that is the difference between one round
	// trip and one per torrent.
	ShouldSyncAll(ctx context.Context, items []CheckItem) []Decision
}

// noopFilter always returns SYNC. Used when no arr instances are configured.
type noopFilter struct{}

func (noopFilter) ShouldSync(_ context.Context, _, _ string) Decision {
	return Decision{Sync: true, Reason: ReasonNoCategory}
}

func (noopFilter) ShouldSyncAll(_ context.Context, items []CheckItem) []Decision {
	decisions := make([]Decision, len(items))
	for i := range decisions {
		decisions[i] = Decision{Sync: true, Reason: ReasonNoCategory}
	}
	return decisions
}

// NoopFilter returns a no-op Filter that always returns SYNC. Exported for test wiring.
func NoopFilter() Filter { return noopFilter{} }
