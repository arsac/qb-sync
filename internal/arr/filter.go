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

	// RefreshCategories rediscovers which categories each instance claims.
	// Total, like the rest of this interface, so a caller cannot be handed an
	// implementation that silently does not support it.
	RefreshCategories(ctx context.Context) error

	// RoutedCategories reports the categories currently routed to an instance.
	// A relayed filter reports what the destination told it, so the source can
	// decide locally rather than asking about torrents no *arr owns.
	RoutedCategories() []string
}

// noopFilter always returns SYNC. Used when no arr instances are configured.
type noopFilter struct{}

func (noopFilter) ShouldSync(_ context.Context, _, _ string) Decision {
	return Decision{Sync: true, Reason: ReasonNoCategory}
}

func (noopFilter) RefreshCategories(context.Context) error { return nil }

func (noopFilter) RoutedCategories() []string { return nil }

func (noopFilter) ShouldSyncAll(_ context.Context, items []CheckItem) []Decision {
	decisions := make([]Decision, len(items))
	for i := range decisions {
		decisions[i] = Decision{Sync: true, Reason: ReasonNoCategory}
	}
	return decisions
}

// NoopFilter returns a no-op Filter that always returns SYNC. Exported for test wiring.
func NoopFilter() Filter { return noopFilter{} }
