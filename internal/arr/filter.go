package arr

import "context"

// Filter is the public contract that the source package consumes. The source
// package always holds a Filter, never a *Service — so it never has to nil-check.
type Filter interface {
	// ShouldSync returns the Decision for a single torrent.
	ShouldSync(ctx context.Context, hash, category string) Decision
}

// noopFilter always returns SYNC. Used when no arr instances are configured.
type noopFilter struct{}

func (noopFilter) ShouldSync(_ context.Context, _, _ string) Decision {
	return Decision{Sync: true, Reason: ReasonNoCategory}
}

// NoopFilter returns a no-op Filter that always returns SYNC. Exported for test wiring.
func NoopFilter() Filter { return noopFilter{} }
