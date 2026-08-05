package source

import (
	"context"
	"time"

	"github.com/arsac/qb-sync/internal/arr"
	"github.com/arsac/qb-sync/internal/metrics"
)

// arrInstanceUnknown labels metrics when the destination reports no routing
// instance for a category.
const arrInstanceUnknown = "unknown"

// filter returns the *arr filter, substituting the no-op when none is set.
//
// One accessor rather than a guard at each use: a nil filter is only reachable
// from a QBTask built as a literal, which is how this package's tests construct
// it. Centralising the substitution means a new consumer cannot reintroduce the
// nil panic by forgetting a check.
func (t *QBTask) filter() arr.Filter {
	if t.arrFilter == nil {
		return arr.NoopFilter()
	}
	return t.arrFilter
}

// applyArrSkippedTag marks a source torrent the filter rejected.
//
// The tag is for the operator, not for the filter: skipping is decided by the
// verdict, never by the tag's presence. Deciding on the tag would freeze the
// verdict, because a tagged torrent would be excluded before it could ever be
// re-checked and un-tagged.
func (t *QBTask) applyArrSkippedTag(ctx context.Context, hash string, reason arr.Reason) {
	if t.cfg.ArrSkippedTag == "" || t.cfg.DryRun {
		return
	}
	if err := t.srcClient.AddTagsCtx(ctx, []string{hash}, t.cfg.ArrSkippedTag); err != nil {
		metrics.TagApplicationErrorsTotal.WithLabelValues(metrics.ModeSource).Inc()
		t.logger.ErrorContext(ctx, "failed to add arr-skipped tag",
			"hash", hash, "tag", t.cfg.ArrSkippedTag, "reason", reason, "error", err)
	}
}

// removeArrSkippedTagIfPresent clears the marker once a torrent is syncable
// again, so a verdict that flips back to SYNC does not leave a stale tag behind.
func (t *QBTask) removeArrSkippedTagIfPresent(ctx context.Context, hash, currentTags string) {
	if t.cfg.ArrSkippedTag == "" || t.cfg.DryRun {
		return
	}
	if !hasTag(currentTags, t.cfg.ArrSkippedTag) {
		return
	}
	if err := t.srcClient.RemoveTagsCtx(ctx, []string{hash}, t.cfg.ArrSkippedTag); err != nil {
		metrics.TagApplicationErrorsTotal.WithLabelValues(metrics.ModeSource).Inc()
		t.logger.ErrorContext(ctx, "failed to remove arr-skipped tag",
			"hash", hash, "tag", t.cfg.ArrSkippedTag, "error", err)
	}
}

// recheckArrRejectedTorrents aborts tracked torrents whose verdict has flipped
// to SKIP since tracking began.
//
// A pre-sync check alone is not enough: *arr commonly rejects an import after
// the grab, which is exactly when the transfer is already running and still
// consuming bandwidth and destination disk.
//
// One batched call rather than one per torrent. The verdict comes from the
// destination, so per-torrent calls would each cost a round trip across the
// link; the fan-out and its budget now live behind the filter, where the
// lookups are local.
func (t *QBTask) recheckArrRejectedTorrents(ctx context.Context) {
	tracked := t.store.TrackedSnapshot()
	if len(tracked) == 0 {
		return
	}

	// Reuse the torrents already fetched this cycle rather than issuing another
	// listing purely to learn categories.
	categoryFor := make(map[string]string, len(t.cycleTorrents))
	for i := range t.cycleTorrents {
		categoryFor[t.cycleTorrents[i].Hash] = t.cycleTorrents[i].Category
	}

	items := make([]arr.CheckItem, 0, len(tracked))
	for hash := range tracked {
		category, ok := categoryFor[hash]
		if !ok {
			// Gone from qBittorrent this cycle; removal handling owns it.
			continue
		}
		items = append(items, arr.CheckItem{Hash: hash, Category: category})
	}
	if len(items) == 0 {
		return
	}

	// Bound the whole batch to a fraction of the cycle so a slow *arr cannot
	// stall the orchestrator. Anything unresolved inside it fails open.
	budget := min(t.cfg.SleepInterval/arrRecheckBudgetDivisor, arrRecheckBudgetMax)
	if budget <= 0 {
		budget = arrRecheckBudgetMax
	}
	batchCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	decisions := t.filter().ShouldSyncAll(batchCtx, items)
	if len(decisions) != len(items) {
		t.logger.WarnContext(ctx, "arr re-check returned a mismatched verdict count",
			"requested", len(items), "returned", len(decisions))
		return
	}

	for i, d := range decisions {
		if d.Reason == arr.ReasonBudgetExceeded {
			metrics.ArrLookupSkippedBudgetTotal.Inc()
		}
		if !d.Sync {
			t.abortArrFlipped(ctx, tracked, items[i].Hash, d)
		}
	}
}

// abortArrFlipped abandons an in-progress sync whose verdict flipped to SKIP.
//
// Deletes the destination's partial data, unlike quarantine which preserves it.
// The distinction is intent: quarantine is a fault we expect to clear, whereas
// an *arr rejection means the torrent was never wanted, so keeping the bytes
// would defeat the purpose of the filter.
func (t *QBTask) abortArrFlipped(
	ctx context.Context,
	tracked map[string]TrackedTorrent,
	hash string,
	decision arr.Decision,
) {
	info, ok := tracked[hash]
	if !ok {
		return
	}

	t.applyArrSkippedTag(ctx, hash, decision.Reason)
	t.logger.InfoContext(ctx, "arr re-check: aborting in-progress sync",
		"hash", hash, "name", info.Name, "reason", decision.Reason)

	if !t.cfg.DryRun {
		abortCtx, cancel := withDestRPCTimeout(ctx)
		_, abortErr := t.grpcDest.AbortTorrent(abortCtx, hash, true)
		cancel()
		if abortErr != nil {
			t.logger.WarnContext(ctx, "arr re-check: AbortTorrent failed",
				"hash", hash, "error", abortErr)
		}
	}

	t.releaseTorrent(hash)

	instance := decision.Instance
	if instance == "" {
		instance = arrInstanceUnknown
	}
	metrics.ArrAbortedTotal.WithLabelValues(instance, string(decision.Reason)).Inc()
}

// arr re-check budget. The lookups themselves are local to the destination, so
// this bounds the round trip and the destination's own fan-out together.
const (
	arrRecheckBudgetMax     = 15 * time.Second
	arrRecheckBudgetDivisor = 2
)
