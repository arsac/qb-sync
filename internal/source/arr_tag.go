package source

import (
	"context"

	"github.com/arsac/qb-sync/internal/arr"
)

// applyArrSkippedTag adds the configured arr-skipped tag to the source torrent.
// Honors DryRun. Logs on transient API failure; the next decision cycle will retry.
func (t *QBTask) applyArrSkippedTag(ctx context.Context, hash string, reason arr.Reason) {
	if t.cfg.ArrSkippedTag == "" || t.cfg.DryRun {
		return
	}
	if err := t.srcClient.AddTagsCtx(ctx, []string{hash}, t.cfg.ArrSkippedTag); err != nil {
		t.logger.ErrorContext(ctx, "failed to add arr-skipped tag",
			"hash", hash, "tag", t.cfg.ArrSkippedTag, "reason", reason, "error", err)
	}
}

// removeArrSkippedTagIfPresent removes the arr-skipped tag from the source torrent
// when currentTags contains it. Honors DryRun. No-op if the tag is empty or absent.
func (t *QBTask) removeArrSkippedTagIfPresent(ctx context.Context, hash, currentTags string) {
	if t.cfg.ArrSkippedTag == "" || t.cfg.DryRun {
		return
	}
	if !hasTag(currentTags, t.cfg.ArrSkippedTag) {
		return
	}
	if err := t.srcClient.RemoveTagsCtx(ctx, []string{hash}, t.cfg.ArrSkippedTag); err != nil {
		t.logger.ErrorContext(ctx, "failed to remove arr-skipped tag",
			"hash", hash, "tag", t.cfg.ArrSkippedTag, "error", err)
	}
}
