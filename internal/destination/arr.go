package destination

import (
	"context"
	"log/slog"

	"github.com/arsac/qb-sync/internal/arr"
	pb "github.com/arsac/qb-sync/proto"
)

// buildArrFilter constructs the *arr filter from config, reporting whether any
// instance is actually configured.
//
// A construction failure degrades to the no-op filter rather than failing
// startup. The filter only ever saves work; refusing to serve because Sonarr is
// misconfigured would take the whole destination offline for something no sync
// depends on. Config validation already rejects the shapes worth rejecting.
func buildArrFilter(cfg arr.Config, logger *slog.Logger) (arr.Filter, bool) {
	if cfg.Radarr.IsZero() && cfg.Sonarr.IsZero() {
		return arr.NoopFilter(), false
	}

	filter, err := arr.New(cfg, logger.With("component", "arr"))
	if err != nil {
		logger.Error("arr filter disabled: construction failed", "error", err)
		return arr.NoopFilter(), false
	}
	return filter, true
}

// CheckArrRejections answers whether *arr rejected each torrent.
//
// The lookup runs here because this is where the *arr instances are; the source
// only gets the verdict. Fail-open is preserved end to end: arr.Service maps
// every error to sync=true, so this handler has no error path of its own and
// never returns a gRPC error for an *arr problem. A caller that cannot reach
// this RPC at all fails open on its side.
func (s *Server) CheckArrRejections(
	ctx context.Context,
	req *pb.CheckArrRejectionsRequest,
) (*pb.CheckArrRejectionsResponse, error) {
	resp := &pb.CheckArrRejectionsResponse{
		FilterEnabled: s.arrEnabled,
		Verdicts:      make([]*pb.ArrVerdict, 0, len(req.GetItems())),
	}
	if len(req.GetItems()) == 0 {
		return resp, nil
	}

	items := make([]arr.CheckItem, len(req.GetItems()))
	for i, item := range req.GetItems() {
		items[i] = arr.CheckItem{Hash: item.GetTorrentHash(), Category: item.GetCategory()}
	}

	for i, decision := range s.arrFilter.ShouldSyncAll(ctx, items) {
		resp.Verdicts = append(resp.Verdicts, &pb.ArrVerdict{
			TorrentHash: items[i].Hash,
			Sync:        decision.Sync,
			Reason:      string(decision.Reason),
			Instance:    decision.Instance,
		})
	}
	return resp, nil
}
