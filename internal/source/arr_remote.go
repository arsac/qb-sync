package source

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/arsac/qb-sync/internal/arr"
	"github.com/arsac/qb-sync/internal/metrics"
	pb "github.com/arsac/qb-sync/proto"
)

// remoteArrFilter asks the destination for *arr verdicts instead of querying
// *arr directly.
//
// The instances are colocated with the destination, so the lookup belongs
// there: a history response can carry a hundred records, while the verdict is
// one bit, and the gRPC connection carrying it is already open for pieces.
//
// Fail-open is preserved on this side too. Anything that stops the destination
// answering yields sync, because a filter that blocked syncing whenever it
// could not reach *arr would be worse than one that occasionally syncs
// something unwanted.
type remoteArrFilter struct {
	logger *slog.Logger

	// check is injected so tests can drive the RPC boundary without a server.
	check func(context.Context, *pb.CheckArrRejectionsRequest) (*pb.CheckArrRejectionsResponse, error)

	// routed is what the destination last told us it routes. Held here so the
	// source can decide locally whether a torrent is worth asking about: on a
	// typical library most categories belong to no *arr, and each of those would
	// otherwise cost a round trip to be told so.
	routedMu sync.RWMutex
	routed   []string

	// disabled latches once the destination proves it cannot answer, either
	// because it predates the RPC or because it has no *arr configured. Latched
	// rather than re-tested every cycle so the log stays quiet, and reset only
	// by a restart, which is also when a redeployed destination gets picked up.
	//
	// A single atomic rather than a mutex plus a sync.Once: logging inside the
	// compare-and-swap makes "log exactly once" fall out of the latch itself,
	// and lets each reason keep its own message. A shared Once would let
	// whichever fired first silence the other.
	disabled atomic.Bool
}

// Compile-time interface assertion.
var _ arr.Filter = (*remoteArrFilter)(nil)

// NewRemoteArrFilter builds a relay-backed filter. Exported for e2e wiring,
// which constructs the task directly rather than through Runner.
func NewRemoteArrFilter(
	check func(context.Context, *pb.CheckArrRejectionsRequest) (*pb.CheckArrRejectionsResponse, error),
	logger *slog.Logger,
) arr.Filter {
	return newRemoteArrFilter(check, logger)
}

func newRemoteArrFilter(
	check func(context.Context, *pb.CheckArrRejectionsRequest) (*pb.CheckArrRejectionsResponse, error),
	logger *slog.Logger,
) *remoteArrFilter {
	return &remoteArrFilter{check: check, logger: logger}
}

// ShouldSync resolves a single torrent as a batch of one.
// RefreshCategories asks the destination what it routes. An empty batch is a
// verdict-free request, so this costs one small round trip.
func (r *remoteArrFilter) RefreshCategories(ctx context.Context) error {
	if r.disabled.Load() {
		return nil
	}
	resp, err := r.check(ctx, &pb.CheckArrRejectionsRequest{})
	if err != nil {
		metrics.ArrRelayErrorsTotal.WithLabelValues(status.Code(err).String()).Inc()
		return err
	}
	r.storeRouted(resp)
	return nil
}

// RoutedCategories returns the destination's routed set as last reported.
func (r *remoteArrFilter) RoutedCategories() []string {
	r.routedMu.RLock()
	defer r.routedMu.RUnlock()
	return slices.Clone(r.routed)
}

// storeRouted keeps the routed set current from any response, so an ordinary
// verdict request doubles as a refresh.
func (r *remoteArrFilter) storeRouted(resp *pb.CheckArrRejectionsResponse) {
	r.routedMu.Lock()
	r.routed = resp.GetCategories()
	r.routedMu.Unlock()
}

func (r *remoteArrFilter) ShouldSync(ctx context.Context, hash, category string) arr.Decision {
	decisions := r.ShouldSyncAll(ctx, []arr.CheckItem{{Hash: hash, Category: category}})
	if len(decisions) != 1 {
		return arr.Decision{Sync: true, Reason: arr.ReasonRelayFailed}
	}
	return decisions[0]
}

// ShouldSyncAll resolves a batch in one round trip.
func (r *remoteArrFilter) ShouldSyncAll(ctx context.Context, items []arr.CheckItem) []arr.Decision {
	if len(items) == 0 {
		return nil
	}
	if r.disabled.Load() {
		return failOpen(items, arr.ReasonNoCategory)
	}

	req := &pb.CheckArrRejectionsRequest{Items: make([]*pb.ArrCheckItem, len(items))}
	for i, item := range items {
		req.Items[i] = &pb.ArrCheckItem{TorrentHash: item.Hash, Category: item.Category}
	}

	resp, err := r.check(ctx, req)
	if err != nil {
		return r.failOpenOnError(ctx, items, err)
	}

	if !resp.GetFilterEnabled() {
		// Not an error: the destination simply has no *arr configured. Stop
		// asking rather than paying a round trip per torrent per cycle forever.
		if r.disabled.CompareAndSwap(false, true) {
			r.logger.InfoContext(ctx, "arr filter inactive: destination has no arr instances configured")
		}
		return failOpen(items, arr.ReasonNoCategory)
	}

	r.storeRouted(resp)

	verdicts := resp.GetVerdicts()
	if len(verdicts) != len(items) {
		// A response that does not line up cannot be attributed to torrents, and
		// guessing would risk skipping the wrong one.
		metrics.ArrRelayErrorsTotal.WithLabelValues("malformed").Inc()
		r.logger.WarnContext(ctx, "arr relay returned a mismatched verdict count",
			"requested", len(items), "returned", len(verdicts))
		return failOpen(items, arr.ReasonRelayFailed)
	}

	decisions := make([]arr.Decision, len(items))
	for i, v := range verdicts {
		decisions[i] = arr.Decision{
			Sync:     v.GetSync(),
			Reason:   arr.Reason(v.GetReason()),
			Instance: v.GetInstance(),
		}
	}
	return decisions
}

// failOpenOnError maps a transport failure to sync verdicts, and latches off
// permanently when the destination does not implement the RPC at all.
func (r *remoteArrFilter) failOpenOnError(
	ctx context.Context,
	items []arr.CheckItem,
	err error,
) []arr.Decision {
	code := status.Code(err)
	metrics.ArrRelayErrorsTotal.WithLabelValues(code.String()).Inc()

	if code == codes.Unimplemented {
		// An older destination that predates this RPC. It will never start
		// answering without a restart, so stop asking.
		if r.disabled.CompareAndSwap(false, true) {
			r.logger.WarnContext(ctx, "arr filter inactive: destination does not support arr checks",
				"error", err)
		}
		return failOpen(items, arr.ReasonNoCategory)
	}

	// Everything else is transient. Worth noting that the source cannot sync
	// anything while the destination is unreachable either, so failing open here
	// costs nothing in practice.
	r.logger.WarnContext(ctx, "arr relay unavailable, syncing without a verdict",
		"code", code.String(), "items", len(items), "error", err)
	return failOpen(items, arr.ReasonRelayFailed)
}

// failOpen returns a sync verdict for every item.
func failOpen(items []arr.CheckItem, reason arr.Reason) []arr.Decision {
	decisions := make([]arr.Decision, len(items))
	for i := range decisions {
		decisions[i] = arr.Decision{Sync: true, Reason: reason}
	}
	return decisions
}
