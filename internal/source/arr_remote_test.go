package source

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/arsac/qb-sync/internal/arr"
	"github.com/arsac/qb-sync/internal/config"
	pb "github.com/arsac/qb-sync/proto"
)

func remoteItems() []arr.CheckItem {
	return []arr.CheckItem{
		{Hash: "aaa", Category: "radarr"},
		{Hash: "bbb", Category: "tv-sonarr"},
	}
}

// TestRemoteArrFilterFailsOpen covers every way the destination can fail to
// answer. All of them must sync: the filter only ever saves work, so refusing
// to sync because a verdict is unavailable would be strictly worse than syncing
// something unwanted.
func TestRemoteArrFilterFailsOpen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		resp *pb.CheckArrRejectionsResponse
	}{
		{
			name: "destination unreachable",
			err:  status.Error(codes.Unavailable, "connection refused"),
		},
		{
			name: "deadline exceeded",
			err:  status.Error(codes.DeadlineExceeded, "budget spent"),
		},
		{
			name: "destination predates the rpc",
			err:  status.Error(codes.Unimplemented, "unknown method"),
		},
		{
			name: "plain transport error",
			err:  errors.New("broken pipe"),
		},
		{
			name: "destination has no arr configured",
			resp: &pb.CheckArrRejectionsResponse{FilterEnabled: false},
		},
		{
			name: "verdict count does not match the request",
			resp: &pb.CheckArrRejectionsResponse{
				FilterEnabled: true,
				Verdicts:      []*pb.ArrVerdict{{TorrentHash: "aaa", Sync: false}},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			filter := newRemoteArrFilter(
				func(context.Context, *pb.CheckArrRejectionsRequest) (*pb.CheckArrRejectionsResponse, error) {
					return tc.resp, tc.err
				},
				testStoreLogger(),
			)

			decisions := filter.ShouldSyncAll(context.Background(), remoteItems())

			if len(decisions) != len(remoteItems()) {
				t.Fatalf("got %d decisions, want %d", len(decisions), len(remoteItems()))
			}
			for i, d := range decisions {
				if !d.Sync {
					t.Errorf("decision %d must fail open, got skip with reason %q", i, d.Reason)
				}
			}
		})
	}
}

// TestRemoteArrFilterRelaysVerdicts checks the happy path, including that the
// instance name survives the round trip: the source no longer knows the
// category routing, so a dropped instance would silently degrade the abort
// metric to "unknown".
func TestRemoteArrFilterRelaysVerdicts(t *testing.T) {
	t.Parallel()

	filter := newRemoteArrFilter(
		func(_ context.Context, req *pb.CheckArrRejectionsRequest) (*pb.CheckArrRejectionsResponse, error) {
			if len(req.GetItems()) != 2 {
				t.Errorf("expected 2 items in one batch, got %d", len(req.GetItems()))
			}
			return &pb.CheckArrRejectionsResponse{
				FilterEnabled: true,
				Verdicts: []*pb.ArrVerdict{
					{TorrentHash: "aaa", Sync: false, Reason: "download_ignored", Instance: "radarr"},
					{TorrentHash: "bbb", Sync: true, Reason: "not_rejected", Instance: "sonarr"},
				},
			}, nil
		},
		testStoreLogger(),
	)

	decisions := filter.ShouldSyncAll(context.Background(), remoteItems())

	if decisions[0].Sync {
		t.Error("a rejected torrent must come back as skip")
	}
	if decisions[0].Reason != arr.ReasonIgnored {
		t.Errorf("reason = %q, want %q", decisions[0].Reason, arr.ReasonIgnored)
	}
	if decisions[0].Instance != "radarr" {
		t.Errorf("instance = %q, want radarr", decisions[0].Instance)
	}
	if !decisions[1].Sync {
		t.Error("an accepted torrent must come back as sync")
	}
}

// TestRemoteArrFilterStopsAskingWhenUnsupported pins the latch. Without it a
// destination that cannot answer would be asked once per torrent per cycle
// forever, which is the cost this whole relay exists to avoid.
func TestRemoteArrFilterStopsAskingWhenUnsupported(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		err  error
		resp *pb.CheckArrRejectionsResponse
	}{
		"unimplemented": {err: status.Error(codes.Unimplemented, "unknown method")},
		"not configured": {
			resp: &pb.CheckArrRejectionsResponse{FilterEnabled: false},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var calls int
			filter := newRemoteArrFilter(
				func(context.Context, *pb.CheckArrRejectionsRequest) (*pb.CheckArrRejectionsResponse, error) {
					calls++
					return tc.resp, tc.err
				},
				testStoreLogger(),
			)

			for range 3 {
				filter.ShouldSyncAll(context.Background(), remoteItems())
			}

			if calls != 1 {
				t.Errorf("asked the destination %d times, want 1: it cannot answer until restarted", calls)
			}
		})
	}
}

// TestRemoteArrFilterKeepsAskingOnTransientErrors is the counterpart: an
// unreachable destination comes back, so latching off there would disable the
// filter for the lifetime of the process over a blip.
func TestRemoteArrFilterKeepsAskingOnTransientErrors(t *testing.T) {
	t.Parallel()

	var calls int
	filter := newRemoteArrFilter(
		func(context.Context, *pb.CheckArrRejectionsRequest) (*pb.CheckArrRejectionsResponse, error) {
			calls++
			return nil, status.Error(codes.Unavailable, "connection refused")
		},
		testStoreLogger(),
	)

	for range 3 {
		filter.ShouldSyncAll(context.Background(), remoteItems())
	}

	if calls != 3 {
		t.Errorf("asked the destination %d times, want 3: unavailable is transient", calls)
	}
}

// TestBuildArrFilterPicksModeFromConfig pins the selection rule: where the
// instances are configured is the mode. Getting this backwards would send every
// lookup across the link for a service the process can already reach, or query
// instances it cannot see at all.
func TestBuildArrFilterPicksModeFromConfig(t *testing.T) {
	t.Parallel()

	radarrCfg := config.ArrInstanceConfig{
		URL:        "http://127.0.0.1:1",
		APIKey:     "k",
		Categories: []string{"radarr"},
	}
	// A distinct category: routing the same one to both instances is a conflict
	// that arr.New rejects, which would fall back to relaying and make the
	// "both configured" case pass for the wrong reason.
	sonarrCfg := config.ArrInstanceConfig{
		URL:        "http://127.0.0.1:2",
		APIKey:     "k",
		Categories: []string{"tv-sonarr"},
	}

	tests := []struct {
		name       string
		radarr     config.ArrInstanceConfig
		sonarr     config.ArrInstanceConfig
		wantRemote bool
	}{
		{name: "nothing configured locally relays", wantRemote: true},
		{name: "radarr configured locally queries directly", radarr: radarrCfg},
		{name: "sonarr configured locally queries directly", sonarr: sonarrCfg},
		{name: "both configured locally queries directly", radarr: radarrCfg, sonarr: sonarrCfg},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := &config.SourceConfig{}
			cfg.Radarr = tc.radarr
			cfg.Sonarr = tc.sonarr
			runner := &Runner{cfg: cfg, logger: testStoreLogger()}

			// dest is only dereferenced for its method value, which the remote
			// filter stores without calling.
			filter := runner.buildArrFilter(context.Background(), nil)

			// Assert on the direct type: the relay is wrapped in a cache, so its
			// concrete type is an implementation detail, whereas querying *arr
			// directly always yields a *arr.Service.
			_, isDirect := filter.(*arr.Service)
			if isDirect == tc.wantRemote {
				t.Errorf("direct=%v with wantRemote=%v", isDirect, tc.wantRemote)
			}
		})
	}
}
