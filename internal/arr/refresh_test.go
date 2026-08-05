package arr

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/arsac/qb-sync/internal/metrics"
)

// categoryServer serves a download-client list containing one category, or
// fails with 500 when down is set.
func categoryServer(t *testing.T, field, category string, down *bool) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if *down {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"name":"qbit","enable":true,"protocol":"torrent","fields":[
			{"name":"` + field + `","value":"` + category + `"}
		]}]`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func refreshService(t *testing.T, instances map[string]*httptest.Server) *Service {
	t.Helper()
	im := make(map[string]*instanceState, len(instances))
	for name, srv := range instances {
		im[name] = &instanceState{name: name, client: NewClient(srv.URL, "k", time.Second)}
	}
	return &Service{
		instances: im,
		routes:    map[string]string{},
		cache:     newVerdictCache(),
		logger:    slog.New(slog.NewTextHandler(testWriter{t: t}, nil)),
		now:       time.Now,
	}
}

// Both instances contribute, and each category routes to the one that claims
// it. Testing a single instance would not catch a merge that dropped the other.
func TestRefreshCategoriesPullsFromBothInstances(t *testing.T) {
	t.Parallel()

	up := false
	svc := refreshService(t, map[string]*httptest.Server{
		instanceRadarr: categoryServer(t, "movieCategory", "radarr", &up),
		instanceSonarr: categoryServer(t, "tvCategory", "tv-sonarr", &up),
	})

	if err := svc.RefreshCategories(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := svc.RoutedCategories(); !slices.Equal(got, []string{"radarr", "tv-sonarr"}) {
		t.Errorf("routed = %v, want both categories", got)
	}
	for category, want := range map[string]string{"radarr": instanceRadarr, "tv-sonarr": instanceSonarr} {
		if got, ok := svc.instanceFor(category); !ok || got != want {
			t.Errorf("%q routes to %q (found=%v), want %q", category, got, ok, want)
		}
	}
}

// One instance being unreachable must not unroute the other's categories, nor
// its own: dropping them would silently stop filtering torrents that were
// being filtered a moment ago.
func TestRefreshCategoriesKeepsRoutesWhenOneInstanceFails(t *testing.T) {
	t.Parallel()

	radarrDown, sonarrDown := false, false
	svc := refreshService(t, map[string]*httptest.Server{
		instanceRadarr: categoryServer(t, "movieCategory", "radarr", &radarrDown),
		instanceSonarr: categoryServer(t, "tvCategory", "tv-sonarr", &sonarrDown),
	})

	if err := svc.RefreshCategories(context.Background()); err != nil {
		t.Fatalf("first refresh: %v", err)
	}

	radarrDown = true
	err := svc.RefreshCategories(context.Background())
	if err == nil {
		t.Error("a failing instance must be reported, not swallowed")
	}

	// Sonarr re-discovered, Radarr carried forward from the last good refresh.
	if got := svc.RoutedCategories(); !slices.Equal(got, []string{"radarr", "tv-sonarr"}) {
		t.Errorf("routed = %v, want both retained", got)
	}
	if got, ok := svc.instanceFor("radarr"); !ok || got != instanceRadarr {
		t.Errorf("the failing instance's routes were dropped: %q found=%v", got, ok)
	}
}

// Two instances can legitimately claim one category if an operator points both
// at the same qBittorrent category. Map iteration would hand it to a different
// owner each refresh, so a torrent would be judged by whichever won that round.
func TestRefreshCategoriesResolvesConflictsDeterministically(t *testing.T) {
	t.Parallel()

	up := false
	for range 8 {
		svc := refreshService(t, map[string]*httptest.Server{
			instanceRadarr: categoryServer(t, "movieCategory", "shared", &up),
			instanceSonarr: categoryServer(t, "tvCategory", "shared", &up),
		})
		if err := svc.RefreshCategories(context.Background()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		owner, ok := svc.instanceFor("shared")
		if !ok {
			t.Fatal("the contested category should still route somewhere")
		}
		if owner != instanceRadarr {
			t.Fatalf("owner = %q, want %q consistently: sorted order decides, not map iteration",
				owner, instanceRadarr)
		}
	}
}

// The gauge has to count what an instance actually owns, not what it reported.
// A category lost to a conflict still comes back in the loser's reply, so
// counting the reply would advertise a filter that is in fact inert for it -
// exactly the "is this thing wired up?" question the metric exists to answer.
//
// Instance names are unique to this test: the gauge is a package-level
// collector, so sharing labels with the parallel tests above would let them
// overwrite each other's values.
func TestRefreshCategoriesGaugeCountsOwnedNotReported(t *testing.T) {
	t.Parallel()

	const winner, loser = "radarr-gauge", "sonarr-gauge"

	up := false
	svc := refreshService(t, map[string]*httptest.Server{
		winner: categoryServer(t, "movieCategory", "contested", &up),
		loser:  categoryServer(t, "tvCategory", "contested", &up),
	})
	if err := svc.RefreshCategories(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := testutil.ToFloat64(metrics.ArrRoutedCategories.WithLabelValues(winner)); got != 1 {
		t.Errorf("winner gauge = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.ArrRoutedCategories.WithLabelValues(loser)); got != 0 {
		t.Errorf("loser gauge = %v, want 0: it reported the category but does not own it", got)
	}
}

// Discovery replacing a renamed category is the whole reason it is discovered
// rather than configured.
func TestRefreshCategoriesPicksUpRenames(t *testing.T) {
	t.Parallel()

	up := false
	category := "radarr"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"name":"qbit","enable":true,"protocol":"torrent","fields":[
			{"name":"movieCategory","value":"` + category + `"}
		]}]`))
	}))
	t.Cleanup(srv.Close)
	_ = up

	svc := refreshService(t, map[string]*httptest.Server{instanceRadarr: srv})
	if err := svc.RefreshCategories(context.Background()); err != nil {
		t.Fatalf("first refresh: %v", err)
	}

	category = "radarr-4k"
	if err := svc.RefreshCategories(context.Background()); err != nil {
		t.Fatalf("second refresh: %v", err)
	}

	if got := svc.RoutedCategories(); !slices.Equal(got, []string{"radarr-4k"}) {
		t.Errorf("routed = %v, want only the renamed category", got)
	}
	if _, ok := svc.instanceFor("radarr"); ok {
		t.Error("the old category should stop routing once *arr no longer uses it")
	}
}
