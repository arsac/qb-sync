package arr

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/arsac/qb-sync/internal/metrics"
)

// historyHandler returns a handler that responds with the given records list as JSON.
func historyHandler(t *testing.T, records string) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/api/v3/history") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"records":` + records + `}`))
	})
}

func newTestService(t *testing.T, instances ...*instanceState) *Service {
	t.Helper()
	im := make(map[string]*instanceState, len(instances))
	rt := make(map[string]string)
	for _, ins := range instances {
		im[ins.name] = ins
		for _, cat := range ins.categories {
			rt[cat] = ins.name
		}
	}
	return &Service{
		instances: im,
		routes:    rt,
		cache:     newVerdictCache(50 * time.Millisecond),
		logger:    slog.New(slog.NewTextHandler(testWriter{t: t}, nil)),
	}
}

// testWriter forwards writes to t.Log so test logs appear with the test.
type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) { w.t.Log(string(p)); return len(p), nil }

func TestServiceShouldSyncNoCategoryRoute(t *testing.T) {
	svc := newTestService(t)
	d := svc.ShouldSync(context.Background(), "abc", "unmapped-category")
	if !d.Sync || d.Reason != ReasonNoCategory {
		t.Fatalf("expected SYNC/NoCategory, got %+v", d)
	}
}

func TestServiceShouldSyncIgnoredEvent(t *testing.T) {
	srv := httptest.NewServer(historyHandler(t,
		`[{"eventType":"downloadIgnored","downloadId":"abc","date":"2026-04-29T10:00:00Z"}]`))
	t.Cleanup(srv.Close)

	svc := newTestService(t, &instanceState{
		name:       "radarr",
		client:     NewClient(srv.URL, "k", time.Second),
		categories: []string{"radarr"},
	})

	d := svc.ShouldSync(context.Background(), "abc", "radarr")
	if d.Sync {
		t.Fatalf("expected SKIP, got %+v", d)
	}
	if d.Reason != ReasonIgnored {
		t.Fatalf("expected ReasonIgnored, got %q", d.Reason)
	}
}

func TestServiceShouldSyncEmptyHistory(t *testing.T) {
	srv := httptest.NewServer(historyHandler(t, `[]`))
	t.Cleanup(srv.Close)

	svc := newTestService(t, &instanceState{
		name:       "sonarr",
		client:     NewClient(srv.URL, "k", time.Second),
		categories: []string{"tv-sonarr"},
	})

	d := svc.ShouldSync(context.Background(), "abc", "tv-sonarr")
	if !d.Sync || d.Reason != ReasonEmptyHistory {
		t.Fatalf("expected SYNC/EmptyHistory, got %+v", d)
	}
}

func TestServiceShouldSyncCacheHit(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"records":[{"eventType":"downloadIgnored","downloadId":"abc"}]}`))
	}))
	t.Cleanup(srv.Close)

	svc := newTestService(t, &instanceState{
		name:       "radarr",
		client:     NewClient(srv.URL, "k", time.Second),
		categories: []string{"radarr"},
	})

	for range 5 {
		_ = svc.ShouldSync(context.Background(), "abc", "radarr")
	}
	if calls != 1 {
		t.Fatalf("expected 1 HTTP call (rest cached), got %d", calls)
	}
}

func TestServiceCircuitBreakerOpensAfterFailures(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	inst := &instanceState{
		name:       "radarr",
		client:     NewClient(srv.URL, "k", 100*time.Millisecond),
		categories: []string{"radarr"},
	}
	attachBreaker(inst, breakerConfig{MaxFailures: 3, ResetTimeout: time.Hour})

	svc := newTestService(t, inst)
	// Drive 3 distinct hashes through the failing endpoint to trip the breaker.
	for i := range 3 {
		hash := []string{"a", "b", "c"}[i]
		_ = svc.ShouldSync(context.Background(), hash, "radarr")
	}
	// Next call should short-circuit with ReasonCircuitOpen.
	d := svc.ShouldSync(context.Background(), "d", "radarr")
	if !d.Sync || d.Reason != ReasonCircuitOpen {
		t.Fatalf("expected SYNC/CircuitOpen, got %+v", d)
	}
}

func TestServiceEmitsDecisionMetric(t *testing.T) {
	srv := httptest.NewServer(historyHandler(t,
		`[{"eventType":"downloadIgnored","downloadId":"abc"}]`))
	t.Cleanup(srv.Close)

	svc := newTestService(t, &instanceState{
		name:       "radarr",
		client:     NewClient(srv.URL, "k", time.Second),
		categories: []string{"radarr"},
	})

	before := testutil.ToFloat64(metrics.ArrDecisionsTotal.WithLabelValues("radarr", "skipped"))
	_ = svc.ShouldSync(context.Background(), "abc", "radarr")
	after := testutil.ToFloat64(metrics.ArrDecisionsTotal.WithLabelValues("radarr", "skipped"))
	if after-before != 1 {
		t.Fatalf("expected counter to increment by 1, got delta=%v", after-before)
	}
}
