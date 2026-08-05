package arr

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"
)

// The response shape mirrors what a real Sonarr returns: categories live in the
// fields array, named per app, alongside the enable and protocol flags.
const downloadClientsJSON = `[
  {"name":"qbit","enable":true,"protocol":"torrent","implementation":"QBittorrent","fields":[
    {"name":"host","value":"127.0.0.1"},
    {"name":"port","value":8080},
    {"name":"tvCategory","value":"tv-sonarr"},
    {"name":"tvImportedCategory","value":"tv-done"}
  ]},
  {"name":"disabled","enable":false,"protocol":"torrent","implementation":"QBittorrent","fields":[
    {"name":"tvCategory","value":"never-used"}
  ]},
  {"name":"usenet","enable":true,"protocol":"usenet","implementation":"Sabnzbd","fields":[
    {"name":"tvCategory","value":"nzb-tv"}
  ]},
  {"name":"blank","enable":true,"protocol":"torrent","implementation":"QBittorrent","fields":[
    {"name":"tvCategory","value":"  "}
  ]}
]`

func TestDownloadClientCategories(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v3/downloadclient" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(downloadClientsJSON))
	}))
	t.Cleanup(srv.Close)

	got, err := NewClient(srv.URL, "k", time.Second).DownloadClientCategories(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Both category kinds, because *arr moves a torrent to the imported one
	// after a successful import: knowing only the download category would stop
	// recognising exactly the torrents that completed.
	want := []string{"tv-sonarr", "tv-done"}
	if !slices.Equal(got, want) {
		t.Errorf("categories = %v, want %v", got, want)
	}
}

// TestDownloadClientCategoriesIgnoresIrrelevantClients pins the exclusions. A
// disabled client assigns nothing, and usenet has no torrents to sync - either
// would otherwise widen the routed set and cost lookups for torrents no *arr
// download client ever touched.
func TestDownloadClientCategoriesIgnoresIrrelevantClients(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(downloadClientsJSON))
	}))
	t.Cleanup(srv.Close)

	got, _ := NewClient(srv.URL, "k", time.Second).DownloadClientCategories(context.Background())

	for _, unwanted := range []string{"never-used", "nzb-tv", "  "} {
		if slices.Contains(got, unwanted) {
			t.Errorf("category %q should have been ignored, got %v", unwanted, got)
		}
	}
}

// Radarr calls the field movieCategory where Sonarr calls it tvCategory. The
// suffix match is what keeps the client unaware of which app it is talking to.
func TestDownloadClientCategoriesWorksForBothApps(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"name":"qbit","enable":true,"protocol":"torrent","fields":[
			{"name":"movieCategory","value":"radarr"},
			{"name":"movieImportedCategory","value":"radarr-done"}
		]}]`))
	}))
	t.Cleanup(srv.Close)

	got, err := NewClient(srv.URL, "k", time.Second).DownloadClientCategories(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !slices.Equal(got, []string{"radarr", "radarr-done"}) {
		t.Errorf("categories = %v, want radarr and radarr-done", got)
	}
}

func TestDownloadClientCategoriesSurfacesFailure(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	// Discovery must report failure rather than return an empty set: empty
	// means "this instance routes nothing", which would silently disable
	// filtering for it.
	if _, err := NewClient(srv.URL, "k", time.Second).DownloadClientCategories(context.Background()); err == nil {
		t.Fatal("expected an error, got nil")
	}
}
