package arr

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestClientPingSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v3/system/status" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if got := r.Header.Get("X-Api-Key"); got != "test-key" {
			t.Errorf("expected X-Api-Key header 'test-key', got %q", got)
		}
		if r.URL.Query().Get("apikey") != "" {
			t.Errorf("API key must not appear in URL query")
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"appName":"Radarr","version":"5.0.0"}`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "test-key", time.Second)
	if err := c.Ping(context.Background()); err != nil {
		t.Fatalf("Ping returned error: %v", err)
	}
}

func TestClientPingUnauthorized(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "bad-key", time.Second)
	err := c.Ping(context.Background())
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	var arrErr *Error
	if !errors.As(err, &arrErr) || arrErr.Kind != KindUnauthorized {
		t.Fatalf("expected *Error with Kind=KindUnauthorized, got %v", err)
	}
}
