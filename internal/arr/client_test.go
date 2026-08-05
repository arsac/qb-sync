package arr

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
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

func TestGetHistoryByDownloadIDReturnsRecords(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v3/history" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("downloadId"); got != "ABC123" {
			t.Errorf("expected uppercase downloadId=ABC123, got %q", got)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"records":[
			{"eventType":"grabbed","downloadId":"ABC123","date":"2026-04-29T10:00:00Z"}
		]}`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "k", time.Second)
	records, err := c.GetHistoryByDownloadID(context.Background(), "abc123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].EventType != "grabbed" {
		t.Errorf("expected eventType=grabbed, got %q", records[0].EventType)
	}
	if !strings.EqualFold(records[0].DownloadID, "abc123") {
		t.Errorf("expected DownloadID to match abc123 case-insensitively, got %q", records[0].DownloadID)
	}
}

// TestGetHistoryByDownloadIDQueryShape pins the request parameters, because
// each one silently changes the answer rather than failing.
//
// *arr stores DownloadId as torrent.Hash.ToUpper() and filters on exact SQL
// equality, so a lowercase hash matches nothing. No records reads as "not
// rejected", which means a wrong case turns the whole filter into a no-op that
// still looks healthy. The page size matters for the same reason: it defaults
// to 10, and Sonarr writes one row per episode, so a season pack can push the
// terminal event off the first page.
func TestGetHistoryByDownloadIDQueryShape(t *testing.T) {
	var got url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.URL.Query()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"records":[]}`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "k", time.Second)
	if _, err := c.GetHistoryByDownloadID(context.Background(), "abc123"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := map[string]string{
		"downloadId":    "ABC123",
		"pageSize":      "100",
		"sortKey":       "date",
		"sortDirection": "descending",
	}
	for key, expected := range want {
		if got.Get(key) != expected {
			t.Errorf("query %s = %q, want %q", key, got.Get(key), expected)
		}
	}
}

// TestGetHistoryByDownloadIDIgnoresOtherTorrents guards against an *arr that
// stops honouring the downloadId filter. Unfiltered history would otherwise let
// another torrent's downloadFailed skip a torrent the user wanted.
func TestGetHistoryByDownloadIDIgnoresOtherTorrents(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"records":[
			{"eventType":"downloadFailed","downloadId":"OTHERHASH","date":"2026-04-29T11:00:00Z"},
			{"eventType":"grabbed","downloadId":"abc123","date":"2026-04-29T10:00:00Z"}
		]}`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "k", time.Second)
	records, err := c.GetHistoryByDownloadID(context.Background(), "ABC123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected only this torrent's record, got %d", len(records))
	}
	if records[0].EventType != "grabbed" {
		t.Errorf("kept the wrong record: %q", records[0].EventType)
	}
}

func TestGetHistoryByDownloadIDClassifies5xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "k", time.Second)
	_, err := c.GetHistoryByDownloadID(context.Background(), "abc")
	var arrErr *Error
	if !errors.As(err, &arrErr) || arrErr.Kind != KindHTTP5xx {
		t.Fatalf("expected Error.Kind=KindHTTP5xx, got %v", err)
	}
}

// The rate-limit kind is still classified, since it labels
// ArrLookupErrorsTotal. The Retry-After value itself is not read: nothing
// honoured it, and an unused field implies a backoff that does not exist.
func TestClassifyStatusErrorClassifiesRateLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "k", time.Second)
	_, err := c.GetHistoryByDownloadID(context.Background(), "h")
	var arrErr *Error
	if !errors.As(err, &arrErr) || arrErr.Kind != KindRateLimited {
		t.Fatalf("expected Kind=KindRateLimited, got %v", err)
	}
}

func TestPerCallTimeoutFiresKindTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "k", 50*time.Millisecond)
	err := c.Ping(context.Background())
	var arrErr *Error
	if !errors.As(err, &arrErr) || arrErr.Kind != KindTimeout {
		t.Fatalf("expected KindTimeout, got %v", err)
	}
}
