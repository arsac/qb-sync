package arr

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
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
		if got := r.URL.Query().Get("downloadId"); got != "abc123" {
			t.Errorf("expected downloadId=abc123, got %q", got)
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

func TestGetHistoryByDownloadIDLowercasesQuery(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("downloadId"); got != "abc123" {
			t.Errorf("expected lowercased downloadId=abc123, got %q", got)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"records":[]}`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "k", time.Second)
	_, err := c.GetHistoryByDownloadID(context.Background(), "ABC123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
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

func TestClassifyStatusErrorReadsRetryAfter(t *testing.T) {
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
	if arrErr.RetryAfter != 7*time.Second {
		t.Fatalf("expected RetryAfter=7s, got %v", arrErr.RetryAfter)
	}
}
