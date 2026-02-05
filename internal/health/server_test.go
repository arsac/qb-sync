package health

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestServer_Healthz(t *testing.T) {
	logger := slog.Default()
	s := NewServer(Config{Addr: ":0"}, logger)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()

	s.handleHealthz(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}

	var resp Response
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp.Status != "ok" {
		t.Errorf("expected status 'ok', got %q", resp.Status)
	}
}

func TestServer_Livez_NoChecks(t *testing.T) {
	logger := slog.Default()
	s := NewServer(Config{Addr: ":0"}, logger)

	req := httptest.NewRequest(http.MethodGet, "/livez", nil)
	w := httptest.NewRecorder()

	s.handleLivez(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}
}

func TestServer_Livez_WithHealthyCheck(t *testing.T) {
	logger := slog.Default()
	s := NewServer(Config{Addr: ":0"}, logger)

	s.RegisterCheck("test", func(_ context.Context) error {
		return nil
	})

	req := httptest.NewRequest(http.MethodGet, "/livez", nil)
	w := httptest.NewRecorder()

	s.handleLivez(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}

	var resp Response
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp.Checks["test"] != "ok" {
		t.Errorf("expected check 'test' to be 'ok', got %q", resp.Checks["test"])
	}
}

func TestServer_Livez_WithUnhealthyCheck(t *testing.T) {
	logger := slog.Default()
	s := NewServer(Config{Addr: ":0"}, logger)

	s.RegisterCheck("failing", func(_ context.Context) error {
		return errors.New("connection refused")
	})

	req := httptest.NewRequest(http.MethodGet, "/livez", nil)
	w := httptest.NewRecorder()

	s.handleLivez(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status %d, got %d", http.StatusServiceUnavailable, w.Code)
	}

	var resp Response
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp.Status != "unhealthy" {
		t.Errorf("expected status 'unhealthy', got %q", resp.Status)
	}
}

func TestServer_Readyz_NotReady(t *testing.T) {
	logger := slog.Default()
	s := NewServer(Config{Addr: ":0"}, logger)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()

	s.handleReadyz(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status %d, got %d", http.StatusServiceUnavailable, w.Code)
	}

	var resp Response
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp.Status != "not ready" {
		t.Errorf("expected status 'not ready', got %q", resp.Status)
	}
}

func TestServer_Readyz_Ready(t *testing.T) {
	logger := slog.Default()
	s := NewServer(Config{Addr: ":0"}, logger)
	s.SetReady(true)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()

	s.handleReadyz(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}
}

func TestServer_Readyz_ReadyButUnhealthy(t *testing.T) {
	logger := slog.Default()
	s := NewServer(Config{Addr: ":0"}, logger)
	s.SetReady(true)

	s.RegisterCheck("failing", func(_ context.Context) error {
		return errors.New("database down")
	})

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()

	s.handleReadyz(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status %d, got %d", http.StatusServiceUnavailable, w.Code)
	}

	var resp Response
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp.Status != "not ready" {
		t.Errorf("expected status 'not ready', got %q", resp.Status)
	}
}

func TestQBHealthCheck(t *testing.T) {
	t.Run("healthy", func(t *testing.T) {
		check := QBHealthCheck(func(_ context.Context) error {
			return nil
		})

		if err := check(context.Background()); err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
	})

	t.Run("unhealthy", func(t *testing.T) {
		check := QBHealthCheck(func(_ context.Context) error {
			return errors.New("auth failed")
		})

		err := check(context.Background())
		if err == nil {
			t.Error("expected error, got nil")
		}
	})
}

func TestGRPCHealthCheck(t *testing.T) {
	t.Run("healthy", func(t *testing.T) {
		check := GRPCHealthCheck(func(_ context.Context) error {
			return nil
		})

		if err := check(context.Background()); err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
	})

	t.Run("unhealthy", func(t *testing.T) {
		check := GRPCHealthCheck(func(_ context.Context) error {
			return errors.New("connection refused")
		})

		err := check(context.Background())
		if err == nil {
			t.Error("expected error, got nil")
		}
	})
}
