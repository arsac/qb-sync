package health

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestServer constructs a server with default test config and a default logger.
func newTestServer() *Server {
	return NewServer(Config{Addr: ":0"}, slog.Default())
}

// callHandler invokes a handler with a fresh recorder and unmarshals the JSON body.
func callHandler(t *testing.T, h http.HandlerFunc, path string) (*httptest.ResponseRecorder, Response) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	w := httptest.NewRecorder()
	h(w, req)
	var resp Response
	if w.Body.Len() > 0 {
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	}
	return w, resp
}

func TestServer_Healthz(t *testing.T) {
	s := newTestServer()
	w, resp := callHandler(t, s.handleHealthz, "/healthz")
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "ok", resp.Status)
}

func TestServer_Livez_NoChecks(t *testing.T) {
	s := newTestServer()
	w, _ := callHandler(t, s.handleLivez, "/livez")
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestServer_Livez_WithHealthyCheck(t *testing.T) {
	s := newTestServer()
	s.RegisterCheck("test", func(_ context.Context) error { return nil })

	w, resp := callHandler(t, s.handleLivez, "/livez")
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "ok", resp.Checks["test"])
}

func TestServer_Livez_WithUnhealthyCheck(t *testing.T) {
	s := newTestServer()
	s.RegisterCheck("failing", func(_ context.Context) error { return errors.New("connection refused") })

	w, resp := callHandler(t, s.handleLivez, "/livez")
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, "unhealthy", resp.Status)
}

func TestServer_Readyz_NotReady(t *testing.T) {
	s := newTestServer()
	w, resp := callHandler(t, s.handleReadyz, "/readyz")
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, "not ready", resp.Status)
}

func TestServer_Readyz_Ready(t *testing.T) {
	s := newTestServer()
	s.SetReady(true)
	w, _ := callHandler(t, s.handleReadyz, "/readyz")
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestServer_Readyz_ReadyButUnhealthy(t *testing.T) {
	s := newTestServer()
	s.SetReady(true)
	s.RegisterCheck("failing", func(_ context.Context) error { return errors.New("database down") })

	w, resp := callHandler(t, s.handleReadyz, "/readyz")
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, "not ready", resp.Status)
}

func TestQBHealthCheck(t *testing.T) {
	t.Run("healthy", func(t *testing.T) {
		check := QBHealthCheck(func(_ context.Context) error { return nil })
		assert.NoError(t, check(context.Background()))
	})

	t.Run("unhealthy", func(t *testing.T) {
		check := QBHealthCheck(func(_ context.Context) error { return errors.New("auth failed") })
		assert.Error(t, check(context.Background()))
	})
}

func TestGRPCHealthCheck(t *testing.T) {
	t.Run("healthy", func(t *testing.T) {
		check := GRPCHealthCheck(func(_ context.Context) error { return nil })
		assert.NoError(t, check(context.Background()))
	})

	t.Run("unhealthy", func(t *testing.T) {
		check := GRPCHealthCheck(func(_ context.Context) error { return errors.New("connection refused") })
		assert.Error(t, check(context.Background()))
	})
}
