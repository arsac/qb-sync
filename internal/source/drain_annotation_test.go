package source

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// The drain gate is the one place qb-sync talks to Kubernetes, and every error
// path in it skips the drain. These tests drive a real *kubernetes.Clientset
// against a stub API server rather than a fake clientset: the fake short
// circuits transport and decoding, which is precisely the client-go and
// OpenAPI machinery a dependency bump moves underneath this call.

// podAPIServer serves one Pod object at the URL client-go requests for
// namespace/name, so the request path and the response decode are both real.
func podAPIServer(t *testing.T, status int, body string) kubernetes.Interface {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if want := "/api/v1/namespaces/test-ns/pods/test-pod"; r.URL.Path != want {
			t.Errorf("unexpected request path %q, want %q", r.URL.Path, want)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)

	clientset, err := kubernetes.NewForConfig(&rest.Config{Host: srv.URL})
	if err != nil {
		t.Fatalf("building clientset: %v", err)
	}
	return clientset
}

func podJSON(annotations string) string {
	return `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"test-pod",` +
		`"namespace":"test-ns"` + annotations + `}}`
}

func TestPodAnnotationIsTrue(t *testing.T) {
	tests := []struct {
		name        string
		annotations string
		want        bool
	}{
		{
			name:        "annotation true allows the drain",
			annotations: `,"annotations":{"qbsync/drain":"true"}`,
			want:        true,
		},
		{
			name:        "annotation false blocks the drain",
			annotations: `,"annotations":{"qbsync/drain":"false"}`,
			want:        false,
		},
		{
			// Only the exact string "true" opens the gate, so a pod annotated
			// with something truthy-looking still does not drain.
			name:        "annotation set to another value blocks the drain",
			annotations: `,"annotations":{"qbsync/drain":"yes"}`,
			want:        false,
		},
		{
			name:        "annotation absent blocks the drain",
			annotations: `,"annotations":{"other":"true"}`,
			want:        false,
		},
		{
			name:        "pod carries no annotations at all",
			annotations: ``,
			want:        false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("POD_NAME", "test-pod")
			t.Setenv("POD_NAMESPACE", "test-ns")

			client := podAPIServer(t, http.StatusOK, podJSON(tc.annotations))
			got, err := podAnnotationIsTrue(context.Background(), client, "qbsync/drain")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("podAnnotationIsTrue = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestPodAnnotationIsTrueReportsFailures covers the paths that skip the drain.
// They must stay distinguishable in the shutdown log, because a deployment
// missing its downward-API env vars and an unreachable API server call for
// different fixes.
func TestPodAnnotationIsTrueReportsFailures(t *testing.T) {
	t.Run("missing pod env vars are reported before any request", func(t *testing.T) {
		t.Setenv("POD_NAME", "")
		t.Setenv("POD_NAMESPACE", "test-ns")

		// A nil client is safe here only because the env check must come first.
		// If that ordering ever changes this panics rather than passing quietly.
		_, err := podAnnotationIsTrue(context.Background(), nil, "qbsync/drain")
		if err == nil {
			t.Fatal("expected an error when POD_NAME is unset")
		}
	})

	t.Run("api failure is surfaced", func(t *testing.T) {
		t.Setenv("POD_NAME", "test-pod")
		t.Setenv("POD_NAMESPACE", "test-ns")

		client := podAPIServer(t, http.StatusInternalServerError, `{"message":"boom"}`)
		if _, err := podAnnotationIsTrue(context.Background(), client, "qbsync/drain"); err == nil {
			t.Fatal("expected an error when the API server fails")
		}
	})

	t.Run("undecodable response is surfaced", func(t *testing.T) {
		t.Setenv("POD_NAME", "test-pod")
		t.Setenv("POD_NAMESPACE", "test-ns")

		// Guards the decode path itself, which is what moves when the
		// Kubernetes and OpenAPI libraries are upgraded.
		client := podAPIServer(t, http.StatusOK, `{"apiVersion":"v1","kind":"Pod",`)
		if _, err := podAnnotationIsTrue(context.Background(), client, "qbsync/drain"); err == nil {
			t.Fatal("expected an error when the response cannot be decoded")
		}
	})
}
