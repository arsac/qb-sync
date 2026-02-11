package utils

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsRetriableError_NetworkErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"connection refused", errors.New("dial tcp 127.0.0.1:8080: connection refused")},
		{"connection reset", errors.New("read: connection reset by peer")},
		{"connection timed out", errors.New("dial tcp: connection timed out")},
		{"no such host", errors.New("dial tcp: lookup qbit.local: no such host")},
		{"network unreachable", errors.New("dial tcp: network is unreachable")},
		{"i/o timeout", errors.New("read tcp 127.0.0.1:8080: i/o timeout")},
		{"eof", errors.New("unexpected EOF")},
		{"broken pipe", errors.New("write: broken pipe")},
		{"temporary failure", errors.New("lookup qbit: temporary failure in name resolution")},
		{"dns error", errors.New("dial tcp: dns resolution failed")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.True(t, IsRetriableError(tt.err))
		})
	}
}

func TestIsRetriableError_HTTPErrors(t *testing.T) {
	retriable := []struct {
		name string
		err  error
	}{
		{"502 bad gateway", errors.New("unexpected status: 502")},
		{"503 service unavailable", errors.New("status 503")},
		{"504 gateway timeout", errors.New("status: 504")},
		{"429 too many requests", errors.New("status 429")},
	}

	for _, tt := range retriable {
		t.Run(tt.name, func(t *testing.T) {
			assert.True(t, IsRetriableError(tt.err))
		})
	}
}

func TestIsRetriableError_NonRetriable(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"nil error", nil},
		{"404 not found", errors.New("status: 404")},
		{"not found text", errors.New("torrent not found")},
		{"401 unauthorized", errors.New("status: 401")},
		{"403 forbidden", errors.New("status: 403")},
		{"unauthorized text", errors.New("unauthorized access")},
		{"forbidden text", errors.New("forbidden")},
		{"context canceled", context.Canceled},
		{"context deadline", context.DeadlineExceeded},
		{"wrapped context canceled", fmt.Errorf("operation failed: %w", context.Canceled)},
		{"generic error", errors.New("some random error")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.False(t, IsRetriableError(tt.err))
		})
	}
}

func TestIsCircuitBreakerFailure_Excluded(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"nil error", nil},
		{"404 status", errors.New("status: 404")},
		{"not found text", errors.New("torrent not found")},
		{"context canceled", context.Canceled},
		{"context deadline", context.DeadlineExceeded},
		{"wrapped context canceled", fmt.Errorf("op: %w", context.Canceled)},
		// Uppercase 'N' is intentional: verifies case-insensitive match (strings.ToLower in production code)
		{
			"qbt unmarshal quirk",
			errors.New("could not unmarshal body: invalid character 'N' looking for beginning of value"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.False(t, IsCircuitBreakerFailure(tt.err))
		})
	}
}

func TestIsCircuitBreakerFailure_Included(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"connection refused", errors.New("dial tcp: connection refused")},
		{"500 internal server error", errors.New("status: 500")},
		{"502 bad gateway", errors.New("status: 502")},
		{"generic error", errors.New("something went wrong")},
		{"i/o timeout", errors.New("i/o timeout")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.True(t, IsCircuitBreakerFailure(tt.err))
		})
	}
}
