// Package arr integrates with Sonarr and Radarr to filter torrents
// that *arr has explicitly rejected before they are synced.
package arr

import (
	"errors"
	"fmt"
	"time"
)

// Reason describes why a Decision was reached. Used as a metric label and in logs.
type Reason string

const (
	ReasonNoCategory     Reason = "no_category"      // category not mapped — SYNC
	ReasonNotRejected    Reason = "not_rejected"     // history shows no rejection — SYNC
	ReasonEmptyHistory   Reason = "empty_history"    // no record — SYNC
	ReasonIgnored        Reason = "download_ignored" // *arr explicitly rejected import — SKIP
	ReasonFailed         Reason = "download_failed"  // *arr's grab/import failed terminally — SKIP
	ReasonLookupFailed   Reason = "lookup_failed"    // error — SYNC fail-open
	ReasonCircuitOpen    Reason = "circuit_open"     // breaker open — SYNC fail-open
	ReasonBudgetExceeded Reason = "budget_exceeded"  // per-cycle budget hit — SYNC fail-open
)

// Decision is the verdict returned by the Filter for a single torrent.
type Decision struct {
	Sync   bool
	Reason Reason
}

// HistoryRecord mirrors the fields we care about from /api/v3/history.
// *arr returns more fields; only these are decoded.
type HistoryRecord struct {
	EventType  string    `json:"eventType"`
	DownloadID string    `json:"downloadId"`
	Date       time.Time `json:"date"`
}

// Kind classifies a lookup error for metric labels.
type Kind string

const (
	KindTimeout      Kind = "timeout"
	KindHTTP5xx      Kind = "http_5xx"
	KindUnauthorized Kind = "unauthorized"
	KindNetwork      Kind = "network"
	KindRateLimited  Kind = "rate_limited"
)

// Error wraps an HTTP/transport error with a Kind classification for metrics.
type Error struct {
	Kind  Kind
	Cause error
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("arr: %s: %v", e.Kind, e.Cause)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// ErrCircuitOpen is returned when the breaker for a given instance is open.
// Mirrors the sentinel pattern from circuitbreaker.ErrOpen.
var ErrCircuitOpen = errors.New("arr: circuit open")
