package arr

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	apiV3Prefix           = "/api/v3"
	headerKey             = "X-Api-Key"
	userAgent             = "qbsync/arr"
	defaultPerCallTimeout = 3 * time.Second
	transportTimeoutMult  = 2
	maxResponseBodySize   = 1 << 20
)

// Client is a thin HTTP client for Sonarr/Radarr. It is unaware of which app
// it is talking to — both expose the same v3 endpoints we use.
type Client struct {
	baseURL        string
	apiKey         string
	httpc          *http.Client
	perCallTimeout time.Duration
}

// NewClient constructs a Client. perCallTimeout bounds each HTTP round-trip
// via [context.WithTimeout] derived inside each method. The transport timeout
// is set to perCallTimeout*transportTimeoutMult as a safety net only.
func NewClient(baseURL, apiKey string, perCallTimeout time.Duration) *Client {
	if perCallTimeout <= 0 {
		perCallTimeout = defaultPerCallTimeout
	}
	return &Client{
		baseURL:        strings.TrimRight(baseURL, "/"),
		apiKey:         apiKey,
		perCallTimeout: perCallTimeout,
		httpc: &http.Client{
			Timeout: perCallTimeout * transportTimeoutMult,
		},
	}
}

// Ping calls GET /api/v3/system/status. Returns nil on 200, *Error otherwise.
func (c *Client) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, c.perCallTimeout)
	defer cancel()

	req, err := c.newRequest(ctx, http.MethodGet, apiV3Prefix+"/system/status", nil)
	if err != nil {
		return err
	}
	resp, doErr := c.httpc.Do(req)
	if doErr != nil {
		return classifyDoError(doErr)
	}
	defer func() { drainAndClose(resp.Body) }()

	if resp.StatusCode == http.StatusOK {
		return nil
	}
	return classifyStatusError(resp)
}

// newRequest builds an [http.Request] with the X-Api-Key header set and a
// per-call timeout context derived from ctx. The API key is never placed
// in the URL.
func (c *Client) newRequest(ctx context.Context, method, path string, query url.Values) (*http.Request, error) {
	u := c.baseURL + path
	if len(query) > 0 {
		u = u + "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, nil)
	if err != nil {
		return nil, &Error{Kind: KindNetwork, Cause: err}
	}
	req.Header.Set(headerKey, c.apiKey)
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "application/json")
	return req, nil
}

// drainAndClose drains and closes a response body, allowing connection reuse.
func drainAndClose(body io.ReadCloser) {
	if body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(body, maxResponseBodySize))
	_ = body.Close()
}

// classifyDoError maps a transport-layer error to *Error{Kind}.
func classifyDoError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return &Error{Kind: KindTimeout, Cause: err}
	}
	return &Error{Kind: KindNetwork, Cause: err}
}

// classifyStatusError maps an HTTP non-2xx response to *Error{Kind}.
func classifyStatusError(resp *http.Response) error {
	switch {
	case resp.StatusCode == http.StatusUnauthorized:
		return &Error{Kind: KindUnauthorized, Cause: errors.New(resp.Status)}
	case resp.StatusCode == http.StatusTooManyRequests:
		return &Error{
			Kind:       KindRateLimited,
			Cause:      errors.New(resp.Status),
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
		}
	case resp.StatusCode >= http.StatusInternalServerError:
		return &Error{Kind: KindHTTP5xx, Cause: errors.New(resp.Status)}
	default:
		return &Error{Kind: KindNetwork, Cause: errors.New(resp.Status)}
	}
}

// parseRetryAfter accepts the integer-seconds form of Retry-After. The HTTP
// date form is rare in *arr responses; return 0 (caller will use a default backoff).
func parseRetryAfter(v string) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	secs, err := strconv.Atoi(v)
	if err != nil || secs < 0 {
		return 0
	}
	return time.Duration(secs) * time.Second
}

// GetHistoryByDownloadID calls GET /api/v3/history?downloadId={hash}.
// hash is normalized to lowercase before sending. Records returned by *arr
// may have uppercase DownloadID values; callers should compare case-insensitively.
// Returns nil records on 200 with empty body or empty list. Returns *Error on failure.
func (c *Client) GetHistoryByDownloadID(ctx context.Context, hash string) ([]HistoryRecord, error) {
	ctx, cancel := context.WithTimeout(ctx, c.perCallTimeout)
	defer cancel()

	q := url.Values{}
	q.Set("downloadId", strings.ToLower(hash))

	req, err := c.newRequest(ctx, http.MethodGet, apiV3Prefix+"/history", q)
	if err != nil {
		return nil, err
	}
	resp, doErr := c.httpc.Do(req)
	if doErr != nil {
		return nil, classifyDoError(doErr)
	}
	defer func() { drainAndClose(resp.Body) }()

	if resp.StatusCode != http.StatusOK {
		return nil, classifyStatusError(resp)
	}

	var hr historyResponse
	if decodeErr := json.NewDecoder(resp.Body).Decode(&hr); decodeErr != nil {
		return nil, &Error{Kind: KindNetwork, Cause: decodeErr}
	}
	return hr.Records, nil
}
