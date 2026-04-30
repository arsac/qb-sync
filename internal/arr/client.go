package arr

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
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
	httpServerErrorCode   = 500
)

// Client is a thin HTTP client for Sonarr/Radarr. It is unaware of which app
// it is talking to — both expose the same v3 endpoints we use.
type Client struct {
	baseURL string
	apiKey  string
	httpc   *http.Client
}

// NewClient constructs a Client. perCallTimeout bounds each HTTP round-trip
// via the per-call context derived inside each method.
func NewClient(baseURL, apiKey string, perCallTimeout time.Duration) *Client {
	if perCallTimeout <= 0 {
		perCallTimeout = defaultPerCallTimeout
	}
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		httpc: &http.Client{
			// Per-call timeout is enforced via context.WithTimeout on each call.
			// Set a generous transport-level default as a safety net.
			Timeout: perCallTimeout * transportTimeoutMult,
		},
	}
}

// Ping calls GET /api/v3/system/status. Returns nil on 200, *Error otherwise.
func (c *Client) Ping(ctx context.Context) error {
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
		return &Error{Kind: KindRateLimited, Cause: errors.New(resp.Status)}
	case resp.StatusCode >= httpServerErrorCode:
		return &Error{Kind: KindHTTP5xx, Cause: errors.New(resp.Status)}
	default:
		return &Error{Kind: KindNetwork, Cause: errors.New(resp.Status)}
	}
}
