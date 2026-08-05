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

	// historyPageSize overrides the endpoint's default of 10. Sonarr records one
	// history row per episode, so a season pack alone can exceed that.
	historyPageSize = 100
)

// Client is a thin HTTP client for Sonarr/Radarr. It is unaware of which app
// it is talking to - both expose the same v3 endpoints we use.
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
		return &Error{Kind: KindRateLimited, Cause: errors.New(resp.Status)}
	case resp.StatusCode >= http.StatusInternalServerError:
		return &Error{Kind: KindHTTP5xx, Cause: errors.New(resp.Status)}
	default:
		return &Error{Kind: KindNetwork, Cause: errors.New(resp.Status)}
	}
}

// GetHistoryByDownloadID calls GET /api/v3/history?downloadId={hash}.
//
// The hash is sent uppercase to match how *arr stores it, and the response is
// filtered again locally so a record for another torrent can never be read as
// this one's. Returns no records on 200 with an empty list, and *Error on failure.
func (c *Client) GetHistoryByDownloadID(ctx context.Context, hash string) ([]HistoryRecord, error) {
	ctx, cancel := context.WithTimeout(ctx, c.perCallTimeout)
	defer cancel()

	q := url.Values{}
	// Uppercase is required, not cosmetic. Every *arr download client stores
	// DownloadId as torrent.Hash.ToUpper(), and the history filter is an exact
	// SQL equality against a column declared TEXT with no COLLATE NOCASE. A
	// lowercase hash therefore matches nothing, and since no records reads as
	// "not rejected", the whole filter fails open on every torrent while
	// appearing perfectly healthy. An earlier implementation shipped with
	// exactly that bug, and a unit test that asserted it was correct.
	//
	// Verified against Sonarr, not inferred. To re-check after an *arr upgrade:
	// run a container with SONARR__AUTH__APIKEY preset, INSERT a History row
	// with an uppercase DownloadId straight into sonarr.db, restart it, then
	// query /api/v3/history with both cases. The uppercase query must select the
	// row and the lowercase one must return the same empty result as a hash that
	// does not exist. Assert the uppercase match: without it, both queries
	// return zero and the check proves nothing.
	q.Set("downloadId", strings.ToUpper(hash))
	// The page size defaults to 10, confirmed against a live instance, and
	// Sonarr writes one history row per episode, so a season pack can push the
	// terminal event off the first page.
	q.Set("pageSize", strconv.Itoa(historyPageSize))
	q.Set("sortKey", "date")
	q.Set("sortDirection", "descending")

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
	return recordsForHash(hr.Records, hash), nil
}

// recordsForHash keeps only the records belonging to hash.
//
// The server already filters by downloadId, so this normally removes nothing.
// It matters when the filter does not apply - an *arr version that stops
// honouring the parameter would return unfiltered history, and a downloadFailed
// belonging to some other torrent would then read as this one's rejection and
// skip a torrent the user wanted. Dropping unrelated records fails open, which
// is the direction this filter should err in.
//
// Compared case-insensitively: *arr stores the hash uppercase for torrents, but
// downloadId is whatever the download client uses, so the case is not ours to
// assume for every client.
func recordsForHash(records []HistoryRecord, hash string) []HistoryRecord {
	kept := make([]HistoryRecord, 0, len(records))
	for _, r := range records {
		if strings.EqualFold(r.DownloadID, hash) {
			kept = append(kept, r)
		}
	}
	return kept
}
