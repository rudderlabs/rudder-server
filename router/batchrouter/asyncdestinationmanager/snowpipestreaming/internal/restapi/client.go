// Package restapi is a client for Snowflake's Snowpipe Streaming high-performance (HP) REST API.
//
// The client takes care of what the HP Java SDK would otherwise hide: key-pair authentication and scoped
// token refresh, ingest host discovery, the undocumented request details (an Accept header and a UUID request id
// on every call), typed errors and retries. Continuation tokens and offsets are the caller's responsibility:
// there must be at most one append in flight per channel, always with the latest continuation token.
//
// See https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-rest-api
package restapi

import (
	"bytes"
	"context"
	"crypto/rsa"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-retryablehttp"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	"github.com/rudderlabs/rudder-server/utils/httputil"
)

type requestDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// Config holds the credentials of a Snowflake user with key-pair authentication.
type Config struct {
	// Account is the account identifier as configured on the destination, e.g. xy12345.us-east-1 or myorg-myaccount.
	Account string
	User    string
	// Role is the role to use. Empty means the user's default role.
	Role       string
	PrivateKey *rsa.PrivateKey
}

// Opt configures a [Client].
type Opt func(*Client)

// WithHTTPClient sets the HTTP client used for every call.
func WithHTTPClient(httpClient *http.Client) Opt {
	return func(c *Client) {
		c.httpClient = httpClient
	}
}

// WithAccountURL overrides the account URL, which defaults to https://<account>.snowflakecomputing.com.
func WithAccountURL(accountURL string) Opt {
	return func(c *Client) {
		c.accountURL = strings.TrimSuffix(accountURL, "/")
	}
}

// WithIngestScheme overrides the scheme used for the ingest host, which defaults to https.
func WithIngestScheme(scheme string) Opt {
	return func(c *Client) {
		c.ingestScheme = scheme
	}
}

// WithRetries sets how many times a retryable call is retried, and the bounds of the exponential backoff between attempts.
func WithRetries(maxRetries int, minWait, maxWait time.Duration) Opt {
	return func(c *Client) {
		c.maxRetries = maxRetries
		c.minWait = minWait
		c.maxWait = maxWait
	}
}

// WithClock sets the clock used for signing and token expiry.
func WithClock(now func() time.Time) Opt {
	return func(c *Client) {
		c.now = now
	}
}

// Client calls the Snowpipe Streaming HP REST API for one Snowflake user. It is safe for concurrent use.
type Client struct {
	httpClient   *http.Client
	ingest       *retryablehttp.Client // httpClient, with retries
	accountURL   string                // e.g. https://xy12345.us-east-1.snowflakecomputing.com
	ingestScheme string
	maxRetries   int
	minWait      time.Duration
	maxWait      time.Duration
	now          func() time.Time

	tokens *tokenSource

	hostMu     sync.Mutex
	ingestHost string // discovered lazily, then cached
}

// New creates a new client.
func New(conf Config, opts ...Opt) (*Client, error) {
	if conf.Account == "" || conf.User == "" || conf.PrivateKey == nil {
		return nil, errors.New("creating snowpipe streaming client: account, user and private key are required")
	}
	c := &Client{
		accountURL:   "https://" + conf.Account + ".snowflakecomputing.com",
		ingestScheme: "https",
		maxRetries:   5,
		minWait:      100 * time.Millisecond,
		maxWait:      10 * time.Second,
		now:          time.Now,
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.httpClient == nil {
		c.httpClient = &http.Client{Timeout: time.Minute}
	}
	c.ingest = &retryablehttp.Client{
		HTTPClient:   c.httpClient,
		RetryWaitMin: c.minWait,
		RetryWaitMax: c.maxWait,
		RetryMax:     c.maxRetries,
		CheckRetry:   c.checkRetry,
		Backoff:      retryablehttp.DefaultBackoff, // exponential, honouring Retry-After on 429 and 503
		PrepareRetry: c.authorize,                  // a retry after a 401 carries the refreshed token
		ErrorHandler: retryablehttp.PassthroughErrorHandler,
	}
	fingerprint, err := publicKeyFingerprint(conf.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("creating snowpipe streaming client: %w", err)
	}
	c.tokens = &tokenSource{
		account:     conf.Account,
		user:        conf.User,
		role:        conf.Role,
		key:         conf.PrivateKey,
		fingerprint: fingerprint,
		accountURL:  c.accountURL,
		doer:        c.httpClient,
		now:         c.now,
	}
	return c, nil
}

// OpenChannel opens a channel, creating it if it doesn't exist. Reopening an open channel discards its uncommitted rows
// and invalidates every continuation token issued before. No offset token is ever passed: Snowflake keeps the
// channel's committed offset, and passing one would overwrite it.
func (c *Client) OpenChannel(ctx context.Context, ref ChannelRef) (*OpenChannelResponse, error) {
	body, err := c.call(ctx, request{
		method:      http.MethodPut,
		path:        "/v2/streaming" + channelPath(ref),
		body:        []byte("{}"),
		contentType: contentTypeJSON,
	})
	if err != nil {
		return nil, fmt.Errorf("opening channel %s on pipe %s: %w", ref.Channel, ref.Pipe, err)
	}
	var res OpenChannelResponse
	if err := jsonrs.Unmarshal(body, &res); err != nil {
		return nil, fmt.Errorf("decoding open channel response: %w", err)
	}
	if res.NextContinuationToken == "" {
		return nil, errors.New("decoding open channel response: missing continuation token")
	}
	return &res, nil
}

// AppendRows appends one chunk of rows to a channel. Retrying the same chunk with the same continuation token
// is deduplicated by Snowflake, so retries are safe.
func (c *Client) AppendRows(ctx context.Context, ref ChannelRef, req AppendRowsRequest) (*AppendRowsResponse, error) {
	if len(req.Body) > maxRequestBodyBytes {
		return nil, &APIError{
			StatusCode: http.StatusRequestEntityTooLarge,
			Code:       CodeRequestBodyTooLarge,
			Message:    fmt.Sprintf("body of %d bytes exceeds %d bytes", len(req.Body), maxRequestBodyBytes),
		}
	}
	path := "/v2/streaming/data" + channelPath(ref) + "/rows"
	query := url.Values{}
	query.Set("continuationToken", req.ContinuationToken)
	if req.StartOffsetToken != "" {
		query.Set("startOffsetToken", req.StartOffsetToken)
	}
	if req.EndOffsetToken != "" {
		query.Set("endOffsetToken", req.EndOffsetToken)
	}
	body, err := c.call(ctx, request{
		method:          http.MethodPost,
		path:            path,
		query:           query,
		body:            req.Body,
		contentType:     contentTypeNDJSON,
		contentEncoding: "zstd",
	})
	if err != nil {
		return nil, fmt.Errorf("appending rows to channel %s on pipe %s: %w", ref.Channel, ref.Pipe, err)
	}
	var res AppendRowsResponse
	if err := jsonrs.Unmarshal(body, &res); err != nil {
		return nil, fmt.Errorf("decoding append rows response: %w", err)
	}
	if res.NextContinuationToken == "" {
		return nil, errors.New("decoding append rows response: missing continuation token")
	}
	return &res, nil
}

// BulkChannelStatus returns the status of the given channels of a pipe. Channels that don't exist are absent from the result.
func (c *Client) BulkChannelStatus(ctx context.Context, pipe PipeRef, channelNames []string) (map[string]ChannelStatus, error) {
	path := "/v2/streaming" + pipePath(pipe) + ":bulk-channel-status"
	reqBody, err := jsonrs.Marshal(bulkChannelStatusRequest{ChannelNames: channelNames})
	if err != nil {
		return nil, fmt.Errorf("marshalling bulk channel status request: %w", err)
	}
	body, err := c.call(ctx, request{method: http.MethodPost, path: path, body: reqBody, contentType: contentTypeJSON})
	if err != nil {
		return nil, fmt.Errorf("getting bulk channel status on pipe %s: %w", pipe.Pipe, err)
	}
	var res bulkChannelStatusResponse
	if err := jsonrs.Unmarshal(body, &res); err != nil {
		return nil, fmt.Errorf("decoding bulk channel status response: %w", err)
	}
	return res.ChannelStatuses, nil
}

// pipePath is the path of a pipe, below the API's root.
func pipePath(pipe PipeRef) string {
	return "/databases/" + url.PathEscape(pipe.Database) +
		"/schemas/" + url.PathEscape(pipe.Schema) +
		"/pipes/" + url.PathEscape(pipe.Pipe)
}

// channelPath is the path of a channel, below the API's root.
func channelPath(ref ChannelRef) string {
	return pipePath(ref.PipeRef) + "/channels/" + url.PathEscape(ref.Channel)
}

// request is a call to the ingest host.
type request struct {
	method          string
	path            string     // below the ingest host, e.g. /v2/streaming/databases/...
	query           url.Values // optional; a request id is always added
	body            []byte
	contentType     string
	contentEncoding string // optional
}

// call sends a request to the ingest host and returns the body of a 200 response. Retries send the same body and
// request id; Snowflake deduplicates an append replayed with the same continuation token.
func (c *Client) call(ctx context.Context, r request) ([]byte, error) {
	host, err := c.host(ctx)
	if err != nil {
		return nil, err
	}
	query := r.query
	if query == nil {
		query = url.Values{}
	}
	query.Set("requestId", uuid.NewString())

	req, err := retryablehttp.NewRequestWithContext(ctx, r.method, c.ingestScheme+"://"+host+r.path+"?"+query.Encode(), r.body)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set(headerSnowflakeAuthorizationToken, "OAUTH")
	req.Header.Set("Accept", contentTypeJSON) // without it, Snowflake answers 400
	req.Header.Set("Content-Type", r.contentType)
	if r.contentEncoding != "" {
		req.Header.Set("Content-Encoding", r.contentEncoding)
	}
	if err := c.authorize(req.Request); err != nil {
		return nil, err
	}

	resp, err := c.ingest.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}
	defer func() { httputil.CloseResponse(resp) }()

	resBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, newAPIError(resp.StatusCode, resBody)
	}
	return resBody, nil
}

// authorize sets the scoped token for the request's ingest host.
func (c *Client) authorize(req *http.Request) error {
	token, err := c.tokens.Token(req.Context(), req.URL.Host)
	if err != nil {
		return fmt.Errorf("getting token: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	return nil
}

// checkRetry is the retry policy: connection errors as go-retryablehttp's default policy decides, a 401 after
// refreshing the token, and the error responses [APIError.retryable] lists.
func (c *Client) checkRetry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if err != nil {
		return retryablehttp.DefaultRetryPolicy(ctx, resp, err)
	}
	switch resp.StatusCode {
	case http.StatusOK:
		return false, nil
	case http.StatusUnauthorized:
		c.tokens.Invalidate()
		return true, nil
	}
	return retryableResponse(resp), nil
}

// retryableResponse reports whether an error response is worth another attempt. The error code decides, so the body
// is read here and put back for the caller. A body that can't be read is retried, as a connection error would be.
func retryableResponse(resp *http.Response) bool {
	body, err := io.ReadAll(resp.Body)
	resp.Body = io.NopCloser(bytes.NewReader(body))
	return err != nil || newAPIError(resp.StatusCode, body).retryable()
}

// host returns the account's ingest host, discovering it on first use.
func (c *Client) host(ctx context.Context) (string, error) {
	c.hostMu.Lock()
	defer c.hostMu.Unlock()
	if c.ingestHost != "" {
		return c.ingestHost, nil
	}

	signed, err := c.tokens.keyPairJWT()
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.accountURL+"/v2/streaming/hostname", http.NoBody)
	if err != nil {
		return "", fmt.Errorf("creating hostname request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+signed)
	req.Header.Set(headerSnowflakeAuthorizationToken, "KEYPAIR_JWT")
	req.Header.Set("Accept", contentTypeJSON)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("sending hostname request: %w", err)
	}
	defer func() { httputil.CloseResponse(resp) }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading hostname response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("discovering ingest host: %w", newAPIError(resp.StatusCode, body))
	}
	host := strings.TrimSpace(string(body))
	var parsed struct {
		Hostname string `json:"hostname"`
	}
	if jsonrs.Unmarshal(body, &parsed) == nil && parsed.Hostname != "" {
		host = parsed.Hostname
	}
	if host == "" {
		return "", errors.New("discovering ingest host: empty hostname")
	}
	// Underscores aren't valid in host names; Snowflake documents replacing them with dashes.
	c.ingestHost = strings.ReplaceAll(host, "_", "-")
	return c.ingestHost, nil
}
