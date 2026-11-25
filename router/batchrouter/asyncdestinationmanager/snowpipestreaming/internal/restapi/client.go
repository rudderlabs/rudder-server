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

	"github.com/cenkalti/backoff/v5"
	"github.com/google/uuid"

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
func WithHTTPClient(doer requestDoer) Opt {
	return func(c *Client) {
		c.doer = doer
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
	doer         requestDoer
	accountURL   string // e.g. https://xy12345.us-east-1.snowflakecomputing.com
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
	if c.doer == nil {
		c.doer = &http.Client{Timeout: time.Minute}
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
		doer:        c.doer,
		now:         c.now,
	}
	return c, nil
}

// OpenChannel opens a channel, creating it if it doesn't exist. Reopening an open channel discards its uncommitted rows
// and invalidates every continuation token issued before. No offset token is ever passed: Snowflake keeps the
// channel's committed offset, and passing one would overwrite it.
func (c *Client) OpenChannel(ctx context.Context, ref ChannelRef) (*OpenChannelResponse, error) {
	path := "/v2/streaming/databases/" + url.PathEscape(ref.Database) +
		"/schemas/" + url.PathEscape(ref.Schema) +
		"/pipes/" + url.PathEscape(ref.Pipe) +
		"/channels/" + url.PathEscape(ref.Channel)
	body, err := c.call(ctx, http.MethodPut, path, nil, []byte("{}"), contentTypeJSON, "")
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
		return nil, &APIError{StatusCode: http.StatusRequestEntityTooLarge, Code: ErrCodeRequestBodyTooLarge, Message: fmt.Sprintf("body of %d bytes exceeds %d bytes", len(req.Body), maxRequestBodyBytes)}
	}
	path := "/v2/streaming/data/databases/" + url.PathEscape(ref.Database) +
		"/schemas/" + url.PathEscape(ref.Schema) +
		"/pipes/" + url.PathEscape(ref.Pipe) +
		"/channels/" + url.PathEscape(ref.Channel) + "/rows"
	query := url.Values{}
	query.Set("continuationToken", req.ContinuationToken)
	if req.StartOffsetToken != "" {
		query.Set("startOffsetToken", req.StartOffsetToken)
	}
	if req.EndOffsetToken != "" {
		query.Set("endOffsetToken", req.EndOffsetToken)
	}
	body, err := c.call(ctx, http.MethodPost, path, query, req.Body, contentTypeNDJSON, "zstd")
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

// DropChannel drops a channel, discarding its uncommitted rows and its committed offset.
// The next open starts the channel afresh, with no committed offset.
func (c *Client) DropChannel(ctx context.Context, ref ChannelRef) error {
	path := "/v2/streaming/databases/" + url.PathEscape(ref.Database) +
		"/schemas/" + url.PathEscape(ref.Schema) +
		"/pipes/" + url.PathEscape(ref.Pipe) +
		"/channels/" + url.PathEscape(ref.Channel)
	if _, err := c.call(ctx, http.MethodDelete, path, nil, nil, contentTypeJSON, ""); err != nil {
		return fmt.Errorf("dropping channel %s on pipe %s: %w", ref.Channel, ref.Pipe, err)
	}
	return nil
}

// BulkChannelStatus returns the status of the given channels of a pipe. Channels that don't exist are absent from the result.
func (c *Client) BulkChannelStatus(ctx context.Context, pipe PipeRef, channelNames []string) (map[string]ChannelStatus, error) {
	path := "/v2/streaming/databases/" + url.PathEscape(pipe.Database) +
		"/schemas/" + url.PathEscape(pipe.Schema) +
		"/pipes/" + url.PathEscape(pipe.Pipe) + ":bulk-channel-status"
	reqBody, err := jsonrs.Marshal(bulkChannelStatusRequest{ChannelNames: channelNames})
	if err != nil {
		return nil, fmt.Errorf("marshalling bulk channel status request: %w", err)
	}
	body, err := c.call(ctx, http.MethodPost, path, nil, reqBody, contentTypeJSON, "")
	if err != nil {
		return nil, fmt.Errorf("getting bulk channel status on pipe %s: %w", pipe.Pipe, err)
	}
	var res bulkChannelStatusResponse
	if err := jsonrs.Unmarshal(body, &res); err != nil {
		return nil, fmt.Errorf("decoding bulk channel status response: %w", err)
	}
	return res.ChannelStatuses, nil
}

// call sends a request to the ingest host and returns the body of a 200 response.
// The same body and request id are sent on every attempt. A 401 refreshes the token and is retried once,
// without counting towards the retry budget.
func (c *Client) call(ctx context.Context, method, path string, query url.Values, body []byte, contentType, contentEncoding string) ([]byte, error) {
	if query == nil {
		query = url.Values{}
	}
	query.Set("requestId", uuid.NewString())

	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = c.minWait
	bo.MaxInterval = c.maxWait

	var reauthenticated bool
	for attempt := 0; ; attempt++ {
		resBody, err := c.attempt(ctx, method, path, query, body, contentType, contentEncoding)
		if err == nil {
			return resBody, nil
		}

		apiErr, isAPIErr := errors.AsType[*APIError](err)
		if isAPIErr && apiErr.StatusCode == http.StatusUnauthorized && !reauthenticated {
			reauthenticated = true
			c.tokens.Invalidate()
			attempt-- // a token refresh doesn't count as a retry
			continue
		}
		if isAPIErr && !apiErr.retryable() {
			return nil, err
		}
		if attempt >= c.maxRetries {
			return nil, fmt.Errorf("giving up after %d attempts: %w", attempt+1, err)
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%w: %w", ctx.Err(), err)
		case <-time.After(bo.NextBackOff()):
		}
	}
}

func (c *Client) attempt(ctx context.Context, method, path string, query url.Values, body []byte, contentType, contentEncoding string) ([]byte, error) {
	host, err := c.host(ctx)
	if err != nil {
		return nil, err
	}
	token, err := c.tokens.Token(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("getting token: %w", err)
	}

	reqURL := c.ingestScheme + "://" + host + path + "?" + query.Encode()
	req, err := http.NewRequestWithContext(ctx, method, reqURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set(headerSnowflakeAuthorizationToken, "OAUTH")
	req.Header.Set("Accept", contentTypeJSON) // without it, Snowflake answers 400
	req.Header.Set("Content-Type", contentType)
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}

	resp, err := c.doer.Do(req)
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

	resp, err := c.doer.Do(req)
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
