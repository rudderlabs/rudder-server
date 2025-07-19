package suppression

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/cenkalti/backoff"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

// SyncerOpt represents a configuration option for the syncer
type SyncerOpt func(*Syncer)

// WithHttpClient sets the http client to use
func WithHttpClient(client *http.Client) SyncerOpt {
	return func(c *Syncer) {
		c.client = client
	}
}

// WithPageSize sets the page size for each sync request
func WithPageSize(pageSize int) SyncerOpt {
	return func(c *Syncer) {
		c.pageSize = pageSize
	}
}

// WithPollIntervalFn sets the interval at which the syncer will poll the backend
func WithPollIntervalFn(pollIntervalFn func() time.Duration) SyncerOpt {
	return func(c *Syncer) {
		c.pollIntervalFn = pollIntervalFn
	}
}

// WithLogger sets the logger to use in the syncer
func WithLogger(log logger.Logger) SyncerOpt {
	return func(c *Syncer) {
		c.log = log
	}
}

// MustNewSyncer creates a new syncer, panics if an error occurs
func MustNewSyncer(baseURL string, identifier identity.Identifier, r Repository, opts ...SyncerOpt) *Syncer {
	s, err := NewSyncer(baseURL, identifier, r, opts...)
	if err != nil {
		panic(err)
	}
	return s
}

// NewSyncer creates a new syncer
func NewSyncer(baseURL string, identifier identity.Identifier, r Repository, opts ...SyncerOpt) (*Syncer, error) {
	var url string
	switch identifier.Type() {
	case deployment.DedicatedType:
		url = fmt.Sprintf("%s/dataplane/workspaces/%s/regulations/suppressions", baseURL, identifier.ID())
	case deployment.MultiTenantType:
		url = fmt.Sprintf("%s/dataplane/namespaces/%s/regulations/suppressions", baseURL, identifier.ID())
	default:
		return nil, fmt.Errorf("unsupported deployment type: %s", identifier.Type())
	}

	s := &Syncer{
		url:                url,
		id:                 identifier,
		r:                  r,
		log:                logger.NOP,
		client:             &http.Client{},
		pageSize:           100,
		pollIntervalFn:     func() time.Duration { return 30 * time.Second },
		defaultWorkspaceID: identifier.ID(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// Syncer is responsible for syncing suppressions from the backend to the repository
type Syncer struct {
	url string
	id  identity.Identifier
	r   Repository

	client             *http.Client
	log                logger.Logger
	pageSize           int
	pollIntervalFn     func() time.Duration
	defaultWorkspaceID string
}

// SyncLoop runs the sync loop until the provided context is done
func (s *Syncer) SyncLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.pollIntervalFn()):
		}
		err := s.Sync(ctx)
		if err != nil {
			s.log.Errorn("Failed to sync suppressions", obskit.Error(err))
		}
	}
}

// Sync synchronises suppressions from the data regulation service in batches, until
// it completes, or an error occurs. Synchronisation completes when the service responds
// with suppressions whose number is less than the page size.
func (s *Syncer) Sync(ctx context.Context) error {
again:
	token, err := s.r.GetToken()
	if err != nil {
		if errors.Is(err, model.ErrRestoring) {
			if err := misc.SleepCtx(ctx, 1*time.Second); err != nil {
				return err
			}
			goto again
		}
		s.log.Errorn("Failed to get token from repository", obskit.Error(err))
		return err
	}

	suppressions, nextToken, err := s.sync(token)
	if err != nil {
		return fmt.Errorf("sync failed: %w", err)
	}
	err = s.r.Add(suppressions, nextToken)
	if err != nil {
		s.log.Errorn("Failed to add suppressions to repository", logger.NewIntField("suppressions", int64(len(suppressions))), obskit.Error(err))
		return err
	}
	if len(suppressions) >= s.pageSize {
		goto again
	}
	return nil
}

// sync fetches suppressions from the backend
func (s *Syncer) sync(token []byte) ([]model.Suppression, []byte, error) {
	urlStr := s.url
	urlValQuery := url.Values{}
	if s.pageSize > 0 {
		urlValQuery.Set("pageSize", strconv.Itoa(s.pageSize))
	}
	if len(token) > 0 {
		urlValQuery.Set("pageToken", string(token))
	}
	if len(urlValQuery) > 0 {
		urlStr += "?" + urlValQuery.Encode()
	}

	var resp *http.Response
	var respBody []byte

	operation := func() error {
		var err error
		req, err := http.NewRequest("GET", urlStr, http.NoBody)
		s.log.Debugn("regulation service URL", logger.NewStringField("url", urlStr))
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}
		req.SetBasicAuth(s.id.BasicAuth())
		req.Header.Set("Content-Type", "application/json")

		resp, err = s.client.Do(req)
		if err != nil {
			return fmt.Errorf("failed to make request: %w", err)
		}
		defer func() { httputil.CloseResponse(resp) }()

		// If statusCode is not 2xx, then returning empty regulations
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("failed to fetch source regulations: statusCode: %d", resp.StatusCode)
		}
		respBody, err = io.ReadAll(resp.Body)
		if err != nil {
			s.log.Errorn("failed to read response body", obskit.Error(err))
			return fmt.Errorf("failed to read response body: %w", err)
		}
		return err
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)
	err := backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		s.log.Errorn("Failed to fetch source regulations from API, retrying", obskit.Error(err), logger.NewStringField("retryAfter", t.String()))
	})
	if err != nil {
		s.log.Errorn("Error sending request to the server", obskit.Error(err))
		return []model.Suppression{}, nil, err
	}
	if respBody == nil {
		s.log.Errorn("nil response body, returning")
		return []model.Suppression{}, nil, errors.New("nil response body")
	}
	var respJSON suppressionsResponse
	err = jsonrs.Unmarshal(respBody, &respJSON)
	if err != nil {
		s.log.Errorn("Error while parsing response", obskit.Error(err), logger.NewIntField("statusCode", int64(resp.StatusCode)))
		return []model.Suppression{}, nil, err
	}

	if respJSON.Token == "" {
		s.log.Errorn("No token found in the source regulations response", logger.NewStringField("responseBody", string(respBody)))
		return respJSON.Items, nil, fmt.Errorf("no token returned in regulation API response")
	}
	return respJSON.Items, []byte(respJSON.Token), nil
}

type suppressionsResponse struct {
	Items []model.Suppression `json:"items"`
	Token string              `json:"token"`
}
