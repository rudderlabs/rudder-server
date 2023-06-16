package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"time"

	"github.com/cenkalti/backoff"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/utils/httputil"
)

var (
	defaultTimeout    = 30 * time.Second
	defaultMaxRetries = 3
)

type OptFn func(c *commonClient)

func WithHTTPClient(httpClient *http.Client) OptFn {
	return func(c *commonClient) {
		c.client = httpClient
	}
}

func WithTimeout(timeout time.Duration) OptFn {
	return func(c *commonClient) {
		c.client.Timeout = timeout
	}
}

func WithMaxRetries(retries int) OptFn {
	return func(c *commonClient) {
		c.retries = retries
	}
}

func WithRegion(region string) OptFn {
	return func(c *commonClient) {
		c.region = region
	}
}

type commonClient struct {
	client  *http.Client
	retries int
	ua      string
	url     string
	region  string
}

type Client struct {
	*commonClient
	identity identity.Identifier
}

type AdminClient struct {
	*commonClient
	authorizer identity.Authorizer
}

type payloadSchema struct {
	Components []componentSchema `json:"components"`
}

type componentSchema struct {
	Name     string   `json:"name"`
	Features []string `json:"features"`
}

type SSHKeyPair struct {
	PublicKey  string
	PrivateKey string
}

func hostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	return hostname
}

func newCommonClient(baseURL string, fns ...OptFn) *commonClient {
	c := &commonClient{
		url:     baseURL,
		retries: defaultMaxRetries,
		client:  &http.Client{Timeout: defaultTimeout},
		ua: fmt.Sprintf(
			"Go-http-client/1.1; %s; control-plane/features; %s",
			runtime.Version(), hostname(),
		),
	}
	for _, fn := range fns {
		fn(c)
	}
	return c
}

func NewClient(baseURL string, identity identity.Identifier, fns ...OptFn) *Client {
	return &Client{
		identity:     identity,
		commonClient: newCommonClient(baseURL, fns...),
	}
}

func NewAdminClient(baseURL string, authorizer identity.Authorizer, fns ...OptFn) *AdminClient {
	return &AdminClient{
		authorizer:   authorizer,
		commonClient: newCommonClient(baseURL, fns...),
	}
}

type PerComponent = map[string][]string

func (c *commonClient) retry(ctx context.Context, fn func() error) error {
	var opts backoff.BackOff

	opts = backoff.NewExponentialBackOff()
	opts = backoff.WithMaxRetries(opts, uint64(c.retries))
	opts = backoff.WithContext(opts, ctx)

	return backoff.Retry(fn, opts)
}

func (c *AdminClient) GetDestinationSSHKeyPair(ctx context.Context, destID string) (kp SSHKeyPair, err error) {
	endpoint := fmt.Sprintf("%s/dataplane/admin/destinations/%s/sshKeys", c.url, destID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, http.NoBody)
	if err != nil {
		return kp, fmt.Errorf("cannot create request to get ssh key pair: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", c.ua)
	req.SetBasicAuth(c.authorizer.BasicAuth())

	err = c.retry(ctx, func() error {
		resp, err := c.client.Do(req)
		if err != nil {
			return fmt.Errorf("cannot make request to get ssh key pair: %w", err)
		}

		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status code while getting ssh key pair: %d", resp.StatusCode)
		}

		if err := json.NewDecoder(resp.Body).Decode(&kp); err != nil {
			return fmt.Errorf("cannot decode ssh key pair response: %w", err)
		}

		return nil
	})

	return
}

func (c *Client) SendFeatures(ctx context.Context, component string, features []string) error {
	var endpoint string
	switch t := c.identity.(type) {
	case *identity.Namespace:
		endpoint = fmt.Sprintf("%s/data-plane/v1/namespaces/%s/settings", c.url, c.identity.ID())
	case *identity.Workspace:
		endpoint = fmt.Sprintf("%s/data-plane/v1/workspaces/%s/settings", c.url, c.identity.ID())
	default:
		return fmt.Errorf("identity not supported %T", t)
	}

	payload := payloadSchema{
		Components: []componentSchema{
			{
				Name:     component,
				Features: features,
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("could not marshal payload: %w", err)
	}

	return c.retry(ctx, func() error {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("new request: %w", err)
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", c.ua)

		req.SetBasicAuth(c.identity.BasicAuth())

		resp, err := c.client.Do(req)
		if err != nil {
			return fmt.Errorf("doing http request: %w", err)
		}
		defer func() { httputil.CloseResponse(resp) }()

		if resp.StatusCode != http.StatusNoContent {
			// we don't expect a body, unless there is an error
			b, err := io.ReadAll(resp.Body)
			if err != nil {
				return fmt.Errorf("read response body: %w", err)
			}

			err = fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(b))
			if !httputil.RetriableStatus(resp.StatusCode) {
				return backoff.Permanent(fmt.Errorf("non retriable: %w", err))
			}
			return err
		}
		return err
	})
}

func (c *Client) DestinationHistory(ctx context.Context, revisionID string) (backendconfig.DestinationT, error) {
	urlStr := fmt.Sprintf("%s/workspaces/destinationHistory/%s", c.url, revisionID)

	urlValues := url.Values{}
	if c.region != "" {
		urlValues.Set("region", c.region)
	}

	if len(urlValues) > 0 {
		urlStr = fmt.Sprintf("%s?%s", urlStr, urlValues.Encode())
	}

	var destination backendconfig.DestinationT
	err := c.retry(ctx, func() error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, http.NoBody)
		if err != nil {
			return err
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", c.ua)

		req.SetBasicAuth(c.identity.BasicAuth())

		resp, err := c.client.Do(req)
		if err != nil {
			return err
		}
		defer func() { httputil.CloseResponse(resp) }()

		if resp.StatusCode != http.StatusOK {
			b, err := io.ReadAll(resp.Body)
			if err != nil {
				return fmt.Errorf("read response body: %w", err)
			}

			err = fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(b))
			if !httputil.RetriableStatus(resp.StatusCode) {
				return backoff.Permanent(fmt.Errorf("non retriable: %w", err))
			}
			return err
		}

		err = json.NewDecoder(resp.Body).Decode(&destination)
		if err != nil {
			return fmt.Errorf("unmarshal response body: %w", err)
		}

		return nil
	})

	return destination, err
}
