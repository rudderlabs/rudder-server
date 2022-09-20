package features

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
)

var (
	defaultTimeout    = 30 * time.Second
	defaultMaxRetries = 3
)

type OptFn func(c *Client)

func WithHTTPClient(httpClient *http.Client) OptFn {
	return func(c *Client) {
		c.client = httpClient
	}
}

func WithTimeout(timeout time.Duration) OptFn {
	return func(c *Client) {
		c.client.Timeout = timeout
	}
}

func WithMaxRetries(retries int) OptFn {
	return func(c *Client) {
		c.retries = retries
	}
}

type Client struct {
	client   *http.Client
	retries  int
	ua       string
	url      string
	identity identity.Identifier
}

type payloadSchema struct {
	Components []componentSchema `json:"components"`
}

type componentSchema struct {
	Name     string   `json:"name"`
	Features []string `json:"features"`
}

func hostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	return hostname
}

func New(baseURL string, identity identity.Identifier, fns ...OptFn) *Client {
	c := &Client{
		url:      baseURL,
		identity: identity,

		client: &http.Client{
			Timeout: defaultTimeout,
		},
		retries: defaultMaxRetries,
		ua:      fmt.Sprintf("Go-http-client/1.1; %s; control-plane/features; %s", runtime.Version(), hostname()),
	}

	for _, fn := range fns {
		fn(c)
	}

	return c
}

type PerComponent = map[string][]string

func (c *Client) Send(ctx context.Context, component string, features []string) error {
	var url string

	switch t := c.identity.(type) {
	case *identity.Namespace:
		url = fmt.Sprintf("%s/data-plane/v1/namespaces/%s/settings", c.url, c.identity.ID())
	case *identity.Workspace:
		url = fmt.Sprintf("%s/data-plane/v1/workspaces/%s/settings", c.url, c.identity.ID())
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
		return err
	}

	backoffWithMaxRetry := backoff.WithContext(backoff.WithMaxRetries(backoff.NewExponentialBackOff(), uint64(c.retries)), ctx)
	return backoff.Retry(func() error {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
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
		defer resp.Body.Close()

		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		if resp.StatusCode != http.StatusNoContent {
			return fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(b))
		}
		return err
	}, backoffWithMaxRetry)
}
