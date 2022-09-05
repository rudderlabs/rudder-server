package features

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
)

var (
	DefaultTimeout = 30 * time.Second
	MaxRetries     = uint64(3)
)

type optFn func(c *client)

func WithURL(url string) optFn {
	return func(c *client) {
		c.url = url
	}
}

func WithHTTPClient(httpClient *http.Client) optFn {
	return func(c *client) {
		c.client = httpClient
	}
}

func WithTimeout(timeout time.Duration) optFn {
	return func(c *client) {
		c.client.Timeout = timeout
	}
}

type client struct {
	client   *http.Client
	url      string
	identity identity.Identifier
}

type payload struct {
	Components []component `json:"components"`
}

type component struct {
	Name     string   `json:"name"`
	Features []string `json:"features"`
}

func New(identity identity.Identifier, fns ...optFn) *client {
	c := &client{
		client: &http.Client{
			Timeout: DefaultTimeout,
		},
		url:      config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com"),
		identity: identity,
	}

	for _, fn := range fns {
		fn(c)
	}

	return c
}

func (c *client) Send(ctx context.Context, registry *Registry) error {
	url := fmt.Sprintf("%s/data-plane/%s/%s/settings", c.url, c.identity.Resource(), c.identity.ID())

	payload := payload{
		Components: []component{},
	}

	registry.Each(func(name string, features []string) {
		payload.Components = append(payload.Components, component{
			Name:     name,
			Features: features,
		})
	})

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	backoffWithMaxRetry := backoff.WithContext(backoff.WithMaxRetries(backoff.NewExponentialBackOff(), MaxRetries), ctx)
	return backoff.Retry(func() error {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return err
		}

		req.Header.Set("Content-Type", "application/json")

		c.identity.HTTPAuth(req)

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
