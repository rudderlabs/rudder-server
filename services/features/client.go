package features

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/cenkalti/backoff"
)

type Client struct {
	Client   *http.Client
	URL      string
	Identity identity
}

type payload struct {
	Components []component `json:"components"`
}

type component struct {
	Name     string   `json:"name"`
	Features []string `json:"features"`
}

func (c *Client) Send(ctx context.Context, registry *Registry) error {
	url := fmt.Sprintf("%s/data-plane/%s/%s/settings", c.URL, c.Identity.Resource(), c.Identity.ID())

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

	backoffWithMaxRetry := backoff.WithContext(backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3), ctx)
	return backoff.Retry(func() error {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return err
		}

		c.Identity.HTTPAuth(req)

		resp, err := c.Client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status code: %d", req.Response.StatusCode)
		}
		return err
	}, backoffWithMaxRetry)
}
