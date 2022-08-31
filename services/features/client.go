package features

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/cenkalti/backoff"
)

type NamespaceClient struct {
	Client      *http.Client
	URL         string
	NamespaceID string
	Auth        string
}

type payload struct {
	Components []component `json:"components"`
}

type component struct {
	Name     string   `json:"name"`
	Features []string `json:"features"`
}

func (c *NamespaceClient) Send(ctx context.Context, registry *Registry) error {

	url := fmt.Sprintf("%s/data-plane/namespaces/%s/settings", c.URL, c.NamespaceID)

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
