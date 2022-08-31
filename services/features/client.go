package features

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

type NamespaceClient struct {
	client      *http.Client
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

	_, statusCode := misc.HTTPCallWithRetryWithTimeout(url, body, 60)
	if statusCode != 200 {
		return fmt.Errorf("unexpected status code %d", statusCode)
	}

	return nil
}
