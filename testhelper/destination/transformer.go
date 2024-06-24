package destination

import (
	_ "encoding/json"
	"errors"
	"fmt"
	"net/http"

	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"

	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
)

const repo = "rudderstack/rudder-transformer"

type TransformerResource struct {
	TransformURL string
	Port         string
}

func SetupTransformer(pool *dockertest.Pool, d Cleaner) (*TransformerResource, error) {
	// Set Rudder Transformer
	// pulls an image first to make sure we don't have an old cached version locally,
	// then it creates a container based on it and runs it
	err := pool.Client.PullImage(docker.PullImageOptions{
		Repository: repo,
		Tag:        "latest",
	}, docker.AuthConfiguration{})
	if err != nil {
		return nil, fmt.Errorf("failed to pull image: %w", err)
	}
	transformerContainer, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   repo,
		Tag:          "latest",
		ExposedPorts: []string{"9090"},
		Env: []string{
			"CONFIG_BACKEND_URL=https://api.rudderstack.com",
		},
	})
	if err != nil {
		return nil, err
	}

	d.Cleanup(func() {
		if err := pool.Purge(transformerContainer); err != nil {
			d.Log("Could not purge resource:", err)
		}
	})

	tr := &TransformerResource{
		TransformURL: fmt.Sprintf("http://localhost:%s", transformerContainer.GetPort("9090/tcp")),
		Port:         transformerContainer.GetPort("9090/tcp"),
	}

	err = pool.Retry(func() (err error) {
		resp, err := http.Get(tr.TransformURL + "/health")
		if err != nil {
			return err
		}
		defer func() { kithttputil.CloseResponse(resp) }()
		if resp.StatusCode != 200 {
			return errors.New(resp.Status)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return tr, nil
}
