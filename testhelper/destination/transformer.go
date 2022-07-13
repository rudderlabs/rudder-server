package destination

import (
	_ "encoding/json"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

type TransformerResource struct {
	TransformURL string
	Port         string
}

func SetupTransformer(pool *dockertest.Pool, d cleaner) (*TransformerResource, error) {
	// Set Rudder Transformer
	// pulls an image first to make sure we don't have an old cached version locally,
	// then it creates a container based on it and runs it
	err := pool.Client.PullImage(docker.PullImageOptions{
		Repository: "rudderlabs/rudder-transformer",
		Tag:        "latest",
	}, docker.AuthConfiguration{})
	if err != nil {
		return nil, fmt.Errorf("failed to pull image: %w", err)
	}
	transformerContainer, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "rudderlabs/rudder-transformer",
		Tag:          "latest",
		ExposedPorts: []string{"9090"},
		Env: []string{
			"CONFIG_BACKEND_URL=https://api.rudderlabs.com",
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

	return &TransformerResource{
		TransformURL: fmt.Sprintf("http://localhost:%s", transformerContainer.GetPort("9090/tcp")),
		Port:         transformerContainer.GetPort("9090/tcp"),
	}, nil
}
