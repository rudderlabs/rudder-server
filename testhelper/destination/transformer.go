package destination

import (
	_ "encoding/json"
	"fmt"
	"log"

	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
)

type TransformerResource struct {
	TransformURL string
	Port         string
}

func SetupTransformer(pool *dockertest.Pool, d deferer) (*TransformerResource, error) {
	// Set Rudder Transformer
	// pulls an image, creates a container based on it and runs it
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

	d.Defer(func() error {
		if err := pool.Purge(transformerContainer); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
		return nil
	})

	return &TransformerResource{
		TransformURL: fmt.Sprintf("http://localhost:%s", transformerContainer.GetPort("9090/tcp")),
		Port:         transformerContainer.GetPort("9090/tcp"),
	}, nil
}
