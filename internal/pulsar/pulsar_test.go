package pulsar

import (
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/stretchr/testify/require"
)

func Test_Pulsar(t *testing.T) {
	pulsarContainer := PulsarResource(t)
	t.Logf("Pulsar container started: %s", pulsarContainer.URL)
	conf := config.New()
	conf.Set("Pulsar.Client.url", pulsarContainer.URL)
	conf.Set("Pulsar.Producer.topic", "test-topic")
	t.Logf("Pulsar config: %v", conf.GetString("Pulsar.Producer.topic", ""))
	producer, err := New(conf)
	require.NoError(t, err)
	require.NotNil(t, producer)
}

// PulsarResource returns a pulsar container resource
func PulsarResource(t *testing.T) *resource.PulsarResource {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pulsarContainer, err := resource.SetupPulsar(pool, t)
	require.NoError(t, err)
	return pulsarContainer
}
