package testhelper

import (
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/testhelper"
	dt "github.com/rudderlabs/rudder-go-kit/testhelper/docker"
)

const healthPort = "13133"

type StartOTelCollectorOpt func(*startOTelCollectorConf)

// WithStartCollectorPort allows to specify the port on which the collector will be listening for gRPC requests.
func WithStartCollectorPort(port int) StartOTelCollectorOpt {
	return func(c *startOTelCollectorConf) {
		c.port = port
	}
}

func StartOTelCollector(t testing.TB, metricsPort, configPath string, opts ...StartOTelCollectorOpt) (
	container *docker.Container,
	grpcEndpoint string,
) {
	t.Helper()

	conf := &startOTelCollectorConf{}
	for _, opt := range opts {
		opt(conf)
	}

	if conf.port == 0 {
		var err error
		conf.port, err = testhelper.GetFreePort()
		require.NoError(t, err)
	}

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	collector, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "otel/opentelemetry-collector",
		Tag:          "0.67.0",
		ExposedPorts: []string{healthPort, metricsPort},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"4317/tcp": {{HostPort: strconv.Itoa(conf.port)}},
		},
		Mounts: []string{configPath + ":/etc/otelcol/config.yaml"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := pool.Purge(collector); err != nil {
			t.Logf("Could not purge resource: %v", err)
		}
	})

	healthEndpoint := fmt.Sprintf("http://localhost:%d", dt.GetHostPort(t, healthPort, collector.Container))
	require.Eventually(t, func() bool {
		resp, err := http.Get(healthEndpoint)
		if err != nil {
			return false
		}
		defer func() { httputil.CloseResponse(resp) }()
		return resp.StatusCode == http.StatusOK
	}, 10*time.Second, 100*time.Millisecond, "Collector was not ready on health port")

	t.Log("Container is healthy")

	return collector.Container, "localhost:" + strconv.Itoa(conf.port)
}

type startOTelCollectorConf struct {
	port int
}
