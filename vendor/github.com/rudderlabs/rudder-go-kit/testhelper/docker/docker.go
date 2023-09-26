package docker

import (
	"strconv"
	"testing"

	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
)

// GetHostPort returns the desired port mapping
func GetHostPort(t testing.TB, port string, container *docker.Container) int {
	t.Helper()
	for p, bindings := range container.NetworkSettings.Ports {
		if p.Port() == port {
			pi, err := strconv.Atoi(bindings[0].HostPort)
			require.NoError(t, err)
			return pi
		}
	}
	return 0
}
