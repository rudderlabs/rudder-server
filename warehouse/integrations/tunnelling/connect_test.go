package tunnelling

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/sshserver"
	"github.com/rudderlabs/rudder-go-kit/testhelper/keygen"
)

func TestConnect(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	// Start shared Docker network
	network, err := pool.Client.CreateNetwork(dc.CreateNetworkOptions{Name: "uploads_tunneling_network"})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := pool.Client.RemoveNetwork(network.ID); err != nil {
			t.Logf("Error while removing Docker network: %v", err)
		}
	})

	privateKeyPath, publicKeyPath, err := keygen.NewRSAKeyPair(2048, keygen.SaveTo(t.TempDir()))
	require.NoError(t, err)

	var (
		group             errgroup.Group
		postgresResource  *postgres.Resource
		sshServerResource *sshserver.Resource
	)
	group.Go(func() (err error) {
		postgresResource, err = postgres.Setup(pool, t, postgres.WithNetwork(network))
		return err
	})
	group.Go(func() (err error) {
		sshServerResource, err = sshserver.Setup(pool, t,
			sshserver.WithPublicKeyPath(publicKeyPath),
			sshserver.WithCredentials("linuxserver.io", ""),
			sshserver.WithDockerNetwork(network),
		)
		return err
	})
	require.NoError(t, group.Wait())

	postgresContainer, err := pool.Client.InspectContainer(postgresResource.ContainerID)
	require.NoError(t, err)

	tunnelledHost := postgresContainer.NetworkSettings.Networks[network.Name].IPAddress
	tunnelledDatabase := "jobsdb"
	tunnelledUser := "rudder"
	tunnelledPassword := "password"
	tunnelledSSHUser := "linuxserver.io"
	tunnelledSSHHost := "localhost"
	tunnelledSSHPort := strconv.Itoa(sshServerResource.Port)
	tunnelledPrivateKey, err := os.ReadFile(privateKeyPath)
	require.NoError(t, err)

	testCases := []struct {
		name          string
		dsn           string
		config        Config
		errorContains string
	}{
		{
			name:          "empty config",
			dsn:           "dsn",
			config:        Config{},
			errorContains: ErrMissingKey.Error(),
		},
		{
			name: "invalid config",
			dsn:  "dsn",
			config: Config{
				sshUser:       "user",
				sshHost:       "host",
				sshPort:       22,
				sshPrivateKey: "privateKey",
			},
			errorContains: "unexpected type: sshPort expected string",
		},
		{
			name: "missing sshUser",
			dsn:  "dsn",
			config: Config{
				sshHost:       "host",
				sshPort:       "port",
				sshPrivateKey: "privateKey",
			},
			errorContains: ErrMissingKey.Error(),
		},
		{
			name: "missing sshHost",
			dsn:  "dsn",
			config: Config{
				sshUser:       "user",
				sshPort:       "port",
				sshPrivateKey: "privateKey",
			},
			errorContains: ErrMissingKey.Error(),
		},
		{
			name: "missing sshPort",
			dsn:  "dsn",
			config: Config{
				sshUser:       "user",
				sshHost:       "host",
				sshPrivateKey: "privateKey",
			},
			errorContains: ErrMissingKey.Error(),
		},
		{
			name: "missing sshPrivateKey",
			dsn:  "dsn",
			config: Config{
				sshUser: "user",
				sshHost: "host",
				sshPort: "port",
			},
			errorContains: ErrMissingKey.Error(),
		},
		{
			name: "invalid sshPort",
			dsn:  "dsn",
			config: Config{
				sshUser:       "user",
				sshHost:       "host",
				sshPort:       "port",
				sshPrivateKey: "privateKey",
			},
			errorContains: `parsing "port": invalid syntax`,
		},
		{
			name: "invalid dsn",
			dsn:  "postgres://user:password@host:5439/db?query1=val1&query2=val2",
			config: Config{
				sshUser:       tunnelledSSHUser,
				sshHost:       tunnelledSSHHost,
				sshPort:       tunnelledSSHPort,
				sshPrivateKey: string(tunnelledPrivateKey),
			},
			errorContains: "connection reset by peer",
		},
		{
			name: "valid dsn",
			dsn: fmt.Sprintf(
				"postgres://%s:%s@%s:5432/%s?sslmode=disable",
				tunnelledUser, tunnelledPassword, tunnelledHost, tunnelledDatabase,
			),
			config: Config{
				sshUser:       tunnelledSSHUser,
				sshHost:       tunnelledSSHHost,
				sshPort:       tunnelledSSHPort,
				sshPrivateKey: string(tunnelledPrivateKey),
			},
			errorContains: "", // No error expected
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Connect(tc.dsn, tc.config)
			if tc.errorContains != "" {
				require.Nil(t, db)
				require.ErrorContains(t, err, tc.errorContains)
			} else {
				require.NoError(t, err)
				require.NotNil(t, db)
			}
		})
	}
}
