package tunnelling

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/sshserver"
	"github.com/rudderlabs/rudder-go-kit/testhelper/keygen"
)

func TestConnectConfigValidation(t *testing.T) {
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
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Connect(tc.dsn, tc.config)
			require.Nil(t, db)
			require.ErrorContains(t, err, tc.errorContains)
		})
	}
}

func TestConnectOverSSHTunnel(t *testing.T) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Skipping SSH tunnel integration because Docker is unavailable: %v", err)
	}

	// Start shared Docker network
	network, err := pool.Client.CreateNetwork(dc.CreateNetworkOptions{Name: "uploads_tunneling_network"})
	if err != nil {
		t.Skipf("Skipping SSH tunnel integration because Docker network setup is unavailable: %v", err)
	}
	t.Cleanup(func() {
		if err := pool.Client.RemoveNetwork(network.ID); err != nil {
			t.Logf("Error while removing Docker network: %v", err)
		}
	})

	privateKeyPath, publicKeyPath, err := keygen.NewRSAKeyPair(2048, keygen.SaveTo(t.TempDir()))
	require.NoError(t, err)

	postgresResource, err := postgres.Setup(pool, t, postgres.WithNetwork(network))
	if err != nil {
		t.Skipf("Skipping SSH tunnel integration because Docker postgres setup is unavailable: %v", err)
	}
	sshServerResource, err := sshserver.Setup(pool, panicSafeCleaner{t: t},
		sshserver.WithPublicKeyPath(publicKeyPath),
		sshserver.WithCredentials("linuxserver.io", ""),
		sshserver.WithDockerNetwork(network),
	)
	if err != nil {
		t.Skipf("Skipping SSH tunnel integration because Docker sshserver setup is unavailable: %v", err)
	}

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

type panicSafeCleaner struct {
	t *testing.T
}

func (c panicSafeCleaner) Cleanup(fn func()) {
	c.t.Cleanup(func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				c.t.Logf("Recovered panic during Docker cleanup: %v", recovered)
			}
		}()
		fn()
	})
}

func (c panicSafeCleaner) Log(args ...any) {
	c.t.Log(args...)
}

func (c panicSafeCleaner) Logf(format string, args ...any) {
	c.t.Logf(format, args...)
}

func (c panicSafeCleaner) Failed() bool {
	return c.t.Failed()
}
