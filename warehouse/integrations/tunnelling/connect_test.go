package tunnelling

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"

	"github.com/stretchr/testify/require"
)

func TestConnect(t *testing.T) {
	privateKey, err := os.ReadFile("testdata/test_key")
	require.Nil(t, err)

	ctx := context.Background()

	c := testcompose.New(t, compose.FilePaths{"./testdata/docker-compose.yml"})
	c.Start(context.Background())

	host := "0.0.0.0"
	user := c.Env("openssh-server", "USER_NAME")
	port := c.Port("openssh-server", 2222)
	postgresPort := c.Port("postgres", 5432)

	testCases := []struct {
		name      string
		dsn       string
		config    Config
		wantError error
	}{
		{
			name:      "empty config",
			dsn:       "dsn",
			config:    Config{},
			wantError: ErrMissingKey,
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
			wantError: errors.New("invalid type"),
		},
		{
			name: "missing sshUser",
			dsn:  "dsn",
			config: Config{
				sshHost:       "host",
				sshPort:       "port",
				sshPrivateKey: "privateKey",
			},
			wantError: ErrMissingKey,
		},
		{
			name: "missing sshHost",
			dsn:  "dsn",
			config: Config{
				sshUser:       "user",
				sshPort:       "port",
				sshPrivateKey: "privateKey",
			},
			wantError: ErrMissingKey,
		},
		{
			name: "missing sshPort",
			dsn:  "dsn",
			config: Config{
				sshUser:       "user",
				sshHost:       "host",
				sshPrivateKey: "privateKey",
			},
			wantError: ErrMissingKey,
		},
		{
			name: "missing sshPrivateKey",
			dsn:  "dsn",
			config: Config{
				sshUser: "user",
				sshHost: "host",
				sshPort: "port",
			},
			wantError: ErrMissingKey,
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
			wantError: errors.New("invalid port"),
		},
		{
			name: "invalid dsn",
			dsn:  "postgres://user:password@host:5439/db?query1=val1&query2=val2",
			config: Config{
				sshUser:       "user",
				sshHost:       "0.0.0.0",
				sshPort:       "22",
				sshPrivateKey: "privateKey",
			},
			wantError: errors.New("invalid dsn"),
		},
		{
			name: "valid dsn",
			dsn:  fmt.Sprintf("postgres://postgres:postgres@db_postgres:%d/postgres?sslmode=disable", postgresPort),
			config: Config{
				sshUser:       user,
				sshHost:       host,
				sshPort:       port,
				sshPrivateKey: privateKey,
			},
			wantError: errors.New("invalid dsn"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := Connect(tc.dsn, tc.config)
			t.Log(err)
			if tc.wantError != nil {
				require.Error(t, err, tc.wantError)
				return
			}
			require.NoError(t, err)
			require.NoError(t, db.PingContext(ctx))
		})
	}
}
