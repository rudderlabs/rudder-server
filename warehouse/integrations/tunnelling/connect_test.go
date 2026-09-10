package tunnelling

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"

	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-go-kit/testhelper/keygen"
)

func TestConnect(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	privateKeyPath, publicKeyPath, err := keygen.NewRSAKeyPair(2048, keygen.SaveTo(t.TempDir()))
	require.NoError(t, err)
	publicKey, err := os.ReadFile(publicKeyPath)
	require.NoError(t, err)
	tunnelledPrivateKey, err := os.ReadFile(privateKeyPath)
	require.NoError(t, err)

	postgresResource, err := postgres.Setup(pool, t)
	require.NoError(t, err)
	sshServerPort := startSSHForwarder(t, publicKey)

	tunnelledHost := postgresResource.Host
	tunnelledDatabase := "jobsdb"
	tunnelledUser := "rudder"
	tunnelledPassword := "password"
	tunnelledSSHUser := "any"
	tunnelledSSHHost := "localhost"
	tunnelledSSHPort := strconv.Itoa(sshServerPort)
	unreachablePort := unusedLocalPort(t)

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
			dsn:  fmt.Sprintf("postgres://user:password@127.0.0.1:%s/db?query1=val1&query2=val2", unreachablePort),
			config: Config{
				sshUser:       tunnelledSSHUser,
				sshHost:       tunnelledSSHHost,
				sshPort:       tunnelledSSHPort,
				sshPrivateKey: string(tunnelledPrivateKey),
			},
			errorContains: "pinging warehouse connection",
		},
		{
			name: "valid dsn",
			dsn: fmt.Sprintf(
				"postgres://%s:%s@%s:%s/%s?sslmode=disable",
				tunnelledUser, tunnelledPassword, tunnelledHost, postgresResource.Port, tunnelledDatabase,
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
				t.Cleanup(func() {
					require.NoError(t, db.Close())
				})
			}
		})
	}
}

func unusedLocalPort(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, listener.Close())
	}()

	return strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
}

type directTCPIPPayload struct {
	DestAddr   string
	DestPort   uint32
	OriginAddr string
	OriginPort uint32
}

func startSSHForwarder(t *testing.T, publicAuthorizedKey []byte) int {
	t.Helper()

	hostKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	hostSigner, err := ssh.NewSignerFromKey(hostKey)
	require.NoError(t, err)

	authorizedKey, _, _, _, err := ssh.ParseAuthorizedKey(publicAuthorizedKey)
	require.NoError(t, err)

	serverConfig := &ssh.ServerConfig{
		PublicKeyCallback: func(_ ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			if bytes.Equal(key.Marshal(), authorizedKey.Marshal()) {
				return &ssh.Permissions{}, nil
			}
			return nil, fmt.Errorf("unauthorized public key")
		},
	}
	serverConfig.AddHostKey(hostSigner)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var wg sync.WaitGroup
	var (
		connMu sync.Mutex
		conns  []net.Conn
	)
	t.Cleanup(func() {
		require.NoError(t, listener.Close())
		connMu.Lock()
		for _, conn := range conns {
			_ = conn.Close()
		}
		connMu.Unlock()
		wg.Wait()
	})

	wg.Go(func() {
		for {
			conn, err := listener.Accept()
			if errors.Is(err, net.ErrClosed) {
				return
			}
			if err != nil {
				t.Logf("accepting SSH connection: %v", err)
				return
			}
			connMu.Lock()
			conns = append(conns, conn)
			connMu.Unlock()

			wg.Go(func() {
				handleSSHForwarderConn(t, conn, serverConfig, &wg)
			})
		}
	})

	return listener.Addr().(*net.TCPAddr).Port
}

func handleSSHForwarderConn(t *testing.T, conn net.Conn, config *ssh.ServerConfig, wg *sync.WaitGroup) {
	t.Helper()

	sshConn, chans, reqs, err := ssh.NewServerConn(conn, config)
	if err != nil {
		t.Logf("accepting SSH handshake: %v", err)
		_ = conn.Close()
		return
	}
	defer sshConn.Close()
	go ssh.DiscardRequests(reqs)

	for newChannel := range chans {
		if newChannel.ChannelType() != "direct-tcpip" {
			_ = newChannel.Reject(ssh.UnknownChannelType, "unsupported channel type")
			continue
		}

		var payload directTCPIPPayload
		if err := ssh.Unmarshal(newChannel.ExtraData(), &payload); err != nil {
			_ = newChannel.Reject(ssh.Prohibited, err.Error())
			continue
		}
		target, err := net.Dial("tcp", net.JoinHostPort(payload.DestAddr, strconv.Itoa(int(payload.DestPort))))
		if err != nil {
			_ = newChannel.Reject(ssh.ConnectionFailed, err.Error())
			continue
		}

		channel, requests, err := newChannel.Accept()
		if err != nil {
			_ = target.Close()
			continue
		}
		go ssh.DiscardRequests(requests)

		var closeOnce sync.Once
		closeBoth := func() {
			_ = channel.Close()
			_ = target.Close()
		}

		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = io.Copy(channel, target)
			closeOnce.Do(closeBoth)
		}()
		go func() {
			defer wg.Done()
			_, _ = io.Copy(target, channel)
			closeOnce.Do(closeBoth)
		}()
	}
}
