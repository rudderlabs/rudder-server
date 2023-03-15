package sshserver

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"

	"github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
)

const exposedPort = "2222"

type Option func(*config)

type config struct {
	publicKeyPath      string
	username, password string

	network *dc.Network
	logger  destination.Logger
}

func (c *config) defaults() {
	if c.logger == nil {
		c.logger = &destination.NOPLogger{}
	}
}

// WithPublicKeyPath sets the public key path to use for the SSH server.
func WithPublicKeyPath(publicKeyPath string) Option {
	return func(c *config) {
		c.publicKeyPath = publicKeyPath
	}
}

// WithCredentials sets the username and password to use for the SSH server.
func WithCredentials(username, password string) Option {
	return func(c *config) {
		c.username = username
		c.password = password
	}
}

// WithDockerNetwork sets the Docker network to use for the SSH server.
func WithDockerNetwork(network *dc.Network) Option {
	return func(c *config) {
		c.network = network
	}
}

// WithLogger sets the logger to use for the SSH server.
func WithLogger(logger destination.Logger) Option {
	return func(c *config) {
		c.logger = logger
	}
}

type Resource struct {
	Port string

	container *dockertest.Resource
}

func Setup(pool *dockertest.Pool, cln destination.Cleaner, opts ...Option) (*Resource, error) {
	var c config
	for _, opt := range opts {
		opt(&c)
	}
	c.defaults()

	network := c.network
	if c.network == nil {
		var err error
		network, err = pool.Client.CreateNetwork(dc.CreateNetworkOptions{Name: "sshserver_network"})
		if err != nil {
			return nil, fmt.Errorf("could not create docker network: %w", err)
		}
		cln.Cleanup(func() {
			if err := pool.Client.RemoveNetwork(network.ID); err != nil {
				cln.Log(fmt.Errorf("could not remove sshserver_network: %w", err))
			}
		})
	}

	portInt, err := testhelper.GetFreePort()
	if err != nil {
		return nil, err
	}

	var (
		port    = fmt.Sprintf("%s/tcp", strconv.Itoa(portInt))
		mounts  []string
		envVars = []string{
			"SUDO_ACCESS=false",
			"DOCKER_MODS=linuxserver/mods:openssh-server-ssh-tunnel",
		}
	)
	if c.username != "" {
		envVars = append(envVars, "USER_NAME="+c.username)
		if c.password != "" {
			envVars = append(envVars, []string{
				"USER_PASSWORD=" + c.password,
				"PASSWORD_ACCESS=true",
			}...)
		}
	}
	if c.publicKeyPath != "" {
		envVars = append(envVars, "PUBLIC_KEY_FILE=/test_key.pub")
		mounts = []string{c.publicKeyPath + ":/test_key.pub"}
	}
	container, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "lscr.io/linuxserver/openssh-server",
		Tag:        "latest",
		NetworkID:  network.ID,
		Hostname:   "sshserver",
		PortBindings: map[dc.Port][]dc.PortBinding{
			exposedPort + "/tcp": {{HostIP: "sshserver", HostPort: port}},
		},
		Env:    envVars,
		Mounts: mounts,
	})
	if err != nil {
		return nil, err
	}
	cln.Cleanup(func() {
		if err := pool.Purge(container); err != nil {
			cln.Log("Could not purge resource", err)
		}
	})

	var (
		buf     *bytes.Buffer
		timeout = time.After(30 * time.Second)
		ticker  = time.NewTicker(200 * time.Millisecond)
	)
loop:
	for {
		select {
		case <-ticker.C:
			buf = bytes.NewBuffer(nil)
			exitCode, err := container.Exec([]string{"cat", "/config/logs/openssh/current"}, dockertest.ExecOptions{
				StdOut: buf,
			})
			if err != nil {
				c.logger.Log("could not exec into SSH server:", err)
				continue
			}
			if exitCode != 0 {
				c.logger.Log("invalid exit code while exec-ing into SSH server:", exitCode)
				continue
			}
			if buf.String() == "" {
				c.logger.Log("SSH server not ready yet")
				continue
			}
			if !strings.Contains(buf.String(), "Server listening on :: port "+exposedPort) {
				c.logger.Log("SSH server not listening on port yet")
				continue
			}
			c.logger.Log("SSH server is ready:", exposedPort, "=>", container.GetPort(exposedPort+"/tcp"))
			break loop
		case <-timeout:
			return nil, fmt.Errorf("ssh server not health within timeout")
		}
	}

	return &Resource{
		Port:      container.GetPort(exposedPort + "/tcp"),
		container: container,
	}, nil
}
