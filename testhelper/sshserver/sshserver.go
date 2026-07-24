package sshserver

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"

	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
)

const exposedPort = "2222"

type Option func(*config)

type config struct {
	publicKeyPath      string
	username, password string
	network            *dc.Network
}

func WithCredentials(username, password string) Option {
	return func(c *config) {
		c.username = username
		c.password = password
	}
}

func WithPublicKeyPath(publicKeyPath string) Option {
	return func(c *config) {
		c.publicKeyPath = publicKeyPath
	}
}

func WithDockerNetwork(network *dc.Network) Option {
	return func(c *config) {
		c.network = network
	}
}

type Resource struct {
	Host string
	Port int

	container *dockertest.Resource
}

func Setup(pool *dockertest.Pool, cln resource.Cleaner, opts ...Option) (*Resource, error) {
	var c config
	for _, opt := range opts {
		opt(&c)
	}

	network := c.network
	if c.network == nil {
		var err error
		network, err = pool.Client.CreateNetwork(dc.CreateNetworkOptions{Name: "sshserver_network"})
		if err != nil {
			return nil, fmt.Errorf("could not create docker network: %w", err)
		}
		cln.Cleanup(func() {
			if err := pool.Client.RemoveNetwork(network.ID); err != nil {
				cln.Log(fmt.Sprintf("could not remove sshserver_network: %v", err))
			}
		})
	}

	var (
		mounts  []string
		envVars = []string{
			"SUDO_ACCESS=false",
			"DOCKER_MODS=linuxserver/mods:openssh-server-ssh-tunnel",
		}
	)
	if c.username != "" {
		envVars = append(envVars, "USER_NAME="+c.username)
		if c.password != "" {
			envVars = append(envVars,
				"USER_PASSWORD="+c.password,
				"PASSWORD_ACCESS=true",
			)
		}
	}
	if c.publicKeyPath != "" {
		envVars = append(envVars, "PUBLIC_KEY_FILE=/test_key.pub")
		mounts = []string{c.publicKeyPath + ":/test_key.pub"}
	}

	container, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "lscr.io/linuxserver/openssh-server",
		Tag:        "9.3_p2-r1-ls145",
		NetworkID:  network.ID,
		Hostname:   "sshserver",
		ExposedPorts: []string{
			exposedPort + "/tcp",
		},
		PortBindings: map[dc.Port][]dc.PortBinding{
			exposedPort + "/tcp": {
				{HostIP: "127.0.0.1", HostPort: "0"},
			},
		},
		Env:    envVars,
		Mounts: mounts,
	}, func(hc *dc.HostConfig) {
		hc.PublishAllPorts = false
	})
	if err != nil {
		return nil, err
	}
	cln.Cleanup(func() {
		if err := pool.Purge(container); err != nil {
			cln.Log("Could not purge resource", err)
		}
	})

	host := container.GetBoundIP(exposedPort + "/tcp")
	port := container.GetPort(exposedPort + "/tcp")
	if err := waitForSSHBanner(host, port, time.Minute); err != nil {
		logs := bytes.NewBuffer(nil)
		if logErr := pool.Client.Logs(dc.LogsOptions{
			Container:    container.Container.ID,
			Stdout:       true,
			Stderr:       true,
			OutputStream: logs,
			ErrorStream:  logs,
		}); logErr != nil {
			cln.Log("could not read SSH server logs:", logErr)
		}
		if logs.Len() > 0 {
			cln.Log("SSH server logs:\n" + logs.String())
		}
		return nil, err
	}

	p, err := strconv.Atoi(port)
	if err != nil {
		return nil, fmt.Errorf("could not convert port %q to int: %w", port, err)
	}

	return &Resource{
		Host:      host,
		Port:      p,
		container: container,
	}, nil
}

func waitForSSHBanner(host, port string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	address := net.JoinHostPort(host, port)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, time.Second)
		if err == nil {
			buf := make([]byte, 256)
			_ = conn.SetReadDeadline(time.Now().Add(time.Second))
			n, readErr := conn.Read(buf)
			_ = conn.Close()
			banner := string(buf[:n])
			if strings.HasPrefix(banner, "SSH-") {
				return nil
			}
			if readErr != nil {
				lastErr = readErr
			} else {
				lastErr = fmt.Errorf("unexpected SSH banner %q", banner)
			}
		} else {
			lastErr = err
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("ssh server not healthy within timeout: %w", lastErr)
}
