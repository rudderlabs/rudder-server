package tunnel

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/sync/errgroup"
)

type SSHConfig struct {
	User       string
	Host       string
	Port       int
	PrivateKey []byte

	RemoteHost string
	RemotePort int
}

type SSH struct {
	localServer net.Listener
	sshClient   *ssh.Client
	remoteAddr  string

	err   error
	errMu sync.Mutex

	backgroundWG sync.WaitGroup
}

func ListenAndForward(config *SSHConfig) (*SSH, error) {
	singer, err := ssh.ParsePrivateKey(config.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("parsing private key: %s", err.Error())
	}

	endpoint := net.JoinHostPort(config.Host, strconv.Itoa(config.Port))
	sshClient, err := ssh.Dial("tcp", endpoint, &ssh.ClientConfig{
		User: config.User,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(singer),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		BannerCallback:  ssh.BannerDisplayStderr(),
		Timeout:         10 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("server %q dial error: %w", endpoint, err)
	}

	listener, err := net.Listen("tcp", `127.0.0.1:0`)
	if err != nil {
		sshClient.Close()
		return nil, fmt.Errorf("listening on %s://%s: %w", "tcp", listener.Addr().String(), err)
	}

	tunnel := &SSH{
		localServer: listener,
		sshClient:   sshClient,
		remoteAddr: net.JoinHostPort(
			config.RemoteHost,
			strconv.Itoa(config.RemotePort),
		),
	}

	go tunnel.listen()

	return tunnel, nil
}

func (t *SSH) Addr() string {
	return t.localServer.Addr().String()
}

func (t *SSH) Error() error {
	t.errMu.Lock()
	defer t.errMu.Unlock()

	return t.err
}

func (t *SSH) Close() error {
	_ = t.localServer.Close()
	err := t.sshClient.Close()
	t.backgroundWG.Wait()
	return err
}

func (t *SSH) listen() {
	t.backgroundWG.Add(1)
	defer t.backgroundWG.Done()

	for {
		localConn, err := t.localServer.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			t.errMu.Lock()
			t.err = err
			t.errMu.Unlock()
			return
		}

		remoteConn, err := t.sshClient.Dial("tcp", t.remoteAddr)
		if err != nil {
			t.errMu.Lock()
			t.err = err
			t.errMu.Unlock()

			localConn.Close()
			continue
		}

		t.backgroundWG.Add(1)
		go func() {
			defer t.backgroundWG.Done()

			err := t.forward(localConn, remoteConn)
			if err != nil {
				t.errMu.Lock()
				t.err = fmt.Errorf("forwarding: %w", err)
				t.errMu.Unlock()
			}
		}()
	}
}

func (t *SSH) forward(localConn, remoteConn net.Conn) error {
	g := errgroup.Group{}
	g.Go(func() error {
		defer remoteConn.Close()

		_, err := io.Copy(remoteConn, localConn)
		if err != nil {
			return err
		}

		return nil
	})
	g.Go(func() error {
		defer localConn.Close()

		_, err := io.Copy(localConn, remoteConn)
		if err != nil {
			return err
		}
		return nil
	})

	return g.Wait()
}
