package ssh

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"golang.org/x/crypto/ssh"
)

type logger interface {
	Printf(string, ...interface{})
}

type ImportedSSHTunnel struct {
	Local    *Endpoint
	Server   *Endpoint
	Remote   *Endpoint
	Config   *ssh.ClientConfig
	Log      logger
	Conns    []net.Conn
	SvrConns []*ssh.Client
	isOpen   bool
	close    chan interface{}
	cancel   context.CancelFunc
	ctx      context.Context
}

func (t *ImportedSSHTunnel) logf(fmt string, args ...interface{}) {
	if t.Log != nil {
		t.Log.Printf(fmt, args...)
	}
}

func newConnectionWaiter(listener net.Listener, c chan net.Conn) {

	conn, err := listener.Accept()
	if err != nil {
		fmt.Printf("accepting connection errored out: %s", err.Error())
		return
	}
	c <- conn
}

func (tunnel *ImportedSSHTunnel) Start(parentCtx context.Context) error {
	fmt.Printf("Starting the tunnelling of ssh connection: %s\n", tunnel.Local.String())

	tunnel.ctx, tunnel.cancel = context.WithCancel(parentCtx)
	// TODO: Use the local address to be `127.0.0.1` vs `localhost`
	listener, err := net.Listen("tcp", tunnel.Local.String())
	if err != nil {
		return fmt.Errorf("listening on %s://%s: %v", "tcp", tunnel.Local.String(), err)
	}

	defer listener.Close()
	tunnel.Local.Port = listener.Addr().(*net.TCPAddr).Port

	isOpen := true
	for {
		fmt.Println("Looping to check if the tunnel is open")

		if !isOpen {
			break
		}

		c := make(chan net.Conn)
		go newConnectionWaiter(listener, c)

		tunnel.logf("listening for new connections")

		select {

		case <-tunnel.ctx.Done():
			tunnel.logf("done with the tunnelling operations, returning")
			isOpen = false

		case conn := <-c:
			tunnel.logf("accepted connection")
			go tunnel.forward(conn)
		}
	}

	tunnel.logf("successfully tunnel closed")
	return nil
}

func (tunnel *ImportedSSHTunnel) forward(localConn net.Conn) {
	defer localConn.Close()

	serverConn, err := ssh.Dial("tcp", tunnel.Server.String(), tunnel.Config)
	if err != nil {
		tunnel.logf("server dial error: %s", err)
		return
	}

	defer serverConn.Close()

	tunnel.logf("connected to %s (1 of 2)\n", tunnel.Server.String())
	tunnel.logf("connecting to remote: %s\n", tunnel.Remote.String())
	remoteConn, err := serverConn.Dial("tcp", tunnel.Remote.String())
	if err != nil {
		tunnel.logf("remote dial error: %s", err)
		return
	}

	defer remoteConn.Close()

	myCtx, myCancel := context.WithCancel(tunnel.ctx)

	go func() {
		_, err = io.Copy(remoteConn, localConn)
		if err != nil {
			//log.Printf("Error on io.Copy remote->local on connection %s: %s", connStr, err.Error())
			myCancel()
			return
		}
	}()

	go func() {
		_, err = io.Copy(localConn, remoteConn)
		if err != nil {
			//log.Printf("Error on io.Copy local->remote on connection %s: %s", connStr, err.Error())
			myCancel()
			return
		}
	}()

	select {
	case <-myCtx.Done():
		myCancel()
		log.Printf("SSH tunnel CLOSE")
	}
	// // done gets set by the customer when we have
	// // all the ctx cancelling or exception capturing.
	// done := make(chan bool)
	// go func() {
	// 	tunnel.pipe(tunnel.ctx, localConn, remoteConn, done)
	// 	fmt.Println("done with pipe")
	// 	done <- true
	// }()

	// select {
	// case <-done:
	// 	fmt.Println("draining done")
	// 	break
	// }

	fmt.Println("$$$ returning from forward $$$")
}

func (tunnel *ImportedSSHTunnel) pipe(
	ctx context.Context,
	local net.Conn,
	remote net.Conn,
	done chan bool) {

	fmt.Println("Starting with the pipe of local to remote")

	var (
		wg sync.WaitGroup
	)

	ctxLR, cancelLR := context.WithCancel(ctx)
	errCopyLR := make(chan error)
	go func() {
		wg.Add(1)
		_, err := io.Copy(local, remote)
		if err != nil {
			fmt.Printf("copying from local to remote: %s\n", err)
		}
		errCopyLR <- err
	}()

	ctxRL, cancelRL := context.WithCancel(ctx)
	errCopyRL := make(chan error)
	go func() {
		wg.Add(1)
		_, err := io.Copy(remote, local)
		if err != nil {
			fmt.Printf("copying from remote to local: %s\n", err)
		}
		errCopyRL <- err
	}()

	go func() {

		defer func() {
			fmt.Println("done and cancelLR")
			cancelLR()
			wg.Done()
		}()

		select {
		case <-ctxLR.Done():
			fmt.Println("called done on the ctxLR")
		case <-errCopyLR:
		}
	}()

	go func() {
		defer func() {
			fmt.Println("done and cancelRL")

			cancelRL()
			wg.Done()
		}()

		select {
		case <-ctxRL.Done():
			fmt.Println("called done on the ctxRL")
		case <-errCopyRL:
		}
	}()

	wg.Wait()
}

func (tunnel *ImportedSSHTunnel) Close() {
	tunnel.logf("closing the connection")
	tunnel.cancel()
}

// NewSSHTunnel creates a new single-use tunnel. Supplying "0" for localport will use a random port.
func NewImportedSSHTunnel(
	tunnel string,
	auth ssh.AuthMethod,
	destination string,
	localport string) *ImportedSSHTunnel {
	fmt.Printf("Started with creating a new imported ssh tunnel, dest: %s, local: %s\n", destination, tunnel)

	localEndpoint := NewEndpoint("localhost:" + localport)

	server := NewEndpoint(tunnel)
	if server.Port == 0 {
		server.Port = 22
	}

	sshTunnel := &ImportedSSHTunnel{
		Config: &ssh.ClientConfig{
			User: server.User,
			Auth: []ssh.AuthMethod{auth},
			HostKeyCallback: func(hostname string, remote net.Addr, key ssh.PublicKey) error {
				// Always accept key.
				return nil
			},
		},
		Local:  localEndpoint,
		Server: server,
		Remote: NewEndpoint(destination),
		close:  make(chan interface{}),
	}

	return sshTunnel
}
