package tcpproxy

import (
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type Proxy struct {
	LocalAddr     string
	RemoteAddr    string
	BytesSent     atomic.Int64
	BytesReceived atomic.Int64
	Verbose       bool

	wg   sync.WaitGroup
	stop chan struct{}
}

func (p *Proxy) Start(t testing.TB) {
	p.wg.Add(1)
	defer p.wg.Done()

	listener, err := net.Listen("tcp", p.LocalAddr)
	require.NoError(t, err)

	p.stop = make(chan struct{})
	p.wg.Add(1)
	go func() {
		<-p.stop
		_ = listener.Close()
		p.wg.Done()
	}()

	for {
		select {
		case <-p.stop:
			return

		default:
			connRcv, err := listener.Accept()
			if err != nil {
				continue // error accepting connection
			}

			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				defer func() { _ = connRcv.Close() }()

				connSend, err := net.Dial("tcp", p.RemoteAddr)
				if err != nil {
					t.Logf("Cannot dial remote: %v", err)
					return // cannot dial remote, return and listen for new connections
				}

				defer func() { _ = connSend.Close() }()

				p.wg.Add(2)
				done := make(chan struct{}, 2)
				go p.pipe(connRcv, connSend, &p.BytesReceived, done)
				go p.pipe(connSend, connRcv, &p.BytesSent, done)
				select {
				case <-done: // one of the connections got terminated
				case <-p.stop: // TCP proxy stopped
				}
			}()
		}
	}
}

func (p *Proxy) Stop() {
	close(p.stop)
	p.wg.Wait()
}

func (p *Proxy) pipe(src io.Reader, dst io.Writer, bytesMetric *atomic.Int64, done chan struct{}) {
	defer p.wg.Done()

	wrt, rdr := dst, src
	if p.Verbose {
		wrt = os.Stdout
		rdr = io.TeeReader(src, dst)
	}
	n, _ := io.Copy(wrt, rdr) // this is a blocking call, it terminates when the connection is closed
	bytesMetric.Add(n)

	done <- struct{}{} // connection is closed, send signal to stop proxy
}
