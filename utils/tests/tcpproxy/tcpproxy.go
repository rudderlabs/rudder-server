package tcpproxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
)

type logger interface {
	Log(args ...interface{})
}

func Start(ctx context.Context, localAddr, remoteAddr string, l logger) (interface{ Wait() }, error) {
	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", localAddr)
	if err != nil {
		return nil, fmt.Errorf("could not listen on tcp/%s", localAddr)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		_ = listener.Close()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				conn, err := listener.Accept()
				if err != nil {
					l.Log("Error accepting connection", err)
					continue
				}

				wg.Add(1)
				go func(remoteAddr string) {
					defer wg.Done()
					defer func() { _ = conn.Close() }()

					conn2, err := net.Dial("tcp", remoteAddr)
					if err != nil {
						l.Log("Error dialing remote addr", err)
						return
					}

					defer func() { _ = conn2.Close() }()

					closer := make(chan struct{}, 2)

					wg.Add(2)
					go proxyData(closer, conn2, conn, wg)
					go proxyData(closer, conn, conn2, wg)

					select {
					case <-closer: // one of the connections got terminated
					case <-ctx.Done(): // TCP proxy stopped
					}

					l.Log("Connection complete", remoteAddr)
				}(remoteAddr)
			}
		}
	}()

	return wg, nil
}

func proxyData(closer chan struct{}, dst io.Writer, src io.Reader, wg *sync.WaitGroup) {
	defer wg.Done()
	if testing.Verbose() {
		_, _ = io.Copy(os.Stdout, io.TeeReader(src, dst))
	} else {
		_, _ = io.Copy(dst, src)
	}
	closer <- struct{}{} // connection is closed, send signal to stop proxy
}
