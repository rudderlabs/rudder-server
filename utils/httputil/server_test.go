package httputil

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/phayes/freeport"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func Test_ListenAndServe(t *testing.T) {
	t.Run("no error when context gets canceled", func(t *testing.T) {
		srv := &http.Server{
			ReadHeaderTimeout: time.Second,
			Addr:              fmt.Sprintf(":%d", freeport.GetPort()),
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := ListenAndServe(ctx, srv)
		require.NoError(t, err)
	})

	t.Run("expected http errors", func(t *testing.T) {
		srv1 := httptest.NewServer(nil)
		defer srv1.Close()

		t.Log("running server on the same port")
		srv2 := &http.Server{
			ReadHeaderTimeout: time.Second,
			Addr:              strings.TrimPrefix(srv1.URL, "http://"),
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err := ListenAndServe(ctx, srv2)
		require.ErrorContains(t, err, "bind: address already in use")
	})

	t.Run("block until all connections are closed", func(t *testing.T) {
		unblockMsg := "UNBLOCKED"
		blocker := make(chan struct{})
		firstCall := make(chan struct{})

		addr := fmt.Sprintf("127.0.0.1:%d", freeport.GetPort())

		srv := &http.Server{
			ReadHeaderTimeout: time.Second,
			Addr:              addr,
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Log(r.URL)
				switch r.URL.Path {
				case "/ping":
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte("OK"))
				case "/block":
					close(firstCall)
					<-blocker
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(unblockMsg))
				}
			}),
		}
		ctx, cancel := context.WithCancel(context.Background())

		g, _ := errgroup.WithContext(context.Background())
		g.Go(func() error {
			return ListenAndServe(ctx, srv)
		})

		g.Go(func() error {
			require.Eventually(t, func() bool {
				return pingServer(srv)
			}, time.Second, time.Millisecond)

			client := http.Client{}
			resp, err := client.Get(fmt.Sprintf("http://%s/block", addr))
			if err != nil {
				return err
			}
			b, err := io.ReadAll(resp.Body)
			if err != nil {
				return err
			}
			if string(b) != unblockMsg {
				return fmt.Errorf("unexpected payload: %s", b)
			}

			return resp.Body.Close()
		})

		t.Log("wait for the first blocking call")
		require.Eventually(t, func() bool {
			<-firstCall
			return true
		}, time.Second, time.Millisecond)

		t.Log("shutdown server")
		cancel()

		t.Log("server should not accept new connections")
		require.Eventually(t, func() bool {
			return !pingServer(srv)
		}, time.Second, time.Millisecond)

		t.Log("unblock connection")
		close(blocker)

		err := g.Wait()
		require.NoError(t, err, "both server and client should with no error")
	})

	t.Run("timeout if connections are not closed", func(t *testing.T) {
		addr := fmt.Sprintf("127.0.0.1:%d", freeport.GetPort())
		firstCall := make(chan struct{})

		srv := &http.Server{
			ReadHeaderTimeout: time.Second,

			Addr: addr,
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Log(r.URL)
				switch r.URL.Path {
				case "/ping":
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte("OK"))
				case "/block":
					close(firstCall)
					<-make(chan struct{})
				}
			}),
		}
		srvCtx, cancelSrv := context.WithCancel(context.Background())
		srvErrCh := make(chan error)

		clientCtx, cancelClient := context.WithCancel(context.Background())
		clientErrCh := make(chan error)

		go func() {
			srvErrCh <- ListenAndServe(srvCtx, srv, time.Millisecond)
		}()

		go func() {
			require.Eventually(t, func() bool {
				return pingServer(srv)
			}, time.Second, time.Millisecond)

			client := http.Client{}

			req, _ := http.NewRequestWithContext(clientCtx, http.MethodGet, fmt.Sprintf("http://%s/block", addr), http.NoBody)

			resp, err := client.Do(req)
			if err != nil {
				clientErrCh <- err
				return
			}
			clientErrCh <- resp.Body.Close()
		}()

		t.Log("wait for the first blocking call")
		require.Eventually(t, func() bool {
			<-firstCall
			return true
		}, time.Second, time.Millisecond)

		t.Log("shutdown server")
		cancelSrv()
		require.ErrorIs(t, <-srvErrCh, context.DeadlineExceeded)

		cancelClient()
		require.ErrorIs(t, <-clientErrCh, context.Canceled)
	})
}

func Test_Serve(t *testing.T) {
	t.Run("no error when context gets canceled", func(t *testing.T) {
		srv := &http.Server{
			ReadHeaderTimeout: time.Second,
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		dir, err := os.MkdirTemp("", "test-graceful-serve")
		require.NoError(t, err)

		l, err := net.Listen("unix", dir+"/unix.socket")
		require.NoError(t, err)
		defer l.Close()

		require.NoError(t, Serve(ctx, srv, l, time.Second))
	})
}

func pingServer(srv *http.Server) bool {
	client := http.Client{}
	resp, err := client.Get(fmt.Sprintf("http://%s/ping", srv.Addr))
	if err != nil {
		return false
	}
	resp.Body.Close()

	return (resp.StatusCode == http.StatusOK)
}
