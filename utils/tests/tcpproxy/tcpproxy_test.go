package tcpproxy

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/phayes/freeport"
	"github.com/stretchr/testify/require"
)

func TestProxy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("OK"))
	}))
	t.Cleanup(srv.Close)

	parsedURL, err := url.Parse(srv.URL)
	require.NoError(t, err)

	proxyPort, err := freeport.GetFreePort()
	require.NoError(t, err)

	var (
		proxyEndpoint     = fmt.Sprintf("localhost:%d", proxyPort)
		httpSrvEndpoint   = fmt.Sprintf("localhost:%s", parsedURL.Port())
		proxyHTTPEndpoint = "http://" + proxyEndpoint
	)

	ctx, stopProxy := context.WithCancel(context.Background())
	proxyWg, err := Start(ctx, proxyEndpoint, httpSrvEndpoint, t)
	require.NoError(t, err)
	t.Cleanup(proxyWg.Wait)
	t.Cleanup(stopProxy)

	httpClient := srv.Client()
	httpClient.Timeout = time.Second

	// test that the HTTP server works by connecting directly to it
	t.Run("http server works", func(t *testing.T) {
		resp, err := httpClient.Get(srv.URL)
		require.NoError(t, err)
		require.EqualValues(t, http.StatusOK, resp.StatusCode)

		body, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
		require.EqualValues(t, "OK", body)
	})

	// test that the proxy works by connecting via the proxy
	t.Run("proxy works", func(t *testing.T) {
		resp, err := httpClient.Get(proxyHTTPEndpoint)
		require.NoError(t, err)
		require.EqualValues(t, http.StatusOK, resp.StatusCode)

		body, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
		require.EqualValues(t, "OK", body)
	})

	// test that the proxy closure works by terminating and trying to connect again afterwards
	t.Run("proxy closure", func(t *testing.T) {
		stopProxy()
		proxyWg.Wait()

		_, err := httpClient.Get(proxyHTTPEndpoint)
		require.Error(t, err)
	})
}
