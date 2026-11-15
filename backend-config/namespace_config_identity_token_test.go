package backendconfig

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

func TestNamespaceIdentityToken(t *testing.T) {
	ctx := context.Background()
	const configPath = "/data-plane/v1/namespaces/a-namespace/config"

	writeToken := func(t *testing.T, path, token string) {
		t.Helper()
		require.NoError(t, os.WriteFile(path, []byte(token), 0o600))
	}

	// headerRecorder answers v1 config requests and records the identity header of each one
	type headerRecorder struct {
		mu      sync.Mutex
		headers []string
		before  func(n int, w http.ResponseWriter) bool // returns true when it already answered
	}
	record := func(rec *headerRecorder) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			rec.mu.Lock()
			rec.headers = append(rec.headers, r.Header.Get(identityTokenHeader))
			n := len(rec.headers)
			rec.mu.Unlock()
			if rec.before != nil && rec.before(n, w) {
				return
			}
			_, _ = w.Write([]byte(`{}`))
		}
	}
	newV1 := func(t *testing.T, serverURL, tokenFile string) *namespaceConfig {
		t.Helper()
		u, err := url.Parse(serverURL)
		require.NoError(t, err)
		conf := config.New()
		if tokenFile != "" {
			conf.Set("DATAPLANE_IDENTITY_TOKEN_FILE", tokenFile)
		}
		nc := &namespaceConfig{
			config:              conf,
			logger:              logger.NOP,
			stats:               stats.NOP,
			configBackendURL:    u,
			namespace:           "a-namespace",
			hostedServiceSecret: "service-secret",
		}
		require.NoError(t, nc.SetUp())
		return nc
	}

	t.Run("setup reads the token file path from DATAPLANE_IDENTITY_TOKEN_FILE", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "token")
		nc := newV1(t, "http://localhost", path)
		require.Equal(t, path, nc.identityTokenFile)
		require.NotNil(t, nc.client.CheckRedirect)

		nc = newV1(t, "http://localhost", "")
		require.Empty(t, nc.identityTokenFile)
		require.Nil(t, nc.client.CheckRedirect, "nothing changes when the feature is off")
	})

	t.Run("v1 reads the token on every attempt and omits it when the file is missing", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "token")
		writeToken(t, path, "token-1\n")
		rec := &headerRecorder{before: func(n int, w http.ResponseWriter) bool {
			if n == 1 { // rotate the token and fail, so the retry has to read it again
				writeToken(t, path, "token-2")
				w.WriteHeader(http.StatusInternalServerError)
				return true
			}
			return false
		}}
		ts := httptest.NewServer(record(rec))
		defer ts.Close()
		nc := newV1(t, ts.URL, path)

		_, err := nc.Get(ctx)
		require.NoError(t, err)
		require.Equal(t, []string{"token-1", "token-2"}, rec.headers)

		writeToken(t, path, "token-3")
		_, err = nc.Get(ctx)
		require.NoError(t, err)
		require.Equal(t, "token-3", rec.headers[2], "the next poll picks up the rotated token")

		require.NoError(t, os.Remove(path))
		_, err = nc.Get(ctx)
		require.NoError(t, err)
		require.Empty(t, rec.headers[3])
	})

	t.Run("v1 sends no header when the feature is off", func(t *testing.T) {
		rec := &headerRecorder{}
		ts := httptest.NewServer(record(rec))
		defer ts.Close()

		_, err := newV1(t, ts.URL, "").Get(ctx)
		require.NoError(t, err)
		require.Equal(t, []string{""}, rec.headers)
	})

	t.Run("a redirect to another host drops the header, one to the same host keeps it", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "token")
		writeToken(t, path, "a-token")

		other := &headerRecorder{}
		otherHost := httptest.NewServer(record(other))
		defer otherHost.Close()

		same := &headerRecorder{}
		sameHost := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Query().Get("hop") {
			case "":
				http.Redirect(w, r, configPath+"?hop=same", http.StatusFound)
			case "same":
				record(same)(w, r)
			}
		}))
		defer sameHost.Close()

		_, err := newV1(t, sameHost.URL, path).Get(ctx)
		require.NoError(t, err)
		require.Equal(t, []string{"a-token"}, same.headers)

		redirecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, otherHost.URL+configPath, http.StatusFound)
		}))
		defer redirecting.Close()

		_, err = newV1(t, redirecting.URL, path).Get(ctx)
		require.NoError(t, err)
		require.Equal(t, []string{""}, other.headers)
	})

	t.Run("v2 sends the token when the file is set", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "token")
		writeToken(t, path, " a-token\n")
		srv := &v2Server{t: t, secret: "service-secret"}
		srv.responses = [][]byte{v2Doc{Workspaces: map[string]any{"ws-1": v2Workspaceof("2026-07-24T17:01:55.777Z")}}.json(t)}
		fetcher, _ := newV2TestFetcher(t, srv, func(nc *namespaceConfig) { nc.identityTokenFile = path })

		_, err := fetcher.Get(ctx)
		require.NoError(t, err)
		require.Equal(t, "a-token", srv.headers[0].Get(identityTokenHeader))
	})

	t.Run("v2 sends no header when the file is not set", func(t *testing.T) {
		srv := &v2Server{t: t, secret: "service-secret"}
		srv.responses = [][]byte{v2Doc{Workspaces: map[string]any{"ws-1": v2Workspaceof("2026-07-24T17:01:55.777Z")}}.json(t)}
		fetcher, _ := newV2TestFetcher(t, srv)

		_, err := fetcher.Get(ctx)
		require.NoError(t, err)
		require.NotContains(t, srv.headers[0], identityTokenHeader)
	})
}
