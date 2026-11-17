package backendconfig

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

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

	// headerRecorder answers v1 config requests and records the credential of each one:
	// "basic:<user>" for Basic auth, otherwise the raw Authorization header
	type headerRecorder struct {
		mu      sync.Mutex
		headers []string
		before  func(n int, w http.ResponseWriter) bool // returns true when it already answered
	}
	record := func(rec *headerRecorder) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			rec.mu.Lock()
			auth := r.Header.Get("Authorization")
			if user, _, ok := r.BasicAuth(); ok {
				auth = "basic:" + user
			}
			rec.headers = append(rec.headers, auth)
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

		nc = newV1(t, "http://localhost", "")
		require.Empty(t, nc.identityTokenFile)
	})

	t.Run("v1 sends the token instead of the secret and reuses it for a minute, retries included", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "token")
		writeToken(t, path, "token-1\n")
		rec := &headerRecorder{before: func(n int, w http.ResponseWriter) bool {
			if n == 1 { // rotate the token and fail: the retry still reuses the cached one
				writeToken(t, path, "token-2")
				w.WriteHeader(http.StatusInternalServerError)
				return true
			}
			return false
		}}
		ts := httptest.NewServer(record(rec))
		defer ts.Close()
		nc := newV1(t, ts.URL, path)
		now := time.Now()
		nc.fetcher.(*v1ConfigFetcher).identityToken.now = func() time.Time { return now }

		_, err := nc.Get(ctx)
		require.NoError(t, err)
		require.Equal(t, []string{"Bearer token-1", "Bearer token-1"}, rec.headers)

		now = now.Add(identityTokenMaxAge)
		_, err = nc.Get(ctx)
		require.NoError(t, err)
		require.Equal(t, "Bearer token-2", rec.headers[2], "a minute later the rotated token is read")

		writeToken(t, path, " \n")
		now = now.Add(identityTokenMaxAge)
		_, err = nc.Get(ctx)
		require.NoError(t, err)
		require.Equal(t, "Bearer token-2", rec.headers[3], "an empty file keeps the last token read")

		require.NoError(t, os.Remove(path))
		_, err = nc.Get(ctx)
		require.NoError(t, err)
		require.Equal(t, "Bearer token-2", rec.headers[4], "a missing file keeps the last token read")
	})

	t.Run("v1 sends the secret while the file is missing or empty", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "token")
		rec := &headerRecorder{}
		ts := httptest.NewServer(record(rec))
		defer ts.Close()
		nc := newV1(t, ts.URL, path)

		_, err := nc.Get(ctx)
		require.NoError(t, err)
		writeToken(t, path, " \n")
		_, err = nc.Get(ctx)
		require.NoError(t, err)
		writeToken(t, path, "token-1")
		_, err = nc.Get(ctx)
		require.NoError(t, err)
		require.Equal(t, []string{"basic:service-secret", "basic:service-secret", "Bearer token-1"}, rec.headers,
			"no token is cached until a read succeeds, so the first good read is used at once")
	})

	t.Run("v1 sends the secret when the feature is off", func(t *testing.T) {
		rec := &headerRecorder{}
		ts := httptest.NewServer(record(rec))
		defer ts.Close()

		_, err := newV1(t, ts.URL, "").Get(ctx)
		require.NoError(t, err)
		require.Equal(t, []string{"basic:service-secret"}, rec.headers)
	})

	t.Run("a redirect to another host drops the token, one to the same host keeps it", func(t *testing.T) {
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
		require.Equal(t, []string{"Bearer a-token"}, same.headers)

		redirecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// net/http compares hosts without ports, so use a different host name for the same listener
			http.Redirect(w, r, strings.Replace(otherHost.URL, "127.0.0.1", "localhost", 1)+configPath, http.StatusFound)
		}))
		defer redirecting.Close()

		_, err = newV1(t, redirecting.URL, path).Get(ctx)
		require.NoError(t, err)
		require.Equal(t, []string{""}, other.headers)
	})

	t.Run("v2 sends the token instead of the secret when the file is set", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "token")
		writeToken(t, path, " a-token\n")
		srv := &v2Server{t: t, secret: "service-secret", bearer: "a-token"}
		srv.responses = [][]byte{v2Doc{Workspaces: map[string]any{"ws-1": v2Workspaceof("2026-07-24T17:01:55.777Z")}}.json(t)}
		fetcher, _ := newV2TestFetcher(t, srv, func(nc *namespaceConfig) { nc.identityTokenFile = path })

		_, err := fetcher.Get(ctx)
		require.NoError(t, err)
		require.Len(t, srv.headers, 1)
	})

	t.Run("v2 sends the secret when the file is not set", func(t *testing.T) {
		srv := &v2Server{t: t, secret: "service-secret"}
		srv.responses = [][]byte{v2Doc{Workspaces: map[string]any{"ws-1": v2Workspaceof("2026-07-24T17:01:55.777Z")}}.json(t)}
		fetcher, _ := newV2TestFetcher(t, srv)

		_, err := fetcher.Get(ctx)
		require.NoError(t, err)
		require.Len(t, srv.headers, 1) // v2Server requires the Basic secret
	})
}
