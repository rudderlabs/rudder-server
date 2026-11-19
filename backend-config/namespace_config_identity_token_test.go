package backendconfig

import (
	"context"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-cp-sdk/tokenfile"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

func TestNamespaceIdentityToken(t *testing.T) {
	ctx := context.Background()

	writeToken := func(t *testing.T, path, token string) {
		t.Helper()
		require.NoError(t, os.WriteFile(path, []byte(token), 0o600))
	}

	// headerRecorder answers v1 config requests and records the credential of each one:
	// "basic:<user>" for Basic auth, otherwise the raw Authorization header
	type headerRecorder struct {
		mu      sync.Mutex
		headers []string
		respond func(n int, w http.ResponseWriter) bool // optional: returns true when it answered request n itself
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
			if rec.respond != nil && rec.respond(n, w) {
				return
			}
			_, _ = w.Write([]byte(`{}`))
		}
	}
	setUp := func(serverURL, tokenFile string) (*namespaceConfig, error) {
		u, err := url.Parse(serverURL)
		if err != nil {
			return nil, err
		}
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
		return nc, nc.SetUp()
	}
	newV1 := func(t *testing.T, serverURL, tokenFile string) *namespaceConfig {
		t.Helper()
		nc, err := setUp(serverURL, tokenFile)
		require.NoError(t, err)
		return nc
	}

	t.Run("setup fails when the token file is set but missing or empty", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "token")
		_, err := setUp("http://localhost", path)
		require.ErrorIs(t, err, fs.ErrNotExist)

		writeToken(t, path, " \n")
		_, err = setUp("http://localhost", path)
		require.Error(t, err)
	})

	t.Run("a token set before setup is kept over the config and loaded", func(t *testing.T) {
		dir := t.TempDir()
		preset, configured := filepath.Join(dir, "preset"), filepath.Join(dir, "configured")
		writeToken(t, configured, "configured-token")
		conf := config.New()
		conf.Set("DATAPLANE_IDENTITY_TOKEN_FILE", configured)
		nc := &namespaceConfig{
			config:              conf,
			logger:              logger.NOP,
			stats:               stats.NOP,
			namespace:           "a-namespace",
			hostedServiceSecret: "service-secret",
			identityToken:       tokenfile.New(preset),
		}
		require.ErrorIs(t, nc.SetUp(), fs.ErrNotExist, "a bad preset token fails setup")

		writeToken(t, preset, "preset-token")
		require.NoError(t, nc.SetUp())
		require.Equal(t, "preset-token", nc.identityToken.Get())
	})

	t.Run("v1 sends the token instead of the secret, retries included", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "token")
		writeToken(t, path, "token-1\n")
		rec := &headerRecorder{respond: func(n int, w http.ResponseWriter) bool {
			if n == 1 { // rotate the token and fail: the retry still reuses the token read at setup
				writeToken(t, path, "token-2")
				w.WriteHeader(http.StatusInternalServerError)
				return true
			}
			return false
		}}
		ts := httptest.NewServer(record(rec))
		defer ts.Close()

		_, err := newV1(t, ts.URL, path).Get(ctx)
		require.NoError(t, err)
		require.Equal(t, []string{"Bearer token-1", "Bearer token-1"}, rec.headers)
	})

	t.Run("v1 and v2 share one token", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "token")
		writeToken(t, path, "token-1")
		rec := &headerRecorder{}
		ts := httptest.NewServer(record(rec))
		defer ts.Close()
		nc := newV1(t, ts.URL, path)
		_, err := nc.Get(ctx)
		require.NoError(t, err)

		writeToken(t, path, "token-2")
		_, err = newV2ConfigFetcher(nc) // the control plane client loads the token it is given
		require.NoError(t, err)
		_, err = nc.Get(ctx)
		require.NoError(t, err)
		require.Equal(t, []string{"Bearer token-1", "Bearer token-2"}, rec.headers, "v1 sees the token v2 loaded")
	})

	t.Run("v1 sends the secret when the feature is off", func(t *testing.T) {
		rec := &headerRecorder{}
		ts := httptest.NewServer(record(rec))
		defer ts.Close()

		_, err := newV1(t, ts.URL, "").Get(ctx)
		require.NoError(t, err)
		require.Equal(t, []string{"basic:service-secret"}, rec.headers)
	})

	t.Run("v2 sends the token instead of the secret when the file is set", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "token")
		writeToken(t, path, " a-token\n")
		srv := &v2Server{t: t, secret: "service-secret", bearer: "a-token"}
		srv.responses = [][]byte{v2Doc{Workspaces: map[string]any{"ws-1": v2Workspaceof("2026-07-24T17:01:55.777Z")}}.json(t)}
		fetcher, _ := newV2TestFetcher(t, srv, func(nc *namespaceConfig) { nc.config.Set("DATAPLANE_IDENTITY_TOKEN_FILE", path) })

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
