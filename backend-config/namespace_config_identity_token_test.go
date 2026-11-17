package backendconfig

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/logger/mock_logger"
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
		rec := &headerRecorder{respond: func(n int, w http.ResponseWriter) bool {
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

		writeToken(t, path, "token-3")
		_, err = nc.Get(ctx)
		require.NoError(t, err)
		require.Equal(t, "Bearer token-3", rec.headers[5], "failed reads do not restart the minute, so the next good read is used at once")
	})

	t.Run("an unreadable file is logged once until it is read again", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "token")
		log := mock_logger.NewMockLogger(gomock.NewController(t))
		log.EXPECT().Warnn("Cannot read the identity token file", logger.NewStringField("path", path), gomock.Any()).Times(2)
		now := time.Now()
		tok := &cachedToken{now: func() time.Time { return now }}

		require.Empty(t, tok.get(path, log), "missing: warns")
		require.Empty(t, tok.get(path, log), "still missing: no warning")
		writeToken(t, path, "token-1")
		require.Equal(t, "token-1", tok.get(path, log))
		require.NoError(t, os.Remove(path))
		now = now.Add(identityTokenMaxAge)
		require.Equal(t, "token-1", tok.get(path, log), "missing again: warns and keeps the last token")
		require.Equal(t, "token-1", tok.get(path, log), "still missing: no warning")
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

	t.Run("a redirect keeps the credential only on the original scheme, host and port", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "token")
		writeToken(t, path, "a-token")

		// the first request is redirected to redirectTo, the redirected one is recorded and answered with response
		var redirectTo string
		var response []byte
		rec := &headerRecorder{respond: func(_ int, w http.ResponseWriter) bool {
			_, _ = w.Write(response)
			return true
		}}
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/redirected" {
				http.Redirect(w, r, redirectTo, http.StatusFound)
				return
			}
			record(rec)(w, r)
		})
		plain := httptest.NewServer(handler)
		defer plain.Close()
		secure := httptest.NewTLSServer(handler)
		defer secure.Close()
		// whatever host and port a URL names, plain connections reach plain and TLS ones reach secure,
		// so each redirect below changes one thing only
		transport := &http.Transport{
			DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, plain.Listener.Addr().String())
			},
			DialTLSContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				d := &tls.Dialer{Config: secure.Client().Transport.(*http.Transport).TLSClientConfig}
				return d.DialContext(ctx, network, secure.Listener.Addr().String())
			},
		}

		versions := []struct {
			name     string
			response []byte
			get      func(t *testing.T, start *url.URL) error
		}{
			{"v1", []byte(`{}`), func(t *testing.T, start *url.URL) error {
				nc := newV1(t, start.String(), path)
				nc.client.Transport = transport
				_, err := nc.Get(ctx)
				return err
			}},
			{"v2", v2Doc{Workspaces: map[string]any{"ws-1": v2Workspaceof("2026-07-24T17:01:55.777Z")}}.json(t), func(t *testing.T, start *url.URL) error {
				var nc *namespaceConfig
				fetcher, _ := newV2TestFetcher(t, &v2Server{t: t, secret: "service-secret"}, func(c *namespaceConfig) {
					nc, c.client, c.configBackendURL, c.identityTokenFile = c, nil, start, path // nil: the client is built by SetUp
				})
				nc.client.Transport = transport
				_, err := fetcher.Get(ctx)
				return err
			}},
		}
		for _, v := range versions {
			for _, tc := range []struct{ name, start, target, want string }{
				{"same origin", "http://example.com:8080", "http://example.com:8080/redirected", "Bearer a-token"},
				{"another host", "http://example.com:8080", "http://other.test:8080/redirected", ""},
				{"https to http on the same host and port", "https://example.com:8443", "http://example.com:8443/redirected", ""},
				{"same host on another port", "http://example.com:8080", "http://example.com:8081/redirected", ""},
				{"a subdomain", "http://example.com:8080", "http://sub.example.com:8080/redirected", ""},
			} {
				t.Run(v.name+" "+tc.name, func(t *testing.T) {
					start, err := url.Parse(tc.start)
					require.NoError(t, err)
					rec.headers, redirectTo, response = nil, tc.target, v.response
					require.NoError(t, v.get(t, start))
					require.Equal(t, []string{tc.want}, rec.headers)
				})
			}
		}
		require.EqualError(t, newV1(t, "http://example.com", "").client.CheckRedirect(nil, make([]*http.Request, 10)),
			"stopped after 10 redirects", "net/http's limit is kept")
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
