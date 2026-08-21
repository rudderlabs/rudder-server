package backendconfig

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

func TestV2ConfigFetcher(t *testing.T) {
	t.Run("get", func(t *testing.T) {
		ctx := context.Background()

		t.Run("the request carries secrets=embed and the updatedAfter format the gateway validates", func(t *testing.T) {
			srv := &v2Server{t: t, secret: "service-secret"}
			srv.responses = [][]byte{
				v2Doc{Workspaces: map[string]any{"ws-1": v2Workspaceof("2026-07-24T17:01:55.777Z")}}.json(t),
				v2Doc{Workspaces: map[string]any{"ws-1": nil}}.json(t),
			}
			fetcher, _ := newV2TestFetcher(t, srv)

			_, err := fetcher.Get(ctx)
			require.NoError(t, err)
			require.Equal(t, "embed", srv.lastQuery().Get("secrets"))
			require.NotContains(t, srv.lastQuery(), "updatedAfter", "nothing has been fetched yet")

			_, err = fetcher.Get(ctx)
			require.NoError(t, err)
			require.Equal(t, "embed", srv.lastQuery().Get("secrets"))
			updatedAfter := srv.lastQuery().Get("updatedAfter")
			require.Equal(t, "2026-07-24T17:01:55.777Z", updatedAfter)
			_, err = time.Parse(updatedAfterTimeFormat, updatedAfter)
			require.NoError(t, err, "the control plane validates this format strictly")
		})

		t.Run("the workspaces are mapped and stamped with the connection flags", func(t *testing.T) {
			srv := &v2Server{t: t, secret: "service-secret"}
			srv.responses = [][]byte{v2Doc{Workspaces: map[string]any{
				"ws-1": v2Workspaceof("2026-07-24T17:01:55.777Z"),
				"ws-2": v2Workspaceof("2026-07-20T10:00:00.000Z"),
			}}.json(t)}
			fetcher, _ := newV2TestFetcher(t, srv)

			configs, err := fetcher.Get(ctx)
			require.NoError(t, err)
			require.Len(t, configs, 2)
			for workspaceID, config := range configs {
				require.Equal(t, workspaceID, config.WorkspaceID)
				require.Equal(t, "mockCpRouterURL", config.ConnectionFlags.URL)
				require.True(t, config.ConnectionFlags.Services["warehouse"])
				require.True(t, config.ConnectionFlags.Services["rudderstack-processor"])
			}
			// the newest workspace drives the next updatedAfter
			require.Equal(t, "2026-07-24T17:01:55.777Z", fetcher.lastUpdatedAt.Format(updatedAfterTimeFormat))
		})

		t.Run("a workspace removed on a poll where every survivor is null does not linger", func(t *testing.T) {
			srv := &v2Server{t: t, secret: "service-secret"}
			srv.responses = [][]byte{
				v2Doc{Workspaces: map[string]any{
					"ws-1": v2Workspaceof("2026-07-24T17:01:55.777Z"),
					"ws-2": v2Workspaceof("2026-07-20T10:00:00.000Z"),
				}}.json(t),
				// ws-2 is gone and ws-1 did not change
				v2Doc{Workspaces: map[string]any{"ws-1": nil}}.json(t),
			}
			fetcher, _ := newV2TestFetcher(t, srv)

			configs, err := fetcher.Get(ctx)
			require.NoError(t, err)
			require.Len(t, configs, 2)

			// a workspace the cache holds on to, standing in for any cache entry the differ does not
			// drop: membership is read off the response, so it must not reach the result either way
			fetcher.cache.Workspaces["ws-3"] = fetcher.cache.Workspaces["ws-2"]

			configs, err = fetcher.Get(ctx)
			require.NoError(t, err)
			require.Len(t, configs, 1)
			require.Contains(t, configs, "ws-1")
			require.NotContains(t, configs, "ws-2")
			require.NotContains(t, configs, "ws-3")
			require.NotContains(t, fetcher.memo, "ws-2", "the memo is rebuilt from the response too")
			require.NotContains(t, fetcher.memo, "ws-3")
		})

		t.Run("a workspace that was never seen before triggers a full fetch", func(t *testing.T) {
			srv := &v2Server{t: t, secret: "service-secret"}
			full := v2Doc{Workspaces: map[string]any{
				"ws-1": v2Workspaceof("2026-07-24T17:01:55.777Z"),
				"ws-2": v2Workspaceof("2026-07-25T10:00:00.000Z"),
			}}.json(t)
			srv.responses = [][]byte{
				v2Doc{Workspaces: map[string]any{"ws-1": v2Workspaceof("2026-07-24T17:01:55.777Z")}}.json(t),
				// ws-2 arrives null without ever having been sent in full
				v2Doc{Workspaces: map[string]any{"ws-1": nil, "ws-2": nil}}.json(t),
				full,
			}
			fetcher, _ := newV2TestFetcher(t, srv)

			_, err := fetcher.Get(ctx)
			require.NoError(t, err)

			configs, err := fetcher.Get(ctx)
			require.NoError(t, err, "the failed incremental update must be retried as a full one")
			require.Len(t, configs, 2)
			require.Len(t, srv.requests, 3)
			require.Empty(t, srv.requests[2].Query().Get("updatedAfter"), "the retry asks for everything")
		})
	})

	t.Run("rejects", func(t *testing.T) {
		ctx := context.Background()

		t.Run("a response from an endpoint that is not v2", func(t *testing.T) {
			srv := &v2Server{t: t, secret: "service-secret"}
			srv.responses = [][]byte{[]byte(`{"ws-1":{"sources":[],"updatedAt":"2026-07-24T17:01:55.777Z"}}`)}
			fetcher, _ := newV2TestFetcher(t, srv)

			_, err := fetcher.Get(ctx)
			require.ErrorContains(t, err, "unexpected config version 0")
		})
	})

	t.Run("memo", func(t *testing.T) {
		ctx := context.Background()
		unchanged := map[string]any{"ws-1": nil}

		for _, tc := range []struct {
			name      string
			second    v2Doc
			wantCalls int
		}{
			{
				name:      "hit on an unchanged workspace and an unchanged generation",
				second:    v2Doc{Workspaces: unchanged},
				wantCalls: 1,
			},
			{
				name:      "miss on a changed workspace",
				second:    v2Doc{Workspaces: map[string]any{"ws-1": v2Workspaceof("2026-07-25T17:01:55.777Z")}},
				wantCalls: 2,
			},
			{
				name: "miss on a definition edit with no workspace change",
				second: v2Doc{
					Workspaces:        unchanged,
					SourceDefinitions: map[string]any{"webhook": v2Definition("src-1", "2026-06-01T00:00:00.000Z")},
				},
				wantCalls: 2,
			},
			{
				name: "miss on a definition deletion, which bumps no timestamp",
				second: v2Doc{
					Workspaces:             unchanged,
					DestinationDefinitions: map[string]any{},
				},
				wantCalls: 2,
			},
			{
				name: "miss on a definition addition",
				second: v2Doc{
					Workspaces: unchanged,
					AccountDefinitions: map[string]any{
						"ACC":   v2Definition("acc-1", "2026-01-03T00:00:00.000Z"),
						"ACC-2": v2Definition("acc-2", "2026-01-03T00:00:00.000Z"),
					},
				},
				wantCalls: 2,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				srv := &v2Server{t: t, secret: "service-secret"}
				srv.responses = [][]byte{
					v2Doc{Workspaces: map[string]any{"ws-1": v2Workspaceof("2026-07-24T17:01:55.777Z")}}.json(t),
					tc.second.json(t),
				}
				fetcher, counting := newV2TestFetcher(t, srv)

				_, err := fetcher.Get(ctx)
				require.NoError(t, err)
				require.Equal(t, 1, counting.calls["ws-1"])

				configs, err := fetcher.Get(ctx)
				require.NoError(t, err)
				require.Len(t, configs, 1)
				require.Equal(t, tc.wantCalls, counting.calls["ws-1"])
			})
		}
	})

	t.Run("incremental updates disabled", func(t *testing.T) {
		// the production default: every poll asks for the whole config
		ctx := context.Background()
		srv := &v2Server{t: t, secret: "service-secret"}
		srv.responses = [][]byte{v2Doc{Workspaces: map[string]any{
			"ws-1": v2Workspaceof("2026-07-24T17:01:55.777Z"),
			"ws-2": v2Workspaceof("2026-07-20T10:00:00.000Z"),
		}}.json(t)}
		fetcher, counting := newV2TestFetcher(t, srv, func(nc *namespaceConfig) {
			nc.incrementalConfigUpdates = false
		})

		for range 3 {
			configs, err := fetcher.Get(ctx)
			require.NoError(t, err)
			require.Len(t, configs, 2)
		}
		require.Len(t, srv.requests, 3)
		for i, request := range srv.requests {
			require.Empty(t, request.Query().Get("updatedAfter"), "request %d must ask for everything", i)
			require.Equal(t, "embed", request.Query().Get("secrets"))
		}
		// nothing arrives as null, so every workspace counts as changed and is transformed every time
		require.Equal(t, 3, counting.calls["ws-1"])
		require.Equal(t, 3, counting.calls["ws-2"])
	})

	t.Run("updatedAfter across polls", func(t *testing.T) {
		ctx := context.Background()
		srv := &v2Server{t: t, secret: "service-secret"}
		srv.responses = [][]byte{
			v2Doc{Workspaces: map[string]any{"ws-1": v2Workspaceof("2026-07-24T17:01:55.777Z")}}.json(t),
			v2Doc{Workspaces: map[string]any{"ws-1": nil}}.json(t),
			v2Doc{Workspaces: map[string]any{"ws-1": nil}}.json(t),
		}
		fetcher, _ := newV2TestFetcher(t, srv)

		for range 3 {
			_, err := fetcher.Get(ctx)
			require.NoError(t, err)
		}
		// a poll in which nothing was updated leaves updatedAfter where it was: there is no way to
		// know the control plane's clock, so moving it forward would skip updates
		require.Equal(t, "2026-07-24T17:01:55.777Z", srv.requests[1].Query().Get("updatedAfter"))
		require.Equal(t, "2026-07-24T17:01:55.777Z", srv.requests[2].Query().Get("updatedAfter"))
	})

	t.Run("mixed workspaces in one poll", func(t *testing.T) {
		ctx := context.Background()
		firstPoll := v2Doc{Workspaces: map[string]any{
			"ws-1": v2Workspaceof("2026-07-24T17:01:55.777Z"),
			"ws-2": v2Workspaceof("2026-07-20T10:00:00.000Z"),
		}}.json(t)
		srv := &v2Server{t: t, secret: "service-secret"}
		srv.responses = [][]byte{
			firstPoll,
			// ws-1 changed, ws-2 did not: the memo has to discriminate within a single poll
			v2Doc{Workspaces: map[string]any{
				"ws-1": v2Workspaceof("2026-07-26T17:01:55.777Z"),
				"ws-2": nil,
			}}.json(t),
		}
		fetcher, counting := newV2TestFetcher(t, srv)

		_, err := fetcher.Get(ctx)
		require.NoError(t, err)
		configs, err := fetcher.Get(ctx)
		require.NoError(t, err)

		require.Len(t, configs, 2)
		require.Equal(t, 2, counting.calls["ws-1"], "the changed workspace is transformed again")
		require.Equal(t, 1, counting.calls["ws-2"], "the unchanged one is served from the memo")
		require.Equal(t, "2026-07-26T17:01:55.777Z", configs["ws-1"].UpdatedAt.Format(updatedAfterTimeFormat))
		require.Equal(t, "2026-07-20T10:00:00.000Z", configs["ws-2"].UpdatedAt.Format(updatedAfterTimeFormat))
	})

	t.Run("maps the cached body on a memo miss", func(t *testing.T) {
		// the resolver caches the raw workspace body, so a workspace that arrives as null can still be
		// transformed when something else - here a definition edit - invalidates its memo entry
		ctx := context.Background()
		srv := &v2Server{t: t, secret: "service-secret"}
		srv.responses = [][]byte{
			v2Doc{Workspaces: map[string]any{"ws-1": v2Workspaceof("2026-07-24T17:01:55.777Z")}}.json(t),
			v2Doc{
				Workspaces:        map[string]any{"ws-1": nil},
				SourceDefinitions: map[string]any{"webhook": v2Definition("src-1", "2026-06-01T00:00:00.000Z")},
			}.json(t),
		}
		fetcher, counting := newV2TestFetcher(t, srv)

		_, err := fetcher.Get(ctx)
		require.NoError(t, err)
		firstRaw := string(counting.raw["ws-1"])
		require.Contains(t, firstRaw, `"updatedAt":"2026-07-24T17:01:55.777Z"`)

		configs, err := fetcher.Get(ctx)
		require.NoError(t, err)
		require.Equal(t, 2, counting.calls["ws-1"])
		require.Equal(t, firstRaw, string(counting.raw["ws-1"]), "the cached body, not the null that arrived")
		require.Equal(t, "2026-07-24T17:01:55.777Z", configs["ws-1"].UpdatedAt.Format(updatedAfterTimeFormat))
		// and it is mapped against the edited catalogue
		require.Equal(t, "2026-06-01T00:00:00.000Z",
			fetcher.cache.SourceDefinitions["webhook"].UpdatedAt.Format(updatedAfterTimeFormat))
	})

	t.Run("a poll that fails to map leaves updatedAfter where it was", func(t *testing.T) {
		ctx := context.Background()
		srv := &v2Server{t: t, secret: "service-secret"}
		srv.responses = [][]byte{
			v2Doc{Workspaces: map[string]any{"ws-1": v2Workspaceof("2026-07-24T17:01:55.777Z")}}.json(t),
			v2Doc{Workspaces: map[string]any{"ws-1": v2Workspaceof("2026-07-26T10:00:00.000Z")}}.json(t),
			v2Doc{Workspaces: map[string]any{"ws-1": v2Workspaceof("2026-07-26T10:00:00.000Z")}}.json(t),
		}
		fetcher, counting := newV2TestFetcher(t, srv)

		_, err := fetcher.Get(ctx)
		require.NoError(t, err)

		// the update arrives but cannot be mapped
		counting.err = errors.New("mapper is having a day")
		_, err = fetcher.Get(ctx)
		require.ErrorContains(t, err, "mapper is having a day")

		// had updatedAfter moved, the control plane would answer the next poll with a null for a
		// workspace nothing has mapped yet, and the memo would keep serving the config it replaced
		counting.err = nil
		configs, err := fetcher.Get(ctx)
		require.NoError(t, err)
		require.Equal(t, "2026-07-24T17:01:55.777Z", srv.requests[2].Query().Get("updatedAfter"))
		require.Equal(t, "2026-07-26T10:00:00.000Z", configs["ws-1"].UpdatedAt.Format(updatedAfterTimeFormat))
	})

	t.Run("a response without workspaces", func(t *testing.T) {
		ctx := context.Background()

		t.Run("is not worth refetching when it already was a full response", func(t *testing.T) {
			srv := &v2Server{t: t, secret: "service-secret"}
			srv.responses = [][]byte{v2Doc{Workspaces: map[string]any{}}.json(t)}
			fetcher, _ := newV2TestFetcher(t, srv)

			_, err := fetcher.Get(ctx)
			require.ErrorContains(t, err, "no workspaces in response")
			require.NotErrorIs(t, err, ErrIncrementalUpdateFailed)
			require.Len(t, srv.requests, 1, "asking again would only repeat it")
		})

		t.Run("is retried in full when a delta was asked for", func(t *testing.T) {
			srv := &v2Server{t: t, secret: "service-secret"}
			srv.responses = [][]byte{
				v2Doc{Workspaces: map[string]any{"ws-1": v2Workspaceof("2026-07-24T17:01:55.777Z")}}.json(t),
				v2Doc{Workspaces: map[string]any{}}.json(t),
				v2Doc{Workspaces: map[string]any{"ws-1": v2Workspaceof("2026-07-24T17:01:55.777Z")}}.json(t),
			}
			fetcher, _ := newV2TestFetcher(t, srv)

			_, err := fetcher.Get(ctx)
			require.NoError(t, err)
			configs, err := fetcher.Get(ctx)
			require.NoError(t, err, "the retry asks for everything and gets it")
			require.Len(t, configs, 1)
			require.Len(t, srv.requests, 3)
			require.Empty(t, srv.requests[2].Query().Get("updatedAfter"))
		})
	})

	t.Run("request failures", func(t *testing.T) {
		ctx := context.Background()

		t.Run("a transient failure is retried", func(t *testing.T) {
			srv := &v2Server{t: t, secret: "service-secret", failFirst: 1}
			srv.responses = [][]byte{v2Doc{Workspaces: map[string]any{
				"ws-1": v2Workspaceof("2026-07-24T17:01:55.777Z"),
			}}.json(t)}
			fetcher, _ := newV2TestFetcher(t, srv)

			configs, err := fetcher.Get(ctx)
			require.NoError(t, err)
			require.Len(t, configs, 1)
			require.Len(t, srv.requests, 2, "the failed attempt and the one that succeeded")
		})

		t.Run("invalid credentials", func(t *testing.T) {
			srv := &v2Server{t: t, secret: "service-secret"}
			srv.responses = [][]byte{v2Doc{Workspaces: map[string]any{
				"ws-1": v2Workspaceof("2026-07-24T17:01:55.777Z"),
			}}.json(t)}
			fetcher, _ := newV2TestFetcher(t, srv, func(nc *namespaceConfig) {
				nc.hostedServiceSecret = "wrong-secret"
			})

			configs, err := fetcher.Get(ctx)
			require.ErrorContains(t, err, "unexpected status code: 401")
			require.Empty(t, configs)
		})
	})

	t.Run("config env replacement", func(t *testing.T) {
		ctx := context.Background()
		previous := configEnvReplacementEnabled
		configEnvReplacementEnabled = true
		t.Cleanup(func() { configEnvReplacementEnabled = previous })

		t.Run("the payload is rewritten before it is decoded", func(t *testing.T) {
			srv := &v2Server{t: t, secret: "service-secret"}
			srv.responses = [][]byte{v2Doc{Workspaces: map[string]any{
				"ws-PLACEHOLDER": v2Workspaceof("2026-07-24T17:01:55.777Z"),
			}}.json(t)}
			configEnv := &replacingConfigEnv{old: "PLACEHOLDER", new: "1"}
			fetcher, _ := newV2TestFetcher(t, srv, func(nc *namespaceConfig) {
				nc.configEnvHandler = configEnv
			})

			configs, err := fetcher.Get(ctx)
			require.NoError(t, err)
			require.Equal(t, 1, configEnv.calls)
			require.Contains(t, configs, "ws-1")
			require.NotContains(t, configs, "ws-PLACEHOLDER")
		})

		t.Run("a non 200 still surfaces, body and all", func(t *testing.T) {
			srv := &v2Server{t: t, secret: "service-secret"}
			srv.responses = [][]byte{v2Doc{Workspaces: map[string]any{
				"ws-1": v2Workspaceof("2026-07-24T17:01:55.777Z"),
			}}.json(t)}
			fetcher, _ := newV2TestFetcher(t, srv, func(nc *namespaceConfig) {
				nc.configEnvHandler = &replacingConfigEnv{old: "PLACEHOLDER", new: "1"}
				nc.hostedServiceSecret = "wrong-secret"
			})

			_, err := fetcher.Get(ctx)
			require.ErrorContains(t, err, "unexpected status code: 401")
			require.ErrorContains(t, err, "Unauthorized", "the body has to survive the rewrite")
		})
	})
}

// a v2 response, built the way the control plane serves it: definitions in full every time,
// workspaces null unless they changed since updatedAfter.
type v2Doc struct {
	Version                int            `json:"version"`
	SourceDefinitions      map[string]any `json:"sourceDefinitions"`
	DestinationDefinitions map[string]any `json:"destinationDefinitions"`
	AccountDefinitions     map[string]any `json:"accountDefinitions"`
	Workspaces             map[string]any `json:"workspaces"`
}

func v2Definition(id, updatedAt string) map[string]any {
	return map[string]any{"id": id, "updatedAt": updatedAt, "displayName": id}
}

func v2Workspaceof(updatedAt string) map[string]any {
	return map[string]any{"updatedAt": updatedAt, "sources": map[string]any{}}
}

func (d v2Doc) json(t *testing.T) []byte {
	t.Helper()
	d.Version = v2ConfigVersion
	if d.SourceDefinitions == nil {
		d.SourceDefinitions = map[string]any{"webhook": v2Definition("src-1", "2026-01-01T00:00:00.000Z")}
	}
	if d.DestinationDefinitions == nil {
		d.DestinationDefinitions = map[string]any{"WEBHOOK": v2Definition("dst-1", "2026-01-02T00:00:00.000Z")}
	}
	if d.AccountDefinitions == nil {
		d.AccountDefinitions = map[string]any{"ACC": v2Definition("acc-1", "2026-01-03T00:00:00.000Z")}
	}
	body, err := jsonrs.Marshal(d)
	require.NoError(t, err)
	return body
}

// v2Server serves a canned response per poll and records what was asked of it.
type v2Server struct {
	t         *testing.T
	responses [][]byte
	requests  []*url.URL
	secret    string
	failFirst int // how many of the first requests to answer with a 500
}

func (s *v2Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	user, _, ok := r.BasicAuth()
	require.True(s.t, ok)

	s.requests = append(s.requests, r.URL)
	if user != s.secret {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"message":"Unauthorized"}`))
		return
	}
	if len(s.requests) <= s.failFirst {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	i := len(s.requests) - 1
	if i >= len(s.responses) {
		i = len(s.responses) - 1 // keep serving the last one
	}
	_, _ = w.Write(s.responses[i])
}

func (s *v2Server) lastQuery() url.Values { return s.requests[len(s.requests)-1].Query() }

// countingMapper records how often the transform actually ran, per workspace.
type countingMapper struct {
	inner mapper
	calls map[string]int
	raw   map[string]json.RawMessage
	err   error // when set, every mapping fails
}

func (m *countingMapper) Map(workspaceID string, raw json.RawMessage, catalogues v2Catalogues) (ConfigT, error) {
	if m.calls == nil {
		m.calls, m.raw = make(map[string]int), make(map[string]json.RawMessage)
	}
	m.calls[workspaceID]++
	m.raw[workspaceID] = raw
	if m.err != nil {
		return ConfigT{}, m.err
	}
	return m.inner.Map(workspaceID, raw, catalogues)
}

func newV2TestFetcher(t *testing.T, srv *v2Server, opts ...func(*namespaceConfig)) (*v2ConfigFetcher, *countingMapper) {
	t.Helper()
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	conf := config.New()
	conf.Set("CONFIG_BACKEND_URL", ts.URL)

	nc := &namespaceConfig{
		config:                   conf,
		logger:                   logger.NOP,
		stats:                    stats.NOP,
		client:                   ts.Client(),
		namespace:                "free-us-1",
		hostedServiceSecret:      srv.secret,
		cpRouterURL:              "mockCpRouterURL",
		incrementalConfigUpdates: true,
	}
	for _, opt := range opts {
		opt(nc)
	}
	require.NoError(t, nc.SetUp())

	// built directly: nothing selects the v2 fetcher until the real mapper lands
	fetcher, err := newV2ConfigFetcher(nc)
	require.NoError(t, err)
	counting := &countingMapper{inner: fetcher.mapper}
	fetcher.mapper = counting
	return fetcher, counting
}

// replacingConfigEnv stands in for the env variable injection the deployment wires in.
type replacingConfigEnv struct {
	old, new string
	calls    int
}

func (c *replacingConfigEnv) ReplaceConfigWithEnvVariables(workspaceConfig []byte) []byte {
	c.calls++
	return bytes.ReplaceAll(workspaceConfig, []byte(c.old), []byte(c.new))
}
