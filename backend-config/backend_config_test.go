package backendconfig

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"

	adminpkg "github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/backend-config/internal/cache"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

var sampleWorkspaceID = "sampleWorkspaceID"

var sampleBackendConfig = ConfigT{
	WorkspaceID: sampleWorkspaceID,
	Sources: []SourceT{
		{
			ID:       "1",
			WriteKey: "d",
			Enabled:  false,
		}, {
			ID:       "2",
			WriteKey: "d2",
			Enabled:  false,
			Destinations: []DestinationT{
				{
					ID:                 "d1",
					Name:               "processor Disabled",
					IsProcessorEnabled: false,
				}, {
					ID:                 "d2",
					Name:               "processor Enabled",
					IsProcessorEnabled: true,
				},
			},
		},
	},
}

var sampleConfigWithConnection = ConfigT{
	WorkspaceID: sampleWorkspaceID,
	Sources: []SourceT{
		{
			ID:       "1",
			WriteKey: "d",
			Enabled:  false,
		}, {
			ID:       "2",
			WriteKey: "d2",
			Enabled:  false,
			Destinations: []DestinationT{
				{
					ID:                 "d1",
					Name:               "processor Disabled",
					IsProcessorEnabled: false,
				}, {
					ID:                 "d2",
					Name:               "processor Enabled",
					IsProcessorEnabled: true,
				},
			},
		},
	},
	Connections: map[string]Connection{
		"1": {
			SourceID:         "2",
			DestinationID:    "d1",
			Enabled:          true,
			Config:           map[string]interface{}{"key": "value"},
			ProcessorEnabled: false,
		},
		"2": {
			SourceID:         "2",
			DestinationID:    "d2",
			Enabled:          true,
			Config:           map[string]interface{}{"key2": "value2"},
			ProcessorEnabled: true,
		},
	},
}

// This configuration is assumed by all gateway tests and, is returned on Subscribe of mocked backend config
var sampleFilteredSources = ConfigT{
	Sources: []SourceT{
		{
			ID:       "1",
			WriteKey: "d",
			Enabled:  false,
		}, {
			ID:       "2",
			WriteKey: "d2",
			Enabled:  false,
			Destinations: []DestinationT{
				{
					ID:                 "d2",
					Name:               "processor Enabled",
					IsProcessorEnabled: true,
				},
			},
		},
	},
}

var sampleBackendConfig2 = ConfigT{
	WorkspaceID: sampleWorkspaceID,
	Sources: []SourceT{
		{
			ID:       "3",
			WriteKey: "d3",
			Enabled:  false,
		}, {
			ID:       "4",
			WriteKey: "d4",
			Enabled:  false,
		},
	},
}

func TestBadResponse(t *testing.T) {
	initBackendConfig()
	t.Setenv("RSERVER_BACKEND_CONFIG_POLL_INTERVAL", "10ms")

	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		defer atomic.AddInt32(&calls, 1)
		t.Log("Server got called")
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	parsedURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	configs := map[string]workspaceConfig{
		"namespace": &namespaceConfig{
			configBackendURL:     parsedURL,
			namespace:            "some-namespace",
			client:               http.DefaultClient,
			logger:               logger.NOP,
			httpCallsStat:        stats.NOP.NewStat("backend_config_http_calls", stats.CountType),
			httpResponseSizeStat: stats.NOP.NewStat("backend_config_http_response_size", stats.HistogramType),
		},
		"single-workspace": &singleWorkspaceConfig{
			configBackendURL:     parsedURL,
			logger:               logger.NOP,
			httpCallsStat:        stats.NOP.NewStat("backend_config_http_calls", stats.CountType),
			httpResponseSizeStat: stats.NOP.NewStat("backend_config_http_response_size", stats.HistogramType),
		},
	}
	disableCache()

	for name, conf := range configs {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			pkgLogger = logger.NOP
			atomic.StoreInt32(&calls, 0)

			bc := &backendConfigImpl{
				workspaceConfig: conf,
				eb:              pubsub.New(),
			}
			bc.StartWithIDs(ctx, "")
			go bc.WaitForConfig(ctx)

			timeout := time.NewTimer(3 * time.Second)
			for {
				select {
				case <-timeout.C:
					t.Fatal("Timeout while waiting for 3 calls to HTTP server")
				default:
					if atomic.LoadInt32(&calls) == 3 {
						return
					}
				}
			}
		})
	}
}

func TestNewForDeployment(t *testing.T) {
	initBackendConfig()
	t.Run("dedicated", func(t *testing.T) {
		t.Setenv("WORKSPACE_TOKEN", "foobar")
		conf, err := newForDeployment(deployment.DedicatedType, "US", nil)
		require.NoError(t, err)
		cb, ok := conf.(*backendConfigImpl)
		require.True(t, ok)
		_, ok = cb.workspaceConfig.(*singleWorkspaceConfig)
		require.True(t, ok)
	})

	t.Run("multi-tenant", func(t *testing.T) {
		t.Setenv("WORKSPACE_NAMESPACE", "spaghetti")
		t.Setenv("HOSTED_SERVICE_SECRET", "foobar")
		conf, err := newForDeployment(deployment.MultiTenantType, "", nil)
		require.NoError(t, err)

		cb, ok := conf.(*backendConfigImpl)
		require.True(t, ok)
		_, ok = cb.workspaceConfig.(*namespaceConfig)
		require.True(t, ok)
	})

	t.Run("unsupported", func(t *testing.T) {
		_, err := newForDeployment("UNSUPPORTED_TYPE", "", nil)
		require.ErrorContains(t, err, `deployment type "UNSUPPORTED_TYPE" not supported`)
	})
}

func TestConfigUpdate(t *testing.T) {
	initBackendConfig()

	t.Run("on get failure", func(t *testing.T) {
		var (
			ctrl       = gomock.NewController(t)
			ctx        = context.Background()
			fakeError  = errors.New("fake error")
			cacheError = errors.New("cache error")
			cacheStore = cache.NewMockCache(ctrl)
		)
		defer ctrl.Finish()

		wc := NewMockworkspaceConfig(ctrl)
		wc.EXPECT().Get(gomock.Eq(ctx)).Return(map[string]ConfigT{}, fakeError).Times(1)

		bc := &backendConfigImpl{
			workspaceConfig: wc,
			cache:           cacheStore,
		}
		cacheStore.EXPECT().Get(ctx).Return([]byte{}, cacheError).Times(1)
		bc.configUpdate(ctx)
		require.False(t, bc.initialized)
	})

	t.Run("no new config", func(t *testing.T) {
		var (
			ctrl       = gomock.NewController(t)
			ctx        = context.Background()
			workspaces = "foo"
			cacheStore = cache.NewMockCache(ctrl)
		)
		defer ctrl.Finish()

		wc := NewMockworkspaceConfig(ctrl)
		wc.EXPECT().Get(gomock.Eq(ctx)).Return(map[string]ConfigT{workspaces: sampleBackendConfig}, nil).Times(1)

		bc := &backendConfigImpl{
			workspaceConfig: wc,
			curSourceJSON:   map[string]ConfigT{workspaces: sampleBackendConfig}, // same as the one returned by the workspace config
			cache:           cacheStore,
		}
		bc.configUpdate(ctx)
		require.True(t, bc.initialized)
	})

	t.Run("new config", func(t *testing.T) {
		var (
			ctrl        = gomock.NewController(t)
			ctx, cancel = context.WithCancel(context.Background())
			workspaces  = "foo"
			cacheStore  = cache.NewMockCache(ctrl)
		)
		defer ctrl.Finish()
		defer cancel()

		wc := NewMockworkspaceConfig(ctrl)
		wc.EXPECT().Get(gomock.Eq(ctx)).Return(map[string]ConfigT{workspaces: sampleBackendConfig}, nil).Times(1)

		var pubSub pubsub.PublishSubscriber
		bc := &backendConfigImpl{
			eb:              &pubSub,
			workspaceConfig: wc,
			cache:           cacheStore,
		}
		bc.curSourceJSON = map[string]ConfigT{workspaces: sampleBackendConfig2}

		chProcess := pubSub.Subscribe(ctx, string(TopicProcessConfig))
		chBackend := pubSub.Subscribe(ctx, string(TopicBackendConfig))

		bc.configUpdate(ctx)
		require.True(t, bc.initialized)
		require.Equal(t, (<-chProcess).Data, map[string]ConfigT{workspaces: sampleFilteredSources})
		require.Equal(t, (<-chBackend).Data, map[string]ConfigT{workspaces: sampleBackendConfig})
	})

	t.Run("new config with connections", func(t *testing.T) {
		var (
			ctrl        = gomock.NewController(t)
			ctx, cancel = context.WithCancel(context.Background())
			workspaces  = "foo"
			cacheStore  = cache.NewMockCache(ctrl)
		)
		defer ctrl.Finish()
		defer cancel()

		wc := NewMockworkspaceConfig(ctrl)
		wc.EXPECT().Get(gomock.Eq(ctx)).Return(map[string]ConfigT{workspaces: sampleConfigWithConnection}, nil).Times(1)

		var pubSub pubsub.PublishSubscriber
		bc := &backendConfigImpl{
			eb:              &pubSub,
			workspaceConfig: wc,
			cache:           cacheStore,
		}
		bc.curSourceJSON = map[string]ConfigT{workspaces: sampleBackendConfig2}

		chProcess := pubSub.Subscribe(ctx, string(TopicProcessConfig))
		chBackend := pubSub.Subscribe(ctx, string(TopicBackendConfig))

		bc.configUpdate(ctx)
		require.True(t, bc.initialized)
		require.Equal(t, (<-chProcess).Data, map[string]ConfigT{workspaces: sampleFilteredSources})
		require.Equal(t, (<-chBackend).Data, map[string]ConfigT{workspaces: sampleConfigWithConnection})
		require.Equal(t, bc.curSourceJSON[workspaces].Connections, sampleConfigWithConnection.Connections)
	})
}

func TestFilterProcessorEnabledDestinations(t *testing.T) {
	initBackendConfig()

	configToFilter := map[string]ConfigT{sampleWorkspaceID: sampleBackendConfig}
	originalConfig := map[string]ConfigT{sampleWorkspaceID: sampleBackendConfig}
	result := filterProcessorEnabledWorkspaceConfig(configToFilter)
	require.Equal(t, originalConfig, configToFilter)
	require.Equal(t, map[string]ConfigT{sampleWorkspaceID: sampleFilteredSources}, result)
}

func TestSubscribe(t *testing.T) {
	initBackendConfig()

	t.Run("processConfig topic", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		bc := &backendConfigImpl{
			eb:            &pubsub.PublishSubscriber{},
			curSourceJSON: map[string]ConfigT{sampleWorkspaceID: sampleBackendConfig},
		}

		filteredSourcesJSON := filterProcessorEnabledWorkspaceConfig(bc.curSourceJSON)
		bc.eb.Publish(string(TopicProcessConfig), filteredSourcesJSON)

		ch := bc.Subscribe(ctx, TopicProcessConfig)
		bc.eb.Publish(string(TopicProcessConfig), filteredSourcesJSON)
		require.Equal(t, (<-ch).Data, filteredSourcesJSON)
	})

	t.Run("backendConfig topic", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		bc := &backendConfigImpl{
			eb:            &pubsub.PublishSubscriber{},
			curSourceJSON: map[string]ConfigT{sampleWorkspaceID: sampleBackendConfig},
		}

		ch := bc.Subscribe(ctx, TopicBackendConfig)
		bc.eb.Publish(string(TopicBackendConfig), bc.curSourceJSON)
		require.Equal(t, (<-ch).Data, bc.curSourceJSON)
	})
}

func TestWaitForConfig(t *testing.T) {
	initBackendConfig()

	t.Run("no wait if initialized already", func(t *testing.T) {
		var (
			ctrl = gomock.NewController(t)
			ctx  = context.Background()
		)
		defer ctrl.Finish()

		bc := &backendConfigImpl{initialized: true}
		bc.WaitForConfig(ctx)
	})

	t.Run("it should wait until initialized", func(t *testing.T) {
		var (
			ctrl = gomock.NewController(t)
			ctx  = context.Background()
		)
		defer ctrl.Finish()

		pkgLogger = logger.NOP
		pollInterval = config.SingleValueLoader(time.Millisecond)
		bc := &backendConfigImpl{initialized: false}

		var done int32
		go func() {
			defer atomic.StoreInt32(&done, 1)
			bc.WaitForConfig(ctx)
		}()

		require.False(t, bc.initialized)
		for i := 0; i < 10; i++ {
			require.EqualValues(t, atomic.LoadInt32(&done), 0)
			time.Sleep(10 * time.Millisecond)
		}

		bc.initializedLock.Lock()
		bc.initialized = true
		bc.initializedLock.Unlock()

		require.Eventually(t, func() bool {
			return atomic.LoadInt32(&done) == 1
		}, 100*time.Millisecond, time.Millisecond)
	})
}

func TestCache(t *testing.T) {
	initBackendConfig()
	t.Setenv("RSERVER_BACKEND_CONFIG_POLL_INTERVAL", "10ms")
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		defer atomic.AddInt32(&calls, 1)
		t.Log("Server got called")
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	t.Run("noCache if database is not configured", func(t *testing.T) {
		var (
			ctx            = context.Background()
			ctrl           = gomock.NewController(t)
			wc             = NewMockworkspaceConfig(ctrl)
			workspaces     = "foo"
			workspaceToken = `token`
			mockID         = &mockIdentifier{key: workspaces, token: workspaceToken}
		)
		wc.EXPECT().Identity().Return(mockID).Times(1)
		wc.EXPECT().Get(gomock.Any()).
			Return(
				map[string]ConfigT{sampleWorkspaceID: sampleBackendConfig},
				nil,
			).AnyTimes()
		bc := &backendConfigImpl{
			workspaceConfig: wc,
			eb:              pubsub.New(),
			curSourceJSON:   map[string]ConfigT{},
		}
		bc.StartWithIDs(ctx, workspaces)

		cacheVal, err := bc.cache.Get(ctx)
		require.Equal(t, fmt.Errorf(`noCache: cache disabled`), err)
		require.Nil(t, cacheVal)
	})

	// set up database
	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Fatalf("Could not connect to docker: %s\n", err)
	}
	resourcePostgres, err := postgres.Setup(pool, t)
	if err != nil {
		t.Fatalf("Could not start resource: %s\n", err)
	}
	t.Logf("DB_DSN: %s", resourcePostgres.DBDsn)
	t.Setenv("JOBS_DB_DB_NAME", resourcePostgres.Database)
	t.Setenv("JOBS_DB_HOST", resourcePostgres.Host)
	t.Setenv("JOBS_DB_NAME", resourcePostgres.Database)
	t.Setenv("JOBS_DB_USER", resourcePostgres.User)
	t.Setenv("JOBS_DB_PASSWORD", resourcePostgres.Password)
	t.Setenv("JOBS_DB_PORT", resourcePostgres.Port)
	db := resourcePostgres.DB
	t.Run("initialize from cache when a call to control plane fails", func(t *testing.T) {
		var (
			ctrl        = gomock.NewController(t)
			ctx, cancel = context.WithCancel(context.Background())
			cacheStore  = cache.NewMockCache(ctrl)
		)
		defer ctrl.Finish()
		defer cancel()

		wc := NewMockworkspaceConfig(ctrl)
		wc.EXPECT().Get(gomock.Eq(ctx)).Return(map[string]ConfigT{}, errors.New("control plane down")).Times(1)
		sampleBackendConfigBytes, _ := json.Marshal(map[string]ConfigT{sampleWorkspaceID: sampleBackendConfig})
		unmarshalledConfig := make(map[string]ConfigT)
		err = json.Unmarshal(sampleBackendConfigBytes, &unmarshalledConfig)
		require.NoError(t, err)
		cacheStore.EXPECT().Get(gomock.Eq(ctx)).Return(sampleBackendConfigBytes, nil).Times(1)
		var pubSub pubsub.PublishSubscriber
		bc := &backendConfigImpl{
			eb:              &pubSub,
			workspaceConfig: wc,
			cache:           cacheStore,
		}
		bc.curSourceJSON = map[string]ConfigT{sampleWorkspaceID: sampleBackendConfig2}

		chProcess := pubSub.Subscribe(ctx, string(TopicProcessConfig))
		chBackend := pubSub.Subscribe(ctx, string(TopicBackendConfig))

		bc.configUpdate(ctx)
		require.True(t, bc.initialized)
		require.Equal(t, (<-chProcess).Data, map[string]ConfigT{sampleWorkspaceID: sampleFilteredSources})
		require.Equal(t, (<-chBackend).Data, unmarshalledConfig)
	})

	t.Run("not initialized from cache when a call to control plane fails and nothing exists in cache", func(t *testing.T) {
		var (
			ctrl        = gomock.NewController(t)
			ctx, cancel = context.WithCancel(context.Background())
			cacheStore  = cache.NewMockCache(ctrl)
		)
		defer ctrl.Finish()
		defer cancel()

		wc := NewMockworkspaceConfig(ctrl)
		wc.EXPECT().Get(gomock.Eq(ctx)).Return(map[string]ConfigT{}, errors.New("control plane down")).Times(1)
		cacheStore.EXPECT().Get(gomock.Eq(ctx)).Return([]byte{}, sql.ErrNoRows).Times(1)
		var pubSub pubsub.PublishSubscriber
		bc := &backendConfigImpl{
			eb:              &pubSub,
			workspaceConfig: wc,
			cache:           cacheStore,
		}
		bc.curSourceJSON = map[string]ConfigT{sampleWorkspaceID: sampleBackendConfig2}

		bc.configUpdate(ctx)
		require.False(t, bc.initialized)
	})

	t.Run("caches config to database", func(t *testing.T) {
		var (
			ctrl           = gomock.NewController(t)
			ctx            = context.Background()
			workspaces     = "foo"
			workspaceToken = `token`
		)
		defer ctrl.Finish()

		mockID := &mockIdentifier{key: workspaces, token: workspaceToken}

		wc := NewMockworkspaceConfig(ctrl)
		wc.EXPECT().Identity().Return(mockID).Times(1)
		wc.EXPECT().Get(gomock.Any()).Return(map[string]ConfigT{sampleWorkspaceID: sampleBackendConfig}, nil).AnyTimes()
		bc := &backendConfigImpl{
			workspaceConfig: wc,
			eb:              pubsub.New(),
			curSourceJSON:   map[string]ConfigT{},
		}
		bc.StartWithIDs(ctx, workspaces)
		bc.WaitForConfig(ctx)
		var (
			configBytes []byte
			config      map[string]ConfigT
			cipherBlock cipher.Block
		)
		secret := sha256.Sum256([]byte(workspaceToken))
		key := workspaces
		cipherBlock, err = aes.NewCipher(secret[:])
		require.NoError(t, err)
		gcm, err := cipher.NewGCM(cipherBlock)
		require.NoError(t, err)
		nonceSize := gcm.NonceSize()
		require.Eventually(t, func() bool {
			err = db.QueryRowContext(
				ctx,
				`SELECT config FROM config_cache WHERE key = $1`,
				key,
			).Scan(&configBytes)
			if err != nil {
				t.Logf("error while fetching config from cache: %v", err)
				return false
			}
			return true
		},
			10*time.Second,
			100*time.Millisecond,
		)
		nonce, ciphertext := configBytes[:nonceSize], configBytes[nonceSize:]
		out, err := gcm.Open(nil, nonce, ciphertext, nil)
		require.NoError(t, err)
		err = json.Unmarshal(out, &config)
		require.NoError(t, err)
		require.Equal(t, map[string]ConfigT{sampleWorkspaceID: sampleBackendConfig}, config)
	})

	t.Run(`panics if database is configured but connection fails during cache setup`, func(t *testing.T) {
		t.Setenv("JOBS_DB_DB_NAME", `nodb`)
		t.Setenv("JOBS_DB_HOST", "nodb")
		t.Setenv("JOBS_DB_NAME", `nodb`)
		t.Setenv("JOBS_DB_USER", "nodb")
		t.Setenv("JOBS_DB_PASSWORD", "nodb")
		t.Setenv("JOBS_DB_PORT", `nodb`)

		var (
			ctx            = context.Background()
			ctrl           = gomock.NewController(t)
			wc             = NewMockworkspaceConfig(ctrl)
			workspaces     = "foo"
			workspaceToken = `token`
			mockID         = &mockIdentifier{key: workspaces, token: workspaceToken}
		)
		wc.EXPECT().Identity().Return(mockID).Times(1)
		bc := &backendConfigImpl{
			workspaceConfig: wc,
			eb:              pubsub.New(),
			curSourceJSON:   map[string]ConfigT{},
		}
		require.Panics(t, func() { bc.StartWithIDs(ctx, workspaces) })
	})
}

func initBackendConfig() {
	config.Reset()
	adminpkg.Init()
	cacheOverride = nil
	diagnostics.Init()
	logger.Reset()
	Init()
}

type mockIdentifier struct {
	key   string
	token string
}

func (m *mockIdentifier) ID() string                  { return m.key }
func (m *mockIdentifier) BasicAuth() (string, string) { return m.token, "" }
func (*mockIdentifier) Type() deployment.Type         { return "mockType" }
