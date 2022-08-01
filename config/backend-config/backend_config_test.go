package backendconfig

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/sha1"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	adminpkg "github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/config/backend-config/internal/cache"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

var sampleBackendConfig = ConfigT{
	WorkspaceID: "sampleWorkspaceID",
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

// This configuration is assumed by all gateway tests and, is returned on Subscribe of mocked backend config
var sampleFilteredSources = ConfigT{
	Sources: []SourceT{
		{
			ID:           "1",
			WriteKey:     "d",
			Enabled:      false,
			Destinations: []DestinationT{},
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
			ConfigBackendURL: parsedURL,
			Namespace:        "some-namespace",
			Client:           http.DefaultClient,
			Logger:           logger.NOP,
		},
		"single-workspace": &singleWorkspaceConfig{
			configBackendURL: parsedURL,
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
		conf, err := newForDeployment(deployment.DedicatedType, nil)
		require.NoError(t, err)
		cb, ok := conf.(*backendConfigImpl)
		require.True(t, ok)
		_, ok = cb.workspaceConfig.(*singleWorkspaceConfig)
		require.True(t, ok)
	})

	t.Run("multi-tenant", func(t *testing.T) {
		t.Setenv("WORKSPACE_NAMESPACE", "spaghetti")
		t.Setenv("HOSTED_SERVICE_SECRET", "foobar")
		conf, err := newForDeployment(deployment.MultiTenantType, nil)
		require.NoError(t, err)

		cb, ok := conf.(*backendConfigImpl)
		require.True(t, ok)
		_, ok = cb.workspaceConfig.(*namespaceConfig)
		require.True(t, ok)
	})

	t.Run("unsupported", func(t *testing.T) {
		_, err := newForDeployment("UNSUPPORTED_TYPE", nil)
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
			workspaces = "foo"
			cacheStore = cache.NewMockCache(ctrl)
		)
		defer ctrl.Finish()

		wc := NewMockworkspaceConfig(ctrl)
		wc.EXPECT().Get(gomock.Eq(ctx), workspaces).Return(ConfigT{}, fakeError).Times(1)

		bc := &backendConfigImpl{
			workspaceConfig: wc,
			cache:           cacheStore,
		}
		cacheStore.EXPECT().Get(ctx).Return([]byte{}, cacheError).Times(1)
		bc.configUpdate(ctx, workspaces)
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
		wc.EXPECT().Get(gomock.Eq(ctx), workspaces).Return(sampleBackendConfig, nil).Times(1)

		bc := &backendConfigImpl{
			workspaceConfig: wc,
			curSourceJSON:   sampleBackendConfig, // same as the one returned by the workspace config
			cache:           cacheStore,
		}
		bc.configUpdate(ctx, workspaces)
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
		wc.EXPECT().Get(gomock.Eq(ctx), workspaces).Return(sampleBackendConfig, nil).Times(1)

		pubSub := pubsub.PublishSubscriber{}
		bc := &backendConfigImpl{
			eb:              &pubSub,
			workspaceConfig: wc,
			cache:           cacheStore,
		}
		bc.curSourceJSON = sampleBackendConfig2

		chProcess := pubSub.Subscribe(ctx, string(TopicProcessConfig))
		chBackend := pubSub.Subscribe(ctx, string(TopicBackendConfig))

		bc.configUpdate(ctx, workspaces)
		require.True(t, bc.initialized)
		require.Equal(t, (<-chProcess).Data, sampleFilteredSources)
		require.Equal(t, (<-chBackend).Data, sampleBackendConfig)
	})
}

func TestFilterProcessorEnabledDestinations(t *testing.T) {
	initBackendConfig()

	result := filterProcessorEnabledDestinations(sampleBackendConfig)
	require.Equal(t, result, sampleFilteredSources)
}

func TestSubscribe(t *testing.T) {
	initBackendConfig()

	t.Run("processConfig topic", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		bc := &backendConfigImpl{
			eb:            &pubsub.PublishSubscriber{},
			curSourceJSON: sampleBackendConfig,
		}

		filteredSourcesJSON := filterProcessorEnabledDestinations(bc.curSourceJSON)
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
			curSourceJSON: sampleBackendConfig,
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
		pollInterval = time.Millisecond
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

	// set up database
	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Fatalf("Could not connect to docker: %s\n", err)
	}
	database := "jobsdb"
	resourcePostgres, err := pool.Run("postgres", "11-alpine", []string{
		"POSTGRES_PASSWORD=password",
		"POSTGRES_DB=" + database,
		"POSTGRES_USER=rudder",
	})
	if err != nil {
		t.Fatalf("Could not start resource: %s\n", err)
	}
	defer func() {
		if err := pool.Purge(resourcePostgres); err != nil {
			t.Fatalf("Could not purge resource: %s \n", err)
		}
	}()
	port := resourcePostgres.GetPort("5432/tcp")
	DB_DSN := fmt.Sprintf("postgres://rudder:password@localhost:%s/%s?sslmode=disable", port, database)
	fmt.Println("DB_DSN:", DB_DSN)
	t.Setenv("JOBS_DB_DB_NAME", database)
	t.Setenv("JOBS_DB_HOST", "localhost")
	t.Setenv("JOBS_DB_NAME", database)
	t.Setenv("JOBS_DB_USER", "rudder")
	t.Setenv("JOBS_DB_PASSWORD", "password")
	t.Setenv("JOBS_DB_PORT", port)
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	var db *sql.DB
	if err := pool.Retry(func() error {
		var err error
		db, err = sql.Open("postgres", DB_DSN)
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		t.Fatalf("Could not connect to docker: %s\n", err)
	}

	t.Run("initialize from cache when a call to control plane fails", func(t *testing.T) {
		var (
			ctrl        = gomock.NewController(t)
			ctx, cancel = context.WithCancel(context.Background())
			workspaces  = "foo"
			cacheStore  = cache.NewMockCache(ctrl)
		)
		defer ctrl.Finish()
		defer cancel()

		wc := NewMockworkspaceConfig(ctrl)
		wc.EXPECT().Get(gomock.Eq(ctx), workspaces).Return(ConfigT{}, errors.New("control plane down")).Times(1)
		sampleBackendConfigBytes, _ := json.Marshal(sampleBackendConfig)
		cacheStore.EXPECT().Get(gomock.Eq(ctx)).Return(sampleBackendConfigBytes, nil).Times(1)
		pubSub := pubsub.PublishSubscriber{}
		bc := &backendConfigImpl{
			eb:              &pubSub,
			workspaceConfig: wc,
			cache:           cacheStore,
		}
		bc.curSourceJSON = sampleBackendConfig2

		chProcess := pubSub.Subscribe(ctx, string(TopicProcessConfig))
		chBackend := pubSub.Subscribe(ctx, string(TopicBackendConfig))

		bc.configUpdate(ctx, workspaces)
		require.True(t, bc.initialized)
		require.Equal(t, (<-chProcess).Data, sampleFilteredSources)
		require.Equal(t, (<-chBackend).Data, sampleBackendConfig)
	})

	t.Run("not initialized from cache when a call to control plane fails and nothing exists in cache", func(t *testing.T) {
		var (
			ctrl        = gomock.NewController(t)
			ctx, cancel = context.WithCancel(context.Background())
			workspaces  = "foo"
			cacheStore  = cache.NewMockCache(ctrl)
		)
		defer ctrl.Finish()
		defer cancel()

		wc := NewMockworkspaceConfig(ctrl)
		wc.EXPECT().Get(gomock.Eq(ctx), workspaces).Return(ConfigT{}, errors.New("control plane down")).Times(1)
		cacheStore.EXPECT().Get(gomock.Eq(ctx)).Return([]byte{}, sql.ErrNoRows).Times(1)
		pubSub := pubsub.PublishSubscriber{}
		bc := &backendConfigImpl{
			eb:              &pubSub,
			workspaceConfig: wc,
			cache:           cacheStore,
		}
		bc.curSourceJSON = sampleBackendConfig2

		bc.configUpdate(ctx, workspaces)
		require.False(t, bc.initialized)
	})

	t.Run("caches config to database", func(t *testing.T) {
		var (
			ctrl       = gomock.NewController(t)
			ctx        = context.Background()
			workspaces = "foo"
			// cacheStore  = NewMockCache(ctrl)
			accessToken = `accessToken`
		)
		defer ctrl.Finish()

		wc := NewMockworkspaceConfig(ctrl)
		wc.EXPECT().AccessToken().Return(accessToken).Times(1)
		wc.EXPECT().Get(gomock.Any(), workspaces).Return(sampleBackendConfig, nil).AnyTimes()
		bc := &backendConfigImpl{
			workspaceConfig: wc,
			eb:              pubsub.New(),
		}
		bc.StartWithIDs(ctx, workspaces)
		bc.WaitForConfig(ctx)
		var (
			configBytes []byte
			config      ConfigT
			cipherBlock cipher.Block
		)
		key := []byte(fmt.Sprintf(`%x`, md5.Sum([]byte(accessToken)))) // skipcq: GSC-G401, GO-S1025
		cipherBlock, err = aes.NewCipher(key)
		require.NoError(t, err)
		gcm, err := cipher.NewGCM(cipherBlock)
		require.NoError(t, err)
		nonceSize := gcm.NonceSize()
		require.Eventually(t, func() bool {
			err = db.QueryRowContext(
				ctx,
				`SELECT config FROM config_cache WHERE key = $1`,
				fmt.Sprintf(`%x`, sha1.Sum([]byte(workspaces))),
			).Scan(&configBytes)
			if err != nil {
				t.Logf("error while fetching config from cache: %v", err)
				return false
			}
			nonce, ciphertext := configBytes[:nonceSize], configBytes[nonceSize:]
			out, err := gcm.Open(nil, nonce, ciphertext, nil)
			if err != nil {
				t.Logf("error while decrypting config: %v", err)
				return false
			}
			require.NoError(t, err)
			err = json.Unmarshal(out, &config)
			if err != nil {
				t.Logf("error while unmarshaling config: %v", err)
				return false
			}
			return reflect.DeepEqual(config, sampleBackendConfig)
		},
			10*time.Second,
			100*time.Millisecond,
		)
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
