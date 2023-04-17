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

	"github.com/golang/mock/gomock"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	adminpkg "github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/backend-config/internal/cache"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	serverUtilsTypes "github.com/rudderlabs/rudder-server/utils/types"
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
			configBackendURL: parsedURL,
			namespace:        "some-namespace",
			client:           http.DefaultClient,
			logger:           logger.NOP,
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
			workspaces = "foo"
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
		wc.EXPECT().Get(gomock.Eq(ctx)).Return(map[string]ConfigT{workspaces: sampleBackendConfig}, nil).Times(1)

		bc := &backendConfigImpl{
			workspaceConfig: wc,
			curSourceJSON:   map[string]ConfigT{workspaces: sampleBackendConfig}, // same as the one returned by the workspace config
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

		bc.configUpdate(ctx, workspaces)
		require.True(t, bc.initialized)
		require.Equal(t, (<-chProcess).Data, map[string]ConfigT{workspaces: sampleFilteredSources})
		require.Equal(t, (<-chBackend).Data, map[string]ConfigT{workspaces: sampleBackendConfig})
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
	database := "jobsdb"
	resourcePostgres, err := pool.Run("postgres", "15-alpine", []string{
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

		bc.configUpdate(ctx, workspaces)
		require.True(t, bc.initialized)
		require.Equal(t, (<-chProcess).Data, map[string]ConfigT{sampleWorkspaceID: sampleFilteredSources})
		require.Equal(t, (<-chBackend).Data, unmarshalledConfig)
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
		wc.EXPECT().Get(gomock.Eq(ctx)).Return(map[string]ConfigT{}, errors.New("control plane down")).Times(1)
		cacheStore.EXPECT().Get(gomock.Eq(ctx)).Return([]byte{}, sql.ErrNoRows).Times(1)
		var pubSub pubsub.PublishSubscriber
		bc := &backendConfigImpl{
			eb:              &pubSub,
			workspaceConfig: wc,
			cache:           cacheStore,
		}
		bc.curSourceJSON = map[string]ConfigT{sampleWorkspaceID: sampleBackendConfig2}

		bc.configUpdate(ctx, workspaces)
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

func (m *mockIdentifier) ID() string {
	return m.key
}

func (m *mockIdentifier) BasicAuth() (string, string) {
	return m.token, ""
}

func (*mockIdentifier) Type() deployment.Type {
	return deployment.Type(`mockType`)
}

func TestDestinationFilterCondition(t *testing.T) {
	type testDestinationT struct {
		id                 string
		isProcessorEnabled bool
		definitionID       string
		definitionConfig   map[string]interface{}
		config             map[string]interface{}
	}

	type testSourceT struct {
		name          string
		id            string
		sourceDefType string
	}

	type filterConditionT struct {
		destination testDestinationT
		source      testSourceT
		event       serverUtilsTypes.SingularEventT
	}

	type testCaseT struct {
		caseName string
		input    filterConditionT
		expected bool
	}

	testCases := []testCaseT{
		{
			caseName: "when mode is cloud for web srcType if track event is sent, return true",
			input: filterConditionT{
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowedValues": []interface{}{"track", "group"},
									},
								},
								"hybrid": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowAll": true,
									},
								},
							},
						},
					},
					definitionID: "1DefId",
					config: map[string]interface{}{
						"connectionMode": "cloud",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "web",
				},
				event: serverUtilsTypes.SingularEventT{
					"type": "track",
				},
			},
			expected: true,
		},
		{
			caseName: "when mode is hybrid for web srcType if page event is sent, return false",
			input: filterConditionT{
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowedValues": []interface{}{"track", "group"},
									},
								},
								"hybrid": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowAll": true,
									},
								},
							},
						},
					},
					definitionID: "1DefId",
					config: map[string]interface{}{
						"connectionMode": "cloud",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "web",
				},
				event: serverUtilsTypes.SingularEventT{
					"type": "page",
				},
			},
			expected: false,
		},
		{
			caseName: "when mode is device for web srcType if track event is sent, return false",
			input: filterConditionT{
				destination: testDestinationT{
					id: "2",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowedValues": []interface{}{"track", "group"},
									},
								},
								"device": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowAll": true,
									},
								},
							},
						},
					},
					definitionID: "1DefId",
					config: map[string]interface{}{
						"connectionMode": "device",
					},
					isProcessorEnabled: false,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "web",
				},
				event: serverUtilsTypes.SingularEventT{
					"type": "track",
				},
			},
			expected: false,
		},
		{
			caseName: "when mode is hybrid for web srcType if event type is empty, return true",
			input: filterConditionT{
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowedValues": []interface{}{"track", "group"},
									},
								},
								"hybrid": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowAll": true,
									},
								},
							},
						},
					},
					definitionID: "1DefId",
					config: map[string]interface{}{
						"connectionMode": "cloud",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "web",
				},
				event: serverUtilsTypes.SingularEventT{},
			},
			expected: true,
		},
		{
			caseName: "when mode is hybrid for web srcType if event type is not a valid string, return false",
			input: filterConditionT{
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowedValues": []interface{}{"track", "group"},
									},
								},
								"hybrid": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowAll": true,
									},
								},
							},
						},
					},
					definitionID: "1DefId",
					config: map[string]interface{}{
						"connectionMode": "cloud",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "web",
				},
				event: serverUtilsTypes.SingularEventT{
					"type": 12343,
				},
			},
			expected: false,
		},
		{
			caseName: "when mode is not present for web srcType if event type is track, return true",
			input: filterConditionT{
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowedValues": []interface{}{"track", "group"},
									},
								},
								"hybrid": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowAll": true,
									},
								},
							},
						},
					},
					definitionID:       "1DefId",
					config:             map[string]interface{}{},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "web",
				},
				event: serverUtilsTypes.SingularEventT{
					"type": "track",
				},
			},
			expected: true,
		},
		{
			caseName: "when supportedConnectionModes is not present, connectionMode is cloud for web srcType and if event type is track, return true",
			input: filterConditionT{
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
					},
					definitionID: "1DefId",
					config: map[string]interface{}{
						"connectionMode": "cloud",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "web",
				},
				event: serverUtilsTypes.SingularEventT{
					"type": "track",
				},
			},
			expected: true,
		},
		{
			caseName: "when connected sourceType(android) is not present in supportedConnectionModes but present in supportedSourceTypes with cloud connectionMode if event type is track, return true",
			input: filterConditionT{
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web", "android"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowedValues": []interface{}{"track", "group"},
									},
								},
								"hybrid": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowAll": true,
									},
								},
							},
						},
					},
					definitionID: "1DefId",
					config: map[string]interface{}{
						"connectionMode": "cloud",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "android",
				},
				event: serverUtilsTypes.SingularEventT{
					"type": "track",
				},
			},
			expected: true,
		},
		{
			caseName: "when connected sourceType(web) is present in supportedConnectionModes but connectionMode(hybrid) is not available if event type is track, return false",
			input: filterConditionT{
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web", "android"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowedValues": []interface{}{"track", "group"},
									},
								},
								"device": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowAll": true,
									},
								},
							},
						},
					},
					definitionID: "1DefId",
					config: map[string]interface{}{
						"connectionMode": "hybrid",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "web",
				},
				event: serverUtilsTypes.SingularEventT{
					"type": "track",
				},
			},
			expected: false,
		},
		{
			caseName: "when supportedConnectionModes.web.cloud is an empty map({}) destination is cloud-mode for web srcType if track event is sent, return true",
			input: filterConditionT{
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{},
								"hybrid": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowAll": true,
									},
								},
							},
						},
					},
					definitionID: "1DefId",
					config: map[string]interface{}{
						"connectionMode": "cloud",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "web",
				},
				event: serverUtilsTypes.SingularEventT{
					"type": "track",
				},
			},
			expected: true,
		},
		{
			caseName: "when mode is cloud for web srcType & allowedMessageTypes is [track, group] if page event is sent, return false",
			input: filterConditionT{
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowedValues": []interface{}{"track", "group"},
									},
								},
							},
						},
					},
					definitionID: "1DefId",
					config: map[string]interface{}{
						"connectionMode": "cloud",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "web",
				},
				event: serverUtilsTypes.SingularEventT{
					"type": "page",
				},
			},
			expected: false,
		},
		{
			caseName: "when mode is cloud for web srcType & allowedMessageTypes is [track, group] if track event is sent, return true",
			input: filterConditionT{
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowedValues": []interface{}{"track", "group"},
									},
								},
							},
						},
					},
					definitionID: "1DefId",
					config: map[string]interface{}{
						"connectionMode": "cloud",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "web",
				},
				event: serverUtilsTypes.SingularEventT{
					"type": "track",
				},
			},
			expected: true,
		},
		{
			caseName: "when mode is cloud for web srcType & allowAll is true if page event is sent, return false",
			input: filterConditionT{
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowAll": true,
									},
								},
							},
						},
					},
					definitionID: "1DefId",
					config: map[string]interface{}{
						"connectionMode": "cloud",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "web",
				},
				event: serverUtilsTypes.SingularEventT{
					"type": "page",
				},
			},
			expected: true,
		},
		{
			caseName: "when mode is cloud for web srcType & allowAll is true if identify event(not in supportedMessageTypes) is sent, return false",
			input: filterConditionT{
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowAll": true,
									},
								},
							},
						},
					},
					definitionID: "1DefId",
					config: map[string]interface{}{
						"connectionMode": "cloud",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "web",
				},
				event: serverUtilsTypes.SingularEventT{
					"type": "identify",
				},
			},
			expected: false,
		},

		{
			caseName: "when mode is cloud for web srcType & allowAll is true & allowedValues contains [track] if page event(in supportedMessageTypes) is sent, return true",
			input: filterConditionT{
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowAll":    true,
										"allowValues": []interface{}{"track"},
									},
								},
							},
						},
					},
					definitionID: "1DefId",
					config: map[string]interface{}{
						"connectionMode": "cloud",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "web",
				},
				event: serverUtilsTypes.SingularEventT{
					"type": "page",
				},
			},
			expected: true,
		},
		{
			caseName: "when mode is cloud for web srcType & allowAll, allowedValues not present if page event(in supportedMessageTypes) is sent, return true",
			input: filterConditionT{
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{},
								},
							},
						},
					},
					definitionID: "1DefId",
					config: map[string]interface{}{
						"connectionMode": "cloud",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "web",
				},
				event: serverUtilsTypes.SingularEventT{
					"type": "page",
				},
			},
			expected: true,
		},
		{
			caseName: "when mode is cloud for web srcType & allowAll, allowedValues not present if alias event(not in supportedMessageTypes) is sent, return false",
			input: filterConditionT{
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{},
								},
							},
						},
					},
					definitionID: "1DefId",
					config: map[string]interface{}{
						"connectionMode": "cloud",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "web",
				},
				event: serverUtilsTypes.SingularEventT{
					"type": "alias",
				},
			},
			expected: false,
		},
		{
			caseName: "when mode is cloud for web srcType & allowedValues contains [identify] but not present in supportedMessageTypes if identify event is sent, return false",
			input: filterConditionT{
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowValues": []interface{}{"identify"},
									},
								},
							},
						},
					},
					definitionID: "1DefId",
					config: map[string]interface{}{
						"connectionMode": "cloud",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "web",
				},
				event: serverUtilsTypes.SingularEventT{
					"type": "identify",
				},
			},
			expected: false,
		},
		{
			caseName: "when mode is cloud for web srcType & allowAll is false & allowedValues contains [track] if track event(in supportedMessageTypes) is sent, return true",
			input: filterConditionT{
				destination: testDestinationT{
					id: "1",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"web"},
						"supportedConnectionModes": map[string]interface{}{
							"web": map[string]interface{}{
								"cloud": map[string]interface{}{
									"messageType": map[string]interface{}{
										"allowAll":    false,
										"allowValues": []interface{}{"track"},
									},
								},
							},
						},
					},
					definitionID: "1DefId",
					config: map[string]interface{}{
						"connectionMode": "cloud",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "web",
				},
				event: serverUtilsTypes.SingularEventT{
					"type": "track",
				},
			},
			expected: true,
		},

		{
			caseName: "when mode is `cloud` for `cloud` srcType & supportedConnectionModes is in previous version if track event is sent, return true",
			input: filterConditionT{
				destination: testDestinationT{
					id: "3",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"cloud", "android", "ios", "flutter", "reactnative"},
						"supportedConnectionModes": map[string]interface{}{
							"cloud":       []interface{}{"cloud", "device"},
							"android":     []interface{}{"cloud", "device"},
							"ios":         []interface{}{"cloud", "device"},
							"flutter":     []interface{}{"cloud", "device"},
							"reactnative": []interface{}{"cloud", "device"},
						},
					},
					definitionID: "2DefId",
					config: map[string]interface{}{
						"connectionMode": "cloud",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "cloud",
				},
				event: serverUtilsTypes.SingularEventT{
					"type": "track",
				},
			},
			expected: true,
		},
		{
			caseName: "when mode is `cloud` for `cloud` srcType & supportedConnectionModes is in previous version if alias event is sent, return false",
			input: filterConditionT{
				destination: testDestinationT{
					id: "3",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"cloud", "android", "ios", "flutter", "reactnative"},
						"supportedConnectionModes": map[string]interface{}{
							"cloud":       []interface{}{"cloud", "device"},
							"android":     []interface{}{"cloud", "device"},
							"ios":         []interface{}{"cloud", "device"},
							"flutter":     []interface{}{"cloud", "device"},
							"reactnative": []interface{}{"cloud", "device"},
						},
					},
					definitionID: "2DefId",
					config: map[string]interface{}{
						"connectionMode": "cloud",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "cloud",
				},
				event: serverUtilsTypes.SingularEventT{
					"type": "alias",
				},
			},
			expected: false,
		},
		{
			caseName: "when mode is `cloud` for `cloud` srcType(not in supportedConnectionModes) & supportedConnectionModes is in previous version if track event is sent, return true",
			input: filterConditionT{
				destination: testDestinationT{
					id: "4",
					definitionConfig: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "page", "group"},
						"supportedSourceTypes":  []interface{}{"cloud", "android", "ios", "flutter", "reactnative"},
						"supportedConnectionModes": map[string]interface{}{
							"android":     []interface{}{"cloud", "device"},
							"ios":         []interface{}{"cloud", "device"},
							"flutter":     []interface{}{"cloud", "device"},
							"reactnative": []interface{}{"cloud", "device"},
						},
					},
					definitionID: "3DefId",
					config: map[string]interface{}{
						"connectionMode": "cloud",
					},
					isProcessorEnabled: true,
				},
				source: testSourceT{
					name:          "demo-source",
					id:            "1src",
					sourceDefType: "cloud",
				},
				event: serverUtilsTypes.SingularEventT{
					"type": "track",
				},
			},
			expected: true,
		},
	}

	destination := &DestinationT{
		ID:   "1",
		Name: "GA4-demo",
		DestinationDefinition: DestinationDefinitionT{
			ID:     "acpo9i",
			Name:   "GA4",
			Config: map[string]interface{}{},
		},
		Config: map[string]interface{}{},
	}
	for _, testCase := range testCases {
		t.Run(testCase.caseName, func(t *testing.T) {
			// Setting test-case values to some default values for destination
			destination.Config = testCase.input.destination.config
			destination.DestinationDefinition.Config = testCase.input.destination.definitionConfig
			destination.DestinationDefinition.ID = testCase.input.destination.definitionID
			destination.ID = testCase.input.destination.id
			destination.IsProcessorEnabled = testCase.input.destination.isProcessorEnabled

			// Setting test-case values for source
			source := &SourceT{
				ID: testCase.input.source.id,
				SourceDefinition: SourceDefinitionT{
					ID:   "src1",
					Type: testCase.input.source.sourceDefType,
				},
			}

			actualOutput := destination.AllowEventToDestination(source, testCase.input.event)

			require.Equal(t, testCase.expected, actualOutput)
		})
	}
}

func TestConvertToArrayOfType(t *testing.T) {
	type convertTestCases[T EventPropsTypes] struct {
		caseName string
		input    interface{}
		expected []T
	}

	strTestCases := []convertTestCases[string]{
		{
			caseName: "when proper input array of strings(internally) is sent, return proper []string",
			input:    []interface{}{"1", "2", "3"},
			expected: []string{"1", "2", "3"},
		},
		{
			caseName: "when empty array of strings(internally) is sent, return empty string array",
			input:    []interface{}{},
			expected: []string{},
		},
		{
			caseName: "when empty array of different types is sent, return empty string array",
			input:    []interface{}{"1", 2, "omgo"},
			expected: []string{},
		},
		{
			caseName: "when proper(golang type) string array is sent, return same string array",
			input:    []string{"lks", "omgo"},
			expected: []string{"lks", "omgo"},
		},
	}

	for _, strTestCase := range strTestCases {
		t.Run(strTestCase.caseName, func(t *testing.T) {
			actual := ConvertToArrayOfType[string](strTestCase.input)
			require.EqualValues(t, strTestCase.expected, actual)
		})
	}
}
