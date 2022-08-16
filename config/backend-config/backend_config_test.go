package backendconfig

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	adminpkg "github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

var sampleBackendConfig = ConfigT{
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
			Logger:           &logger.NOP{},
		},
		"multi-tenant": &multiTenantWorkspacesConfig{
			configBackendURL: parsedURL,
		},
		"single-workspace": &singleWorkspaceConfig{
			configBackendURL: parsedURL,
		},
	}

	for name, conf := range configs {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			pkgLogger = &logger.NOP{}
			atomic.StoreInt32(&calls, 0)

			bc := &backendConfigImpl{
				workspaceConfig: conf,
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
		t.Setenv("HOSTED_SERVICE_SECRET", "foobar")
		conf, err := newForDeployment(deployment.MultiTenantType, nil)
		require.NoError(t, err)

		cb, ok := conf.(*backendConfigImpl)
		require.True(t, ok)
		_, ok = cb.workspaceConfig.(*multiTenantWorkspacesConfig)
		require.True(t, ok)
	})

	t.Run("multi-tenant-with-namespace", func(t *testing.T) {
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
			workspaces = "foo"
		)
		defer ctrl.Finish()

		wc := NewMockworkspaceConfig(ctrl)
		wc.EXPECT().Get(gomock.Eq(ctx), workspaces).Return(ConfigT{}, fakeError).Times(1)
		statConfigBackendError := stats.DefaultStats.NewStat("config_backend.errors", stats.CountType)

		bc := &backendConfigImpl{workspaceConfig: wc}
		bc.configUpdate(ctx, statConfigBackendError, workspaces)
		require.False(t, bc.initialized)
	})

	t.Run("no new config", func(t *testing.T) {
		var (
			ctrl       = gomock.NewController(t)
			ctx        = context.Background()
			workspaces = "foo"
		)
		defer ctrl.Finish()

		wc := NewMockworkspaceConfig(ctrl)
		wc.EXPECT().Get(gomock.Eq(ctx), workspaces).Return(sampleBackendConfig, nil).Times(1)
		statConfigBackendError := stats.DefaultStats.NewStat("config_backend.errors", stats.CountType)

		bc := &backendConfigImpl{
			workspaceConfig: wc,
			curSourceJSON:   sampleBackendConfig, // same as the one returned by the workspace config
		}
		bc.configUpdate(ctx, statConfigBackendError, workspaces)
		require.True(t, bc.initialized)
	})

	t.Run("new config", func(t *testing.T) {
		var (
			ctrl        = gomock.NewController(t)
			ctx, cancel = context.WithCancel(context.Background())
			workspaces  = "foo"
		)
		defer ctrl.Finish()
		defer cancel()

		wc := NewMockworkspaceConfig(ctrl)
		wc.EXPECT().Get(gomock.Eq(ctx), workspaces).Return(sampleBackendConfig, nil).Times(1)
		statConfigBackendError := stats.DefaultStats.NewStat("config_backend.errors", stats.CountType)

		pubSub := pubsub.PublishSubscriber{}
		bc := &backendConfigImpl{
			eb:              &pubSub,
			workspaceConfig: wc,
		}
		bc.curSourceJSON = sampleBackendConfig2

		chProcess := pubSub.Subscribe(ctx, string(TopicProcessConfig))
		chBackend := pubSub.Subscribe(ctx, string(TopicBackendConfig))

		bc.configUpdate(ctx, statConfigBackendError, workspaces)
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

		pkgLogger = &logger.NOP{}
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

func initBackendConfig() {
	config.Load()
	adminpkg.Init()
	diagnostics.Init()
	logger.Init()
	stats.Setup()
	Init()
}
