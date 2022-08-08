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

func TestBadResponse(t *testing.T) {
	stats.Setup()
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
		"namespace": &NamespaceConfig{
			ConfigBackendURL: parsedURL,
			Namespace:        "some-namespace",
			Client:           http.DefaultClient,
			Logger:           &logger.NOP{},
		},
		"multi-tenant": &MultiTenantWorkspacesConfig{
			configBackendURL: server.URL,
		},
		"single-workspace": &SingleWorkspaceConfig{
			configBackendURL: server.URL,
		},
	}

	for name, conf := range configs {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			pkgLogger = &logger.NOP{}
			atomic.StoreInt32(&calls, 0)

			bc := &commonBackendConfig{
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
		config, err := newForDeployment(deployment.DedicatedType, nil)
		require.NoError(t, err)
		cb, ok := config.(*commonBackendConfig)
		require.True(t, ok)
		_, ok = cb.workspaceConfig.(*SingleWorkspaceConfig)
		require.True(t, ok)
	})

	t.Run("multi-tenant", func(t *testing.T) {
		t.Setenv("HOSTED_MULTITENANT_SERVICE_SECRET", "foobar")
		config, err := newForDeployment(deployment.MultiTenantType, nil)
		require.NoError(t, err)

		cb, ok := config.(*commonBackendConfig)
		require.True(t, ok)
		_, ok = cb.workspaceConfig.(*MultiTenantWorkspacesConfig)
		require.True(t, ok)
	})

	t.Run("multi-tenant-with-namespace", func(t *testing.T) {
		t.Setenv("WORKSPACE_NAMESPACE", "spaghetti")
		t.Setenv("HOSTED_MULTITENANT_SERVICE_SECRET", "foobar")
		t.Setenv("CONTROL_PLANE_BASIC_AUTH_USERNAME", "Clark")
		t.Setenv("CONTROL_PLANE_BASIC_AUTH_PASSWORD", "Kent")
		config, err := newForDeployment(deployment.MultiTenantType, nil)
		require.NoError(t, err)

		cb, ok := config.(*commonBackendConfig)
		require.True(t, ok)
		_, ok = cb.workspaceConfig.(*NamespaceConfig)
		require.True(t, ok)
	})

	t.Run("unsupported", func(t *testing.T) {
		_, err := newForDeployment("UNSUPPORTED_TYPE", nil)
		require.ErrorContains(t, err, `deployment type "UNSUPPORTED_TYPE" not supported`)
	})
}

func TestConfigUpdate(t *testing.T) {
	initBackendConfig()
	logger.Init()
	stats.Setup()

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

		bc := &commonBackendConfig{workspaceConfig: wc}
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

		bc := &commonBackendConfig{
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
		bc := &commonBackendConfig{
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
