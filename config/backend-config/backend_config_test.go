package backendconfig

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

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
