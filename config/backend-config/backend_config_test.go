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
