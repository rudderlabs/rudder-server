package backendconfig

import (
	"context"
	"io"
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
	configs := map[string]workspaceConfig{
		"namespace": &NamespaceConfig{
			ConfigBackendURL: &url.URL{},
			Namespace:        "some-namespace",
			Client:           http.DefaultClient,
			Logger:           &logger.NOP{},
		},
		"multi-tenant":     &MultiTenantWorkspacesConfig{},
		"single-workspace": &SingleWorkspaceConfig{},
	}

	for name, conf := range configs {
		t.Run(name, func(t *testing.T) {
			var (
				calls int32
				ctx   = context.Background()
			)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				defer atomic.AddInt32(&calls, 1)
				t.Log("Server got called")
				w.WriteHeader(http.StatusBadRequest)
			}))
			defer server.Close()

			testRequest, err := http.NewRequest("GET", server.URL, http.NoBody)
			require.NoError(t, err)
			Http = &fakeHttp{req: testRequest}

			pkgLogger = &logger.NOP{}
			bc := &commonBackendConfig{workspaceConfig: conf}
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

type fakeHttp struct {
	req *http.Request
	err error
}

func (h *fakeHttp) NewRequest(_, _ string, _ io.Reader) (*http.Request, error) { return h.req, h.err }
func (h *fakeHttp) NewRequestWithContext(_ context.Context, _, _ string, _ io.Reader) (*http.Request, error) {
	return h.req, h.err
}
