package suppression

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/admin"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/stretchr/testify/require"
)

func TestSuppressionSetup(t *testing.T) {
	srv := httptest.NewServer(httpHandler(t))
	defer t.Cleanup(srv.Close)
	t.Setenv("WORKSPACE_TOKEN", "216Co97d9So9TkqphM0cxBzRxc3")
	t.Setenv("CONFIG_BACKEND_URL", srv.URL)
	t.Setenv("SUPPRESS_USER_BACKUP_SERVICE_URL", srv.URL)
	t.Setenv("SUPPRESS_BACKUP_URL", srv.URL)
	t.Setenv("SUPPRESS_USER_BACKEND_URL", srv.URL)
	dir, err := os.MkdirTemp("/tmp", "rudder-server")
	require.NoError(t, err)
	t.Setenv("RUDDER_TMPDIR", dir)
	defer os.RemoveAll(dir)
	config.Set("Diagnostics.enableDiagnostics", false)
	admin.Init()
	misc.Init()
	diagnostics.Init()
	backendconfig.Init()

	require.NoError(t, backendconfig.Setup(nil))
	defer backendconfig.DefaultBackendConfig.Stop()
	backendconfig.DefaultBackendConfig.StartWithIDs(context.TODO(), "")

	t.Run(
		"should setup badgerdb and syncer successfully after getting suppression from backup service",
		func(t *testing.T) {
			f := Factory{
				EnterpriseToken: "token",
				Log:             logger.NOP,
			}
			ctx, cancel := context.WithCancel(context.Background())
			h, err := f.Setup(ctx, backendconfig.DefaultBackendConfig)
			require.NoError(t, err, "Error in setting up suppression feature")
			v := h.IsSuppressedUser("workspace-1", "user-1", "src-1")
			require.True(t, v)
			require.Eventually(t, func() bool {
				return h.IsSuppressedUser("workspace-2", "user-2", "src-4")
			}, time.Second*15, time.Millisecond*100, "User should be suppressed")
			cancel()
			time.Sleep(time.Second * 2)
			h2, err := f.Setup(context.Background(), backendconfig.DefaultBackendConfig)
			require.NoError(t, err, "Error in setting up suppression feature")
			require.True(t, h2.IsSuppressedUser("workspace-2", "user-2", "src-4"))
		},
	)
	t.Run(
		"should setup in-memory suppression db",
		func(t *testing.T) {
			f := Factory{
				EnterpriseToken: "token",
				Log:             logger.NOP,
			}
			t.Setenv("RSERVER_BACKEND_CONFIG_REGULATIONS_USE_BADGER_DB", "false")
			h, err := f.Setup(context.Background(), backendconfig.DefaultBackendConfig)
			require.NoError(t, err, "Error in setting up suppression feature")
			require.False(t, h.IsSuppressedUser("workspace-1", "user-1", "src-1"))
		},
	)
}

func httpHandler(t *testing.T) http.Handler {
	t.Helper()
	srvMux := chi.NewMux()

	srvMux.Get("/workspaceConfig", getSingleTenantWorkspaceConfig)
	srvMux.Get("/full-export", func(w http.ResponseWriter, r *http.Request) { http.ServeFile(w, r, "testdata/full-export") })
	srvMux.Get("/latest-export", func(w http.ResponseWriter, r *http.Request) { http.ServeFile(w, r, "testdata/latest-export") })
	srvMux.Get("/dataplane/workspaces/{workspace_id}/regulations/suppressions", getSuppressionFromManager)

	return srvMux
}

func getSingleTenantWorkspaceConfig(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	config := backendconfig.ConfigT{
		WorkspaceID: "reg-test-workspaceId",
	}
	body, err := json.Marshal(config)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(body)
}

func getSuppressionFromManager(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	s := suppressionsResponse{
		Items: []model.Suppression{},
		Token: "_token_",
	}
	body, err := json.Marshal(s)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(body)
}
