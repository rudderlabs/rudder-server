package suppression

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/admin"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/stretchr/testify/require"
)

const (
	namespaceID = "spaghetti"
)

func TestSuppressionSetup(t *testing.T) {
	srv := httptest.NewServer(httpHandler(t))
	defer t.Cleanup(srv.Close)
	t.Setenv("WORKSPACE_TOKEN", "216Co97d9So9TkqphM0cxBzRxc3")
	t.Setenv("CONFIG_BACKEND_URL", srv.URL)
	t.Setenv("SUPPRESS_USER_BACKEND_URL", srv.URL)
	t.Setenv("SUPPRESS_BACKUP_URL", srv.URL)
	dir, err := os.MkdirTemp("/tmp", "rudder-server")
	require.NoError(t, err)
	t.Setenv("RUDDER_TMPDIR", dir)

	{
		config.Set("Diagnostics.enableDiagnostics", false)
		admin.Init()
		misc.Init()
		diagnostics.Init()
		backendconfig.Init()

		require.NoError(t, backendconfig.Setup(nil))
		defer backendconfig.DefaultBackendConfig.Stop()
		backendconfig.DefaultBackendConfig.StartWithIDs(context.TODO(), "")
	}

	f := Factory{
		EnterpriseToken: "token",
		Log:             logger.NOP,
	}

	h, err := f.Setup(context.Background(), backendconfig.DefaultBackendConfig)
	require.NoError(t, err, "Error in setting up suppression feature")
	v := h.IsSuppressedUser("workspace-1", "user-1", "src-1")
	require.True(t, v)
	require.Eventually(t, func() bool {
		return h.IsSuppressedUser("workspace-2", "user-2", "src-4")
	}, time.Second*30, time.Millisecond*100, "User should be suppressed")
}

func httpHandler(t *testing.T) http.Handler {
	t.Helper()
	srvMux := mux.NewRouter()
	srvMux.HandleFunc("/workspaceConfig", getSingleTenantWorkspaceConfig).Methods(http.MethodGet)
	srvMux.HandleFunc("/data-plane/v1/namespaces/{namespace_id}/config", getMultiTenantNamespaceConfig).Methods(http.MethodGet)
	srvMux.HandleFunc("/full-export", func(w http.ResponseWriter, r *http.Request) { http.ServeFile(w, r, "testdata/full-export") }).Methods(http.MethodGet)
	srvMux.HandleFunc("/latest-export", func(w http.ResponseWriter, r *http.Request) { http.ServeFile(w, r, "testdata/latest-export") }).Methods(http.MethodGet)
	srvMux.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req)
		})
	})

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

func getMultiTenantNamespaceConfig(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	config := map[string]backendconfig.ConfigT{namespaceID: {
		WorkspaceID: "reg-test-workspaceId",
	}}
	body, err := json.Marshal(config)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(body)
}
