package suppression

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/admin"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/internal/badgerdb"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func TestSuppressionSetup(t *testing.T) {
	tmpDir, err := backupDir(t.TempDir())
	require.NoError(t, err)
	srv := httptest.NewServer(httpHandler(t, tmpDir))
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

	generateTestData(t, tmpDir)

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
			v := h.GetSuppressedUser("workspace-1", "user-1", "src-1")
			require.NotNil(t, v)
			require.Eventually(t, func() bool {
				return h.GetSuppressedUser("workspace-2", "user-2", "src-4") != nil
			}, time.Second*15, time.Millisecond*100, "User should be suppressed")
			cancel()
			time.Sleep(time.Second * 2)
			h2, err := f.Setup(context.Background(), backendconfig.DefaultBackendConfig)
			require.NoError(t, err, "Error in setting up suppression feature")
			require.NotNil(t, h2.GetSuppressedUser("workspace-2", "user-2", "src-4"))
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
			require.Nil(t, h.GetSuppressedUser("workspace-1", "user-1", "src-1"))
		},
	)
}

func httpHandler(t *testing.T, dir string) http.Handler {
	t.Helper()
	srvMux := chi.NewMux()

	srvMux.Get("/workspaceConfig", getSingleTenantWorkspaceConfig)
	srvMux.Get("/full-export", func(w http.ResponseWriter, r *http.Request) { http.ServeFile(w, r, path.Join(dir, "full-export")) })
	srvMux.Get("/latest-export", func(w http.ResponseWriter, r *http.Request) { http.ServeFile(w, r, path.Join(dir, "latest-export")) })
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

func generateTestData(t *testing.T, tmpDir string) {
	generateBackupFiles := func(t *testing.T, dir, backupFilename string, repo *badgerdb.Repository, suppressions []model.Suppression) {
		token := []byte("__token__")

		require.NoError(t, repo.Add(suppressions, token))

		f, err := os.Create(path.Join(dir, backupFilename))
		defer func() { _ = f.Close() }()
		require.NoError(t, err)
		require.NoError(t, repo.Backup(f))
	}

	repo, err := badgerdb.NewRepository(tmpDir, logger.NOP, stats.Default)
	require.NoError(t, err)

	suppressions := []model.Suppression{
		{
			Canceled:    false,
			WorkspaceID: "workspace-1",
			UserID:      "user-1",
			SourceIDs:   []string{"src-1"},
		},
		{
			Canceled:    false,
			WorkspaceID: "workspace-1",
			UserID:      "user-1",
			SourceIDs:   []string{"src-2"},
		},
	}
	generateBackupFiles(t, tmpDir, "latest-export", repo, suppressions)

	suppressions = append(suppressions, []model.Suppression{
		{
			Canceled:    false,
			WorkspaceID: "workspace-2",
			UserID:      "user-2",
			SourceIDs:   []string{"src-3"},
		},
		{
			Canceled:    false,
			WorkspaceID: "workspace-2",
			UserID:      "user-2",
			SourceIDs:   []string{"src-4"},
		},
	}...)
	generateBackupFiles(t, tmpDir, "full-export", repo, suppressions)
}

func backupDir(tmpDir string) (string, error) {
	baseDir := path.Join(tmpDir, "backup")
	err := os.MkdirAll(baseDir, 0o700)
	if err != nil {
		return "", err
	}
	return baseDir, nil
}
