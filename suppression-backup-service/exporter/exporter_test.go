package exporter_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	suppression "github.com/rudderlabs/rudder-server/enterprise/suppress-user"
	suppressModel "github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/suppression-backup-service/exporter"
	"github.com/rudderlabs/rudder-server/suppression-backup-service/model"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	tokenKey    = "__token__"
	namespaceID = "spaghetti"
)

// test export function
func TestExport(t *testing.T) {
	misc.Init()

	repo, err := suppression.NewBadgerRepository(t.TempDir(), logger.NOP)
	require.NoError(t, err)

	seed := func(repo suppression.Repository) {
		token := []byte("token")
		require.NoError(t, repo.Add([]suppressModel.Suppression{
			{
				WorkspaceID: "workspace1",
				UserID:      "user1",
				SourceIDs:   []string{"source1"},
			},
			{
				WorkspaceID: "workspace2",
				UserID:      "user2",
				SourceIDs:   []string{"source1"},
			},
		}, token), "could not add data to badgerdb")
	}
	verify := func(repo suppression.Repository) {
		metadata, err := repo.Suppressed("workspace1", "user1", "source1")
		require.NoError(t, err)
		require.NotNil(t, metadata)
		metadata, err = repo.Suppressed("workspace2", "user2", "source1")
		require.NoError(t, err)
		require.NotNil(t, metadata)
	}

	seed(repo)
	verify(repo)

	file, err := os.CreateTemp(t.TempDir(), "exportV2")
	require.NoError(t, err)
	require.NoError(t, file.Close())
	require.NoError(t, exporter.Export(repo, model.File{Path: file.Name(), Mu: &sync.RWMutex{}}), "could not export data")
	require.NoError(t, repo.Stop())
	file, err = os.Open(file.Name())
	require.NoError(t, err)

	repo, err = suppression.NewBadgerRepository(t.TempDir(), logger.NOP)
	require.NoError(t, err)
	require.NoError(t, repo.Restore(file))
	require.NoError(t, file.Close())

	verify(repo)

	require.NoError(t, repo.Stop())
}

func TestExportLoop(t *testing.T) {
	misc.Init()
	srv := httptest.NewServer(handler(t))
	defer t.Cleanup(srv.Close)
	t.Setenv("WORKSPACE_TOKEN", "216Co97d9So9TkqphM0cxBzRxc3")
	t.Setenv("CONFIG_BACKEND_URL", srv.URL)
	t.Setenv("SUPPRESS_USER_BACKEND_URL", srv.URL)

	identifier := &identity.Workspace{
		WorkspaceID: "workspace-1",
	}
	exportBaseDir, err := exportPath()
	require.NoError(t, err)

	fullExportFile := model.File{Path: path.Join(exportBaseDir, "full-export"), Mu: &sync.RWMutex{}}
	latestExportFile := model.File{Path: path.Join(exportBaseDir, "latest-export"), Mu: &sync.RWMutex{}}
	fullExporter := exporter.Exporter{
		Id:   identifier,
		File: fullExportFile,
		Log:  logger.NOP,
	}
	latestExporter := exporter.Exporter{
		Id:   identifier,
		File: latestExportFile,
		Log:  logger.NOP,
	}

	ctx := context.Background()
	go func() {
		require.NoError(t, latestExporter.LatestExporterLoop(ctx))
	}()
	go func() {
		require.NoError(t, fullExporter.FullExporterLoop(ctx))
	}()
	t.Run("test full export", func(t *testing.T) {
		require.Eventually(t, func() bool {
			_, err := os.Stat(fullExportFile.Path)
			if !os.IsNotExist(err) {
				repo, err := suppression.NewBadgerRepository(path.Join(exportBaseDir, "full-export-restore"), logger.NOP)
				require.NoError(t, err)
				file, err := os.OpenFile(fullExportFile.Path, os.O_RDONLY, 0o400)
				require.NoError(t, err)
				err = repo.Restore(file)
				require.NoError(t, err)
				metadata, err := repo.Suppressed("workspace-1", "user-1", "src-1")
				require.NoError(t, err)
				return metadata != nil
			}
			return false
		}, 3*time.Second, 10*time.Millisecond, "full export should be done in 10 seconds")
	})
	t.Run("test latest export", func(t *testing.T) {
		require.Eventually(t, func() bool {
			_, err := os.Stat(latestExportFile.Path)
			if !os.IsNotExist(err) {

				repo, err := suppression.NewBadgerRepository(path.Join(exportBaseDir, "latest-export-restore"), logger.NOP)
				require.NoError(t, err)
				file, err := os.OpenFile(latestExportFile.Path, os.O_RDONLY, 0o600)
				require.NoError(t, err)
				err = repo.Restore(file)
				require.NoError(t, err)
				metadata, err := repo.Suppressed("workspace-1", "user-1", "src-1")
				require.NoError(t, err)
				return metadata != nil
			} else {
				fmt.Println("err: ", err)
			}
			return false
		}, 3*time.Second, time.Millisecond*10, "latest export should be done in 10 seconds")
	})
	ctx.Done()
}

// exportPath creates a tmp dir and returns the path to it
func exportPath() (baseDir string, err error) {
	tmpDir, err := misc.CreateTMPDIR()
	if err != nil {
		return "", fmt.Errorf("could not create tmp dir: %w", err)
	}
	baseDir = path.Join(tmpDir, "exportV2")
	err = os.MkdirAll(baseDir, 0o700)
	if err != nil {
		return "", fmt.Errorf("could not create tmp dir: %w", err)
	}
	return
}

func handler(t *testing.T) http.Handler {
	t.Helper()
	srvMux := chi.NewMux()
	srvMux.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req)
		})
	})
	srvMux.Get("/dataplane/workspaces/{workspace_id}/regulations/suppressions", getSuppressions)
	srvMux.Get("/dataplane/namespaces/{namespace_id}/regulations/suppressions", getSuppressions)
	srvMux.Get("/workspaceConfig", getSingleTenantWorkspaceConfig)
	srvMux.Get("/data-plane/v1/namespaces/{namespace_id}/config", getMultiTenantNamespaceConfig)

	return srvMux
}

func getSuppressions(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	defaultSuppression := suppressModel.Suppression{
		Canceled:    false,
		WorkspaceID: "workspace-1",
		UserID:      "user-1",
		SourceIDs:   []string{"src-1", "src-2"},
	}
	respStruct := suppressionsResponse{
		Items: []suppressModel.Suppression{defaultSuppression},
		Token: tokenKey,
	}
	body, err := json.Marshal(respStruct)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(body)
}

type suppressionsResponse struct {
	Items []suppressModel.Suppression `json:"items"`
	Token string                      `json:"token"`
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
