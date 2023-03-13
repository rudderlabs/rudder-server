package exporter_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	suppression "github.com/rudderlabs/rudder-server/enterprise/suppress-user"
	suppressModel "github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/suppression-backup-service/exporter"
	"github.com/rudderlabs/rudder-server/suppression-backup-service/model"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/stretchr/testify/require"
)

const (
	tokenKey    = "__token__"
	namespaceID = "spaghetti"
)

// test export function
func TestExport(t *testing.T) {
	misc.Init()
	basePath := path.Join(t.TempDir(), strings.ReplaceAll(uuid.New().String(), "-", ""))
	token := []byte("token")
	repo, err := suppression.NewBadgerRepository(basePath, logger.NOP)
	require.NoError(t, err)
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

	isSuppressed, err := repo.Suppressed("workspace1", "user1", "source1")
	require.NoError(t, err)
	require.Equal(t, true, isSuppressed)

	file, err := os.CreateTemp("", "export")
	require.NoError(t, err)
	require.NoError(t, exporter.Export(repo, model.File{Path: file.Name(), Mu: &sync.RWMutex{}}), "could not export data")
	file.Close()

	file, err = os.Open(file.Name())
	require.NoError(t, err)
	originalBackup, err := io.ReadAll(file)
	require.NoError(t, err)

	// reading golden file
	goldenFile, err := os.OpenFile("testdata/goldenFile.txt", os.O_RDONLY, 0o400)
	require.NoError(t, err)
	expectedBackup, err := io.ReadAll(goldenFile)
	require.NoError(t, err)

	require.Equal(t, expectedBackup, originalBackup)
	require.NoError(t, file.Close())
	require.NoError(t, os.Remove(file.Name()))
	require.NoError(t, os.RemoveAll(basePath))
}

func TestExportLoop(t *testing.T) {
	misc.Init()
	srv := httptest.NewServer(handler(t))
	defer t.Cleanup(srv.Close)
	t.Setenv("WORKSPACE_TOKEN", "216Co97d9So9TkqphM0cxBzRxc3")
	t.Setenv("WORKSPACE_NAMESPACE", "216Co97d9So9TkqphM0cxBzRxc3")
	t.Setenv("CONFIG_BACKEND_URL", srv.URL)
	t.Setenv("SUPPRESS_USER_BACKEND_URL", srv.URL)
	t.Setenv("DEST_TRANSFORM_URL", "http://localhost:9090")
	t.Setenv("URL_PREFIX", srv.URL)

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
				isSuppressed, err := repo.Suppressed("workspace-1", "user-1", "src-1")
				require.NoError(t, err)
				return isSuppressed
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
				isSuppressed, err := repo.Suppressed("workspace-1", "user-1", "src-1")
				require.NoError(t, err)
				return isSuppressed
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
	baseDir = path.Join(tmpDir, "export")
	err = os.MkdirAll(baseDir, 0o700)
	if err != nil {
		return "", fmt.Errorf("could not create tmp dir: %w", err)
	}
	return
}

func handler(t *testing.T) http.Handler {
	t.Helper()
	srvMux := mux.NewRouter()
	srvMux.HandleFunc("/dataplane/workspaces/{workspace_id}/regulations/suppressions", getSuppressions).Methods(http.MethodGet)
	srvMux.HandleFunc("/dataplane/namespaces/{namespace_id}/regulations/suppressions", getSuppressions).Methods(http.MethodGet)
	srvMux.HandleFunc("/workspaceConfig", getSingleTenantWorkspaceConfig).Methods(http.MethodGet)
	srvMux.HandleFunc("/data-plane/v1/namespaces/{namespace_id}/config", getMultiTenantNamespaceConfig).Methods(http.MethodGet)

	srvMux.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req)
		})
	})

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
