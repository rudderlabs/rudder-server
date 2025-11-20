package exporter_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
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
	tmpDir, err := misc.GetTmpDir()
	if err != nil {
		return "", fmt.Errorf("could not create tmp dir: %w", err)
	}
	baseDir = path.Join(tmpDir, "exportV2")
	err = os.MkdirAll(baseDir, 0o700)
	if err != nil {
		return "", fmt.Errorf("could not create tmp dir: %w", err)
	}
	return baseDir, err
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
	body, err := jsonrs.Marshal(respStruct)
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
	body, err := jsonrs.Marshal(config)
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
	body, err := jsonrs.Marshal(config)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(body)
}

func TestCleanupLingeringExportFiles(t *testing.T) {
	misc.Init()

	tests := []struct {
		name        string
		setupFiles  []string
		expectedErr bool
		description string
	}{
		{
			name:        "no files to cleanup",
			setupFiles:  []string{},
			expectedErr: false,
			description: "should succeed when no tmp-export files exist",
		},
		{
			name:        "single tmp-export file",
			setupFiles:  []string{exporter.TmpExportFilePrefix + "-123"},
			expectedErr: false,
			description: "should remove a single tmp-export file",
		},
		{
			name:        "multiple tmp-export files",
			setupFiles:  []string{exporter.TmpExportFilePrefix + "-123", exporter.TmpExportFilePrefix + "-456", exporter.TmpExportFilePrefix + "-789"},
			expectedErr: false,
			description: "should remove multiple tmp-export files",
		},
		{
			name:        "mixed files with tmp-export prefix",
			setupFiles:  []string{exporter.TmpExportFilePrefix + "-123", "other-file", exporter.TmpExportFilePrefix + "-456", "regular-file.txt"},
			expectedErr: false,
			description: "should only remove files with tmp-export prefix",
		},
		{
			name:        "files with similar names but different prefix",
			setupFiles:  []string{"export-123", "tmp-123", "tmpexport-456", exporter.TmpExportFilePrefix + "tmp-export-real"},
			expectedErr: false,
			description: "should only remove files that start with exactly 'tmp-export'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpExportFilePrefixLength := len(exporter.TmpExportFilePrefix)
			// Get the temp directory
			tmpDir, err := misc.GetTmpDir()
			require.NoError(t, err, "failed to get tmp dir")

			// Create test files
			var createdFiles []string
			var tmpExportFiles []string
			for _, fileName := range tt.setupFiles {
				filePath := path.Join(tmpDir, fileName)
				file, err := os.Create(filePath)
				require.NoError(t, err, "failed to create test file: %s", fileName)
				require.NoError(t, file.Close(), "failed to close test file: %s", fileName)
				createdFiles = append(createdFiles, filePath)
				if len(fileName) >= tmpExportFilePrefixLength && fileName[:tmpExportFilePrefixLength] == exporter.TmpExportFilePrefix {
					tmpExportFiles = append(tmpExportFiles, filePath)
				}
			}

			// Verify files were created
			for _, filePath := range createdFiles {
				_, err := os.Stat(filePath)
				require.NoError(t, err, "test file should exist before cleanup: %s", filePath)
			}

			// Run the cleanup function
			err = exporter.CleanupLingeringTmpExportFiles()

			// Check expectations
			if tt.expectedErr {
				require.Error(t, err, "expected an error but got none")
			} else {
				require.NoError(t, err, "expected no error but got: %v", err)

				// Verify tmp-export files were removed
				for _, filePath := range tmpExportFiles {
					_, err := os.Stat(filePath)
					require.True(t, os.IsNotExist(err), "tmp-export file should be removed: %s", filePath)
				}

				// Verify other files still exist
				for _, filePath := range createdFiles {
					fileName := path.Base(filePath)
					if len(fileName) < tmpExportFilePrefixLength || fileName[:tmpExportFilePrefixLength] != exporter.TmpExportFilePrefix {
						_, err := os.Stat(filePath)
						require.NoError(t, err, "non-tmp-export file should still exist: %s", filePath)
					}
				}
			}

			// Cleanup remaining test files
			for _, filePath := range createdFiles {
				_ = os.Remove(filePath)
			}
		})
	}
}
