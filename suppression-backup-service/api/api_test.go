package api_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/phayes/freeport"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/suppression-backup-service/api"
	"github.com/rudderlabs/rudder-server/suppression-backup-service/model"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// exportPath creates a tmp dir and returns the path to it
func exportPath() (baseDir string, err error) {
	tmpDir, err := misc.CreateTMPDIR()
	if err != nil {
		return "", fmt.Errorf("could not create tmp dir: %w", err)
	}
	baseDir, err = os.MkdirTemp(tmpDir, "exportV2")
	if err != nil {
		return "", fmt.Errorf("could not create tmp dir: %w", err)
	}
	return
}

func TestDataplaneAPIHandler(t *testing.T) {
	config.Reset()
	logger.Reset()
	misc.Init()
	latestExportPostfix := "latest-export"
	fullExportPostfix := "full-export"
	tests := []struct {
		name                 string
		endpoint             string
		method               string
		responseCode         int
		expectedResponseBody string
		random               string
		expectedEncoding     string
	}{
		{
			name:                 "get full backup badgerdb file",
			endpoint:             "/full-export",
			method:               http.MethodGet,
			responseCode:         http.StatusOK,
			expectedResponseBody: `this file contains full mock data exported by exporter.`,
		},
		{
			name:                 "get latest backup badgerdb file",
			endpoint:             "/latest-export",
			method:               http.MethodGet,
			responseCode:         http.StatusOK,
			expectedResponseBody: `this file contains latest mock data exported by exporter.`,
		},
		{
			name:         "requested file not found, to test if service has not exported any file yet.",
			endpoint:     "/latest-export",
			method:       http.MethodGet,
			responseCode: http.StatusNotFound,
			random:       "random",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Log("endpoint tested:", tt.endpoint)
			port, err := freeport.GetFreePort()
			require.NoError(t, err)
			baseUrl := fmt.Sprintf("http://localhost:%d", port)
			req, err := http.NewRequest(tt.method, baseUrl+tt.endpoint, http.NoBody)
			require.NoError(t, err)
			resp := httptest.NewRecorder()
			exportBaseDir, err := exportPath()
			require.NoError(t, err)
			defer os.RemoveAll(exportBaseDir)

			latestGoldenFile, err := os.OpenFile("./testdata/latestExport.golden", os.O_RDONLY, 0o400)
			require.NoError(t, err)
			defer func() { _ = latestGoldenFile.Close() }()

			fullGoldenFile, err := os.OpenFile("./testdata/fullExport.golden", os.O_RDONLY, 0o400)
			require.NoError(t, err)
			defer func() { _ = fullGoldenFile.Close() }()

			latestExporterFile, err := os.Create(path.Join(exportBaseDir, latestExportPostfix))
			require.NoError(t, err)
			_, err = io.Copy(latestExporterFile, latestGoldenFile)
			require.NoError(t, err)
			latestExporterFile.Close()

			fullExporterFile, err := os.Create(path.Join(exportBaseDir, fullExportPostfix))
			require.NoError(t, err)
			_, err = io.Copy(fullExporterFile, fullGoldenFile)
			require.NoError(t, err)
			fullExporterFile.Close()

			api := api.NewAPI(logger.NOP, model.File{Path: path.Join(exportBaseDir, fullExportPostfix), Mu: &sync.RWMutex{}}, model.File{Path: path.Join(exportBaseDir, fmt.Sprint(latestExportPostfix+tt.random)), Mu: &sync.RWMutex{}})
			api.Handler(context.Background()).ServeHTTP(resp, req)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, tt.responseCode, resp.Code, "Response code mismatch")
			require.Equal(t, http.DetectContentType(body), resp.Header().Get("Content-Type"))

			if tt.responseCode < 300 && tt.responseCode >= 200 {
				require.Equal(t, tt.expectedResponseBody, string(body))
				fmt.Println("content encoding: ", resp.Header().Get("Content-Encoding"))
				require.Equal(t, tt.expectedEncoding, resp.Header().Get("Content-Encoding"))
			}
		})
	}
}
