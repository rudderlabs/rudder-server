package lyticsBulkUpload

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUploadBulkFileSuccessAndError(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "bulk.csv")
	require.NoError(t, os.WriteFile(filePath, []byte("col1,col2\n1,2\n"), 0o644))

	t.Run("success", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(server.Close)

		err := (&LyticsServiceImpl{}).UploadBulkFile(&HttpRequestData{Endpoint: server.URL}, filePath)
		require.NoError(t, err)
	})

	t.Run("http error status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusInternalServerError)
		}))
		t.Cleanup(server.Close)

		err := (&LyticsServiceImpl{}).UploadBulkFile(&HttpRequestData{Endpoint: server.URL}, filePath)
		require.Error(t, err)
	})

	t.Run("missing file", func(t *testing.T) {
		err := (&LyticsServiceImpl{}).UploadBulkFile(&HttpRequestData{Endpoint: "http://127.0.0.1"}, filepath.Join(t.TempDir(), "missing.csv"))
		require.Error(t, err)
	})
}
