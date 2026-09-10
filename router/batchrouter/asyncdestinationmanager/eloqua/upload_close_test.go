package eloqua

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUploadDataSuccessAndError(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "bulk.csv")
	require.NoError(t, os.WriteFile(filePath, []byte("C_EmailAddress\ntest@mail.com\n"), 0o644))
	svc := NewEloquaServiceImpl("2.0")

	t.Run("success", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusNoContent)
		}))
		t.Cleanup(server.Close)

		err := svc.UploadData(&HttpRequestData{
			BaseEndpoint: server.URL,
			DynamicPart:  "/imports/1",
		}, filePath)
		require.NoError(t, err)
	})

	t.Run("http error status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusBadRequest)
		}))
		t.Cleanup(server.Close)

		err := svc.UploadData(&HttpRequestData{
			BaseEndpoint: server.URL,
			DynamicPart:  "/imports/1",
		}, filePath)
		require.Error(t, err)
	})

	t.Run("missing file", func(t *testing.T) {
		err := svc.UploadData(&HttpRequestData{
			BaseEndpoint: "http://127.0.0.1",
			DynamicPart:  "/imports/1",
		}, filepath.Join(t.TempDir(), "missing.csv"))
		require.Error(t, err)
	})
}
