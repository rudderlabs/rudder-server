package onelake

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeCredential struct {
	bootstrapCalls int
}

func (f *fakeCredential) Bootstrap(context.Context) error {
	f.bootstrapCalls++
	return nil
}

func (*fakeCredential) StorageToken(context.Context) (string, error) { return "storage-token", nil }

func TestManagerRoundTripAndPathConstruction(t *testing.T) {
	const workspaceID = "11111111-1111-1111-1111-111111111111"
	const lakehouseID = "22222222-2222-2222-2222-222222222222"
	var uploaded strings.Builder
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		require.Equal(t, "Bearer storage-token", request.Header.Get("Authorization"))
		require.Equal(t, "/"+workspaceID+"/"+lakehouseID+".Lakehouse/Files/folder/load.parquet", request.URL.Path)
		switch {
		case request.Method == http.MethodPut && request.URL.Query().Get("resource") == "file":
			writer.WriteHeader(http.StatusCreated)
		case request.Method == http.MethodPatch && request.URL.Query().Get("action") == "append":
			_, _ = io.Copy(&uploaded, request.Body)
			writer.WriteHeader(http.StatusAccepted)
		case request.Method == http.MethodPatch && request.URL.Query().Get("action") == "flush":
			writer.WriteHeader(http.StatusOK)
		case request.Method == http.MethodGet:
			_, _ = writer.Write([]byte(uploaded.String()))
		case request.Method == http.MethodDelete:
			writer.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected request: %s %s", request.Method, request.URL.String())
		}
	}))
	defer server.Close()

	credential := &fakeCredential{}
	manager, err := newManager(map[string]any{"host": server.URL, "fabricWorkspaceId": workspaceID, "lakehouseId": lakehouseID}, credential, server.Client())
	require.NoError(t, err)

	uploadedFile, err := manager.UploadReader(context.Background(), "folder/load.parquet", strings.NewReader("parquet-data"))
	require.NoError(t, err)
	require.Equal(t, "folder/load.parquet", uploadedFile.ObjectName)
	require.NotContains(t, uploadedFile.Location, "storage-token")

	download, err := os.CreateTemp(t.TempDir(), "download")
	require.NoError(t, err)
	require.NoError(t, manager.Download(context.Background(), download, uploadedFile.Location))
	contents, err := os.ReadFile(download.Name())
	require.NoError(t, err)
	require.Equal(t, "parquet-data", string(contents))
	require.NoError(t, manager.Delete(context.Background(), []string{uploadedFile.Location}))
	require.GreaterOrEqual(t, credential.bootstrapCalls, 1)
}

func TestManagerRejectsInvalidPathIdentifiers(t *testing.T) {
	_, err := newManager(map[string]any{
		"host": "onelake.dfs.fabric.microsoft.com", "fabricWorkspaceId": "friendly-name", "lakehouseId": "22222222-2222-2222-2222-222222222222",
	}, &fakeCredential{}, http.DefaultClient)
	require.EqualError(t, err, "fabricWorkspaceId must be a GUID")
}

func TestManagerListsFiles(t *testing.T) {
	const workspaceID = "11111111-1111-1111-1111-111111111111"
	const lakehouseID = "22222222-2222-2222-2222-222222222222"
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		require.Equal(t, "/"+workspaceID, request.URL.Path)
		require.Equal(t, "filesystem", request.URL.Query().Get("resource"))
		require.Equal(t, lakehouseID+".Lakehouse/Files/events", request.URL.Query().Get("directory"))
		_, _ = writer.Write([]byte(`{"paths":[{"name":"` + lakehouseID + `.Lakehouse/Files/events/one.parquet","lastModified":"Mon, 28 Sep 2026 12:00:00 GMT"}]}`))
	}))
	defer server.Close()

	manager, err := newManager(map[string]any{"host": server.URL, "fabricWorkspaceId": workspaceID, "lakehouseId": lakehouseID}, &fakeCredential{}, server.Client())
	require.NoError(t, err)
	files, err := manager.ListFilesWithPrefix(context.Background(), "", "events", 1).Next()
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Equal(t, "events/one.parquet", files[0].Key)
}

func TestCleanObjectNameRejectsTraversal(t *testing.T) {
	_, err := cleanObjectName("../secret")
	require.EqualError(t, err, "invalid OneLake object name")
}
