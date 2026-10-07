package onelake

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/filemanager"
)

type staticCredential struct {
	token  string
	scopes []string
}

func (c *staticCredential) GetToken(_ context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
	c.scopes = append([]string(nil), options.Scopes...)
	return azcore.AccessToken{Token: c.token, ExpiresOn: time.Now().Add(time.Hour)}, nil
}

type writerAtBuffer struct {
	mu   sync.Mutex
	data []byte
}

func (w *writerAtBuffer) WriteAt(data []byte, offset int64) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	end := int(offset) + len(data)
	if end > len(w.data) {
		w.data = append(w.data, make([]byte, end-len(w.data))...)
	}
	copy(w.data[offset:], data)
	return len(data), nil
}

func testConfig(host string) config {
	return config{
		Host:              host,
		FabricWorkspaceID: "11111111-1111-1111-1111-111111111111",
		LakehouseID:       "22222222-2222-2222-2222-222222222222",
		TenantID:          "tenant",
		ClientID:          "client",
		ClientSecret:      "secret",
		Prefix:            "rudder-prefix",
	}
}

func TestValidateConfig(t *testing.T) {
	cfg := testConfig(defaultOneLakeHost)
	require.NoError(t, validateConfig(cfg))

	cfg.FabricWorkspaceID = "workspace-name"
	require.ErrorContains(t, validateConfig(cfg), "canonical GUID")

	cfg = testConfig(defaultOneLakeHost)
	cfg.FabricWorkspaceID = "{11111111-1111-1111-1111-111111111111}"
	require.ErrorContains(t, validateConfig(cfg), "canonical GUID")

	cfg = testConfig("https://onelake.dfs.fabric.microsoft.com/path")
	require.ErrorContains(t, validateConfig(cfg), "hostname")

	cfg = testConfig("attacker.example")
	require.ErrorContains(t, validateConfig(cfg), "Microsoft OneLake endpoint")
}

func TestHostFromConfigDefaultsAndRejectsArbitraryHosts(t *testing.T) {
	host, err := HostFromConfig(map[string]any{"host": "sql.fabric.example"})
	require.NoError(t, err)
	require.Equal(t, defaultOneLakeHost, host)

	host, err = HostFromConfig(map[string]any{"host": "sql.fabric.example", "oneLakeHost": "https://onelake.dfs.fabric.microsoft.com"})
	require.NoError(t, err)
	require.Equal(t, defaultOneLakeHost, host)

	_, err = HostFromConfig(map[string]any{"oneLakeHost": "attacker.example"})
	require.ErrorContains(t, err, "Microsoft OneLake endpoint")
}

func TestObjectLocationAndParsing(t *testing.T) {
	manager := newManager(testConfig(defaultOneLakeHost), &staticCredential{}, http.DefaultClient)
	locationURL := manager.objectURL("folder/a name.parquet")
	location := locationURL.String()
	require.Equal(t, "https://onelake.dfs.fabric.microsoft.com/11111111-1111-1111-1111-111111111111/22222222-2222-2222-2222-222222222222/Files/folder/a%20name.parquet", location)

	name, err := manager.GetObjectNameFromLocation(location)
	require.NoError(t, err)
	require.Equal(t, "folder/a name.parquet", name)

	_, err = manager.GetObjectNameFromLocation("https://other.example/file.parquet")
	require.ErrorContains(t, err, "outside the configured OneLake Lakehouse")
}

func TestUploadDownloadDeleteAndList(t *testing.T) {
	var stored []byte
	var uploadedPath string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer token", r.Header.Get("Authorization"))
		switch {
		case r.Method == http.MethodPut && r.URL.Query().Get("resource") == "file":
			uploadedPath = r.URL.EscapedPath()
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodPatch && r.URL.Query().Get("action") == "append":
			require.Equal(t, int64(13), r.ContentLength)
			require.Empty(t, r.TransferEncoding)
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			stored = append(stored, body...)
			w.WriteHeader(http.StatusAccepted)
		case r.Method == http.MethodPatch && r.URL.Query().Get("action") == "flush":
			require.Equal(t, "13", r.URL.Query().Get("position"))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Query().Get("resource") == "filesystem":
			w.Header().Set("x-ms-continuation", "")
			_, _ = io.WriteString(w, `{"paths":[{"name":"rudder-prefix/load/file.parquet","lastModified":"Wed, 21 Oct 2015 07:28:00 GMT"}]}`)
		case r.Method == http.MethodGet:
			_, _ = w.Write(stored)
		case r.Method == http.MethodDelete:
			stored = nil
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "https://")
	credential := &staticCredential{token: "token"}
	manager := newManager(testConfig(host), credential, server.Client())

	uploaded, err := manager.UploadReader(context.Background(), "load/file.parquet", io.LimitReader(strings.NewReader("hello OneLake"), 13))
	require.NoError(t, err)
	require.Equal(t, "rudder-prefix/load/file.parquet", uploaded.ObjectName)
	require.Contains(t, uploaded.Location, "/Files/rudder-prefix/load/file.parquet")
	require.Equal(t, "/11111111-1111-1111-1111-111111111111/22222222-2222-2222-2222-222222222222/Files/rudder-prefix/load/file.parquet", uploadedPath)
	require.Equal(t, []string{storageScope}, credential.scopes)

	output := &writerAtBuffer{}
	require.NoError(t, manager.Download(context.Background(), output, uploaded.Location))
	require.Equal(t, []byte("hello OneLake"), output.data)

	files, err := manager.ListFilesWithPrefix(context.Background(), "", "load", 10).Next()
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Equal(t, "rudder-prefix/load/file.parquet", files[0].Key)

	require.NoError(t, manager.Delete(context.Background(), []string{uploaded.Location}))
	require.Empty(t, stored)
}

func TestUploadUsesFileSizeForContentLength(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("action") {
		case "append":
			require.Equal(t, int64(7), r.ContentLength)
			require.Empty(t, r.TransferEncoding)
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			require.Equal(t, []byte("payload"), body)
			w.WriteHeader(http.StatusAccepted)
		case "flush":
			require.Equal(t, "7", r.URL.Query().Get("position"))
			w.WriteHeader(http.StatusOK)
		default:
			require.Equal(t, "file", r.URL.Query().Get("resource"))
			w.WriteHeader(http.StatusCreated)
		}
	}))
	defer server.Close()

	file, err := os.CreateTemp(t.TempDir(), "payload-*.parquet")
	require.NoError(t, err)
	_, err = file.WriteString("payload")
	require.NoError(t, err)

	manager := newManager(testConfig(strings.TrimPrefix(server.URL, "https://")), &staticCredential{token: "token"}, server.Client())
	_, err = manager.Upload(context.Background(), file, "load")
	require.NoError(t, err)
}

func TestDownloadRejectsRangeOptions(t *testing.T) {
	manager := newManager(testConfig(defaultOneLakeHost), &staticCredential{}, http.DefaultClient)
	err := manager.Download(context.Background(), &writerAtBuffer{}, "file.parquet", filemanager.WithDownloadOffSetAndLength(2, 4))
	require.ErrorContains(t, err, "range downloads are unsupported")
}

func TestObjectNamesRejectTraversalAndExternalLocations(t *testing.T) {
	manager := newManager(testConfig(defaultOneLakeHost), &staticCredential{}, http.DefaultClient)
	for _, objectName := range []string{"", "/absolute.parquet", "../escape.parquet", "folder/../escape.parquet", `folder\file.parquet`, "folder//file.parquet"} {
		_, err := manager.UploadReader(context.Background(), objectName, strings.NewReader("payload"))
		require.ErrorContains(t, err, "object name is invalid", objectName)
	}

	externalLocation := "https://other.example/file.parquet"
	require.Empty(t, manager.GetDownloadKeyFromFileLocation(externalLocation))
	require.ErrorContains(t, manager.Download(context.Background(), &writerAtBuffer{}, externalLocation), "outside the configured")
	require.ErrorContains(t, manager.Delete(context.Background(), []string{externalLocation}), "outside the configured")
}

func TestListFollowsContinuation(t *testing.T) {
	var requests int
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if requests == 1 {
			require.Empty(t, r.URL.Query().Get("continuation"))
			w.Header().Set("x-ms-continuation", "next-page")
			_, _ = io.WriteString(w, `{"paths":[{"name":"rudder-prefix/a.parquet"}]}`)
			return
		}
		require.Equal(t, "next-page", r.URL.Query().Get("continuation"))
		_, _ = io.WriteString(w, `{"paths":[{"name":"rudder-prefix/b.parquet"}]}`)
	}))
	defer server.Close()
	manager := newManager(testConfig(strings.TrimPrefix(server.URL, "https://")), &staticCredential{token: "token"}, server.Client())
	session := manager.ListFilesWithPrefix(context.Background(), "", "", 10)

	files, err := session.Next()
	require.NoError(t, err)
	require.Equal(t, "rudder-prefix/a.parquet", files[0].Key)
	files, err = session.Next()
	require.NoError(t, err)
	require.Equal(t, "rudder-prefix/b.parquet", files[0].Key)
	require.Equal(t, 2, requests)
}

func TestRequestErrorsDoNotExposeTokenOrURL(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()
	manager := newManager(testConfig(strings.TrimPrefix(server.URL, "https://")), &staticCredential{token: "sensitive-token"}, server.Client())

	_, err := manager.UploadReader(context.Background(), "payload.parquet", bytes.NewReader(nil))
	require.Error(t, err)
	require.NotContains(t, err.Error(), "sensitive-token")
	require.NotContains(t, err.Error(), server.URL)
}

func TestGetObjectNameRejectsMalformedURL(t *testing.T) {
	manager := newManager(testConfig(defaultOneLakeHost), &staticCredential{}, http.DefaultClient)
	_, err := manager.GetObjectNameFromLocation("https://onelake.dfs.fabric.microsoft.com/%zz")
	var urlError *url.Error
	require.ErrorAs(t, err, &urlError)
}
