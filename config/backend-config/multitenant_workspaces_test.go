package backendconfig

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMultiTenantWorkspacesConfig_GetWorkspaceIDForWriteKey(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		writeKey := "some-write-key"
		workspaceID := "some-workspace-id"
		wc := &multiTenantWorkspacesConfig{
			writeKeyToWorkspaceIDMap: map[string]string{
				writeKey: workspaceID,
			},
		}
		require.Equal(t, workspaceID, wc.GetWorkspaceIDForWriteKey(writeKey))
	})

	t.Run("not found", func(t *testing.T) {
		writeKey := "some-write-key"
		workspaceID := "some-workspace-id"
		wc := &multiTenantWorkspacesConfig{
			writeKeyToWorkspaceIDMap: map[string]string{
				writeKey: workspaceID,
			},
		}
		require.Equal(t, "", wc.GetWorkspaceIDForWriteKey("non-existent-write-key"))
	})
}

func TestMultiTenantWorkspacesConfig_GetWorkspaceIDForSourceID(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		source := "some-source"
		workspaceID := "some-workspace-id"
		wc := &multiTenantWorkspacesConfig{
			sourceToWorkspaceIDMap: map[string]string{
				source: workspaceID,
			},
		}
		require.Equal(t, workspaceID, wc.GetWorkspaceIDForSourceID(source))
	})

	t.Run("not found", func(t *testing.T) {
		source := "some-source"
		workspaceID := "some-workspace-id"
		wc := &multiTenantWorkspacesConfig{
			sourceToWorkspaceIDMap: map[string]string{
				source: workspaceID,
			},
		}
		require.Equal(t, "", wc.GetWorkspaceIDForSourceID("non-existent-source"))
	})
}

func TestMultiTenantWorkspacesConfig_GetWorkspaceLibrariesForWorkspaceID(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		workspaceID := "some-workspace-id"
		libraries := LibrariesT{{
			VersionID: "123",
		}, {
			VersionID: "456",
		}}
		wc := &multiTenantWorkspacesConfig{
			workspaceIDToLibrariesMap: map[string]LibrariesT{
				workspaceID: libraries,
			},
		}
		require.Equal(t, libraries, wc.GetWorkspaceLibrariesForWorkspaceID(workspaceID))
	})

	t.Run("not found", func(t *testing.T) {
		workspaceID := "some-workspace-id"
		libraries := LibrariesT{{
			VersionID: "123",
		}, {
			VersionID: "456",
		}}
		wc := &multiTenantWorkspacesConfig{
			workspaceIDToLibrariesMap: map[string]LibrariesT{
				workspaceID: libraries,
			},
		}
		require.Equal(t, LibrariesT{}, wc.GetWorkspaceLibrariesForWorkspaceID("non-existent-workspace-id"))
	})
}

func TestMultiTenantWorkspacesConfig_Get(t *testing.T) {
	initBackendConfig()

	t.Run("ok", func(t *testing.T) {
		var (
			secretToken            = "multitenantWorkspaceSecret"
			workspaceID            = "testWorkspaceId"
			sampleWorkspaceSources = map[string]ConfigT{
				workspaceID: sampleBackendConfig,
			}
			cpRouterURL = "mockCPRouterURL"
		)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			username, password, ok := r.BasicAuth()
			require.Equal(t, secretToken, username)
			require.Equal(t, "", password)
			require.True(t, ok)
			require.Equal(t, "application/json", r.Header.Get("Content-Type"))

			js, err := json.Marshal(sampleWorkspaceSources)
			require.NoError(t, err)
			w.WriteHeader(http.StatusAccepted)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(js)
		}))
		t.Cleanup(srv.Close)

		parsedSrvURL, err := url.Parse(srv.URL)
		require.NoError(t, err)

		wc := &multiTenantWorkspacesConfig{
			Token:            secretToken,
			configBackendURL: parsedSrvURL,
			cpRouterURL:      cpRouterURL,
		}
		conf, err := wc.Get(context.Background(), "")
		require.NoError(t, err)
		multiTenantWorkspacesConfig := sampleBackendConfig
		multiTenantWorkspacesConfig.ConnectionFlags = ConnectionFlags{URL: cpRouterURL, Services: map[string]bool{"warehouse": true}}
		require.Equal(t, multiTenantWorkspacesConfig, conf)
		require.Equal(t, workspaceID, wc.GetWorkspaceIDForWriteKey("d2"))
		require.Equal(t, workspaceID, wc.GetWorkspaceIDForWriteKey("d"))
	})

	t.Run("ok with empty response", func(t *testing.T) {
		secretToken := "multitenantWorkspaceSecret"
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			username, password, ok := r.BasicAuth()
			require.Equal(t, secretToken, username)
			require.Equal(t, "", password)
			require.True(t, ok)
			require.Equal(t, "application/json", r.Header.Get("Content-Type"))

			w.WriteHeader(http.StatusNoContent)
			w.Header().Set("Content-Type", "application/json")
		}))
		t.Cleanup(srv.Close)

		parsedSrvURL, err := url.Parse(srv.URL)
		require.NoError(t, err)

		wc := &multiTenantWorkspacesConfig{
			Token:            secretToken,
			configBackendURL: parsedSrvURL,
		}
		conf, err := wc.Get(context.Background(), "")
		require.ErrorContains(t, err, "invalid response from backend config")
		require.Equal(t, ConfigT{}, conf)
	})

	t.Run("invalid url", func(t *testing.T) {
		configBackendURL, err := url.Parse("")
		require.NoError(t, err)

		wc := &multiTenantWorkspacesConfig{
			Token:            "some-token",
			configBackendURL: configBackendURL,
		}
		conf, err := wc.Get(context.Background(), "")
		require.ErrorContains(t, err, "unsupported protocol scheme")
		require.Equal(t, ConfigT{}, conf)
	})

	t.Run("nil url", func(t *testing.T) {
		wc := &multiTenantWorkspacesConfig{
			Token:            "some-token",
			configBackendURL: nil,
		}
		conf, err := wc.Get(context.Background(), "")
		require.ErrorContains(t, err, "config backend url is nil")
		require.Equal(t, ConfigT{}, conf)
	})
}
