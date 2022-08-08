package backendconfig

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
			workspaceId            = "testWorkspaceId"
			sampleWorkspaceSources = map[string]ConfigT{
				workspaceId: sampleBackendConfig,
			}
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

		wc := &multiTenantWorkspacesConfig{
			Token:            secretToken,
			configBackendURL: srv.URL,
		}
		conf, err := wc.Get(context.Background(), "")
		require.NoError(t, err)
		require.Equal(t, sampleBackendConfig, conf)
		require.Equal(t, workspaceId, wc.GetWorkspaceIDForWriteKey("d2"))
		require.Equal(t, workspaceId, wc.GetWorkspaceIDForWriteKey("d"))
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

		wc := &multiTenantWorkspacesConfig{
			Token:            secretToken,
			configBackendURL: srv.URL,
		}
		conf, err := wc.Get(context.Background(), "")
		require.ErrorContains(t, err, "invalid response from backend config")
		require.Equal(t, ConfigT{}, conf)
	})

	t.Run("invalid request", func(t *testing.T) {
		wc := &multiTenantWorkspacesConfig{
			Token:            "some-token",
			configBackendURL: "://example.com",
		}
		conf, err := wc.Get(context.Background(), "")
		require.ErrorContains(t, err, "missing protocol scheme")
		require.Equal(t, ConfigT{}, conf)
	})

	t.Run("invalid URL", func(t *testing.T) {
		wc := &multiTenantWorkspacesConfig{
			Token:            "some-token",
			configBackendURL: "invalid",
		}
		conf, err := wc.Get(context.Background(), "")
		require.Error(t, err)
		require.Equal(t, ConfigT{}, conf)
	})
}
