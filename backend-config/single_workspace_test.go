package backendconfig

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
)

func TestSingleWorkspaceGetFromAPI(t *testing.T) {
	initBackendConfig()

	t.Run("success", func(t *testing.T) {
		token := "testToken"
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			username, password, ok := r.BasicAuth()
			require.Equal(t, token, username)
			require.Equal(t, "", password)
			require.True(t, ok)
			require.Equal(t, "application/json", r.Header.Get("Content-Type"))

			js, err := json.Marshal(sampleBackendConfig)
			require.NoError(t, err)
			w.WriteHeader(http.StatusAccepted)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(js)
		}))
		t.Cleanup(srv.Close)

		parsedSrvURL, err := url.Parse(srv.URL)
		require.NoError(t, err)

		wc := &singleWorkspaceConfig{
			token:            token,
			configBackendURL: parsedSrvURL,
		}
		require.NoError(t, wc.SetUp())
		conf, err := wc.getFromAPI(context.Background())
		require.NoError(t, err)
		require.Equal(t, map[string]ConfigT{sampleWorkspaceID: sampleBackendConfig}, conf)

		ident := wc.Identity()
		require.Equal(t, &identity.Workspace{
			WorkspaceID:    sampleBackendConfig.WorkspaceID,
			WorkspaceToken: token,
		}, ident)
	})

	t.Run("invalid url", func(t *testing.T) {
		configBackendURL, err := url.Parse("")
		require.NoError(t, err)

		wc := &singleWorkspaceConfig{
			token:            "testToken",
			configBackendURL: configBackendURL,
		}
		require.NoError(t, wc.SetUp())
		conf, err := wc.getFromAPI(context.Background())
		require.ErrorContains(t, err, "unsupported protocol scheme")
		require.Equal(t, map[string]ConfigT{}, conf)
	})

	t.Run("nil url", func(t *testing.T) {
		wc := &singleWorkspaceConfig{
			token:            "testToken",
			configBackendURL: nil,
		}
		require.NoError(t, wc.SetUp())
		conf, err := wc.getFromAPI(context.Background())
		require.ErrorContains(t, err, "config backend url is nil")
		require.Equal(t, map[string]ConfigT{}, conf)
	})
}

func TestSingleWorkspaceGetFromFile(t *testing.T) {
	initBackendConfig()

	t.Run("invalid file", func(t *testing.T) {
		wc := &singleWorkspaceConfig{
			token:          "testToken",
			configJSONPath: "invalid-path",
		}
		require.NoError(t, wc.SetUp())
		conf, err := wc.getFromFile()
		require.Error(t, err)
		require.Equal(t, map[string]ConfigT{}, conf)
	})

	t.Run("valid file but cannot parse", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("./testdata", "testSingleWorkspaceGetFromFile1")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, tmpFile.Close()) })
		t.Cleanup(func() { require.NoError(t, os.Remove(tmpFile.Name())) })

		wc := &singleWorkspaceConfig{
			token:          "testToken",
			configJSONPath: tmpFile.Name(),
		}
		require.NoError(t, wc.SetUp())
		conf, err := wc.getFromFile()
		require.Error(t, err)
		require.Equal(t, map[string]ConfigT{}, conf)
	})

	t.Run("valid file", func(t *testing.T) {
		data := []byte(`{
			"workspaceID": "sampleWorkspaceID",
			"sources": [
				{
					"id": "1",
					"writeKey": "d",
					"enabled": false
				},
				{
					"id": "2",
					"writeKey": "d2",
					"enabled": false,
					"destinations": [
						{
							"id": "d1",
							"name": "processor Disabled",
							"isProcessorEnabled": false
						},
						{
							"id": "d2",
							"name": "processor Enabled",
							"isProcessorEnabled": true
						}
					]
				}
			]
		}`)
		tmpFile, err := os.CreateTemp("./testdata", "testSingleWorkspaceGetFromFile2")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, tmpFile.Close()) })
		t.Cleanup(func() { require.NoError(t, os.Remove(tmpFile.Name())) })

		err = os.WriteFile(tmpFile.Name(), data, 0o600)
		require.NoError(t, err)

		wc := &singleWorkspaceConfig{
			token:          "testToken",
			configJSONPath: tmpFile.Name(),
		}
		require.NoError(t, wc.SetUp())
		conf, err := wc.getFromFile()
		require.NoError(t, err)
		require.Equal(t, map[string]ConfigT{sampleWorkspaceID: sampleBackendConfig}, conf)
	})
}
