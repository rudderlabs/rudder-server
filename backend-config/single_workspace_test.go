package backendconfig

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/jsonrs"
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

			js, err := jsonrs.Marshal(sampleBackendConfig)
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

// TestDynamicConfigInSingleWorkspace tests that the HasDynamicConfig field is properly set
// when loading the configuration from the API in a single workspace setup.
func TestDynamicConfigInSingleWorkspace(t *testing.T) {
	initBackendConfig()

	// Create a mock server that returns a configuration with a destination that has dynamic config
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return a valid configuration with a destination that has dynamic config
		config := ConfigT{
			Sources: []SourceT{
				{
					ID: "source-1",
					Destinations: []DestinationT{
						{
							ID:         "dest-1",
							Name:       "Destination with dynamic config",
							RevisionID: "rev-1",
							Config: map[string]interface{}{
								"apiKey": "{{ message.context.apiKey || \"default-api-key\" }}",
							},
						},
						{
							ID:         "dest-2",
							Name:       "Destination without dynamic config",
							RevisionID: "rev-2",
							Config: map[string]interface{}{
								"apiKey": "static-api-key",
							},
						},
					},
				},
			},
			WorkspaceID: "workspace-1",
		}

		// Write the configuration as JSON
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		js, _ := jsonrs.Marshal(config)
		_, _ = w.Write(js)
	}))
	t.Cleanup(srv.Close)

	parsedSrvURL, err := url.Parse(srv.URL)
	require.NoError(t, err)

	// Create a single workspace config with the mock server URL
	wc := &singleWorkspaceConfig{
		token:            "testToken",
		configBackendURL: parsedSrvURL,
	}
	require.NoError(t, wc.SetUp())

	// Get the configuration from the API
	configs, err := wc.getFromAPI(context.Background())
	require.NoError(t, err)
	require.Len(t, configs, 1)

	// Get the workspace configuration
	config, ok := configs["workspace-1"]
	require.True(t, ok)

	// Verify that the HasDynamicConfig field is properly set for each destination
	require.Len(t, config.Sources, 1)
	require.Len(t, config.Sources[0].Destinations, 2)

	// Destination with dynamic config should have HasDynamicConfig=true
	require.True(t, config.Sources[0].Destinations[0].HasDynamicConfig, "Destination with dynamic config should have HasDynamicConfig=true")

	// Destination without dynamic config should have HasDynamicConfig=false
	require.False(t, config.Sources[0].Destinations[1].HasDynamicConfig, "Destination without dynamic config should have HasDynamicConfig=false")
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
							"isProcessorEnabled": false,
							"revisionID": "rev-d1"
						},
						{
							"id": "d2",
							"name": "processor Enabled",
							"isProcessorEnabled": true,
							"revisionID": "rev-d2"
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

	t.Run("file with dynamic config", func(t *testing.T) {
		data := []byte(`{
			"workspaceID": "workspace-1",
			"sources": [
				{
					"id": "source-1",
					"destinations": [
						{
							"id": "dest-1",
							"name": "Destination with dynamic config",
							"revisionId": "rev-1",
							"config": {
								"apiKey": "{{ message.context.apiKey || \"default-api-key\" }}"
							}
						},
						{
							"id": "dest-2",
							"name": "Destination without dynamic config",
							"revisionId": "rev-2",
							"config": {
								"apiKey": "static-api-key"
							}
						}
					]
				}
			]
		}`)
		tmpFile, err := os.CreateTemp("./testdata", "testSingleWorkspaceGetFromFile3")
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
		require.Len(t, conf, 1)

		// Get the workspace configuration
		config, ok := conf["workspace-1"]
		require.True(t, ok)

		// Verify that the HasDynamicConfig field is properly set for each destination
		require.Len(t, config.Sources, 1)
		require.Len(t, config.Sources[0].Destinations, 2)

		// Destination with dynamic config should have HasDynamicConfig=true
		require.True(t, config.Sources[0].Destinations[0].HasDynamicConfig, "Destination with dynamic config should have HasDynamicConfig=true")

		// Destination without dynamic config should have HasDynamicConfig=false
		require.False(t, config.Sources[0].Destinations[1].HasDynamicConfig, "Destination without dynamic config should have HasDynamicConfig=false")
	})
}
