package backendconfig

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
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

		wc := &SingleWorkspaceConfig{
			Token:            token,
			configBackendURL: srv.URL,
		}
		conf, err := wc.getFromAPI(context.Background(), "")
		require.NoError(t, err)
		require.Equal(t, sampleBackendConfig, conf)
	})

	t.Run("invalid", func(t *testing.T) {
		wc := &SingleWorkspaceConfig{
			Token:            "testToken",
			configBackendURL: "://example.com",
		}
		conf, err := wc.getFromAPI(context.Background(), "")
		require.Error(t, err)
		require.Equal(t, ConfigT{}, conf)
	})
}

func TestSingleWorkspaceGetFromFile(t *testing.T) {
	initBackendConfig()

	t.Run("invalid file", func(t *testing.T) {
		wc := &SingleWorkspaceConfig{
			Token:          "testToken",
			configJSONPath: "invalid-path",
		}
		conf, err := wc.getFromFile()
		require.Error(t, err)
		require.Equal(t, ConfigT{}, conf)
	})

	t.Run("valid file but cannot parse", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("./testdata", "testSingleWorkspaceGetFromFile1")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, tmpFile.Close()) })
		t.Cleanup(func() { require.NoError(t, os.Remove(tmpFile.Name())) })

		wc := &SingleWorkspaceConfig{
			Token:          "testToken",
			configJSONPath: tmpFile.Name(),
		}
		conf, err := wc.getFromFile()
		require.Error(t, err)
		require.Equal(t, ConfigT{}, conf)
	})

	t.Run("valid file", func(t *testing.T) {
		data := []byte(`{
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

		err = ioutil.WriteFile(tmpFile.Name(), data, 0o644)
		require.NoError(t, err)

		wc := &SingleWorkspaceConfig{
			Token:          "testToken",
			configJSONPath: tmpFile.Name(),
		}
		conf, err := wc.getFromFile()
		require.NoError(t, err)
		require.Equal(t, sampleBackendConfig, conf)
	})
}
