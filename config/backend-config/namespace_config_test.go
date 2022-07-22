package backendconfig_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/require"
)

type backendConfigServer struct {
	responses map[string]string

	authUser string
	authPass string
}

func (server *backendConfigServer) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	u, p, ok := req.BasicAuth()
	if !ok || u != server.authUser || p != server.authPass {
		resp.WriteHeader(http.StatusUnauthorized)
		_, _ = resp.Write([]byte(`{"message":"Unauthorized"}`))
		return
	}

	body, ok := server.responses[req.URL.Path]
	if !ok {
		resp.WriteHeader(http.StatusNotFound)
		return
	}

	resp.WriteHeader(http.StatusOK)
	_, _ = resp.Write([]byte(body))
}

func (server *backendConfigServer) AddNamespace(t *testing.T, namespace, path string) {
	t.Helper()

	if server.responses == nil {
		server.responses = make(map[string]string)
	}

	payload, err := os.ReadFile(path)
	require.NoError(t, err)

	server.responses["/dataPlane/v1/namespace/"+namespace+"/config"] = string(payload)
}

func Test_Namespace_SetUp(t *testing.T) {
	client := &backendconfig.NamespaceConfig{}

	t.Log("test defaults")

	t.Setenv("WORKSPACE_NAMESPACE", "a-testing-namespace")
	t.Setenv("CONTROL_PLANE_BASIC_AUTH_USERNAME", "username")
	t.Setenv("CONTROL_PLANE_BASIC_AUTH_PASSWORD", "password")
	t.Setenv("HOSTED_MULTITENANT_SERVICE_SECRET", "service-secret")

	t.Setenv("CONFIG_BACKEND_URL", "https://api.test.rudderlabs.com")

	err := client.SetUp()
	require.NoError(t, err)

	require.Equal(t, "username", client.BasicAuthUsername)
	require.Equal(t, "password", client.BasicAuthPassword)
	require.Equal(t, "https://api.test.rudderlabs.com", client.ConfigBackendURL)
	require.Equal(t, "a-testing-namespace", client.Namespace)
	require.Equal(t, "service-secret", client.AccessToken())
	require.Equal(t, "service-secret", client.ServiceSecret)
}

func Test_Namespace_Get(t *testing.T) {
	logger.Init()

	namespace := "free-us-1"

	be := &backendConfigServer{
		authUser: "cp-user",
		authPass: "cp-password",
	}
	be.AddNamespace(t, namespace, "./testdata/sample_namespace.json")

	ts := httptest.NewServer(be)
	defer ts.Close()

	client := &backendconfig.NamespaceConfig{
		Logger: logger.NewLogger(),

		Client:           ts.Client(),
		ConfigBackendURL: ts.URL,

		Namespace:         namespace,
		BasicAuthUsername: "cp-user",
		BasicAuthPassword: "cp-password",

		ServiceSecret: "service-secret",
	}

	err := client.SetUp()
	require.NoError(t, err)

	c, err := client.Get(context.Background(), "2CCgbmvBSa8Mv81YaIgtR36M7aW")
	require.NoError(t, err)
	require.Equal(t, "", c.WorkspaceID)
	require.Len(t, c.Sources, 3)

	t.Log("correct writeKey to workspaceID mapping")
	{
		require.Equal(t, "2CCgbmvBSa8Mv81YaIgtR36M7aW", client.GetWorkspaceIDForWriteKey("2CCggSFf....jBLNxmXtSlvZ"))
		require.Equal(t, "2CCgbmvBSa8Mv81YaIgtR36M7aW", client.GetWorkspaceIDForWriteKey("2CCgpXME....WBD9C5nQtsFg"))
		require.Equal(t, "2CChLejq5aIWi3qsKVm1PjHkyTj", client.GetWorkspaceIDForWriteKey("2CChOrwP....9qESA9FgLFXL"))
	}

	t.Log("correct sourceID to workspaceID mapping")
	{
		require.Equal(t, "2CCgbmvBSa8Mv81YaIgtR36M7aW", client.GetWorkspaceIDForSourceID("2CCggVGqbSRLhqP8trntINSihFe"))
		require.Equal(t, "2CCgbmvBSa8Mv81YaIgtR36M7aW", client.GetWorkspaceIDForSourceID("2CCgpZlqlXRDRz8rChhQKtuwqKA"))
		require.Equal(t, "2CChLejq5aIWi3qsKVm1PjHkyTj", client.GetWorkspaceIDForSourceID("2CChOtDTWeXIQiRmHMU56C3htPf"))
	}

	for _, workspaceID := range []string{"2CCgbmvBSa8Mv81YaIgtR36M7aW", "2CChLejq5aIWi3qsKVm1PjHkyTj"} {
		require.Equal(t,
			backendconfig.LibrariesT{
				{
					VersionID: "20MirO0IhCtS39Qjva2PSAbA9KM",
				},
				{
					VersionID: "ghi",
				},
				{
					VersionID: "2AWJpFCIGcpZhOrsIp7Kasw72vb",
				},
				{
					VersionID: "2AWIMafC3YPKHXazWWvVn5hSGnR",
				},
			},
			client.GetWorkspaceLibrariesForWorkspaceID(workspaceID),
		)
	}

	t.Run("Invalid credentials", func(t *testing.T) {
		client := &backendconfig.NamespaceConfig{
			Client:           ts.Client(),
			ConfigBackendURL: ts.URL,

			Namespace:         namespace,
			BasicAuthUsername: "cp-user",
			BasicAuthPassword: "cp-wrong-password",
			ServiceSecret:     "service-secret",
		}

		err := client.SetUp()
		require.NoError(t, err)

		c, err := client.Get(context.Background(), "")
		require.EqualError(t, err, "unexpected status code: 401")
		require.Empty(t, c)
	})

	t.Run("empty namespace", func(t *testing.T) {
		client := &backendconfig.NamespaceConfig{
			Client:           ts.Client(),
			ConfigBackendURL: ts.URL,

			Namespace:         "namespace-does-not-exist",
			BasicAuthUsername: "cp-user",
			BasicAuthPassword: "cp-password",
			ServiceSecret:     "service-secret",
		}

		err := client.SetUp()
		require.NoError(t, err)
		c, err := client.Get(context.Background(), "2CCgbmvBSa8Mv81YaIgtR36M7aW")
		require.EqualError(t, err, "unexpected status code: 404")
		require.Empty(t, c)
	})
}
