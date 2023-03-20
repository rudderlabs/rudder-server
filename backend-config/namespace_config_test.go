package backendconfig

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
)

func Test_Namespace_SetUp(t *testing.T) {
	var (
		client = &namespaceConfig{
			logger: logger.NOP,
		}
		configBackendURL = "https://api.test.rudderstack.com"
	)
	parsedConfigBackendURL, err := url.Parse(configBackendURL)
	require.NoError(t, err)

	t.Setenv("WORKSPACE_NAMESPACE", "a-testing-namespace")
	t.Setenv("HOSTED_SERVICE_SECRET", "service-secret")
	t.Setenv("CONFIG_BACKEND_URL", parsedConfigBackendURL.String())

	require.NoError(t, client.SetUp())
	require.Equal(t, parsedConfigBackendURL, client.configBackendURL)
	require.Equal(t, "a-testing-namespace", client.namespace)
	require.Equal(t, "service-secret", client.AccessToken())
	require.Equal(t, "service-secret", client.hostedServiceSecret)
}

func Test_Namespace_Get(t *testing.T) {
	config.Reset()
	logger.Reset()

	var (
		namespace   = "free-us-1"
		cpRouterURL = "mockCpRouterURL"
	)

	be := &backendConfigServer{
		token: "service-secret",
	}
	be.AddNamespace(t, namespace, "./testdata/sample_namespace.json")

	ts := httptest.NewServer(be)
	defer ts.Close()
	httpSrvURL, err := url.Parse(ts.URL)
	require.NoError(t, err)

	client := &namespaceConfig{
		logger: logger.NOP,

		client:           ts.Client(),
		configBackendURL: httpSrvURL,

		namespace: namespace,

		hostedServiceSecret: "service-secret",
		cpRouterURL:         cpRouterURL,
	}
	require.NoError(t, client.SetUp())

	c, err := client.Get(context.Background())
	require.NoError(t, err)
	require.Len(t, c, 2)

	for workspace := range c {
		require.Equal(t, cpRouterURL, c[workspace].ConnectionFlags.URL)
		require.True(t, c[workspace].ConnectionFlags.Services["warehouse"])
	}

	t.Run("Invalid credentials", func(t *testing.T) {
		client := &namespaceConfig{
			client:           ts.Client(),
			configBackendURL: httpSrvURL,

			namespace:           namespace,
			hostedServiceSecret: "invalid-service-secret",
		}

		require.NoError(t, client.SetUp())

		c, err := client.Get(context.Background())
		require.EqualError(t, err, `backend config request failed with 401: {"message":"Unauthorized"}`) // Unauthorized
		require.Empty(t, c)
	})

	t.Run("empty namespace", func(t *testing.T) {
		client := &namespaceConfig{
			client:           ts.Client(),
			configBackendURL: httpSrvURL,

			namespace:           "namespace-does-not-exist",
			hostedServiceSecret: "service-secret",
		}

		require.NoError(t, client.SetUp())

		c, err := client.Get(context.Background())
		require.EqualError(t, err, "backend config request failed with 404")
		require.Empty(t, c)
	})
}

func Test_Namespace_Identity(t *testing.T) {
	config.Reset()
	logger.Reset()

	var (
		namespace = "free-us-1"
		secret    = "service-secret"
	)

	be := &backendConfigServer{
		token: secret,
	}

	ts := httptest.NewServer(be)
	defer ts.Close()
	httpSrvURL, err := url.Parse(ts.URL)
	require.NoError(t, err)

	client := &namespaceConfig{
		logger: logger.NOP,

		client:           ts.Client(),
		configBackendURL: httpSrvURL,

		namespace: namespace,

		hostedServiceSecret: "service-secret",
		cpRouterURL:         cpRouterURL,
	}
	require.NoError(t, client.SetUp())

	ident := client.Identity()

	require.Equal(t, &identity.Namespace{
		Namespace:    namespace,
		HostedSecret: secret,
	}, ident)
}

type backendConfigServer struct {
	responses map[string]string

	token string
}

func (server *backendConfigServer) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	user, _, ok := req.BasicAuth()
	if !ok || user != server.token {
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

	server.responses["/data-plane/v1/namespaces/"+namespace+"/config"] = string(payload)
}
