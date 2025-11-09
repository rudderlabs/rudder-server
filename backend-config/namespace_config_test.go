package backendconfig

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
)

func Test_Namespace_SetUp(t *testing.T) {
	var (
		conf   = config.New()
		client = &namespaceConfig{
			config: conf,
			logger: logger.NOP,
		}
		configBackendURL = "https://api.test.rudderstack.com"
	)
	parsedConfigBackendURL, err := url.Parse(configBackendURL)
	require.NoError(t, err)

	conf.Set("WORKSPACE_NAMESPACE", "a-testing-namespace")
	conf.Set("HOSTED_SERVICE_SECRET", "service-secret")
	conf.Set("CONFIG_BACKEND_URL", parsedConfigBackendURL.String())

	require.NoError(t, client.SetUp())
	require.Equal(t, parsedConfigBackendURL, client.configBackendURL)
	require.Equal(t, "a-testing-namespace", client.namespace)
	require.Equal(t, "service-secret", client.AccessToken())
	require.Equal(t, "service-secret", client.hostedServiceSecret)
}

func Test_Namespace_Get(t *testing.T) {
	var (
		ctx               = context.Background()
		namespace         = "free-us-1"
		cpRouterURL       = "mockCpRouterURL"
		hostServiceSecret = "service-secret"
	)

	bcSrv := &backendConfigServer{t: t, token: hostServiceSecret}
	bcSrv.addNamespace(namespace, "./testdata/sample_namespace.json")

	ts := httptest.NewServer(bcSrv)
	defer ts.Close()
	httpSrvURL, err := url.Parse(ts.URL)
	require.NoError(t, err)

	client := &namespaceConfig{
		config: config.New(),
		logger: logger.NOP,

		client:           ts.Client(),
		configBackendURL: httpSrvURL,

		namespace:           namespace,
		hostedServiceSecret: hostServiceSecret,
		cpRouterURL:         cpRouterURL,
	}
	require.NoError(t, client.SetUp())

	c, err := client.Get(ctx)
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

		c, err := client.Get(ctx)
		require.EqualError(t, err, `backend config request failed with 401: {"message":"Unauthorized"}`)
		require.Empty(t, c)
	})

	t.Run("empty namespace", func(t *testing.T) {
		client := &namespaceConfig{
			client:           ts.Client(),
			configBackendURL: httpSrvURL,

			namespace:           "namespace-does-not-exist",
			hostedServiceSecret: hostServiceSecret,
		}
		require.NoError(t, client.SetUp())

		c, err := client.Get(ctx)
		require.EqualError(t, err, "backend config request failed with 404")
		require.Empty(t, c)
	})
}

func Test_Namespace_Identity(t *testing.T) {
	var (
		namespace = "free-us-1"
		secret    = "service-secret"
	)

	bcSrv := &backendConfigServer{t: t, token: secret}

	ts := httptest.NewServer(bcSrv)
	defer ts.Close()
	httpSrvURL, err := url.Parse(ts.URL)
	require.NoError(t, err)

	client := &namespaceConfig{
		logger: logger.NOP,

		client:           ts.Client(),
		configBackendURL: httpSrvURL,

		namespace:           namespace,
		hostedServiceSecret: secret,
		cpRouterURL:         cpRouterURL,
	}
	require.NoError(t, client.SetUp())

	ident := client.Identity()

	require.Equal(t, &identity.Namespace{
		Namespace:    namespace,
		HostedSecret: secret,
	}, ident)
}

// TestDynamicConfigInNamespace tests that the HasDynamicConfig field is properly set
// when loading the configuration from the API in a multi-tenant setup.
func TestDynamicConfigInNamespace(t *testing.T) {
	var (
		ctx               = context.Background()
		namespace         = "dynamic-config-namespace"
		hostServiceSecret = "service-secret"
	)

	bcSrv := &backendConfigServer{t: t, token: hostServiceSecret}
	bcSrv.addNamespace(namespace, "./testdata/namespace_with_dynamic_config.json")

	ts := httptest.NewServer(bcSrv)
	defer ts.Close()
	httpSrvURL, err := url.Parse(ts.URL)
	require.NoError(t, err)

	client := &namespaceConfig{
		config: config.New(),
		logger: logger.NOP,

		client:           ts.Client(),
		configBackendURL: httpSrvURL,

		namespace:           namespace,
		hostedServiceSecret: hostServiceSecret,
	}
	require.NoError(t, client.SetUp())

	// Get the configuration from the API
	configs, err := client.Get(ctx)
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

func Test_Namespace_IncrementalUpdates(t *testing.T) {
	var (
		ctx                  = context.Background()
		namespace            = "free-us-1"
		secret               = "service-secret"
		requestNumber        int
		receivedUpdatedAfter []time.Time
	)

	responseBodyFromFile, err := os.ReadFile("./testdata/sample_namespace.json")
	require.NoError(t, err)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { requestNumber++ }()

		user, _, ok := r.BasicAuth()
		require.True(t, ok)
		require.Equal(t, secret, user)

		var (
			err              error
			updatedAfterTime time.Time
			responseBody     []byte
		)
		for k, v := range r.URL.Query() {
			if k != "updatedAfter" {
				continue
			}

			updatedAfterTime, err = time.Parse(updatedAfterTimeFormat, v[0])
			require.NoError(t, err)

			receivedUpdatedAfter = append(receivedUpdatedAfter, updatedAfterTime)
		}

		switch requestNumber {
		case 0: // 1st request, return file content as is
			responseBody = responseBodyFromFile
		case 1: // 2nd request, return new workspace, no updates for the other 2
			responseBody = []byte(fmt.Sprintf(`{
				"dummy":{"updatedAt":%q,"libraries":[{"versionId":"foo"},{"versionId":"bar"}]},
				"2CCgbmvBSa8Mv81YaIgtR36M7aW":null,
				"2CChLejq5aIWi3qsKVm1PjHkyTj":null
			}`, updatedAfterTime.Add(time.Minute).Format(updatedAfterTimeFormat)))
		case 2: // 3rd request, return updated dummy workspace, no updates for the other 2
			responseBody = []byte(fmt.Sprintf(`{
				"dummy":{"updatedAt":%q,"libraries":[{"versionId":"baz"}]},
				"2CCgbmvBSa8Mv81YaIgtR36M7aW":null,
				"2CChLejq5aIWi3qsKVm1PjHkyTj":null
			}`, updatedAfterTime.Add(time.Minute).Format(updatedAfterTimeFormat)))
		case 3, 4: // 4th and 5th request, delete the dummy workspace
			responseBody = []byte(`{
				"2CCgbmvBSa8Mv81YaIgtR36M7aW":null,
				"2CChLejq5aIWi3qsKVm1PjHkyTj":null
			}`)
		case 5: // new workspace, but it's update time is before the last request, so no updates
			responseBody = []byte(`{"someWorkspaceID": null}`)
		default:
			responseBody = responseBodyFromFile
		}

		_, _ = w.Write(responseBody)
	}))
	defer ts.Close()
	httpSrvURL, err := url.Parse(ts.URL)
	require.NoError(t, err)

	client := &namespaceConfig{
		config: config.New(),
		logger: logger.NOP,

		client:           ts.Client(),
		configBackendURL: httpSrvURL,

		namespace:                namespace,
		hostedServiceSecret:      secret,
		cpRouterURL:              cpRouterURL,
		incrementalConfigUpdates: true,
	}
	require.NoError(t, client.SetUp())

	// send the request the first time
	c, err := client.Get(ctx)
	require.NoError(t, err)
	require.Len(t, c, 2)
	require.Contains(t, c, "2CCgbmvBSa8Mv81YaIgtR36M7aW")
	require.Contains(t, c, "2CChLejq5aIWi3qsKVm1PjHkyTj")
	require.Empty(t, receivedUpdatedAfter, "The first request should not have updatedAfter in the query params")

	// send the request again, should receive the new dummy workspace
	c, err = client.Get(ctx)
	require.NoError(t, err)
	require.Len(t, c, 3)
	require.Contains(t, c, "2CCgbmvBSa8Mv81YaIgtR36M7aW")
	require.Contains(t, c, "2CChLejq5aIWi3qsKVm1PjHkyTj")
	require.Contains(t, c, "dummy")
	require.Equal(t, LibrariesT{{VersionID: "foo"}, {VersionID: "bar"}}, c["dummy"].Libraries)
	require.Len(t, receivedUpdatedAfter, 1)
	expectedUpdatedAt, err := time.Parse(updatedAfterTimeFormat, "2022-07-20T10:00:00.000Z")
	require.NoError(t, err)
	require.Equal(t, receivedUpdatedAfter[0], expectedUpdatedAt, updatedAfterTimeFormat)

	// send the request again, should receive the updated dummy workspace
	c, err = client.Get(ctx)
	require.NoError(t, err)
	require.Len(t, c, 3)
	require.Contains(t, c, "2CCgbmvBSa8Mv81YaIgtR36M7aW")
	require.Contains(t, c, "2CChLejq5aIWi3qsKVm1PjHkyTj")
	require.Contains(t, c, "dummy")
	require.Equal(t, LibrariesT{{VersionID: "baz"}}, c["dummy"].Libraries)
	require.Len(t, receivedUpdatedAfter, 2)
	require.Equal(t, receivedUpdatedAfter[1], expectedUpdatedAt.Add(time.Minute), updatedAfterTimeFormat)

	// send the request again, should not receive dummy since it was deleted
	c, err = client.Get(ctx)
	require.NoError(t, err)
	require.Len(t, c, 2)
	require.Contains(t, c, "2CCgbmvBSa8Mv81YaIgtR36M7aW")
	require.Contains(t, c, "2CChLejq5aIWi3qsKVm1PjHkyTj")
	require.Len(t, receivedUpdatedAfter, 3)
	require.Equal(t, receivedUpdatedAfter[2], expectedUpdatedAt.Add(2*time.Minute), updatedAfterTimeFormat)

	// send the request again, the updatedAfter should be the same as the last request since no updates
	// were received, we have no way of knowing the "control plane time"
	c, err = client.Get(ctx)
	require.NoError(t, err)
	require.Len(t, c, 2)
	require.Contains(t, c, "2CCgbmvBSa8Mv81YaIgtR36M7aW")
	require.Contains(t, c, "2CChLejq5aIWi3qsKVm1PjHkyTj")
	require.Len(t, receivedUpdatedAfter, 4)
	require.Equal(t, receivedUpdatedAfter[3], expectedUpdatedAt.Add(2*time.Minute), updatedAfterTimeFormat)

	// send the request again, should receive the new workspace
	// should trigger a full update
	c, err = client.Get(ctx)
	require.NoError(t, err)
	require.Len(t, c, 2)
	require.Contains(t, c, "2CCgbmvBSa8Mv81YaIgtR36M7aW")
	require.Contains(t, c, "2CChLejq5aIWi3qsKVm1PjHkyTj")
	require.Len(t, receivedUpdatedAfter, 5)
	require.NotContains(t, c, "someNewWorkspaceID")
	expectedUpdatedAt, err = time.Parse(updatedAfterTimeFormat, "2022-07-20T10:00:00.000Z")
	require.NoError(t, err)
	require.Equal(t, receivedUpdatedAfter[0], expectedUpdatedAt, updatedAfterTimeFormat)
	require.Equal(t, receivedUpdatedAfter[0], client.lastUpdatedAt, updatedAfterTimeFormat)
	require.Equal(t, 7, requestNumber)
}

func TestSourceDefinitionOptions(t *testing.T) {
	var (
		ctx               = context.Background()
		namespace         = "dynamic-config-namespace"
		hostServiceSecret = "service-secret"
	)

	bcSrv := &backendConfigServer{t: t, token: hostServiceSecret}
	bcSrv.addNamespace(namespace, "./testdata/namespace_with_source_definition_options.json")

	ts := httptest.NewServer(bcSrv)
	defer ts.Close()
	httpSrvURL, err := url.Parse(ts.URL)
	require.NoError(t, err)

	client := &namespaceConfig{
		config: config.New(),
		logger: logger.NOP,

		client:           ts.Client(),
		configBackendURL: httpSrvURL,

		namespace:           namespace,
		hostedServiceSecret: hostServiceSecret,
	}
	require.NoError(t, client.SetUp())

	// Get the configuration from the API
	configs, err := client.Get(ctx)
	require.NoError(t, err)
	require.Len(t, configs, 1)

	// Get the workspace configuration
	config, ok := configs["workspace-1"]
	require.True(t, ok)

	require.Len(t, config.Sources, 2)
	require.Len(t, config.Sources[0].Destinations, 2)

	// Verify that source definition options are correctly parsed
	srcMap := config.SourcesMap()
	require.Equal(t, srcMap["source-1"].SourceDefinition.Name, "Test Source")
	require.True(t, srcMap["source-1"].SourceDefinition.Options.Hydration.Enabled)
	require.False(t, srcMap["source-2"].SourceDefinition.Options.Hydration.Enabled)
}

func Test_Namespace_FetchInternalSecrets(t *testing.T) {
	var (
		ctx               = context.Background()
		namespace         = "free-us-1"
		hostServiceSecret = "service-secret"
	)

	responseBodyFromFile, err := os.ReadFile("./testdata/namespace_with_source_definition_options.json")
	require.NoError(t, err)

	// Test without fetchInternalSecrets (default behavior)
	t.Run("without fetchInternalSecrets", func(t *testing.T) {
		var queryParams url.Values
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			// Capture query parameters for assertion
			queryParams = r.URL.Query()

			user, _, ok := r.BasicAuth()
			require.True(t, ok)
			require.Equal(t, hostServiceSecret, user)

			_, _ = w.Write(responseBodyFromFile)
		}))
		defer ts.Close()
		httpSrvURL, err := url.Parse(ts.URL)
		require.NoError(t, err)
		client := &namespaceConfig{
			config: config.New(),
			logger: logger.NOP,

			client:           ts.Client(),
			configBackendURL: httpSrvURL,

			namespace:                namespace,
			hostedServiceSecret:      hostServiceSecret,
			cpRouterURL:              cpRouterURL,
			incrementalConfigUpdates: false,
			fetchInternalSecretes:    false, // Explicitly set to false
		}
		require.NoError(t, client.SetUp())

		configs, err := client.Get(ctx)
		require.NoError(t, err)
		require.Len(t, configs, 1)

		require.Empty(t, queryParams.Get("secrets"))

		// Get the workspace configuration
		c, ok := configs["workspace-1"]
		require.True(t, ok)

		require.Len(t, c.Sources, 2)
		require.Len(t, c.Sources[0].Destinations, 2)

		// Verify that source definition options are correctly parsed
		srcMap := c.SourcesMap()
		require.Equal(t, srcMap["source-1"].SourceDefinition.Name, "Test Source")
		require.True(t, srcMap["source-1"].SourceDefinition.Options.Hydration.Enabled)
		require.False(t, srcMap["source-2"].SourceDefinition.Options.Hydration.Enabled)
	})

	// Test with fetchInternalSecrets enabled
	t.Run("with fetchInternalSecrets", func(t *testing.T) {
		var queryParams url.Values
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			// Capture query parameters for assertion
			queryParams = r.URL.Query()

			user, _, ok := r.BasicAuth()
			require.True(t, ok)
			require.Equal(t, hostServiceSecret, user)

			_, _ = w.Write(responseBodyFromFile)
		}))
		defer ts.Close()

		httpSrvURL, err := url.Parse(ts.URL)
		require.NoError(t, err)
		client := &namespaceConfig{
			config: config.New(),
			logger: logger.NOP,

			client:           ts.Client(),
			configBackendURL: httpSrvURL,

			namespace:                namespace,
			hostedServiceSecret:      hostServiceSecret,
			cpRouterURL:              cpRouterURL,
			incrementalConfigUpdates: false,
			fetchInternalSecretes:    true, // Enable fetching internal secrets
		}
		require.NoError(t, client.SetUp())

		configs, err := client.Get(ctx)
		require.NoError(t, err)
		require.Len(t, configs, 1)

		require.Equal(t, queryParams.Get("secrets"), "embed")

		// Get the workspace configuration
		c, ok := configs["workspace-1"]
		require.True(t, ok)

		require.Len(t, c.Sources, 2)
		require.Len(t, c.Sources[0].Destinations, 2)

		// Verify that source definition options are correctly parsed
		srcMap := c.SourcesMap()
		require.Equal(t, srcMap["source-1"].SourceDefinition.Name, "Test Source")
		require.True(t, srcMap["source-1"].SourceDefinition.Options.Hydration.Enabled)
		require.False(t, srcMap["source-2"].SourceDefinition.Options.Hydration.Enabled)
		require.JSONEq(t, string(srcMap["source-1"].InternalSecret), `{"pageAccessToken": "some-page-access-token"}`)
	})
}

type backendConfigServer struct {
	t         *testing.T
	responses map[string]string
	token     string
}

func (s *backendConfigServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	user, _, ok := r.BasicAuth()
	if !ok || user != s.token {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"message":"Unauthorized"}`))
		return
	}

	body, ok := s.responses[r.URL.Path]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(body))
}

func (s *backendConfigServer) addNamespace(namespace, path string) {
	s.t.Helper()

	if s.responses == nil {
		s.responses = make(map[string]string)
	}

	payload, err := os.ReadFile(path)
	require.NoError(s.t, err)

	s.responses["/data-plane/v1/namespaces/"+namespace+"/config"] = string(payload)
}
