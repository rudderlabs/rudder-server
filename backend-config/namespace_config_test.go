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
