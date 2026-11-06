package client

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/credentials-go/credentials"
	"github.com/stretchr/testify/require"
)

func TestODPSCredentials(t *testing.T) {
	var count atomic.Int32
	provider := NewODPSCredentials(func() (*credentials.CredentialModel, error) {
		id := "id"
		key := "secret"
		token := "token"
		count.Add(1)
		return &credentials.CredentialModel{AccessKeyId: &id, AccessKeySecret: &key, SecurityToken: &token}, nil
	})
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			model, err := provider.GetCredential()
			if err != nil || model == nil || *model.SecurityToken != "token" {
				t.Errorf("unexpected credentials: %v", err)
			}
		})
	}
	wg.Wait()
	require.EqualValues(t, 16, count.Load())
	model, err := provider.GetCredential()
	require.NoError(t, err)
	*model.AccessKeyId = "changed"
	next, err := provider.GetCredential()
	require.NoError(t, err)
	require.Equal(t, "id", *next.AccessKeyId)

	for _, tc := range []struct {
		name string
		fn   func() (*credentials.CredentialModel, error)
	}{
		{"nil function", nil},
		{"failure", func() (*credentials.CredentialModel, error) { return nil, errors.New("sensitive token") }},
		{"incomplete", func() (*credentials.CredentialModel, error) { return &credentials.CredentialModel{}, nil }},
		{"missing token", func() (*credentials.CredentialModel, error) {
			id, secret := "id", "secret"
			return &credentials.CredentialModel{AccessKeyId: &id, AccessKeySecret: &secret}, nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewODPSCredentials(tc.fn).GetCredential()
			require.EqualError(t, err, "ODPS credentials unavailable")
		})
	}
}

type odpsTestCredential struct {
	model *credentials.CredentialModel
	err   error
}

func (c *odpsTestCredential) GetCredential() (*credentials.CredentialModel, error) {
	return c.model, c.err
}

func (c *odpsTestCredential) GetType() *string {
	kind := "access_key"
	return &kind
}

func (c *odpsTestCredential) GetAccessKeyId() (*string, error) {
	return nil, errors.New("SECRET from deprecated method")
}

func (c *odpsTestCredential) GetAccessKeySecret() (*string, error) {
	return nil, errors.New("SECRET from deprecated method")
}

func (c *odpsTestCredential) GetSecurityToken() (*string, error) {
	return nil, errors.New("SECRET from deprecated method")
}

func (c *odpsTestCredential) GetBearerToken() *string { return nil }

func TestGuardODPSCredentials(t *testing.T) {
	id, key, token := "sensitive id", "sensitive key", "sensitive token"
	source := &odpsTestCredential{model: &credentials.CredentialModel{
		AccessKeyId: &id, AccessKeySecret: &key, SecurityToken: &token,
	}}
	guard := GuardODPSCredentials(source)
	model, err := guard.GetCredential()
	require.NoError(t, err)
	require.Equal(t, id, *model.AccessKeyId)
	require.Equal(t, key, *model.AccessKeySecret)
	require.Equal(t, &token, model.SecurityToken)
	*model.AccessKeySecret = "changed"
	next, err := guard.GetCredential()
	require.NoError(t, err)
	require.Equal(t, key, *next.AccessKeySecret)
	source.err = errors.New("SECRET from refresh")
	_, err = guard.GetCredential()
	require.EqualError(t, err, "ODPS credentials unavailable")
	missingToken := GuardODPSCredentials(&odpsTestCredential{model: &credentials.CredentialModel{AccessKeyId: &id, AccessKeySecret: &key}})
	_, err = missingToken.GetCredential()
	require.EqualError(t, err, "ODPS credentials unavailable")
	var nilGuard *odpsCredentialGuard
	_, err = nilGuard.GetCredential()
	require.EqualError(t, err, "ODPS credentials unavailable")
}

func TestNewODPSClient(t *testing.T) {
	for _, tc := range []struct{ project, region string }{
		{"", "cn-shanghai"},
		{"project/escape", "cn-shanghai"},
		{"project", "cn-shanghai.evil.com"},
		{"project", "cn-shanghai:8080"},
		{"project", "Internal"},
	} {
		_, err := NewODPSClient(tc.project, tc.region, NewODPSCredentials(nil))
		require.Error(t, err)
	}
	_, err := NewODPSClient("safe_project", "cn-shanghai", nil)
	require.ErrorContains(t, err, "credentials")

	provider := NewODPSCredentials(func() (*credentials.CredentialModel, error) {
		id, key, token := "id", "key", "token"
		return &credentials.CredentialModel{AccessKeyId: &id, AccessKeySecret: &key, SecurityToken: &token}, nil
	})
	instance, err := NewODPSClient("safe_project", "cn-shanghai", provider)
	require.NoError(t, err)
	rest := instance.RestClient()
	require.Equal(t, "https://service.cn-shanghai.maxcompute.aliyun.com/api", rest.Endpoint())
	require.Equal(t, "safe_project", instance.DefaultProjectName())
	for _, endpoint := range []string{
		"http://service.cn-shanghai.maxcompute.aliyun.com/api",
		"https://service.cn-beijing.maxcompute.aliyun.com/api",
		"https://service.cn-shanghai.maxcompute.aliyun.com.evil/api",
		"https://service.cn-shanghai.maxcompute.aliyun.com:443/api",
		"https://service.cn-shanghai.maxcompute.aliyun.com/api/",
		"https://user:SECRET@service.cn-shanghai.maxcompute.aliyun.com/api",
	} {
		_, err := NewODPSClientWithEndpoint("safe_project", "cn-shanghai", endpoint, provider)
		require.EqualError(t, err, "invalid ODPS endpoint")
	}
	configured, err := NewODPSClientWithEndpoint("safe_project", "cn-shanghai", rest.Endpoint(), provider)
	require.NoError(t, err)
	configuredRest := configured.RestClient()
	require.Equal(t, rest.Endpoint(), configuredRest.Endpoint())
	require.Equal(t, "cn-shanghai", configured.Options.RegionId)
	static, err := NewODPSClientWithAccessKey("safe_project", "cn-shanghai", rest.Endpoint(), "id", "secret")
	require.NoError(t, err)
	require.IsType(t, &account.AliyunAccount{}, static.Account())
	otherRegion := "https://service.af-south-1.maxcompute.aliyun.com/api"
	_, err = NewODPSClientWithAccessKey("safe_project", "af-south-1", otherRegion, "id", "secret")
	require.NoError(t, err)
	_, err = NewODPSClientWithAccessKey("safe_project", "me-central-1", otherRegion, "id", "secret")
	require.EqualError(t, err, "invalid ODPS endpoint")
	client, err := NewODPSWarehouseClient("safe_project", "cn-shanghai", provider)
	require.NoError(t, err)
	require.Equal(t, ODPSClient, client.Type)
	client.Close()
	_, err = client.Query("  ")
	require.EqualError(t, err, "ODPS statement is empty")
}

// HttpError.Error has a value receiver; this wrapper tests the pointer form
// without hiding that the SDK itself may return value errors.

func TestODPSQueryCredentialFailure(t *testing.T) {
	id, key, token := "SECRET id", "SECRET key", "SECRET token"
	provider := &odpsTestCredential{model: &credentials.CredentialModel{AccessKeyId: &id, AccessKeySecret: &key, SecurityToken: &token}}
	provider.err = errors.New("SECRET refresh failure")
	guarded := GuardODPSCredentials(provider)
	_, guardErr := guarded.GetCredential()
	require.EqualError(t, guardErr, "ODPS credentials unavailable")
	var calls atomic.Int32
	server := newODPSTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	})
	defer server.Close()
	instance := odps.NewOdps(account.NewStsAccountWithCredential(guarded, "cn-shanghai"), server.URL)
	instance.SetDefaultProjectName("project")
	_, err := (&Client{Type: ODPSClient, ODPS: instance}).Query("SELECT 1")
	require.EqualError(t, err, "ODPS request failed")
	require.Zero(t, calls.Load(), "credential failure must stop before the HTTP request")
}

func TestODPSRoleRefreshWithoutTokenFailsBeforeSigning(t *testing.T) {
	id, secret, token := "SECRET id", "SECRET secret", "SECRET token"
	provider := &odpsTestCredential{model: &credentials.CredentialModel{AccessKeyId: &id, AccessKeySecret: &secret, SecurityToken: &token}}
	provider.model.SecurityToken = nil
	guarded := GuardODPSCredentials(provider)
	_, guardErr := guarded.GetCredential()
	require.EqualError(t, guardErr, "ODPS credentials unavailable")
	var calls atomic.Int32
	server := newODPSTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	})
	defer server.Close()
	instance := odps.NewOdps(account.NewStsAccountWithCredential(guarded, "cn-shanghai"), server.URL)
	instance.SetDefaultProjectName("project")
	_, err := (&Client{Type: ODPSClient, ODPS: instance}).Query("SELECT 1")
	require.EqualError(t, err, "ODPS request failed")
	require.NotContains(t, err.Error(), "SECRET")
	require.Zero(t, calls.Load(), "missing STS token must stop before the HTTP request")
}

// The SDK supplies no context-aware requests, so test the polling state
// machine with a local endpoint.

func newODPSTestServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	return httptest.NewServer(handler)
}
