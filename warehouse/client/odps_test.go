package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/credentials-go/credentials"
	"github.com/stretchr/testify/require"
)

func TestQueryODPSUsesCallerContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := QueryODPS(ctx, nil, "SELECT 1", time.Second, time.Millisecond)
	require.ErrorIs(t, err, context.Canceled)
}

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
type pointerHTTPError struct{ *restclient.HttpError }

func (e pointerHTTPError) Error() string { return e.HttpError.Error() }
func (e pointerHTTPError) Unwrap() error { return e.HttpError }

func TestODPSErrorSanitization(t *testing.T) {
	for _, tc := range []struct {
		input error
		want  string
	}{
		{restclient.HttpError{StatusCode: 403, Body: []byte("SECRET")}, "ODPS authentication or permission denied"},
		{restclient.HttpError{StatusCode: 404}, "ODPS resource not found"},
		{restclient.HttpError{StatusCode: 503}, "ODPS service temporarily unavailable"},
		{restclient.HttpError{StatusCode: 500, Body: []byte("SECRET")}, "ODPS request failed (status 500)"},
		{fmt.Errorf("wrapped: %w", pointerHTTPError{&restclient.HttpError{StatusCode: 500, ErrorMessage: &restclient.ErrorMessage{
			ErrorCode: "TableNotFound", Message: "accessKeyId=SECRET",
		}, Body: []byte("SECRET")}}), "ODPS resource not found: TableNotFound: [credential]"},
		{fmt.Errorf("wrapped: %w", restclient.HttpError{StatusCode: 400, ErrorMessage: &restclient.ErrorMessage{
			ErrorCode: "AccessDenied", Message: "securityToken=SECRET",
		}}), "ODPS authentication or permission denied: AccessDenied: [credential]"},
		{restclient.HttpError{StatusCode: 400, ErrorMessage: &restclient.ErrorMessage{
			ErrorCode: "InvalidParameter", Message: "CREATE SCHEMA syntax is invalid",
		}}, "ODPS request failed (status 400): InvalidParameter: CREATE SCHEMA syntax is invalid"},
		{pointerHTTPError{&restclient.HttpError{StatusCode: 0, Body: []byte("SECRET")}}, "ODPS request failed"},
		{errors.New("AccessDenied: SECRET"), "ODPS authentication or permission denied"},
		{errors.New("TableNotFound: SECRET"), "ODPS resource not found"},
		{errors.New("SECRET"), "ODPS request failed"},
	} {
		err := ODPSSafeError(tc.input)
		require.EqualError(t, err, tc.want)
		require.False(t, strings.Contains(err.Error(), "SECRET"))
	}
	require.NoError(t, ODPSSafeError(nil))
}

func TestODPSErrorKeepsDiagnosticWithoutCredentials(t *testing.T) {
	accessKey := "LTAI1234567890abcdef"
	securityToken := "CAIS1234567890abcdef"
	message := "ODPS-0130161: Parse exception near SCHEMA; accessKeyId=" + accessKey +
		" securityToken=" + securityToken + " https://example.invalid/signed?token=secret"
	body := []byte(`{"Code":"ODPS-0130161","Message":` + strconv.Quote(message) + `}`)
	for _, input := range []error{
		restclient.HttpError{StatusCode: 400, Body: body},
		errors.New(message), // SDK task failures are plain errors.New(result).
	} {
		diagnostic := ODPSSafeError(input).Error()
		require.Contains(t, diagnostic, "ODPS-0130161")
		require.Contains(t, diagnostic, "Parse exception near SCHEMA")
		require.NotContains(t, diagnostic, accessKey)
		require.NotContains(t, diagnostic, securityToken)
		require.NotContains(t, diagnostic, "example.invalid")
		require.NotContains(t, diagnostic, "secret")
	}
}

func TestODPSQueryHints(t *testing.T) {
	hints := odpsQueryHints()
	require.Equal(t, "UTC", hints["odps.sql.timezone"])
	require.Equal(t, "true", hints["odps.sql.type.system.odps2"])
	require.Equal(t, "true", hints["odps.namespace.schema"])
}

// SDK signing is performed before sending a request. A failing role provider
// must not leak its refresh error through Query or send a request.
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
func TestODPSQueryPolling(t *testing.T) {
	for _, tc := range []struct {
		name      string
		taskReply string
		want      string
	}{
		{"failed", `<Instance><Tasks><Task><Status>FAILED</Status><Result><![CDATA[ODPS-0130161: Parse exception]]></Result></Task></Tasks></Instance>`, "ODPS query failed: ODPS-0130161: Parse exception"},
		{"suspended", `<Instance><Tasks><Task><Status>SUSPENDED</Status></Task></Tasks></Instance>`, "ODPS query failed"},
		{"terminated", `<Instance><Status>TERMINATED</Status></Instance>`, "ODPS query terminated"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := newODPSTestServer(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				switch {
				case r.Method == http.MethodPost:
					w.Header().Set("Location", "/api/projects/project/instances/test")
					w.WriteHeader(http.StatusCreated)
				case r.URL.Query().Has("taskstatus"):
					_, _ = w.Write([]byte(tc.taskReply))
				default:
					_, _ = w.Write([]byte(tc.taskReply))
				}
			})
			defer server.Close()
			instance := odps.NewOdps(account.NewAliyunAccount("id", "key"), server.URL)
			instance.SetDefaultProjectName("project")
			result, err := (&Client{Type: ODPSClient, ODPS: instance}).Query("SELECT 1")
			require.EqualError(t, err, tc.want)
			require.Empty(t, result.Columns)
			require.LessOrEqual(t, calls.Load(), int32(3))
		})
	}
}

func TestODPSQueryPollingDeadline(t *testing.T) {
	// Bound the query deadline in this test without waiting five minutes.
	oldTimeout, oldInterval := odpsQueryTimeout, odpsPollInterval
	odpsQueryTimeout, odpsPollInterval = 70*time.Millisecond, 5*time.Millisecond
	defer func() { odpsQueryTimeout, odpsPollInterval = oldTimeout, oldInterval }()

	var calls atomic.Int32
	server := newODPSTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method == http.MethodPost {
			w.Header().Set("Location", "/api/projects/project/instances/test")
			w.WriteHeader(http.StatusCreated)
			return
		}
		_, _ = w.Write([]byte(`<Instance><Status>RUNNING</Status><Tasks><Task><Status>RUNNING</Status></Task></Tasks></Instance>`))
	})
	defer server.Close()
	instance := odps.NewOdps(account.NewAliyunAccount("id", "key"), server.URL)
	instance.SetDefaultProjectName("project")
	started := time.Now()
	_, err := (&Client{Type: ODPSClient, ODPS: instance}).Query("SELECT 1")
	require.EqualError(t, err, "ODPS query timed out")
	require.Less(t, time.Since(started), time.Second)
	require.Greater(t, calls.Load(), int32(2))
}

func TestRunODPSSQLSuccessAndHints(t *testing.T) {
	for _, syncReply := range []bool{false, true} {
		t.Run(fmt.Sprintf("sync=%t", syncReply), func(t *testing.T) {
			var polls atomic.Int32
			server := newODPSTestServer(t, func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPost:
					// The SDK sends the session hints as settings on every SQL task.
					var body []byte
					body, _ = io.ReadAll(r.Body)
					for _, hint := range []string{`odps.namespace.schema`, `odps.sql.type.system.odps2`, `odps.sql.timezone`} {
						require.Contains(t, string(body), hint)
					}
					w.Header().Set("Location", "/api/projects/project/instances/test")
					if syncReply {
						_, _ = w.Write([]byte(`<Instance><Tasks><Task><Name>load</Name><Status>SUCCESS</Status></Task></Tasks></Instance>`))
					} else {
						w.WriteHeader(http.StatusCreated)
					}
				case r.URL.Query().Has("taskstatus"):
					_, _ = w.Write([]byte(`<Instance><Tasks><Task><Name>load</Name><Status>SUCCESS</Status></Task></Tasks></Instance>`))
				default:
					polls.Add(1)
					_, _ = w.Write([]byte(`<Instance><Status>RUNNING</Status></Instance>`))
				}
			})
			defer server.Close()
			service := odps.NewOdps(account.NewAliyunAccount("id", "key"), server.URL)
			service.SetDefaultProjectName("project")
			instance, err := RunODPSSQL(context.Background(), service, "SELECT 1", time.Second, time.Millisecond)
			require.NoError(t, err)
			require.Equal(t, "test", instance.Id())
			if syncReply {
				require.Zero(t, polls.Load())
			} else {
				require.Positive(t, polls.Load())
			}
		})
	}
}

func TestRunODPSSQLDeadlineTerminates(t *testing.T) {
	terminated := make(chan struct{}, 1)
	server := newODPSTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			w.Header().Set("Location", "/api/projects/project/instances/test")
			w.WriteHeader(http.StatusCreated)
		case http.MethodPut:
			terminated <- struct{}{}
		default:
			_, _ = w.Write([]byte(`<Instance><Status>RUNNING</Status><Tasks><Task><Status>RUNNING</Status></Task></Tasks></Instance>`))
		}
	})
	defer server.Close()
	service := odps.NewOdps(account.NewAliyunAccount("id", "key"), server.URL)
	service.SetDefaultProjectName("project")
	instance, err := RunODPSSQL(context.Background(), service, "SELECT 1", 50*time.Millisecond, 5*time.Millisecond)
	require.Nil(t, instance)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	select {
	case <-terminated:
	case <-time.After(time.Second):
		t.Fatal("async instance not terminated on deadline")
	}
}

func newODPSTestServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	return httptest.NewServer(handler)
}
