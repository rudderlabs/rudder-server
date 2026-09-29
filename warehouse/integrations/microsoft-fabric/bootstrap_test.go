package microsoftfabric

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/stretchr/testify/require"
)

type staticCredential struct {
	token  string
	scopes []string
}

func (c *staticCredential) GetToken(_ context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
	c.scopes = append([]string(nil), options.Scopes...)
	return azcore.AccessToken{Token: c.token, ExpiresOn: time.Now().Add(time.Hour)}, nil
}

func TestBootstrapRequestAndCache(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "/v1/workspaces/workspace-id/items", r.URL.Path)
		require.Equal(t, "false", r.URL.Query().Get("recursive"))
		require.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"continuationToken":"ignored"}`)
	}))
	defer server.Close()

	credential := &staticCredential{token: "test-token"}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	bootstrap := newBootstrapper(server.Client())
	bootstrap.endpoint = server.URL
	bootstrap.now = func() time.Time { return now }
	bootstrap.newCredential = func(_, _, _ string) (tokenCredential, error) { return credential, nil }

	require.NoError(t, bootstrap.Bootstrap(context.Background(), "tenant", "client", "secret", "workspace-id"))
	require.Equal(t, []string{fabricAPIScope}, credential.scopes)
	require.EqualValues(t, 1, calls.Load())

	// Same principal is cached regardless of workspace because the Fabric security token is principal-scoped.
	require.NoError(t, bootstrap.Bootstrap(context.Background(), "tenant", "client", "secret", "other-workspace"))
	require.EqualValues(t, 1, calls.Load())

	now = now.Add(bootstrapTTL)
	require.NoError(t, bootstrap.Bootstrap(context.Background(), "tenant", "client", "secret", "workspace-id"))
	require.EqualValues(t, 2, calls.Load())
}

func TestBootstrapCoordinatesOnlyMatchingPrincipals(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- r.URL.Path
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	bootstrap := newBootstrapper(server.Client())
	bootstrap.endpoint = server.URL
	bootstrap.newCredential = func(_, _, _ string) (tokenCredential, error) {
		return &staticCredential{token: "token"}, nil
	}
	errCh := make(chan error, 3)
	go func() {
		errCh <- bootstrap.Bootstrap(context.Background(), "tenant-a", "client-a", "secret", "workspace-a")
	}()
	go func() {
		errCh <- bootstrap.Bootstrap(context.Background(), "tenant-b", "client-b", "secret", "workspace-b")
	}()

	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("different principals did not bootstrap concurrently")
		}
	}
	go func() {
		errCh <- bootstrap.Bootstrap(context.Background(), "tenant-a", "client-a", "secret", "workspace-a")
	}()
	select {
	case <-started:
		t.Fatal("matching principal issued a duplicate request")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	for range 3 {
		require.NoError(t, <-errCh)
	}
}

func TestBootstrapFailureClassificationAndSafety(t *testing.T) {
	tests := []struct {
		name          string
		status        int
		body          string
		wantRetryable bool
	}{
		{name: "rate limit", status: http.StatusTooManyRequests, body: `{}`, wantRetryable: true},
		{name: "server", status: http.StatusBadGateway, body: `{}`, wantRetryable: true},
		{name: "api retryable", status: http.StatusBadRequest, body: `{"errorCode":"Busy","requestId":"request-1","isRetriable":true}`, wantRetryable: true},
		{name: "forbidden", status: http.StatusForbidden, body: `{"error":{"errorCode":"Forbidden","requestId":"request-2","isRetriable":false}}`, wantRetryable: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.WriteHeader(tt.status)
				_, _ = io.WriteString(w, tt.body)
			}))
			defer server.Close()

			bootstrap := newBootstrapper(server.Client())
			bootstrap.endpoint = server.URL
			bootstrap.newCredential = func(_, _, _ string) (tokenCredential, error) {
				return &staticCredential{token: "sensitive-token"}, nil
			}
			err := bootstrap.Bootstrap(context.Background(), "tenant", "client", "sensitive-secret", "workspace")
			var typed *bootstrapError
			require.ErrorAs(t, err, &typed)
			require.Equal(t, tt.wantRetryable, typed.Retryable)
			require.NotContains(t, err.Error(), "sensitive-token")
			require.NotContains(t, err.Error(), "sensitive-secret")
			if !tt.wantRetryable {
				require.Contains(t, err.Error(), "Service principals can use Fabric APIs")
			}

			// Failures are never cached.
			_ = bootstrap.Bootstrap(context.Background(), "tenant", "client", "sensitive-secret", "workspace")
			require.EqualValues(t, 2, calls.Load())
		})
	}
}

func TestBootstrapCredentialFailureIsSafe(t *testing.T) {
	bootstrap := newBootstrapper(nil)
	bootstrap.newCredential = func(_, _, _ string) (tokenCredential, error) {
		return nil, errors.New("credential rejected")
	}
	err := bootstrap.Bootstrap(context.Background(), "tenant", "client", "secret", "workspace")
	require.Error(t, err)
	require.True(t, strings.HasPrefix(err.Error(), "spn_token_bootstrap:"))
	require.NotContains(t, err.Error(), "secret")
}
