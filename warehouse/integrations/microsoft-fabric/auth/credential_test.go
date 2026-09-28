package auth

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/stretchr/testify/require"
)

type staticTokenCredential struct {
	calls int
}

func (c *staticTokenCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	c.calls++
	return azcore.AccessToken{Token: "masked-token", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestBootstrapIsIdempotent(t *testing.T) {
	tokenCredential := &staticTokenCredential{}
	requests := 0
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		require.Equal(t, "Bearer masked-token", request.Header.Get("Authorization"))
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})}
	credential := NewWithTokenCredential(tokenCredential, httpClient)

	require.NoError(t, credential.Bootstrap(context.Background()))
	require.NoError(t, credential.Bootstrap(context.Background()))
	require.Equal(t, 1, requests)
	require.Equal(t, 1, tokenCredential.calls)
}

func TestBootstrapErrorDoesNotExposeResponseBody(t *testing.T) {
	credential := NewWithTokenCredential(&staticTokenCredential{}, &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusForbidden, Body: io.NopCloser(strings.NewReader(`{"client_secret":"do-not-log"}`))}, nil
	})})

	err := credential.Bootstrap(context.Background())
	require.EqualError(t, err, "fabric bootstrap API returned HTTP 403")
	require.NotContains(t, err.Error(), "do-not-log")
}
