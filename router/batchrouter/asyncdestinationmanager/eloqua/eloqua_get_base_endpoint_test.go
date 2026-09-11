package eloqua

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetBaseEndpointReturnsStatusCodeAndBody(t *testing.T) {
	withDefaultTransport(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "https://login.eloqua.com/id", r.URL.String())
		return response(http.StatusUnauthorized, `{"error":"invalid credentials"}`), nil
	}))

	service := NewEloquaServiceImpl("2.0")

	baseEndpoint, err := service.GetBaseEndpoint(&HttpRequestData{})

	require.Empty(t, baseEndpoint)
	require.ErrorContains(t, err, "eloqua login returned status 401")
	require.ErrorContains(t, err, `{"error":"invalid credentials"}`)
}

func TestGetBaseEndpointReturnsErrorForMissingBaseURL(t *testing.T) {
	withDefaultTransport(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "https://login.eloqua.com/id", r.URL.String())
		return response(http.StatusOK, `{"urls":{}}`), nil
	}))

	service := NewEloquaServiceImpl("2.0")

	baseEndpoint, err := service.GetBaseEndpoint(&HttpRequestData{})

	require.Empty(t, baseEndpoint)
	require.ErrorContains(t, err, "eloqua login response missing urls.base")
	require.ErrorContains(t, err, `{"urls":{}}`)
}

func TestGetBaseEndpointReturnsErrorForInvalidLoginResponse(t *testing.T) {
	withDefaultTransport(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "https://login.eloqua.com/id", r.URL.String())
		return response(http.StatusOK, `{"urls":`), nil
	}))

	service := NewEloquaServiceImpl("2.0")

	baseEndpoint, err := service.GetBaseEndpoint(&HttpRequestData{})

	require.Empty(t, baseEndpoint)
	require.ErrorContains(t, err, "Unable to parse eloqua login response")
	require.ErrorContains(t, err, `{"urls":`)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func withDefaultTransport(t *testing.T, transport http.RoundTripper) {
	t.Helper()
	originalTransport := http.DefaultTransport
	http.DefaultTransport = transport
	t.Cleanup(func() {
		http.DefaultTransport = originalTransport
	})
}

func response(statusCode int, body string) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}
