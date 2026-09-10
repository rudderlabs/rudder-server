package eloqua

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetBaseEndpointReturnsStatusCodeAndBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"invalid credentials"}`))
	}))
	defer server.Close()

	service := NewEloquaServiceImpl("2.0")
	service.loginEndpoint = server.URL

	baseEndpoint, err := service.GetBaseEndpoint(&HttpRequestData{})

	require.Empty(t, baseEndpoint)
	require.ErrorContains(t, err, "eloqua login returned status 401")
	require.ErrorContains(t, err, `{"error":"invalid credentials"}`)
}

func TestGetBaseEndpointReturnsErrorForMissingBaseURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"urls":{}}`))
	}))
	defer server.Close()

	service := NewEloquaServiceImpl("2.0")
	service.loginEndpoint = server.URL

	baseEndpoint, err := service.GetBaseEndpoint(&HttpRequestData{})

	require.Empty(t, baseEndpoint)
	require.ErrorContains(t, err, "eloqua login response missing urls.base")
	require.ErrorContains(t, err, `{"urls":{}}`)
}
