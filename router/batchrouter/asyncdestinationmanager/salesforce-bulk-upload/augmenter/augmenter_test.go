package augmenter

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
)

func TestRequestAugmenter_Augment(t *testing.T) {
	tests := []struct {
		name          string
		secret        json.RawMessage
		body          []byte
		expectedError string
		validateFunc  func(t *testing.T, r *http.Request)
	}{
		{
			name:   "valid secret with access_token and instance_url",
			secret: json.RawMessage(`{"access_token":"test_token_123","instance_url":"https://test.salesforce.com"}`),
			body:   []byte(`{"test":"data"}`),
			validateFunc: func(t *testing.T, r *http.Request) {
				require.Equal(t, "Bearer test_token_123", r.Header.Get("Authorization"))
				require.Equal(t, "test.salesforce.com", r.URL.Host)
				bodyBytes, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				require.Equal(t, []byte(`{"test":"data"}`), bodyBytes)
			},
		},
		{
			name:   "valid secret with instance_url without https prefix",
			secret: json.RawMessage(`{"access_token":"token_456","instance_url":"custom.salesforce.com"}`),
			body:   []byte(`{"key":"value"}`),
			validateFunc: func(t *testing.T, r *http.Request) {
				require.Equal(t, "Bearer token_456", r.Header.Get("Authorization"))
				require.Equal(t, "custom.salesforce.com", r.URL.Host)
				bodyBytes, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				require.Equal(t, []byte(`{"key":"value"}`), bodyBytes)
				require.NotContains(t, r.URL.Host, "https://")
			},
		},
		{
			name:          "nil secret",
			secret:        nil,
			body:          []byte(`{"test":"data"}`),
			expectedError: ErrSecretNil.Error(),
		},
		{
			name:          "empty access_token",
			secret:        json.RawMessage(`{"access_token":"","instance_url":"https://test.salesforce.com"}`),
			body:          []byte(`{"test":"data"}`),
			expectedError: ErrAccessTokenEmpty.Error(),
		},
		{
			name:          "missing access_token",
			secret:        json.RawMessage(`{"instance_url":"https://test.salesforce.com"}`),
			body:          []byte(`{"test":"data"}`),
			expectedError: ErrAccessTokenEmpty.Error(),
		},
		{
			name:          "empty instance_url",
			secret:        json.RawMessage(`{"access_token":"test_token","instance_url":""}`),
			body:          []byte(`{"test":"data"}`),
			expectedError: ErrInstanceURLEmpty.Error(),
		},
		{
			name:          "missing instance_url",
			secret:        json.RawMessage(`{"access_token":"test_token"}`),
			body:          []byte(`{"test":"data"}`),
			expectedError: ErrInstanceURLEmpty.Error(),
		},
		{
			name:   "empty body is handled correctly",
			secret: json.RawMessage(`{"access_token":"token_empty","instance_url":"https://test.salesforce.com"}`),
			body:   []byte{},
			validateFunc: func(t *testing.T, r *http.Request) {
				require.Equal(t, "Bearer token_empty", r.Header.Get("Authorization"))
				require.Equal(t, "test.salesforce.com", r.URL.Host)
				bodyBytes, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				require.Empty(t, bodyBytes)
			},
		},
		{
			name:   "large body is handled correctly",
			secret: json.RawMessage(`{"access_token":"token_large","instance_url":"https://test.salesforce.com"}`),
			body:   bytes.Repeat([]byte("a"), 10000),
			validateFunc: func(t *testing.T, r *http.Request) {
				require.Equal(t, "Bearer token_large", r.Header.Get("Authorization"))
				bodyBytes, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				require.Len(t, bodyBytes, 10000)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req, err := http.NewRequest("POST", "https://example.com/api", bytes.NewReader(tt.body))
			require.NoError(t, err)

			err = NewRequestAugmenter().Augment(req, tt.body, tt.secret)

			if tt.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedError)
				return
			}

			require.NoError(t, err)
			if tt.validateFunc != nil {
				tt.validateFunc(t, req)
			}
		})
	}
}

func TestGetAuthErrorCategoryForSalesforce(t *testing.T) {
	tests := []struct {
		name             string
		responseBody     []byte
		expectedCategory string
	}{
		{
			name: "INVALID_SESSION_ID error code returns refresh token category",
			responseBody: []byte(`[
				{
					"message": "Session expired or invalid",
					"errorCode": "INVALID_SESSION_ID"
				}
			]`),
			expectedCategory: common.CategoryRefreshToken,
		},
		{
			name: "multiple errors with INVALID_SESSION_ID",
			responseBody: []byte(`[
				{
					"message": "Some other error",
					"errorCode": "OTHER_ERROR"
				},
				{
					"message": "Session expired or invalid",
					"errorCode": "INVALID_SESSION_ID"
				}
			]`),
			expectedCategory: common.CategoryRefreshToken,
		},
		{
			name: "other error codes return empty category",
			responseBody: []byte(`[
				{
					"message": "Invalid field",
					"errorCode": "INVALID_FIELD"
				}
			]`),
			expectedCategory: "",
		},
		{
			name: "multiple errors without INVALID_SESSION_ID",
			responseBody: []byte(`[
				{
					"message": "Error 1",
					"errorCode": "ERROR_1"
				},
				{
					"message": "Error 2",
					"errorCode": "ERROR_2"
				}
			]`),
			expectedCategory: "",
		},
		{
			name:             "empty response body",
			responseBody:     []byte(`[]`),
			expectedCategory: "",
		},
		{
			name:             "empty array",
			responseBody:     []byte(`[]`),
			expectedCategory: "",
		},
		{
			name: "single error object without errorCode field",
			responseBody: []byte(`[
				{
					"message": "Some error"
				}
			]`),
			expectedCategory: "",
		},
		{
			name: "case sensitive error code matching",
			responseBody: []byte(`[
				{
					"message": "Session expired",
					"errorCode": "invalid_session_id"
				}
			]`),
			expectedCategory: "",
		},
		{
			name:             "invalid JSON returns empty category",
			responseBody:     []byte(`invalid json`),
			expectedCategory: "",
		},
		{
			name:             "empty string response",
			responseBody:     []byte(``),
			expectedCategory: "",
		},
		{
			name:             "null response",
			responseBody:     []byte(`null`),
			expectedCategory: "",
		},
		{
			name: "object instead of array",
			responseBody: []byte(`{
				"errorCode": "INVALID_SESSION_ID",
				"message": "Session expired"
			}`),
			expectedCategory: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			category := GetAuthErrorCategoryForSalesforce(tt.responseBody)

			require.Equal(t, tt.expectedCategory, category)
		})
	}
}

func TestRequestAugmenter_Augment_RequestModification(t *testing.T) {
	t.Run("original request URL is preserved except host", func(t *testing.T) {
		t.Parallel()

		originalURL, err := url.Parse("https://original.com/path?query=value")
		require.NoError(t, err)

		req := &http.Request{
			Method: "POST",
			URL:    originalURL,
			Header: make(http.Header),
		}

		secret := json.RawMessage(`{"access_token":"test_token","instance_url":"https://newhost.salesforce.com"}`)
		body := []byte(`{"test":"data"}`)

		err = NewRequestAugmenter().Augment(req, body, secret)
		require.NoError(t, err)

		require.Equal(t, "newhost.salesforce.com", req.URL.Host)
		require.Equal(t, "/path", req.URL.Path)
		require.Equal(t, "query=value", req.URL.RawQuery)
		require.Equal(t, "Bearer test_token", req.Header.Get("Authorization"))
	})

	t.Run("request body can be read multiple times", func(t *testing.T) {
		t.Parallel()

		req, err := http.NewRequest("POST", "https://example.com/api", nil)
		require.NoError(t, err)

		body := []byte(`{"readable":"multiple times"}`)
		secret := json.RawMessage(`{"access_token":"token","instance_url":"https://test.salesforce.com"}`)

		err = NewRequestAugmenter().Augment(req, body, secret)
		require.NoError(t, err)

		// Read body first time
		bodyBytes1, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		require.Equal(t, body, bodyBytes1)

		// Reset body for second read
		req.Body = io.NopCloser(bytes.NewReader(body))
		bodyBytes2, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		require.Equal(t, body, bodyBytes2)
	})
}
