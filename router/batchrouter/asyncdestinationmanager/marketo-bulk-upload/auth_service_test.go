package marketobulkupload

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestMarketoAuthService_GetAccessToken(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func() *http.Client
		expectedToken string
		expectError   bool
	}{
		{
			name: "successful token fetch",
			setupMock: func() *http.Client {
				json := `{
					"access_token": "test-token",
					"token_type": "bearer",
					"expires_in": 3600,
					"scope": "test-scope"
				}`
				response := &http.Response{
					StatusCode: 200,
					Body:       io.NopCloser(bytes.NewBufferString(json)),
				}
				return createMockClient(response, nil)
			},
			expectedToken: "test-token",
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := &MarketoAuthService{
				httpCLient:   tt.setupMock(),
				munchkinId:   "test-munchkin",
				clientId:     "test-client",
				clientSecret: "test-secret",
			}

			if tt.name == "reuse existing valid token" {
				// Set up an existing valid token
				service.accessToken = MarketoAccessToken{
					AccessToken: "existing-token",
					ExpiresIn:   3600,
					FetchedAt:   time.Now().Unix(),
				}
			}

			token, err := service.GetAccessToken()

			if tt.expectError && err == nil {
				t.Error("expected error but got none")
			}

			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if token != tt.expectedToken {
				t.Errorf("expected token %s, got %s", tt.expectedToken, token)
			}
		})
	}
}

func TestMarketoAuthService_fetchOrUpdateAccessToken(t *testing.T) {
	tests := []struct {
		name        string
		setupMock   func() *http.Client
		expectError bool
	}{
		{
			name: "successful token fetch",
			setupMock: func() *http.Client {
				json := `{
					"access_token": "new-test-token",
					"token_type": "bearer",
					"expires_in": 3600,
					"scope": "test-scope"
				}`
				response := &http.Response{
					StatusCode: 200,
					Body:       io.NopCloser(bytes.NewBufferString(json)),
				}
				return createMockClient(response, nil)
			},
			expectError: false,
		},
		{
			name: "invalid json response",
			setupMock: func() *http.Client {
				response := &http.Response{
					StatusCode: 200,
					Body:       io.NopCloser(bytes.NewBufferString("invalid json")),
				}
				return createMockClient(response, nil)
			},
			expectError: true,
		},
		{
			name: "http error",
			setupMock: func() *http.Client {
				return createMockClient(nil, fmt.Errorf("connection error"))
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := &MarketoAuthService{
				httpCLient:   tt.setupMock(),
				munchkinId:   "test-munchkin",
				clientId:     "test-client",
				clientSecret: "test-secret",
			}

			err := service.fetchOrUpdateAccessToken()

			if tt.expectError && err == nil {
				t.Error("expected error but got none")
			}

			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if !tt.expectError {
				if service.accessToken.AccessToken == "" {
					t.Error("access token should not be empty after successful fetch")
				}
				if service.accessToken.FetchedAt == 0 {
					t.Error("FetchedAt should be set after successful fetch")
				}
			}
		})
	}
}
