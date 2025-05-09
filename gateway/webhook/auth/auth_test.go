package auth

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/gateway/response"

	gwtypes "github.com/rudderlabs/rudder-server/gateway/types"
)

func TestNewWebhookAuth(t *testing.T) {
	tests := []struct {
		name                      string
		mockOnFailure             func(w http.ResponseWriter, r *http.Request, errorMessage string, authCtx *gwtypes.AuthRequestContext)
		mockAuthReqCtxForWriteKey func(writeKey string) (*gwtypes.AuthRequestContext, error)
		writeKey                  string
		expectedResponseCode      int
		expectedResponseMessage   string
	}{
		{
			name: "valid write key",
			mockOnFailure: func(w http.ResponseWriter, r *http.Request, errorMessage string, _ *gwtypes.AuthRequestContext) {
				t.Error("onFailure should not be called for a valid write key")
			},
			mockAuthReqCtxForWriteKey: func(writeKey string) (*gwtypes.AuthRequestContext, error) {
				return &gwtypes.AuthRequestContext{
					SourceCategory: "webhook",
					SourceEnabled:  true,
				}, nil
			},
			writeKey:                "valid-key",
			expectedResponseCode:    http.StatusOK,
			expectedResponseMessage: response.Ok,
		},
		{
			name: "invalid write key",
			mockOnFailure: func(w http.ResponseWriter, r *http.Request, errorMessage string, _ *gwtypes.AuthRequestContext) {
				http.Error(w, errorMessage, http.StatusUnauthorized)
			},
			mockAuthReqCtxForWriteKey: func(writeKey string) (*gwtypes.AuthRequestContext, error) {
				return nil, ErrSourceNotFound
			},
			writeKey:                "invalid-key",
			expectedResponseCode:    http.StatusUnauthorized,
			expectedResponseMessage: fmt.Sprintf("%s\n", response.InvalidWriteKey),
		},
		{
			name: "empty write key",
			mockOnFailure: func(w http.ResponseWriter, r *http.Request, errorMessage string, _ *gwtypes.AuthRequestContext) {
				http.Error(w, errorMessage, http.StatusUnauthorized)
			},
			mockAuthReqCtxForWriteKey: func(writeKey string) (*gwtypes.AuthRequestContext, error) {
				return nil, errors.New("write key is empty")
			},
			writeKey:                "",
			expectedResponseCode:    http.StatusUnauthorized,
			expectedResponseMessage: fmt.Sprintf("%s\n", response.NoWriteKeyInQueryParams),
		},
		{
			name: "disabled source",
			mockOnFailure: func(w http.ResponseWriter, r *http.Request, errorMessage string, _ *gwtypes.AuthRequestContext) {
				http.Error(w, errorMessage, http.StatusForbidden)
			},
			mockAuthReqCtxForWriteKey: func(writeKey string) (*gwtypes.AuthRequestContext, error) {
				return &gwtypes.AuthRequestContext{
					SourceCategory: "webhook",
					SourceEnabled:  false,
				}, nil
			},
			writeKey:                "disabled-source-key",
			expectedResponseCode:    http.StatusForbidden,
			expectedResponseMessage: fmt.Sprintf("%s\n", response.SourceDisabled),
		},
		{
			name: "error getting auth context",
			mockOnFailure: func(w http.ResponseWriter, r *http.Request, errorMessage string, _ *gwtypes.AuthRequestContext) {
				http.Error(w, errorMessage, http.StatusInternalServerError)
			},
			mockAuthReqCtxForWriteKey: func(writeKey string) (*gwtypes.AuthRequestContext, error) {
				return nil, errors.New("error getting auth context")
			},
			writeKey:                "error-auth-key",
			expectedResponseCode:    http.StatusInternalServerError,
			expectedResponseMessage: fmt.Sprintf("%s\n", response.ErrAuthenticatingWebhookRequest),
		},
		{
			name: "source category not webhook",
			mockOnFailure: func(w http.ResponseWriter, r *http.Request, errorMessage string, _ *gwtypes.AuthRequestContext) {
				http.Error(w, errorMessage, http.StatusBadRequest)
			},
			mockAuthReqCtxForWriteKey: func(writeKey string) (*gwtypes.AuthRequestContext, error) {
				return &gwtypes.AuthRequestContext{
					SourceCategory: "cloud",
					SourceEnabled:  true,
				}, nil
			},
			writeKey:                "non-webhook-key",
			expectedResponseCode:    http.StatusBadRequest,
			expectedResponseMessage: fmt.Sprintf("%s\n", response.InvalidWriteKey),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Initialize WebhookAuth instance
			webhookAuth := NewWebhookAuth(tt.mockOnFailure, tt.mockAuthReqCtxForWriteKey)

			// Set up mock HTTP server
			server := httptest.NewServer(webhookAuth.AuthHandler(func(w http.ResponseWriter, request *http.Request) {
				_, _ = w.Write([]byte(response.Ok))
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			// Prepare HTTP request
			req, _ := http.NewRequest(http.MethodGet, server.URL, nil)
			req.SetBasicAuth(tt.writeKey, "")

			// Send HTTP request
			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}
			defer resp.Body.Close()

			// Validate response
			if resp.StatusCode != tt.expectedResponseCode {
				t.Errorf("expected status code %d, got %d", tt.expectedResponseCode, resp.StatusCode)
			}

			respBody, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			if string(respBody) != tt.expectedResponseMessage {
				t.Errorf("expected response body %q, got %q", tt.expectedResponseMessage, string(respBody))
			}
		})
	}
}
