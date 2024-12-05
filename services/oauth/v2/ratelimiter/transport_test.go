package ratelimiter

import (
	"bytes"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	mock_http_client "github.com/rudderlabs/rudder-server/mocks/services/oauth/v2/http"
)

func TestRateLimiterTransport(t *testing.T) {
	tests := []struct {
		name           string
		url            string
		body           string
		setupMocks     func(*mock_http_client.MockRoundTripper, *http.Request)
		expectedStatus int
		expectedErr    error
	}{
		{
			name: "successful token refresh",
			url:  "https://api.rudderstack.com/destination/workspaces/ws1/accounts/123/token",
			body: `{"refresh_token": "token123"}`,
			setupMocks: func(mt *mock_http_client.MockRoundTripper, req *http.Request) {
				successResp := &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader([]byte(`{"access_token":"new_token"}`))),
				}
				mt.EXPECT().RoundTrip(gomock.Any()).Return(successResp, nil)
			},
			expectedStatus: http.StatusOK,
			expectedErr:    nil,
		},
		{
			name: "invalid grant response",
			url:  "https://api.rudderstack.com/destination/workspaces/ws1/accounts/123/token",
			body: `{"refresh_token": "invalid_token"}`,
			setupMocks: func(mt *mock_http_client.MockRoundTripper, req *http.Request) {
				invalidGrantResp := &http.Response{
					StatusCode: http.StatusForbidden,
					Body:       io.NopCloser(bytes.NewReader([]byte(`{"body":{"code":"ref_token_invalid_grant"}}`))),
				}
				mt.EXPECT().RoundTrip(gomock.Any()).Return(invalidGrantResp, nil)
			},
			expectedStatus: http.StatusForbidden,
			expectedErr:    nil,
		},
		{
			name: "non-token endpoint",
			url:  "https://api.rudderstack.com/destination/workspaces/ws1/accounts/123/other",
			body: "",
			setupMocks: func(mt *mock_http_client.MockRoundTripper, req *http.Request) {
				successResp := &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader([]byte(`{"data":"success"}`))),
				}
				mt.EXPECT().RoundTrip(req).Return(successResp, nil)
			},
			expectedStatus: http.StatusOK,
			expectedErr:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockTransport := mock_http_client.NewMockRoundTripper(ctrl)
			rateLimiter := &RefreshTokenRateLimiter{
				AllowedTokenRefreshInterval: 100 * time.Millisecond,
				AllowedInvalidGrantInterval: 50 * time.Millisecond,
				mu:                          kitsync.NewPartitionRWLocker(),
			}

			transport := &RateLimiterTransport{
				RateLimiter: rateLimiter,
				Transport:   mockTransport,
			}

			var body io.Reader
			if tt.body != "" {
				body = bytes.NewReader([]byte(tt.body))
			}
			req, err := http.NewRequest(http.MethodPost, tt.url, body)
			assert.NoError(t, err)

			tt.setupMocks(mockTransport, req)

			resp, err := transport.RoundTrip(req)

			if tt.expectedErr != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedErr.Error(), err.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedStatus, resp.StatusCode)
			}
		})
	}
}

func TestRateLimiterTransport_RateLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTransport := mock_http_client.NewMockRoundTripper(ctrl)
	rateLimiter := &RefreshTokenRateLimiter{
		AllowedTokenRefreshInterval: 100 * time.Millisecond,
		AllowedInvalidGrantInterval: 50 * time.Millisecond,
		mu:                          kitsync.NewPartitionRWLocker(),
	}

	transport := &RateLimiterTransport{
		RateLimiter: rateLimiter,
		Transport:   mockTransport,
	}

	// First request should succeed
	successResp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewReader([]byte(`{"access_token":"new_token"}`))),
	}
	mockTransport.EXPECT().RoundTrip(gomock.Any()).Return(successResp, nil)

	body := bytes.NewReader([]byte(`{"refresh_token": "token123"}`))
	req, _ := http.NewRequest(http.MethodPost, "https://api.rudderstack.com/destination/workspaces/ws1/accounts/123/token", body)

	resp, err := transport.RoundTrip(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Immediate second request should be rate limited
	body = bytes.NewReader([]byte(`{"refresh_token": "token123"}`))
	req, _ = http.NewRequest(http.MethodPost, "https://api.rudderstack.com/destination/workspaces/ws1/accounts/123/token", body)

	resp, err = transport.RoundTrip(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// After waiting, third request should succeed
	time.Sleep(150 * time.Millisecond)

	successResp2 := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewReader([]byte(`{"access_token":"new_token2"}`))),
	}
	mockTransport.EXPECT().RoundTrip(gomock.Any()).Return(successResp2, nil)

	body = bytes.NewReader([]byte(`{"refresh_token": "token123"}`))
	req, _ = http.NewRequest(http.MethodPost, "https://api.rudderstack.com/destination/workspaces/ws1/accounts/123/token", body)

	resp, err = transport.RoundTrip(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
