package ratelimiter

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"

	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	mock_oauthV2 "github.com/rudderlabs/rudder-server/mocks/services/oauthV2"
)

var _ = Describe("Rate Limiter Tests", func() {
	var (
		ctrl          *gomock.Controller
		mockTransport *mock_oauthV2.MockRoundTripper
		rateLimiter   *RefreshTokenRateLimiter
		transport     *RateLimiterTransport
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		mockTransport = mock_oauthV2.NewMockRoundTripper(ctrl)
		rateLimiter = NewRefreshTokenRateLimiter(
			100*time.Millisecond, // token refresh interval
			50*time.Millisecond,  // invalid grant interval
		)
		rateLimiter.mu = kitsync.NewPartitionRWLocker() // Initialize the mutex
		transport = &RateLimiterTransport{
			RateLimiter: rateLimiter,
			Transport:   mockTransport,
		}
	})

	AfterEach(func() {
		ctrl.Finish()
	})

	It("should not rate limit non-token endpoints", func() {
		mockTransport.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader([]byte(`{"data":"success"}`))),
		}, nil)

		req, _ := http.NewRequest(http.MethodGet, "https://api.rudderstack.com/destination/workspaces/ws1/accounts/123/other", nil)
		resp, err := transport.RoundTrip(req)

		Expect(err).To(BeNil())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
	})

	It("should not rate limit token requests with empty body", func() {
		mockTransport.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader([]byte(`{"data":"success"}`))),
		}, nil)

		req, _ := http.NewRequest(http.MethodPost, "https://api.rudderstack.com/destination/workspaces/ws1/accounts/123/token", nil)
		resp, err := transport.RoundTrip(req)

		Expect(err).To(BeNil())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
	})

	It("should rate limit successful token refresh requests", func() {
		// First request succeeds
		mockTransport.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader([]byte(`{"access_token":"new_token"}`))),
		}, nil)

		body := strings.NewReader(`{"refresh_token":"token"}`)
		req, _ := http.NewRequest(http.MethodPost, "https://api.rudderstack.com/destination/workspaces/ws1/accounts/123/token", body)

		resp, err := transport.RoundTrip(req)
		Expect(err).To(BeNil())
		Expect(resp.StatusCode).To(Equal(http.StatusInternalServerError)) // Note: successful refresh gets status changed to 500

		// Second immediate request should be rate limited and return the cached response
		body = strings.NewReader(`{"refresh_token":"token"}`)
		req, _ = http.NewRequest(http.MethodPost, "https://api.rudderstack.com/destination/workspaces/ws1/accounts/123/token", body)

		resp, err = transport.RoundTrip(req)
		Expect(err).To(BeNil())
		Expect(resp.StatusCode).To(Equal(http.StatusInternalServerError))
	})

	It("should handle invalid_grant responses", func() {
		mockTransport.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
			StatusCode: http.StatusUnauthorized,
			Body:       io.NopCloser(bytes.NewReader([]byte(`{"body":{"code":"invalid_grant"}}`))),
		}, nil)

		body := strings.NewReader(`{"refresh_token":"invalid_token"}`)
		req, _ := http.NewRequest(http.MethodPost, "https://api.rudderstack.com/destination/workspaces/ws1/accounts/123/token", body)

		resp, err := transport.RoundTrip(req)
		Expect(err).To(BeNil())
		Expect(resp.StatusCode).To(Equal(http.StatusBadRequest)) // Note: invalid_grant gets changed to 400

		// Second immediate request should be rate limited with same response
		body = strings.NewReader(`{"refresh_token":"invalid_token"}`)
		req, _ = http.NewRequest(http.MethodPost, "https://api.rudderstack.com/destination/workspaces/ws1/accounts/123/token", body)

		resp, err = transport.RoundTrip(req)
		Expect(err).To(BeNil())
		Expect(resp.StatusCode).To(Equal(http.StatusBadRequest))
	})

	It("should allow requests after rate limit window expires", func() {
		// First request
		mockTransport.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader([]byte(`{"access_token":"token1"}`))),
		}, nil)

		body := strings.NewReader(`{"refresh_token":"token"}`)
		req, _ := http.NewRequest(http.MethodPost, "https://api.rudderstack.com/destination/workspaces/ws1/accounts/123/token", body)

		resp, err := transport.RoundTrip(req)
		Expect(err).To(BeNil())
		Expect(resp.StatusCode).To(Equal(http.StatusInternalServerError))

		// Wait for rate limit window to expire
		time.Sleep(150 * time.Millisecond)

		// Second request should succeed
		mockTransport.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader([]byte(`{"access_token":"token2"}`))),
		}, nil)

		body = strings.NewReader(`{"refresh_token":"token"}`)
		req, _ = http.NewRequest(http.MethodPost, "https://api.rudderstack.com/destination/workspaces/ws1/accounts/123/token", body)

		resp, err = transport.RoundTrip(req)
		Expect(err).To(BeNil())
		Expect(resp.StatusCode).To(Equal(http.StatusInternalServerError))
	})

	It("should return error for invalid URLs", func() {
		req, _ := http.NewRequest(http.MethodPost, "invalid-url", nil)
		resp, err := transport.RoundTrip(req)

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("failed to parse account ID from URL"))
		Expect(resp).To(BeNil())
	})
})
