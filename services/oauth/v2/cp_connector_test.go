package v2_test

import (
	"bytes"
	"errors"
	"io"
	"net/http"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-go-kit/logger"
	mock_oauthV2 "github.com/rudderlabs/rudder-server/mocks/services/oauthV2"
	v2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
)

var _ = Describe("CpConnector", func() {
	It("Test CpApiCall function to test success scenario", func() {
		ctrl := gomock.NewController(GinkgoT())
		mockHttpClient := mock_oauthV2.NewMockHttpClient(ctrl)
		mockHttpClient.EXPECT().Do(gomock.Any()).Return(&http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader([]byte("test"))),
		}, nil)
		cpConnector := v2.ControlPlaneConnector{
			Client: mockHttpClient,
			Logger: logger.NewLogger().Child("ControlPlaneConnector"),
		}
		statusCode, respBody := cpConnector.CpApiCall(&v2.ControlPlaneRequestT{
			Method:        "GET",
			Url:           "https://www.google.com",
			BasicAuthUser: &mock_oauthV2.BasicAuthMock{},
		})
		Expect(statusCode).To(Equal(200))
		Expect(respBody).To(Equal("test"))
	})

	It("Test CpApiCall function to test timeout situation", func() {
		ctrl := gomock.NewController(GinkgoT())
		mockHttpClient := mock_oauthV2.NewMockHttpClient(ctrl)
		mockHttpClient.EXPECT().Do(gomock.Any()).Return(&http.Response{
			StatusCode: http.StatusServiceUnavailable,
		}, mock_oauthV2.MyError{
			Err: errors.New("context deadline exceeded (Client.Timeout exceeded while awaiting headers)"),
		})
		cpConnector := v2.ControlPlaneConnector{
			Client: mockHttpClient,
			Logger: logger.NewLogger().Child("ControlPlaneConnector"),
		}
		statusCode, respBody := cpConnector.CpApiCall(&v2.ControlPlaneRequestT{
			Method:        "GET",
			Url:           "https://www.google.com",
			BasicAuthUser: &mock_oauthV2.BasicAuthMock{},
		})
		Expect(statusCode).To(Equal(503))
		Expect(respBody).To(Equal("{\n\t\t\t\t\"error\": \"network_error\",\n\t\t\t\t\"message\": \t\"control plane service is not available or failed due to timeout.\"\n\t\t\t}"))
	})
})
