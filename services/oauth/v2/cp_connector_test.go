package v2_test

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"syscall"

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
		Expect(statusCode).To(Equal(http.StatusOK))
		Expect(respBody).To(Equal("test"))
	})

	It("Test CpApiCall function to test timeout situation", func() {
		ctrl := gomock.NewController(GinkgoT())
		mockHttpClient := mock_oauthV2.NewMockHttpClient(ctrl)
		mockHttpClient.EXPECT().Do(gomock.Any()).Return(&http.Response{
			StatusCode: http.StatusServiceUnavailable,
		}, &net.OpError{
			Op:     "mock",
			Net:    "mock",
			Source: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234},
			Addr:   &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12340},
			Err:    &os.SyscallError{Syscall: "read", Err: syscall.ETIMEDOUT},
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
		expectedResp := fmt.Sprintf("{\n\t\t\t\t\"%v\": \"timeout\",\n\t\t\t\t\"message\": \t\"mock mock 127.0.0.1:1234->127.0.0.1:12340: read: operation timed out\"\n\t\t\t}", v2.ErrorType)
		Expect(statusCode).To(Equal(http.StatusServiceUnavailable))
		Expect(respBody).To(Equal(expectedResp))
	})

	It("Test CpApiCall function to test connection reset by peer", func() {
		ctrl := gomock.NewController(GinkgoT())
		mockHttpClient := mock_oauthV2.NewMockHttpClient(ctrl)
		mockHttpClient.EXPECT().Do(gomock.Any()).Return(&http.Response{
			StatusCode: http.StatusServiceUnavailable,
		}, &net.OpError{
			Op:     "mock",
			Net:    "mock",
			Source: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234},
			Addr:   &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12340},
			Err:    &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET},
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
		expectedResp := fmt.Sprintf("{\n\t\t\t\t\"%v\": \"econnreset\",\n\t\t\t\t\"message\": \t\"mock mock 127.0.0.1:1234->127.0.0.1:12340: read: connection reset by peer\"\n\t\t\t}", v2.ErrorType)
		Expect(statusCode).To(Equal(http.StatusServiceUnavailable))
		Expect(respBody).To(Equal(expectedResp))
	})
})
