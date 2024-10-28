package controlplane

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"os"
	"syscall"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	mockhttp "github.com/rudderlabs/rudder-server/mocks/services/oauth/v2/http"
	testutils "github.com/rudderlabs/rudder-server/utils/tests"
)

var _ = Describe("CpConnector", func() {
	It("Test CpApiCall function to test success scenario", func() {
		ctrl := gomock.NewController(GinkgoT())
		mockHttpClient := mockhttp.NewMockHttpClient(ctrl)
		mockHttpClient.EXPECT().Do(gomock.Any()).Return(&http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader([]byte("test"))),
		}, nil)

		cpConnector := NewConnector(
			config.Default,
			WithClient(mockHttpClient),
			WithStats(stats.Default),
		)
		statusCode, respBody := cpConnector.CpApiCall(&Request{
			Method:        http.MethodGet,
			URL:           "https://www.google.com",
			BasicAuthUser: &testutils.BasicAuthMock{},
		})
		Expect(statusCode).To(Equal(http.StatusOK))
		Expect(respBody).To(Equal("test"))
	})

	It("Test CpApiCall function to test timeout situation", func() {
		ctrl := gomock.NewController(GinkgoT())
		mockHttpClient := mockhttp.NewMockHttpClient(ctrl)
		mockHttpClient.EXPECT().Do(gomock.Any()).Return(&http.Response{
			StatusCode: http.StatusServiceUnavailable,
		}, &net.OpError{
			Op:     "mock",
			Net:    "mock",
			Source: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234},
			Addr:   &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12340},
			Err:    &os.SyscallError{Syscall: "read", Err: syscall.ETIMEDOUT},
		})
		cpConnector := NewConnector(
			config.Default,
			WithClient(mockHttpClient),
			WithStats(stats.Default),
		)
		statusCode, respBody := cpConnector.CpApiCall(&Request{
			Method:        http.MethodGet,
			URL:           "https://www.google.com",
			BasicAuthUser: &testutils.BasicAuthMock{},
		})
		Expect(statusCode).To(Equal(http.StatusInternalServerError))
		Expect(`{"errorType":"timeout","message":"mock mock 127.0.0.1:1234->127.0.0.1:12340: read: connection timed out"}`).To(MatchJSON(respBody))
	})

	It("Test CpApiCall function to test connection reset by peer", func() {
		ctrl := gomock.NewController(GinkgoT())
		mockHttpClient := mockhttp.NewMockHttpClient(ctrl)
		mockHttpClient.EXPECT().Do(gomock.Any()).Return(&http.Response{
			StatusCode: http.StatusServiceUnavailable,
		}, &net.OpError{
			Op:     "mock",
			Net:    "mock",
			Source: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234},
			Addr:   &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12340},
			Err:    &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET},
		})
		cpConnector := NewConnector(
			config.Default,
			WithClient(mockHttpClient),
			WithStats(stats.Default),
		)
		statusCode, respBody := cpConnector.CpApiCall(&Request{
			Method:        http.MethodGet,
			URL:           "https://www.google.com",
			BasicAuthUser: &testutils.BasicAuthMock{},
		})
		Expect(statusCode).To(Equal(http.StatusInternalServerError))
		Expect(`{"errorType":"econnreset","message":"mock mock 127.0.0.1:1234->127.0.0.1:12340: read: connection reset by peer"}`).To(MatchJSON(respBody))
	})

	Describe("Test GetErrorType function", func() {
		It("Call GetErrorType with invalid_grant response from control-plane", func() {
			err := &net.OpError{
				Op:     "mock",
				Net:    "mock",
				Source: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234},
				Addr:   &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12340},
				Err:    &os.SyscallError{Syscall: "read", Err: syscall.ETIMEDOUT},
			}
			errType := GetErrorType(err)
			Expect(errType).To(Equal("timeout"))
		})
		It("Call GetErrorType with invalid_grant response from control-plane", func() {
			err := &net.OpError{
				Op:     "mock",
				Net:    "mock",
				Source: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234},
				Addr:   &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12340},
				Err:    &os.SyscallError{Syscall: "read", Err: syscall.ECONNREFUSED},
			}
			errType := GetErrorType(err)
			Expect(errType).To(Equal("econnrefused"))
		})
	})
})
