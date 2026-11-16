package controlplane

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"os"
	"runtime"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"

	mockhttp "github.com/rudderlabs/rudder-server/mocks/services/oauth/v2/http"
	testutils "github.com/rudderlabs/rudder-server/utils/tests"
)

func TestCpConnector(t *testing.T) {
	timeoutError := "connection timed out"
	if runtime.GOOS == "darwin" {
		timeoutError = "operation timed out"
	}
	t.Run("Test CpApiCall function to test success scenario", func(t *testing.T) {
		ctrl := gomock.NewController(t)
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
			Method:       http.MethodGet,
			URL:          "https://www.google.com",
			AuthIdentity: &testutils.BasicAuthMock{},
		})
		require.Equal(t, http.StatusOK, statusCode)
		require.Equal(t, "test", respBody)
	})

	t.Run("Test CpApiCall function to test timeout situation", func(t *testing.T) {
		ctrl := gomock.NewController(t)
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
			Method:       http.MethodGet,
			URL:          "https://www.google.com",
			AuthIdentity: &testutils.BasicAuthMock{},
		})
		require.Equal(t, http.StatusInternalServerError, statusCode)
		require.JSONEq(t, `{"errorType":"timeout","message":"mock mock 127.0.0.1:1234->127.0.0.1:12340: read: `+timeoutError+`"}`, respBody)
	})

	t.Run("Test CpApiCall function to test connection reset by peer", func(t *testing.T) {
		ctrl := gomock.NewController(t)
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
			Method:       http.MethodGet,
			URL:          "https://www.google.com",
			AuthIdentity: &testutils.BasicAuthMock{},
		})
		require.Equal(t, http.StatusInternalServerError, statusCode)
		require.JSONEq(t, `{"errorType":"econnreset","message":"mock mock 127.0.0.1:1234->127.0.0.1:12340: read: connection reset by peer"}`, respBody)
	})

	t.Run("Test GetErrorType function", func(t *testing.T) {
		t.Run("Call GetErrorType with invalid_grant response from control-plane", func(t *testing.T) {
			err := &net.OpError{
				Op:     "mock",
				Net:    "mock",
				Source: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234},
				Addr:   &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12340},
				Err:    &os.SyscallError{Syscall: "read", Err: syscall.ETIMEDOUT},
			}
			errType := GetErrorType(err)
			require.Equal(t, "timeout", errType)
		})
		t.Run("Call GetErrorType with invalid_grant response from control-plane", func(t *testing.T) {
			err := &net.OpError{
				Op:     "mock",
				Net:    "mock",
				Source: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234},
				Addr:   &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12340},
				Err:    &os.SyscallError{Syscall: "read", Err: syscall.ECONNREFUSED},
			}
			errType := GetErrorType(err)
			require.Equal(t, "econnrefused", errType)
		})
	})
}
