package httputil_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/utils/httputil"
)

func TestRetriableStatus(t *testing.T) {
	retribaleCodes := []int{
		// 4xx
		http.StatusRequestTimeout,
		http.StatusTooManyRequests,

		// 5xx
		http.StatusInternalServerError,
		http.StatusNotImplemented,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout,
		http.StatusHTTPVersionNotSupported,
		http.StatusVariantAlsoNegotiates,
		http.StatusInsufficientStorage,
		http.StatusLoopDetected,
		http.StatusNotExtended,
		http.StatusNetworkAuthenticationRequired,
	}

	nonRetriableCodes := []int{
		// 2xx
		http.StatusOK,
		http.StatusCreated,
		http.StatusAccepted,
		http.StatusNonAuthoritativeInfo,
		http.StatusNoContent,
		http.StatusResetContent,
		http.StatusPartialContent,
		http.StatusMultiStatus,
		http.StatusAlreadyReported,
		http.StatusIMUsed,

		// 4xx
		http.StatusBadRequest,
		http.StatusUnauthorized,
		http.StatusPaymentRequired,
		http.StatusForbidden,
		http.StatusNotFound,
		http.StatusMethodNotAllowed,
		http.StatusNotAcceptable,
		http.StatusProxyAuthRequired,
		http.StatusConflict,
		http.StatusGone,
		http.StatusLengthRequired,
		http.StatusPreconditionFailed,
	}

	for _, code := range retribaleCodes {
		if !httputil.RetriableStatus(code) {
			t.Errorf("Expected %d to be retriable", code)
		}
	}

	for _, code := range nonRetriableCodes {
		if httputil.RetriableStatus(code) {
			t.Errorf("Expected %d to be non retriable", code)
		}
	}
}

func TestReadAndCloseResponse(t *testing.T) {
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("test"))
	}))
	defer httpServer.Close()

	t.Run("it can read & close a non nil response that hasn't been read", func(t *testing.T) {
		resp, err := http.Get(httpServer.URL)
		require.NoError(t, err)
		require.NotNil(t, resp)
		func() { httputil.CloseResponse(resp) }()
	})

	t.Run("it can read & close a non nil response that has already been read", func(t *testing.T) {
		resp, err := http.Get(httpServer.URL)
		require.NoError(t, err)
		require.NotNil(t, resp)
		_, err = io.ReadAll(resp.Body)
		require.NoError(t, err)
		func() { httputil.CloseResponse(resp) }()
	})

	t.Run("it won't panic if we try to read & close a nil response", func(t *testing.T) {
		httputil.CloseResponse(nil)
	})

	t.Run("it won't panic if we try to read & close a non-nil response with a nil body", func(t *testing.T) {
		httputil.CloseResponse(&http.Response{})
	})
}
