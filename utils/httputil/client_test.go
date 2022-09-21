package httputil_test

import (
	"net/http"
	"testing"

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
