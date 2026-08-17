package user_transformer

import (
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	transformerclient "github.com/rudderlabs/rudder-server/internal/transformer-client"
)

// TestIsColdStartError pins which 503/502 responses mean "the PyT pod isn't up yet".
//
// The distinction is what makes Transformer.Client.UserTransformer.retryRudderErrors.maxRetry enforceable.
// A cold start is retried by perWorkspacePyTEndlessRetries, which never gives up, so classifying a PyT application
// error as one would let that outer loop resume the retrying the transport was just told to stop.
//
// On the default retryRudderErrors settings (enabled, maxRetry -1, maxElapsedTime 0) the transport retries such
// responses forever and never returns them, so this only bites once they are bounded or disabled.
func TestIsColdStartError(t *testing.T) {
	testCases := []struct {
		name string
		err  error
		// status 0 means "no response at all", i.e. a transport-level failure.
		status  int
		headers map[string]string
		want    bool
	}{
		{
			// kube-proxy answering for a Service with no endpoints behind it.
			name:   "bare 503 is a cold start",
			status: http.StatusServiceUnavailable,
			want:   true,
		},
		{
			name:   "bare 502 is a cold start",
			status: http.StatusBadGateway,
			want:   true,
		},
		{
			// PyT itself reporting a transient downstream failure — e.g. the
			// geolocation service timing out. The pod is up and answering.
			name:    "503 with X-Rudder-Should-Retry is not a cold start",
			status:  http.StatusServiceUnavailable,
			headers: map[string]string{transformerclient.HeaderShouldRetry: "true"},
			want:    false,
		},
		{
			// The contract is 503-only: pyt normalises downstream failures (a downstream 502 included) into its
			// own 503 plus these headers, so a 502 is infrastructure no matter what headers ride along with it.
			// Honouring the header here too would put 502 in the one state that has no owner — not retried by the
			// transport, which is 503-only, and not retried as a cold start either.
			name:    "502 with X-Rudder-Should-Retry is still a cold start",
			status:  http.StatusBadGateway,
			headers: map[string]string{transformerclient.HeaderShouldRetry: "true"},
			want:    true,
		},
		{
			name:    "X-Rudder-Should-Retry is matched case-insensitively",
			status:  http.StatusServiceUnavailable,
			headers: map[string]string{transformerclient.HeaderShouldRetry: "True"},
			want:    false,
		},
		{
			// Only "true" suppresses the cold-start classification; any other
			// value is not the PyT retry contract.
			name:    "503 with a non-true X-Rudder-Should-Retry is still a cold start",
			status:  http.StatusServiceUnavailable,
			headers: map[string]string{transformerclient.HeaderShouldRetry: "false"},
			want:    true,
		},
		{
			name:   "200 is never a cold start",
			status: http.StatusOK,
			want:   false,
		},
		{
			// The header only means anything inside the 503/502 branch. Pinning this stops a refactor that
			// hoists the header check above the status check from silently reclassifying every 5xx.
			name:    "500 with X-Rudder-Should-Retry is not a cold start",
			status:  http.StatusInternalServerError,
			headers: map[string]string{transformerclient.HeaderShouldRetry: "true"},
			want:    false,
		},
		{
			name: "no error and no response is not a cold start",
			want: false,
		},
		{
			name: "connection refused is a cold start",
			err: &net.OpError{
				Op: "dial", Net: "tcp",
				Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED},
			},
			want: true,
		},
		{
			// Stale iptables / EndpointSlice after a pod replacement.
			name: "no route to host is a cold start",
			err: &net.OpError{
				Op: "dial", Net: "tcp",
				Err: &os.SyscallError{Syscall: "connect", Err: syscall.EHOSTUNREACH},
			},
			want: true,
		},
		{
			// The SYN went unanswered: the pod is coming up, or going down and its DNS record has not
			// been withdrawn yet. Indistinguishable from the dialer, and retryable either way.
			name: "dial timeout is a cold start",
			err:  &net.OpError{Op: "dial", Net: "tcp", Err: timeoutError{}},
			want: true,
		},
		{
			// Not a dial: a read timeout mid-response means the pod answered, so it is up.
			name: "non-dial timeout is not a cold start",
			err:  &net.OpError{Op: "read", Net: "tcp", Err: timeoutError{}},
			want: false,
		},
		{
			name: "DNS error is a cold start",
			err:  &net.DNSError{Err: "no such host", Name: "pyt-ws-1", IsNotFound: true},
			want: true,
		},
		{
			// err wins: the branch returns before resp is consulted, so a transport failure is classified
			// on its own merits even when a stale response is still in hand.
			name:    "err takes precedence over resp",
			err:     &net.DNSError{Err: "no such host", Name: "pyt-ws-1", IsNotFound: true},
			status:  http.StatusOK,
			headers: map[string]string{transformerclient.HeaderShouldRetry: "true"},
			want:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var resp *http.Response
			if tc.status != 0 {
				hdr := http.Header{}
				for k, v := range tc.headers {
					hdr.Set(k, v)
				}
				resp = &http.Response{StatusCode: tc.status, Header: hdr, Body: http.NoBody}
			}
			require.Equal(t, tc.want, isColdStartError(tc.err, resp))
		})
	}
}

// timeoutError is a net.Error that reports Timeout() == true, so a case can exercise the dial-timeout branch
// without waiting on a real unanswered SYN.
type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

// TestRetryReasonTag pins that a transformer-chosen header cannot become an unbounded stats tag.
//
// The value is picked by the peer, so anything that is not a short plain identifier has to collapse to a
// constant. One per-request string reaching the metrics backend is a new time series per request.
func TestRetryReasonTag(t *testing.T) {
	headerWith := func(reason string) http.Header {
		hdr := http.Header{}
		if reason != "" {
			hdr.Set(transformerclient.HeaderErrorReason, reason)
		}
		return hdr
	}

	for _, tc := range []struct{ name, reason, want string }{
		{"a known reason passes through", "geolocation_timeout", "geolocation_timeout"},
		{"hyphens and digits are fine", "config-backend-503", "config-backend-503"},
		{"absent header is reported as unknown", "", "unknown"},
		{"a per-request id is collapsed", "failed for message 0b41-9f2a at 12:04:11", "other"},
		{"an over-long value is collapsed", strings.Repeat("x", 65), "other"},
		{"a value at the length limit is kept", strings.Repeat("x", 64), strings.Repeat("x", 64)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, retryReasonTag(headerWith(tc.reason)))
		})
	}
}
