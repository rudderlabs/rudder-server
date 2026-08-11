package user_transformer

import (
	"net"
	"net/http"
	"os"
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
			name: "connection refused is a cold start",
			err: &net.OpError{
				Op: "dial", Net: "tcp",
				Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED},
			},
			want: true,
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
