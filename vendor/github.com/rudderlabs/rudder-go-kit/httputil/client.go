package httputil

import (
	"io"
	"net/http"
)

// CloseResponse closes the response's body. But reads at least some of the body so if it's
// small the underlying TCP connection will be re-used. No need to check for errors: if it
// fails, the Transport won't reuse it anyway.
func CloseResponse(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		const maxBodySlurpSize = 2 << 10 // 2KB
		_, _ = io.CopyN(io.Discard, resp.Body, maxBodySlurpSize)
		resp.Body.Close()
	}
}
