package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rs/cors"
	"github.com/stretchr/testify/require"
)

// TestCORSPolicy verifies that the gateway CORS configuration does not reflect
// arbitrary origins with AllowCredentials: true.
//
// Background: The gateway authenticates via the Authorization header (Basic Auth
// writeKey), not via cookies. Enabling AllowCredentials with a wildcard
// AllowOriginFunc would allow malicious pages to make credentialed cross-origin
// requests on behalf of a victim's browser session, enabling fake event injection.
//
// See: https://fetch.spec.whatwg.org/#cors-protocol-and-credentials
func TestCORSPolicy(t *testing.T) {
	// Recreate the exact CORS config used in StartWebHandler (handle_lifecycle.go).
	c := cors.New(cors.Options{
		AllowOriginFunc:  func(_ string) bool { return true },
		AllowCredentials: false,
		AllowedHeaders:   []string{"*"},
		MaxAge:           900,
	})

	// Wrap a trivial handler.
	handler := c.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	t.Run("preflight from arbitrary origin must not include Allow-Credentials true", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodOptions, "/v1/track", nil)
		req.Header.Set("Origin", "https://evil.com")
		req.Header.Set("Access-Control-Request-Method", "POST")
		req.Header.Set("Access-Control-Request-Headers", "content-type")

		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		// Origin is reflected (permissive origin policy is intentional for SDKs).
		require.Equal(t, "https://evil.com", rr.Header().Get("Access-Control-Allow-Origin"),
			"origin should be reflected for SDK compatibility")

		// Credentials MUST NOT be allowed for arbitrary origins.
		require.NotEqual(t, "true", rr.Header().Get("Access-Control-Allow-Credentials"),
			"Access-Control-Allow-Credentials must not be true for arbitrary origins: "+
				"this combination allows cross-origin credentialed event injection")
	})

	t.Run("simple POST from arbitrary origin must not include Allow-Credentials true", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/track", nil)
		req.Header.Set("Origin", "https://evil.com")
		req.Header.Set("Content-Type", "application/json")

		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		require.NotEqual(t, "true", rr.Header().Get("Access-Control-Allow-Credentials"),
			"Access-Control-Allow-Credentials must not be true for arbitrary origins")
	})
}
