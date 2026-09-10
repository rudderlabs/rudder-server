package router

import (
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedactURLCredentials(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "query string carrying an api key",
			input:    `504 Unable to make "POST" request for URL : "https://api.example.com/v1/events?api_key=s3cr3t"`,
			expected: `504 Unable to make "POST" request for URL : "https://api.example.com/v1/events?[redacted]"`,
		},
		{
			name:     "userinfo credentials",
			input:    `posting to https://user:hunter2@api.example.com/collect failed`,
			expected: `posting to https://api.example.com/collect?[redacted] failed`,
		},
		{
			name:     "fragment, where implicit-flow tokens land",
			input:    `redirect https://api.example.com/cb#access_token=abc`,
			expected: `redirect https://api.example.com/cb?[redacted]`,
		},
		{
			name:     "no secrets means no marker and no rewriting",
			input:    `400 Unable to construct "GET" request for URL : "https://api.example.com/v1/events"`,
			expected: `400 Unable to construct "GET" request for URL : "https://api.example.com/v1/events"`,
		},
		{
			name:     "every url in the message is redacted, not just the first",
			input:    `https://a.example.com/x?k=1 then https://b.example.com/y?k=2`,
			expected: `https://a.example.com/x?[redacted] then https://b.example.com/y?[redacted]`,
		},
		{
			name:     "text with no url is untouched",
			input:    `500 Invalid Router Payload: body format must be a map found format XML`,
			expected: `500 Invalid Router Payload: body format must be a map found format XML`,
		},
		{
			name:     "plain http is redacted too",
			input:    `http://internal.example.com/ingest?token=abc`,
			expected: `http://internal.example.com/ingest?[redacted]`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, redactURLCredentials(tc.input))
		})
	}
}

// TestRedactURLCredentialsCoversTheWrappedError is the case a fix applied to
// postInfo.URL alone would miss. net/http returns *url.Error from Do, and its Error()
// repeats the request URL — the one carrying the query parameters this package appends
// after postInfo.URL is read, so it is a different string from the interpolated copy.
// Both have to go.
func TestRedactURLCredentialsCoversTheWrappedError(t *testing.T) {
	const (
		configured = "https://api.example.com/v1/events?api_key=configured-secret"
		requested  = "https://api.example.com/v1/events?api_key=configured-secret&ts=17"
	)
	doErr := &url.Error{Op: "Post", URL: requested, Err: fmt.Errorf("dial tcp: i/o timeout")}
	require.Contains(t, doErr.Error(), "configured-secret",
		"the fixture must actually reproduce the leak, or this test proves nothing")

	message := fmt.Sprintf(`504 Unable to make %q request for URL : %q. Error: %v`,
		"POST", configured, doErr)
	require.Equal(t, 2, strings.Count(message, "configured-secret"),
		"the unredacted message must carry the secret twice: interpolated and inside the error")

	redacted := redactURLCredentials(message)

	require.NotContains(t, redacted, "configured-secret", "no copy of the credential may survive")
	require.NotContains(t, redacted, "ts=17", "appended query parameters go with it")
	require.Contains(t, redacted, "https://api.example.com/v1/events?[redacted]",
		"the endpoint must survive so an operator can tell which destination failed")
	require.Contains(t, redacted, "dial tcp: i/o timeout", "the cause must survive")
}

// TestRedactURLFailsClosed pins the choice for input this function cannot parse: it is
// withheld rather than passed through, because these strings are written to a
// customer's warehouse and an unparseable value cannot be shown to be credential-free.
func TestRedactURLFailsClosed(t *testing.T) {
	// A control character is rejected by url.Parse.
	require.Equal(t, "[redacted url]", redactURL("https://example.com/\x7f?k=v"))
}
