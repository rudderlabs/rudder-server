package router

import (
	"errors"
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

// TestRedactURLHandlesSchemelessURLs is the case pattern matching cannot reach, raised
// in review: a destination configured without a scheme (or with a typo'd one) is not a
// "https://..." match, so only a structural pass sees its query string. http.NewRequest
// accepts such a URL and the failure surfaces later from Do, which is how the
// credential used to reach the response body.
func TestRedactURLHandlesSchemelessURLs(t *testing.T) {
	for _, raw := range []string{
		"api.example.com/v1/events?api_key=s3cr3t",
		"htp://api.example.com/v1/events?api_key=s3cr3t", // typo'd scheme
		"//api.example.com/v1/events?api_key=s3cr3t",     // protocol-relative
	} {
		t.Run(raw, func(t *testing.T) {
			got := redactURL(raw)
			require.NotContains(t, got, "s3cr3t", "the query string must go, scheme or not")
			require.Contains(t, got, "api.example.com/v1/events", "the endpoint must survive")
		})
	}
}

// TestRedactURLIsIdempotent matters because redactURLCredentials runs over text whose
// URLs the caller has already redacted structurally; the marker must not accumulate.
func TestRedactURLIsIdempotent(t *testing.T) {
	once := redactURL("https://api.example.com/v1/events?api_key=s3cr3t")
	require.Equal(t, "https://api.example.com/v1/events?[redacted]", once)
	require.Equal(t, once, redactURL(once), "redacting twice must not change the result")
	require.Equal(t, once, redactURLCredentials(once), "nor must the pattern backstop")
}

func TestRedactErrorText(t *testing.T) {
	t.Run("redacts the URL net/http repeats inside url.Error", func(t *testing.T) {
		err := &url.Error{
			Op:  "Post",
			URL: "https://api.example.com/v1/events?api_key=s3cr3t",
			Err: errors.New("dial tcp: i/o timeout"),
		}
		require.Contains(t, err.Error(), "s3cr3t", "the fixture must reproduce the leak")

		got := redactErrorText(err)
		require.NotContains(t, got, "s3cr3t")
		require.Contains(t, got, "https://api.example.com/v1/events?[redacted]")
		require.Contains(t, got, "dial tcp: i/o timeout", "the cause must survive")
		require.Contains(t, got, "Post", "so must the operation")
	})

	t.Run("reaches a url.Error wrapped deeper, keeping the outer text", func(t *testing.T) {
		inner := &url.Error{
			Op:  "Post",
			URL: "api.example.com/collect?token=s3cr3t", // scheme-less, so no pattern match
			Err: errors.New("unsupported protocol scheme"),
		}
		got := redactErrorText(fmt.Errorf("sending batch: %w", inner))

		require.NotContains(t, got, "s3cr3t")
		require.Contains(t, got, "sending batch:", "the wrapping must not be lost")
		require.Contains(t, got, "unsupported protocol scheme")
	})

	t.Run("leaves an error with no URL alone", func(t *testing.T) {
		require.Equal(t, "some failure", redactErrorText(errors.New("some failure")))
		require.Equal(t, "", redactErrorText(nil))
	})
}
