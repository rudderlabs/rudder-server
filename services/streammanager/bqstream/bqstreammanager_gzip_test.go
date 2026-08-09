package bqstream

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGzipTransport covers the gzipTransport RoundTripper. It is self-contained
// (no BigQuery credentials required) so it always runs in CI.
func TestGzipTransport(t *testing.T) {
	type capture struct {
		contentEncoding string
		body            []byte
	}

	// roundTrip sends the request built by newReq through a gzipTransport-backed
	// client to a throwaway server, returning what the server actually received.
	roundTrip := func(t *testing.T, newReq func(url string) *http.Request) capture {
		t.Helper()
		var got capture
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got.contentEncoding = r.Header.Get("Content-Encoding")
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			got.body = body
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		client := &http.Client{Transport: &gzipTransport{base: http.DefaultTransport}}
		resp, err := client.Do(newReq(srv.URL))
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode)
		return got
	}

	t.Run("compresses the request body", func(t *testing.T) {
		payload := `{"properties":{"id":"25","name":"rudder"}}`
		got := roundTrip(t, func(url string) *http.Request {
			req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(payload))
			require.NoError(t, err)
			return req
		})

		require.Equal(t, "gzip", got.contentEncoding)
		gr, err := gzip.NewReader(bytes.NewReader(got.body))
		require.NoError(t, err)
		defer func() { _ = gr.Close() }()
		decompressed, err := io.ReadAll(gr)
		require.NoError(t, err)
		require.Equal(t, payload, string(decompressed))
	})

	t.Run("passes through bodiless requests", func(t *testing.T) {
		got := roundTrip(t, func(url string) *http.Request {
			req, err := http.NewRequest(http.MethodGet, url, nil)
			require.NoError(t, err)
			return req
		})

		require.Empty(t, got.contentEncoding)
		require.Empty(t, got.body)
	})

	t.Run("passes through already-encoded requests", func(t *testing.T) {
		payload := "already-encoded-body"
		got := roundTrip(t, func(url string) *http.Request {
			req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(payload))
			require.NoError(t, err)
			req.Header.Set("Content-Encoding", "custom")
			return req
		})

		// The body must be forwarded untouched (not re-compressed).
		require.Equal(t, "custom", got.contentEncoding)
		require.Equal(t, payload, string(got.body))
	})
}
