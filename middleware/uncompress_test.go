package middleware_test

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/middleware"
)

func TestUncompress(t *testing.T) {
	json := `{"key": "value"}`

	handler := middleware.UncompressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, r.Body.Close())
		fmt.Println(string(b))
		require.NoError(t, err)
		_, err = w.Write(b)
		require.NoError(t, err)
	}))

	getGzipBody := func() *bytes.Buffer {
		var gzippedJson bytes.Buffer
		gz := gzip.NewWriter(&gzippedJson)
		_, err := gz.Write([]byte(json))
		require.NoError(t, err)
		require.NoError(t, gz.Close())
		return &gzippedJson
	}

	t.Run("sending a gzipped body with a Content-Encoding header", func(t *testing.T) {
		req, err := http.NewRequest("GET", "/test", getGzipBody())
		require.NoError(t, err)
		req.Header.Set("Content-Encoding", "gzip")
		res := httptest.NewRecorder()

		handler.ServeHTTP(res, req)

		require.Equal(t, http.StatusOK, res.Code)
		require.Equal(t, json, res.Body.String(), "handler should receive the uncompressed body")
	})

	t.Run("sending a non-gzipped body without a Content-Encoding header", func(t *testing.T) {
		req, err := http.NewRequest("GET", "/test", strings.NewReader(json))
		require.NoError(t, err)
		res := httptest.NewRecorder()

		handler.ServeHTTP(res, req)

		require.Equal(t, http.StatusOK, res.Code)
		require.Equal(t, json, res.Body.String(), "handler should receive the non-compressed body")
	})

	t.Run("sending a gzipped body but without a Content-Encoding header", func(t *testing.T) {
		req, err := http.NewRequest("GET", "/test", getGzipBody())
		require.NoError(t, err)
		res := httptest.NewRecorder()

		handler.ServeHTTP(res, req)

		require.Equal(t, http.StatusOK, res.Code)
		require.Equal(t, getGzipBody().Bytes(), res.Body.Bytes(), "handler should receive the compressed body")
	})

	t.Run("sending a non-gzipped body but with a Content-Encoding header", func(t *testing.T) {
		req, err := http.NewRequest("GET", "/test", strings.NewReader(json))
		require.NoError(t, err)
		res := httptest.NewRecorder()

		handler.ServeHTTP(res, req)

		require.Equal(t, http.StatusOK, res.Code)
		require.Equal(t, json, res.Body.String(), "handler should receive the non-compressed body")
	})
}
