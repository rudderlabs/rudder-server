package middleware

import (
	"compress/gzip"
	"io"
	"net/http"
)

// UncompressMiddleware uncompresses gzipped HTTP requests carrying a 'Content-Encoding: gzip' header.
var UncompressMiddleware = func(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Encoding") == "gzip" {
			r.Body = &gzipReader{body: r.Body}
		}
		h.ServeHTTP(w, r)
	})
}

// gzipReader wraps a body so it can lazily
// call gzip.NewReader on the first call to Read
type gzipReader struct {
	body io.ReadCloser // underlying request body
	zr   *gzip.Reader  // lazily-initialized gzip reader
	zerr error         // any error from gzip.NewReader; sticky
}

func (gz *gzipReader) Read(p []byte) (n int, err error) {
	if gz.zr == nil {
		if gz.zerr == nil {
			gz.zr, gz.zerr = gzip.NewReader(gz.body)
		}
		if gz.zerr != nil {
			return 0, gz.zerr
		}
	}
	return gz.zr.Read(p)
}

func (gz *gzipReader) Close() error {
	if gz.zr != nil {
		_ = gz.zr.Close()
	}
	return gz.body.Close()
}
