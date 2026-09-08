package bqstream

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/http2"
	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"

	"github.com/rudderlabs/rudder-go-kit/config"
)

// newHTTPClient returns the HTTP client used by the BigQuery client. It always
// applies our tuned base transport (see baseTransport) and, when compress is
// true, gzip-compresses request bodies. The standard Google authentication
// middleware is preserved by building the transport via htransport.NewTransport.
func newHTTPClient(ctx context.Context, compress bool, opts ...option.ClientOption) (*http.Client, error) {
	base := baseTransport()
	var rt http.RoundTripper = base
	if compress {
		rt = &gzipTransport{base: base}
	}
	authedTransport, err := htransport.NewTransport(ctx, rt, opts...)
	if err != nil {
		return nil, err
	}
	return &http.Client{Transport: authedTransport}, nil
}

// baseTransport mirrors the defaults the Google API HTTP client applies in
// google.golang.org/api/transport/http (defaultBaseTransport): a clone of
// http.DefaultTransport with a larger idle-connection pool and HTTP/2 idle
// connection health checks. Supplying our own client via option.WithHTTPClient
// bypasses that setup, so we replicate it here to avoid regressing throughput
// and connection resilience. (mTLS/S2A dial options are not reproduced, as they
// depend on the library's internal settings.)
//
// The idle-connection pool is sized to the configured number of router workers,
// consistent with the other stream managers and the HTTP router (services that
// build their own transport all tune these to noOfWorkers).
func baseTransport() *http.Transport {
	// Clone http.DefaultTransport when possible; if it has been replaced by
	// something that is not an *http.Transport, fall back to an equivalent one,
	// mirroring defaultBaseTransport/fallbackBaseTransport in the Google library.
	trans := clonedDefaultTransport()
	if trans == nil {
		trans = fallbackBaseTransport()
	}
	trans.MaxIdleConns = config.GetIntVar(64, 1, "Router.BQSTREAM.httpMaxIdleConns", "Router.BQSTREAM.noOfWorkers", "Router.noOfWorkers")
	trans.MaxIdleConnsPerHost = config.GetIntVar(64, 1, "Router.BQSTREAM.httpMaxIdleConnsPerHost", "Router.BQSTREAM.noOfWorkers", "Router.noOfWorkers")
	if http2Trans, err := http2.ConfigureTransports(trans); err == nil {
		http2Trans.ReadIdleTimeout = 31 * time.Second
	}
	return trans
}

// clonedDefaultTransport returns a clone of http.DefaultTransport, or nil if it
// is not an *http.Transport (e.g. it has been overridden in the process).
func clonedDefaultTransport() *http.Transport {
	t, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil
	}
	return t.Clone()
}

// fallbackBaseTransport mirrors the Google library's fallbackBaseTransport, used
// when http.DefaultTransport is not an *http.Transport we can clone.
func fallbackBaseTransport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}

// gzipTransport is an http.RoundTripper that gzip-compresses the request body and
// sets the Content-Encoding header before delegating to the underlying transport.
type gzipTransport struct {
	base http.RoundTripper
}

func (t *gzipTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Nothing to compress, or the body is already encoded: pass through untouched.
	if req.Body == nil || req.Body == http.NoBody || req.Header.Get("Content-Encoding") != "" {
		return t.base.RoundTrip(req)
	}

	body, err := io.ReadAll(req.Body)
	closeErr := req.Body.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err = gz.Write(body); err != nil {
		return nil, err
	}
	if err = gz.Close(); err != nil {
		return nil, err
	}
	compressed := buf.Bytes()

	// Per the RoundTripper contract we must not modify the caller's request.
	newReq := req.Clone(req.Context())
	newReq.Body = io.NopCloser(bytes.NewReader(compressed))
	newReq.ContentLength = int64(len(compressed))
	newReq.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(compressed)), nil
	}
	newReq.Header.Set("Content-Encoding", "gzip")
	return t.base.RoundTrip(newReq)
}
