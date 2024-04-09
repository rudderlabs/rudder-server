package gateway

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"

	"github.com/tidwall/sjson"

	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"

	gwstats "github.com/rudderlabs/rudder-server/gateway/internal/stats"

	"github.com/rudderlabs/rudder-server/gateway/response"
)

// pixelPageHandler can handle pixel page requests where everything is passed as query params.
// it also writes a pixel response to the client regardless of the actual result of the request
func (gw *Handle) pixelPageHandler() http.HandlerFunc {
	return gw.pixelInterceptor("page", gw.webPageHandler())
}

// pixelTrackHandler can handle pixel track requests where everything is passed as query params.
// it also writes a pixel response to the client regardless of the actual result of the request
func (gw *Handle) pixelTrackHandler() http.HandlerFunc {
	return gw.pixelInterceptor("track", gw.webTrackHandler())
}

// pixelInterceptor reads information from the query parameters to fill in the request's body and authorization header before passing it to the next handler
// It also writes a pixel response to the client regardless of the next handler's response
func (gw *Handle) pixelInterceptor(reqType string, next http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			w.Header().Set("Content-Type", "image/gif")
			_, err := w.Write([]byte(response.GetPixelResponse()))
			if err != nil {
				gw.logger.Warnf("Error while sending pixel response: %v", err)
				return
			}
		}() // write pixel response even if there is an error

		queryParams := r.URL.Query()
		if queryParams["writeKey"] != nil {
			writeKey := queryParams["writeKey"]
			// make a new request
			pr, err := http.NewRequest(http.MethodPost, "", http.NoBody)
			if err != nil {
				return
			}
			// set basic auth header
			pr.SetBasicAuth(writeKey[0], "")
			delete(queryParams, "writeKey")

			// set X-Forwarded-For header
			pr.Header.Add("X-Forwarded-For", r.Header.Get("X-Forwarded-For"))

			// convert the pixel request(r) to a web request(req)
			if err := gw.preparePixelPayload(pr, queryParams, reqType); err == nil {
				pw := newPixelWriter() // create a new writer since the pixel is going to be written to the client regardless of the next handler's response
				next(pw, pr)
				if pw.status != http.StatusOK {
					gw.logger.Infow("Error while handling request",
						"ip", kithttputil.GetRequestIP(r),
						"path", r.URL.Path,
						"status", pw.status,
						"body", string(pw.body))
				}
			}
		} else {
			stat := gwstats.SourceStat{
				Source:   "NoWriteKeyInQueryParams",
				SourceID: "NoWriteKeyInQueryParams",
				WriteKey: "NoWriteKeyInQueryParams",
				ReqType:  reqType,
			}
			stat.RequestFailed("NoWriteKeyInQueryParams")
			stat.Report(gw.stats)
			gw.logger.Infow("Error while handling request",
				"ip", kithttputil.GetRequestIP(r),
				"path", r.URL.Path,
				"body", response.NoWriteKeyInQueryParams)
		}
	})
}

// preparePixelPayload reads a pixel GET request and maps it to a proper payload in the request's body
func (gw *Handle) preparePixelPayload(r *http.Request, qp url.Values, reqType string) error {
	// add default fields to body
	body := []byte(`{"channel": "web","integrations": {"All": true}}`)
	currentTime := gw.now()
	body, _ = sjson.SetBytes(body, "originalTimestamp", currentTime)
	body, _ = sjson.SetBytes(body, "sentAt", currentTime)

	// make sure anonymousId is in correct format
	if anonymousID, ok := qp["anonymousId"]; ok {
		qp["anonymousId"][0] = regexp.MustCompile(`^"(.*)"$`).ReplaceAllString(anonymousID[0], `$1`)
	}

	// add queryParams to body
	for key := range qp {
		body, _ = sjson.SetBytes(body, key, qp[key][0])
	}

	// add request specific fields to body
	body, _ = sjson.SetBytes(body, "type", reqType)
	switch reqType {
	case "page":
		if pageName, ok := qp["name"]; ok {
			if pageName[0] == "" {
				pageName[0] = "Unknown Page"
			}
			body, _ = sjson.SetBytes(body, "name", pageName[0])
		}
	case "track":
		if evName, ok := qp["event"]; ok {
			if evName[0] == "" {
				return errors.New("track: Mandatory field 'event' missing")
			}
			body, _ = sjson.SetBytes(body, "event", evName[0])
		}
	}
	// add body to request
	r.Body = io.NopCloser(bytes.NewReader(body))
	return nil
}

// newPixelWriter returns a new, properly initialized pixel writer
// it is used to capture the status code and body of the response without writing it to the client
func newPixelWriter() *pixelHttpWriter {
	return &pixelHttpWriter{
		status: http.StatusOK,
	}
}

// pixelHttpWriter captures the status code and body of the response
type pixelHttpWriter struct {
	status int
	body   []byte
}

func (w *pixelHttpWriter) Header() http.Header {
	return http.Header{}
}

func (w *pixelHttpWriter) WriteHeader(status int) {
	w.status = status
}

func (w *pixelHttpWriter) Write(b []byte) (int, error) {
	w.body = append(w.body, b...)
	return len(b), nil
}
