package gateway

import (
	"net/http"
	"time"
)

// RegularRequestHandler is an empty struct to capture non-import specific request handling functionality
type RegularRequestHandler struct{}

// ProcessRequest throws a webRequest into the queue and waits for the response before returning
func (*RegularRequestHandler) ProcessRequest(gateway *Handle, w *http.ResponseWriter, r *http.Request, reqType string, payload []byte, writeKey string) string {
	done := make(chan string, 1)
	start := time.Now()
	gateway.addToWebRequestQ(w, r, done, reqType, payload, writeKey)
	gateway.addToWebRequestQWaitTime.SendTiming(time.Since(start))
	defer gateway.processRequestTime.Since(start)
	errorMessage := <-done
	return errorMessage
}
