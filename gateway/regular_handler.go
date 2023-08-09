package gateway

import (
	"net/http"
	"time"

	gwtypes "github.com/rudderlabs/rudder-server/gateway/internal/types"
)

// RegularRequestHandler is an empty struct to capture non-import specific request handling functionality
type RegularRequestHandler struct {
	*Handle
}

// ProcessRequest throws a webRequest into the queue and waits for the response before returning
func (rrh *RegularRequestHandler) ProcessRequest(w *http.ResponseWriter, r *http.Request, reqType string, payload []byte, arctx *gwtypes.AuthRequestContext) string {
	done := make(chan string, 1)
	start := time.Now()
	rrh.addToWebRequestQ(w, r, done, reqType, payload, arctx)
	rrh.addToWebRequestQWaitTime.SendTiming(time.Since(start))
	defer rrh.processRequestTime.Since(start)
	errorMessage := <-done
	return errorMessage
}
