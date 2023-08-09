package gateway

import (
	"errors"
	"net/http"
	"time"

	gwstats "github.com/rudderlabs/rudder-server/gateway/internal/stats"
	"github.com/rudderlabs/rudder-server/gateway/response"
)

// RegularRequestHandler is an empty struct to capture non-import specific request handling functionality
type RegularRequestHandler struct {
	*Handle
}

// ProcessRequest throws a webRequest into the queue and waits for the response before returning
func (rrh *RegularRequestHandler) ProcessRequest(w *http.ResponseWriter, r *http.Request, reqType string, payload []byte, writeKey string) string {
	done := make(chan string, 1)
	start := time.Now()
	rrh.addToWebRequestQ(w, r, done, reqType, payload, writeKey)
	rrh.addToWebRequestQWaitTime.SendTiming(time.Since(start))
	defer rrh.processRequestTime.Since(start)
	errorMessage := <-done
	return errorMessage
}

func (rrh *RegularRequestHandler) AuthenticateRequest(r *http.Request, reqType string) (string, string, error) {
	writeKey, _, ok := r.BasicAuth()
	sourceID := rrh.getSourceIDForWriteKey(writeKey)
	if !ok || writeKey == "" {
		err := errors.New(response.NoWriteKeyInBasicAuth)
		stat := gwstats.SourceStat{
			Source:   "noWriteKey",
			SourceID: "noSourceID",
			WriteKey: "noWriteKey",
			ReqType:  reqType,
		}
		stat.RequestFailed("noWriteKeyInBasicAuth")
		stat.Report(rrh.stats)
		return "", "", err
	}
	if !rrh.isWriteKeyEnabled(writeKey) {
		err := errors.New(response.SourceDisabled)
		stat := gwstats.SourceStat{
			SourceID: sourceID,
			WriteKey: writeKey,
			ReqType:  reqType,
			Source:   rrh.getSourceTagFromWriteKey(writeKey),
		}
		stat.RequestFailed("sourceDisabled")
		stat.Report(rrh.stats)
		return "", "", err
	}
	return writeKey, sourceID, nil
}
