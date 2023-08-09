package gateway

import (
	"errors"
	"net/http"
	"strings"

	gwstats "github.com/rudderlabs/rudder-server/gateway/internal/stats"
	"github.com/rudderlabs/rudder-server/gateway/response"
)

// ImportRequestHandler is an empty struct to capture import specific request handling functionality
type ImportRequestHandler struct {
	*Handle
}

// ProcessRequest on ImportRequestHandler splits payload by user and throws them into the webrequestQ and waits for all their responses before returning
func (irh *ImportRequestHandler) ProcessRequest(w *http.ResponseWriter, r *http.Request, _ string, payload []byte, writeKey string) string {
	usersPayload, payloadError := irh.getUsersPayload(payload)
	if payloadError != nil {
		return payloadError.Error()
	}
	count := len(usersPayload)
	done := make(chan string, count)
	for key := range usersPayload {
		irh.addToWebRequestQ(w, r, done, "batch", usersPayload[key], writeKey)
	}

	var interimMsgs []string
	for index := 0; index < count; index++ {
		interimErrorMessage := <-done
		interimMsgs = append(interimMsgs, interimErrorMessage)
	}
	return strings.Join(interimMsgs, "")
}

func (irh *ImportRequestHandler) AuthenticateRequest(r *http.Request, reqType string) (string, string, error) {
	writeKey, _, ok := r.BasicAuth()
	sourceID := irh.getSourceIDForWriteKey(writeKey)
	if !ok || writeKey == "" {
		err := errors.New(response.NoWriteKeyInBasicAuth)
		stat := gwstats.SourceStat{
			Source:   "noWriteKey",
			SourceID: "noSourceID",
			WriteKey: "noWriteKey",
			ReqType:  reqType,
		}
		stat.RequestFailed("noWriteKeyInBasicAuth")
		stat.Report(irh.stats)
		return "", "", err
	}
	if !irh.isWriteKeyEnabled(writeKey) {
		err := errors.New(response.SourceDisabled)
		stat := gwstats.SourceStat{
			SourceID: sourceID,
			WriteKey: writeKey,
			ReqType:  reqType,
			Source:   irh.getSourceTagFromWriteKey(writeKey),
		}
		stat.RequestFailed("sourceDisabled")
		stat.Report(irh.stats)
		return "", "", err
	}
	return writeKey, sourceID, nil
}
