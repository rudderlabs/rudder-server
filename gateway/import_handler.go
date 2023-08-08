package gateway

import (
	"net/http"
	"strings"
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
