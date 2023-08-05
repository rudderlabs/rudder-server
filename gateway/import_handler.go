package gateway

import (
	"net/http"
	"strings"
)

// ImportRequestHandler is an empty struct to capture import specific request handling functionality
type ImportRequestHandler struct{}

// ProcessRequest on ImportRequestHandler splits payload by user and throws them into the webrequestQ and waits for all their responses before returning
func (*ImportRequestHandler) ProcessRequest(gateway *Handle, w *http.ResponseWriter, r *http.Request, _ string, payload []byte, writeKey string) string {
	usersPayload, payloadError := gateway.getUsersPayload(payload)
	if payloadError != nil {
		return payloadError.Error()
	}
	count := len(usersPayload)
	done := make(chan string, count)
	for key := range usersPayload {
		gateway.addToWebRequestQ(w, r, done, "batch", usersPayload[key], writeKey)
	}

	var interimMsgs []string
	for index := 0; index < count; index++ {
		interimErrorMessage := <-done
		interimMsgs = append(interimMsgs, interimErrorMessage)
	}
	return strings.Join(interimMsgs, "")
}
