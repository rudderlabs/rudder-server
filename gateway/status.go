package gateway

import (
	"net/http"

	"github.com/rudderlabs/rudder-server/config"
)

const (
	//Ok - ok
	Ok = "Ok"
	//RequestBodyNil - Request body is nil
	RequestBodyNil = "RequestBodyNil"
	//TooManyRequests - too many requests
	TooManyRequests = "TooManyRequests"
	//NoWriteKeyInBasicAuth - Failed to read writeKey from header
	NoWriteKeyInBasicAuth = "NoWriteKeyInBasicAuth"
	//RequestBodyReadFailed - Failed to read body from request
	RequestBodyReadFailed = "RequestBodyReadFailed"
	//RequestBodyTooLarge - Request size exceeds max limit
	RequestBodyTooLarge = "RequestBodyTooLarge"
	//InvalidWriteKey - Invalid Write Key
	InvalidWriteKey = "InvalidWriteKey"
)

var (
	statusMap map[string]ResponseStatus
)

//ResponseStatus holds gateway response status message and code
type ResponseStatus struct {
	message string
	code    int
}

func loadStatusMap() {
	//Reponse messages sent to client
	respOk := config.GetString("Gateway.respOk", "OK")
	respRequestBodyNil := config.GetString("Gateway.respRequestBodyNil", "Request body is nil")
	respTooManyRequests := config.GetString("Gateway.respTooManyRequests", "Max Events Limit reached. Dropping Events.")
	respNoWriteKeyInBasicAuth := config.GetString("Gateway.respNoWriteKeyInBasicAuth", "Failed to read writeKey from header")
	respRequestBodyReadFailed := config.GetString("Gateway.respRequestBodyReadFailed", "Failed to read body from request")
	respRequestBodyTooLarge := config.GetString("Gateway.respRequestBodyTooLarge", "Request size exceeds max limit")
	respInvalidWriteKey := config.GetString("Gateway.respInvalidWriteKey", "Invalid Write Key")

	statusMap = make(map[string]ResponseStatus)
	statusMap[Ok] = ResponseStatus{message: respOk, code: http.StatusOK}
	statusMap[RequestBodyNil] = ResponseStatus{message: respRequestBodyNil, code: http.StatusBadRequest}
	statusMap[TooManyRequests] = ResponseStatus{message: respTooManyRequests, code: http.StatusTooManyRequests}
	statusMap[NoWriteKeyInBasicAuth] = ResponseStatus{message: respNoWriteKeyInBasicAuth, code: http.StatusBadRequest}
	statusMap[RequestBodyReadFailed] = ResponseStatus{message: respRequestBodyReadFailed, code: http.StatusBadRequest}
	statusMap[RequestBodyTooLarge] = ResponseStatus{message: respRequestBodyTooLarge, code: http.StatusRequestEntityTooLarge}
	statusMap[InvalidWriteKey] = ResponseStatus{message: respInvalidWriteKey, code: http.StatusUnauthorized}
}

func getStatus(key string) string {
	if status, ok := statusMap[key]; ok {
		return status.message
	}

	return ""
}

func getStatusCode(key string) int {
	if status, ok := statusMap[key]; ok {
		return status.code
	}

	return 200
}
