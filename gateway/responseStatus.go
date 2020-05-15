package gateway

import (
	"net/http"
)

const (
	//Ok - ok
	Ok = "OK"
	//RequestBodyNil - Request body is nil
	RequestBodyNil = "Request body is nil"
	//TooManyRequests - too many requests
	TooManyRequests = "Max Events Limit reached. Dropping Events."
	//NoWriteKeyInBasicAuth - Failed to read writeKey from header
	NoWriteKeyInBasicAuth = "Failed to read writeKey from header"
	//RequestBodyReadFailed - Failed to read body from request
	RequestBodyReadFailed = "Failed to read body from request"
	//SourceTrasnformerResponseReadFailed - Failed to read body from source transformer response
	SourceTrasnformerResponseReadFailed = "Failed to read body from source transformer response"
	//SourceTransformerFailed - Internal server error in source transformer
	SourceTransformerFailed = "Internal server error in source transformer"
	//RequestBodyTooLarge - Request size exceeds max limit
	RequestBodyTooLarge = "Request size exceeds max limit"
	//InvalidWriteKey - Invalid Write Key
	InvalidWriteKey = "Invalid Write Key"
	//InvalidJSON - Invalid JSON
	InvalidJSON = "Invalid JSON"
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
	statusMap = make(map[string]ResponseStatus)
	statusMap[Ok] = ResponseStatus{message: Ok, code: http.StatusOK}
	statusMap[RequestBodyNil] = ResponseStatus{message: RequestBodyNil, code: http.StatusBadRequest}
	statusMap[TooManyRequests] = ResponseStatus{message: TooManyRequests, code: http.StatusTooManyRequests}
	statusMap[NoWriteKeyInBasicAuth] = ResponseStatus{message: NoWriteKeyInBasicAuth, code: http.StatusUnauthorized}
	statusMap[RequestBodyReadFailed] = ResponseStatus{message: RequestBodyReadFailed, code: http.StatusBadRequest}
	statusMap[SourceTransformationFailed] = ResponseStatus{message: SourceTransformationFailed, code: http.StatusBadRequest}
	statusMap[RequestBodyTooLarge] = ResponseStatus{message: RequestBodyTooLarge, code: http.StatusRequestEntityTooLarge}
	statusMap[InvalidWriteKey] = ResponseStatus{message: InvalidWriteKey, code: http.StatusUnauthorized}
	statusMap[InvalidJSON] = ResponseStatus{message: InvalidJSON, code: http.StatusBadRequest}
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
