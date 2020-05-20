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
	//RequestBodyTooLarge - Request size exceeds max limit
	RequestBodyTooLarge = "Request size exceeds max limit"
	//InvalidWriteKey - Invalid Write Key
	InvalidWriteKey = "Invalid Write Key"
	//InvalidJSON - Invalid JSON
	InvalidJSON = "Invalid JSON"
	//InvalidWebhookSource - Source does not accept webhook events
	InvalidWebhookSource = "Source does not accept webhook events"
	//NoWriteKeyInQueryParams - Failed to read writeKey from query params
	NoWriteKeyInQueryParams = "Failed to read writeKey from query params"
	//SourceTrasnformerResponseErrorReadFailed - Failed to read error from source transformer response
	SourceTrasnformerResponseErrorReadFailed = "Failed to read error from source transformer response"
	//SourceTransformerFailed - Internal server error in source transformer
	SourceTransformerFailed = "Internal server error in source transformer"
	//SourceTransformerFailedToReadOutput - Output not found in source transformer response
	SourceTransformerFailedToReadOutput = "Output not found in source transformer response"
	//SourceTransformerInvalidResponseFormat - Invalid format of source transformer response
	SourceTransformerInvalidResponseFormat = "Invalid format of source transformer response"
	//SourceTransformerInvalidOutputFormatInResponse - Invalid output format in source transformer response
	SourceTransformerInvalidOutputFormatInResponse = "Invalid output format in source transformer response"
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
	statusMap[RequestBodyTooLarge] = ResponseStatus{message: RequestBodyTooLarge, code: http.StatusRequestEntityTooLarge}
	statusMap[InvalidWriteKey] = ResponseStatus{message: InvalidWriteKey, code: http.StatusUnauthorized}
	statusMap[InvalidJSON] = ResponseStatus{message: InvalidJSON, code: http.StatusBadRequest}
	// webhook specific status
	statusMap[NoWriteKeyInQueryParams] = ResponseStatus{message: NoWriteKeyInQueryParams, code: http.StatusUnauthorized}
	statusMap[InvalidWebhookSource] = ResponseStatus{message: InvalidWebhookSource, code: http.StatusBadRequest}
	statusMap[SourceTransformerFailed] = ResponseStatus{message: SourceTransformerFailed, code: http.StatusBadRequest}
	statusMap[SourceTrasnformerResponseErrorReadFailed] = ResponseStatus{message: SourceTrasnformerResponseErrorReadFailed, code: http.StatusBadRequest}
	statusMap[SourceTransformerFailedToReadOutput] = ResponseStatus{message: SourceTransformerFailedToReadOutput, code: http.StatusBadRequest}
	statusMap[SourceTransformerInvalidResponseFormat] = ResponseStatus{message: SourceTransformerInvalidResponseFormat, code: http.StatusBadRequest}
	statusMap[SourceTransformerInvalidOutputFormatInResponse] = ResponseStatus{message: SourceTransformerInvalidOutputFormatInResponse, code: http.StatusBadRequest}
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
