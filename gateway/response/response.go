package response

import (
	"fmt"
	"net/http"
)

const (
	//Ok - ok
	Ok = "OK"
	//RequestBodyNil - Request body is nil
	RequestBodyNil = "Request body is nil"
	//InvalidRequestMethod - Request Method is invalid
	InvalidRequestMethod = "Invalid HTTP Request Method"
	//TooManyRequests - too many requests
	TooManyRequests = "Max Events Limit reached. Dropping Events."
	//NoWriteKeyInBasicAuth - Failed to read writeKey from header
	NoWriteKeyInBasicAuth = "Failed to read writeKey from header"
	//NoWriteKeyInQueryParams - Failed to read writeKey from Query Params
	NoWriteKeyInQueryParams = "Failed to read writeKey from Query Params"
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
	//SourceTransformerResponseErrorReadFailed - Failed to read error from source transformer response
	SourceTransformerResponseErrorReadFailed = "Failed to read error from source transformer response"
	//SourceTransformerFailed - Internal server error in source transformer
	SourceTransformerFailed = "Internal server error in source transformer"
	//SourceTransformerFailedToReadOutput - Output not found in source transformer response
	SourceTransformerFailedToReadOutput = "Output not found in source transformer response"
	//SourceTransformerInvalidResponseFormat - Invalid format of source transformer response
	SourceTransformerInvalidResponseFormat = "Invalid format of source transformer response"
	//SourceTransformerInvalidOutputFormatInResponse - Invalid output format in source transformer response
	SourceTransformerInvalidOutputFormatInResponse = "Invalid output format in source transformer response"
	//SourceTransformerInvalidOutputJSON - Invalid output json in source transformer response
	SourceTransformerInvalidOutputJSON = "Invalid output json in source transformer response"
	//NonIdentifiableRequest - Request neither has anonymousId nor userId
	NonIdentifiableRequest = "Request neither has anonymousId nor userId"
)

var (
	statusMap map[string]ResponseStatus
)

//ResponseStatus holds gateway response status message and code
type ResponseStatus struct {
	message string
	code    int
}

func init() {
	loadStatusMap()
}

func loadStatusMap() {
	statusMap = make(map[string]ResponseStatus)
	statusMap[Ok] = ResponseStatus{message: Ok, code: http.StatusOK}
	statusMap[RequestBodyNil] = ResponseStatus{message: RequestBodyNil, code: http.StatusBadRequest}
	statusMap[InvalidRequestMethod] = ResponseStatus{message: InvalidRequestMethod, code: http.StatusBadRequest}
	statusMap[TooManyRequests] = ResponseStatus{message: TooManyRequests, code: http.StatusTooManyRequests}
	statusMap[NoWriteKeyInBasicAuth] = ResponseStatus{message: NoWriteKeyInBasicAuth, code: http.StatusUnauthorized}
	statusMap[NoWriteKeyInQueryParams] = ResponseStatus{message: NoWriteKeyInQueryParams, code: http.StatusUnauthorized}
	statusMap[RequestBodyReadFailed] = ResponseStatus{message: RequestBodyReadFailed, code: http.StatusBadRequest}
	statusMap[RequestBodyTooLarge] = ResponseStatus{message: RequestBodyTooLarge, code: http.StatusRequestEntityTooLarge}
	statusMap[InvalidWriteKey] = ResponseStatus{message: InvalidWriteKey, code: http.StatusUnauthorized}
	statusMap[InvalidJSON] = ResponseStatus{message: InvalidJSON, code: http.StatusBadRequest}
	// webhook specific status
	statusMap[InvalidWebhookSource] = ResponseStatus{message: InvalidWebhookSource, code: http.StatusBadRequest}
	statusMap[SourceTransformerFailed] = ResponseStatus{message: SourceTransformerFailed, code: http.StatusBadRequest}
	statusMap[SourceTransformerResponseErrorReadFailed] = ResponseStatus{message: SourceTransformerResponseErrorReadFailed, code: http.StatusBadRequest}
	statusMap[SourceTransformerFailedToReadOutput] = ResponseStatus{message: SourceTransformerFailedToReadOutput, code: http.StatusBadRequest}
	statusMap[SourceTransformerInvalidResponseFormat] = ResponseStatus{message: SourceTransformerInvalidResponseFormat, code: http.StatusBadRequest}
	statusMap[SourceTransformerInvalidOutputFormatInResponse] = ResponseStatus{message: SourceTransformerInvalidOutputFormatInResponse, code: http.StatusBadRequest}
	statusMap[SourceTransformerInvalidOutputJSON] = ResponseStatus{message: SourceTransformerInvalidOutputJSON, code: http.StatusBadRequest}
	statusMap[NonIdentifiableRequest] = ResponseStatus{message: NonIdentifiableRequest, code: http.StatusBadRequest}
}

func GetStatus(key string) string {
	if status, ok := statusMap[key]; ok {
		return status.message
	}

	return key
}

func GetStatusCode(key string) int {
	if status, ok := statusMap[key]; ok {
		return status.code
	}

	return 200
}

//Always returns a valid response json
func GetResponse(key string) string {
	if status, ok := statusMap[key]; ok {
		return fmt.Sprintf(`{"msg": "%s"}`, status.message)
	}
	return fmt.Sprintf(`{"msg": "%s"}`, key)
}

func MakeResponse(msg string) string {
	return fmt.Sprintf(`{"msg": "%s"}`, msg)
}
