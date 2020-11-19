package response

import (
	"encoding/json"
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

//ResponseStatus holds gateway response status Message and code
type ResponseStatus struct {
	Message string `json:"msg,omitempty"`
	code    int    `json:"-,"`
}

func init() {
	loadStatusMap()
}

func loadStatusMap() {
	statusMap = make(map[string]ResponseStatus)
	statusMap[Ok] = ResponseStatus{Message: Ok, code: http.StatusOK}
	statusMap[RequestBodyNil] = ResponseStatus{Message: RequestBodyNil, code: http.StatusBadRequest}
	statusMap[InvalidRequestMethod] = ResponseStatus{Message: InvalidRequestMethod, code: http.StatusBadRequest}
	statusMap[TooManyRequests] = ResponseStatus{Message: TooManyRequests, code: http.StatusTooManyRequests}
	statusMap[NoWriteKeyInBasicAuth] = ResponseStatus{Message: NoWriteKeyInBasicAuth, code: http.StatusUnauthorized}
	statusMap[NoWriteKeyInQueryParams] = ResponseStatus{Message: NoWriteKeyInQueryParams, code: http.StatusUnauthorized}
	statusMap[RequestBodyReadFailed] = ResponseStatus{Message: RequestBodyReadFailed, code: http.StatusBadRequest}
	statusMap[RequestBodyTooLarge] = ResponseStatus{Message: RequestBodyTooLarge, code: http.StatusRequestEntityTooLarge}
	statusMap[InvalidWriteKey] = ResponseStatus{Message: InvalidWriteKey, code: http.StatusUnauthorized}
	statusMap[InvalidJSON] = ResponseStatus{Message: InvalidJSON, code: http.StatusBadRequest}
	// webhook specific status
	statusMap[InvalidWebhookSource] = ResponseStatus{Message: InvalidWebhookSource, code: http.StatusBadRequest}
	statusMap[SourceTransformerFailed] = ResponseStatus{Message: SourceTransformerFailed, code: http.StatusBadRequest}
	statusMap[SourceTransformerResponseErrorReadFailed] = ResponseStatus{Message: SourceTransformerResponseErrorReadFailed, code: http.StatusBadRequest}
	statusMap[SourceTransformerFailedToReadOutput] = ResponseStatus{Message: SourceTransformerFailedToReadOutput, code: http.StatusBadRequest}
	statusMap[SourceTransformerInvalidResponseFormat] = ResponseStatus{Message: SourceTransformerInvalidResponseFormat, code: http.StatusBadRequest}
	statusMap[SourceTransformerInvalidOutputFormatInResponse] = ResponseStatus{Message: SourceTransformerInvalidOutputFormatInResponse, code: http.StatusBadRequest}
	statusMap[SourceTransformerInvalidOutputJSON] = ResponseStatus{Message: SourceTransformerInvalidOutputJSON, code: http.StatusBadRequest}
	statusMap[NonIdentifiableRequest] = ResponseStatus{Message: NonIdentifiableRequest, code: http.StatusBadRequest}
}

func GetStatus(key string) string {
	if status, ok := statusMap[key]; ok {
		return status.Message
	}

	return ""
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
		response, _ := json.Marshal(status)
		return string(response)
	}
	return "{}"
}

func MakeResponse(msg string) string {
	resp, _ := json.Marshal(ResponseStatus{Message: msg})
	//json.MarshalIndent(ResponseStatus{Message: msg},""," ")
	//json.Marshal(ResponseStatus{Message: msg})
	return string(resp)
}
