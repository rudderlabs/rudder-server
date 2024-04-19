package response

import (
	"fmt"
	"net/http"
)

const (
	// Ok - ok
	Ok = "OK"
	// RequestBodyNil - Request body is nil
	RequestBodyNil = "Request body is nil"
	// InvalidRequestMethod - Request Method is invalid
	InvalidRequestMethod = "Invalid HTTP Request Method"
	// TooManyRequests - too many requests
	TooManyRequests = "Max Requests Limit reached"
	// NoWriteKeyInBasicAuth - Failed to read writeKey from header
	NoWriteKeyInBasicAuth = "Failed to read writeKey from header"
	// NoWriteKeyInQueryParams - Failed to read writeKey from Query Params
	NoWriteKeyInQueryParams = "Failed to read writeKey from Query Params"
	// RequestBodyReadFailed - Failed to read body from request
	RequestBodyReadFailed = "Failed to read body from request"
	// RequestBodyTooLarge - Request size exceeds max limit
	RequestBodyTooLarge = "Request size exceeds max limit"
	// InvalidWriteKey - Invalid Write Key
	InvalidWriteKey = "Invalid Write Key"
	// InvalidJSON - Invalid JSON
	InvalidJSON = "Invalid JSON"
	// EmptyBatchPayload - Empty batch payload
	EmptyBatchPayload = "Empty batch payload"
	// InvalidWebhookSource - Source does not accept webhook events
	InvalidWebhookSource = "Source does not accept webhook events"
	// SourceTransformerResponseErrorReadFailed - Failed to read error from source transformer response
	SourceTransformerResponseErrorReadFailed = "Failed to read error from source transformer response"
	// SourceDisabled - write key is present, but the source for it is disabled.
	SourceDisabled = "Source is disabled"
	// SourceTransformerFailed - Internal server error in source transformer
	SourceTransformerFailed = "Internal server error in source transformer"
	// SourceTransformerFailedToReadOutput - Output not found in source transformer response
	SourceTransformerFailedToReadOutput = "Output not found in source transformer response"
	// SourceTransformerInvalidResponseFormat - Invalid format of source transformer response
	SourceTransformerInvalidResponseFormat = "Invalid format of source transformer response"
	// SourceTransformerInvalidOutputFormatInResponse - Invalid output format in source transformer response
	SourceTransformerInvalidOutputFormatInResponse = "Invalid output format in source transformer response"
	// SourceTransformerInvalidOutputJSON - Invalid output json in source transformer response
	SourceTransformerInvalidOutputJSON = "Invalid output json in source transformer response"
	// NonIdentifiableRequest - Request neither has anonymousId nor userId
	NonIdentifiableRequest = "Request neither has anonymousId nor userId"
	// ErrorInMarshal - Error while marshalling
	ErrorInMarshal = "Error while marshalling"
	// ErrorInParseForm - Error during parsing form
	ErrorInParseForm = "Error during parsing form"
	// ErrorInParseMultiform - Error during parsing multiform
	ErrorInParseMultiform = "Error during parsing multiform"
	// NotRudderEvent = Event is not a Valid Rudder Event
	NotRudderEvent = "Event is not a valid rudder event"
	// ContextDeadlineExceeded - context deadline exceeded
	ContextDeadlineExceeded = "context deadline exceeded"
	// GatewayTimeout - Gateway timeout
	GatewayTimeout = "Gateway timeout"
	// ServiceUnavailable - Service unavailable
	ServiceUnavailable = "Service unavailable"
	// NoSourceIdInHeader - Failed to read source id from header
	NoSourceIdInHeader = "Failed to read source id from header"
	// InvalidSourceID - Invalid source id
	InvalidSourceID = "Invalid source id"
	// InvalidReplaySource - Invalid replay source
	InvalidReplaySource = "Invalid replay source"
	// InvalidDestinationID - Invalid destination id
	InvalidDestinationID = "Invalid destination id"
	// DestinationDisabled - Destination is disabled
	DestinationDisabled = "Destination is disabled"
	// NoDestinationIDInHeader - Failed to read destination id from header
	NoDestinationIDInHeader = "Failed to read destination id from header"

	transPixelResponse = "\x47\x49\x46\x38\x39\x61\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\x00\x00\x00\x21\xF9\x04" +
		"\x01\x00\x00\x00\x00\x2C\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02\x44\x01\x00\x3B"

	InvalidStreamMessage = "missing required fields stream in message"
)

var statusMap = map[string]status{
	Ok:                      {message: Ok, code: http.StatusOK},
	RequestBodyNil:          {message: RequestBodyNil, code: http.StatusBadRequest},
	InvalidRequestMethod:    {message: InvalidRequestMethod, code: http.StatusBadRequest},
	TooManyRequests:         {message: TooManyRequests, code: http.StatusTooManyRequests},
	NoWriteKeyInBasicAuth:   {message: NoWriteKeyInBasicAuth, code: http.StatusUnauthorized},
	NoWriteKeyInQueryParams: {message: NoWriteKeyInQueryParams, code: http.StatusUnauthorized},
	RequestBodyReadFailed:   {message: RequestBodyReadFailed, code: http.StatusInternalServerError},
	RequestBodyTooLarge:     {message: RequestBodyTooLarge, code: http.StatusRequestEntityTooLarge},
	InvalidWriteKey:         {message: InvalidWriteKey, code: http.StatusUnauthorized},
	SourceDisabled:          {message: SourceDisabled, code: http.StatusNotFound},
	InvalidJSON:             {message: InvalidJSON, code: http.StatusBadRequest},
	EmptyBatchPayload:       {message: EmptyBatchPayload, code: http.StatusBadRequest},
	NoSourceIdInHeader:      {message: NoSourceIdInHeader, code: http.StatusUnauthorized},
	InvalidSourceID:         {message: InvalidSourceID, code: http.StatusUnauthorized},
	InvalidReplaySource:     {message: InvalidReplaySource, code: http.StatusUnauthorized},
	DestinationDisabled:     {message: DestinationDisabled, code: http.StatusNotFound},
	InvalidDestinationID:    {message: InvalidDestinationID, code: http.StatusBadRequest},
	NoDestinationIDInHeader: {message: NoDestinationIDInHeader, code: http.StatusBadRequest},
	InvalidStreamMessage:    {message: InvalidStreamMessage, code: http.StatusBadRequest},

	// webhook specific status
	InvalidWebhookSource:                           {message: InvalidWebhookSource, code: http.StatusNotFound},
	SourceTransformerFailed:                        {message: SourceTransformerFailed, code: http.StatusBadRequest},
	SourceTransformerResponseErrorReadFailed:       {message: SourceTransformerResponseErrorReadFailed, code: http.StatusInternalServerError},
	SourceTransformerFailedToReadOutput:            {message: SourceTransformerFailedToReadOutput, code: http.StatusInternalServerError},
	SourceTransformerInvalidResponseFormat:         {message: SourceTransformerInvalidResponseFormat, code: http.StatusInternalServerError},
	SourceTransformerInvalidOutputFormatInResponse: {message: SourceTransformerInvalidOutputFormatInResponse, code: http.StatusInternalServerError},
	SourceTransformerInvalidOutputJSON:             {message: SourceTransformerInvalidOutputJSON, code: http.StatusInternalServerError},
	NonIdentifiableRequest:                         {message: NonIdentifiableRequest, code: http.StatusBadRequest},
	ErrorInMarshal:                                 {message: ErrorInMarshal, code: http.StatusBadRequest},
	ErrorInParseForm:                               {message: ErrorInParseForm, code: http.StatusBadRequest},
	ErrorInParseMultiform:                          {message: ErrorInParseMultiform, code: http.StatusBadRequest},
	NotRudderEvent:                                 {message: NotRudderEvent, code: http.StatusBadRequest},
	ContextDeadlineExceeded:                        {message: GatewayTimeout, code: http.StatusGatewayTimeout},
	GatewayTimeout:                                 {message: GatewayTimeout, code: http.StatusGatewayTimeout},
	ServiceUnavailable:                             {message: ServiceUnavailable, code: http.StatusServiceUnavailable},
}

// status holds the gateway response status message and code
type status struct {
	message string
	code    int
}

func GetStatus(key string) string {
	if status, ok := statusMap[key]; ok {
		return status.message
	}
	return key
}

func GetPixelResponse() string {
	return transPixelResponse
}

func GetErrorStatusCode(key string) int {
	if status, ok := statusMap[key]; ok {
		return status.code
	}
	return http.StatusInternalServerError
}

func MakeResponse(msg string) string {
	return fmt.Sprintf(`{"msg": %q}`, msg)
}
