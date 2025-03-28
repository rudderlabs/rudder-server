package response

import (
	"fmt"
	"net/http"
)

const (
	// Ok - ok
	Ok = "ok"
	// RequestBodyNil - Request body is nil
	RequestBodyNil = "request body is nil"
	// InvalidRequestMethod - Request Method is invalid
	InvalidRequestMethod = "invalid http request method"
	// TooManyRequests - too many requests
	TooManyRequests = "max requests limit reached"
	// NoWriteKeyInBasicAuth - Failed to read writeKey from header
	NoWriteKeyInBasicAuth = "failed to read writekey from header"
	// NoWriteKeyInQueryParams - Failed to read writeKey from Query Params
	NoWriteKeyInQueryParams = "failed to read writekey from query params"
	// RequestBodyReadFailed - Failed to read body from request
	RequestBodyReadFailed = "failed to read body from request"
	// RequestBodyTooLarge - Request size exceeds max limit
	RequestBodyTooLarge = "request size exceeds max limit"
	// InvalidWriteKey - Invalid Write Key
	InvalidWriteKey = "invalid write key"
	// InvalidJSON - Invalid JSON
	InvalidJSON = "invalid json"
	// EmptyBatchPayload - Empty batch payload
	EmptyBatchPayload = "empty batch payload"
	// InvalidWebhookSource - Source does not accept webhook events
	InvalidWebhookSource = "source does not accept webhook events"
	// SourceTransformerResponseErrorReadFailed - failed to read error from source transformer response
	SourceTransformerResponseErrorReadFailed = "failed to read error from source transformer response"
	// SourceDisabled - write key is present, but the source for it is disabled.
	SourceDisabled = "source is disabled"
	// SourceTransformerFailed - internal server error in source transformer
	SourceTransformerFailed = "internal server error in source transformer"
	// SourceTransformerFailedToReadOutput - output not found in source transformer response
	SourceTransformerFailedToReadOutput = "output not found in source transformer response"
	// SourceTransformerInvalidResponseFormat - invalid format of source transformer response
	SourceTransformerInvalidResponseFormat = "invalid format of source transformer response"
	// SourceTransformerInvalidOutputFormatInResponse - invalid output format in source transformer response
	SourceTransformerInvalidOutputFormatInResponse = "invalid output format in source transformer response"
	// SourceTransformerInvalidOutputJSON - invalid output json in source transformer response
	SourceTransformerInvalidOutputJSON = "invalid output json in source transformer response"
	// NonIdentifiableRequest - request neither has anonymousId nor userId
	NonIdentifiableRequest = "request neither has anonymousId nor userId"
	// ErrorInMarshal - error while marshalling
	ErrorInMarshal = "error while marshalling"
	// ErrorInParseForm - error during parsing form
	ErrorInParseForm = "error during parsing form"
	// ErrorInParseMultiform - error during parsing multiform
	ErrorInParseMultiform = "error during parsing multiform"
	// NotRudderEvent = event is not a valid rudder event
	NotRudderEvent = "event is not a valid rudder event"
	// ContextDeadlineExceeded - context deadline exceeded
	ContextDeadlineExceeded = "context deadline exceeded"
	// GatewayTimeout - gateway timeout
	GatewayTimeout = "gateway timeout"
	// ServiceUnavailable - service unavailable
	ServiceUnavailable = "service unavailable"
	// NoSourceIdInHeader - failed to read source id from header
	NoSourceIdInHeader = "failed to read source id from header"
	// InvalidSourceID - invalid source id
	InvalidSourceID = "invalid source id"
	// InvalidReplaySource - invalid replay source
	InvalidReplaySource = "invalid replay source"
	// InvalidDestinationID - invalid destination id
	InvalidDestinationID = "invalid destination id"
	// DestinationDisabled - destination is disabled
	DestinationDisabled = "destination is disabled"
	// NoDestinationIDInHeader - failed to read destination id from header
	NoDestinationIDInHeader = "failed to read destination id from header"

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
