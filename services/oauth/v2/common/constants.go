package common

import "errors"

const (
	CategoryRefreshToken = "REFRESH_TOKEN"
	// CategoryAuthStatusInactive Identifier to be sent from destination(during transformation/delivery)
	CategoryAuthStatusInactive = "AUTH_STATUS_INACTIVE"
	// RefTokenInvalidGrant Identifier for invalid_grant or access_denied errors(during refreshing the token)
	RefTokenInvalidGrant = "ref_token_invalid_grant"

	RefTokenInvalidResponse = "INVALID_REFRESH_RESPONSE"
	TimeOutError            = "timeout"
	NetworkError            = "network_error"
	None                    = "none"

	DestKey            ContextKey = "destination"
	SecretKey          ContextKey = "secret"
	RudderFlowDelivery RudderFlow = "delivery"
	RudderFlowDelete   RudderFlow = "delete"

	AuthStatusInactive = "inactive"

	ErrorType = "errorType"
)

// ErrInvalidGrant is a sentinel error returned when the token is invalid.
var ErrInvalidGrant = errors.New("invalid grant")
