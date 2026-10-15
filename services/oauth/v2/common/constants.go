package common

import (
	"errors"
	"net/http"
)

const (
	CategoryRefreshToken = "REFRESH_TOKEN"
	// CategoryAuthStatusInactive Identifier to be sent from destination(during transformation/delivery)
	CategoryAuthStatusInactive = "AUTH_STATUS_INACTIVE"
	// RefTokenInvalidGrant Identifier for invalid_grant or access_denied errors(during refreshing the token)
	RefTokenInvalidGrant = "ref_token_invalid_grant"

	RefTokenInvalidResponse            = "INVALID_REFRESH_RESPONSE"
	TimeOutError                       = "timeout"
	NetworkError                       = "network_error"
	None                               = "none"
	SecretKey               ContextKey = "secret"
	RudderFlowDelivery      RudderFlow = "delivery"
	RudderFlowDelete        RudderFlow = "delete"

	ErrorType = "errorType"
)

// ErrInvalidGrant is a sentinel error returned when the token is invalid.
var ErrInvalidGrant = errors.New("invalid grant")

// CredentialErrorType returns errorType when it means the OAuth credential itself is dead (the
// refresh token was refused, or the destination reported the account inactive), and "" for every
// other error type. Callers report its result as the delivery's oauthErrorType, a bounded label.
func CredentialErrorType(errorType string) string {
	switch errorType {
	case RefTokenInvalidGrant, CategoryAuthStatusInactive:
		return errorType
	}
	return ""
}

// CredentialErrorStatusCode is the status a job gets when its OAuth credential is dead: 401 when the
// refresh token was refused, 403 when the destination reported the account inactive.
func CredentialErrorStatusCode(errorType string) int {
	switch errorType {
	case RefTokenInvalidGrant:
		return http.StatusUnauthorized
	case CategoryAuthStatusInactive:
		return http.StatusForbidden
	}
	return 0
}
