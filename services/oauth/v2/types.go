package v2

//go:generate mockgen -destination=../../../mocks/services/oauthV2/mock_oauthV2.go -package=mock_oauthV2 github.com/rudderlabs/rudder-server/services/oauth/v2 TokenProvider

import (
	"encoding/json"

	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
)

// AccountSecret is the access token returned by the oauth server
type AccountSecret struct {
	ExpirationDate string          `json:"expirationDate"`
	Secret         json.RawMessage `json:"secret"`
}

type CacheKey struct {
	WorkspaceID string
	AccountID   string
}

type TokenProvider interface {
	Identity() identity.Identifier
}

type AuthResponse struct {
	Account      AccountSecret
	Err          string
	ErrorMessage string
}
type RefreshTokenParams struct {
	AccountID   string
	WorkspaceID string
	DestDefName string
	WorkerID    int
	Secret      json.RawMessage
	Destination *DestinationInfo
}

type RefreshTokenBodyParams struct {
	HasExpired    bool            `json:"hasExpired"`
	ExpiredSecret json.RawMessage `json:"expiredSecret"`
}

type AuthStatusToggleParams struct {
	Destination     *DestinationInfo
	WorkspaceID     string
	RudderAccountID string
	AuthStatus      string
	StatPrefix      string
}

type authStatusToggleResponse struct {
	Message string `json:"message,omitempty"`
}

type OAuthInterceptorResponse struct {
	StatusCode int    `json:"statusCode"`         // This is non-zero when the OAuth interceptor, upon completing its functions, intends to pass on the status code to the caller.
	Response   string `json:"response,omitempty"` // This is non-empty when the OAuth interceptor, upon completing its functions, intends to pass on the response body to the caller.
}

type TransportResponse struct {
	OriginalResponse    string                   `json:"originalResponse"`
	InterceptorResponse OAuthInterceptorResponse `json:"interceptorResponse"`
}
