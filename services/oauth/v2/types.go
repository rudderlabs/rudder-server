package v2

import (
	"encoding/json"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	rudderSync "github.com/rudderlabs/rudder-go-kit/sync"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
)

type (
	RudderFlow string
	AuthType   string
	ContextKey string
)

type expirationDate struct {
	ExpirationDate string `json:"expirationDate"`
}

// AccountSecret is the access token returned by the oauth server
type AccountSecret struct {
	ExpirationDate string          `json:"expirationDate"`
	Secret         json.RawMessage `json:"secret"`
}
type OAuthHandler struct {
	TokenProvider
	Logger                    logger.Logger
	RudderFlowType            RudderFlow
	CpConn                    ControlPlaneConnector
	AuthStatusUpdateActiveMap map[string]bool // Used to check if a authStatusInactive request for a destination is already InProgress
	Cache                     Cache
	CacheMutex                *rudderSync.PartitionRWLocker
	ExpirationTimeDiff        time.Duration
	ConfigBEURL               string
	LoggerName                string
}
type CacheKey struct {
	WorkspaceID string
	AccountID   string
}

type TokenProvider interface {
	Identity() identity.Identifier
}

type ControlPlaneRequest struct {
	Body           string
	ContentType    string
	Url            string
	Method         string
	destName       string
	RequestType    string // This is to add more refined stat tags
	BasicAuthUser  identity.Identifier
	rudderFlowType RudderFlow
}

type AuthResponse struct {
	Account      AccountSecret
	Err          string
	ErrorMessage string
}
type RefreshTokenParams struct {
	AccountId   string
	WorkspaceId string
	DestDefName string
	WorkerId    int
	Secret      json.RawMessage
	Destination *DestinationInfo
}

type Authorizer interface {
	RefreshToken(refTokenParams *RefreshTokenParams) (int, *AuthResponse, error)
	FetchToken(fetchTokenParams *RefreshTokenParams) (int, *AuthResponse, error)
	AuthStatusToggle(authStatusToggleParams *AuthStatusToggleParams) (statusCode int, respBody string)
}

type RefreshTokenBodyParams struct {
	HasExpired    bool            `json:"hasExpired"`
	ExpiredSecret json.RawMessage `json:"expiredSecret"`
}

type OAuthStats struct {
	id              string
	workspaceID     string
	errorMessage    string
	rudderCategory  string
	statName        string
	isCallToCpApi   bool
	authErrCategory string
	destDefName     string
	isTokenFetch    bool // This stats field is used to identify if a request to get token is arising from processor
	flowType        RudderFlow
	action          string // refresh_token, fetch_token, auth_status_toggle
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
type transformerResponse struct {
	AuthErrorCategory string `json:"authErrorCategory"`
}

type OAuthInterceptorResponse struct {
	StatusCode int    `json:"statusCode"`         // This is non-zero when the OAuth interceptor, upon completing its functions, intends to pass on the status code to the caller.
	Response   string `json:"response,omitempty"` // This is non-empty when the OAuth interceptor, upon completing its functions, intends to pass on the response body to the caller.
}

type TransportResponse struct {
	OriginalResponse    string                   `json:"originalResponse"`
	InterceptorResponse OAuthInterceptorResponse `json:"interceptorResponse"`
}
