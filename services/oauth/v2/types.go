package v2

import (
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/logger"
	rudderSync "github.com/rudderlabs/rudder-go-kit/sync"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

type (
	RudderFlow string
	AuthType   string
)

// AccountSecret is the access token returned by the oauth server
type AccountSecret struct {
	ExpirationDate string          `json:"expirationDate"`
	Secret         json.RawMessage `json:"secret"`
}
type OAuthHandler struct {
	tokenProvider
	logger                    logger.Logger
	rudderFlowType            RudderFlow
	CpConn                    *ControlPlaneConnector
	authStatusUpdateActiveMap map[string]bool // Used to check if a authStatusInactive request for a destination is already InProgress
	cache                     Cache
	lock                      *rudderSync.PartitionRWLocker
}
type CacheKey struct {
	WorkspaceID string
	AccountID   string
}

func (c CacheKey) String() string {
	return fmt.Sprintf("%s:%s", c.WorkspaceID, c.AccountID)
}

type tokenProvider interface {
	AccessToken() string
}

type ControlPlaneRequestT struct {
	Body        string
	ContentType string
	Url         string
	Method      string
	destName    string
	RequestType string // This is to add more refined stat tags

	// New fields
	basicAuthUser  string
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
	Destination *backendconfig.DestinationT
}

type Authorizer interface {
	RefreshToken(refTokenParams *RefreshTokenParams) (int, *AuthResponse, error)
	FetchToken(fetchTokenParams *RefreshTokenParams) (int, *AuthResponse, error)
}

type RefreshTokenBodyParams struct {
	HasExpired    bool            `json:"hasExpired"`
	ExpiredSecret json.RawMessage `json:"expiredSecret"`
}

type OAuthStats struct {
	id              string
	workspaceId     string
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
	Destination     *backendconfig.DestinationT
	WorkspaceId     string
	RudderAccountId string
	AuthStatus      string
	StatPrefix      string
}

type AuthStatusToggleResponse struct {
	Message string `json:"message,omitempty"`
}
type TransformerResponse struct {
	AuthErrorCategory string `json:"authErrorCategory"`
}
