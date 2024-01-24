package v2

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

type (
	RudderFlow string
	AuthType   string
)

// AccessToken is the access token returned by the oauth server
type AccessToken struct {
	ExpirationDate time.Time `json:"expirationDate"`
	Token          []byte    `json:"secret"`
}
type OAuthHandler struct {
	tokenProvider
	logger                    logger.Logger
	rudderFlowType            RudderFlow
	CpConn                    *ControlPlaneConnector
	destAuthInfoMap           map[string]*AuthResponse
	tr                        *http.Transport
	client                    *http.Client
	destLockMap               map[string]*sync.RWMutex // This mutex map is used for disable destination locking
	accountLockMap            map[string]*sync.RWMutex // This mutex map is used for refresh token locking
	lockMapWMutex             *sync.RWMutex            // This mutex is used to prevent concurrent writes in lockMap(s) mentioned in the struct
	refreshActiveMap          map[string]bool          // Used to check if a refresh request for an account is already InProgress
	authStatusUpdateActiveMap map[string]bool          // Used to check if a authStatusInactive request for a destination is already InProgress
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
type AccountSecret struct {
	ExpirationDate string          `json:"expirationDate"`
	Secret         json.RawMessage `json:"secret"`
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
}

type Authorizer interface {
	RefreshToken(refTokenParams *RefreshTokenParams) (int, *AuthResponse)
	FetchToken(fetchTokenParams *RefreshTokenParams) (int, *AuthResponse)
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
}

type AuthStatusToggleResponse struct {
	Message string `json:"message,omitempty"`
}
