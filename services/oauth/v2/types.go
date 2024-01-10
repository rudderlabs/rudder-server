package v2

import (
	"encoding/json"
	"fmt"
	"time"
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
