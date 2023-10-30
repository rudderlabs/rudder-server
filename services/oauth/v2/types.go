package v2

import (
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
