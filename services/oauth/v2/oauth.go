package v2

import (
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

type OAuthHandler struct {
	tokenProvider
	logger         logger.Logger
	rudderFlowType RudderFlow
	CpConn         *ControlPlaneConnector
}

var (
	configBEURL string
	pkgLogger   logger.Logger
	loggerNm    string
)

func Init() {
	configBEURL = backendconfig.GetConfigBackendURL()
	pkgLogger = logger.NewLogger().Child("router").Child("OAuthHandler")
	loggerNm = "OAuthHandler"
}

// This function creates a new OauthErrorResponseHandler
func NewOAuthHandler(provider tokenProvider, options ...func(*OAuthHandler)) *OAuthHandler {
	cpTimeoutDuration := config.GetDuration("HttpClient.oauth.timeout", 30, time.Second)
	oAuthHandler := &OAuthHandler{
		tokenProvider: provider,
		logger:        pkgLogger,
		CpConn:        NewControlPlaneConnector(WithCpClientTimeout(cpTimeoutDuration), WithParentLogger(pkgLogger)),
		// This timeout is kind of modifiable & it seemed like 10 mins for this is too much!
		// destLockMap:               make(map[string]*sync.RWMutex),
		// accountLockMap:            make(map[string]*sync.RWMutex),
		// lockMapWMutex:             &sync.RWMutex{},
		// destAuthInfoMap:           make(map[string]*AuthResponse),
		// refreshActiveMap:          make(map[string]bool),
		// authStatusUpdateActiveMap: make(map[string]bool),
		rudderFlowType: RudderFlow_Delivery,
	}
	for _, opt := range options {
		opt(oAuthHandler)
	}
	return oAuthHandler
}

func WithRudderFlow(rudderFlow RudderFlow) func(*OAuthHandler) {
	return func(h *OAuthHandler) {
		h.rudderFlowType = rudderFlow
	}
}
