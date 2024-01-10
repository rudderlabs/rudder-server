package v2

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

type OAuthHandler struct {
	tokenProvider
	logger          logger.Logger
	rudderFlowType  RudderFlow
	CpConn          *ControlPlaneConnector
	destAuthInfoMap map[string]*AuthResponse
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

func (oauthHandler *OAuthHandler) FetchToken(fetchTokenParams *RefreshTokenParams) (int, *AuthResponse) {
	// authStats := &OAuthStats{
	// 	id:              fetchTokenParams.AccountId,
	// 	workspaceId:     fetchTokenParams.WorkspaceId,
	// 	rudderCategory:  "destination",
	// 	statName:        "",
	// 	isCallToCpApi:   false,
	// 	authErrCategory: "",
	// 	errorMessage:    "",
	// 	destDefName:     fetchTokenParams.DestDefName,
	// 	isTokenFetch:    true,
	// 	flowType:        oauthHandler.rudderFlowType,
	// 	action:          "fetch_token",
	// }
	return oauthHandler.GetTokenInfo(fetchTokenParams, "Fetch token")
}

func (oauthHandler *OAuthHandler) GetTokenInfo(refTokenParams *RefreshTokenParams, logTypeName string) (int, *AuthResponse) {
	body := RefreshTokenBodyParams{}
	res, err := json.Marshal(body)
	if err != nil {
		return 400, nil
	}
	refreshUrl := fmt.Sprintf("%s/destination/workspaces/%s/accounts/%s/token", configBEURL, refTokenParams.WorkspaceId, refTokenParams.AccountId)
	cpReq := &ControlPlaneRequestT{
		Body:           string(res),
		ContentType:    "application/json; charset=utf-8",
		Url:            refreshUrl,
		destName:       refTokenParams.DestDefName,
		Method:         http.MethodPost,
		RequestType:    "test",
		rudderFlowType: RudderFlow_Delivery,
		basicAuthUser:  "",
	}
	statusCode, res1 := oauthHandler.CpConn.cpApiCall(cpReq)
	res2 := &AuthResponse{}
	json.Unmarshal([]byte(res1), res2)
	return statusCode, res2
}
