package v2

import (
	"bytes"
	"net/http"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/cachettl"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	rudderlabsSync "github.com/rudderlabs/rudder-go-kit/sync"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	router_utils "github.com/rudderlabs/rudder-server/router/utils"
)

var (
	configBEURL      string
	pkgLogger        logger.Logger
	loggerNm         string
	globalTokenCache *cachettl.Cache[CacheKey, *AccessToken]
	globalLock       *rudderlabsSync.PartitionLocker
)

func Init() {
	configBEURL = backendconfig.GetConfigBackendURL()
	pkgLogger = logger.NewLogger().Child("router").Child("OAuthHandler")
	loggerNm = "OAuthHandler"
	globalTokenCache = cachettl.New[CacheKey, *AccessToken]()
	globalLock = rudderlabsSync.NewPartitionLocker()
}

// This function creates a new OauthErrorResponseHandler
func NewOAuthHandler(provider tokenProvider, options ...func(*OAuthHandler)) *OAuthHandler {
	cpTimeoutDuration := config.GetDuration("HttpClient.oauth.timeout", 30, time.Second)
	oAuthHandler := &OAuthHandler{
		tokenProvider: provider,
		logger:        pkgLogger,
		CpConn:        NewControlPlaneConnector(WithCpClientTimeout(cpTimeoutDuration), WithParentLogger(pkgLogger)),
		// This timeout is kind of modifiable & it seemed like 10 mins for this is too much!
		destLockMap:               make(map[string]*sync.RWMutex),
		accountLockMap:            make(map[string]*sync.RWMutex),
		lockMapWMutex:             &sync.RWMutex{},
		destAuthInfoMap:           make(map[string]*AuthResponse),
		refreshActiveMap:          make(map[string]bool),
		authStatusUpdateActiveMap: make(map[string]bool),
		rudderFlowType:            RudderFlow_Delivery,
	}
	for _, opt := range options {
		opt(oAuthHandler)
	}
	return oAuthHandler
}

func (oauthHandler *OAuthHandler) FetchToken(fetchTokenParams *RefreshTokenParams) (int, *AuthResponse) {
	authStats := &OAuthStats{
		id:              fetchTokenParams.AccountId,
		workspaceId:     fetchTokenParams.WorkspaceId,
		rudderCategory:  "destination",
		statName:        "",
		isCallToCpApi:   false,
		authErrCategory: "",
		errorMessage:    "",
		destDefName:     fetchTokenParams.DestDefName,
		isTokenFetch:    true,
		flowType:        oauthHandler.rudderFlowType,
		action:          "fetch_token",
	}
	return oauthHandler.GetTokenInfo(fetchTokenParams, "Fetch token", authStats)
}

func (oauthHandler *OAuthHandler) GetTokenInfo(refTokenParams *RefreshTokenParams, logTypeName string, authStats *OAuthStats) (int, *AuthResponse) {
	startTime := time.Now()
	defer func() {
		authStats.statName = getOAuthActionStatName("total_latency")
		authStats.isCallToCpApi = false
		authStats.SendTimerStats(startTime)
	}()

	accountMutex := oauthHandler.getKeyMutex(oauthHandler.accountLockMap, refTokenParams.AccountId)
	refTokenBody := RefreshTokenBodyParams{}
	if router_utils.IsNotEmptyString(string(refTokenParams.Secret)) {
		refTokenBody = RefreshTokenBodyParams{
			HasExpired:    true,
			ExpiredSecret: refTokenParams.Secret,
		}
	}
	accountMutex.RLock()
	refVal, ok := oauthHandler.destAuthInfoMap[refTokenParams.AccountId]
	if ok {
		isInvalidAccountSecretForRefresh := router_utils.IsNotEmptyString(string(refVal.Account.Secret)) &&
			!bytes.Equal(refVal.Account.Secret, refTokenParams.Secret)
		if isInvalidAccountSecretForRefresh {
			accountMutex.RUnlock()
			oauthHandler.logger.Debugf("[%s request] [Cache] :: (Read) %s response received(rt-worker-%d): %s\n", loggerNm, logTypeName, refTokenParams.WorkerId, refVal.Account.Secret)
			return http.StatusOK, refVal
		}
	}
	accountMutex.RUnlock()
	accountMutex.Lock()
	if isRefreshActive, isPresent := oauthHandler.refreshActiveMap[refTokenParams.AccountId]; isPresent && isRefreshActive {
		accountMutex.Unlock()
		if refVal != nil {
			secret := refVal.Account.Secret
			oauthHandler.logger.Debugf("[%s request] [Active] :: (Read) %s response received from cache(rt-worker-%d): %s\n", loggerNm, logTypeName, refTokenParams.WorkerId, string(secret))
			return http.StatusOK, refVal
		}
		// Empty Response(valid while many GetToken calls are happening)
		return http.StatusOK, &AuthResponse{
			Account: AccountSecret{
				Secret: []byte(""),
			},
			Err: "",
		}
	}
	// Refresh will start
	oauthHandler.refreshActiveMap[refTokenParams.AccountId] = true
	oauthHandler.logger.Debugf("[%s request] [rt-worker-%v] :: %v request is active!", loggerNm, logTypeName, refTokenParams.WorkerId)
	accountMutex.Unlock()

	defer func() {
		accountMutex.Lock()
		oauthHandler.refreshActiveMap[refTokenParams.AccountId] = false
		oauthHandler.logger.Debugf("[%s request] [rt-worker-%v]:: %v request is inactive!", loggerNm, logTypeName, refTokenParams.WorkerId)
		accountMutex.Unlock()
	}()

	oauthHandler.logger.Debugf("[%s] [%v request] Lock Acquired by rt-worker-%d\n", loggerNm, logTypeName, refTokenParams.WorkerId)
	statusCode := oauthHandler.fetchAccountInfoFromCp(refTokenParams, refTokenBody, authStats, logTypeName)
	return statusCode, oauthHandler.destAuthInfoMap[refTokenParams.AccountId]
}

func (authErrHandler *OAuthHandler) RefreshToken(refTokenParams *RefreshTokenParams) (int, *AuthResponse) {
	authStats := &OAuthStats{
		id:              refTokenParams.AccountId,
		workspaceId:     refTokenParams.WorkspaceId,
		rudderCategory:  "destination",
		statName:        "",
		isCallToCpApi:   false,
		authErrCategory: REFRESH_TOKEN,
		errorMessage:    "",
		destDefName:     refTokenParams.DestDefName,
		flowType:        authErrHandler.rudderFlowType,
		action:          "refresh_token",
	}
	return authErrHandler.GetTokenInfo(refTokenParams, "Refresh token", authStats)
}
