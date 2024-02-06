package v2

import (
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	rudderSync "github.com/rudderlabs/rudder-go-kit/sync"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

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

func WithCache(cache Cache) func(*OAuthHandler) {
	return func(h *OAuthHandler) {
		h.cache = cache
	}
}

func WithLocker(lock *rudderSync.PartitionRWLocker) func(*OAuthHandler) {
	return func(h *OAuthHandler) {
		h.lock = lock
	}
}

// This function creates a new OauthErrorResponseHandler
func NewOAuthHandler(provider tokenProvider, options ...func(*OAuthHandler)) *OAuthHandler {
	cpTimeoutDuration := config.GetDuration("HttpClient.oauth.timeout", 30, time.Second)
	oAuthHandler := &OAuthHandler{
		tokenProvider: provider,
		logger:        pkgLogger,
		CpConn:        NewControlPlaneConnector(WithCpClientTimeout(cpTimeoutDuration), WithParentLogger(pkgLogger)),
		// This timeout is kind of modifiable & it seemed like 10 mins for this is too much!
		rudderFlowType:            RudderFlow_Delivery,
		authStatusUpdateActiveMap: make(map[string]bool),
	}
	for _, opt := range options {
		opt(oAuthHandler)
	}
	if oAuthHandler.cache == nil {
		cache := NewCache()
		oAuthHandler.cache = cache
	}
	if oAuthHandler.lock == nil {
		oAuthHandler.lock = rudderSync.NewPartitionRWLocker()
	}
	return oAuthHandler
}

/*
Fetch token function is used to fetch the token from the cache or from the control plane
- It first checks if the token is present in the cache
- If the token is present in the cache, it checks if the token has expired
- If the token has expired, it fetches the token from the control plane
- If the token is not present in the cache, it fetches the token from the control plane
- It returns the status code, token and error
- It also sends the stats to the statsd
- It also sends the error to the control plane
Parameters:
  - fetchTokenParams: *RefreshTokenParams
  - logTypeName: string
  - authStats: *OAuthStats

Return:
  - int: status code
  - *AuthResponse: token
  - error: error
*/
func (oauthHandler *OAuthHandler) FetchToken(fetchTokenParams *RefreshTokenParams) (int, *AuthResponse, error) {
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

/*
Refresh token function is used to refresh the token from the control plane
- It fetches the token from the cache
- If the token is present in the cache, it checks if the token has expired
- If the token has expired, it fetches the token from the control plane
- If the token is present in the cache, it compares the token with the received token
  - If it doesn't match then it is possible the stored token is not expired but the received token is expired so return the stored token
  - Else if matches then go ahead

- It fetches the token from the control plane
- It returns the status code, token and error
- It also sends the stats to the statsd
- It also sends the error to the control plane
Parameters:
  - refTokenParams: *RefreshTokenParams

Return:
  - int: status code
  - *AuthResponse: token
  - error: error
*/
func (authErrHandler *OAuthHandler) RefreshToken(refTokenParams *RefreshTokenParams) (int, *AuthResponse, error) {
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

func (oauthHandler *OAuthHandler) GetTokenInfo(refTokenParams *RefreshTokenParams, logTypeName string, authStats *OAuthStats) (int, *AuthResponse, error) {
	startTime := time.Now()
	defer func() {
		authStats.statName = getOAuthActionStatName("total_latency")
		authStats.isCallToCpApi = false
		authStats.SendTimerStats(startTime)
	}()
	oauthHandler.lock.Lock(refTokenParams.AccountId)
	defer oauthHandler.lock.Unlock(refTokenParams.AccountId)
	refTokenBody := RefreshTokenBodyParams{}
	storedCache, ok := oauthHandler.cache.Get(refTokenParams.AccountId)
	if ok {
		storedCache := storedCache.(*AuthResponse)
		if checkIfTokenExpired(storedCache.Account, refTokenParams.Secret, authStats) {
			refTokenBody = RefreshTokenBodyParams{
				HasExpired:    true,
				ExpiredSecret: refTokenParams.Secret,
			}
		} else {
			return 200, &AuthResponse{Account: storedCache.Account}, nil
		}
	}
	return oauthHandler.fetchAccountInfoFromCp(refTokenParams, refTokenBody, authStats, logTypeName)
}
