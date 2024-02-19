package v2

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	rudderSync "github.com/rudderlabs/rudder-go-kit/sync"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	router_utils "github.com/rudderlabs/rudder-server/router/utils"
)

var (
	configBEURL string
	pkgLogger   logger.Logger
	loggerNm    string
)

func WithCache(cache Cache) func(*OAuthHandler) {
	return func(h *OAuthHandler) {
		h.Cache = cache
	}
}

func WithLocker(lock *rudderSync.PartitionRWLocker) func(*OAuthHandler) {
	return func(h *OAuthHandler) {
		h.CacheMutex = lock
	}
}

func Init() {
	configBEURL = backendconfig.GetConfigBackendURL()
	pkgLogger = logger.NewLogger().Child("router").Child("OAuthHandler")
	loggerNm = "OAuthHandler"
}

// This function creates a new OauthErrorResponseHandler
func NewOAuthHandler(provider TokenProvider, options ...func(*OAuthHandler)) *OAuthHandler {
	cpTimeoutDuration := config.GetDuration("HttpClient.oauth.timeout", 30, time.Second)
	oAuthHandler := &OAuthHandler{
		TokenProvider: provider,
		Logger:        pkgLogger,
		CpConn:        NewControlPlaneConnector(WithCpClientTimeout(cpTimeoutDuration), WithParentLogger(pkgLogger)),
		// This timeout is kind of modifiable & it seemed like 10 mins for this is too much!
		RudderFlowType:            RudderFlow_Delivery,
		AuthStatusUpdateActiveMap: make(map[string]bool),
	}
	for _, opt := range options {
		opt(oAuthHandler)
	}
	if oAuthHandler.Cache == nil {
		cache := NewCache()
		oAuthHandler.Cache = cache
	}
	if oAuthHandler.CacheMutex == nil {
		oAuthHandler.CacheMutex = rudderSync.NewPartitionRWLocker()
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
		flowType:        oauthHandler.RudderFlowType,
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
func (oauthHandler *OAuthHandler) RefreshToken(refTokenParams *RefreshTokenParams) (int, *AuthResponse, error) {
	authStats := &OAuthStats{
		id:              refTokenParams.AccountId,
		workspaceId:     refTokenParams.WorkspaceId,
		rudderCategory:  "destination",
		statName:        "",
		isCallToCpApi:   false,
		authErrCategory: REFRESH_TOKEN,
		errorMessage:    "",
		destDefName:     refTokenParams.DestDefName,
		flowType:        oauthHandler.RudderFlowType,
		action:          "refresh_token",
	}
	return oauthHandler.GetTokenInfo(refTokenParams, "Refresh token", authStats)
	// handling of refresh token response
	// if statusCode == http.StatusOK {
	// 	// refresh token successful --> retry the event
	// 	statusCode = http.StatusInternalServerError
	// } else {
	// 	// invalid_grant -> 4xx
	// 	// refresh token failed -> erreneous status-code
	// 	if refSecret.Err == REF_TOKEN_INVALID_GRANT {
	// 		// Add some kind of debug logger (if needed)
	// 		statusCode = oauthHandler.updateAuthStatusToInactive(refTokenParams.Destination, refTokenParams.WorkspaceId, refTokenParams.AccountId)
	// 	}
	// }
	// if refErr != nil {
	// 	refErr = fmt.Errorf("failed to refresh token: %w", refErr)
	// }
	// return statusCode, refSecret, refErr
}

func (oauthHandler *OAuthHandler) GetTokenInfo(refTokenParams *RefreshTokenParams, logTypeName string, authStats *OAuthStats) (int, *AuthResponse, error) {
	startTime := time.Now()
	defer func() {
		authStats.statName = getOAuthActionStatName("total_latency")
		authStats.isCallToCpApi = false
		authStats.SendTimerStats(startTime)
	}()
	oauthHandler.CacheMutex.Lock(refTokenParams.AccountId)
	defer oauthHandler.CacheMutex.Unlock(refTokenParams.AccountId)
	refTokenBody := RefreshTokenBodyParams{}
	storedCache, ok := oauthHandler.Cache.Get(refTokenParams.AccountId)
	if ok {
		cachedSecret := storedCache.(*AuthResponse)
		if !checkIfTokenExpired(cachedSecret.Account, refTokenParams.Secret, authStats) {
			return http.StatusOK, cachedSecret, nil
		}
		// Refresh token preparation
		refTokenBody = RefreshTokenBodyParams{
			HasExpired:    true,
			ExpiredSecret: refTokenParams.Secret,
		}
	}
	statusCode, refSecret, refErr := oauthHandler.fetchAccountInfoFromCp(refTokenParams, refTokenBody, authStats, logTypeName)
	// handling of refresh token response
	if statusCode == http.StatusOK {
		// fetching/refreshing through control plane was successful
		return statusCode, refSecret, nil
	}
	if refErr != nil {
		refErr = fmt.Errorf("failed to fetch/refresh token inside getTokenInfo: %w", refErr)
	}
	return statusCode, refSecret, refErr
}

func (authErrHandler *OAuthHandler) UpdateAuthStatusToInactive(destination *backendconfig.DestinationT, workspaceID, rudderAccountId string) int {
	inactiveAuthStatusStatTags := stats.Tags{
		"id":          destination.ID,
		"destType":    destination.DestinationDefinition.Name,
		"workspaceId": workspaceID,
		"success":     "true",
		"flowType":    string(RudderFlow_Delivery),
	}
	errCatStatusCode, _ := authErrHandler.authStatusToggle(&AuthStatusToggleParams{
		Destination:     destination,
		WorkspaceId:     workspaceID,
		RudderAccountId: rudderAccountId,
		StatPrefix:      AuthStatusInactive,
		AuthStatus:      AUTH_STATUS_INACTIVE,
	})
	if errCatStatusCode != http.StatusOK {
		// Error while inactivating authStatus
		inactiveAuthStatusStatTags["success"] = "false"
	}
	stats.Default.NewTaggedStat("auth_status_inactive_category_count", stats.CountType, inactiveAuthStatusStatTags).Increment()
	// Abort the jobs as the destination is disabled
	return http.StatusBadRequest
}

func (authErrHandler *OAuthHandler) authStatusToggle(params *AuthStatusToggleParams) (statusCode int, respBody string) {
	authErrHandlerTimeStart := time.Now()
	destinationId := params.Destination.ID
	action := fmt.Sprintf("auth_status_%v", params.StatPrefix)

	authStatusToggleStats := &OAuthStats{
		id:              destinationId,
		workspaceId:     params.WorkspaceId,
		rudderCategory:  "destination",
		statName:        "",
		isCallToCpApi:   false,
		authErrCategory: params.AuthStatus,
		errorMessage:    "",
		destDefName:     params.Destination.DestinationDefinition.Name,
		flowType:        authErrHandler.RudderFlowType,
		action:          action,
	}
	defer func() {
		authStatusToggleStats.statName = getOAuthActionStatName("total_latency")
		authStatusToggleStats.isCallToCpApi = false
		authStatusToggleStats.SendTimerStats(authErrHandlerTimeStart)
	}()
	authErrHandler.CacheMutex.Lock(params.RudderAccountId)
	isAuthStatusUpdateActive, isAuthStatusUpdateReqPresent := authErrHandler.AuthStatusUpdateActiveMap[destinationId]
	authStatusUpdateActiveReq := strconv.FormatBool(isAuthStatusUpdateReqPresent && isAuthStatusUpdateActive)
	if isAuthStatusUpdateReqPresent && isAuthStatusUpdateActive {
		authErrHandler.CacheMutex.Unlock(params.RudderAccountId)
		authErrHandler.Logger.Debugf("[%s request] :: AuthStatusInactive request Active : %s\n", loggerNm, authStatusUpdateActiveReq)
		return http.StatusConflict, ErrPermissionOrTokenRevoked.Error()
	}

	authErrHandler.AuthStatusUpdateActiveMap[destinationId] = true
	authErrHandler.CacheMutex.Unlock(params.RudderAccountId)

	defer func() {
		authErrHandler.CacheMutex.Lock(params.RudderAccountId)
		authErrHandler.AuthStatusUpdateActiveMap[destinationId] = false
		authErrHandler.Logger.Debugf("[%s request] :: AuthStatusInactive request is inactive!", loggerNm)
		// After trying to inactivate authStatus for destination, need to remove existing accessToken(from in-memory cache)
		// This is being done to obtain new token after an update such as re-authorisation is done
		authErrHandler.Cache.Delete(params.RudderAccountId)
		authErrHandler.CacheMutex.Unlock(params.RudderAccountId)
	}()

	authStatusToggleUrl := fmt.Sprintf("%s/workspaces/%s/destinations/%s/authStatus/toggle", configBEURL, params.WorkspaceId, destinationId)

	authStatusInactiveCpReq := &ControlPlaneRequestT{
		Url:           authStatusToggleUrl,
		Method:        http.MethodPut,
		Body:          fmt.Sprintf(`{"authStatus": "%v"}`, params.AuthStatus),
		ContentType:   "application/json",
		destName:      params.Destination.DestinationDefinition.Name,
		RequestType:   action,
		basicAuthUser: authErrHandler.Identity(),
	}
	authStatusToggleStats.statName = getOAuthActionStatName("request_sent")
	authStatusToggleStats.isCallToCpApi = true
	authStatusToggleStats.SendCountStat()

	cpiCallStartTime := time.Now()
	statusCode, respBody = authErrHandler.CpConn.CpApiCall(authStatusInactiveCpReq)
	authStatusToggleStats.statName = getOAuthActionStatName("request_latency")
	defer authStatusToggleStats.SendTimerStats(cpiCallStartTime)
	authErrHandler.Logger.Errorf(`Response from CP(stCd: %v) for auth status inactive req: %v`, statusCode, respBody)

	var authStatusToggleRes *AuthStatusToggleResponse
	unmarshalErr := json.Unmarshal([]byte(respBody), &authStatusToggleRes)
	if router_utils.IsNotEmptyString(respBody) && (unmarshalErr != nil || !router_utils.IsNotEmptyString(authStatusToggleRes.Message) || statusCode != http.StatusOK) {
		var msg string
		if unmarshalErr != nil {
			msg = unmarshalErr.Error()
		} else {
			msg = fmt.Sprintf("Could not update authStatus to inactive for destination: %v", authStatusToggleRes.Message)
		}
		authStatusToggleStats.statName = getOAuthActionStatName("failure")
		authStatusToggleStats.errorMessage = msg
		authStatusToggleStats.SendCountStat()
		return http.StatusBadRequest, ErrPermissionOrTokenRevoked.Error()
	}

	authErrHandler.Logger.Errorf("[%s request] :: (Write) auth status inactive Response received : %s\n", loggerNm, respBody)
	authStatusToggleStats.statName = getOAuthActionStatName("success")
	authStatusToggleStats.errorMessage = ""
	authStatusToggleStats.SendCountStat()

	return http.StatusBadRequest, ErrPermissionOrTokenRevoked.Error()
}
