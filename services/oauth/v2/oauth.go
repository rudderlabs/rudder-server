package v2

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
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

func WithExpirationTimeDiff(expirationTimeDiff time.Duration) func(*OAuthHandler) {
	return func(h *OAuthHandler) {
		h.ExpirationTimeDiff = expirationTimeDiff
	}
}

func Init() {
	configBEURL = backendconfig.GetConfigBackendURL()
	pkgLogger = logger.NewLogger().Child("router").Child("OAuthHandler")
	loggerNm = "OAuthHandler"
}

// NewOAuthHandler returns a new instance of OAuthHandler
func NewOAuthHandler(provider TokenProvider, options ...func(*OAuthHandler)) *OAuthHandler {
	cpTimeoutDuration := config.GetDuration("HttpClient.oauth.timeout", 30, time.Second)
	h := &OAuthHandler{
		TokenProvider: provider,
		Logger:        pkgLogger,
		CpConn:        NewControlPlaneConnector(WithCpClientTimeout(cpTimeoutDuration), WithParentLogger(pkgLogger)),
		// This timeout is kind of modifiable & it seemed like 10 mins for this is too much!
		RudderFlowType:            RudderFlow_Delivery,
		AuthStatusUpdateActiveMap: make(map[string]bool),
	}
	for _, opt := range options {
		opt(h)
	}
	if h.Cache == nil {
		cache := NewCache()
		h.Cache = cache
	}
	if h.CacheMutex == nil {
		h.CacheMutex = rudderSync.NewPartitionRWLocker()
	}
	if h.ExpirationTimeDiff.Seconds() == 0 {
		h.ExpirationTimeDiff = 1 * time.Minute
	}
	return h
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
func (h *OAuthHandler) FetchToken(fetchTokenParams *RefreshTokenParams) (int, *AuthResponse, error) {
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
		flowType:        h.RudderFlowType,
		action:          "fetch_token",
	}
	return h.GetTokenInfo(fetchTokenParams, "Fetch token", authStats)
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
func (h *OAuthHandler) RefreshToken(refTokenParams *RefreshTokenParams) (int, *AuthResponse, error) {
	authStats := &OAuthStats{
		id:              refTokenParams.AccountId,
		workspaceId:     refTokenParams.WorkspaceId,
		rudderCategory:  "destination",
		statName:        "",
		isCallToCpApi:   false,
		authErrCategory: CategoryRefreshToken,
		errorMessage:    "",
		destDefName:     refTokenParams.DestDefName,
		flowType:        h.RudderFlowType,
		action:          "refresh_token",
	}
	return h.GetTokenInfo(refTokenParams, "Refresh token", authStats)
}

func (h *OAuthHandler) GetTokenInfo(refTokenParams *RefreshTokenParams, logTypeName string, authStats *OAuthStats) (int, *AuthResponse, error) {
	h.Logger.Debugn("[request] :: Get Token Info request received",
		logger.NewStringField("Module name:", loggerNm),
		logger.NewStringField("Call Type", logTypeName),
		logger.NewStringField("AccountId", refTokenParams.AccountId))
	startTime := time.Now()
	defer func() {
		authStats.statName = GetOAuthActionStatName("total_latency")
		authStats.isCallToCpApi = false
		authStats.SendTimerStats(startTime)
	}()
	h.CacheMutex.Lock(refTokenParams.AccountId)
	defer h.CacheMutex.Unlock(refTokenParams.AccountId)
	refTokenBody := RefreshTokenBodyParams{}
	storedCache, ok := h.Cache.Get(refTokenParams.AccountId)
	if ok {
		cachedSecret := storedCache.(*AuthResponse)
		if !checkIfTokenExpired(cachedSecret.Account, refTokenParams.Secret, h.ExpirationTimeDiff, authStats) {
			return http.StatusOK, cachedSecret, nil
		}
		// Refresh token preparation
		refTokenBody = RefreshTokenBodyParams{
			HasExpired:    true,
			ExpiredSecret: refTokenParams.Secret,
		}
	}
	statusCode, refSecret, refErr := h.fetchAccountInfoFromCp(refTokenParams, refTokenBody, authStats, logTypeName)
	// handling of refresh token response
	if statusCode == http.StatusOK {
		// fetching/refreshing through control plane was successful
		return statusCode, refSecret, nil
	}
	// if refErr != nil {
	// 	refErr = fmt.Errorf("%w", refErr)
	// }
	return statusCode, refSecret, refErr
}

func (h *OAuthHandler) AuthStatusToggle(params *AuthStatusToggleParams) (statusCode int, respBody string) {
	authErrHandlerTimeStart := time.Now()
	destinationId := params.Destination.DestinationId
	action := fmt.Sprintf("auth_status_%v", params.StatPrefix)

	authStatusToggleStats := &OAuthStats{
		id:              destinationId,
		workspaceId:     params.WorkspaceId,
		rudderCategory:  "destination",
		statName:        "",
		isCallToCpApi:   false,
		authErrCategory: params.AuthStatus,
		errorMessage:    "",
		destDefName:     params.Destination.DestDefName,
		flowType:        h.RudderFlowType,
		action:          action,
	}
	defer func() {
		authStatusToggleStats.statName = GetOAuthActionStatName("total_latency")
		authStatusToggleStats.isCallToCpApi = false
		authStatusToggleStats.SendTimerStats(authErrHandlerTimeStart)
	}()
	h.CacheMutex.Lock(params.RudderAccountId)
	isAuthStatusUpdateActive, isAuthStatusUpdateReqPresent := h.AuthStatusUpdateActiveMap[destinationId]
	if isAuthStatusUpdateReqPresent && isAuthStatusUpdateActive {
		h.CacheMutex.Unlock(params.RudderAccountId)
		h.Logger.Debugn("[request] :: Received AuthStatusInactive request while another request is active!",
			logger.NewStringField("Module name", loggerNm))
		return http.StatusConflict, ErrPermissionOrTokenRevoked.Error()
	}

	h.AuthStatusUpdateActiveMap[destinationId] = true
	h.CacheMutex.Unlock(params.RudderAccountId)

	defer func() {
		h.CacheMutex.Lock(params.RudderAccountId)
		h.AuthStatusUpdateActiveMap[destinationId] = false
		h.Logger.Debugn("[request] :: AuthStatusInactive request is inactive!", logger.NewStringField("Module name", loggerNm))
		// After trying to inactivate authStatus for destination, need to remove existing accessToken(from in-memory cache)
		// This is being done to obtain new token after an update such as re-authorisation is done
		h.Cache.Delete(params.RudderAccountId)
		h.CacheMutex.Unlock(params.RudderAccountId)
	}()

	authStatusToggleUrl := fmt.Sprintf("%s/workspaces/%s/destinations/%s/authStatus/toggle", configBEURL, params.WorkspaceId, destinationId)

	authStatusInactiveCpReq := &ControlPlaneRequest{
		Url:           authStatusToggleUrl,
		Method:        http.MethodPut,
		Body:          fmt.Sprintf(`{"authStatus": "%v"}`, params.AuthStatus),
		ContentType:   "application/json",
		destName:      params.Destination.DestDefName,
		RequestType:   action,
		BasicAuthUser: h.Identity(),
	}
	authStatusToggleStats.statName = GetOAuthActionStatName("request_sent")
	authStatusToggleStats.isCallToCpApi = true
	authStatusToggleStats.SendCountStat()

	cpiCallStartTime := time.Now()
	statusCode, respBody = h.CpConn.CpApiCall(authStatusInactiveCpReq)
	authStatusToggleStats.statName = GetOAuthActionStatName("request_latency")
	defer authStatusToggleStats.SendTimerStats(cpiCallStartTime)
	h.Logger.Debugn("[request] :: Response from CP for auth status inactive req",
		logger.NewStringField("Module name", loggerNm),
		logger.NewIntField("StatusCode", int64(statusCode)),
		logger.NewStringField("Response", respBody))

	var authStatusToggleRes *AuthStatusToggleResponse
	unmarshalErr := json.Unmarshal([]byte(respBody), &authStatusToggleRes)
	if router_utils.IsNotEmptyString(respBody) && (unmarshalErr != nil || !router_utils.IsNotEmptyString(authStatusToggleRes.Message) || statusCode != http.StatusOK) {
		var msg string
		if unmarshalErr != nil {
			msg = unmarshalErr.Error()
		} else {
			msg = fmt.Sprintf("Could not update authStatus to inactive for destination: %v", authStatusToggleRes.Message)
		}
		authStatusToggleStats.statName = GetOAuthActionStatName("failure")
		authStatusToggleStats.errorMessage = msg
		authStatusToggleStats.SendCountStat()
		return http.StatusBadRequest, ErrPermissionOrTokenRevoked.Error()
	}
	h.Logger.Debugn("[request] :: (Write) auth status inactive Response received",
		logger.NewStringField("Module name", loggerNm),
		logger.NewIntField("StatusCode", int64(statusCode)),
		logger.NewStringField("Response", respBody))
	authStatusToggleStats.statName = GetOAuthActionStatName("success")
	authStatusToggleStats.errorMessage = ""
	authStatusToggleStats.SendCountStat()

	return http.StatusBadRequest, ErrPermissionOrTokenRevoked.Error()
}

func (h *OAuthHandler) GetRefreshTokenErrResp(response string, accountSecret *AccountSecret) (errorType, message string) {
	if gjson.Get(response, ErrorType).String() != "" {
		errorType = gjson.Get(response, ErrorType).String()
		message = gjson.Get(response, "message").String()
	} else if err := json.Unmarshal([]byte(response), &accountSecret); err != nil {
		// Some problem with AccountSecret unmarshalling
		h.Logger.Debugn("Failed with error response", logger.NewErrorField(err))
		message = fmt.Sprintf("Unmarshal of response unsuccessful: %v", response)
		errorType = "unmarshallableResponse"
	} else if gjson.Get(response, "body.code").String() == RefTokenInvalidGrant {
		// User (or) AccessToken (or) RefreshToken has been revoked
		bodyMsg := gjson.Get(response, "body.message").String()
		if bodyMsg == "" {
			// Default message
			h.Logger.Debugn("Unable to get body.message", logger.NewStringField("Response", response))
			message = ErrPermissionOrTokenRevoked.Error()
		} else {
			message = bodyMsg
		}
		errorType = RefTokenInvalidGrant
	}
	return errorType, message
}

// This method hits the Control Plane to get the account information
// As well update the account information into the destAuthInfoMap(which acts as an in-memory cache)
func (h *OAuthHandler) fetchAccountInfoFromCp(refTokenParams *RefreshTokenParams, refTokenBody RefreshTokenBodyParams,
	authStats *OAuthStats, logTypeName string,
) (int, *AuthResponse, error) {
	refreshUrl := fmt.Sprintf("%s/destination/workspaces/%s/accounts/%s/token", configBEURL, refTokenParams.WorkspaceId, refTokenParams.AccountId)
	res, err := json.Marshal(refTokenBody)
	if err != nil {
		authStats.statName = GetOAuthActionStatName("failure")
		authStats.errorMessage = "error in marshalling refresh token body"
		authStats.SendCountStat()
		return http.StatusInternalServerError, nil, err
	}
	refreshCpReq := &ControlPlaneRequest{
		Method:        http.MethodPost,
		Url:           refreshUrl,
		ContentType:   "application/json; charset=utf-8",
		Body:          string(res),
		destName:      refTokenParams.DestDefName,
		RequestType:   authStats.action,
		BasicAuthUser: h.TokenProvider.Identity(),
	}
	var accountSecret AccountSecret
	// Stat for counting number of Refresh Token endpoint calls
	authStats.statName = GetOAuthActionStatName(`request_sent`)
	authStats.isCallToCpApi = true
	authStats.errorMessage = ""
	authStats.SendCountStat()

	cpiCallStartTime := time.Now()
	statusCode, response := h.CpConn.CpApiCall(refreshCpReq)
	authStats.statName = GetOAuthActionStatName(`request_latency`)
	authStats.SendTimerStats(cpiCallStartTime)

	h.Logger.Debugn("[request] :: Response from Control-Plane",
		logger.NewStringField("Module name", loggerNm),
		logger.NewIntField("StatusCode", int64(statusCode)),
		logger.NewIntField("WorkerId", int64(refTokenParams.WorkerId)),
		logger.NewStringField("Call Type", logTypeName))

	// Empty Refresh token response
	if !router_utils.IsNotEmptyString(response) {
		authStats.statName = GetOAuthActionStatName("failure")
		authStats.errorMessage = "Empty secret"
		authStats.SendCountStat()
		// Setting empty accessToken value into in-memory auth info map(cache)
		h.Logger.Debugn("Empty response from Control-Plane",
			logger.NewStringField("Modue Name", loggerNm),
			logger.NewStringField("Response", response),
			logger.NewIntField("WorkerId", int64(refTokenParams.WorkerId)),
			logger.NewStringField("Call Type", logTypeName))

		return http.StatusInternalServerError, nil, errors.New("empty secret")
	}

	if errType, refErrMsg := h.GetRefreshTokenErrResp(response, &accountSecret); router_utils.IsNotEmptyString(refErrMsg) {
		// potential oauth secret alert as we are not setting anything in the cache as secret
		authResponse := &AuthResponse{
			Err:          errType,
			ErrorMessage: refErrMsg,
		}
		authStats.statName = GetOAuthActionStatName("failure")
		authStats.errorMessage = refErrMsg
		authStats.SendCountStat()
		if authResponse.Err == RefTokenInvalidGrant {
			// Should abort the event as refresh is not going to work
			// until we have new refresh token for the account
			return http.StatusBadRequest, authResponse, fmt.Errorf("invalid grant")
		}
		return http.StatusInternalServerError, authResponse, fmt.Errorf("error occurred while fetching/refreshing account info from CP: %s", refErrMsg)
	}
	authStats.statName = GetOAuthActionStatName("success")
	authStats.errorMessage = ""
	authStats.SendCountStat()
	h.Logger.Debugn("[request] :: (Write) Account Secret received",
		logger.NewStringField("Modue Name", loggerNm),
		logger.NewStringField("Response", response),
		logger.NewIntField("WorkerId", int64(refTokenParams.WorkerId)),
		logger.NewStringField("Call Type", logTypeName))
	h.Cache.Set(refTokenParams.AccountId, &AuthResponse{
		Account: accountSecret,
	})
	return http.StatusOK, &AuthResponse{
		Account: accountSecret,
	}, nil
}
