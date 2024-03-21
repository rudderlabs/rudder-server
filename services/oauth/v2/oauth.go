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
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/controlplane"
)

type Authorizer interface {
	RefreshToken(refTokenParams *RefreshTokenParams) (int, *AuthResponse, error)
	FetchToken(fetchTokenParams *RefreshTokenParams) (int, *AuthResponse, error)
	AuthStatusToggle(authStatusToggleParams *AuthStatusToggleParams) (statusCode int, respBody string)
}

type OAuthHandler struct {
	stats stats.Stats
	TokenProvider
	Logger                    logger.Logger
	RudderFlowType            common.RudderFlow
	CpConn                    controlplane.Connector
	AuthStatusUpdateActiveMap map[string]bool // Used to check if a authStatusInactive request for a destination is already InProgress
	Cache                     Cache
	CacheMutex                *kitsync.PartitionRWLocker
	ExpirationTimeDiff        time.Duration
	ConfigBEURL               string
	cpConnectorTimeout        time.Duration
}

func WithCache(cache Cache) func(*OAuthHandler) {
	return func(h *OAuthHandler) {
		h.Cache = cache
	}
}

func WithLocker(lock *kitsync.PartitionRWLocker) func(*OAuthHandler) {
	return func(h *OAuthHandler) {
		h.CacheMutex = lock
	}
}

func WithExpirationTimeDiff(expirationTimeDiff time.Duration) func(*OAuthHandler) {
	return func(h *OAuthHandler) {
		h.ExpirationTimeDiff = expirationTimeDiff
	}
}

func WithLogger(parentLogger logger.Logger) func(*OAuthHandler) {
	return func(h *OAuthHandler) {
		h.Logger = parentLogger
	}
}

func WithCPConnectorTimeout(t time.Duration) func(*OAuthHandler) {
	return func(h *OAuthHandler) {
		h.cpConnectorTimeout = t
	}
}

func WithStats(stats stats.Stats) func(*OAuthHandler) {
	return func(h *OAuthHandler) {
		h.stats = stats
	}
}

func WithCpConnector(cpConn controlplane.Connector) func(*OAuthHandler) {
	return func(h *OAuthHandler) {
		h.CpConn = cpConn
	}
}

// NewOAuthHandler returns a new instance of OAuthHandler
func NewOAuthHandler(provider TokenProvider, options ...func(*OAuthHandler)) *OAuthHandler {
	h := &OAuthHandler{
		TokenProvider: provider,
		// This timeout is kind of modifiable & it seemed like 10 mins for this is too much!
		RudderFlowType:            common.RudderFlowDelivery,
		AuthStatusUpdateActiveMap: make(map[string]bool),
		ConfigBEURL:               backendconfig.GetConfigBackendURL(),
	}
	for _, opt := range options {
		opt(h)
	}
	if h.Logger == nil {
		h.Logger = logger.NewLogger()
	}
	h.Logger = h.Logger.Child("OAuthHandler")
	if h.stats == nil {
		h.stats = stats.Default
	}
	if h.CpConn == nil {
		h.CpConn = controlplane.NewConnector(config.Default,
			controlplane.WithCpClientTimeout(h.cpConnectorTimeout),
			controlplane.WithLogger(h.Logger),
			controlplane.WithStats(h.stats),
		)
	}
	if h.Cache == nil {
		h.Cache = NewCache()
	}
	if h.CacheMutex == nil {
		h.CacheMutex = kitsync.NewPartitionRWLocker()
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
*/
func (h *OAuthHandler) FetchToken(fetchTokenParams *RefreshTokenParams) (int, *AuthResponse, error) {
	authStats := &OAuthStats{
		stats:           h.stats,
		id:              fetchTokenParams.AccountID,
		workspaceID:     fetchTokenParams.WorkspaceID,
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
*/
func (h *OAuthHandler) RefreshToken(refTokenParams *RefreshTokenParams) (int, *AuthResponse, error) {
	authStats := &OAuthStats{
		id:              refTokenParams.AccountID,
		workspaceID:     refTokenParams.WorkspaceID,
		rudderCategory:  "destination",
		statName:        "",
		isCallToCpApi:   false,
		authErrCategory: common.CategoryRefreshToken,
		errorMessage:    "",
		destDefName:     refTokenParams.DestDefName,
		flowType:        h.RudderFlowType,
		action:          "refresh_token",
		stats:           h.stats,
	}
	return h.GetTokenInfo(refTokenParams, "Refresh token", authStats)
}

func (h *OAuthHandler) GetTokenInfo(refTokenParams *RefreshTokenParams, logTypeName string, authStats *OAuthStats) (int, *AuthResponse, error) {
	log := h.Logger.Withn(
		logger.NewStringField("Call Type", logTypeName),
		logger.NewStringField("AccountId", refTokenParams.AccountID),
		obskit.DestinationID(refTokenParams.Destination.ID),
		obskit.WorkspaceID(refTokenParams.WorkspaceID),
		obskit.DestinationType(refTokenParams.DestDefName),
	)
	log.Debugn("[request] :: Get Token Info request received")
	startTime := time.Now()
	defer func() {
		authStats.statName = GetOAuthActionStatName("total_latency")
		authStats.isCallToCpApi = false
		authStats.SendTimerStats(startTime)
	}()
	h.CacheMutex.Lock(refTokenParams.AccountID)
	defer h.CacheMutex.Unlock(refTokenParams.AccountID)
	refTokenBody := RefreshTokenBodyParams{}
	storedCache, ok := h.Cache.Load(refTokenParams.AccountID)
	if ok {
		cachedSecret, ok := storedCache.(*AuthResponse)
		if !ok {
			log.Debugn("[request] :: Failed to type assert the stored cache")
			return http.StatusInternalServerError, nil, errors.New("failed to type assert the stored cache")
		}
		// TODO: verify if the storedCache is nil at this point
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
	return statusCode, refSecret, refErr
}

func (h *OAuthHandler) AuthStatusToggle(params *AuthStatusToggleParams) (statusCode int, respBody string) {
	authErrHandlerTimeStart := time.Now()
	destinationId := params.Destination.ID
	action := fmt.Sprintf("auth_status_%v", params.StatPrefix)

	authStatusToggleStats := &OAuthStats{
		id:              destinationId,
		workspaceID:     params.WorkspaceID,
		rudderCategory:  "destination",
		statName:        "",
		isCallToCpApi:   false,
		authErrCategory: params.AuthStatus,
		errorMessage:    "",
		destDefName:     params.Destination.DefinitionName,
		flowType:        h.RudderFlowType,
		action:          action,
		stats:           h.stats,
	}
	defer func() {
		authStatusToggleStats.statName = GetOAuthActionStatName("total_latency")
		authStatusToggleStats.isCallToCpApi = false
		authStatusToggleStats.SendTimerStats(authErrHandlerTimeStart)
	}()
	h.CacheMutex.Lock(params.RudderAccountID)
	isAuthStatusUpdateActive, isAuthStatusUpdateReqPresent := h.AuthStatusUpdateActiveMap[destinationId]
	if isAuthStatusUpdateReqPresent && isAuthStatusUpdateActive {
		h.CacheMutex.Unlock(params.RudderAccountID)
		h.Logger.Debugn("[request] :: Received AuthStatusInactive request while another request is active!")
		return http.StatusConflict, ErrPermissionOrTokenRevoked.Error()
	}

	h.AuthStatusUpdateActiveMap[destinationId] = true
	h.CacheMutex.Unlock(params.RudderAccountID)

	defer func() {
		h.CacheMutex.Lock(params.RudderAccountID)
		h.AuthStatusUpdateActiveMap[destinationId] = false
		h.Logger.Debugn("[request] :: AuthStatusInactive request is inactive!")
		// After trying to inactivate authStatus for destination, need to remove existing accessToken(from in-memory cache)
		// This is being done to obtain new token after an update such as re-authorisation is done
		h.Cache.Delete(params.RudderAccountID)
		h.CacheMutex.Unlock(params.RudderAccountID)
	}()

	authStatusToggleUrl := fmt.Sprintf("%s/workspaces/%s/destinations/%s/authStatus/toggle", h.ConfigBEURL, params.WorkspaceID, destinationId)

	authStatusInactiveCpReq := &controlplane.Request{
		URL:           authStatusToggleUrl,
		Method:        http.MethodPut,
		Body:          fmt.Sprintf(`{"authStatus": "%v"}`, params.AuthStatus),
		ContentType:   "application/json",
		DestName:      params.Destination.DefinitionName,
		RequestType:   action,
		BasicAuthUser: h.Identity(),
	}
	authStatusToggleStats.statName = GetOAuthActionStatName("request_sent")
	authStatusToggleStats.isCallToCpApi = true
	authStatusToggleStats.SendCountStat()

	cpiCallStartTime := time.Now()
	statusCode, respBody = h.CpConn.CpApiCall(authStatusInactiveCpReq)
	authStatusToggleStats.statName = GetOAuthActionStatName("request_latency")
	authStatusToggleStats.SendTimerStats(cpiCallStartTime)
	h.Logger.Debugn("[request] :: Response from CP for auth status inactive req",
		logger.NewIntField("StatusCode", int64(statusCode)),
		logger.NewStringField("Response", respBody))

	var authStatusToggleRes *authStatusToggleResponse
	unmarshalErr := json.Unmarshal([]byte(respBody), &authStatusToggleRes)
	if routerutils.IsNotEmptyString(respBody) && (unmarshalErr != nil || !routerutils.IsNotEmptyString(authStatusToggleRes.Message) || statusCode != http.StatusOK) {
		var msg string
		if unmarshalErr != nil {
			msg = unmarshalErr.Error()
		} else {
			msg = fmt.Sprintf("Could not update authStatus to inactive for destination: %v", authStatusToggleRes.Message)
		}
		authStatusToggleStats.statName = GetOAuthActionStatName("request")
		authStatusToggleStats.errorMessage = msg
		authStatusToggleStats.SendCountStat()
		return http.StatusBadRequest, ErrPermissionOrTokenRevoked.Error()
	}
	h.Logger.Debugn("[request] :: (Write) auth status inactive Response received",
		logger.NewIntField("StatusCode", int64(statusCode)),
		logger.NewStringField("Response", respBody))
	authStatusToggleStats.statName = GetOAuthActionStatName("request")
	authStatusToggleStats.errorMessage = ""
	authStatusToggleStats.SendCountStat()

	return http.StatusBadRequest, ErrPermissionOrTokenRevoked.Error()
}

func (h *OAuthHandler) GetRefreshTokenErrResp(response string, accountSecret *AccountSecret) (errorType, message string) {
	if gjson.Get(response, common.ErrorType).String() != "" {
		// Network error
		errorType = gjson.Get(response, common.ErrorType).String()
		message = gjson.Get(response, "message").String()
	} else if err := json.Unmarshal([]byte(response), &accountSecret); err != nil {
		// Some problem with AccountSecret unmarshalling
		h.Logger.Debugn("Failed with error response", logger.NewErrorField(err))
		message = fmt.Sprintf("Unmarshal of response unsuccessful: %v", response)
		errorType = "unmarshallableResponse"
	} else if gjson.Get(response, "body.code").String() == common.RefTokenInvalidGrant {
		// User (or) AccessToken (or) RefreshToken has been revoked
		bodyMsg := gjson.Get(response, "body.message").String()
		if bodyMsg == "" {
			// Default message
			h.Logger.Debugn("Unable to get body.message", logger.NewStringField("Response", response))
			message = ErrPermissionOrTokenRevoked.Error()
		} else {
			message = bodyMsg
		}
		errorType = common.RefTokenInvalidGrant
	}
	return errorType, message
}

// This method hits the Control Plane to get the account information
// As well update the account information into the destAuthInfoMap(which acts as an in-memory cache)
func (h *OAuthHandler) fetchAccountInfoFromCp(refTokenParams *RefreshTokenParams, refTokenBody RefreshTokenBodyParams,
	authStats *OAuthStats, logTypeName string,
) (int, *AuthResponse, error) {
	refreshUrl := fmt.Sprintf("%s/destination/workspaces/%s/accounts/%s/token", h.ConfigBEURL, refTokenParams.WorkspaceID, refTokenParams.AccountID)
	res, err := json.Marshal(refTokenBody)
	if err != nil {
		authStats.statName = GetOAuthActionStatName("request")
		authStats.errorMessage = "error in marshalling refresh token body"
		authStats.SendCountStat()
		return http.StatusInternalServerError, nil, err
	}
	refreshCpReq := &controlplane.Request{
		Method:        http.MethodPost,
		URL:           refreshUrl,
		ContentType:   "application/json; charset=utf-8",
		Body:          string(res),
		DestName:      refTokenParams.DestDefName,
		RequestType:   authStats.action,
		BasicAuthUser: h.TokenProvider.Identity(),
	}
	var accountSecret AccountSecret
	// Stat for counting number of Refresh Token endpoint calls
	authStats.statName = GetOAuthActionStatName("request_sent")
	authStats.isCallToCpApi = true
	authStats.errorMessage = ""
	authStats.SendCountStat()

	cpiCallStartTime := time.Now()
	statusCode, response := h.CpConn.CpApiCall(refreshCpReq)
	authStats.statName = GetOAuthActionStatName("request_latency")
	authStats.SendTimerStats(cpiCallStartTime)

	log := h.Logger.Withn(logger.NewIntField("StatusCode", int64(statusCode)),
		logger.NewIntField("WorkerId", int64(refTokenParams.WorkerID)),
		logger.NewStringField("Call Type", logTypeName))
	log.Debugn("[request] :: Response from Control-Plane")

	// Empty Refresh token response
	if !routerutils.IsNotEmptyString(response) {
		authStats.statName = GetOAuthActionStatName("request")
		authStats.errorMessage = "Empty secret"
		authStats.SendCountStat()
		// Setting empty accessToken value into in-memory auth info map(cache)
		h.Logger.Debugn("Empty response from Control-Plane",
			logger.NewStringField("Response", response),
			logger.NewIntField("WorkerId", int64(refTokenParams.WorkerID)),
			logger.NewStringField("Call Type", logTypeName))

		return http.StatusInternalServerError, nil, errors.New("empty secret")
	}

	if errType, refErrMsg := h.GetRefreshTokenErrResp(response, &accountSecret); routerutils.IsNotEmptyString(refErrMsg) {
		// potential oauth secret alert as we are not setting anything in the cache as secret
		authResponse := &AuthResponse{
			Err:          errType,
			ErrorMessage: refErrMsg,
		}
		authStats.statName = GetOAuthActionStatName("request")
		authStats.errorMessage = errType
		authStats.SendCountStat()
		if authResponse.Err == common.RefTokenInvalidGrant {
			// Should abort the event as refresh is not going to work
			// until we have new refresh token for the account
			return http.StatusBadRequest, authResponse, fmt.Errorf("invalid grant")
		}
		return http.StatusInternalServerError, authResponse, fmt.Errorf("error occurred while fetching/refreshing account info from CP: %s", refErrMsg)
	}
	authStats.statName = GetOAuthActionStatName("request")
	authStats.errorMessage = ""
	authStats.SendCountStat()
	log.Debugn("[request] :: (Write) Account Secret received")
	// Store expirationDate information
	accountSecret.ExpirationDate = gjson.Get(response, "secret.expirationDate").String()
	h.Cache.Store(refTokenParams.AccountID, &AuthResponse{
		Account: accountSecret,
	})
	return http.StatusOK, &AuthResponse{
		Account: accountSecret,
	}, nil
}
