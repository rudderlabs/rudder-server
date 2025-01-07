package v2

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
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
		flowType:        h.RudderFlowType,
		action:          "fetch_token",
	}
	statshandler := NewStatsHandlerFromOAuthStats(authStats)
	return h.GetTokenInfo(fetchTokenParams, "Fetch token", statshandler)
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
	statsHandler := NewStatsHandlerFromOAuthStats(authStats)
	return h.GetTokenInfo(refTokenParams, "Refresh token", statsHandler)
}

func (h *OAuthHandler) GetTokenInfo(refTokenParams *RefreshTokenParams, logTypeName string, statsHandler OAuthStatsHandler) (int, *AuthResponse, error) {
	log := h.Logger.Withn(
		logger.NewStringField("Call Type", logTypeName),
		logger.NewStringField("AccountId", refTokenParams.AccountID),
		obskit.DestinationID(refTokenParams.Destination.ID),
		obskit.WorkspaceID(refTokenParams.WorkspaceID),
		obskit.DestinationType(refTokenParams.DestDefName),
	)
	log.Debugn("[request] :: Get Token Info request received")
	startTime := time.Now()
	h.CacheMutex.Lock(refTokenParams.AccountID)
	defer h.CacheMutex.Unlock(refTokenParams.AccountID)
	defer func() {
		statsHandler.SendTiming(startTime, "total_latency", stats.Tags{
			"isCallToCpApi": "false",
		})
	}()
	refTokenBody := RefreshTokenBodyParams{}
	storedCache, ok := h.Cache.Load(refTokenParams.AccountID)
	if ok {
		cachedSecret, ok := storedCache.(*AuthResponse)
		if !ok {
			log.Debugn("[request] :: Failed to type assert the stored cache")
			return http.StatusInternalServerError, nil, errors.New("failed to type assert the stored cache")
		}
		// TODO: verify if the storedCache is nil at this point
		if !checkIfTokenExpired(cachedSecret.Account, refTokenParams.Secret, h.ExpirationTimeDiff, statsHandler) {
			return http.StatusOK, cachedSecret, nil
		}
		// Refresh token preparation
		refTokenBody = RefreshTokenBodyParams{
			HasExpired:    true,
			ExpiredSecret: refTokenParams.Secret,
		}
	}
	statusCode, refSecret, refErr := h.fetchAccountInfoFromCp(refTokenParams, refTokenBody, statsHandler, logTypeName)
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
	statsHandler := NewStatsHandlerFromOAuthStats(authStatusToggleStats)
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
	defer func() {
		statsHandler.SendTiming(authErrHandlerTimeStart, "total_latency", stats.Tags{
			"isCallToCpApi": "false",
		})
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
	statsHandler.Increment("request_sent", stats.Tags{
		"isCallToCpApi": "true",
	})

	cpiCallStartTime := time.Now()
	statusCode, respBody = h.CpConn.CpApiCall(authStatusInactiveCpReq)
	statsHandler.SendTiming(cpiCallStartTime, "request_latency", stats.Tags{
		"isCallToCpApi": "true",
	})
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
		statsHandler.Increment("request", stats.Tags{
			"errorMessage":  msg,
			"isCallToCpApi": "true",
		})
		return http.StatusBadRequest, ErrPermissionOrTokenRevoked.Error()
	}
	h.Logger.Debugn("[request] :: (Write) auth status inactive Response received",
		logger.NewIntField("StatusCode", int64(statusCode)),
		logger.NewStringField("Response", respBody))

	statsHandler.Increment("request", stats.Tags{
		"errorMessage":  "",
		"isCallToCpApi": "true",
	})
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
	} else {
		code := gjson.Get(response, "body.code").String()
		switch code {
		case common.RefTokenInvalidGrant:
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
		case common.RefTokenInvalidResponse:
			errorType = code
			msg := gjson.Get(response, "body.message").String()
			if msg == "" {
				msg = "invalid refresh response"
			}
			message = msg
		}
	}
	return errorType, message
}

// This method hits the Control Plane to get the account information
// As well update the account information into the destAuthInfoMap(which acts as an in-memory cache)
func (h *OAuthHandler) fetchAccountInfoFromCp(refTokenParams *RefreshTokenParams, refTokenBody RefreshTokenBodyParams,
	statsHandler OAuthStatsHandler, logTypeName string,
) (int, *AuthResponse, error) {
	actionType := strings.Join(strings.Fields(strings.ToLower(logTypeName)), "_")
	refreshUrl := fmt.Sprintf("%s/destination/workspaces/%s/accounts/%s/token", h.ConfigBEURL, refTokenParams.WorkspaceID, refTokenParams.AccountID)
	res, err := json.Marshal(refTokenBody)
	if err != nil {
		statsHandler.Increment("request", stats.Tags{
			"errorMessage": "error in marshalling refresh token body",
		})
		return http.StatusInternalServerError, nil, err
	}
	refreshCpReq := &controlplane.Request{
		Method:        http.MethodPost,
		URL:           refreshUrl,
		ContentType:   "application/json; charset=utf-8",
		Body:          string(res),
		DestName:      refTokenParams.DestDefName,
		RequestType:   actionType,
		BasicAuthUser: h.TokenProvider.Identity(),
	}
	var accountSecret AccountSecret
	// Stat for counting number of Refresh Token endpoint calls
	statsHandler.Increment("request_sent", stats.Tags{
		"isCallToCpApi": "true",
		"errorMessage":  "",
	})

	cpiCallStartTime := time.Now()
	statusCode, response := h.CpConn.CpApiCall(refreshCpReq)
	statsHandler.SendTiming(cpiCallStartTime, "request_latency", stats.Tags{
		"isCallToCpApi": "true",
	})

	log := h.Logger.Withn(logger.NewIntField("StatusCode", int64(statusCode)),
		logger.NewIntField("WorkerId", int64(refTokenParams.WorkerID)),
		logger.NewStringField("Call Type", logTypeName))
	log.Debugn("[request] :: Response from Control-Plane")

	// Empty Refresh token response
	if !routerutils.IsNotEmptyString(response) {
		statsHandler.Increment("request", stats.Tags{
			"errorMessage":  "Empty secret",
			"isCallToCpApi": "true",
		})
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
		statsHandler.Increment("request", stats.Tags{
			"errorMessage":  errType,
			"isCallToCpApi": "true",
		})
		if authResponse.Err == common.RefTokenInvalidGrant {
			// Should abort the event as refresh is not going to work
			// until we have new refresh token for the account
			return http.StatusBadRequest, authResponse, fmt.Errorf("invalid grant")
		}
		return http.StatusInternalServerError, authResponse, fmt.Errorf("error occurred while fetching/refreshing account info from CP: %s", refErrMsg)
	}
	statsHandler.Increment("request", stats.Tags{
		"errorMessage":  "",
		"isCallToCpApi": "true",
	})
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
