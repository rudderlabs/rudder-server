package v2

//go:generate mockgen -destination=../../../mocks/services/oauthV2/mock_oauthhandler.go -package=mock_oauthV2 github.com/rudderlabs/rudder-server/services/oauth/v2 OAuthHandler

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/controlplane"
)

// OAuthHandler is the interface that wraps the methods to fetch and refresh OAuth tokens. Handler is using in-memory cache to store the tokens
// and fetches from control plane only when the token is expired or not present in the cache.
type OAuthHandler interface {
	// FetchToken fetches the OAuth token for a given account ID
	FetchToken(params *OAuthTokenParams) (json.RawMessage, StatusCodeError)
	// RefreshToken refreshes the OAuth token for a given account ID. The previousSecret is used to check if the token has already been rotated or not.
	// Token refresh is skipped if the token has already been rotated and previous secret is returned instead.
	RefreshToken(params *OAuthTokenParams, previousSecret json.RawMessage) (json.RawMessage, StatusCodeError)
	// AuthStatusToggle toggles the auth status for a given destination ID.
	// Only one request is allowed at a time per destination ID, concurrent requests will return a 409 conflict error.
	AuthStatusToggle(params *StatusRequestParams) StatusCodeError
}

// WithCache sets the cache for the OAuthHandler
func WithCache(cache OauthTokenCache) func(*oauthHandler) {
	return func(h *oauthHandler) {
		h.cache = cache
	}
}

// WithLocker sets the locker for the OAuthHandler
func WithLocker(lock *kitsync.PartitionRWLocker) func(*oauthHandler) {
	return func(h *oauthHandler) {
		h.cacheMu = lock
	}
}

// WithRefreshBeforeExpiry sets the duration before expiry to refresh the token
func WithRefreshBeforeExpiry(refreshBeforeExpiry time.Duration) func(*oauthHandler) {
	return func(h *oauthHandler) {
		h.refreshBeforeExpiry = refreshBeforeExpiry
	}
}

// WithLogger sets the logger for the OAuthHandler
func WithLogger(parentLogger logger.Logger) func(*oauthHandler) {
	return func(h *oauthHandler) {
		h.logger = parentLogger
	}
}

// WithCPClientTimeout sets the control plane client timeout for the OAuthHandler
func WithCPClientTimeout(cpClientTimeout time.Duration) func(*oauthHandler) {
	return func(h *oauthHandler) {
		h.cpClientTimeout = cpClientTimeout
	}
}

// WithStats sets the stats client for the OAuthHandler
func WithStats(stats stats.Stats) func(*oauthHandler) {
	return func(h *oauthHandler) {
		h.stats = stats
	}
}

// WithCpClient sets the control plane client for the OAuthHandler
func WithCpClient(cpClient controlplane.Connector) func(*oauthHandler) {
	return func(h *oauthHandler) {
		h.cpClient = cpClient
	}
}

// NewOAuthHandler returns a new instance of OAuthHandler
func NewOAuthHandler(provider AuthIdentityProvider, options ...func(*oauthHandler)) OAuthHandler {
	h := &oauthHandler{
		AuthIdentityProvider: provider,
		rudderFlowType:       common.RudderFlowDelivery,
		cbeURL:               backendconfig.GetConfigBackendURL(),

		inflightStatusRequests: make(map[string]struct{}), // keeping track of inflight requests for auth status toggle (allow only one at a time per destination)
	}
	for _, opt := range options {
		opt(h)
	}
	if h.logger == nil {
		h.logger = logger.NewLogger() // default logger
	}
	h.logger = h.logger.Child("OAuthHandler")
	if h.stats == nil {
		h.stats = stats.Default // default stats
	}
	if h.cpClient == nil {
		// default control plane client
		h.cpClient = controlplane.NewConnector(config.Default,
			controlplane.WithCpClientTimeout(h.cpClientTimeout),
			controlplane.WithLogger(h.logger),
			controlplane.WithStats(h.stats),
		)
	}
	if h.cache == nil {
		h.cache = NewOauthTokenCache() // default in-memory cache
	}
	if h.cacheMu == nil {
		h.cacheMu = kitsync.NewPartitionRWLocker() // default locker
	}
	if h.refreshBeforeExpiry.Seconds() == 0 {
		h.refreshBeforeExpiry = 1 * time.Minute // default refresh before expiry duration
	}
	return h
}

// Implementation of OAuthHandler interface
type oauthHandler struct {
	stats stats.Stats
	AuthIdentityProvider
	logger         logger.Logger
	rudderFlowType common.RudderFlow
	cpClient       controlplane.Connector

	inflightStatusRequestsMu sync.Mutex
	inflightStatusRequests   map[string]struct{}

	cacheMu *kitsync.PartitionRWLocker
	cache   OauthTokenCache

	refreshBeforeExpiry time.Duration
	cbeURL              string
	cpClientTimeout     time.Duration
}

// FetchToken fetches the OAuth token for a given account ID
//
//   - If the token is present in the in-memory cache and is not expired, it returns the token from the cache
//   - If the token has expired, it refreshes the token from  control plane
//   - If the token is not present in the cache, it fetches the token from the control plane using a hint to refresh the token if it is expired
func (h *oauthHandler) FetchToken(params *OAuthTokenParams) (json.RawMessage, StatusCodeError) {
	const action = "fetch_token"
	authStats := &OAuthStats{
		stats:           h.stats,
		id:              params.AccountID,
		workspaceID:     params.WorkspaceID,
		rudderCategory:  "destination",
		statName:        "",
		isCallToCpApi:   false,
		authErrCategory: "",
		errorMessage:    "",
		destType:        params.DestType,
		flowType:        h.rudderFlowType,
		action:          action,
	}
	statshandler := NewStatsHandlerFromOAuthStats(authStats)
	return h.getToken(params, nil, action, statshandler)
}

// RefreshToken refreshes the OAuth token for a given account ID. The previousSecret is used to check if the token has already been rotated or not.
// Token refresh is skipped if the token has already been rotated and previous secret is returned instead.
//
//   - If the token is present in the in-memory cache and is not expired and it doesn't match the previous secret it returns the token from the cache
//   - If the token has expired, or it matches the previous secret it refreshes the token from control plane
//   - If the token is not present in the cache, it fetches the token from the control plane using a hint to refresh the token if it is expired
func (h *oauthHandler) RefreshToken(params *OAuthTokenParams, previousSecret json.RawMessage) (json.RawMessage, StatusCodeError) {
	const action = "refresh_token"
	authStats := &OAuthStats{
		stats:           h.stats,
		id:              params.AccountID,
		workspaceID:     params.WorkspaceID,
		rudderCategory:  "destination",
		statName:        "",
		isCallToCpApi:   false,
		authErrCategory: common.CategoryRefreshToken,
		errorMessage:    "",
		destType:        params.DestType,
		flowType:        h.rudderFlowType,
		action:          action,
	}
	statsHandler := NewStatsHandlerFromOAuthStats(authStats)
	return h.getToken(params, previousSecret, action, statsHandler)
}

func (h *oauthHandler) getToken(params *OAuthTokenParams, previousSecret json.RawMessage, action string, statsHandler OAuthStatsHandler) (json.RawMessage, StatusCodeError) {
	// Create a logger with all context fields
	log := h.logger.Withn(
		logger.NewStringField("action", action),
		logger.NewStringField("accountId", params.AccountID),
		obskit.DestinationID(params.DestinationID),
		obskit.WorkspaceID(params.WorkspaceID),
		obskit.DestinationType(params.DestType),
	)

	// Add a unique request ID to trace this specific request through logs
	requestID := uuid.New().String()
	log = log.Withn(logger.NewStringField("RequestID", requestID))

	log.Debugn("[request] :: Get Token Info request received")
	startTime := time.Now()

	log.Debugn("[request] :: Acquiring lock for account")
	lockStartTime := time.Now()
	h.cacheMu.Lock(params.AccountID)
	lockAcquisitionTime := time.Since(lockStartTime)

	log.Debugn("Lock acquisition took ",
		logger.NewDurationField("duration", lockAcquisitionTime))

	defer h.cacheMu.Unlock(params.AccountID)
	defer func() {
		log.Debugn("[request] :: Released lock for account")
		statsHandler.SendTiming(startTime, "total_latency", stats.Tags{
			"isCallToCpApi": "false",
		})
	}()

	refTokenBody := OauthTokenRequestBody{
		HasExpired:    true, // setting this to true as a hint so that the upstream service refreshes the token if it is past its expiry time
		ExpiredSecret: nil,  // we are not forcing upstream service to refresh the token by default, which will only happen if hasExpired is true and expiredSecret is equal to the upstream service's stored secret
	}
	if cached, ok := h.cache.Load(params.AccountID); ok {
		if !isOauthTokenExpired(cached, previousSecret, h.refreshBeforeExpiry, statsHandler) {
			return cached.Secret, nil
		}
		refTokenBody = OauthTokenRequestBody{ // forcing refresh of token as it is expired or about to expire, or previousSecret matches the cached secret
			HasExpired:    true,
			ExpiredSecret: cached.Secret,
		}
	}
	token, err := h.getTokenFromAPI(params, refTokenBody, statsHandler, action)
	if err != nil {
		return nil, err
	}
	return token.Secret, nil
}

// AuthStatusToggle toggles the auth status for a given destination ID.
// Only one request is allowed at a time per destination ID, concurrent requests will return a 409 conflict error.
func (h *oauthHandler) AuthStatusToggle(params *StatusRequestParams) StatusCodeError {
	authErrHandlerTimeStart := time.Now()
	action := fmt.Sprintf("auth_status_%v", params.Status)

	oauthStats := &OAuthStats{
		id:              params.DestinationID,
		workspaceID:     params.WorkspaceID,
		rudderCategory:  "destination",
		statName:        "",
		isCallToCpApi:   false,
		authErrCategory: params.Status,
		errorMessage:    "",
		destType:        params.DestType,
		flowType:        h.rudderFlowType,
		action:          action,
		stats:           h.stats,
	}
	statsHandler := NewStatsHandlerFromOAuthStats(oauthStats)
	h.inflightStatusRequestsMu.Lock()
	if _, ok := h.inflightStatusRequests[params.DestinationID]; ok {
		h.inflightStatusRequestsMu.Unlock()
		h.logger.Debugn("[request] :: Received AuthStatusInactive request while another request is active!")
		return NewStatusCodeError(http.StatusConflict, errors.New("another request for the same destination is active"))
	}
	h.inflightStatusRequests[params.DestinationID] = struct{}{}
	h.inflightStatusRequestsMu.Unlock()

	defer func() {
		h.inflightStatusRequestsMu.Lock()
		delete(h.inflightStatusRequests, params.DestinationID)
		h.inflightStatusRequestsMu.Unlock()
		h.logger.Debugn("[request] :: AuthStatusInactive request is inactive!")
		if params.Status == common.CategoryAuthStatusInactive {
			// After trying to inactivate authStatus for destination, need to remove existing accessToken(from in-memory cache)
			// This is being done to obtain new token after an update such as re-authorisation is done
			h.cacheMu.Lock(params.AccountID)
			h.cache.Delete(params.AccountID)
			h.cacheMu.Unlock(params.AccountID)
		}
	}()
	defer func() {
		statsHandler.SendTiming(authErrHandlerTimeStart, "total_latency", stats.Tags{
			"isCallToCpApi": "false",
		})
	}()

	url := fmt.Sprintf("%s/workspaces/%s/destinations/%s/authStatus/toggle", h.cbeURL, params.WorkspaceID, params.DestinationID)

	cpReq := &controlplane.Request{
		URL:          url,
		Method:       http.MethodPut,
		Body:         fmt.Sprintf(`{"authStatus": "%s"}`, params.Status),
		ContentType:  "application/json",
		DestType:     params.DestType,
		RequestType:  action,
		AuthIdentity: h.Identity(),
	}
	statsHandler.Increment("request_sent", stats.Tags{
		"isCallToCpApi": "true",
	})

	cpiCallStartTime := time.Now()
	statusCode, respBody := h.cpClient.CpApiCall(cpReq)
	statsHandler.SendTiming(cpiCallStartTime, "request_latency", stats.Tags{
		"isCallToCpApi": "true",
	})
	h.logger.Debugn("[request] :: Response from CP for auth status inactive req",
		logger.NewIntField("StatusCode", int64(statusCode)),
		logger.NewStringField("response", respBody))

	var err StatusCodeError
	var errorMessage string
	if statusCode != http.StatusOK {
		errorMessage = strconv.Itoa(statusCode)
		var errorBody struct {
			Message string `json:"message"`
		}
		if unmarshalErr := jsonrs.Unmarshal([]byte(respBody), &errorBody); unmarshalErr == nil && errorBody.Message != "" {
			err = NewStatusCodeError(statusCode, errors.New("message: "+errorBody.Message))
		} else {
			err = NewStatusCodeError(statusCode, errors.New("response: "+respBody))
		}
	}
	statsHandler.Increment("request", stats.Tags{"errorMessage": errorMessage, "isCallToCpApi": "true"})
	return err
}

func getRefreshTokenErrResp(response string, oauthToken *OAuthToken, l logger.Logger) (errorType, message string) {
	if gjson.Get(response, common.ErrorType).String() != "" { // TODO: is this indeed a valid case?
		errorType = gjson.Get(response, common.ErrorType).String()
		message = gjson.Get(response, "message").String()
	} else if err := jsonrs.Unmarshal([]byte(response), &oauthToken); err != nil {
		// Some problem with AccessToken unmarshalling
		l.Debugn("Failed with error response", obskit.Error(err))
		errorType = "unmarshallableResponse"
		message = fmt.Sprintf("Unmarshal of response unsuccessful: %s", response)
	} else {
		code := gjson.Get(response, "body.code").String()
		switch code {
		case common.RefTokenInvalidGrant:
			// User (or) AccessToken (or) RefreshToken has been revoked
			errorType = common.RefTokenInvalidGrant
			bodyMsg := gjson.Get(response, "body.message").String()
			if bodyMsg == "" {
				// Default message
				l.Debugn("Unable to get body.message", logger.NewStringField("response", response))
				message = ErrPermissionOrTokenRevoked.Error()
			} else {
				message = bodyMsg
			}
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

// This method hits the Control Plane to get the token
func (h *oauthHandler) getTokenFromAPI(params *OAuthTokenParams, body OauthTokenRequestBody, statsHandler OAuthStatsHandler, action string) (*OAuthToken, StatusCodeError) {
	url := fmt.Sprintf("%s/destination/workspaces/%s/accounts/%s/token", h.cbeURL, params.WorkspaceID, params.AccountID)
	resBody, err := jsonrs.Marshal(body)
	if err != nil {
		statsHandler.Increment("request", stats.Tags{
			"errorMessage": "error in marshalling refresh token body",
		})
		return nil, NewStatusCodeError(http.StatusInternalServerError, fmt.Errorf("marshalling request body: %w", err))
	}
	cpReq := &controlplane.Request{
		Method:       http.MethodPost,
		URL:          url,
		ContentType:  "application/json; charset=utf-8",
		Body:         string(resBody),
		DestType:     params.DestType,
		RequestType:  action,
		AuthIdentity: h.Identity(),
	}
	var oauthToken OAuthToken
	// Stat for counting number of Refresh Token endpoint calls
	statsHandler.Increment("request_sent", stats.Tags{
		"isCallToCpApi": "true",
		"errorMessage":  "",
	})

	cpiCallStartTime := time.Now()
	statusCode, response := h.cpClient.CpApiCall(cpReq)
	statsHandler.SendTiming(cpiCallStartTime, "request_latency", stats.Tags{
		"isCallToCpApi": "true",
	})

	log := h.logger.Withn(logger.NewIntField("StatusCode", int64(statusCode)),
		logger.NewStringField("action", action))
	log.Debugn("[request] :: Response from Control-Plane")

	// Empty Refresh token response
	if !routerutils.IsNotEmptyString(response) {
		statsHandler.Increment("request", stats.Tags{
			"errorMessage":  "Empty secret",
			"isCallToCpApi": "true",
		})
		// Setting empty accessToken value into in-memory auth info map(cache)
		h.logger.Debugn("Empty response from Control-Plane",
			logger.NewStringField("response", response),
			logger.NewStringField("action", action))
		return nil, NewStatusCodeError(http.StatusInternalServerError, errors.New("empty secret in response from control plane"))
	}

	if errType, errMessage := getRefreshTokenErrResp(response, &oauthToken, h.logger); routerutils.IsNotEmptyString(errMessage) {
		// potential oauth secret alert as we are not setting anything in the cache as secret
		statsHandler.Increment("request", stats.Tags{
			"errorMessage":  errType,
			"isCallToCpApi": "true",
		})
		statusCode := http.StatusInternalServerError
		if errType == common.RefTokenInvalidGrant {
			statusCode = http.StatusBadRequest
		}
		return nil, NewStatusCodeError(statusCode, &TypeMessageError{Type: errType, Message: errMessage})
	}

	if oauthToken.IsEmpty() {
		err := errors.New("empty secret received from CP")
		statsHandler.Increment("request", stats.Tags{
			"errorMessage":  err.Error(),
			"isCallToCpApi": "true",
		})
		return nil, NewStatusCodeError(http.StatusInternalServerError, err)
	}
	statsHandler.Increment("request", stats.Tags{
		"errorMessage":  "",
		"isCallToCpApi": "true",
	})
	log.Debugn("[request] :: (Write) Account Secret received")
	oauthToken.ExpirationDate = gjson.Get(response, "secret.expirationDate").String()
	h.cache.Store(params.AccountID, oauthToken)
	return &oauthToken, nil
}
