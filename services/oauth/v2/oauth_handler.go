package v2

//go:generate mockgen -destination=../../../mocks/services/oauthV2/mock_oauthhandler.go -package=mock_oauthV2 github.com/rudderlabs/rudder-server/services/oauth/v2 OAuthHandler

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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

// WithOauthBreakerOptions sets the OAuth breaker options for the OAuthHandler
func WithOauthBreakerOptions(options *OAuthBreakerOptions) func(*oauthHandler) {
	return func(h *oauthHandler) {
		h.breakerOptions = options
	}
}

func WithConfigBackendURL(url string) func(*oauthHandler) {
	return func(h *oauthHandler) {
		h.cbeURL = url
	}
}

// NewOAuthHandler returns a new instance of OAuthHandler
func NewOAuthHandler(provider AuthIdentityProvider, options ...func(*oauthHandler)) OAuthHandler {
	h := &oauthHandler{
		AuthIdentityProvider: provider,
		rudderFlowType:       common.RudderFlowDelivery,
		cbeURL:               backendconfig.GetConfigBackendURL(),
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

	// If breaker options are provided, wrap the handler with the breaker decorator
	if h.breakerOptions != nil {
		if h.breakerOptions.Stats == nil {
			h.breakerOptions.Stats = h.stats
		}
		return newOAuthBreaker(h, *h.breakerOptions)
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

	cacheMu *kitsync.PartitionRWLocker
	cache   OauthTokenCache

	refreshBeforeExpiry time.Duration
	cbeURL              string
	cpClientTimeout     time.Duration
	breakerOptions      *OAuthBreakerOptions
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
