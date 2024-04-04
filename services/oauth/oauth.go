package oauth

//go:generate mockgen -destination=../../mocks/services/oauth/mock_oauth.go -package=mocks_oauth github.com/rudderlabs/rudder-server/services/oauth Authorizer
import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	router_utils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type (
	AuthType   string
	RudderFlow string
)

const (
	OAuth           AuthType = "OAuth"
	InvalidAuthType AuthType = "InvalidAuthType"

	RudderFlow_Delivery RudderFlow = "delivery"
	RudderFlow_Delete   RudderFlow = "delete"

	DeleteAccountIdKey   = "rudderDeleteAccountId"
	DeliveryAccountIdKey = "rudderAccountId"

	AuthStatusInactive = "inactive"
)

type AccountSecret struct {
	ExpirationDate string          `json:"expirationDate"`
	Secret         json.RawMessage `json:"secret"`
}
type AuthResponse struct {
	Account      AccountSecret
	Err          string
	ErrorMessage string
}

type OAuthStats struct {
	id              string
	workspaceId     string
	errorMessage    string
	rudderCategory  string
	statName        string
	isCallToCpApi   bool
	authErrCategory string
	destDefName     string
	isTokenFetch    bool // This stats field is used to identify if a request to get token is arising from processor
	flowType        RudderFlow
	action          string // refresh_token, fetch_token, auth_status_toggle
}

type DisableDestinationResponse struct {
	Enabled       bool   `json:"enabled"`
	DestinationId string `json:"id"`
}

type AuthStatusToggleResponse struct {
	Message string `json:"message,omitempty"`
}

type AuthStatusToggleParams struct {
	Destination     *backendconfig.DestinationT
	WorkspaceId     string
	RudderAccountId string
	AuthStatus      string
}

type RefreshTokenParams struct {
	AccountId   string
	WorkspaceId string
	DestDefName string
	WorkerId    int
	Secret      json.RawMessage
}

// OAuthErrResHandler is the handle for this class
type OAuthErrResHandler struct {
	tr                        *http.Transport
	client                    *http.Client
	logger                    logger.Logger
	destLockMap               map[string]*sync.RWMutex // This mutex map is used for disable destination locking
	accountLockMap            map[string]*sync.RWMutex // This mutex map is used for refresh token locking
	lockMapWMutex             *sync.RWMutex            // This mutex is used to prevent concurrent writes in lockMap(s) mentioned in the struct
	destAuthInfoMap           map[string]*AuthResponse
	refreshActiveMap          map[string]bool // Used to check if a refresh request for an account is already InProgress
	authStatusUpdateActiveMap map[string]bool // Used to check if a authStatusInactive request for a destination is already InProgress
	tokenProvider             tokenProvider
	rudderFlowType            RudderFlow
}

type Authorizer interface {
	AuthStatusToggle(*AuthStatusToggleParams) (int, string)
	RefreshToken(refTokenParams *RefreshTokenParams) (int, *AuthResponse)
	FetchToken(fetchTokenParams *RefreshTokenParams) (int, *AuthResponse)
}

type ControlPlaneRequestT struct {
	Body        string
	ContentType string
	Url         string
	Method      string
	destName    string
	RequestType string // This is to add more refined stat tags
}

var (
	configBEURL string
	pkgLogger   logger.Logger
	loggerNm    string
)

const (
	REFRESH_TOKEN = "REFRESH_TOKEN"
	// Identifier to be sent from destination(during transformation/delivery)
	AUTH_STATUS_INACTIVE = "AUTH_STATUS_INACTIVE"

	// Identifier for invalid_grant or access_denied errors(during refreshing the token)
	REF_TOKEN_INVALID_GRANT = "ref_token_invalid_grant"
)

var ErrPermissionOrTokenRevoked = errors.New("problem with user permission or access/refresh token have been revoked")

// This struct only exists for marshalling and sending payload to control-plane
type RefreshTokenBodyParams struct {
	HasExpired    bool            `json:"hasExpired"`
	ExpiredSecret json.RawMessage `json:"expiredSecret"`
}

type tokenProvider interface {
	AccessToken() string
}

func Init() {
	configBEURL = backendconfig.GetConfigBackendURL()
	pkgLogger = logger.NewLogger().Child("router").Child("OAuthResponseHandler")
	loggerNm = "OAuthResponseHandler"
}

func GetAuthType(config map[string]interface{}) AuthType {
	var lookupErr *misc.MapLookupError
	var authValue interface{}
	if authValue, lookupErr = misc.NestedMapLookup(config, "auth", "type"); lookupErr != nil {
		return ""
	}
	authType, ok := authValue.(string)
	if !ok {
		return ""
	}
	return AuthType(authType)
}

// This function creates a new OauthErrorResponseHandler
func NewOAuthErrorHandler(provider tokenProvider, options ...func(*OAuthErrResHandler)) *OAuthErrResHandler {
	oAuthErrResHandler := &OAuthErrResHandler{
		tokenProvider: provider,
		logger:        pkgLogger,
		tr:            &http.Transport{},
		client:        &http.Client{Timeout: config.GetDuration("HttpClient.oauth.timeout", 30, time.Second)},
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
		opt(oAuthErrResHandler)
	}
	return oAuthErrResHandler
}

func GetAccountId(config map[string]interface{}, idKey string) string {
	if rudderAccountIdInterface, found := config[idKey]; found {
		if rudderAccountId, ok := rudderAccountIdInterface.(string); ok {
			return rudderAccountId
		}
	}
	return ""
}

func WithRudderFlow(rudderFlow RudderFlow) func(*OAuthErrResHandler) {
	return func(authErrHandle *OAuthErrResHandler) {
		authErrHandle.rudderFlowType = rudderFlow
	}
}

func WithOAuthClientTimeout(timeout time.Duration) func(*OAuthErrResHandler) {
	return func(authErrHandle *OAuthErrResHandler) {
		authErrHandle.client.Timeout = timeout
	}
}

func (authErrHandler *OAuthErrResHandler) RefreshToken(refTokenParams *RefreshTokenParams) (int, *AuthResponse) {
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

func getOAuthActionStatName(stat string) string {
	return fmt.Sprintf("oauth_action_%v", stat)
}

func (authErrHandler *OAuthErrResHandler) FetchToken(fetchTokenParams *RefreshTokenParams) (int, *AuthResponse) {
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
		flowType:        authErrHandler.rudderFlowType,
		action:          "fetch_token",
	}
	return authErrHandler.GetTokenInfo(fetchTokenParams, "Fetch token", authStats)
}

func (authErrHandler *OAuthErrResHandler) GetTokenInfo(refTokenParams *RefreshTokenParams, logTypeName string, authStats *OAuthStats) (int, *AuthResponse) {
	startTime := time.Now()
	defer func() {
		authStats.statName = getOAuthActionStatName("total_latency")
		authStats.isCallToCpApi = false
		authStats.SendTimerStats(startTime)
	}()

	accountMutex := authErrHandler.getKeyMutex(authErrHandler.accountLockMap, refTokenParams.AccountId)
	refTokenBody := RefreshTokenBodyParams{}
	if router_utils.IsNotEmptyString(string(refTokenParams.Secret)) {
		refTokenBody = RefreshTokenBodyParams{
			HasExpired:    true,
			ExpiredSecret: refTokenParams.Secret,
		}
	}
	accountMutex.RLock()
	refVal, ok := authErrHandler.destAuthInfoMap[refTokenParams.AccountId]
	if ok {
		isInvalidAccountSecretForRefresh := router_utils.IsNotEmptyString(string(refVal.Account.Secret)) &&
			!bytes.Equal(refVal.Account.Secret, refTokenParams.Secret)
		if isInvalidAccountSecretForRefresh {
			accountMutex.RUnlock()
			authErrHandler.logger.Debugf("[%s request] [Cache] :: (Read) %s response received(rt-worker-%d): %s\n", loggerNm, logTypeName, refTokenParams.WorkerId, refVal.Account.Secret)
			return http.StatusOK, refVal
		}
	}
	accountMutex.RUnlock()

	accountMutex.Lock()
	if isRefreshActive, isPresent := authErrHandler.refreshActiveMap[refTokenParams.AccountId]; isPresent && isRefreshActive {
		accountMutex.Unlock()
		if refVal != nil {
			secret := refVal.Account.Secret
			authErrHandler.logger.Debugf("[%s request] [Active] :: (Read) %s response received from cache(rt-worker-%d): %s\n", loggerNm, logTypeName, refTokenParams.WorkerId, string(secret))
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
	authErrHandler.refreshActiveMap[refTokenParams.AccountId] = true
	authErrHandler.logger.Debugf("[%s request] [rt-worker-%v] :: %v request is active!", loggerNm, logTypeName, refTokenParams.WorkerId)
	accountMutex.Unlock()

	defer func() {
		accountMutex.Lock()
		authErrHandler.refreshActiveMap[refTokenParams.AccountId] = false
		authErrHandler.logger.Debugf("[%s request] [rt-worker-%v]:: %v request is inactive!", loggerNm, logTypeName, refTokenParams.WorkerId)
		accountMutex.Unlock()
	}()

	authErrHandler.logger.Debugf("[%s] [%v request] Lock Acquired by rt-worker-%d\n", loggerNm, logTypeName, refTokenParams.WorkerId)

	statusCode := authErrHandler.fetchAccountInfoFromCp(refTokenParams, refTokenBody, authStats, logTypeName)
	return statusCode, authErrHandler.destAuthInfoMap[refTokenParams.AccountId]
}

// This method hits the Control Plane to get the account information
// As well update the account information into the destAuthInfoMap(which acts as an in-memory cache)
func (authErrHandler *OAuthErrResHandler) fetchAccountInfoFromCp(refTokenParams *RefreshTokenParams, refTokenBody RefreshTokenBodyParams,
	authStats *OAuthStats, logTypeName string,
) (statusCode int) {
	refreshUrl := fmt.Sprintf("%s/destination/workspaces/%s/accounts/%s/token", configBEURL, refTokenParams.WorkspaceId, refTokenParams.AccountId)
	res, err := json.Marshal(refTokenBody)
	if err != nil {
		panic(err)
	}
	refreshCpReq := &ControlPlaneRequestT{
		Method:      http.MethodPost,
		Url:         refreshUrl,
		ContentType: "application/json; charset=utf-8",
		Body:        string(res),
		destName:    refTokenParams.DestDefName,
		RequestType: authStats.action,
	}
	var accountSecret AccountSecret
	// Stat for counting number of Refresh Token endpoint calls
	authStats.statName = getOAuthActionStatName(`request_sent`)
	authStats.isCallToCpApi = true
	authStats.errorMessage = ""
	authStats.SendCountStat()

	cpiCallStartTime := time.Now()
	statusCode, response := authErrHandler.cpApiCall(refreshCpReq)
	authStats.statName = getOAuthActionStatName(`request_latency`)
	authStats.SendTimerStats(cpiCallStartTime)

	authErrHandler.logger.Debugf("[%s] Got the response from Control-Plane: rt-worker-%d with statusCode: %d\n", loggerNm, refTokenParams.WorkerId, statusCode)

	// Empty Refresh token response
	if !router_utils.IsNotEmptyString(response) {
		authStats.statName = getOAuthActionStatName("failure")
		authStats.errorMessage = "Empty secret"
		authStats.SendCountStat()
		// Setting empty accessToken value into in-memory auth info map(cache)
		authErrHandler.destAuthInfoMap[refTokenParams.AccountId] = &AuthResponse{
			Account: AccountSecret{
				Secret: []byte(""),
			},
			Err: "Empty secret",
		}
		authErrHandler.logger.Debugf("[%s request] :: Empty %s response received(rt-worker-%d) : %s\n", loggerNm, logTypeName, refTokenParams.WorkerId, response)
		return http.StatusInternalServerError
	}

	if errType, refErrMsg := authErrHandler.getRefreshTokenErrResp(response, &accountSecret); router_utils.IsNotEmptyString(refErrMsg) {
		if _, ok := authErrHandler.destAuthInfoMap[refTokenParams.AccountId]; !ok {
			authErrHandler.destAuthInfoMap[refTokenParams.AccountId] = &AuthResponse{
				Err:          errType,
				ErrorMessage: refErrMsg,
			}
		} else {
			authErrHandler.destAuthInfoMap[refTokenParams.AccountId].Err = errType
			authErrHandler.destAuthInfoMap[refTokenParams.AccountId].ErrorMessage = refErrMsg
		}
		authStats.statName = getOAuthActionStatName("failure")
		authStats.errorMessage = refErrMsg
		authStats.SendCountStat()
		if refErrMsg == REF_TOKEN_INVALID_GRANT {
			// Should abort the event as refresh is not going to work
			// until we have new refresh token for the account
			return http.StatusBadRequest
		}
		return http.StatusInternalServerError
	}
	// Update the refreshed account information into in-memory map(cache)
	authErrHandler.destAuthInfoMap[refTokenParams.AccountId] = &AuthResponse{
		Account: accountSecret,
	}
	authStats.statName = getOAuthActionStatName("success")
	authStats.errorMessage = ""
	authStats.SendCountStat()
	authErrHandler.logger.Debugf("[%s request] :: (Write) %s response received(rt-worker-%d): %s\n", loggerNm, logTypeName, refTokenParams.WorkerId, response)
	return http.StatusOK
}

func (authErrHandler *OAuthErrResHandler) getRefreshTokenErrResp(response string, accountSecret *AccountSecret) (errorType, message string) {
	if err := json.Unmarshal([]byte(response), &accountSecret); err != nil {
		// Some problem with AccountSecret unmarshalling
		message = fmt.Sprintf("Unmarshal of response unsuccessful: %v", response)
		errorType = "unmarshallableResponse"
	} else if gjson.Get(response, "body.code").String() == REF_TOKEN_INVALID_GRANT {
		// User (or) AccessToken (or) RefreshToken has been revoked
		bodyMsg := gjson.Get(response, "body.message").String()
		if bodyMsg == "" {
			// Default message
			authErrHandler.logger.Debugf("Failed with error response: %v\n", response)
			message = ErrPermissionOrTokenRevoked.Error()
		} else {
			message = bodyMsg
		}
		errorType = REF_TOKEN_INVALID_GRANT
	}
	return errorType, message
}

func (authStats *OAuthStats) SendTimerStats(startTime time.Time) {
	statsTags := stats.Tags{
		"id":              authStats.id,
		"workspaceId":     authStats.workspaceId,
		"rudderCategory":  authStats.rudderCategory,
		"isCallToCpApi":   strconv.FormatBool(authStats.isCallToCpApi),
		"authErrCategory": authStats.authErrCategory,
		"destType":        authStats.destDefName,
		"flowType":        string(authStats.flowType),
		"action":          authStats.action,
	}
	stats.Default.NewTaggedStat(authStats.statName, stats.TimerType, statsTags).SendTiming(time.Since(startTime))
}

// Send count type stats related to OAuth(Destination)
func (refStats *OAuthStats) SendCountStat() {
	statsTags := stats.Tags{
		"id":              refStats.id,
		"workspaceId":     refStats.workspaceId,
		"rudderCategory":  refStats.rudderCategory,
		"errorMessage":    refStats.errorMessage,
		"isCallToCpApi":   strconv.FormatBool(refStats.isCallToCpApi),
		"authErrCategory": refStats.authErrCategory,
		"destType":        refStats.destDefName,
		"isTokenFetch":    strconv.FormatBool(refStats.isTokenFetch),
		"flowType":        string(refStats.flowType),
		"action":          refStats.action,
	}
	stats.Default.NewTaggedStat(refStats.statName, stats.CountType, statsTags).Increment()
}

func (authErrHandler *OAuthErrResHandler) AuthStatusToggle(params *AuthStatusToggleParams) (statusCode int, respBody string) {
	authErrHandlerTimeStart := time.Now()
	destinationId := params.Destination.ID
	authStatusToggleMutex := authErrHandler.getKeyMutex(authErrHandler.destLockMap, destinationId)
	action := fmt.Sprintf("auth_status_%v", params.AuthStatus)

	authStatusToggleStats := &OAuthStats{
		id:              destinationId,
		workspaceId:     params.WorkspaceId,
		rudderCategory:  "destination",
		statName:        "",
		isCallToCpApi:   false,
		authErrCategory: AUTH_STATUS_INACTIVE,
		errorMessage:    "",
		destDefName:     params.Destination.DestinationDefinition.Name,
		flowType:        authErrHandler.rudderFlowType,
		action:          action,
	}
	defer func() {
		authStatusToggleStats.statName = getOAuthActionStatName("total_latency")
		authStatusToggleStats.isCallToCpApi = false
		authStatusToggleStats.SendTimerStats(authErrHandlerTimeStart)
	}()

	authStatusToggleMutex.Lock()
	isAuthStatusUpdateActive, isAuthStatusUpdateReqPresent := authErrHandler.authStatusUpdateActiveMap[destinationId]
	authStatusUpdateActiveReq := strconv.FormatBool(isAuthStatusUpdateReqPresent && isAuthStatusUpdateActive)
	if isAuthStatusUpdateReqPresent && isAuthStatusUpdateActive {
		authStatusToggleMutex.Unlock()
		authErrHandler.logger.Debugf("[%s request] :: AuthStatusInactive request Active : %s\n", loggerNm, authStatusUpdateActiveReq)
		return http.StatusConflict, ErrPermissionOrTokenRevoked.Error()
	}

	authErrHandler.authStatusUpdateActiveMap[destinationId] = true
	authStatusToggleMutex.Unlock()

	defer func() {
		authStatusToggleMutex.Lock()
		authErrHandler.authStatusUpdateActiveMap[destinationId] = false
		authErrHandler.logger.Debugf("[%s request] :: AuthStatusInactive request is inactive!", loggerNm)
		authStatusToggleMutex.Unlock()
		// After trying to inactivate authStatus for destination, need to remove existing accessToken(from in-memory cache)
		// This is being done to obtain new token after an update such as re-authorisation is done
		accountMutex := authErrHandler.getKeyMutex(authErrHandler.accountLockMap, params.RudderAccountId)
		accountMutex.Lock()
		delete(authErrHandler.destAuthInfoMap, params.RudderAccountId)
		accountMutex.Unlock()
	}()

	authStatusToggleUrl := fmt.Sprintf("%s/workspaces/%s/destinations/%s/authStatus/toggle", configBEURL, params.WorkspaceId, destinationId)

	authStatusInactiveCpReq := &ControlPlaneRequestT{
		Url:         authStatusToggleUrl,
		Method:      http.MethodPut,
		Body:        fmt.Sprintf(`{"authStatus": "%v"}`, params.AuthStatus),
		ContentType: "application/json",
		destName:    params.Destination.DestinationDefinition.Name,
		RequestType: action,
	}

	authStatusToggleStats.statName = getOAuthActionStatName("request_sent")
	authStatusToggleStats.isCallToCpApi = true
	authStatusToggleStats.SendCountStat()

	cpiCallStartTime := time.Now()
	statusCode, respBody = authErrHandler.cpApiCall(authStatusInactiveCpReq)
	authStatusToggleStats.statName = getOAuthActionStatName("request_latency")
	defer authStatusToggleStats.SendTimerStats(cpiCallStartTime)
	authErrHandler.logger.Errorf(`Response from CP(stCd: %v) for auth status inactive req: %v`, statusCode, respBody)

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

	authErrHandler.logger.Errorf("[%s request] :: (Write) auth status inactive Response received : %s\n", loggerNm, respBody)
	authStatusToggleStats.statName = getOAuthActionStatName("success")
	authStatusToggleStats.errorMessage = ""
	authStatusToggleStats.SendCountStat()

	return http.StatusBadRequest, ErrPermissionOrTokenRevoked.Error()
}

func processResponse(resp *http.Response) (statusCode int, respBody string) {
	var respData []byte
	var ioUtilReadErr error
	if resp != nil && resp.Body != nil {
		respData, ioUtilReadErr = io.ReadAll(resp.Body)
		defer func() { httputil.CloseResponse(resp) }()
		if ioUtilReadErr != nil {
			return http.StatusInternalServerError, ioUtilReadErr.Error()
		}
	}
	// Detecting content type of the respData
	contentTypeHeader := strings.ToLower(http.DetectContentType(respData))
	// If content type is not of type "*text*", overriding it with empty string
	if !(strings.Contains(contentTypeHeader, "text") ||
		strings.Contains(contentTypeHeader, "application/json") ||
		strings.Contains(contentTypeHeader, "application/xml")) {
		respData = []byte("")
	}

	return resp.StatusCode, string(respData)
}

func (authErrHandler *OAuthErrResHandler) cpApiCall(cpReq *ControlPlaneRequestT) (int, string) {
	cpStatTags := stats.Tags{
		"url":         cpReq.Url,
		"requestType": cpReq.RequestType,
		"destType":    cpReq.destName,
		"method":      cpReq.Method,
		"flowType":    string(authErrHandler.rudderFlowType),
	}

	var reqBody *bytes.Buffer
	var req *http.Request
	var err error
	if router_utils.IsNotEmptyString(cpReq.Body) {
		reqBody = bytes.NewBufferString(cpReq.Body)
		req, err = http.NewRequest(cpReq.Method, cpReq.Url, reqBody)
	} else {
		req, err = http.NewRequest(cpReq.Method, cpReq.Url, http.NoBody)
	}
	if err != nil {
		authErrHandler.logger.Errorf("[%s request] :: destination request failed: %+v\n", loggerNm, err)
		// Abort on receiving an error in request formation
		return http.StatusBadRequest, err.Error()
	}
	// Authorisation setting
	req.SetBasicAuth(authErrHandler.tokenProvider.AccessToken(), "")

	// Set content-type in order to send the body in request correctly
	if router_utils.IsNotEmptyString(cpReq.ContentType) {
		req.Header.Set("Content-Type", cpReq.ContentType)
	}

	cpApiDoTimeStart := time.Now()
	res, doErr := authErrHandler.client.Do(req)
	stats.Default.NewTaggedStat("cp_request_latency", stats.TimerType, cpStatTags).SendTiming(time.Since(cpApiDoTimeStart))
	authErrHandler.logger.Debugf("[%s request] :: destination request sent\n", loggerNm)
	if doErr != nil {
		// Abort on receiving an error
		authErrHandler.logger.Errorf("[%s request] :: destination request failed: %+v\n", loggerNm, doErr)
		if os.IsTimeout(doErr) {
			stats.Default.NewTaggedStat("cp_request_timeout", stats.CountType, cpStatTags)
		}
		return http.StatusBadRequest, doErr.Error()
	}
	defer func() { httputil.CloseResponse(res) }()
	statusCode, resp := processResponse(res)
	return statusCode, resp
}

func (resHandler *OAuthErrResHandler) getKeyMutex(mutexMap map[string]*sync.RWMutex, id string) *sync.RWMutex {
	resHandler.lockMapWMutex.Lock()
	defer resHandler.lockMapWMutex.Unlock()
	// mutexMap will not be nil
	if _, ok := mutexMap[id]; !ok {
		resHandler.logger.Debugf("[%s request] :: Creating new mutex for %s\n", loggerNm, id)
		mutexMap[id] = &sync.RWMutex{}
	}
	return mutexMap[id]
}
