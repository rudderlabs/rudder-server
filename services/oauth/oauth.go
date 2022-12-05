package oauth

//go:generate mockgen -destination=../../mocks/services/oauth/mock_oauth.go -package=mocks_oauth github.com/rudderlabs/rudder-server/services/oauth Authorizer
import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	router_utils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/tidwall/gjson"
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
)

type AccountSecret struct {
	ExpirationDate string          `json:"expirationDate"`
	Secret         json.RawMessage `json:"secret"`
}
type AuthResponse struct {
	Account AccountSecret
	Err     string
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
}

type DisableDestinationResponse struct {
	Enabled       bool   `json:"enabled"`
	DestinationId string `json:"id"`
}

type RefreshTokenParams struct {
	AccountId       string
	WorkspaceId     string
	DestDefName     string
	EventNamePrefix string
	WorkerId        int
	Secret          json.RawMessage
}

// OAuthErrResHandler is the handle for this class
type OAuthErrResHandler struct {
	tr                   *http.Transport
	client               *http.Client
	logger               logger.Logger
	destLockMap          map[string]*sync.RWMutex // This mutex map is used for disable destination locking
	accountLockMap       map[string]*sync.RWMutex // This mutex map is used for refresh token locking
	lockMapWMutex        *sync.RWMutex            // This mutex is used to prevent concurrent writes in lockMap(s) mentioned in the struct
	destAuthInfoMap      map[string]*AuthResponse
	refreshActiveMap     map[string]bool // Used to check if a refresh request for an account is already InProgress
	disableDestActiveMap map[string]bool // Used to check if a disable destination request for a destination is already InProgress
	tokenProvider        tokenProvider
	rudderFlowType       RudderFlow
}

type Authorizer interface {
	DisableDestination(destination *backendconfig.DestinationT, workspaceId, rudderAccountId string) (statusCode int, resBody string)
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
	DISABLE_DEST                = "DISABLE_DESTINATION"
	REFRESH_TOKEN               = "REFRESH_TOKEN"
	INVALID_REFRESH_TOKEN_GRANT = "refresh_token_invalid_grant"
)

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
	var lookupErr error
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
		destLockMap:          make(map[string]*sync.RWMutex),
		accountLockMap:       make(map[string]*sync.RWMutex),
		lockMapWMutex:        &sync.RWMutex{},
		destAuthInfoMap:      make(map[string]*AuthResponse),
		refreshActiveMap:     make(map[string]bool),
		disableDestActiveMap: make(map[string]bool),
		rudderFlowType:       RudderFlow_Delivery,
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
	}
	return authErrHandler.GetTokenInfo(refTokenParams, "Refresh token", authStats)
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
	}
	return authErrHandler.GetTokenInfo(fetchTokenParams, "Fetch token", authStats)
}

func (authErrHandler *OAuthErrResHandler) GetTokenInfo(refTokenParams *RefreshTokenParams, logTypeName string, authStats *OAuthStats) (int, *AuthResponse) {
	startTime := time.Now()
	defer func() {
		authStats.statName = fmt.Sprintf("%v_total_req_latency", refTokenParams.EventNamePrefix)
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

	errHandlerReqTimeStart := time.Now()
	defer func() {
		authStats.statName = fmt.Sprintf("%v_request_exec_time", refTokenParams.EventNamePrefix)
		authStats.isCallToCpApi = true
		authStats.SendTimerStats(errHandlerReqTimeStart)
	}()

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
		RequestType: logTypeName,
	}
	var accountSecret AccountSecret
	// Stat for counting number of Refresh Token endpoint calls
	authStats.statName = fmt.Sprintf(`%v_request_sent`, refTokenParams.EventNamePrefix)
	authStats.isCallToCpApi = true
	authStats.errorMessage = ""
	authStats.SendCountStat()

	cpiCallStartTime := time.Now()
	statusCode, response := authErrHandler.cpApiCall(refreshCpReq)
	authStats.statName = fmt.Sprintf(`%v_request_latency`, refTokenParams.EventNamePrefix)
	authStats.SendTimerStats(cpiCallStartTime)

	authErrHandler.logger.Debugf("[%s] Got the response from Control-Plane: rt-worker-%d with statusCode: %d\n", loggerNm, refTokenParams.WorkerId, statusCode)

	// Empty Refresh token response
	if !router_utils.IsNotEmptyString(response) {
		authStats.statName = fmt.Sprintf("%s_failure", refTokenParams.EventNamePrefix)
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

	if refErrMsg := getRefreshTokenErrResp(response, &accountSecret); router_utils.IsNotEmptyString(refErrMsg) {
		if _, ok := authErrHandler.destAuthInfoMap[refTokenParams.AccountId]; !ok {
			authErrHandler.destAuthInfoMap[refTokenParams.AccountId] = &AuthResponse{
				Err: refErrMsg,
			}
		} else {
			authErrHandler.destAuthInfoMap[refTokenParams.AccountId].Err = refErrMsg
		}
		authStats.statName = fmt.Sprintf("%s_failure", refTokenParams.EventNamePrefix)
		authStats.errorMessage = refErrMsg
		authStats.SendCountStat()
		if refErrMsg == INVALID_REFRESH_TOKEN_GRANT {
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
	authStats.statName = fmt.Sprintf("%s_success", refTokenParams.EventNamePrefix)
	authStats.errorMessage = ""
	authStats.SendCountStat()
	authErrHandler.logger.Debugf("[%s request] :: (Write) %s response received(rt-worker-%d): %s\n", loggerNm, logTypeName, refTokenParams.WorkerId, response)
	return http.StatusOK
}

func getRefreshTokenErrResp(response string, accountSecret *AccountSecret) (message string) {
	if err := json.Unmarshal([]byte(response), &accountSecret); err != nil {
		// Some problem with AccountSecret unmarshalling
		message = fmt.Sprintf("Unmarshal of response unsuccessful: %v", response)
	} else if gjson.Get(response, "body.code").String() == INVALID_REFRESH_TOKEN_GRANT {
		// User (or) AccessToken (or) RefreshToken has been revoked
		message = INVALID_REFRESH_TOKEN_GRANT
	}
	return message
}

func (authStats *OAuthStats) SendTimerStats(startTime time.Time) {
	stats.Default.NewTaggedStat(authStats.statName, stats.TimerType, stats.Tags{
		"id":              authStats.id,
		"workspaceId":     authStats.workspaceId,
		"rudderCategory":  authStats.rudderCategory,
		"isCallToCpApi":   strconv.FormatBool(authStats.isCallToCpApi),
		"authErrCategory": authStats.authErrCategory,
		"destType":        authStats.destDefName,
		"flowType":        string(authStats.flowType),
	}).SendTiming(time.Since(startTime))
}

// Send count type stats related to OAuth(Destination)
func (refStats *OAuthStats) SendCountStat() {
	stats.Default.NewTaggedStat(refStats.statName, stats.CountType, stats.Tags{
		"id":              refStats.id,
		"workspaceId":     refStats.workspaceId,
		"rudderCategory":  refStats.rudderCategory,
		"errorMessage":    refStats.errorMessage,
		"isCallToCpApi":   strconv.FormatBool(refStats.isCallToCpApi),
		"authErrCategory": refStats.authErrCategory,
		"destType":        refStats.destDefName,
		"isTokenFetch":    strconv.FormatBool(refStats.isTokenFetch),
		"flowType":        string(refStats.flowType),
	}).Increment()
}

func (authErrHandler *OAuthErrResHandler) DisableDestination(destination *backendconfig.DestinationT, workspaceId, rudderAccountId string) (statusCode int, respBody string) {
	authErrHandlerTimeStart := time.Now()
	destinationId := destination.ID
	disableDestMutex := authErrHandler.getKeyMutex(authErrHandler.destLockMap, destinationId)

	disableDestStats := &OAuthStats{
		id:              destinationId,
		workspaceId:     workspaceId,
		rudderCategory:  "destination",
		statName:        "",
		isCallToCpApi:   false,
		authErrCategory: DISABLE_DEST,
		errorMessage:    "",
		destDefName:     destination.DestinationDefinition.Name,
		flowType:        authErrHandler.rudderFlowType,
	}
	defer func() {
		disableDestStats.statName = "disable_destination_total_req_latency"
		disableDestStats.isCallToCpApi = false
		disableDestStats.SendTimerStats(authErrHandlerTimeStart)
	}()

	disableDestMutex.Lock()
	isDisableDestActive, isDisableDestReqPresent := authErrHandler.disableDestActiveMap[destinationId]
	disableActiveReq := strconv.FormatBool(isDisableDestReqPresent && isDisableDestActive)
	if isDisableDestReqPresent && isDisableDestActive {
		disableDestMutex.Unlock()
		authErrHandler.logger.Debugf("[%s request] :: Disable Destination Active : %s\n", loggerNm, disableActiveReq)
		return http.StatusOK, fmt.Sprintf(`{response: {isDisabled: %v, activeRequest: %v}`, false, disableActiveReq)
	}

	authErrHandler.disableDestActiveMap[destinationId] = true
	disableDestMutex.Unlock()

	defer func() {
		disableDestMutex.Lock()
		authErrHandler.disableDestActiveMap[destinationId] = false
		authErrHandler.logger.Debugf("[%s request] :: Disable request is inactive!", loggerNm)
		disableDestMutex.Unlock()
	}()

	disableURL := fmt.Sprintf("%s/workspaces/%s/destinations/%s/disable", configBEURL, workspaceId, destinationId)
	disableCpReq := &ControlPlaneRequestT{
		Url:         disableURL,
		Method:      http.MethodDelete,
		destName:    destination.DestinationDefinition.Name,
		RequestType: "Disable destination",
	}

	disableDestStats.statName = "disable_destination_request_sent"
	disableDestStats.isCallToCpApi = true
	disableDestStats.SendCountStat()

	cpiCallStartTime := time.Now()
	statusCode, respBody = authErrHandler.cpApiCall(disableCpReq)
	disableDestStats.statName = `disable_destination_request_latency`
	defer disableDestStats.SendTimerStats(cpiCallStartTime)
	authErrHandler.logger.Debugf(`Response from CP(stCd: %v) for disable dest req: %v`, statusCode, respBody)

	var disableDestRes *DisableDestinationResponse
	if disableErr := json.Unmarshal([]byte(respBody), &disableDestRes); disableErr != nil || !router_utils.IsNotEmptyString(disableDestRes.DestinationId) {
		var msg string
		if disableErr != nil {
			msg = disableErr.Error()
		} else {
			msg = "Could not disable the destination"
		}
		disableDestStats.statName = "disable_destination_failure"
		disableDestStats.errorMessage = msg
		disableDestStats.SendCountStat()
		return http.StatusBadRequest, msg
	}

	authErrHandler.logger.Debugf("[%s request] :: (Write) Disable Response received : %s\n", loggerNm, respBody)
	disableDestStats.statName = "disable_destination_success"
	disableDestStats.errorMessage = ""
	disableDestStats.SendCountStat()

	// After a successfully disabling the destination, need to remove existing accessToken(from in-memory cache)
	// This is being done to obtain new token after re-enabling disabled destination
	accountMutex := authErrHandler.getKeyMutex(authErrHandler.accountLockMap, rudderAccountId)
	accountMutex.Lock()
	defer accountMutex.Unlock()
	delete(authErrHandler.destAuthInfoMap, rudderAccountId)

	return statusCode, fmt.Sprintf(`{response: {isDisabled: %v, activeRequest: %v}`, !disableDestRes.Enabled, false)
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
