package oauthResponseHandler

//go:generate mockgen -destination=../../mocks/router/oauthResponseHandler/mock_oauthResponseHandler.go -package=mocks_oauthResponseHandler github.com/rudderlabs/rudder-server/router/oauthResponseHandler Authorizer
import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	router_utils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

type AccountSecret struct {
	AccessToken    string `json:"accessToken"`
	ExpirationDate string `json:"expirationDate"`
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
	eventName       string
	isCallToCpApi   bool
	authErrCategory string
	destDefName     string
	isFromProc      bool // This stats field is used to identify if a request to get token is arising from processor
}

type DisableDestinationResponse struct {
	Enabled       bool   `json:"enabled"`
	DestinationId string `json:"id"`
}

type RefreshTokenParams struct {
	AccountId       string
	WorkspaceId     string
	AccessToken     string
	DestDefName     string
	EventNamePrefix string
	WorkerId        int
}

// OAuthErrResHandler is the handle for this class
type OAuthErrResHandler struct {
	tr                             *http.Transport
	client                         *http.Client
	oauthErrHandlerReqTimerStat    stats.RudderStats
	oauthErrHandlerNetReqTimerStat stats.RudderStats
	logger                         logger.LoggerI
	destLockMap                    map[string]*sync.RWMutex // This mutex map is used for disable destination locking
	accountLockMap                 map[string]*sync.RWMutex // This mutex map is used for refresh token locking
	lockMapWMutex                  *sync.RWMutex            // This mutex is used to prevent concurrent writes in lockMap(s) mentioned in the struct
	destAuthInfoMap                map[string]*AuthResponse
	disableDestMap                 map[string]bool // Used to see if a destination is disabled or not
}

type Authorizer interface {
	Setup()
	DisableDestination(destination backendconfig.DestinationT, workspaceId string) (statusCode int, resBody string)
	RefreshToken(refTokenParams *RefreshTokenParams) (int, *AuthResponse)
	FetchToken(fetchTokenParams *RefreshTokenParams) (int, *AuthResponse)
}

type ControlPlaneRequestT struct {
	Body        string
	ContentType string
	Url         string
	Method      string
}

// This function creates a new OauthErrorResponseHandler
func NewOAuthErrorHandler() *OAuthErrResHandler {
	return &OAuthErrResHandler{}
}

var (
	configBEURL      string
	pkgLogger        logger.LoggerI
	loggerNm         string
	workspaceToken   string
	isMultiWorkspace bool
	destAuthInfoMap  map[string]*AuthResponse // Stores information about account after refresh token/fetch token
	accountLockMap   map[string]*sync.RWMutex // Lock used at account level to handle multiple refresh/fetch token requests
	destLockMap      map[string]*sync.RWMutex // Lock used at destination level to handle multiple disable destination requests
	lockMapMutex     *sync.RWMutex            // Lock used at destination level to handle multiple disable destination requests
	disableDestMap   map[string]bool          // Stores information about destination if it has been disabled
)

const (
	DISABLE_DEST  = "DISABLE_DESTINATION"
	REFRESH_TOKEN = "REFRESH_TOKEN"
)

// The response from the transformer network layer will be sent with a output property(in case of an error)
type ErrorOutput struct {
	Output ErrorResponse `json:"output"`
}

// This struct represents the datastructure present in Transformer network layer Error builder
type ErrorResponse struct {
	Message           string                 `json:"message"`
	Destination       map[string]interface{} `json:"destination"`
	NetworkFailure    bool                   `json:"networkFailure"`
	Status            int                    `json:"status"`
	AuthErrorCategory string                 `json:"authErrorCategory"`
	AccessToken       string                 `json:"accessToken"`
	StatTags          map[string]string      `json:"statTags"`
	StatName          string                 `json:"statName"`
}

type RefreshTokenBodyParams struct {
	HasExpired   bool   `json:"hasExpired"`
	ExpiredToken string `json:"expiredToken"`
}

func Init() {
	configBEURL = backendconfig.GetConfigBackendURL()
	pkgLogger = logger.NewLogger().Child("router").Child("OAuthResponseHandler")
	loggerNm = "OAuthResponseHandler"
	workspaceToken = config.GetWorkspaceToken()
	isMultiWorkspace = config.GetEnvAsBool("HOSTED_SERVICE", false)
	if isMultiWorkspace {
		workspaceToken = config.GetEnv("HOSTED_SERVICE_SECRET", "password")
	}
	destAuthInfoMap = make(map[string]*AuthResponse)
	accountLockMap = make(map[string]*sync.RWMutex)
	destLockMap = make(map[string]*sync.RWMutex)
	disableDestMap = make(map[string]bool)
	lockMapMutex = &sync.RWMutex{}
}

func (authErrHandler *OAuthErrResHandler) Setup() {
	authErrHandler.logger = pkgLogger
	authErrHandler.tr = &http.Transport{}
	//This timeout is kind of modifiable & it seemed like 10 mins for this is too much!
	authErrHandler.client = &http.Client{}
	authErrHandler.oauthErrHandlerReqTimerStat = stats.NewStat("router.processor.oauthErrorHandler_request_time", stats.TimerType)
	authErrHandler.oauthErrHandlerNetReqTimerStat = stats.NewStat("router.oauthErrorHandler_network_request_time", stats.TimerType)
	authErrHandler.destLockMap = destLockMap
	authErrHandler.accountLockMap = accountLockMap
	authErrHandler.lockMapWMutex = lockMapMutex
	authErrHandler.destAuthInfoMap = destAuthInfoMap
	authErrHandler.disableDestMap = disableDestMap
}

func (authErrHandler *OAuthErrResHandler) RefreshToken(refTokenParams *RefreshTokenParams) (int, *AuthResponse) {
	authStats := &OAuthStats{
		id:              refTokenParams.AccountId,
		workspaceId:     refTokenParams.WorkspaceId,
		rudderCategory:  "destination",
		eventName:       "",
		isCallToCpApi:   false,
		authErrCategory: REFRESH_TOKEN,
		errorMessage:    "",
		destDefName:     refTokenParams.DestDefName,
	}
	return authErrHandler.GetTokenInfo(refTokenParams, "Refresh token", authStats)
}

func (authErrHandler *OAuthErrResHandler) FetchToken(fetchTokenParams *RefreshTokenParams) (int, *AuthResponse) {
	authStats := &OAuthStats{
		id:              fetchTokenParams.AccountId,
		workspaceId:     fetchTokenParams.WorkspaceId,
		rudderCategory:  "destination",
		eventName:       "",
		isCallToCpApi:   false,
		authErrCategory: "",
		errorMessage:    "",
		destDefName:     fetchTokenParams.DestDefName,
		isFromProc:      true,
	}
	return authErrHandler.GetTokenInfo(fetchTokenParams, "Fetch token", authStats)
}

func (authErrHandler *OAuthErrResHandler) GetTokenInfo(refTokenParams *RefreshTokenParams, logTypeName string, authStats *OAuthStats) (int, *AuthResponse) {

	resMgrErr := authErrHandler.NewMutex(refTokenParams.AccountId, REFRESH_TOKEN)
	if resMgrErr != nil {
		panic(resMgrErr)
	}

	refTokenBody := RefreshTokenBodyParams{}
	if router_utils.IsNotEmptyString(refTokenParams.AccessToken) {
		refTokenBody = RefreshTokenBodyParams{
			HasExpired:   true,
			ExpiredToken: refTokenParams.AccessToken,
		}
	}
	authErrHandler.accountLockMap[refTokenParams.AccountId].RLock()
	authMap := authErrHandler.destAuthInfoMap
	if refVal, ok := authMap[refTokenParams.AccountId]; ok {
		if router_utils.IsNotEmptyString(refVal.Account.AccessToken) && refVal.Account.AccessToken != refTokenParams.AccessToken {
			authErrHandler.accountLockMap[refTokenParams.AccountId].RUnlock()
			authStats.eventName = fmt.Sprintf("%s_success", refTokenParams.EventNamePrefix)
			authStats.errorMessage = ""
			authStats.SendCountStat()
			authErrHandler.logger.Infof("[%s request] :: (Read) %s response received(rt-worker-%d): %s\n", loggerNm, logTypeName, refTokenParams.WorkerId, refVal.Account.AccessToken)
			return http.StatusOK, refVal
		}
	}
	authErrHandler.accountLockMap[refTokenParams.AccountId].RUnlock()

	authErrHandler.accountLockMap[refTokenParams.AccountId].Lock()
	defer authErrHandler.accountLockMap[refTokenParams.AccountId].Unlock()

	authErrHandler.logger.Infof("[%s] Refresh Lock Acquired by rt-worker-%d\n", loggerNm, refTokenParams.WorkerId)

	errHandlerReqTimeStart := time.Now()
	defer authErrHandler.oauthErrHandlerReqTimerStat.SendTiming(time.Since(errHandlerReqTimeStart))

	statusCode := authErrHandler.fetchAccountInfoFromCp(refTokenParams, refTokenBody, authStats, logTypeName)
	// authErrHandler.logger.Debugf("[%s request] :: Refresh token response received : %s", loggerNm, response)
	return statusCode, authErrHandler.destAuthInfoMap[refTokenParams.AccountId]
}

// This method hits the Control Plane to get the account information
// As well update the account information into the destAuthInfoMap(which acts as an in-memory cache)
func (authErrHandler *OAuthErrResHandler) fetchAccountInfoFromCp(refTokenParams *RefreshTokenParams, refTokenBody RefreshTokenBodyParams,
	authStats *OAuthStats, logTypeName string) (statusCode int) {
	refreshUrl := fmt.Sprintf("%s/dest/workspaces/%s/accounts/%s/token", configBEURL, refTokenParams.WorkspaceId, refTokenParams.AccountId)
	res, err := json.Marshal(refTokenBody)
	if err != nil {
		panic(err)
	}
	refreshCpReq := &ControlPlaneRequestT{
		Method:      http.MethodPost,
		Url:         refreshUrl,
		ContentType: "application/json; charset=utf-8",
		Body:        string(res),
	}
	var accountSecret AccountSecret
	// Stat for counting number of Refresh Token endpoint calls
	authStats.isCallToCpApi = true
	authStats.errorMessage = ""
	authStats.SendCountStat()

	cpiCallStartTime := time.Now()
	statusCode, response := authErrHandler.cpApiCall(refreshCpReq)
	authErrHandler.oauthErrHandlerNetReqTimerStat.SendTiming(time.Since(cpiCallStartTime))

	authErrHandler.logger.Infof("[%s] Got the response from Control-Plane: rt-worker-%d\n", loggerNm, refTokenParams.WorkerId)

	// Empty Refresh token response
	if !router_utils.IsNotEmptyString(response) {
		authStats.eventName = fmt.Sprintf("%s_failure", refTokenParams.EventNamePrefix)
		authStats.errorMessage = "Empty Response"
		authStats.SendCountStat()
		// Setting empty accessToken value into in-memory auth info map(cache)
		authErrHandler.destAuthInfoMap[refTokenParams.AccountId] = &AuthResponse{
			Account: AccountSecret{},
		}
		authErrHandler.logger.Infof("[%s request] :: Empty %s response received(rt-worker-%d) : %s\n", loggerNm, logTypeName, refTokenParams.WorkerId, response)
		// authErrHandler.logger.Debugf("[%s request] :: Refresh token response received : %s", loggerNm, response)
		return statusCode
	}

	if refErrMsg := getRefreshTokenErrResp(response, &accountSecret); router_utils.IsNotEmptyString(refErrMsg) {
		if _, ok := authErrHandler.destAuthInfoMap[refTokenParams.AccountId]; !ok {
			authErrHandler.destAuthInfoMap[refTokenParams.AccountId] = &AuthResponse{
				Err: refErrMsg,
			}
		} else {
			authErrHandler.destAuthInfoMap[refTokenParams.AccountId].Err = refErrMsg
		}
		authStats.eventName = fmt.Sprintf("%s_failure", refTokenParams.EventNamePrefix)
		authStats.errorMessage = refErrMsg
		authStats.SendCountStat()
		return http.StatusInternalServerError
	}
	// Update the refreshed account information into in-memory map(cache)
	authErrHandler.destAuthInfoMap[refTokenParams.AccountId] = &AuthResponse{
		Account: accountSecret,
	}
	authStats.eventName = fmt.Sprintf("%s_success", refTokenParams.EventNamePrefix)
	authStats.errorMessage = ""
	authStats.SendCountStat()
	authErrHandler.logger.Infof("[%s request] :: (Write) %s response received(rt-worker-%d): %s\n", loggerNm, logTypeName, refTokenParams.WorkerId, response)
	return http.StatusOK
}

func getRefreshTokenErrResp(response string, accountSecret *AccountSecret) (message string) {
	if err := json.Unmarshal([]byte(response), &accountSecret); err != nil {
		// Some problem with AccountSecret unmarshalling
		message = err.Error()
	} else if !router_utils.IsNotEmptyString(accountSecret.AccessToken) {
		// Status is 200, but no accesstoken is sent
		message = `Empty Token cannot be processed further`
	}
	return message
}

// Send count type stats related to OAuth(Destination)
func (refStats *OAuthStats) SendCountStat() {
	stats.NewTaggedStat(refStats.eventName, stats.CountType, stats.Tags{
		"accountId":       refStats.id,
		"workspaceId":     refStats.workspaceId,
		"rudderCategory":  refStats.rudderCategory,
		"errorMessage":    refStats.errorMessage,
		"isCallToCpApi":   strconv.FormatBool(refStats.isCallToCpApi),
		"authErrCategory": refStats.authErrCategory,
		"destDefName":     refStats.destDefName,
		"isFromProc":      strconv.FormatBool(refStats.isFromProc),
	}).Increment()
}

func (authErrHandler *OAuthErrResHandler) DisableDestination(destination backendconfig.DestinationT, workspaceId string) (statusCode int, respBody string) {
	authErrHandlerTimeStart := time.Now()
	destinationId := destination.ID
	resMgrErr := authErrHandler.NewMutex(destinationId, DISABLE_DEST)
	if resMgrErr != nil {
		panic(resMgrErr)
	}

	disableDestStats := &OAuthStats{
		id:              destinationId,
		workspaceId:     workspaceId,
		rudderCategory:  "destination",
		eventName:       "",
		isCallToCpApi:   false,
		authErrCategory: DISABLE_DEST,
		errorMessage:    "",
		destDefName:     destination.DestinationDefinition.Name,
	}

	authErrHandler.destLockMap[destinationId].RLock()
	if isDisabled, ok := authErrHandler.disableDestMap[destinationId]; ok && isDisabled {
		authErrHandler.destLockMap[destinationId].RUnlock()
		disableDestStats.eventName = "disable_destination_success"
		disableDestStats.errorMessage = ""
		disableDestStats.SendCountStat()
		authErrHandler.logger.Infof("[%s request] :: (Read) Disable Response received : %s\n", loggerNm, strconv.FormatBool(isDisabled))
		return http.StatusOK, "Successfully disabled"
	}
	authErrHandler.destLockMap[destinationId].RUnlock()
	disableURL := fmt.Sprintf("%s/workspaces/%s/destinations/%s/disable", configBEURL, workspaceId, destinationId)
	disableCpReq := &ControlPlaneRequestT{
		Url:    disableURL,
		Method: http.MethodDelete,
	}

	authErrHandler.destLockMap[destinationId].Lock()
	defer authErrHandler.destLockMap[destinationId].Unlock()

	disableDestStats.eventName = "disable_destination_request_sent"
	disableDestStats.isCallToCpApi = true
	disableDestStats.SendCountStat()

	cpiCallStartTime := time.Now()
	statusCode, respBody = authErrHandler.cpApiCall(disableCpReq)
	authErrHandler.oauthErrHandlerNetReqTimerStat.SendTiming(time.Since(cpiCallStartTime))

	var disableDestRes *DisableDestinationResponse
	if disableErr := json.Unmarshal([]byte(respBody), &disableDestRes); disableErr != nil || !router_utils.IsNotEmptyString(disableDestRes.DestinationId) {
		var msg string
		if disableErr != nil {
			msg = disableErr.Error()
		} else {
			msg = "Could not disable the destination"
		}
		disableDestStats.eventName = "disable_destination_failure"
		disableDestStats.errorMessage = msg
		disableDestStats.SendCountStat()
		return http.StatusBadRequest, msg
	}
	authErrHandler.disableDestMap[destinationId] = !disableDestRes.Enabled

	authErrHandler.oauthErrHandlerReqTimerStat.SendTiming(time.Since(authErrHandlerTimeStart))
	authErrHandler.logger.Infof("[%s request] :: (Write) Disable Response received : %s\n", loggerNm, respBody)
	disableDestStats.eventName = "disable_destination_success"
	disableDestStats.errorMessage = ""
	disableDestStats.SendCountStat()
	return statusCode, "Successfully disabled"
}

func processResponse(resp *http.Response) (statusCode int, respBody string) {
	var respData []byte
	var ioUtilReadErr error
	if resp != nil && resp.Body != nil {
		respData, ioUtilReadErr = ioutil.ReadAll(resp.Body)
		defer resp.Body.Close()
		if ioUtilReadErr != nil {
			return http.StatusInternalServerError, ioUtilReadErr.Error()
		}
	}
	//Detecting content type of the respData
	contentTypeHeader := http.DetectContentType(respData)
	//If content type is not of type "*text*", overriding it with empty string
	if !(strings.Contains(strings.ToLower(contentTypeHeader), "text") ||
		strings.Contains(strings.ToLower(contentTypeHeader), "application/json") ||
		strings.Contains(strings.ToLower(contentTypeHeader), "application/xml")) {
		respData = []byte("")
	}

	return resp.StatusCode, string(respData)
}

func (authErrHandler *OAuthErrResHandler) cpApiCall(cpReq *ControlPlaneRequestT) (int, string) {
	var reqBody *bytes.Buffer
	var req *http.Request
	var err error
	if router_utils.IsNotEmptyString(cpReq.Body) {
		reqBody = bytes.NewBufferString(cpReq.Body)
		req, err = http.NewRequest(cpReq.Method, cpReq.Url, reqBody)
	} else {
		req, err = http.NewRequest(cpReq.Method, cpReq.Url, nil)
	}
	if err != nil {
		authErrHandler.logger.Errorf("[%s request] :: destination request failed: %+v\n", loggerNm, err)
		// Abort on receiving an error in request formation
		return http.StatusBadRequest, err.Error()
	}
	// Authorisation setting
	req.SetBasicAuth(workspaceToken, "")

	// Set content-type in order to send the body in request correctly
	if router_utils.IsNotEmptyString(cpReq.ContentType) {
		req.Header.Set("Content-Type", cpReq.ContentType)
	}

	authErrHandlerTimeStart := time.Now()
	res, doErr := authErrHandler.client.Do(req)
	authErrHandler.oauthErrHandlerNetReqTimerStat.SendTiming(time.Since(authErrHandlerTimeStart))
	authErrHandler.logger.Debugf("[%s request] :: destination request sent\n", loggerNm)
	if doErr != nil {
		// Abort on receiving an error
		authErrHandler.logger.Errorf("[%s request] :: destination request failed: %+v\n", loggerNm, doErr)
		return http.StatusBadRequest, doErr.Error()
	}
	if res.Body != nil {
		defer res.Body.Close()
	}
	statusCode, resp := processResponse(res)
	return statusCode, resp
}

func (resHandler *OAuthErrResHandler) NewMutex(id string, errCategory string) error {
	var mutexMap map[string]*sync.RWMutex
	switch errCategory {
	case DISABLE_DEST:
		mutexMap = resHandler.destLockMap
	case REFRESH_TOKEN:
		mutexMap = resHandler.accountLockMap
	default:
		resHandler.logger.Infof("[%s request] :: Case missing for mutex for %s\n", loggerNm, id)
		return fmt.Errorf(`except %v, %v error category is not supported`, DISABLE_DEST, REFRESH_TOKEN)
	}
	resHandler.lockMapWMutex.Lock()
	defer resHandler.lockMapWMutex.Unlock()
	// mutexMap will not be nil
	if _, ok := mutexMap[id]; !ok {
		resHandler.logger.Infof("[%s request] :: Creating new mutex for %s\n", loggerNm, id)
		mutexMap[id] = &sync.RWMutex{}
	}
	resHandler.logger.Infof("[%s request] :: Already created mutex for %s\n", loggerNm, id)
	return nil
}
