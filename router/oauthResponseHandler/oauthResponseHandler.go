package oauthResponseHandler

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
type RefreshSecret struct {
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
}

type DisableDestinationResponse struct {
	Enabled       bool   `json:"enabled"`
	DestinationId string `json:"id"`
}

type RefreshTokenParams struct {
	AccountId   string
	WorkspaceId string
	AccessToken string
	DestDefName string
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
	lockMapWMutex                  sync.RWMutex             // This mutex is used to prevent concurrent writes in lockMap(s) mentioned in the struct
	refreshTokenMap                map[string]*RefreshSecret
	disableDestMap                 map[string]bool // Used to see if a destination is disabled or not
}

type Authorizer interface {
	Setup()
	DisableDestination(destination backendconfig.DestinationT, workspaceId string) (statusCode int, resBody string)
	RefreshToken(refTokenParams *RefreshTokenParams) (int, *RefreshSecret)
}

type ControlPlaneRequestT struct {
	Body        []byte
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
}

type RefreshTokenBody struct {
	hasExpired   bool
	expiredToken string
}

// Custom Marshalling for RefreshTokenBody struct
func (refTokenBody RefreshTokenBody) MarshalJSON() ([]byte, error) {
	j, err := json.Marshal(struct {
		HasExpired   bool
		ExpiredToken string
	}{
		HasExpired:   refTokenBody.hasExpired,
		ExpiredToken: refTokenBody.expiredToken,
	})
	if err != nil {
		return nil, err
	}
	return j, nil
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
}

func (authErrHandler *OAuthErrResHandler) Setup() {
	authErrHandler.logger = pkgLogger
	authErrHandler.tr = &http.Transport{}
	//This timeout is kind of modifiable & it seemed like 10 mins for this is too much!
	authErrHandler.client = &http.Client{Transport: authErrHandler.tr, Timeout: 5 * time.Minute}
	authErrHandler.oauthErrHandlerReqTimerStat = stats.NewStat("router.processor.oauthErrorHandler_request_time", stats.TimerType)
	authErrHandler.oauthErrHandlerNetReqTimerStat = stats.NewStat("router.oauthErrorHandler_network_request_time", stats.TimerType)
	authErrHandler.destLockMap = make(map[string]*sync.RWMutex)
	authErrHandler.accountLockMap = make(map[string]*sync.RWMutex)
	authErrHandler.lockMapWMutex = sync.RWMutex{}
	authErrHandler.refreshTokenMap = make(map[string]*RefreshSecret)
	authErrHandler.disableDestMap = make(map[string]bool)
}

func (authErrHandler *OAuthErrResHandler) RefreshToken(refTokenParams *RefreshTokenParams) (int, *RefreshSecret) {

	resMgrErr := authErrHandler.NewMutex(refTokenParams.AccountId, REFRESH_TOKEN)
	if resMgrErr != nil {
		panic(resMgrErr)
	}

	refTokenStats := &OAuthStats{
		id:              refTokenParams.AccountId,
		workspaceId:     refTokenParams.WorkspaceId,
		rudderCategory:  "destination",
		eventName:       "",
		isCallToCpApi:   false,
		authErrCategory: REFRESH_TOKEN,
		errorMessage:    "",
		destDefName:     refTokenParams.DestDefName,
	}
	authErrHandler.accountLockMap[refTokenParams.AccountId].RLock()
	refMap := authErrHandler.refreshTokenMap
	if refVal, ok := refMap[refTokenParams.AccountId]; ok {
		if router_utils.IsNotEmptyString(refVal.Account.AccessToken) && refVal.Account.AccessToken != refTokenParams.AccessToken {
			authErrHandler.accountLockMap[refTokenParams.AccountId].RUnlock()
			refTokenStats.eventName = "refresh_token_success"
			refTokenStats.errorMessage = ""
			refTokenStats.SendCountStat()
			authErrHandler.logger.Infof("[%s request] :: (Read) Refresh token response received : %s\n", loggerNm, refVal.Account.AccessToken)
			return http.StatusOK, refVal
		}
	}
	authErrHandler.accountLockMap[refTokenParams.AccountId].RUnlock()

	// Refresh Token from the endpoint in Cp
	authErrHandler.accountLockMap[refTokenParams.AccountId].Lock()
	defer authErrHandler.accountLockMap[refTokenParams.AccountId].Unlock()

	authErrHandler.oauthErrHandlerReqTimerStat.Start()
	defer authErrHandler.oauthErrHandlerReqTimerStat.End()

	if !router_utils.IsNotEmptyString(refTokenParams.AccessToken) {
		authErrHandler.refreshTokenMap[refTokenParams.AccountId] = &RefreshSecret{
			Err: `Cannot proceed with refresh token request as accessToken is empty`,
		}
		refTokenStats.eventName = "refresh_token_failure"
		refTokenStats.errorMessage = `Cannot proceed with refresh token request as accessToken is empty`
		refTokenStats.SendCountStat()
		return http.StatusBadRequest, authErrHandler.refreshTokenMap[refTokenParams.AccountId]
	}
	refreshUrl := fmt.Sprintf("%s/dest/workspaces/%s/accounts/%s/token", configBEURL, refTokenParams.WorkspaceId, refTokenParams.AccountId)
	refTokenBody := RefreshTokenBody{
		hasExpired:   true,
		expiredToken: refTokenParams.AccessToken,
	}
	res, err := json.Marshal(refTokenBody)
	if err != nil {
		panic(err)
	}
	refreshCpReq := &ControlPlaneRequestT{
		Method:      http.MethodPost,
		Url:         refreshUrl,
		ContentType: "application/json; charset=utf-8",
		Body:        []byte(res),
	}
	var accountSecret AccountSecret
	// Stat for counting number of Refresh Token endpoint calls
	refTokenStats.eventName = "refresh_token_request_sent"
	refTokenStats.isCallToCpApi = true
	refTokenStats.errorMessage = ""
	refTokenStats.SendCountStat()

	authErrHandler.oauthErrHandlerNetReqTimerStat.Start()
	statusCode, response := authErrHandler.cpApiCall(refreshCpReq)
	authErrHandler.oauthErrHandlerNetReqTimerStat.End()

	// Empty Refresh token response
	if !router_utils.IsNotEmptyString(response) {
		refTokenStats.eventName = "refresh_token_failure"
		refTokenStats.errorMessage = "Empty Response"
		refTokenStats.SendCountStat()
		authErrHandler.logger.Infof("[%s request] :: Empty Refresh token response received : %s\n", loggerNm, response)
		// authErrHandler.logger.Debugf("[%s request] :: Refresh token response received : %s", loggerNm, response)
		return statusCode, authErrHandler.refreshTokenMap[refTokenParams.AccountId]
	}

	if refErrMsg := getRefreshTokenErrResp(response, &accountSecret); router_utils.IsNotEmptyString(refErrMsg) {
		if _, ok := authErrHandler.refreshTokenMap[refTokenParams.AccountId]; !ok {
			authErrHandler.refreshTokenMap[refTokenParams.AccountId] = &RefreshSecret{
				Err: refErrMsg,
			}
		} else {
			authErrHandler.refreshTokenMap[refTokenParams.AccountId].Err = refErrMsg
		}
		refTokenStats.eventName = "refresh_token_failure"
		refTokenStats.errorMessage = refErrMsg
		refTokenStats.SendCountStat()
		return http.StatusInternalServerError, authErrHandler.refreshTokenMap[refTokenParams.AccountId]
	}
	authErrHandler.refreshTokenMap[refTokenParams.AccountId] = &RefreshSecret{
		Account: accountSecret,
	}
	refTokenStats.eventName = "refresh_token_success"
	refTokenStats.errorMessage = ""
	refTokenStats.SendCountStat()
	authErrHandler.logger.Infof("[%s request] :: (Write) Refresh token response received : %s\n", loggerNm, response)
	// authErrHandler.logger.Debugf("[%s request] :: Refresh token response received : %s", loggerNm, response)
	return statusCode, authErrHandler.refreshTokenMap[refTokenParams.AccountId]
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
	}).Increment()
}

func (authErrHandler *OAuthErrResHandler) DisableDestination(destination backendconfig.DestinationT, workspaceId string) (statusCode int, respBody string) {
	authErrHandler.oauthErrHandlerReqTimerStat.Start()
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

	authErrHandler.oauthErrHandlerNetReqTimerStat.Start()
	statusCode, respBody = authErrHandler.cpApiCall(disableCpReq)
	authErrHandler.oauthErrHandlerNetReqTimerStat.End()

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

	authErrHandler.oauthErrHandlerReqTimerStat.End()
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
	if cpReq.Body != nil {
		reqBody = bytes.NewBuffer(cpReq.Body)
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
	authErrHandler.oauthErrHandlerNetReqTimerStat.Start()
	res, doErr := authErrHandler.client.Do(req)
	authErrHandler.oauthErrHandlerNetReqTimerStat.End()
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
	}
	(&resHandler.lockMapWMutex).Lock()
	defer (&resHandler.lockMapWMutex).Unlock()
	if mutexMap != nil {
		if _, ok := mutexMap[id]; !ok {
			resHandler.logger.Infof("[%s request] :: Creating new mutex for %s\n", loggerNm, id)
			mutexMap[id] = &sync.RWMutex{}
		}
		resHandler.logger.Infof("[%s request] :: Already created mutex for %s\n", loggerNm, id)
		return nil
	}
	resHandler.logger.Infof("[%s request] :: Case missing for mutex for %s\n", loggerNm, id)
	return fmt.Errorf(`except %v, %v error category is not supported`, DISABLE_DEST, REFRESH_TOKEN)
}
