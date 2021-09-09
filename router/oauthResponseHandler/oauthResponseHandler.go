package oauthResponseHandler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

type AccountSecret struct {
	AccessToken    string `json:"accessToken"`
	ExpirationDate string `json:"expirationDate"`
}

// OAuthErrResHandler is the handle for this class
type OAuthErrResHandler struct {
	tr                             *http.Transport
	client                         *http.Client
	oauthErrHandlerReqTimerStat    stats.RudderStats
	oauthErrHandlerNetReqTimerStat stats.RudderStats
	logger                         logger.LoggerI
}

type Authorizer interface {
	Setup()
	DisableDestination(destination backendconfig.DestinationT, workspaceId string) (statusCode int, resBody string)
	RefreshToken(workspaceId string, accountId string, accessToken string) (statusCode int, resBody string)
}

//NewOauthErrorHandler creates a new transformer
func NewOAuthErrorHandler() *OAuthErrResHandler {
	return &OAuthErrResHandler{}
}

var (
	configBEURL string
	pkgLogger   logger.LoggerI
	loggerNm    string
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

func init() {
	configBEURL = backendconfig.GetConfigBackendURL()
	pkgLogger = logger.NewLogger().Child("router").Child("OAuthResponseHandler")
	loggerNm = "OAuthResponseHandler"
}

func (authErrHandler *OAuthErrResHandler) Setup() {
	authErrHandler.logger = pkgLogger
	authErrHandler.tr = &http.Transport{}
	//This timeout is kind of modifiable & it seemed like 10 mins for this is too much!
	authErrHandler.client = &http.Client{Transport: authErrHandler.tr, Timeout: 5 * time.Minute}
	authErrHandler.oauthErrHandlerReqTimerStat = stats.NewStat("router.processor.oauthErrorHandler_request_time", stats.TimerType)
	authErrHandler.oauthErrHandlerNetReqTimerStat = stats.NewStat("router.oauthErrorHandler_network_request_time", stats.TimerType)
}

func (authErrHandler *OAuthErrResHandler) RefreshToken(workspaceId string, accountId string, accessToken string) (statusCode int, respBody string) {
	authErrHandler.oauthErrHandlerReqTimerStat.Start()
	if len(strings.TrimSpace(accessToken)) == 0 {
		return http.StatusBadRequest, `Cannot proceed with refresh token request as accessToken is empty`
	}
	authErrHandler.oauthErrHandlerNetReqTimerStat.Start()
	refreshUrl := fmt.Sprintf("%s/dest/workspaces/%s/accounts/%s/token", configBEURL, workspaceId, accountId)
	authErrHandler.oauthErrHandlerNetReqTimerStat.End()
	refTokenBody := RefreshTokenBody{
		hasExpired:   true,
		expiredToken: accessToken,
	}
	res, err := json.Marshal(refTokenBody)
	if err != nil {
		// TODO: Is this way ok ? - Check with team
		return http.StatusBadRequest, err.Error()
	}
	resp, refErr := authErrHandler.client.Post(refreshUrl, "application/json; charset=utf-8", bytes.NewBuffer(res))
	if refErr != nil {
		panic(refErr)
	}
	statusCode, response := authErrHandler.processResponse(resp, err)
	authErrHandler.oauthErrHandlerReqTimerStat.End()
	authErrHandler.logger.Debugf("[%s request] :: Refresh token response received : %s", loggerNm, response)
	return statusCode, response
}

func (authErrHandler *OAuthErrResHandler) DisableDestination(destination backendconfig.DestinationT, workspaceId string) (statusCode int, respBody string) {
	authErrHandler.oauthErrHandlerReqTimerStat.Start()
	destinationId := destination.ID
	disableURL := fmt.Sprintf("%s/workspaces/%s/destinations/%s/disable", configBEURL, workspaceId, destinationId)
	req, err := http.NewRequest(http.MethodDelete, disableURL, nil)
	if err != nil {
		// Abort on receiving an error in request formation
		return http.StatusBadRequest, err.Error()
	}
	authErrHandler.logger.Debugf("[%s request] :: Disable Request sent : %s", loggerNm)
	authErrHandler.oauthErrHandlerNetReqTimerStat.Start()
	res, doErr := authErrHandler.client.Do(req)
	authErrHandler.oauthErrHandlerNetReqTimerStat.End()
	if doErr != nil {
		// Abort on receiving an error
		return http.StatusBadRequest, err.Error()
	}
	statusCode, resp := authErrHandler.processResponse(res, doErr)
	authErrHandler.oauthErrHandlerReqTimerStat.End()
	authErrHandler.logger.Debugf("[%s request] :: Disable Response received : %s", loggerNm)
	return statusCode, resp
}

func (authErrHandler *OAuthErrResHandler) processResponse(resp *http.Response, err error) (statusCode int, respBody string) {
	var respData []byte
	if resp != nil && resp.Body != nil {
		respData, _ = ioutil.ReadAll(resp.Body)
		defer resp.Body.Close()
	}
	var contentTypeHeader string
	if resp != nil && resp.Header != nil {
		contentTypeHeader = resp.Header.Get("Content-Type")
	}
	if contentTypeHeader == "" {
		//Detecting content type of the respBody
		contentTypeHeader = http.DetectContentType(respData)
	}
	//If content type is not of type "*text*", overriding it with empty string
	if !(strings.Contains(strings.ToLower(contentTypeHeader), "text") ||
		strings.Contains(strings.ToLower(contentTypeHeader), "application/json") ||
		strings.Contains(strings.ToLower(contentTypeHeader), "application/xml")) {
		respData = []byte("")
	}
	if err != nil {
		respData = []byte("")
		authErrHandler.logger.Errorf("[%s request] :: destination request failed: %+v", loggerNm, err)
		return http.StatusInternalServerError, string(respData)
	}
	return resp.StatusCode, string(respData)
}
