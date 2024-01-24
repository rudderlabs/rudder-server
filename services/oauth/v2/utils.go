package v2

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	router_utils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/services/oauth"
	"github.com/tidwall/gjson"
)

const (
	REFRESH_TOKEN = "REFRESH_TOKEN"
	// Identifier to be sent from destination(during transformation/delivery)
	AUTH_STATUS_INACTIVE = "AUTH_STATUS_INACTIVE"

	// Identifier for invalid_grant or access_denied errors(during refreshing the token)
	REF_TOKEN_INVALID_GRANT = "ref_token_invalid_grant"
)

var ErrPermissionOrTokenRevoked = errors.New("Problem with user permission or access/refresh token have been revoked")

func getOAuthActionStatName(stat string) string {
	return fmt.Sprintf("oauth_action_%v", stat)
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

func (resHandler *OAuthHandler) getKeyMutex(mutexMap map[string]*sync.RWMutex, id string) *sync.RWMutex {
	resHandler.lockMapWMutex.Lock()
	defer resHandler.lockMapWMutex.Unlock()
	// mutexMap will not be nil
	if _, ok := mutexMap[id]; !ok {
		resHandler.logger.Debugf("[%s request] :: Creating new mutex for %s\n", loggerNm, id)
		mutexMap[id] = &sync.RWMutex{}
	}
	return mutexMap[id]
}

// This method hits the Control Plane to get the account information
// As well update the account information into the destAuthInfoMap(which acts as an in-memory cache)
func (authErrHandler *OAuthHandler) fetchAccountInfoFromCp(refTokenParams *RefreshTokenParams, refTokenBody RefreshTokenBodyParams,
	authStats *OAuthStats, logTypeName string,
) (statusCode int) {
	refreshUrl := fmt.Sprintf("%s/destination/workspaces/%s/accounts/%s/token", configBEURL, refTokenParams.WorkspaceId, refTokenParams.AccountId)
	res, err := json.Marshal(refTokenBody)
	if err != nil {
		panic(err)
	}
	refreshCpReq := &ControlPlaneRequestT{
		Method:        http.MethodPost,
		Url:           refreshUrl,
		ContentType:   "application/json; charset=utf-8",
		Body:          string(res),
		destName:      refTokenParams.DestDefName,
		RequestType:   authStats.action,
		basicAuthUser: "1oVajZWp673TO1pq5lUK6laxdlw",
	}
	var accountSecret AccountSecret
	// Stat for counting number of Refresh Token endpoint calls
	authStats.statName = getOAuthActionStatName(`request_sent`)
	authStats.isCallToCpApi = true
	authStats.errorMessage = ""
	authStats.SendCountStat()

	cpiCallStartTime := time.Now()
	statusCode, response := authErrHandler.CpConn.cpApiCall(refreshCpReq)
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

func (authErrHandler *OAuthHandler) getRefreshTokenErrResp(response string, accountSecret *AccountSecret) (errorType, message string) {
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

func (authErrHandler *OAuthHandler) updateAuthStatusToInactive(destination *backendconfig.DestinationT, workspaceID, rudderAccountId string) int {
	inactiveAuthStatusStatTags := stats.Tags{
		"id":          destination.ID,
		"destType":    destination.DestinationDefinition.Name,
		"workspaceId": workspaceID,
		"success":     "true",
		"flowType":    string(oauth.RudderFlow_Delivery),
	}
	errCatStatusCode, _ := authErrHandler.AuthStatusToggle(&AuthStatusToggleParams{
		Destination:     destination,
		WorkspaceId:     workspaceID,
		RudderAccountId: rudderAccountId,
		AuthStatus:      oauth.AuthStatusInactive,
	})
	if errCatStatusCode != http.StatusOK {
		// Error while inactivating authStatus
		inactiveAuthStatusStatTags["success"] = "false"
	}
	stats.Default.NewTaggedStat("auth_status_inactive_category_count", stats.CountType, inactiveAuthStatusStatTags).Increment()
	// Abort the jobs as the destination is disabled
	return http.StatusBadRequest
}

func (authErrHandler *OAuthHandler) AuthStatusToggle(params *AuthStatusToggleParams) (statusCode int, respBody string) {
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
	statusCode, respBody = authErrHandler.CpConn.cpApiCall(authStatusInactiveCpReq)
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
