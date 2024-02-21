package v2

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	router_utils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

const (
	REFRESH_TOKEN = "REFRESH_TOKEN"
	// Identifier to be sent from destination(during transformation/delivery)
	AUTH_STATUS_INACTIVE = "AUTH_STATUS_INACTIVE"
	// Identifier for invalid_grant or access_denied errors(during refreshing the token)
	REF_TOKEN_INVALID_GRANT = "ref_token_invalid_grant"
)

var ErrPermissionOrTokenRevoked = errors.New("Problem with user permission or access/refresh token have been revoked")

func GetOAuthActionStatName(stat string) string {
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

// This method hits the Control Plane to get the account information
// As well update the account information into the destAuthInfoMap(which acts as an in-memory cache)
func (authErrHandler *OAuthHandler) fetchAccountInfoFromCp(refTokenParams *RefreshTokenParams, refTokenBody RefreshTokenBodyParams,
	authStats *OAuthStats, logTypeName string,
) (int, *AuthResponse, error) {
	refreshUrl := fmt.Sprintf("%s/destination/workspaces/%s/accounts/%s/token", configBEURL, refTokenParams.WorkspaceId, refTokenParams.AccountId)
	res, err := json.Marshal(refTokenBody)
	if err != nil {
		authStats.statName = GetOAuthActionStatName("failure")
		authStats.errorMessage = "error in marshalling refresh token body"
		authStats.SendCountStat()
		return http.StatusInternalServerError, nil, err
	}
	refreshCpReq := &ControlPlaneRequestT{
		Method:        http.MethodPost,
		Url:           refreshUrl,
		ContentType:   "application/json; charset=utf-8",
		Body:          string(res),
		destName:      refTokenParams.DestDefName,
		RequestType:   authStats.action,
		BasicAuthUser: authErrHandler.TokenProvider.Identity(),
	}
	var accountSecret AccountSecret
	// Stat for counting number of Refresh Token endpoint calls
	authStats.statName = GetOAuthActionStatName(`request_sent`)
	authStats.isCallToCpApi = true
	authStats.errorMessage = ""
	authStats.SendCountStat()

	cpiCallStartTime := time.Now()
	statusCode, response := authErrHandler.CpConn.CpApiCall(refreshCpReq)
	authStats.statName = GetOAuthActionStatName(`request_latency`)
	authStats.SendTimerStats(cpiCallStartTime)

	authErrHandler.Logger.Debugf("[%s] Got the response from Control-Plane: rt-worker-%d with statusCode: %d\n", loggerNm, refTokenParams.WorkerId, statusCode)

	// Empty Refresh token response
	if !router_utils.IsNotEmptyString(response) {
		authStats.statName = GetOAuthActionStatName("failure")
		authStats.errorMessage = "Empty secret"
		authStats.SendCountStat()
		// Setting empty accessToken value into in-memory auth info map(cache)
		authErrHandler.Logger.Debugf("[%s request] :: Empty %s response received(rt-worker-%d) : %s\n", loggerNm, logTypeName, refTokenParams.WorkerId, response)
		return http.StatusInternalServerError, nil, errors.New("empty secret")
	}

	if errType, refErrMsg := authErrHandler.GetRefreshTokenErrResp(response, &accountSecret); router_utils.IsNotEmptyString(refErrMsg) {
		// potential oauth secret alert as we are not setting anything in the cache as secret
		authResponse := &AuthResponse{
			Err:          errType,
			ErrorMessage: refErrMsg,
		}
		authStats.statName = GetOAuthActionStatName("failure")
		authStats.errorMessage = refErrMsg
		authStats.SendCountStat()
		if authResponse.Err == REF_TOKEN_INVALID_GRANT {
			// Should abort the event as refresh is not going to work
			// until we have new refresh token for the account
			return http.StatusBadRequest, authResponse, fmt.Errorf("invalid grant")
		}
		return http.StatusInternalServerError, authResponse, fmt.Errorf("error occurred while fetching/refreshing account info from CP: %s", refErrMsg)
	}
	authStats.statName = GetOAuthActionStatName("success")
	authStats.errorMessage = ""
	authStats.SendCountStat()
	authErrHandler.Logger.Debugf("[%s request] :: (Write) %s response received(rt-worker-%d): %s\n", loggerNm, logTypeName, refTokenParams.WorkerId, response)
	authErrHandler.Cache.Set(refTokenParams.AccountId, &AuthResponse{
		Account: accountSecret,
	})
	return http.StatusOK, &AuthResponse{
		Account: accountSecret,
	}, nil
}

func (authErrHandler *OAuthHandler) GetRefreshTokenErrResp(response string, accountSecret *AccountSecret) (errorType, message string) {
	if err := json.Unmarshal([]byte(response), &accountSecret); err != nil {
		// Some problem with AccountSecret unmarshalling
		authErrHandler.Logger.Debugf("Failed with error response: %v\n", err)
		message = fmt.Sprintf("Unmarshal of response unsuccessful: %v", response)
		errorType = "unmarshallableResponse"
	} else if gjson.Get(response, "body.code").String() == REF_TOKEN_INVALID_GRANT {
		// User (or) AccessToken (or) RefreshToken has been revoked
		bodyMsg := gjson.Get(response, "body.message").String()
		if bodyMsg == "" {
			// Default message
			authErrHandler.Logger.Debugf("Failed with error response: %v\n", response)
			message = ErrPermissionOrTokenRevoked.Error()
		} else {
			message = bodyMsg
		}
		errorType = REF_TOKEN_INVALID_GRANT
	} else if gjson.Get(response, "error").String() == "network_error" {
		errorType = gjson.Get(response, "error").String()
		message = gjson.Get(response, "message").String()
	}
	return errorType, message
}

func GetAuthErrorCategoryFromTransformResponse(respData []byte) (string, error) {
	transformedJobs := &TransformerResponse{}
	err := jsonfast.Unmarshal([]byte(gjson.GetBytes(respData, "output.0").Raw), &transformedJobs)
	if err != nil {
		return "", err
	}
	return transformedJobs.AuthErrorCategory, nil
}

func GetAuthErrorCategoryFromTransformProxyResponse(respData []byte) (string, error) {
	transformedJobs := &TransformerResponse{}
	err := jsonfast.Unmarshal([]byte(gjson.GetBytes(respData, "output").Raw), &transformedJobs)
	if err != nil {
		return "", err
	}
	return transformedJobs.AuthErrorCategory, nil
}

func checkIfTokenExpired(secret AccountSecret, oldSecret json.RawMessage, stats *OAuthStats) bool {
	if secret.ExpirationDate != "" && verifyExpirationDate(secret.ExpirationDate, stats) {
		return true
	}
	if router_utils.IsNotEmptyString(string(oldSecret)) {
		if bytes.Equal(secret.Secret, oldSecret) {
			return true
		}
	}
	return false
}

func verifyExpirationDate(expirationDate string, stats *OAuthStats) bool {
	date, err := time.Parse(misc.RFC3339Milli, expirationDate)
	if err != nil {
		stats.errorMessage = "error in parsing expiration date"
		stats.statName = GetOAuthActionStatName("proActive_token_refresh")
		stats.SendCountStat()
		return false
	}
	if date.Before(time.Now().Add(config.GetDurationVar(5, time.Minute, "Router."+stats.destDefName+".oauth.expirationTimeDiff", "Router.oauth.expirationTimeDiff"))) {
		return true
	}
	return false
}

func UpdateAuthStatusToInactive(destination *backendconfig.DestinationT, workspaceID, rudderAccountId string, authStatusToggle func(params *AuthStatusToggleParams) (statusCode int, respBody string)) int {
	inactiveAuthStatusStatTags := stats.Tags{
		"id":          destination.ID,
		"destType":    destination.DestinationDefinition.Name,
		"workspaceId": workspaceID,
		"success":     "true",
		"flowType":    string(RudderFlow_Delivery),
	}
	errCatStatusCode, _ := authStatusToggle(&AuthStatusToggleParams{
		Destination:     destination,
		WorkspaceId:     workspaceID,
		RudderAccountId: rudderAccountId,
		StatPrefix:      AuthStatusInactive,
		AuthStatus:      AUTH_STATUS_INACTIVE,
	})
	if errCatStatusCode != http.StatusOK {
		// Error while inactivating authStatus
		inactiveAuthStatusStatTags["success"] = "false"
	}
	stats.Default.NewTaggedStat("auth_status_inactive_category_count", stats.CountType, inactiveAuthStatusStatTags).Increment()
	// Abort the jobs as the destination is disabled
	return http.StatusBadRequest
}
