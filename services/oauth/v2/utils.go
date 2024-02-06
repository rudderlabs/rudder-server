package v2

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	router_utils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/services/oauth"
	"github.com/rudderlabs/rudder-server/utils/misc"
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
		authStats.statName = getOAuthActionStatName("failure")
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
		basicAuthUser: authErrHandler.AccessToken(),
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
		authErrHandler.logger.Debugf("[%s request] :: Empty %s response received(rt-worker-%d) : %s\n", loggerNm, logTypeName, refTokenParams.WorkerId, response)
		return http.StatusInternalServerError, nil, errors.New("empty secret")
	}

	if errType, refErrMsg := authErrHandler.getRefreshTokenErrResp(response, &accountSecret); router_utils.IsNotEmptyString(refErrMsg) {
		// potential oauth secret alert as we are not setting anything in the cache as secret
		authErrHandler.cache.Set(refTokenParams.AccountId, &AuthResponse{
			Err:          errType,
			ErrorMessage: refErrMsg,
		})

		authStats.statName = getOAuthActionStatName("failure")
		authStats.errorMessage = refErrMsg
		authStats.SendCountStat()
		if refErrMsg == REF_TOKEN_INVALID_GRANT {
			// Should abort the event as refresh is not going to work
			// until we have new refresh token for the account
			return http.StatusBadRequest, nil, errors.New("invalid grant")
		}
		return http.StatusInternalServerError, nil, errors.New("error occurred while fetching/refreshing account info from CP: %v" + refErrMsg)
	}
	authStats.statName = getOAuthActionStatName("success")
	authStats.errorMessage = ""
	authStats.SendCountStat()
	authErrHandler.logger.Debugf("[%s request] :: (Write) %s response received(rt-worker-%d): %s\n", loggerNm, logTypeName, refTokenParams.WorkerId, response)
	authErrHandler.cache.Set(refTokenParams.AccountId, &AuthResponse{
		Account: accountSecret,
	})
	return http.StatusOK, &AuthResponse{
		Account: accountSecret,
	}, nil
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
	authErrHandler.lock.Lock(params.RudderAccountId)
	isAuthStatusUpdateActive, isAuthStatusUpdateReqPresent := authErrHandler.authStatusUpdateActiveMap[destinationId]
	authStatusUpdateActiveReq := strconv.FormatBool(isAuthStatusUpdateReqPresent && isAuthStatusUpdateActive)
	if isAuthStatusUpdateReqPresent && isAuthStatusUpdateActive {
		authErrHandler.lock.Unlock(params.RudderAccountId)
		authErrHandler.logger.Debugf("[%s request] :: AuthStatusInactive request Active : %s\n", loggerNm, authStatusUpdateActiveReq)
		return http.StatusConflict, ErrPermissionOrTokenRevoked.Error()
	}

	authErrHandler.authStatusUpdateActiveMap[destinationId] = true
	authErrHandler.lock.Unlock(params.RudderAccountId)

	defer func() {
		authErrHandler.lock.Lock(params.RudderAccountId)
		authErrHandler.authStatusUpdateActiveMap[destinationId] = false
		authErrHandler.logger.Debugf("[%s request] :: AuthStatusInactive request is inactive!", loggerNm)
		authErrHandler.lock.Unlock(params.RudderAccountId)
		// After trying to inactivate authStatus for destination, need to remove existing accessToken(from in-memory cache)
		// This is being done to obtain new token after an update such as re-authorisation is done
		authErrHandler.lock.Lock(params.RudderAccountId)
		authErrHandler.cache.Delete(params.RudderAccountId)
		authErrHandler.lock.Unlock(params.RudderAccountId)
	}()

	authStatusToggleUrl := fmt.Sprintf("%s/workspaces/%s/destinations/%s/authStatus/toggle", configBEURL, params.WorkspaceId, destinationId)

	authStatusInactiveCpReq := &ControlPlaneRequestT{
		Url:           authStatusToggleUrl,
		Method:        http.MethodPut,
		Body:          fmt.Sprintf(`{"authStatus": "%v"}`, params.AuthStatus),
		ContentType:   "application/json",
		destName:      params.Destination.DestinationDefinition.Name,
		RequestType:   action,
		basicAuthUser: authErrHandler.AccessToken(),
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
		stats.SendCountStat()
		return false
	}
	if date.Before(time.Now().Add(time.Minute * 10)) {
		return true
	}
	return true
}

func (t *Oauth2Transport) preTransformerCallHandling(req *http.Request) error {
	if t.flow == RudderFlow_Delivery {
		t.accountId = t.destination.GetAccountID("rudderAccountId")
	} else if t.flow == RudderFlow_Delete {
		t.accountId = t.destination.GetAccountID("rudderDeleteAccountId")
	}

	t.refreshTokenParams = &RefreshTokenParams{
		AccountId:   t.accountId,
		WorkspaceId: t.destination.WorkspaceID,
		DestDefName: t.destination.DestinationDefinition.Name,
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return fmt.Errorf("failed to read request body: %w", err)
	}
	matched, _ := regexp.MatchString(req.URL.Path, "/routerTransform")
	if matched {
		fetchErr := t.Augmenter.Augment(req, body, func() (json.RawMessage, error) {
			statusCode, authResponse, err := t.oauthHandler.FetchToken(t.refreshTokenParams)
			if statusCode == http.StatusOK {
				return authResponse.Account.Secret, nil
			}
			return nil, err
		})
		if fetchErr != nil {
			return fmt.Errorf("failed to fetch token: %w", fetchErr)
		}
	} else {
		req.Body = io.NopCloser(bytes.NewReader(body))
	}
	return nil
}

func (t *Oauth2Transport) postTransformerCallHandling(req *http.Request, res *http.Response) (*http.Response, error) {
	respData, _ := io.ReadAll(res.Body)
	res.Body = io.NopCloser(bytes.NewReader(respData))
	authErrorCategory, err := t.getAuthErrorCategory(respData)
	if err != nil {
		return res, err
	}
	if authErrorCategory == REFRESH_TOKEN {
		// since same token that was used to make the http call needs to be refreshed, we need the current token information
		var oldSecret json.RawMessage
		if req.Context().Value("secret") != nil {
			oldSecret = req.Context().Value("secret").(json.RawMessage)
		}
		t.refreshTokenParams.Secret = oldSecret
		t.oauthHandler.logger.Info("refreshing token")
		statusCode, refSecret, refErr := t.oauthHandler.RefreshToken(t.refreshTokenParams)
		if statusCode == http.StatusOK {
			// refresh token successful --> retry the event
			res.StatusCode = http.StatusInternalServerError
		} else {
			// invalid_grant -> 4xx
			// refresh token failed -> erreneous status-code
			if refSecret.Err == REF_TOKEN_INVALID_GRANT {
				t.oauthHandler.updateAuthStatusToInactive(t.destination, t.destination.WorkspaceID, t.accountId)
			}
			res.StatusCode = statusCode
		}
		if refErr != nil {
			err = fmt.Errorf("failed to refresh token: %w", refErr)
		}
		// // When expirationDate is available, only then parse
		// refer this: router/handle.go ---> handleOAuthDestResponse & make relevant changes
	} else if authErrorCategory == AUTH_STATUS_INACTIVE {
		res.StatusCode = t.oauthHandler.updateAuthStatusToInactive(t.destination, t.destination.WorkspaceID, t.accountId)
	}
	return res, err
}
