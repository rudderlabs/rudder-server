package api

// This is simply going to make any API call to transfomer as per the API spec here: https://www.notion.so/rudderstacks/GDPR-Transformer-API-Spec-c3303b5b70c64225815d56c7767f8d22
// to get deletion done.
// called by delete/deleteSvc with (model.Job, model.Destination).
// returns final status,error ({successful, failure}, err)
import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	oauth "github.com/rudderlabs/rudder-server/router/oauthResponseHandler"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var (
	pkgLogger             = logger.NewLogger().Child("api")
	supportedDestinations = []string{"BRAZE", "AM", "INTERCOM", "CLEVERTAP", "AF", "MP", "GA"}
)

type APIManager struct {
	Client           *http.Client
	DestTransformURL string
	OAuth            oauth.Authorizer
}

// prepares payload based on (job,destDetail) & make an API call to transformer.
// gets (status, failure_reason) which is converted to appropriate model.Error & returned to caller.
func (api *APIManager) Delete(ctx context.Context, job model.Job, destDetail model.Destination) model.JobStatus {
	pkgLogger.Debugf("deleting: %v", job, " from API destination: %v", destDetail.Name)
	method := http.MethodPost
	endpoint := "/deleteUsers"
	url := fmt.Sprint(api.DestTransformURL, endpoint)

	bodySchema := mapJobToPayload(job, strings.ToLower(destDetail.Name), destDetail.Config)
	pkgLogger.Debugf("payload: %#v", bodySchema)

	reqBody, err := json.Marshal(bodySchema)
	if err != nil {
		return model.JobStatusFailed
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(reqBody))
	if err != nil {
		return model.JobStatusFailed
	}
	req.Header.Set("Content-Type", "application/json")

	tokenInfo, getTokenInfoErr := api.getOAuthTokenInfo(job.WorkspaceID, &destDetail)
	if getTokenInfoErr != nil {
		// Problem in fetching token
		return model.JobStatusFailed
	}
	setFailErr := api.setOAuthTokenInfo(*tokenInfo, req)
	if setFailErr != nil {
		// Marshal failure
		return model.JobStatusFailed
	}

	fileCleaningTime := stats.Default.NewTaggedStat("file_cleaning_time", stats.TimerType, stats.Tags{
		"jobId":       fmt.Sprintf("%d", job.ID),
		"workspaceId": job.WorkspaceID,
		"destType":    "api",
		"destName":    strings.ToLower(destDetail.Name),
	})
	fileCleaningTime.Start()
	defer fileCleaningTime.End()

	resp, err := api.Client.Do(req)
	if err != nil {
		if os.IsTimeout(err) {
			stats.Default.NewStat("regulation_worker_delete_api_timeout", stats.CountType).Count(1)
		}
		return model.JobStatusFailed
	}
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return model.JobStatusFailed
	}

	var jobResp []JobRespSchema
	if err := json.Unmarshal(bodyBytes, &jobResp); err != nil {
		return model.JobStatusFailed
	}

	// Refresh Flow Start
	if tokenInfo.IsOAuthEnabled {
		respParams := &handleRefreshFlowParams{
			jobResponses: jobResp,
			secret:       tokenInfo.AccountSecretInfo.Account.Secret,
			workspaceId:  job.WorkspaceID,
			accountId:    tokenInfo.DeleteAccountId,
			destName:     destDetail.Name,
		}
		refreshFlowStatus := api.handleRefreshFlow(respParams)
		if refreshFlowStatus != model.JobStatusUndefined {
			return refreshFlowStatus
		}
	}
	// Refresh Flow ends

	switch resp.StatusCode {

	case http.StatusOK:
		return model.JobStatusComplete

	case http.StatusBadRequest:
		pkgLogger.Warnf("Error: %v", jobResp)
		return model.JobStatusAborted

	case http.StatusUnauthorized:
		pkgLogger.Warnf("Error: %v", jobResp)
		return model.JobStatusAborted

	case http.StatusNotFound, http.StatusMethodNotAllowed:
		pkgLogger.Warnf("Error: %v", jobResp)
		return model.JobStatusNotSupported

	case http.StatusTooManyRequests, http.StatusRequestTimeout:
		pkgLogger.Warnf("Error: %v", jobResp)
		return model.JobStatusFailed

	default:
		pkgLogger.Warnf("Bad request: %v", jobResp)
		return model.JobStatusFailed
	}
}

func (*APIManager) GetSupportedDestinations() []string {
	return supportedDestinations
}

func mapJobToPayload(job model.Job, destName string, destConfig map[string]interface{}) []apiDeletionPayloadSchema {
	uas := make([]userAttributesSchema, len(job.Users))
	for i, ua := range job.Users {
		uas[i] = make(map[string]string)
		uas[i]["userId"] = ua.ID
		for k, v := range ua.Attributes {
			uas[i][k] = v
		}
	}

	return []apiDeletionPayloadSchema{
		{
			JobID:          fmt.Sprintf("%d", job.ID),
			DestType:       destName,
			Config:         destConfig,
			UserAttributes: uas,
		},
	}
}

type handleRefreshFlowParams struct {
	secret       json.RawMessage
	destName     string
	workspaceId  string
	accountId    string
	jobResponses []JobRespSchema
}

/*
*
This method handles the refresh flow for OAuth destinations
When the status undefined(model.JobStatusUndefined) is returned, we can understand it is due to one of two things
1. The error that was received was not an error that could trigger Refresh flow
2. The destination itself is not OAuth one in the first place so it doesn't have errorCategory at all
*
*/
func (api *APIManager) handleRefreshFlow(params *handleRefreshFlowParams) model.JobStatus {
	var isRefresh bool
	for _, jobResponse := range params.jobResponses {
		isRefresh = jobResponse.AuthErrorCategory == oauth.REFRESH_TOKEN
		if isRefresh {
			break
		}
	}
	if isRefresh {
		pkgLogger.Debugf("Refresh flow triggered for %v\n", params.destName)
		// Refresh OAuth flow
		var refSecret *oauth.AuthResponse
		var errCatStatusCode int
		refTokenParams := &oauth.RefreshTokenParams{
			Secret:          params.secret,
			WorkspaceId:     params.workspaceId,
			AccountId:       params.accountId,
			DestDefName:     params.destName,
			EventNamePrefix: "refresh_token",
		}
		errCatStatusCode, refSecret = api.OAuth.RefreshToken(refTokenParams)
		// TODO: Does it make sense to have disable destination here ?
		refSec := *refSecret
		if strings.TrimSpace(refSec.Err) != "" {
			// There is an error occurring
			pkgLogger.Warnf("Error: %v, Status: %v", refSec.Err, errCatStatusCode)
			return model.JobStatusAborted
		}
		// Refresh is complete, the job has to be re-tried
		return model.JobStatusFailed
	}
	// Indicates that OAuth refresh flow is not triggered
	return model.JobStatusUndefined
}

type OAuthTokenResult struct {
	AccountSecretInfo *oauth.AuthResponse
	DeleteAccountId   string
	IsOAuthEnabled    bool
}

func (api *APIManager) setOAuthTokenInfo(tokenInfo OAuthTokenResult, req *http.Request) error {
	if tokenInfo.IsOAuthEnabled {
		// setting oauth related information
		payload, marshalErr := json.Marshal(tokenInfo.AccountSecretInfo.Account)
		if marshalErr != nil {
			marshalFailErr := fmt.Sprintf("error while marshalling account secret information: %v", marshalErr)
			pkgLogger.Errorf(marshalFailErr)
			return errors.New(marshalFailErr)
		}
		req.Header.Set("X-Rudder-Dest-Info", string(payload))
	}
	return nil
}

func (api *APIManager) getOAuthTokenInfo(workspaceId string, destDetail *model.Destination) (*OAuthTokenResult, error) {
	var tokenStatusCode int
	var accountSecretInfo *oauth.AuthResponse
	// identifier to know if the destination supports OAuth for regulation-api
	oAuthParamsResult := oauth.GetOAuthParams(oauth.OAuthParams{
		DestConfig:    destDetail.Config,
		DestDefConfig: destDetail.DestDefConfig,
		IdKey:         "rudderDeleteAccountId",
	})
	if oAuthParamsResult.Enabled {
		// Fetch Token call
		// Get Access Token Information to send it as part of the event
		tokenStatusCode, accountSecretInfo = api.OAuth.FetchToken(&oauth.RefreshTokenParams{
			AccountId:       oAuthParamsResult.AccountId,
			WorkspaceId:     workspaceId,
			DestDefName:     destDetail.Name,
			EventNamePrefix: "fetch_token",
		})
		pkgLogger.Debugf(`[%s][FetchToken] Token Fetch Method finished (statusCode, value): (%v, %+v)`, destDetail.Name, tokenStatusCode, accountSecretInfo)
		if tokenStatusCode != http.StatusOK {
			fetchFailStr := fmt.Sprintf(`[%s][FetchToken] Error in Token Fetch statusCode: %d\t error: %s\n`, destDetail.Name, tokenStatusCode, accountSecretInfo.Err)
			pkgLogger.Errorf(fetchFailStr)
			return nil, errors.New(fetchFailStr)
		}

	} else {
		pkgLogger.Errorf("[%v] Destination probably doesn't support OAuth or some issue happened while doing OAuth for deletion [Enabled: %v]", destDetail.Name, oAuthParamsResult.Enabled)
	}
	return &OAuthTokenResult{
		AccountSecretInfo: accountSecretInfo,
		DeleteAccountId:   oAuthParamsResult.AccountId,
		IsOAuthEnabled:    oAuthParamsResult.Enabled,
	}, nil
}
