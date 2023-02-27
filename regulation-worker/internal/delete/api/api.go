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
	"github.com/rudderlabs/rudder-server/services/oauth"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var (
	pkgLogger             = logger.NewLogger().Child("api")
	supportedDestinations = []string{"BRAZE", "AM", "INTERCOM", "CLEVERTAP", "AF", "MP", "GA"}
)

type APIManager struct {
	Client                       *http.Client
	DestTransformURL             string
	OAuth                        oauth.Authorizer
	MaxOAuthRefreshRetryAttempts int
}

type oauthDetail struct {
	secretToken *oauth.AuthResponse
	id          string
}

func (*APIManager) GetSupportedDestinations() []string {
	return supportedDestinations
}

func (api *APIManager) deleteWithRetry(ctx context.Context, job model.Job, destination model.Destination, currentOauthRetryAttempt int) model.JobStatus {
	pkgLogger.Debugf("deleting: %v", job, " from API destination: %v", destination.Name)
	method := http.MethodPost
	endpoint := "/deleteUsers"
	url := fmt.Sprint(api.DestTransformURL, endpoint)

	bodySchema := mapJobToPayload(job, strings.ToLower(destination.Name), destination.Config)
	pkgLogger.Debugf("payload: %#v", bodySchema)

	reqBody, err := json.Marshal(bodySchema)
	if err != nil {
		return model.JobStatus{Status: model.JobStatusFailed, Error: err}
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(reqBody))
	if err != nil {
		return model.JobStatus{Status: model.JobStatusFailed, Error: err}
	}
	req.Header.Set("Content-Type", "application/json")

	// check if OAuth destination
	isOAuthEnabled := oauth.GetAuthType(destination.DestDefConfig) == oauth.OAuth
	var oAuthDetail oauthDetail
	if isOAuthEnabled {
		oAuthDetail, err = api.getOAuthDetail(&destination, job.WorkspaceID)
		if err != nil {
			pkgLogger.Error(err)
			return model.JobStatus{Status: model.JobStatusFailed, Error: err}
		}
		err = setOAuthHeader(oAuthDetail.secretToken, req)
		if err != nil {
			pkgLogger.Errorf("[%v] error occurred while setting oauth header for workspace: %v, destination: %v", destination.Name, job.WorkspaceID, destination.DestinationID)
			pkgLogger.Error(err)
			return model.JobStatus{Status: model.JobStatusFailed, Error: err}
		}
	}

	defer stats.Default.NewTaggedStat(
		"regulation_worker_cleaning_time",
		stats.TimerType,
		stats.Tags{
			"destinationId": job.DestinationID,
			"workspaceId":   job.WorkspaceID,
			"jobType":       "api",
		}).RecordDuration()()

	resp, err := api.Client.Do(req)
	if err != nil {
		if os.IsTimeout(err) {
			stats.Default.NewStat("regulation_worker_delete_api_timeout", stats.CountType).Count(1)
		}
		return model.JobStatus{Status: model.JobStatusFailed, Error: err}
	}
	defer func() { httputil.CloseResponse(resp) }()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return model.JobStatus{Status: model.JobStatusFailed, Error: err}
	}

	var jobResp []JobRespSchema
	if err := json.Unmarshal(bodyBytes, &jobResp); err != nil {
		return model.JobStatus{Status: model.JobStatusFailed, Error: err}
	}
	jobStatus := getJobStatus(resp.StatusCode, jobResp)
	pkgLogger.Debugf("[%v] Job: %v, JobStatus: %v", destination.Name, job.ID, jobStatus)

	if isOAuthEnabled && isTokenExpired(jobResp) && currentOauthRetryAttempt < api.MaxOAuthRefreshRetryAttempts {
		err = api.refreshOAuthToken(destination.Name, job.WorkspaceID, oAuthDetail)
		if err != nil {
			pkgLogger.Error(err)
			return model.JobStatus{Status: model.JobStatusFailed, Error: err}
		}
		// retry the request
		pkgLogger.Infof("[%v] Retrying deleteRequest job(id: %v) for the whole batch, RetryAttempt: %v", destination.Name, job.ID, currentOauthRetryAttempt+1)
		return api.deleteWithRetry(ctx, job, destination, currentOauthRetryAttempt+1)
	}

	return jobStatus
}

// prepares payload based on (job,destDetail) & make an API call to transformer.
// gets (status, failure_reason) which is converted to appropriate model.Error & returned to caller.
func (api *APIManager) Delete(ctx context.Context, job model.Job, destination model.Destination) model.JobStatus {
	return api.deleteWithRetry(ctx, job, destination, 0)
}

func getJobStatus(statusCode int, jobResp []JobRespSchema) model.JobStatus {
	switch statusCode {
	case http.StatusOK:
		return model.JobStatus{Status: model.JobStatusComplete}
	case http.StatusNotFound, http.StatusMethodNotAllowed:
		return model.JobStatus{Status: model.JobStatusAborted, Error: fmt.Errorf("destination not supported by transformer")}
	default:
		return model.JobStatus{Status: model.JobStatusFailed, Error: fmt.Errorf("error: code: %d, body: %s", statusCode, jobResp)}
	}
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

func isTokenExpired(jobResponses []JobRespSchema) bool {
	for _, jobResponse := range jobResponses {
		if jobResponse.AuthErrorCategory == oauth.REFRESH_TOKEN {
			return true
		}
	}
	return false
}

func setOAuthHeader(secretToken *oauth.AuthResponse, req *http.Request) error {
	payload, marshalErr := json.Marshal(secretToken.Account)
	if marshalErr != nil {
		marshalFailErr := fmt.Sprintf("error while marshalling account secret information: %v", marshalErr)
		pkgLogger.Errorf(marshalFailErr)
		return errors.New(marshalFailErr)
	}
	req.Header.Set("X-Rudder-Dest-Info", string(payload))
	return nil
}

func (api *APIManager) getOAuthDetail(destDetail *model.Destination, workspaceId string) (oauthDetail, error) {
	id := oauth.GetAccountId(destDetail.Config, oauth.DeleteAccountIdKey)
	if strings.TrimSpace(id) == "" {
		return oauthDetail{}, fmt.Errorf("[%v] Delete account ID key (%v) is not present for destination: %v", destDetail.Name, oauth.DeleteAccountIdKey, destDetail.DestinationID)
	}
	tokenStatusCode, secretToken := api.OAuth.FetchToken(&oauth.RefreshTokenParams{
		AccountId:       id,
		WorkspaceId:     workspaceId,
		DestDefName:     destDetail.Name,
		EventNamePrefix: "fetch_token",
	})
	if tokenStatusCode != http.StatusOK {
		return oauthDetail{}, fmt.Errorf("[%s][FetchToken] Error in Token Fetch statusCode: %d\t error: %s", destDetail.Name, tokenStatusCode, secretToken.Err)
	}
	return oauthDetail{
		id:          id,
		secretToken: secretToken,
	}, nil
}

func (api *APIManager) refreshOAuthToken(destName, workspaceId string, oAuthDetail oauthDetail) error {
	refTokenParams := &oauth.RefreshTokenParams{
		Secret:          oAuthDetail.secretToken.Account.Secret,
		WorkspaceId:     workspaceId,
		AccountId:       oAuthDetail.id,
		DestDefName:     destName,
		EventNamePrefix: "refresh_token",
	}
	statusCode, refreshResponse := api.OAuth.RefreshToken(refTokenParams)
	if statusCode != http.StatusOK {
		var refreshRespErr string
		if refreshResponse != nil {
			refreshRespErr = refreshResponse.Err
		}
		return fmt.Errorf("[%v] Failed to refresh token for destination in workspace(%v) & account(%v) with %v", destName, workspaceId, oAuthDetail.id, refreshRespErr)
	}
	return nil
}
