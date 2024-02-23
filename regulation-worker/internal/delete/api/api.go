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

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/services/oauth"
	oauthv2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	"github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/utils/httputil"
)

var (
	pkgLogger             = logger.NewLogger().Child("api")
	SupportedDestinations = []string{"BRAZE", "AM", "INTERCOM", "CLEVERTAP", "AF", "MP", "GA", "ITERABLE", "ENGAGE", "CUSTIFY", "SENDGRID", "SPRIG"}
)

type APIManager struct {
	Client                       *http.Client
	DestTransformURL             string
	OAuth                        oauth.Authorizer
	MaxOAuthRefreshRetryAttempts int
	TransformerFeaturesService   transformer.FeaturesService
	IsOAuthV2Enabled             bool
}

type oauthDetail struct {
	secretToken *oauth.AuthResponse
	id          string
}

func (m *APIManager) GetSupportedDestinations() []string {
	// Wait for transformer features to be available
	<-m.TransformerFeaturesService.Wait()
	destinations := m.TransformerFeaturesService.Regulations()
	if len(destinations) == 0 {
		// Fallback to default supported destinations
		destinations = SupportedDestinations
	}
	return destinations
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
	if isOAuthEnabled && !api.IsOAuthV2Enabled {
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

	if isOAuthEnabled && api.IsOAuthV2Enabled {
		dest := backendconfig.DestinationT{
			ID:     destination.DestinationID,
			Config: destination.Config,
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				Config: destination.DestDefConfig,
			},
			Name: destination.Name,
		}
		req = req.WithContext(context.WithValue(req.Context(), oauthv2.DestKey, dest))
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
		// TODO: re-check this
		if api.IsOAuthV2Enabled && resp.StatusCode >= http.StatusInternalServerError {
			// TODO: Add log and stat here
			return api.deleteWithRetry(ctx, job, destination, currentOauthRetryAttempt+1)
		}
		return model.JobStatus{Status: model.JobStatusFailed, Error: err}
	}
	defer func() { httputil.CloseResponse(resp) }()
	return api.PostResponse(ctx, PostResponseParams{
		destination:              destination,
		job:                      job,
		isOAuthEnabled:           isOAuthEnabled,
		currentOAuthRetryAttempt: currentOauthRetryAttempt,
		oAuthDetail:              oAuthDetail,
		resp:                     resp,
	})
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

func getOAuthErrorJob(jobResponses []JobRespSchema) (JobRespSchema, bool) {
	return lo.Find(jobResponses, func(item JobRespSchema) bool {
		return lo.Contains([]string{oauth.AUTH_STATUS_INACTIVE, oauth.REFRESH_TOKEN}, item.AuthErrorCategory)
	})
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
		AccountId:   id,
		WorkspaceId: workspaceId,
		DestDefName: destDetail.Name,
	})
	if tokenStatusCode != http.StatusOK {
		return oauthDetail{}, fmt.Errorf("[%s][FetchToken] Error in Token Fetch statusCode: %d\t error: %s", destDetail.Name, tokenStatusCode, secretToken.ErrorMessage)
	}
	return oauthDetail{
		id:          id,
		secretToken: secretToken,
	}, nil
}

func (api *APIManager) inactivateAuthStatus(destination *model.Destination, job model.Job, oAuthDetail oauthDetail) (jobStatus model.JobStatus) {
	dest := &backendconfig.DestinationT{
		ID:     destination.DestinationID,
		Config: destination.Config,
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			Name:   destination.Name,
			Config: destination.DestDefConfig,
		},
	}
	_, resp := api.OAuth.AuthStatusToggle(&oauth.AuthStatusToggleParams{
		Destination:     dest,
		WorkspaceId:     job.WorkspaceID,
		RudderAccountId: oAuthDetail.id,
		AuthStatus:      oauth.AuthStatusInactive,
	})
	jobStatus.Status = model.JobStatusAborted
	jobStatus.Error = fmt.Errorf(resp)
	return jobStatus
}

func (api *APIManager) refreshOAuthToken(destination *model.Destination, job model.Job, oAuthDetail oauthDetail) error {
	refTokenParams := &oauth.RefreshTokenParams{
		Secret:      oAuthDetail.secretToken.Account.Secret,
		WorkspaceId: job.WorkspaceID,
		AccountId:   oAuthDetail.id,
		DestDefName: destination.Name,
	}
	statusCode, refreshResponse := api.OAuth.RefreshToken(refTokenParams)
	if statusCode != http.StatusOK {
		if refreshResponse.Err == oauth.REF_TOKEN_INVALID_GRANT {
			// authStatus should be made inactive
			api.inactivateAuthStatus(destination, job, oAuthDetail)
			return fmt.Errorf(refreshResponse.ErrorMessage)
		}

		var refreshRespErr string
		if refreshResponse != nil {
			refreshRespErr = refreshResponse.ErrorMessage
		}
		return fmt.Errorf("[%v] Failed to refresh token for destination in workspace(%v) & account(%v) with %v", destination.Name, job.WorkspaceID, oAuthDetail.id, refreshRespErr)
	}
	return nil
}

type PostResponseParams struct {
	destination              model.Destination
	isOAuthEnabled           bool
	currentOAuthRetryAttempt int
	job                      model.Job
	oAuthDetail              oauthDetail
	resp                     *http.Response
}

func (api *APIManager) PostResponse(ctx context.Context, params PostResponseParams) model.JobStatus {
	var err error

	bodyBytes, err := io.ReadAll(params.resp.Body)
	if err != nil {
		return model.JobStatus{Status: model.JobStatusFailed, Error: err}
	}

	var jobResp []JobRespSchema
	if err := json.Unmarshal(bodyBytes, &jobResp); err != nil {
		return model.JobStatus{Status: model.JobStatusFailed, Error: err}
	}
	jobStatus := getJobStatus(params.resp.StatusCode, jobResp)
	pkgLogger.Debugf("[%v] Job: %v, JobStatus: %v", params.destination.Name, params.job.ID, jobStatus)

	oauthErrJob, oauthErrJobFound := getOAuthErrorJob(jobResp)

	// old oauth handling
	if oauthErrJobFound && params.isOAuthEnabled && !api.IsOAuthV2Enabled {
		if oauthErrJob.AuthErrorCategory == oauth.AUTH_STATUS_INACTIVE {
			return api.inactivateAuthStatus(&params.destination, params.job, params.oAuthDetail)
		}
		if oauthErrJob.AuthErrorCategory == oauth.REFRESH_TOKEN && params.currentOAuthRetryAttempt < api.MaxOAuthRefreshRetryAttempts {
			err = api.refreshOAuthToken(&params.destination, params.job, params.oAuthDetail)
			if err != nil {
				pkgLogger.Error(err)
				return model.JobStatus{Status: model.JobStatusFailed, Error: err}
			}
			// retry the request
			pkgLogger.Infof("[%v] Retrying deleteRequest job(id: %v) for the whole batch, RetryAttempt: %v", params.destination.Name, params.job.ID, params.currentOAuthRetryAttempt+1)
			return api.deleteWithRetry(ctx, params.job, params.destination, params.currentOAuthRetryAttempt+1)
		}
	}
	// new oauth handling
	if oauthErrJob.AuthErrorCategory == oauth.REFRESH_TOKEN && params.isOAuthEnabled && api.IsOAuthV2Enabled {
		// All the handling related to OAuth has been done(inside api.Client.Do() itself)!
		// retry the request
		pkgLogger.Infof("[%v] Retrying deleteRequest job(id: %v) for the whole batch, RetryAttempt: %v", params.destination.Name, params.job.ID, params.currentOAuthRetryAttempt+1)
		return api.deleteWithRetry(ctx, params.job, params.destination, params.currentOAuthRetryAttempt+1)
	}
	return jobStatus
}
