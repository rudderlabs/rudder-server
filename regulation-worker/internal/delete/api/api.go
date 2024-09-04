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
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/services/oauth"
	oauthv2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	cntx "github.com/rudderlabs/rudder-server/services/oauth/v2/context"
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

func GetAuthErrorCategoryFromResponse(bodyBytes []byte) (string, error) {
	var jobResp []JobRespSchema
	if err := json.Unmarshal(bodyBytes, &jobResp); err != nil {
		pkgLogger.Errorf("unmarshal error: %s\tvalue to unmarshal: %s\n", err.Error(), string(bodyBytes))
		return "", fmt.Errorf("failed to parse authErrorCategory from response: %s", string(bodyBytes))
	}
	if oauthErrJob, oauthErrJobFound := getOAuthErrorJob(jobResp); oauthErrJobFound {
		return oauthErrJob.AuthErrorCategory, nil
	}
	return "", nil
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

func (m *APIManager) deleteWithRetry(ctx context.Context, job model.Job, destination model.Destination, currentOauthRetryAttempt int) model.JobStatus {
	if currentOauthRetryAttempt > m.MaxOAuthRefreshRetryAttempts {
		return job.Status
	}
	pkgLogger.Debugf("deleting: %v", job, " from API destination: %v", destination.Name)
	method := http.MethodPost
	endpoint := "/deleteUsers"
	url := fmt.Sprint(m.DestTransformURL, endpoint)

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
	dest := &oauthv2.DestinationInfo{
		WorkspaceID:      job.WorkspaceID,
		DefinitionName:   destination.Name,
		ID:               destination.DestinationID,
		Config:           destination.Config,
		DefinitionConfig: destination.DestDefConfig,
	}
	isOAuth, err := dest.IsOAuthDestination(common.RudderFlowDelete)
	if err != nil {
		pkgLogger.Error(err)
		return model.JobStatus{Status: model.JobStatusFailed, Error: err}
	}
	var oAuthDetail oauthDetail
	if isOAuth && !m.IsOAuthV2Enabled {
		oAuthDetail, err = m.getOAuthDetail(&destination, job.WorkspaceID)
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

	if isOAuth && m.IsOAuthV2Enabled {
		req = req.WithContext(cntx.CtxWithDestInfo(req.Context(), dest))
	}

	defer stats.Default.NewTaggedStat(
		"regulation_worker_cleaning_time",
		stats.TimerType,
		stats.Tags{
			"destinationId": job.DestinationID,
			"workspaceId":   job.WorkspaceID,
			"jobType":       "api",
		}).RecordDuration()()

	resp, err := m.Client.Do(req)
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
	respStatusCode := resp.StatusCode
	respBodyBytes := bodyBytes
	// Post response work to be done for OAuthV2
	if isOAuth && m.IsOAuthV2Enabled {
		var transportResponse oauthv2.TransportResponse
		// We don't need to handle it, as we can receive a string response even before executing OAuth operations like Refresh Token or Auth Status Toggle.
		// It's acceptable if the structure of bodyBytes doesn't match the oauthv2.TransportResponse struct.
		err = json.Unmarshal(bodyBytes, &transportResponse)
		if err == nil && transportResponse.OriginalResponse != "" {
			// most probably it was thrown before postRoundTrip through interceptor itself
			respBodyBytes = []byte(transportResponse.OriginalResponse) // setting original response
		}
		if transportResponse.InterceptorResponse.StatusCode > 0 {
			respStatusCode = transportResponse.InterceptorResponse.StatusCode
		}
		if transportResponse.InterceptorResponse.Response != "" {
			pkgLogger.Debugf("Actual response received: %v", respBodyBytes)
			// Update the same error response to all as the response received would be []JobRespSchema
			respBodyBytes, err = sjson.SetRawBytes(respBodyBytes, "#.error", []byte(transportResponse.InterceptorResponse.Response))
			if err != nil {
				return model.JobStatus{Status: model.JobStatusFailed, Error: err}
			}
		}
	}
	return m.PostResponse(ctx, PostResponseParams{
		destination:              destination,
		job:                      job,
		isOAuthEnabled:           isOAuth,
		currentOAuthRetryAttempt: currentOauthRetryAttempt,
		oAuthDetail:              oAuthDetail,
		responseBodyBytes:        respBodyBytes,
		responseStatusCode:       respStatusCode,
	})
}

// prepares payload based on (job,destDetail) & make an API call to transformer.
// gets (status, failure_reason) which is converted to appropriate model.Error & returned to caller.
func (m *APIManager) Delete(ctx context.Context, job model.Job, destination model.Destination) model.JobStatus {
	return m.deleteWithRetry(ctx, job, destination, 0)
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

func (m *APIManager) getOAuthDetail(destDetail *model.Destination, workspaceId string) (oauthDetail, error) {
	id := oauth.GetAccountId(destDetail.Config, oauth.DeleteAccountIdKey)
	if strings.TrimSpace(id) == "" {
		return oauthDetail{}, fmt.Errorf("[%v] Delete account ID key (%v) is not present for destination: %v", destDetail.Name, oauth.DeleteAccountIdKey, destDetail.DestinationID)
	}
	tokenStatusCode, secretToken := m.OAuth.FetchToken(&oauth.RefreshTokenParams{
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

func (m *APIManager) inactivateAuthStatus(destination *model.Destination, job model.Job, oAuthDetail oauthDetail) (jobStatus model.JobStatus) {
	dest := &backendconfig.DestinationT{
		ID:     destination.DestinationID,
		Config: destination.Config,
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			Name:   destination.Name,
			Config: destination.DestDefConfig,
		},
	}
	_, resp := m.OAuth.AuthStatusToggle(&oauth.AuthStatusToggleParams{
		Destination:     dest,
		WorkspaceId:     job.WorkspaceID,
		RudderAccountId: oAuthDetail.id,
		AuthStatus:      oauth.AuthStatusInactive,
	})
	jobStatus.Status = model.JobStatusAborted
	jobStatus.Error = errors.New(resp)
	return jobStatus
}

func (m *APIManager) refreshOAuthToken(destination *model.Destination, job model.Job, oAuthDetail oauthDetail) error {
	refTokenParams := &oauth.RefreshTokenParams{
		Secret:      oAuthDetail.secretToken.Account.Secret,
		WorkspaceId: job.WorkspaceID,
		AccountId:   oAuthDetail.id,
		DestDefName: destination.Name,
	}
	statusCode, refreshResponse := m.OAuth.RefreshToken(refTokenParams)
	if statusCode != http.StatusOK {
		if refreshResponse.Err == oauth.REF_TOKEN_INVALID_GRANT {
			// authStatus should be made inactive
			m.inactivateAuthStatus(destination, job, oAuthDetail)
			return errors.New(refreshResponse.ErrorMessage)
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
	responseBodyBytes        []byte
	responseStatusCode       int
}

func (m *APIManager) PostResponse(ctx context.Context, params PostResponseParams) model.JobStatus {
	var jobResp []JobRespSchema
	if err := json.Unmarshal(params.responseBodyBytes, &jobResp); err != nil {
		pkgLogger.Errorf("unmarshal error: %s\tvalue to unmarshal: %s\n", err.Error(), string(params.responseBodyBytes))
		return model.JobStatus{Status: model.JobStatusFailed, Error: fmt.Errorf("failed to parse authErrorCategory from response: %s", string(params.responseBodyBytes))}
	}

	jobStatus := getJobStatus(params.responseStatusCode, jobResp)
	pkgLogger.Debugf("[%v] Job: %v, JobStatus: %v", params.destination.Name, params.job.ID, jobStatus)

	var authErrorCategory string
	if oauthErrorJob, ok := getOAuthErrorJob(jobResp); ok {
		authErrorCategory = oauthErrorJob.AuthErrorCategory
	}
	// old oauth handling
	if authErrorCategory != "" && params.isOAuthEnabled && !m.IsOAuthV2Enabled {
		if authErrorCategory == oauth.AUTH_STATUS_INACTIVE {
			return m.inactivateAuthStatus(&params.destination, params.job, params.oAuthDetail)
		}
		if authErrorCategory == oauth.REFRESH_TOKEN && params.currentOAuthRetryAttempt < m.MaxOAuthRefreshRetryAttempts {
			if err := m.refreshOAuthToken(&params.destination, params.job, params.oAuthDetail); err != nil {
				pkgLogger.Errorn("Error while refreshing authToken", obskit.Error(err))
				return model.JobStatus{Status: model.JobStatusFailed, Error: err}
			}
			// retry the request
			pkgLogger.Infon("[%v] Retrying deleteRequest job(id: %v) for the whole batch, RetryAttempt: %v", logger.NewStringField("destinationName", params.destination.Name), logger.NewIntField("jobId", int64(params.job.ID)), logger.NewIntField("retryAttempt", int64(params.currentOAuthRetryAttempt+1)))
			return m.deleteWithRetry(ctx, params.job, params.destination, params.currentOAuthRetryAttempt+1)
		}
	}
	// new oauth handling
	if params.isOAuthEnabled && m.IsOAuthV2Enabled {
		if authErrorCategory == common.CategoryRefreshToken {
			// All the handling related to OAuth has been done(inside api.Client.Do() itself)!
			// retry the request
			pkgLogger.Infon("[%v] Retrying deleteRequest job(id: %v) for the whole batch, RetryAttempt: %v", logger.NewStringField("destinationName", params.destination.Name), logger.NewIntField("jobId", int64(params.job.ID)), logger.NewIntField("retryAttempt", int64(params.currentOAuthRetryAttempt+1)))
			return m.deleteWithRetry(ctx, params.job, params.destination, params.currentOAuthRetryAttempt+1)
		}
		if authErrorCategory == common.CategoryAuthStatusInactive {
			// Abort the regulation request
			return model.JobStatus{Status: model.JobStatusAborted, Error: errors.New(string(params.responseBodyBytes))}
		}
	}
	return jobStatus
}
