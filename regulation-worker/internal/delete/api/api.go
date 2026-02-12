package api

// This is simply going to make any API call to transfomer as per the API spec here: https://www.notion.so/rudderstacks/GDPR-Transformer-API-Spec-c3303b5b70c64225815d56c7767f8d22
// to get deletion done.
// called by delete/deleteSvc with (model.Job, model.Destination).
// returns final status,error ({successful, failure}, err)
import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/samber/lo"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
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
	MaxOAuthRefreshRetryAttempts int
	TransformerFeaturesService   transformer.FeaturesService
}

func GetAuthErrorCategoryFromResponse(bodyBytes []byte) (string, error) {
	var jobResp []JobRespSchema
	if err := jsonrs.Unmarshal(bodyBytes, &jobResp); err != nil {
		pkgLogger.Errorn("Unmarshal error", obskit.Error(err),
			logger.NewStringField("valueToUnmarshal", string(bodyBytes)))
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
	pkgLogger.Debugn("Deleting job from API destination",
		logger.NewIntField("jobID", int64(job.ID)),
		obskit.DestinationType(destination.Name))
	method := http.MethodPost
	endpoint := "/deleteUsers"
	url := fmt.Sprint(m.DestTransformURL, endpoint)

	bodySchema := mapJobToPayload(job, strings.ToLower(destination.Name), destination.Config)
	if pkgLogger.IsDebugLevel() { // reflection might be an expensive operation, checking if we need to print it
		pkgLogger.Debugn("Payload", logger.NewStringField("payload", fmt.Sprintf("%#v", bodySchema))) // nolint:forbidigo
	}

	reqBody, err := jsonrs.Marshal(bodySchema)
	if err != nil {
		return model.JobStatus{Status: model.JobStatusFailed, Error: err}
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(reqBody))
	if err != nil {
		return model.JobStatus{Status: model.JobStatusFailed, Error: err}
	}
	req.Header.Set("Content-Type", "application/json")

	// check if OAuth destination
	dest := &backendconfig.DestinationT{
		ID:          destination.DestinationID,
		Config:      destination.Config,
		WorkspaceID: job.WorkspaceID,
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			Name:   destination.Name,
			Config: destination.DestDefConfig,
		},
	}
	isOAuth, err := dest.IsOAuthDestination(common.RudderFlowDelete)
	if err != nil {
		pkgLogger.Errorn("deleteWithRetry IsOAuthDestination error", obskit.Error(err))
		return model.JobStatus{Status: model.JobStatusFailed, Error: err}
	}

	req = req.WithContext(cntx.CtxWithDestInfo(req.Context(), dest))

	defer stats.Default.NewTaggedStat(
		"regulation_worker_cleaning_time",
		stats.TimerType,
		stats.Tags{
			"destinationId":   job.DestinationID,
			"workspaceId":     job.WorkspaceID,
			"destinationType": destination.Name,
			"jobType":         "api",
		}).RecordDuration()()

	resp, err := m.Client.Do(req)
	if err != nil {
		if os.IsTimeout(err) {
			stats.Default.NewTaggedStat(
				"regulation_worker_delete_api_timeout",
				stats.CountType,
				stats.Tags{
					"destinationId":   job.DestinationID,
					"workspaceId":     job.WorkspaceID,
					"destinationType": destination.Name,
					"jobType":         "api",
				}).Count(1)
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
	if isOAuth {
		var transportResponse oauthv2.TransportResponse
		// We don't need to handle it, as we can receive a string response even before executing OAuth operations like Refresh Token.
		// It's acceptable if the structure of bodyBytes doesn't match the oauthv2.TransportResponse struct.
		err = jsonrs.Unmarshal(bodyBytes, &transportResponse)
		if err == nil && transportResponse.OriginalResponse != "" {
			// most probably it was thrown before postRoundTrip through interceptor itself
			respBodyBytes = []byte(transportResponse.OriginalResponse) // setting original response
		}
		if transportResponse.InterceptorResponse.StatusCode > 0 {
			respStatusCode = transportResponse.InterceptorResponse.StatusCode
		}
		if transportResponse.InterceptorResponse.Response != "" {
			pkgLogger.Debugn("Actual response received", logger.NewStringField("response", string(respBodyBytes)))
			// Update the same error response to all as the response received would be []JobRespSchema
			respBodyBytes, err = sjson.SetBytes(respBodyBytes, "#.error", []byte(transportResponse.InterceptorResponse.Response))
			if err != nil {
				return model.JobStatus{Status: model.JobStatusFailed, Error: err}
			}
		}
	}
	return m.PostResponse(ctx, PostResponseParams{
		destination:              destination,
		job:                      job,
		isOAuthEnabled:           isOAuth,
		responseBodyBytes:        respBodyBytes,
		responseStatusCode:       respStatusCode,
		currentOAuthRetryAttempt: currentOauthRetryAttempt,
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
		return model.JobStatus{Status: model.JobStatusFailed, Error: fmt.Errorf("error: code: %d, body: %+v", statusCode, jobResp)}
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
		return lo.Contains([]string{common.CategoryAuthStatusInactive, common.CategoryRefreshToken}, item.AuthErrorCategory)
	})
}

type PostResponseParams struct {
	destination              model.Destination
	isOAuthEnabled           bool
	currentOAuthRetryAttempt int
	job                      model.Job
	responseBodyBytes        []byte
	responseStatusCode       int
}

func (m *APIManager) PostResponse(ctx context.Context, params PostResponseParams) model.JobStatus {
	var jobResp []JobRespSchema
	if err := jsonrs.Unmarshal(params.responseBodyBytes, &jobResp); err != nil {
		pkgLogger.Errorn("Unmarshal error", obskit.Error(err),
			logger.NewStringField("valueToUnmarshal", string(params.responseBodyBytes)))
		return model.JobStatus{Status: model.JobStatusFailed, Error: fmt.Errorf("failed to parse authErrorCategory from response: %s", string(params.responseBodyBytes))}
	}

	jobStatus := getJobStatus(params.responseStatusCode, jobResp)
	pkgLogger.Debugn("Job and JobStatus",
		obskit.DestinationType(params.destination.Name),
		logger.NewIntField("jobId", int64(params.job.ID)),
		logger.NewStringField("jobStatus", jobStatus.String()))

	var authErrorCategory string
	if oauthErrorJob, ok := getOAuthErrorJob(jobResp); ok {
		authErrorCategory = oauthErrorJob.AuthErrorCategory
	}
	// new oauth handling
	if params.isOAuthEnabled {
		if authErrorCategory == common.CategoryRefreshToken && params.currentOAuthRetryAttempt < m.MaxOAuthRefreshRetryAttempts {
			// All the handling related to OAuth has been done(inside api.Client.Do() itself)!
			// retry the request
			pkgLogger.Infon("Retrying deleteRequest for the whole batch",
				logger.NewStringField("destinationName", params.destination.Name),
				logger.NewIntField("jobId", int64(params.job.ID)),
				logger.NewIntField("retryAttempt", int64(params.currentOAuthRetryAttempt+1)))
			return m.deleteWithRetry(ctx, params.job, params.destination, params.currentOAuthRetryAttempt+1)
		}
		if authErrorCategory == common.CategoryAuthStatusInactive {
			// Abort the regulation request
			return model.JobStatus{Status: model.JobStatusAborted, Error: errors.New(string(params.responseBodyBytes))}
		}
	}
	return jobStatus
}
