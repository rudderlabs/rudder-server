package api

//This is simply going to make any API call to transfomer as per the API spec here: https://www.notion.so/rudderstacks/GDPR-Transformer-API-Spec-c3303b5b70c64225815d56c7767f8d22
//to get deletion done.
//called by delete/deleteSvc with (model.Job, model.Destination).
//returns final status,error ({successful, failure}, err)
import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var (
	pkgLogger             = logger.NewLogger().Child("api")
	supportedDestinations = []string{"BRAZE", "AM", "INTERCOM"}
)

type APIManager struct {
	Client           *http.Client
	DestTransformURL string
}

//prepares payload based on (job,destDetail) & make an API call to transformer.
//gets (status, failure_reason) which is converted to appropriate model.Error & returned to caller.
func (api *APIManager) Delete(ctx context.Context, job model.Job, destConfig map[string]interface{}, destName string) model.JobStatus {
	pkgLogger.Debugf("deleting: %v", job, " from API destination: %v", destName)
	method := "POST"
	endpoint := "/delete-users"
	url := fmt.Sprint(api.DestTransformURL, endpoint)
	pkgLogger.Debugf("transformer url: %v", url)

	bodySchema := mapJobToPayload(job, destName, destConfig)

	reqBody, err := json.Marshal(bodySchema)
	if err != nil {
		pkgLogger.Errorf("error while marshalling job request body: %v", err)
		return model.JobStatusFailed
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(reqBody))
	if err != nil {
		pkgLogger.Errorf("error while create new request: %v", err)
		return model.JobStatusFailed
	}
	req.Header.Set("Content-Type", "application/json")

	fileCleaningTime := stats.NewTaggedStat("file_cleaning_time", stats.TimerType, stats.Tags{
		"jobId":       fmt.Sprintf("%d", job.ID),
		"workspaceId": job.WorkspaceID,
		"destType":    "api",
		"destName":    destName,
	})
	fileCleaningTime.Start()
	defer fileCleaningTime.End()

	pkgLogger.Debugf("sending request: %v", req)
	resp, err := api.Client.Do(req)
	if err != nil {
		pkgLogger.Errorf("error while making http request: %v", err)
		return model.JobStatusFailed
	}
	defer resp.Body.Close()
	pkgLogger.Debugf("response status code: %v", resp.StatusCode, " response body: %v", resp.Body)

	var jobResp JobRespSchema
	if err := json.NewDecoder(resp.Body).Decode(&jobResp); err != nil {
		pkgLogger.Errorf("error while decoding response body: %v", err)
		return model.JobStatusFailed
	}
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

func (bm *APIManager) GetSupportedDestinations() []string {

	return supportedDestinations
}

func mapJobToPayload(job model.Job, destName string, destConfig map[string]interface{}) []apiDeletionPayloadSchema {
	uas := make([]userAttributesSchema, len(job.UserAttributes))
	for i, ua := range job.UserAttributes {
		uas[i] = userAttributesSchema{
			UserID: ua.UserID,

			Phone: ua.Phone,
			Email: ua.Email,
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
