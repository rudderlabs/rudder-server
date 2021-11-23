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
)

//prepares payload based on (job,destDetail) & make an API call to transformer.
//gets (status, failure_reason) which is converted to appropriate model.Error & returned to caller.
func Delete(ctx context.Context, job model.Job, destConfig map[string]interface{}, destName string) error {

	method := "DELETE"

	url := "d-transformer/delete-users"

	bodySchema := mapJobToPayload(job, destName, destConfig)

	reqBody, err := json.Marshal(bodySchema)
	if err != nil {
		return fmt.Errorf("error while preparing API deletion request body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(reqBody))
	if err != nil {
		return err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	var jobResp jobRespSchema
	if err := json.NewDecoder(resp.Body).Decode(&jobResp); err != nil {
		return fmt.Errorf("error while decoding API deletion response: %w", err)
	}

	if resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("invalid format/credentials: %w", jobResp.Error)
	} else if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed {
		return fmt.Errorf("deletion not supported: %w", jobResp.Error)
	} else if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusRequestTimeout || (resp.StatusCode >= 500 && resp.StatusCode < 600) {
		return fmt.Errorf("failed")
	}
	return nil
}

func mapJobToPayload(job model.Job, destName string, destConfig map[string]interface{}) apiDeletionPayloadSchema {
	uas := make([]userAttributesSchema, len(job.UserAttributes))
	for i, ua := range job.UserAttributes {
		uas[i] = userAttributesSchema{
			UserID: ua.UserID,

			Phone: ua.Phone,
			Email: ua.Email,
		}
	}

	return apiDeletionPayloadSchema{
		JobID:          fmt.Sprintf("%d", job.ID),
		DestType:       destName,
		Config:         destConfig,
		UserAttributes: uas,
	}
}
