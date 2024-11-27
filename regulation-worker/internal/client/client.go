package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

var pkgLogger = logger.NewLogger().Child("client")

type JobAPI struct {
	Client    *http.Client
	URLPrefix string
	Identity  identity.Identifier
}

func (j *JobAPI) URL() string {
	switch j.Identity.Type() {
	case deployment.MultiTenantType:
		return fmt.Sprintf("%s/dataplane/namespaces/%s/regulations/workerJobs", j.URLPrefix, j.Identity.ID())
	default:
		return fmt.Sprintf("%s/dataplane/workspaces/%s/regulations/workerJobs", j.URLPrefix, j.Identity.ID())
	}
}

// Get sends http request with ID in the url and receives a json payload
// which is decoded using schema and then mapped from schema to internal model.Job struct,
// which is actually returned.
func (j *JobAPI) Get(ctx context.Context) (model.Job, error) {
	pkgLogger.Debugf("making http request to regulation manager to get new job")
	url := j.URL()
	pkgLogger.Debugf("making GET request to URL: %v", url)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		pkgLogger.Errorf("error while create new http request: %v", err)
		return model.Job{}, err
	}
	req.SetBasicAuth(j.Identity.BasicAuth())
	req.Header.Set("Content-Type", "application/json")
	reqTime := time.Now()
	resp, err := j.Client.Do(req)
	stats.Default.NewTaggedStat("regulation_manager.request_time", stats.TimerType, stats.Tags{"op": "get"}).Since(reqTime)
	if os.IsTimeout(err) {
		stats.Default.NewStat("regulation_manager.request_timeout", stats.CountType).Count(1)
		return model.Job{}, model.ErrRequestTimeout
	}
	if err != nil {
		return model.Job{}, err
	}
	defer func() { httputil.CloseResponse(resp) }()
	pkgLogger.Debugf("obtained response code: %v with resp body %v", resp.StatusCode, resp.Body)

	// if successful
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if resp.StatusCode == http.StatusNoContent {
			pkgLogger.Debugf("no runnable job found")
			return model.Job{}, model.ErrNoRunnableJob
		}

		var jobSchema jobSchema
		if err := json.NewDecoder(resp.Body).Decode(&jobSchema); err != nil {
			pkgLogger.Errorf("error while decoding response body: %v", err)
			return model.Job{}, fmt.Errorf("error while decoding job: %w", err)
		}

		userCountPerJob := stats.Default.NewTaggedStat(
			"regulation_worker_user_count_per_job",
			stats.CountType,
			stats.Tags{
				"destinationId": jobSchema.DestinationID,
			})
		userCountPerJob.Count(len(jobSchema.UserAttributes))

		job, err := mapPayloadToJob(jobSchema)
		if err != nil {
			pkgLogger.Errorf("error while mapping response payload to job: %v", err)
			return model.Job{}, fmt.Errorf("error while getting job: %w", err)
		}

		pkgLogger.Debugf("obtained job: %v", job)
		return job, nil

	} else {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			pkgLogger.Errorf("error while reading response body: %v", err)
			return model.Job{}, fmt.Errorf("error while reading response body: %w", err)
		}
		pkgLogger.Debugf("obtained response body: %v", string(body))

		return model.Job{}, fmt.Errorf("unexpected response code: %d", resp.StatusCode)
	}
}

// UpdateStatus marshals status into appropriate status schema, and sent as payload
// checked for returned status code.
func (j *JobAPI) UpdateStatus(ctx context.Context, status model.JobStatus, jobID int) error {
	pkgLogger.Debugf("sending PATCH request to update job status for jobId: ", jobID, "with status: %v", status)
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	url := fmt.Sprintf("%s/%d", j.URL(), jobID)
	pkgLogger.Debugf("sending request to URL: %v", url)
	statusSchema := statusJobSchema{
		Status: string(status.Status),
	}
	if status.Error != nil {
		statusSchema.Reason = status.Error.Error()
	}
	body, err := json.Marshal(statusSchema)
	if err != nil {
		pkgLogger.Errorf("error while marshalling status schema: %v", err)
		return fmt.Errorf("error while marshalling status: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.SetBasicAuth(j.Identity.BasicAuth())
	req.Header.Set("Content-Type", "application/json")
	reqTime := time.Now()
	resp, err := j.Client.Do(req)
	stats.Default.NewTaggedStat("regulation_manager.request_time", stats.TimerType, stats.Tags{"op": "updateStatus"}).Since(reqTime)
	if os.IsTimeout(err) {
		stats.Default.NewStat("regulation_manager.request_timeout", stats.CountType).Count(1)
		return model.ErrRequestTimeout
	}
	if err != nil {
		return err
	}
	defer func() { httputil.CloseResponse(resp) }()

	pkgLogger.Debugf("response code: %v", resp.StatusCode, "response body: %v", resp.Body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	} else {
		pkgLogger.Errorf("update status failed with status code: %v", resp.StatusCode)
		return fmt.Errorf("update status failed with status code: %d", resp.StatusCode)
	}
}

func mapPayloadToJob(wjs jobSchema) (model.Job, error) {
	usrAttribute := make([]model.User, len(wjs.UserAttributes))
	for i, usrAttr := range wjs.UserAttributes {
		usrAttribute[i].Attributes = make(map[string]string)
		for key, value := range usrAttr {
			if key == "userId" {
				usrAttribute[i].ID = value
			} else {
				usrAttribute[i].Attributes[key] = value
			}
		}
	}
	jobID, err := strconv.Atoi(wjs.JobID)
	if err != nil {
		pkgLogger.Errorf("error while getting jobId: %v", err)
		return model.Job{}, fmt.Errorf("error while get JobID:%w", err)
	}

	return model.Job{
		ID:             jobID,
		WorkspaceID:    wjs.WorkspaceId,
		DestinationID:  wjs.DestinationID,
		Status:         model.JobStatus{Status: model.JobStatusRunning},
		Users:          usrAttribute,
		FailedAttempts: wjs.FailedAttempts,
	}, nil
}
