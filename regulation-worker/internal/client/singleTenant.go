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

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var pkgLogger = logger.NewLogger().Child("client")

type SingleTenantJobAPI struct {
	Client         *http.Client
	WorkspaceID    string
	URLPrefix      string
	WorkspaceToken string
}

// Get sends http request with workspaceID in the url and receives a json payload
// which is decoded using schema and then mapped from schema to internal model.Job struct,
// which is actually returned.
func (j *SingleTenantJobAPI) Get(ctx context.Context) (model.Job, error) {
	pkgLogger.Debugf("making http request to regulation manager to get new job")

	url := fmt.Sprintf("%s/dataplane/workspaces/%s/regulations/workerJobs", j.URLPrefix, j.WorkspaceID)
	pkgLogger.Debugf("making GET request to URL: %v", url)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		pkgLogger.Errorf("error while create new http request: %v", err)
		return model.Job{}, err
	}

	req.SetBasicAuth(j.WorkspaceToken, "")
	req.Header.Set("Content-Type", "application/json")

	resp, err := j.Client.Do(req)
	if os.IsTimeout(err) {
		stats.Default.NewTaggedStat("regulation_manager.request_timeout", stats.CountType, stats.Tags{"workspace": j.WorkspaceID}).Count(1)
		return model.Job{}, model.ErrRequestTimeout
	}
	if err != nil {
		return model.Job{}, err
	}
	defer func() { httputil.CloseResponse(resp) }()
	pkgLogger.Debugf("obtained response code: %v", resp.StatusCode, "response body: ", resp.Body)

	// if successful

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if resp.StatusCode == http.StatusNoContent {
			pkgLogger.Debugf("no runnable job found")
			return model.Job{}, model.ErrNoRunnableJob
		}

		var jobSchema singleTenantJobSchema
		if err := json.NewDecoder(resp.Body).Decode(&jobSchema); err != nil {
			pkgLogger.Errorf("error while decoding response body: %v", err)
			return model.Job{}, fmt.Errorf("error while decoding job: %w", err)
		}

		userCountPerJob := stats.Default.NewTaggedStat("user_count_per_job", stats.CountType, stats.Tags{"jobId": jobSchema.JobID, "workspaceId": j.WorkspaceID})
		userCountPerJob.Count(len(jobSchema.UserAttributes))

		job, err := mapPayloadToJob(jobSchema.JobID, j.WorkspaceID, jobSchema.DestinationID, jobSchema.UserAttributes)
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
func (j *SingleTenantJobAPI) UpdateStatus(ctx context.Context, status model.JobStatus, jobID int) error {
	pkgLogger.Debugf("sending PATCH request to update job status for jobId: ", jobID, "with status: %v", status)
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	url := fmt.Sprintf("%s/dataplane/workspaces/%s/regulations/workerJobs/%s", j.URLPrefix, j.WorkspaceID, fmt.Sprint(jobID))
	pkgLogger.Debugf("sending request to URL: %v", url)

	statusSchema := statusJobSchema{
		Status: string(status),
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
	pkgLogger.Debugf("sending request: %v", req)
	req.SetBasicAuth(j.WorkspaceToken, "")
	req.Header.Set("Content-Type", "application/json")

	resp, err := j.Client.Do(req)
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

func mapPayloadToJob(jobID, workspaceID, destinationID string, userAttributes []userAttributesSchema) (model.Job, error) {
	usrAttribute := make([]model.User, len(userAttributes))
	for i, usrAttr := range userAttributes {
		usrAttribute[i].Attributes = make(map[string]string)
		for key, value := range usrAttr {
			if key == "userId" {
				usrAttribute[i].ID = value
			} else {
				usrAttribute[i].Attributes[key] = value
			}
		}
	}
	ID, err := strconv.Atoi(jobID)
	if err != nil {
		pkgLogger.Errorf("error while getting jobId: %v", err)
		return model.Job{}, fmt.Errorf("error while get JobID:%w", err)
	}

	return model.Job{
		ID:            ID,
		WorkspaceID:   workspaceID,
		DestinationID: destinationID,
		Status:        model.JobStatusRunning,
		Users:         usrAttribute,
	}, nil
}
