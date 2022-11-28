package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var pkgLogger = logger.NewLogger().Child("client")

type JobAPI struct {
	Client         *http.Client
	WorkspaceID    string
	URLPrefix      string
	WorkspaceToken string
}

// Get sends http request with workspaceID in the url and receives a json payload
// which is decoded using schema and then mapped from schema to internal model.Job struct,
// which is actually returned.
func (j *JobAPI) Get(ctx context.Context) (model.Job, error) {
	pkgLogger.Infof("making http request to regulation manager to get new job")

	url := fmt.Sprintf("%s/dataplane/workspaces/%s/regulations/workerJobs", j.URLPrefix, j.WorkspaceID)
	pkgLogger.Infof("making GET request to URL: %v", url)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		pkgLogger.Errorf("error while create new http request: %v", err)
		return model.Job{}, err
	}

	req.SetBasicAuth(j.WorkspaceToken, "")
	req.Header.Set("Content-Type", "application/json")

	resp, err := j.Client.Do(req)
	if os.IsTimeout(err) {
		stats.Default.NewStat("regulation_manager.request_timeout", stats.CountType).Count(1)
		return model.Job{}, model.ErrRequestTimeout
	}
	if err != nil {
		return model.Job{}, err
	}
	defer func() {
		err := resp.Body.Close()
		if err != nil {
			pkgLogger.Errorf("error while closing response body: %v", err)
		}
	}()
	pkgLogger.Infof("WorkspaceToken: %v", j.WorkspaceToken)
	pkgLogger.Infof("obtained response code: %v", resp.StatusCode)
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		pkgLogger.Infof("CP response body: ", string(bodyBytes))
	}

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

		userCountPerJob := stats.Default.NewTaggedStat("user_count_per_job", stats.CountType, stats.Tags{"jobId": jobSchema.JobID, "workspaceId": j.WorkspaceID})
		userCountPerJob.Count(len(jobSchema.UserAttributes))

		job, err := mapPayloadToJob(jobSchema, j.WorkspaceID)
		if err != nil {
			pkgLogger.Errorf("error while mapping response payload to job: %v", err)
			return model.Job{}, fmt.Errorf("error while getting job: %w", err)
		}

		pkgLogger.Debugf("obtained job: %v", job)
		return job, nil

	} else if resp.StatusCode == http.StatusNotFound {
		// NOTE: `http.StatusNotFound` has to be deprecated once,
		// regulation manager is updated with the latest changes to respond with `http.StatusNoContent`204`
		// when no job is found.
		pkgLogger.Debugf("no runnable job found")
		return model.Job{}, model.ErrNoRunnableJob
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

	method := "PATCH"

	genEndPoint := "/dataplane/workspaces/{workspace_id}/regulations/workerJobs/{job_id}"
	url := fmt.Sprint(j.URLPrefix, prepURL(genEndPoint, j.WorkspaceID, fmt.Sprint(jobID)))
	pkgLogger.Debugf("sending request to URL: %v", url)

	statusSchema := statusJobSchema{
		Status: string(status),
	}
	body, err := json.Marshal(statusSchema)
	if err != nil {
		pkgLogger.Errorf("error while marshalling status schema: %v", err)
		return fmt.Errorf("error while marshalling status: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
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
	defer func() { _ = resp.Body.Close() }()

	pkgLogger.Debugf("response code: %v", resp.StatusCode, "response body: %v", resp.Body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	} else {
		pkgLogger.Errorf("update status failed with status code: %v", resp.StatusCode)
		return fmt.Errorf("update status failed with status code: %d", resp.StatusCode)
	}
}

func prepURL(url string, params ...string) string {
	re := regexp.MustCompile(`{.*?}`)
	i := 0
	return string(re.ReplaceAllFunc([]byte(url), func(matched []byte) []byte {
		if i >= len(params) {
			pkgLogger.Errorf("value for %v not provided", matched)
		}
		v := params[i]
		i++
		return []byte(v)
	}))
}

func mapPayloadToJob(wjs jobSchema, workspaceID string) (model.Job, error) {
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
		ID:            jobID,
		WorkspaceID:   workspaceID,
		DestinationID: wjs.DestinationID,
		Status:        model.JobStatusRunning,
		Users:         usrAttribute,
	}, nil
}
