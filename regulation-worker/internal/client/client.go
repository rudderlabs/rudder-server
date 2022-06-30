package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
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
	pkgLogger.Debugf("making http request to regulation manager to get new job")
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	method := "GET"
	genEndPoint := "/dataplane/workspaces/{workspace_id}/regulations/workerJobs"
	url := fmt.Sprint(j.URLPrefix, prepURL(genEndPoint, j.WorkspaceID))
	pkgLogger.Debugf("making GET request to URL: %v", url)

	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		pkgLogger.Errorf("error while create new http request: %v", err)
		return model.Job{}, err
	}

	req.SetBasicAuth(j.WorkspaceToken, "")
	req.Header.Set("Content-Type", "application/json")

	pkgLogger.Debugf("making request: %v", req)
	resp, err := j.Client.Do(req)
	if err != nil {
		pkgLogger.Errorf("http request failed with error: %v", err)
		return model.Job{}, err
	}
	defer func() {
		err := resp.Body.Close()
		if err != nil {
			pkgLogger.Errorf("error while closing response body: %v", err)
		}
	}()
	pkgLogger.Debugf("obtained response code: %v", resp.StatusCode, "response body: ", resp.Body)

	// if successful

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		var jobSchema jobSchema
		if err := json.NewDecoder(resp.Body).Decode(&jobSchema); err != nil {
			pkgLogger.Errorf("error while decoding response body: %v", err)
			return model.Job{}, fmt.Errorf("error while decoding job: %w", err)
		}

		userCountPerJob := stats.NewTaggedStat("user_count_per_job", stats.CountType, stats.Tags{"jobId": jobSchema.JobID, "workspaceId": j.WorkspaceID})
		userCountPerJob.Count(len(jobSchema.UserAttributes))

		job, err := mapPayloadToJob(jobSchema, j.WorkspaceID)
		if err != nil {
			pkgLogger.Errorf("error while mapping response payload to job: %v", err)
			return model.Job{}, fmt.Errorf("error while getting job: %w", err)
		}

		pkgLogger.Debugf("obtained job: %v", job)
		return job, nil

	} else if resp.StatusCode == http.StatusNotFound {
		pkgLogger.Debugf("no runnable job found")
		return model.Job{}, model.ErrNoRunnableJob
	} else {

		body, err := ioutil.ReadAll(resp.Body)
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
	if err != nil {
		pkgLogger.Errorf("error while making http request: %v", err)
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
	usrAttribute := make([]model.UserAttribute, len(wjs.UserAttributes))
	for i := 0; i < len(wjs.UserAttributes); i++ {
		usrAttribute[i] = model.UserAttribute{
			UserID: wjs.UserAttributes[i].UserID,
			Phone:  wjs.UserAttributes[i].Phone,
			Email:  wjs.UserAttributes[i].Email,
		}
	}
	jobID, err := strconv.Atoi(wjs.JobID)
	if err != nil {
		pkgLogger.Errorf("error while getting jobId: %v", err)
		return model.Job{}, fmt.Errorf("error while get JobID:%w", err)
	}

	return model.Job{
		ID:             jobID,
		WorkspaceID:    workspaceID,
		DestinationID:  wjs.DestinationID,
		Status:         model.JobStatusRunning,
		UserAttributes: usrAttribute,
	}, nil
}
