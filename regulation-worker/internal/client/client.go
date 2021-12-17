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
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var pkgLogger = logger.NewLogger().Child("client")

type JobAPI struct {
	Client      *http.Client
	WorkspaceID string
	URLPrefix   string
}

//Get sends http request with workspaceID in the url and receives a json payload
//which is decoded using schema and then mapped from schema to internal model.Job struct,
//which is actually returned.
func (j *JobAPI) Get(ctx context.Context) (model.Job, error) {
	pkgLogger.Debugf("making http request to regulation manager to get new job")
	ctx, cancel := context.WithTimeout(ctx, time.Duration(time.Minute))
	defer cancel()

	method := "GET"

	genEndPoint := "/dataplane/workspaces/{workspace_id}/regulations/workerJobs"
	url := fmt.Sprint(j.URLPrefix, prepURL(genEndPoint, j.WorkspaceID))
	pkgLogger.Debugf("making GET request to URL: %w", url)

	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		pkgLogger.Errorf("error while create new http request: %w", err)
		return model.Job{}, err
	}

	pkgLogger.Debugf("making request: %w", req)
	resp, err := j.Client.Do(req)
	if err != nil {
		pkgLogger.Errorf("http request failed with error: %w", err)
		return model.Job{}, err
	}
	defer resp.Body.Close()
	pkgLogger.Debugf("obtained response code: %w", resp.StatusCode, "response body: ", resp.Body)

	//if successful

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		var jobSchema jobSchema
		if err := json.NewDecoder(resp.Body).Decode(&jobSchema); err != nil {
			pkgLogger.Errorf("error while decoding reponse body: %w", err)
			return model.Job{}, fmt.Errorf("error while decoding job: %w", err)
		}

		job, err := mapPayloadToJob(jobSchema, j.WorkspaceID)
		if err != nil {
			pkgLogger.Errorf("error while mapping response payload to job: %w", err)
			return model.Job{}, fmt.Errorf("error while getting job: %w", err)
		}

		pkgLogger.Debugf("obtained job: %w", job)
		return job, nil

	} else if resp.StatusCode == http.StatusNotFound {
		pkgLogger.Debugf("no runnable job found")
		return model.Job{}, model.ErrNoRunnableJob
	} else {

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			pkgLogger.Errorf("error while reading response body: %w", err)
			return model.Job{}, fmt.Errorf("error while reading response body: %w", err)
		}
		pkgLogger.Debugf("obtained response body: %w", body)

		return model.Job{}, fmt.Errorf("error while getting job: %w", err)
	}

}

//marshals status into appropriate status schema, and sent as payload
//checked for returned status code.
func (j *JobAPI) UpdateStatus(ctx context.Context, status model.JobStatus, jobID int) error {
	pkgLogger.Debugf("sending PATCH request to update job status for jobId: ", jobID, "with status: %w", status)
	ctx, cancel := context.WithTimeout(ctx, time.Duration(time.Minute))
	defer cancel()

	method := "PATCH"

	genEndPoint := "/dataplane/workspaces/{workspace_id}/regulations/workerJobs/{job_id}"
	url := fmt.Sprint(j.URLPrefix, prepURL(genEndPoint, j.WorkspaceID, fmt.Sprint(jobID)))
	pkgLogger.Debugf("sending request to URL: %w", url)

	statusSchema := statusJobSchema{
		Status: string(status),
	}
	body, err := json.Marshal(statusSchema)
	if err != nil {
		pkgLogger.Errorf("error while marshalling status schema: %w", err)
		return fmt.Errorf("error while marshalling status: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	pkgLogger.Debugf("sending request: %w", req)

	resp, err := j.Client.Do(req)
	if err != nil {
		pkgLogger.Errorf("error while making http request: %w", err)
		return err
	}
	defer resp.Body.Close()

	pkgLogger.Debugf("response code: %w", resp.StatusCode, "response body: %w", resp.Body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	} else {
		pkgLogger.Errorf("update status failed with status code: %w", resp.StatusCode)
		return fmt.Errorf("update status failed with status code: %d", resp.StatusCode)
	}
}

func prepURL(url string, params ...string) string {
	var re = regexp.MustCompile(`{.*?}`)
	i := 0
	return string(re.ReplaceAllFunc([]byte(url), func(matched []byte) []byte {
		if i >= len(params) {
			pkgLogger.Errorf("value for %q not provided", matched)

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
		pkgLogger.Errorf("error while getting jobId: %w", err)
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
