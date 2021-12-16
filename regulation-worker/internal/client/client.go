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

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

type JobAPI struct {
	WorkspaceID string
	URLPrefix   string
}

//Get sends http request with workspaceID in the url and receives a json payload
//which is decoded using schema and then mapped from schema to internal model.Job struct,
//which is actually returned.
func (j *JobAPI) Get(ctx context.Context) (model.Job, error) {

	ctx, cancel := context.WithTimeout(ctx, time.Duration(time.Minute))
	defer cancel()

	method := "GET"
	genEndPoint := "/dataplane/workspaces/{workspace_id}/regulations/workerJobs"
	url := fmt.Sprint(j.URLPrefix, prepURL(genEndPoint, j.WorkspaceID))

	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return model.Job{}, err
	}

	workspaceToken := config.GetEnv("CONFIG_BACKEND_TOKEN", "22JnFdS3ZKDd0UfuEYowUeNi9fe")
	req.SetBasicAuth(workspaceToken, "")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return model.Job{}, err
	}
	defer resp.Body.Close()
	if err != nil {
		fmt.Println(err)
	}

	//if successful
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		var jobSchema jobSchema
		if err := json.NewDecoder(resp.Body).Decode(&jobSchema); err != nil {
			return model.Job{}, fmt.Errorf("error while decoding job: %w", err)
		}

		job, err := mapPayloadToJob(jobSchema, j.WorkspaceID)
		if err != nil {
			return model.Job{}, fmt.Errorf("error while getting job: %w", err)
		}
		return job, nil

	} else if resp.StatusCode == http.StatusNotFound {

		return model.Job{}, model.ErrNoRunnableJob
	} else {

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			respErr := model.APIReqErr{
				StatusCode: resp.StatusCode,
				Err:        err,
			}
			return model.Job{}, fmt.Errorf("error while reading request body: %v", respErr)
		}

		respErr := model.APIReqErr{
			StatusCode: resp.StatusCode,
			Body:       string(body),
		}
		return model.Job{}, fmt.Errorf("error while getting job:%v", respErr)
	}

}

//marshals status into appropriate status schema, and sent as payload
//checked for returned status code.
func (j *JobAPI) UpdateStatus(ctx context.Context, status model.JobStatus, jobID int) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(time.Minute))
	defer cancel()

	method := "PATCH"

	genEndPoint := "/dataplane/workspaces/{workspace_id}/regulations/workerJobs/{job_id}"
	url := fmt.Sprint(j.URLPrefix, prepURL(genEndPoint, j.WorkspaceID, fmt.Sprint(jobID)))
	statusSchema := statusJobSchema{
		Status: string(status),
	}
	body, err := json.Marshal(statusSchema)
	if err != nil {
		return fmt.Errorf("error while marshalling status: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	} else {
		return fmt.Errorf("update status failed with status code: %d", resp.StatusCode)
	}
}

func prepURL(url string, params ...string) string {
	var re = regexp.MustCompile(`{.*?}`)
	i := 0
	return string(re.ReplaceAllFunc([]byte(url), func(matched []byte) []byte {
		if i >= len(params) {
			panic(fmt.Sprintf("value for %q not provided", matched))
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
