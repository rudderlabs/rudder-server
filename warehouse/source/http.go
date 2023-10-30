package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	ierrors "github.com/rudderlabs/rudder-server/warehouse/internal/errors"
	lf "github.com/rudderlabs/rudder-server/warehouse/logfield"
)

// emptyRegex matches empty strings
var emptyRegex = regexp.MustCompile(`^\s*$`)

// InsertJobHandler adds a job to the warehouse_jobs table
func (m *Manager) InsertJobHandler(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()

	var payload insertJobRequest
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		m.logger.Warnw("invalid JSON in request body for inserting source jobs", lf.Error, err.Error())
		http.Error(w, ierrors.ErrInvalidJSONRequestBody.Error(), http.StatusBadRequest)
		return
	}

	if err := validatePayload(&payload); err != nil {
		m.logger.Warnw("invalid payload for inserting source job", lf.Error, err.Error())
		http.Error(w, fmt.Sprintf("invalid payload: %s", err.Error()), http.StatusBadRequest)
		return
	}

	jobIds, err := m.InsertJobs(r.Context(), payload)
	if err != nil {
		if errors.Is(r.Context().Err(), context.Canceled) {
			http.Error(w, ierrors.ErrRequestCancelled.Error(), http.StatusBadRequest)
			return
		}
		m.logger.Errorw("inserting source jobs", lf.Error, err.Error())
		http.Error(w, "can't insert source jobs", http.StatusInternalServerError)
		return
	}

	resBody, err := json.Marshal(insertJobResponse{
		JobIds: jobIds,
		Err:    nil,
	})
	if err != nil {
		m.logger.Errorw("marshalling response for inserting source job", lf.Error, err.Error())
		http.Error(w, ierrors.ErrMarshallResponse.Error(), http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(resBody)
}

// StatusJobHandler The following handler gets called for getting the status of the async job
func (m *Manager) StatusJobHandler(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()

	queryParams := r.URL.Query()
	payload := insertJobRequest{
		TaskRunID:     queryParams.Get("task_run_id"),
		JobRunID:      queryParams.Get("job_run_id"),
		SourceID:      queryParams.Get("source_id"),
		DestinationID: queryParams.Get("destination_id"),
		WorkspaceID:   queryParams.Get("workspace_id"),
	}
	if err := validatePayload(&payload); err != nil {
		m.logger.Warnw("invalid payload for source job status", lf.Error, err.Error())
		http.Error(w, fmt.Sprintf("invalid request: %s", err.Error()), http.StatusBadRequest)
		return
	}

	sourceJob, err := m.sourceRepo.GetByJobRunTaskRun(r.Context(), payload.JobRunID, payload.TaskRunID)
	if err != nil {
		if errors.Is(r.Context().Err(), context.Canceled) {
			http.Error(w, ierrors.ErrRequestCancelled.Error(), http.StatusBadRequest)
			return
		}
		if errors.Is(err, model.ErrSourcesJobNotFound) {
			http.Error(w, model.ErrSourcesJobNotFound.Error(), http.StatusNotFound)
			return
		}
		m.logger.Warnw("unable to get source job status", lf.Error, err.Error())
		http.Error(w, fmt.Sprintf("can't get source job status: %s", err.Error()), http.StatusBadRequest)
		return
	}

	var statusResponse jobStatusResponse
	switch sourceJob.Status {
	case model.SourceJobStatusFailed, model.SourceJobStatusAborted:
		errorMessage := "source job failed"
		if sourceJob.Error != nil {
			errorMessage = sourceJob.Error.Error()
		}
		statusResponse.Status = sourceJob.Status.String()
		statusResponse.Err = errorMessage
	default:
		statusResponse.Status = sourceJob.Status.String()
	}

	resBody, err := json.Marshal(statusResponse)
	if err != nil {
		m.logger.Errorw("marshalling response for source job status", lf.Error, err.Error())
		http.Error(w, ierrors.ErrMarshallResponse.Error(), http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(resBody)
}

func validatePayload(payload *insertJobRequest) error {
	switch true {
	case emptyRegex.MatchString(payload.SourceID):
		return errors.New("source_id is required")
	case emptyRegex.MatchString(payload.DestinationID):
		return errors.New("destination_id is required")
	case emptyRegex.MatchString(payload.JobRunID):
		return errors.New("job_run_id is required")
	case emptyRegex.MatchString(payload.TaskRunID):
		return errors.New("task_run_id is required")
	default:
		return nil
	}
}
