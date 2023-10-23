package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/rudderlabs/rudder-server/services/notifier"

	ierrors "github.com/rudderlabs/rudder-server/warehouse/internal/errors"
	lf "github.com/rudderlabs/rudder-server/warehouse/logfield"

	"github.com/samber/lo"
)

type insertJobRequest struct {
	SourceID      string `json:"source_id"`
	DestinationID string `json:"destination_id"`
	StartTime     string `json:"start_time"`
	JobRunID      string `json:"job_run_id"`
	TaskRunID     string `json:"task_run_id"`
	JobType       string `json:"async_job_type"`
	WorkspaceID   string `json:"workspace_id"`
}

type insertJobResponse struct {
	JobIds []int64 `json:"jobids"`
	Err    error   `json:"error"`
}

type jobStatusResponse struct {
	Status string
	Err    string
}

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

	tableUploads, err := m.tableUploadsRepo.GetByJobRunTaskRun(
		r.Context(),
		payload.SourceID,
		payload.DestinationID,
		payload.JobRunID,
		payload.TaskRunID,
	)
	if err != nil {
		if errors.Is(r.Context().Err(), context.Canceled) {
			http.Error(w, ierrors.ErrRequestCancelled.Error(), http.StatusBadRequest)
			return
		}
		m.logger.Errorw("extracting tableNames for inserting source job", lf.Error, err.Error())
		http.Error(w, "can't extract tableNames", http.StatusInternalServerError)
		return
	}

	tableNames := lo.Map(tableUploads, func(item model.TableUpload, index int) string {
		return item.TableName
	})
	tableNames = lo.Filter(tableNames, func(tableName string, i int) bool {
		switch strings.ToLower(tableName) {
		case "rudder_discards", "rudder_identity_mappings", "rudder_identity_merge_rules":
			return false
		default:
			return true
		}
	})

	type metadata struct {
		JobRunID  string `json:"job_run_id"`
		TaskRunID string `json:"task_run_id"`
		JobType   string `json:"jobtype"`
		StartTime string `json:"start_time"`
	}

	metadataJson, err := json.Marshal(metadata{
		JobRunID:  payload.JobRunID,
		TaskRunID: payload.TaskRunID,
		StartTime: payload.StartTime,
		JobType:   string(notifier.JobTypeAsync),
	})
	if err != nil {
		m.logger.Errorw("marshalling metadata for inserting source job", lf.Error, err.Error())
		http.Error(w, "can't marshall metadata", http.StatusInternalServerError)
		return
	}

	jobIds, err := m.sourceRepo.Insert(r.Context(), lo.Map(tableNames, func(item string, index int) model.SourceJob {
		return model.SourceJob{
			SourceID:      payload.SourceID,
			DestinationID: payload.DestinationID,
			WorkspaceID:   payload.WorkspaceID,
			TableName:     item,
			JobType:       payload.JobType,
			Metadata:      metadataJson,
		}
	}))
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
		statusResponse.Status = sourceJob.Status
		statusResponse.Err = errorMessage
	default:
		statusResponse.Status = sourceJob.Status
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
	case payload.SourceID == "":
		return errors.New("source_id is required")
	case payload.DestinationID == "":
		return errors.New("destination_id is required")
	case payload.JobRunID == "":
		return errors.New("job_run_id is required")
	case payload.TaskRunID == "":
		return errors.New("task_run_id is required")
	default:
		return nil
	}
}
