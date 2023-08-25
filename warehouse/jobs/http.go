package jobs

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	ierrors "github.com/rudderlabs/rudder-server/warehouse/internal/errors"
	lf "github.com/rudderlabs/rudder-server/warehouse/logfield"

	"github.com/samber/lo"
)

type insertJobResponse struct {
	JobIds []int64 `json:"jobids"`
	Err    error   `json:"error"`
}

// InsertJobHandler adds a job to the warehouse_jobs table
func (a *AsyncJobWh) InsertJobHandler(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()

	if !a.enabled {
		a.logger.Error("jobs api not initialized for inserting async job")
		http.Error(w, ierrors.ErrJobsApiNotInitialized.Error(), http.StatusInternalServerError)
		return
	}

	var payload StartJobReqPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		a.logger.Warnw("invalid JSON in request body for inserting async jobs", lf.Error, err.Error())
		http.Error(w, ierrors.ErrInvalidJSONRequestBody.Error(), http.StatusBadRequest)
		return
	}

	if err := validatePayload(&payload); err != nil {
		a.logger.Warnw("invalid payload for inserting async job", lf.Error, err.Error())
		http.Error(w, fmt.Sprintf("invalid payload: %s", err.Error()), http.StatusBadRequest)
		return
	}

	// TODO: Move to repository
	tableNames, err := a.tableNamesBy(payload.SourceID, payload.DestinationID, payload.JobRunID, payload.TaskRunID)
	if err != nil {
		a.logger.Errorw("extracting tableNames for inserting async job", lf.Error, err.Error())
		http.Error(w, "can't extract tableNames", http.StatusInternalServerError)
		return
	}

	tableNames = lo.Filter(tableNames, func(tableName string, i int) bool {
		switch strings.ToLower(tableName) {
		case "rudder_discards", "rudder_identity_mappings", "rudder_identity_merge_rules":
			return false
		default:
			return true
		}
	})

	jobIds := make([]int64, 0, len(tableNames))
	for _, table := range tableNames {
		metadataJson, err := json.Marshal(WhJobsMetaData{
			JobRunID:  payload.JobRunID,
			TaskRunID: payload.TaskRunID,
			StartTime: payload.StartTime,
			JobType:   AsyncJobType,
		})
		if err != nil {
			a.logger.Errorw("marshalling metadata for inserting async job", lf.Error, err.Error())
			http.Error(w, "can't marshall metadata", http.StatusInternalServerError)
			return
		}

		// TODO: Move to repository
		id, err := a.addJobsToDB(&AsyncJobPayload{
			SourceID:      payload.SourceID,
			DestinationID: payload.DestinationID,
			TableName:     table,
			AsyncJobType:  payload.AsyncJobType,
			MetaData:      metadataJson,
			WorkspaceID:   payload.WorkspaceID,
		})
		if err != nil {
			a.logger.Errorw("inserting async job", lf.Error, err.Error())
			http.Error(w, "can't insert async job", http.StatusInternalServerError)
			return
		}

		jobIds = append(jobIds, id)
	}

	resBody, err := json.Marshal(insertJobResponse{
		JobIds: jobIds,
		Err:    nil,
	})
	if err != nil {
		a.logger.Errorw("marshalling response for inserting async job", lf.Error, err.Error())
		http.Error(w, ierrors.ErrMarshallResponse.Error(), http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(resBody)
}

// StatusJobHandler The following handler gets called for getting the status of the async job
func (a *AsyncJobWh) StatusJobHandler(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()

	if !a.enabled {
		a.logger.Error("jobs api not initialized for async job status")
		http.Error(w, ierrors.ErrJobsApiNotInitialized.Error(), http.StatusInternalServerError)
		return
	}

	queryParams := r.URL.Query()
	payload := StartJobReqPayload{
		TaskRunID:     queryParams.Get("task_run_id"),
		JobRunID:      queryParams.Get("job_run_id"),
		SourceID:      queryParams.Get("source_id"),
		DestinationID: queryParams.Get("destination_id"),
		WorkspaceID:   queryParams.Get("workspace_id"),
	}
	if err := validatePayload(&payload); err != nil {
		a.logger.Warnw("invalid payload for async job status", lf.Error, err.Error())
		http.Error(w, fmt.Sprintf("invalid request: %s", err.Error()), http.StatusBadRequest)
		return
	}

	// TODO: Move to repository
	jobStatus := a.jobStatus(&payload)
	resBody, err := json.Marshal(jobStatus)
	if err != nil {
		a.logger.Errorw("marshalling response for async job status", lf.Error, err.Error())
		http.Error(w, ierrors.ErrMarshallResponse.Error(), http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(resBody)
}

func validatePayload(payload *StartJobReqPayload) error {
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
