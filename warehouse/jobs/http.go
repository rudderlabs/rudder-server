/*
	Warehouse jobs package provides the capability to run arbitrary jobs on the warehouses using the query parameters provided.
	Some jobs that can be run are
	1) delete by task run id,
	2) delete by job run id,
	3) delete by update_at
	4) any other update / clean up operations

	The following handlers file is the entry point for the handlers.
*/

package jobs

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/samber/lo"
)

type insertJobResponse struct {
	JobIds []int64 `json:"jobids"`
	Err    error   `json:"error"`
}

// InsertJobHandler adds a job to the warehouse_jobs table
func (a *AsyncJobWh) InsertJobHandler(w http.ResponseWriter, r *http.Request) {
	a.logger.LogRequest(r)

	defer func() { _ = r.Body.Close() }()

	if !a.enabled {
		a.logger.Error("jobs api not initialized")
		http.Error(w, "warehouse jobs api not initialized", http.StatusInternalServerError)
		return
	}

	var payload StartJobReqPayload

	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		a.logger.Errorf("invalid JSON body for inserting async jobs: %v", err)
		http.Error(w, "invalid JSON in request body", http.StatusBadRequest)
		return
	}

	if !validatePayload(&payload) {
		a.logger.Errorf("invalid request while inserting async job %v", payload)
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	// TODO: Move to repository
	tableNames, err := a.tableNamesBy(payload.SourceID, payload.DestinationID, payload.JobRunID, payload.TaskRunID)
	if err != nil {
		a.logger.Errorf("extracting tableNames while inserting async job for payload: %v, with error: %v", payload, err)
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
			a.logger.Errorf("marshalling metadata while inserting async job for payload: %v with error: %v", payload, err)
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
			a.logger.Errorf("inserting async job: %v", err)
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
		a.logger.Errorf("marshalling response while inserting async job: %v", err)
		http.Error(w, "can't marshall response", http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(resBody)
}

// StatusJobHandler The following handler gets called for getting the status of the async job
func (a *AsyncJobWh) StatusJobHandler(w http.ResponseWriter, r *http.Request) {
	a.logger.LogRequest(r)

	defer func() { _ = r.Body.Close() }()

	if !a.enabled {
		a.logger.Error("jobs api not initialized")
		http.Error(w, "warehouse jobs api not initialized", http.StatusInternalServerError)
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
	if !validatePayload(&payload) {
		a.logger.Errorf("invalid request while getting status for async job: %v", payload)
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	// TODO: Move to repository
	jobStatus := a.jobStatus(&payload)

	resBody, err := json.Marshal(jobStatus)
	if err != nil {
		a.logger.Errorf("marshalling response while getting status for async job: %v", err)
		http.Error(w, "can't marshall response", http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(resBody)
}

func validatePayload(payload *StartJobReqPayload) bool {
	return payload.SourceID != "" && payload.JobRunID != "" && payload.TaskRunID != "" && payload.DestinationID != ""
}
