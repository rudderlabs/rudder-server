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
		a.logger.Errorf("Warehouse Jobs API not initialized")
		http.Error(w, "warehouse jobs api not initialized", http.StatusInternalServerError)
		return
	}

	var payload StartJobReqPayload

	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		a.logger.Errorf("unmarshalling body for adding async jobs: %v", err)
		http.Error(w, "can't unmarshall body", http.StatusBadRequest)
		return
	}

	if !validatePayload(&payload) {
		a.logger.Errorf("Invalid Payload for adding async job %v", payload)
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	tableNames, err := a.tableNamesBy(payload.SourceID, payload.DestinationID, payload.JobRunID, payload.TaskRunID)
	if err != nil {
		a.logger.Errorf("extracting tableNames for adding async job %v: with error %v", payload, err)
		http.Error(w, "can't extract tableNames", http.StatusBadRequest)
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

	var jobIds []int64
	for _, table := range tableNames {
		metadataJson, err := json.Marshal(WhJobsMetaData{
			JobRunID:  payload.JobRunID,
			TaskRunID: payload.TaskRunID,
			StartTime: payload.StartTime,
			JobType:   AsyncJobType,
		})
		if err != nil {
			a.logger.Errorf("unmarshall metadata while adding jobs for table %s: %v", table, err)
			http.Error(w, "can't unmarshall metadata while adding jobs", http.StatusInternalServerError)
			return
		}

		id, err := a.addJobsToDB(&AsyncJobPayload{
			SourceID:      payload.SourceID,
			DestinationID: payload.DestinationID,
			TableName:     table,
			AsyncJobType:  payload.AsyncJobType,
			MetaData:      metadataJson,
			WorkspaceID:   payload.WorkspaceID,
		})
		if err != nil {
			a.logger.Errorf("unmarshall adding jobs to db: %v", err)
			http.Error(w, "can't unmarshall adding jobs to db", http.StatusInternalServerError)
			return
		}

		jobIds = append(jobIds, id)
	}

	response, err := json.Marshal(insertJobResponse{
		JobIds: jobIds,
		Err:    nil,
	})
	if err != nil {
		a.logger.Errorf("unmarshall response for adding async jobs: %v", err)
		http.Error(w, "can't unmarshall response", http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(response)
}

// StatusJobHandler The following handler gets called for getting the status of the async job
func (a *AsyncJobWh) StatusJobHandler(w http.ResponseWriter, r *http.Request) {
	a.logger.LogRequest(r)

	defer func() { _ = r.Body.Close() }()

	if !a.enabled {
		a.logger.Errorf("Warehouse Jobs API not initialized")
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
		a.logger.Errorf("Invalid Payload for stauts async job %v", payload)
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	response := a.jobStatus(&payload)

	writeResponse, err := json.Marshal(response)
	if err != nil {
		a.logger.Errorf("unmarshall response for status async job: %v", err)
		http.Error(w, "can't unmarshall response", http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(writeResponse)
}

func validatePayload(payload *StartJobReqPayload) bool {
	return payload.SourceID != "" && payload.JobRunID != "" && payload.TaskRunID != "" && payload.DestinationID != ""
}
