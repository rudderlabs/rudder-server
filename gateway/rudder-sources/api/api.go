package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/rudderlabs/rudder-server/gateway/rudder-sources/model"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var (
	pkgLogger logger.LoggerI
)

// JobService manages information about jobs created by rudder-sources
//go:generate mockgen -source=api.go -destination=mock_api_test.go -package=api github.com/rudderlabs/rudder-server/gateway/rudder-sources/api
type SourcesService interface {

	// Delete deletes all relevant information for a given jobRunId
	Delete(ctx context.Context, jobId string) error

	// GetStatus gets the current status of a job
	GetStatus(ctx context.Context, jobId string, jobFilter model.JobFilter) (model.JobStatus, error)

	// IncrementStats increments the existing statistic counters
	// for a specific job measurement.
	IncrementStats(ctx context.Context, tx sql.Tx, jobRunId string, key model.JobFilter, stats model.Stats) error

	// TODO: future extension
	AddFailedRecords(ctx context.Context, tx sql.Tx, jobRunId string, key model.JobFilter, records []json.RawMessage) error

	// TODO: future extension
	GetFailedRecords(ctx context.Context, tx sql.Tx, jobRunId string, filter model.JobFilter) (model.FailedRecords, error)
}

type API struct {
	SVC SourcesService
}

func Init() {
	pkgLogger = logger.NewLogger().Child("gateway").Child("webhook")
}

func (a *API) Handler() http.Handler {
	srvMux := mux.NewRouter()
	srvMux.HandleFunc("/v1/job-status/{job_id}", a.getStatus).Methods("GET")
	srvMux.HandleFunc("/v1/job-status/{job_id}", a.delete).Methods("DELETE")

	return srvMux
}

func (a *API) delete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var jobId string
	var ok bool
	jobId, ok = mux.Vars(r)["job_id"]
	if !ok {
		http.Error(w, "job_id not found", http.StatusBadRequest)
	}

	err := a.SVC.Delete(ctx, jobId)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *API) getStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var jobId string
	var taskId, sourceId, destinationId *string
	var ok bool

	jobId, ok = mux.Vars(r)["job_id"]
	if !ok {
		http.Error(w, "job_id not found", http.StatusBadRequest)
	}

	tId, ok := r.URL.Query()["task_id"]
	if ok {
		if len(tId) > 0 {
			taskId = &tId[0]
		}
	}

	sId, ok := r.URL.Query()["source_id"]
	if ok {
		if len(sId) > 0 {
			sourceId = &sId[0]
		}
	}

	dId, ok := r.URL.Query()["destination_id"]
	if ok {
		if len(dId) > 0 {
			destinationId = &dId[0]
		}
	}

	jobStatus, err := s.SVC.GetStatus(
		ctx,
		jobId,
		model.JobFilter{
			TaskRunId:     taskId,
			SourceId:      sourceId,
			DestinationId: destinationId,
		})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	body, err := json.Marshal(mapJobToPayload(jobStatus))
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	_, err = w.Write(body)
	if err != nil {
		pkgLogger.Errorf("error while writing response body: %v", err)
		return
	}

}

//to map jobStatus output from internal data type to external data type.
func mapJobToPayload(job model.JobStatus) JobStatusSchema {

	taskStatusList := make([]TaskStatusSchema, len(job.TasksStatus))
	for i, taskStatus := range job.TasksStatus {
		sourceStatusList := make([]SourceStatusSchema, len(taskStatus.SourcesStatus))
		for j, sourceStatus := range taskStatus.SourcesStatus {
			destStatusList := make([]DestinationStatusSchema, len(sourceStatus.DestinationsStatus))
			for k, destStatus := range sourceStatus.DestinationsStatus {
				destStatusList[k] = DestinationStatusSchema{
					ID:        destStatus.ID,
					Completed: destStatus.Completed,
					Stats:     StatsSchema(destStatus.Stats),
				}
			}
			sourceStatusList[j] = SourceStatusSchema{
				ID:                 sourceStatus.ID,
				Completed:          sourceStatus.Completed,
				Stats:              StatsSchema(sourceStatus.Stats),
				DestinationsStatus: destStatusList,
			}
		}
		taskStatusList[i] = TaskStatusSchema{
			ID:            taskStatus.ID,
			SourcesStatus: sourceStatusList,
		}
	}
	return JobStatusSchema{
		ID:          job.ID,
		TasksStatus: taskStatusList,
	}
}
