package http

import (
	"errors"
	"net/http"

	"github.com/gorilla/mux"
	jsoniter "github.com/json-iterator/go"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

func NewHandler(service rsources.JobService, logger logger.Logger) http.Handler {
	h := &handler{
		service: service,
		logger:  logger,
	}
	srvMux := mux.NewRouter()
	srvMux.HandleFunc("/v1/job-status/{job_run_id}", h.getStatus).Methods("GET")
	srvMux.HandleFunc("/v1/job-status/{job_run_id}", h.delete).Methods("DELETE")
	srvMux.HandleFunc("/v1/job-status/{job_run_id}/failed-records", h.failedRecords).Methods("GET")
	return srvMux
}

type handler struct {
	logger  logger.Logger
	service rsources.JobService
}

func (h *handler) delete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	jobRunId, taskRunId, sourceId := getQueryParams(r)
	if jobRunId == "" {
		http.Error(w, "job_run_id not found", http.StatusBadRequest)
		return
	}

	err := h.service.Delete(ctx, jobRunId, rsources.JobFilter{TaskRunID: taskRunId, SourceID: sourceId})
	if err != nil {
		httpStatus := http.StatusInternalServerError
		switch {
		case errors.Is(err, rsources.ErrStatusNotFound):
			httpStatus = http.StatusNotFound
		case errors.Is(err, rsources.ErrSourceNotCompleted):
			httpStatus = http.StatusBadRequest
		}
		http.Error(w, err.Error(), httpStatus)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) getStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var jobRunId string
	var taskRunId, sourceId []string

	jobRunId, taskRunId, sourceId = getQueryParams(r)
	if jobRunId == "" {
		http.Error(w, "job_run_id not found", http.StatusBadRequest)
		return
	}

	jobStatus, err := h.service.GetStatus(
		ctx,
		jobRunId,
		rsources.JobFilter{
			TaskRunID: taskRunId,
			SourceID:  sourceId,
		})
	if err != nil {
		if errors.Is(err, rsources.ErrStatusNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)

	err = marshalAndWriteResponse(w, jobStatus)
	if err != nil {
		h.logger.Errorf("error while marshalling and writing response body: %v", err)
	}
}

func (h *handler) failedRecords(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var jobRunId string
	var taskRunId, sourceId []string

	jobRunId, taskRunId, sourceId = getQueryParams(r)
	if jobRunId == "" {
		http.Error(w, "job_run_id not found", http.StatusBadRequest)
		return
	}

	failedRecords, err := h.service.GetFailedRecords(
		ctx, jobRunId, rsources.JobFilter{
			TaskRunID: taskRunId,
			SourceID:  sourceId,
		},
	)
	if err != nil {
		httpStatus := http.StatusInternalServerError
		if errors.Is(err, rsources.ErrOperationNotSupported) {
			httpStatus = http.StatusBadRequest
		}
		http.Error(w, err.Error(), httpStatus)
		return
	}

	err = marshalAndWriteResponse(w, failedRecords)
	if err != nil {
		h.logger.Errorf("error while marshalling and writing response body: %v", err)
	}
}

func getQueryParams(r *http.Request) (jobRunID string, taskRunID, sourceID []string) {
	var ok bool
	jobRunID, ok = mux.Vars(r)["job_run_id"]
	if !ok {
		return
	}
	tID, okTID := r.URL.Query()["task_run_id"]
	if okTID {
		if len(tID) > 0 {
			taskRunID = tID
		}
	}

	sID, okSID := r.URL.Query()["source_id"]
	if okSID {
		if len(sID) > 0 {
			sourceID = sID
		}
	}

	return jobRunID, taskRunID, sourceID
}

func marshalAndWriteResponse(w http.ResponseWriter, response interface{}) (err error) {
	body, err := jsoniter.Marshal(response)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return err
	}
	_, err = w.Write(body)
	return err
}
