package http

import (
	"errors"
	"net/http"

	"github.com/gorilla/mux"
	jsoniter "github.com/json-iterator/go"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

func NewHandler(service rsources.JobService, logger logger.LoggerI) http.Handler {
	h := &handler{
		service: service,
		logger:  logger,
	}
	srvMux := mux.NewRouter()
	srvMux.HandleFunc("/v1/job-status/{job_run_id}", h.getStatus).Methods("GET")
	srvMux.HandleFunc("/v1/job-status/{job_run_id}", h.delete).Methods("DELETE")
	return srvMux
}

type handler struct {
	logger  logger.LoggerI
	service rsources.JobService
}

func (h *handler) delete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var jobRunId string
	var ok bool
	jobRunId, ok = mux.Vars(r)["job_run_id"]
	if !ok {
		http.Error(w, "job_run_id not found", http.StatusBadRequest)
	}

	err := h.service.Delete(ctx, jobRunId)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) getStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var jobRunId string
	var taskRunId, sourceId []string
	var ok bool

	jobRunId, ok = mux.Vars(r)["job_run_id"]
	if !ok {
		http.Error(w, "job_run_id not found", http.StatusBadRequest)
	}

	tId, ok := r.URL.Query()["task_run_id"]
	if ok {
		if len(tId) > 0 {
			taskRunId = tId
		}
	}

	sId, ok := r.URL.Query()["source_id"]
	if ok {
		if len(sId) > 0 {
			sourceId = sId
		}
	}

	jobStatus, err := h.service.GetStatus(
		ctx,
		jobRunId,
		rsources.JobFilter{
			TaskRunID: taskRunId,
			SourceID:  sourceId,
		})
	if err != nil {
		if errors.Is(err, rsources.StatusNotFoundError) {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)

	body, err := jsoniter.Marshal(jobStatus)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	_, err = w.Write(body)
	if err != nil {
		h.logger.Errorf("error while writing response body: %v", err)
		return
	}
}
