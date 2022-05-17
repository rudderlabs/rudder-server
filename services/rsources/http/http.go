package http

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

func NewHandler(service rsources.JobService, logger logger.LoggerI) http.Handler {
	h := &handler{
		service: service,
		logger:  logger,
	}
	srvMux := mux.NewRouter()
	srvMux.HandleFunc("/v1/job-status/{job_id}", h.getStatus).Methods("GET")
	srvMux.HandleFunc("/v1/job-status/{job_id}", h.delete).Methods("DELETE")
	return srvMux
}

type handler struct {
	logger  logger.LoggerI
	service rsources.JobService
}

func (h *handler) delete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var jobId string
	var ok bool
	jobId, ok = mux.Vars(r)["job_id"]
	if !ok {
		http.Error(w, "job_id not found", http.StatusBadRequest)
	}

	err := h.service.Delete(ctx, jobId)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) getStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var jobId string
	var taskId, sourceId []string
	var ok bool

	jobId, ok = mux.Vars(r)["job_id"]
	if !ok {
		http.Error(w, "job_id not found", http.StatusBadRequest)
	}

	tId, ok := r.URL.Query()["task_id"]
	if ok {
		if len(tId) > 0 {

			taskId = tId
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
		jobId,
		rsources.JobFilter{
			TaskRunId: taskId,
			SourceId:  sourceId,
		})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	body, err := json.Marshal(jobStatus)
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
