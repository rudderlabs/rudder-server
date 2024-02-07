package http

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	jsoniter "github.com/json-iterator/go"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/services/rsources"
)

// NewV1Handler is a legacy handler for job status
//
//   - GET /v1/job-status/{job_run_id} - returns job status
//   - DELETE /v1/job-status/{job_run_id} - deletes job status AND failed records
//   - GET /v1/job-status/{job_run_id}/failed-records - returns failed records
//
// TODO: delete this handler once we remove support for the v1 api
func NewV1Handler(service rsources.JobService, logger logger.Logger) http.Handler {
	h := &handler{
		service: service,
		logger:  logger,
	}
	srvMux := chi.NewRouter()
	srvMux.Get("/{job_run_id}", h.getStatus)
	srvMux.Delete("/{job_run_id}", h.delete)
	srvMux.Get("/{job_run_id}/failed-records", h.failedRecordsV1)
	return srvMux
}

// NewFailedKeysHandler creates a handler for failed keys
//
//   - GET /v2/job-status/{job_run_id}
//   - DELETE /v2/job-status/{job_run_id}
//   - GET /v2/failed-keys/{job_run_id}
//   - DELETE /v2/failed-keys/{job_run_id}
func NewV2Handler(service rsources.JobService, logger logger.Logger) http.Handler {
	h := &handler{
		service: service,
		logger:  logger,
	}
	srvMux := chi.NewRouter()
	srvMux.Get("/{job_run_id}", h.getStatus)
	srvMux.Delete("/{job_run_id}", h.deleteJobStatus)
	srvMux.Get("/{job_run_id}/failed-records", h.failedRecords)
	srvMux.Delete("/{job_run_id}/failed-records", h.deleteFailedRecords)
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

	jobStatus.FixCorruptedStats(h.logger)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)

	err = marshalAndWriteResponse(w, jobStatus)
	if err != nil {
		h.logger.Errorf("error while marshalling and writing response body: %v", err)
	}
}

func (h *handler) deleteJobStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	jobRunId, taskRunId, sourceId := getQueryParams(r)
	if jobRunId == "" {
		http.Error(w, "job_run_id not found", http.StatusBadRequest)
		return
	}

	if err := h.service.DeleteJobStatus(
		ctx,
		jobRunId,
		rsources.JobFilter{TaskRunID: taskRunId, SourceID: sourceId},
	); err != nil {
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

func (h *handler) failedRecordsV1(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var jobRunId string
	var taskRunId, sourceId []string

	jobRunId, taskRunId, sourceId = getQueryParams(r)
	if jobRunId == "" {
		http.Error(w, "job_run_id not found", http.StatusBadRequest)
		return
	}
	var paging rsources.PagingInfo
	if pageSize, ok := r.URL.Query()["pageSize"]; ok && len(pageSize) > 0 {
		paging.Size, _ = strconv.Atoi(pageSize[0])
		if nextPageToken, ok := r.URL.Query()["pageToken"]; ok && len(nextPageToken) > 0 {
			paging.NextPageToken = nextPageToken[0]
		}
	}

	failedRecords, err := h.service.GetFailedRecordsV1(
		ctx, jobRunId, rsources.JobFilter{
			TaskRunID: taskRunId,
			SourceID:  sourceId,
		},
		paging,
	)
	if err != nil {
		httpStatus := http.StatusInternalServerError
		if errors.Is(err, rsources.ErrOperationNotSupported) ||
			errors.Is(err, rsources.ErrInvalidPaginationToken) {
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

func (h *handler) failedRecords(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var jobRunId string
	var taskRunId, sourceId []string

	jobRunId, taskRunId, sourceId = getQueryParams(r)
	if jobRunId == "" {
		http.Error(w, "job_run_id not found", http.StatusBadRequest)
		return
	}
	var paging rsources.PagingInfo
	if pageSize, ok := r.URL.Query()["pageSize"]; ok && len(pageSize) > 0 {
		paging.Size, _ = strconv.Atoi(pageSize[0])
		if nextPageToken, ok := r.URL.Query()["pageToken"]; ok && len(nextPageToken) > 0 {
			paging.NextPageToken = nextPageToken[0]
		}
	}

	failedRecords, err := h.service.GetFailedRecords(
		ctx, jobRunId, rsources.JobFilter{
			TaskRunID: taskRunId,
			SourceID:  sourceId,
		},
		paging,
	)
	if err != nil {
		httpStatus := http.StatusInternalServerError
		if errors.Is(err, rsources.ErrOperationNotSupported) ||
			errors.Is(err, rsources.ErrInvalidPaginationToken) {
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

func (h *handler) deleteFailedRecords(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	jobRunId, taskRunId, sourceId := getQueryParams(r)
	if jobRunId == "" {
		http.Error(w, "job_run_id not found", http.StatusBadRequest)
		return
	}

	if err := h.service.DeleteFailedRecords(
		ctx,
		jobRunId,
		rsources.JobFilter{TaskRunID: taskRunId, SourceID: sourceId},
	); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func getQueryParams(r *http.Request) (jobRunID string, taskRunID, sourceID []string) {
	jobRunID = chi.URLParam(r, "job_run_id")
	if jobRunID == "" {
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
