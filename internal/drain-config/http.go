package drain_config

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"
)

func (dcm *drainConfigManager) DrainConfigHttpHandler() http.Handler {
	srvMux := chi.NewRouter()
	srvMux.Put("/job/{JobRunId}", dcm.drainJob)
	return srvMux
}

func (dcm *drainConfigManager) drainJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	jobRunID := getQueryParams(r)
	if jobRunID == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := dcm.insert(ctx, jobRunID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func getQueryParams(r *http.Request) string {
	return chi.URLParam(r, "JobRunId")
}

func (dcm *drainConfigManager) insert(ctx context.Context, jobRunID string) error {
	_, err := dcm.db.ExecContext(
		ctx,
		"INSERT INTO drain_config (key, value) VALUES ('drain.jobRunIDs', $1)",
		jobRunID,
	)
	if err != nil {
		return err
	}
	return nil
}
