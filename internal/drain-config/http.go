package drain_config

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"
)

func (dcm *drainConfigManager) DrainConfigHttpHandler() http.Handler {
	srvMux := chi.NewRouter()
	srvMux.Put("/job/{job_run_id}", dcm.drainJob)
	return srvMux
}

func (dcm *drainConfigManager) drainJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	jobRunIDVal := chi.URLParam(r, "job_run_id")
	if jobRunIDVal == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := dcm.insert(ctx, jobRunIDKey, jobRunIDVal); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (dcm *drainConfigManager) insert(ctx context.Context, key, value string) error {
	_, err := dcm.db.ExecContext(
		ctx,
		"INSERT INTO drain_config (key, value) VALUES ($1, $2)",
		key,
		value,
	)
	if err != nil {
		return err
	}
	return nil
}

func ErrorResponder(errMsg string) http.Handler {
	return http.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, errMsg, http.StatusInternalServerError)
	}))
}
