package api

import (
	"compress/gzip"
	"context"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/rudderlabs/rudder-go-kit/chiware"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/suppression-backup-service/model"
)

type API struct {
	log          logger.Logger
	latestBackup model.File
}

func NewAPI(logger logger.Logger, latestBackup model.File) *API {
	return &API{
		log:          logger,
		latestBackup: latestBackup,
	}
}

func (api *API) Handler(ctx context.Context) http.Handler {
	srvMux := chi.NewMux()
	srvMux.Use(chiware.StatMiddleware(ctx, srvMux, stats.Default, "suppression_backup_service"))
	srvMux.Use(middleware.Compress(gzip.BestSpeed))
	srvMux.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintln(rw, "OK")
	})
	srvMux.Get("/full-export", ServeFile(api.latestBackup))
	srvMux.Get("/latest-export", ServeFile(api.latestBackup)) // TODO: remove this endpoint in future

	api.log.Info("Suppression backup service Handler declared")
	return srvMux
}

func ServeFile(file model.File) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		file.Mu.RLock()
		defer file.Mu.RUnlock()
		http.ServeFile(w, r, file.Path)
	}
}
