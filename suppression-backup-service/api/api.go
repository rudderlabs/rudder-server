package api

import (
	"compress/gzip"
	"context"
	"fmt"
	"net/http"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/rudderlabs/rudder-server/middleware"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/suppression-backup-service/model"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

type API struct {
	log          logger.Logger
	fullBackup   model.File
	latestBackup model.File
}

func NewAPI(logger logger.Logger, fullBackup, latestBackup model.File) *API {
	return &API{
		log:          logger,
		fullBackup:   fullBackup,
		latestBackup: latestBackup,
	}
}

func (api *API) Handler(ctx context.Context) http.Handler {
	srvMux := mux.NewRouter()
	srvMux.Use(middleware.StatMiddleware(ctx, srvMux, stats.Default, "suppression_backup_service"))
	srvMux.HandleFunc("/health", http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusOK)
		fmt.Fprintln(rw, "OK")
	}))
	srvMux.HandleFunc("/full-export", ServeFile(api.fullBackup)).Methods(http.MethodGet)
	srvMux.HandleFunc("/latest-export", ServeFile(api.latestBackup)).Methods(http.MethodGet)
	api.log.Info("Suppression backup service Handler declared")
	return handlers.CompressHandlerLevel(srvMux, gzip.BestSpeed)
}

func ServeFile(file model.File) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		file.Mu.RLock()
		defer file.Mu.RUnlock()
		http.ServeFile(w, r, file.Path)
		setHeader(w, http.StatusOK)
	}
}

func setHeader(w http.ResponseWriter, code int) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(code)
}
