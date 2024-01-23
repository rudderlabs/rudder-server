package api

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/rudderlabs/rudder-go-kit/chiware"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	suppression "github.com/rudderlabs/rudder-server/enterprise/suppress-user"
	"github.com/rudderlabs/rudder-server/suppression-backup-service/model"
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

func (api *API) Handler(ctx context.Context, opts ...OptFunc) http.Handler {
	opt := &Opt{}
	for _, o := range opts {
		o(opt)
	}
	srvMux := chi.NewMux()
	srvMux.Use(chiware.StatMiddleware(ctx, stats.Default, "suppression_backup_service"))
	srvMux.Use(middleware.Compress(gzip.BestSpeed))
	srvMux.HandleFunc("/health", http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusOK)
		fmt.Fprintln(rw, "OK")
	}))
	srvMux.Get("/full-export", ServeFile(api.fullBackup))
	srvMux.Get("/latest-export", ServeFile(api.latestBackup))
	srvMux.Get("/full-export/checkpoint", func(w http.ResponseWriter, r *http.Request) {
		if opt.currentToken == nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		token, ok := opt.currentToken.Load().([]byte)
		if !ok {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, token)
	})
	srvMux.Get("/full-export/setCheckpoint", func(w http.ResponseWriter, r *http.Request) {
		seqID := chi.URLParam(r, "seqID")
		if seqID == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		seqIDInt, err := strconv.Atoi(seqID)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if opt.syncInProgress == nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if !opt.syncInProgress.CompareAndSwap(false, true) {
			w.WriteHeader(http.StatusConflict)
			return
		}
		defer opt.syncInProgress.Store(false)

		if opt.repo == nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		var tokenStruct struct {
			SyncStartTime time.Time
			SyncSeqId     int
		}
		tokenStruct.SyncStartTime = time.Now().Add(-1 * config.GetDuration("Migration.SyncStartTime", 1, time.Hour))
		tokenStruct.SyncSeqId = seqIDInt
		token, err := json.Marshal(tokenStruct)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if err := opt.repo.Add(nil, token); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

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

type Opt struct {
	currentToken   *atomic.Value
	syncInProgress *atomic.Bool
	repo           suppression.Repository
}

type OptFunc func(*Opt)

func WithSyncInProgress(syncInProgress *atomic.Bool) OptFunc {
	return func(opt *Opt) {
		opt.syncInProgress = syncInProgress
	}
}

func WithRepo(repo suppression.Repository) OptFunc {
	return func(opt *Opt) {
		opt.repo = repo
	}
}

func WithCurrentToken(currentToken *atomic.Value) OptFunc {
	return func(opt *Opt) {
		opt.currentToken = currentToken
	}
}
