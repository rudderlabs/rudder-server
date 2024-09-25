package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/utils/crash"
	"github.com/rudderlabs/rudder-server/warehouse/internal/mode"

	"github.com/rudderlabs/rudder-server/services/notifier"
	"github.com/rudderlabs/rudder-server/warehouse/bcm"

	"github.com/go-chi/chi/v5"

	"github.com/rudderlabs/rudder-server/warehouse/internal/api"
	ierrors "github.com/rudderlabs/rudder-server/warehouse/internal/errors"
	lf "github.com/rudderlabs/rudder-server/warehouse/logfield"

	"github.com/rudderlabs/rudder-go-kit/chiware"
	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
	"github.com/rudderlabs/rudder-server/warehouse/source"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const triggerUploadQPName = "triggerUpload"

type pendingEventsRequest struct {
	SourceID  string `json:"source_id"`
	TaskRunID string `json:"task_run_id"`
}

type pendingEventsResponse struct {
	PendingEvents            bool  `json:"pending_events"`
	PendingStagingFilesCount int64 `json:"pending_staging_files"`
	PendingUploadCount       int64 `json:"pending_uploads"`
	AbortedEvents            bool  `json:"aborted_events"`
}

type fetchTablesRequest struct {
	Connections []warehouseutils.SourceIDDestinationID `json:"connections"`
}

type fetchTablesResponse struct {
	ConnectionsTables []warehouseutils.FetchTableInfo `json:"connections_tables"`
}

type triggerUploadRequest struct {
	SourceID      string `json:"source_id"`
	DestinationID string `json:"destination_id"`
}

type Api struct {
	mode          string
	logger        logger.Logger
	statsFactory  stats.Stats
	db            *sqlmw.DB
	notifier      *notifier.Notifier
	bcConfig      backendconfig.BackendConfig
	tenantManager *multitenant.Manager
	bcManager     *bcm.BackendConfigManager
	sourceManager *source.Manager
	stagingRepo   *repo.StagingFiles
	uploadRepo    *repo.Uploads
	schemaRepo    *repo.WHSchema
	triggerStore  *sync.Map

	config struct {
		healthTimeout       time.Duration
		readerHeaderTimeout time.Duration
		runningMode         string
		webPort             int
		mode                string
	}
}

func NewApi(
	mode string,
	conf *config.Config,
	log logger.Logger,
	statsFactory stats.Stats,
	bcConfig backendconfig.BackendConfig,
	db *sqlmw.DB,
	notifier *notifier.Notifier,
	tenantManager *multitenant.Manager,
	bcManager *bcm.BackendConfigManager,
	sourceManager *source.Manager,
	triggerStore *sync.Map,
) *Api {
	a := &Api{
		mode:          mode,
		logger:        log.Child("api"),
		db:            db,
		notifier:      notifier,
		bcConfig:      bcConfig,
		statsFactory:  statsFactory,
		tenantManager: tenantManager,
		bcManager:     bcManager,
		sourceManager: sourceManager,
		triggerStore:  triggerStore,
		stagingRepo:   repo.NewStagingFiles(db),
		uploadRepo:    repo.NewUploads(db),
		schemaRepo:    repo.NewWHSchemas(db),
	}
	a.config.healthTimeout = conf.GetDuration("Warehouse.healthTimeout", 10, time.Second)
	a.config.readerHeaderTimeout = conf.GetDuration("Warehouse.readerHeaderTimeout", 3, time.Second)
	a.config.runningMode = conf.GetString("Warehouse.runningMode", "")
	a.config.webPort = conf.GetInt("Warehouse.webPort", 8082)

	return a
}

func (a *Api) Start(ctx context.Context) error {
	srvMux := chi.NewRouter()
	srvMux.Use(
		chiware.StatMiddleware(ctx, a.statsFactory, "warehouse"),
	)

	if mode.IsStandAlone(a.mode) {
		srvMux.Get("/health", a.healthHandler)
	}
	if !mode.IsDegraded(a.config.runningMode) {
		if mode.IsMaster(a.mode) {
			a.addMasterEndpoints(ctx, srvMux)

			a.logger.Infow("Starting warehouse master service on" + strconv.Itoa(a.config.webPort))
		} else {
			a.logger.Infow("Starting warehouse slave service on" + strconv.Itoa(a.config.webPort))
		}
	}

	srv := &http.Server{
		Addr:              net.JoinHostPort("", strconv.Itoa(a.config.webPort)),
		Handler:           crash.Handler(srvMux),
		ReadHeaderTimeout: a.config.readerHeaderTimeout,
	}
	return kithttputil.ListenAndServe(ctx, srv)
}

func (a *Api) addMasterEndpoints(ctx context.Context, r chi.Router) {
	a.logger.Infow("waiting for BackendConfig before starting on " + strconv.Itoa(a.config.webPort))

	a.bcConfig.WaitForConfig(ctx)

	r.Handle("/v1/process", (&api.WarehouseAPI{
		Logger:      a.logger,
		Stats:       a.statsFactory,
		Repo:        a.stagingRepo,
		Multitenant: a.tenantManager,
	}).Handler())

	r.Route("/v1", func(r chi.Router) {
		r.Route("/warehouse", func(r chi.Router) {
			r.Post("/pending-events", a.logMiddleware(a.pendingEventsHandler))
			r.Post("/trigger-upload", a.logMiddleware(a.triggerUploadHandler))

			r.Post("/jobs", a.logMiddleware(a.sourceManager.InsertJobHandler))       // TODO: add degraded mode
			r.Get("/jobs/status", a.logMiddleware(a.sourceManager.StatusJobHandler)) // TODO: add degraded mode

			r.Get("/fetch-tables", a.logMiddleware(a.fetchTablesHandler)) // TODO: Remove this endpoint once sources change is released
		})
	})
	r.Route("/internal", func(r chi.Router) {
		r.Route("/v1", func(r chi.Router) {
			r.Route("/warehouse", func(r chi.Router) {
				r.Get("/fetch-tables", a.logMiddleware(a.fetchTablesHandler))
			})
		})
	})
}

func (a *Api) healthHandler(w http.ResponseWriter, r *http.Request) {
	var dbService, notifierService string

	ctx, cancel := context.WithTimeout(r.Context(), a.config.healthTimeout)
	defer cancel()

	if !mode.IsDegraded(a.config.runningMode) {
		if !a.notifier.CheckHealth(ctx) {
			http.Error(w, "Cannot connect to notifierService", http.StatusInternalServerError)
			return
		}
		notifierService = "UP"
	}

	if mode.IsMaster(a.mode) {
		if !checkHealth(ctx, a.db.DB) {
			http.Error(w, "Cannot connect to dbService", http.StatusInternalServerError)
			return
		}
		dbService = "UP"
	}

	healthVal := fmt.Sprintf(`
{
	"server": "UP",
	"db": %q,
	"notifier": %q,
	"acceptingEvents": "TRUE",
	"warehouseMode": %q
}
	`,
		dbService,
		notifierService,
		strings.ToUpper(a.mode),
	)

	_, _ = w.Write([]byte(healthVal))
}

func checkHealth(ctx context.Context, db *sql.DB) bool {
	if db == nil {
		return false
	}

	healthCheckMsg := "Rudder Warehouse DB Health Check"
	msg := ""

	err := db.QueryRowContext(ctx, `SELECT '`+healthCheckMsg+`'::text as message;`).Scan(&msg)
	if err != nil {
		return false
	}

	return healthCheckMsg == msg
}

// pendingEventsHandler check whether there are any pending staging files or uploads for the given source id
func (a *Api) pendingEventsHandler(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()

	var payload pendingEventsRequest
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		a.logger.Warnw("invalid JSON in request body for pending events", lf.Error, err.Error())
		http.Error(w, ierrors.ErrInvalidJSONRequestBody.Error(), http.StatusBadRequest)
		return
	}

	sourceID, taskRunID := payload.SourceID, payload.TaskRunID
	if sourceID == "" || taskRunID == "" {
		a.logger.Warnw("empty source or task run id for pending events",
			lf.SourceID, payload.SourceID,
			lf.TaskRunID, payload.TaskRunID,
		)
		http.Error(w, "empty source or task run id", http.StatusBadRequest)
		return
	}

	workspaceID, err := a.tenantManager.SourceToWorkspace(r.Context(), sourceID)
	if err != nil {
		a.logger.Warnw("workspace from source not found for pending events", lf.SourceID, payload.SourceID)
		http.Error(w, ierrors.ErrWorkspaceFromSourceNotFound.Error(), http.StatusBadRequest)
		return
	}

	if a.tenantManager.DegradedWorkspace(workspaceID) {
		a.logger.Infow("workspace is degraded for pending events", lf.WorkspaceID, workspaceID)
		http.Error(w, ierrors.ErrWorkspaceDegraded.Error(), http.StatusServiceUnavailable)
		return
	}

	pendingStagingFileCount, err := a.stagingRepo.CountPendingForSource(r.Context(), sourceID)
	if err != nil {
		if errors.Is(r.Context().Err(), context.Canceled) {
			http.Error(w, ierrors.ErrRequestCancelled.Error(), http.StatusBadRequest)
			return
		}
		a.logger.Errorw("counting pending staging files", lf.Error, err.Error())
		http.Error(w, "can't get pending staging files count", http.StatusInternalServerError)
		return
	}

	filters := []repo.FilterBy{
		{Key: "source_id", Value: sourceID},
		{Key: "metadata->>'source_task_run_id'", Value: taskRunID},
		{Key: "status", NotEquals: true, Value: model.ExportedData},
		{Key: "status", NotEquals: true, Value: model.Aborted},
	}
	pendingUploadCount, err := a.uploadRepo.Count(r.Context(), filters...)
	if err != nil {
		if errors.Is(r.Context().Err(), context.Canceled) {
			http.Error(w, ierrors.ErrRequestCancelled.Error(), http.StatusBadRequest)
			return
		}
		a.logger.Errorw("counting pending uploads", lf.Error, err.Error())
		http.Error(w, "can't get pending uploads count", http.StatusInternalServerError)
		return
	}

	filters = []repo.FilterBy{
		{Key: "source_id", Value: sourceID},
		{Key: "metadata->>'source_task_run_id'", Value: payload.TaskRunID},
		{Key: "status", Value: "aborted"},
	}
	abortedUploadCount, err := a.uploadRepo.Count(r.Context(), filters...)
	if err != nil {
		if errors.Is(r.Context().Err(), context.Canceled) {
			http.Error(w, ierrors.ErrRequestCancelled.Error(), http.StatusBadRequest)
			return
		}
		a.logger.Errorw("counting aborted uploads", lf.Error, err.Error())
		http.Error(w, "can't get aborted uploads count", http.StatusInternalServerError)
		return
	}

	pendingEventsAvailable := (pendingStagingFileCount + pendingUploadCount) > 0
	triggerPendingUpload, _ := strconv.ParseBool(r.URL.Query().Get(triggerUploadQPName))

	if pendingEventsAvailable && triggerPendingUpload {
		a.logger.Infow("triggering upload for all destinations connected to source",
			lf.WorkspaceID, workspaceID,
			lf.SourceID, payload.SourceID,
		)

		wh := a.bcManager.WarehousesBySourceID(sourceID)
		if len(wh) == 0 {
			a.logger.Warnw("no warehouse found for pending events",
				lf.WorkspaceID, workspaceID,
				lf.SourceID, payload.SourceID,
			)
			http.Error(w, ierrors.ErrNoWarehouseFound.Error(), http.StatusBadRequest)
			return
		}

		for _, warehouse := range wh {
			a.triggerStore.Store(warehouse.Identifier, struct{}{})
		}
	}

	resBody, err := json.Marshal(pendingEventsResponse{
		PendingEvents:            pendingEventsAvailable,
		PendingStagingFilesCount: pendingStagingFileCount,
		PendingUploadCount:       pendingUploadCount,
		AbortedEvents:            abortedUploadCount > 0,
	})
	if err != nil {
		a.logger.Errorw("marshalling response for pending events", lf.Error, err.Error())
		http.Error(w, ierrors.ErrMarshallResponse.Error(), http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(resBody)
}

func (a *Api) triggerUploadHandler(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()

	var payload triggerUploadRequest
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		a.logger.Warnw("invalid JSON in request body for triggering upload", lf.Error, err.Error())
		http.Error(w, ierrors.ErrInvalidJSONRequestBody.Error(), http.StatusBadRequest)
		return
	}

	workspaceID, err := a.tenantManager.SourceToWorkspace(r.Context(), payload.SourceID)
	if err != nil {
		a.logger.Warnw("workspace from source not found for triggering upload", lf.SourceID, payload.SourceID)
		http.Error(w, ierrors.ErrWorkspaceFromSourceNotFound.Error(), http.StatusBadRequest)
		return
	}

	if a.tenantManager.DegradedWorkspace(workspaceID) {
		a.logger.Infow("workspace is degraded for triggering upload", lf.WorkspaceID, workspaceID)
		http.Error(w, ierrors.ErrWorkspaceDegraded.Error(), http.StatusServiceUnavailable)
		return
	}

	var wh []model.Warehouse
	if payload.SourceID != "" && payload.DestinationID == "" {
		wh = a.bcManager.WarehousesBySourceID(payload.SourceID)
	} else if payload.DestinationID != "" {
		wh = a.bcManager.WarehousesByDestID(payload.DestinationID)
	}
	if len(wh) == 0 {
		a.logger.Warnw("no warehouse found for triggering upload",
			lf.WorkspaceID, workspaceID,
			lf.SourceID, payload.SourceID,
			lf.DestinationID, payload.DestinationID,
		)
		http.Error(w, ierrors.ErrNoWarehouseFound.Error(), http.StatusBadRequest)
		return
	}

	for _, warehouse := range wh {
		a.triggerStore.Store(warehouse.Identifier, struct{}{})
	}

	w.WriteHeader(http.StatusOK)
}

func (a *Api) fetchTablesHandler(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()

	var payload fetchTablesRequest
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		a.logger.Warnw("invalid JSON in request body for fetching tables", lf.Error, err.Error())
		http.Error(w, ierrors.ErrInvalidJSONRequestBody.Error(), http.StatusBadRequest)
		return
	}

	tables, err := a.schemaRepo.GetTablesForConnection(r.Context(), payload.Connections)
	if err != nil {
		if errors.Is(r.Context().Err(), context.Canceled) {
			http.Error(w, ierrors.ErrRequestCancelled.Error(), http.StatusBadRequest)
			return
		}
		a.logger.Errorw("fetching tables", lf.Error, err.Error())
		http.Error(w, "can't fetch tables", http.StatusInternalServerError)
		return
	}

	resBody, err := json.Marshal(fetchTablesResponse{
		ConnectionsTables: tables,
	})
	if err != nil {
		a.logger.Errorw("marshalling response for fetching tables", lf.Error, err.Error())
		http.Error(w, ierrors.ErrMarshallResponse.Error(), http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(resBody)
}

func (a *Api) logMiddleware(delegate http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		a.logger.LogRequest(r)
		delegate.ServeHTTP(w, r)
	}
}
