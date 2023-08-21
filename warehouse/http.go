package warehouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bugsnag/bugsnag-go/v2"
	"github.com/go-chi/chi/v5"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/api"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/rudderlabs/rudder-server/warehouse/jobs"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

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
	notifier      *pgnotifier.PGNotifier
	bcConfig      backendconfig.BackendConfig
	tenantManager *multitenant.Manager
	bcManager     *backendConfigManager
	asyncManager  *jobs.AsyncJobWh
	stagingRepo   *repo.StagingFiles
	uploadRepo    *repo.Uploads
	schemaRepo    *repo.WHSchema

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
	notifier *pgnotifier.PGNotifier,
	tenantManager *multitenant.Manager,
	bcManager *backendConfigManager,
	asyncManager *jobs.AsyncJobWh,
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
		asyncManager:  asyncManager,
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

	if isStandAlone(a.mode) {
		srvMux.Get("/health", a.healthHandler)
	}
	if a.config.runningMode != DegradedMode {
		if isMaster(a.mode) {
			a.addWarehouseEndpoints(ctx, srvMux)

			a.logger.Infof("Starting warehouse master service in %d", a.config.webPort)
		} else {
			a.logger.Infof("Starting warehouse slave service in %d", a.config.webPort)
		}
	}

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", a.config.webPort),
		Handler:           bugsnag.Handler(srvMux),
		ReadHeaderTimeout: a.config.readerHeaderTimeout,
	}
	return kithttputil.ListenAndServe(ctx, srv)
}

func (a *Api) addWarehouseEndpoints(ctx context.Context, mux *chi.Mux) {
	a.logger.Infof("waiting for BackendConfig before starting on %d", a.config.webPort)

	a.bcConfig.WaitForConfig(ctx)

	// process the staging file
	mux.Handle("/v1/process", (&api.WarehouseAPI{
		Logger:      a.logger,
		Stats:       a.statsFactory,
		Repo:        a.stagingRepo,
		Multitenant: a.tenantManager,
	}).Handler())

	// triggers upload only when there are pending events and triggerUpload is sent for a sourceId
	mux.Post("/v1/warehouse/pending-events", a.pendingEventsHandler)

	// triggers uploads for a source
	mux.Post("/v1/warehouse/trigger-upload", a.triggerUploadHandler)

	// Warehouse Async Job end-points
	mux.Post("/v1/warehouse/jobs", a.asyncManager.InsertJobHandler)       // FIXME: add degraded mode
	mux.Get("/v1/warehouse/jobs/status", a.asyncManager.StatusJobHandler) // FIXME: add degraded mode

	// TODO: Remove this endpoint once sources change is released
	// fetch schema info
	mux.Get("/v1/warehouse/fetch-tables", a.fetchTablesHandler)
	mux.Get("/internal/v1/warehouse/fetch-tables", a.fetchTablesHandler)
}

func (a *Api) healthHandler(w http.ResponseWriter, r *http.Request) {
	var dbService, pgNotifierService string

	ctx, cancel := context.WithTimeout(r.Context(), a.config.healthTimeout)
	defer cancel()

	if a.config.runningMode != DegradedMode {
		if !checkHealth(ctx, a.notifier.GetDBHandle()) {
			http.Error(w, "Cannot connect to pgNotifierService", http.StatusInternalServerError)
			return
		}
		pgNotifierService = "UP"
	}

	if isMaster(a.mode) {
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
	"pgNotifier": %q,
	"acceptingEvents": "TRUE",
	"warehouseMode": %q
}
	`,
		dbService,
		pgNotifierService,
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
	a.logger.LogRequest(r)

	defer func() { _ = r.Body.Close() }()

	var pendingEventsReq pendingEventsRequest

	if err := json.NewDecoder(r.Body).Decode(&pendingEventsReq); err != nil {
		a.logger.Errorf("invalid JSON body for pending events: %v", err)
		http.Error(w, "invalid JSON in request body", http.StatusBadRequest)
		return
	}

	sourceID, taskRunID := pendingEventsReq.SourceID, pendingEventsReq.TaskRunID
	if sourceID == "" || taskRunID == "" {
		a.logger.Errorf("empty source_id or task_run_id in the pending events request")
		http.Error(w, "empty source_id or task_run_id", http.StatusBadRequest)
		return
	}

	workspaceID, err := a.tenantManager.SourceToWorkspace(r.Context(), sourceID)
	if err != nil {
		a.logger.Errorf("checking if source is degraded: %v", err)
		http.Error(w, "workspaceID from sourceID not found", http.StatusBadRequest)
		return
	}

	if a.tenantManager.DegradedWorkspace(workspaceID) {
		a.logger.Infof("workspace (id: %q) is degrade", workspaceID)
		http.Error(w, "workspace is degraded", http.StatusServiceUnavailable)
		return
	}

	pendingStagingFileCount, err := a.stagingRepo.CountPendingForSource(r.Context(), sourceID)
	if err != nil {
		if errors.Is(r.Context().Err(), context.Canceled) {
			http.Error(w, "request canceled", http.StatusBadRequest)
			return
		}
		a.logger.Errorf("fetching pending staging file: %v", err)
		http.Error(w, "can't get pending staging file", http.StatusInternalServerError)
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
			http.Error(w, "request canceled", http.StatusBadRequest)
			return
		}
		a.logger.Errorf("fetching pending uploads: %s", err)
		http.Error(w, "can't get pending uploads", http.StatusInternalServerError)
		return
	}

	filters = []repo.FilterBy{
		{Key: "source_id", Value: sourceID},
		{Key: "metadata->>'source_task_run_id'", Value: pendingEventsReq.TaskRunID},
		{Key: "status", Value: "aborted"},
	}
	abortedUploadCount, err := a.uploadRepo.Count(r.Context(), filters...)
	if err != nil {
		if errors.Is(r.Context().Err(), context.Canceled) {
			http.Error(w, "request canceled", http.StatusBadRequest)
			return
		}
		a.logger.Errorf("fetching aborted uploads: %s", err.Error())
		http.Error(w, "can't get aborted uploads", http.StatusInternalServerError)
		return
	}

	pendingEventsAvailable := (pendingStagingFileCount + pendingUploadCount) > 0
	triggerPendingUpload, _ := strconv.ParseBool(r.URL.Query().Get(triggerUploadQPName))

	if pendingEventsAvailable && triggerPendingUpload {
		a.logger.Infof("Triggering upload for all destinations connected to source '%s'", sourceID)

		wh := a.bcManager.WarehousesBySourceID(sourceID)
		if len(wh) == 0 {
			a.logger.Warnf("no warehouse destinations found for source id '%s'", sourceID)
			http.Error(w, "no warehouse found", http.StatusBadRequest)
			return
		}

		for _, warehouse := range wh {
			triggerUpload(warehouse)
		}
	}

	resBody, err := json.Marshal(pendingEventsResponse{
		PendingEvents:            pendingEventsAvailable,
		PendingStagingFilesCount: pendingStagingFileCount,
		PendingUploadCount:       pendingUploadCount,
		AbortedEvents:            abortedUploadCount > 0,
	})
	if err != nil {
		a.logger.Errorf("marshalling response for pending events: %v", err)
		http.Error(w, "can't marshall response", http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(resBody)
}

func (a *Api) triggerUploadHandler(w http.ResponseWriter, r *http.Request) {
	a.logger.LogRequest(r)

	defer func() { _ = r.Body.Close() }()

	var triggerUploadReq triggerUploadRequest

	if err := json.NewDecoder(r.Body).Decode(&triggerUploadReq); err != nil {
		a.logger.Errorf("invalid JSON body for triggering upload: %v", err)
		http.Error(w, "invalid JSON in request body", http.StatusBadRequest)
		return
	}

	sourceID, destinationID := triggerUploadReq.SourceID, triggerUploadReq.DestinationID

	workspaceID, err := a.tenantManager.SourceToWorkspace(r.Context(), sourceID)
	if err != nil {
		a.logger.Errorf("checking if source is degraded: %v", err)
		http.Error(w, "workspaceID from sourceID not found", http.StatusBadRequest)
		return
	}

	if a.tenantManager.DegradedWorkspace(workspaceID) {
		a.logger.Infof("workspace (id: %q) is degrade", workspaceID)
		http.Error(w, "workspace is degraded", http.StatusServiceUnavailable)
		return
	}

	var wh []model.Warehouse
	if sourceID != "" && destinationID == "" {
		wh = a.bcManager.WarehousesBySourceID(sourceID)
	} else if destinationID != "" {
		wh = a.bcManager.WarehousesByDestID(destinationID)
	}

	if len(wh) == 0 {
		a.logger.Warnf("no warehouse destinations found for source id '%s' or destination id '%s'", sourceID, destinationID)
		http.Error(w, "no warehouse found", http.StatusBadRequest)
		return
	}

	for _, warehouse := range wh {
		triggerUpload(warehouse)
	}

	w.WriteHeader(http.StatusOK)
}

func (a *Api) fetchTablesHandler(w http.ResponseWriter, r *http.Request) {
	a.logger.LogRequest(r)

	defer func() { _ = r.Body.Close() }()

	var connectionsTableRequest fetchTablesRequest

	if err := json.NewDecoder(r.Body).Decode(&connectionsTableRequest); err != nil {
		a.logger.Errorf("invalid JSON body for fetching tables: %v", err)
		http.Error(w, "invalid JSON in request body", http.StatusBadRequest)
		return
	}

	tables, err := a.schemaRepo.GetTablesForConnection(r.Context(), connectionsTableRequest.Connections)
	if err != nil {
		if errors.Is(r.Context().Err(), context.Canceled) {
			http.Error(w, "request canceled", http.StatusBadRequest)
			return
		}
		a.logger.Errorf("fetching tables: %v", err)
		http.Error(w, "can't fetch tables", http.StatusInternalServerError)
		return
	}

	resBody, err := json.Marshal(fetchTablesResponse{
		ConnectionsTables: tables,
	})
	if err != nil {
		a.logger.Errorf("marshalling response while fetching tables: %v", err)
		http.Error(w, "can't marshall response", http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(resBody)
}
