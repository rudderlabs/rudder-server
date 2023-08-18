package warehouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/encoding"

	"github.com/bugsnag/bugsnag-go/v2"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-chi/chi/v5"
	"github.com/samber/lo"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/app"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/info"
	"github.com/rudderlabs/rudder-server/services/controlplane"
	"github.com/rudderlabs/rudder-server/services/db"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/services/validators"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/warehouse/archive"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/api"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/rudderlabs/rudder-server/warehouse/jobs"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	application                app.App
	webPort                    int
	dbHandle                   *sql.DB
	wrappedDBHandle            *sqlquerywrapper.DB
	dbHandleTimeout            time.Duration
	notifier                   pgnotifier.PGNotifier
	tenantManager              *multitenant.Manager
	controlPlaneClient         *controlplane.Client
	uploadFreqInS              int64
	lastProcessedMarkerMap     map[string]int64
	lastProcessedMarkerExp     = expvar.NewMap("lastProcessedMarkerMap")
	lastProcessedMarkerMapLock sync.RWMutex
	warehouseMode              string
	bcManager                  *backendConfigManager
	triggerUploadsMap          map[string]bool // `whType:sourceID:destinationID` -> boolean value representing if an upload was triggered or not
	triggerUploadsMapLock      sync.RWMutex
	pkgLogger                  logger.Logger
	runningMode                string
	ShouldForceSetLowerVersion bool
	asyncWh                    *jobs.AsyncJobWh
)

var (
	host, user, password, dbname, sslMode, appName string
	port                                           int
)

var defaultUploadPriority = 100

// warehouses worker modes
const (
	EmbeddedMode       = "embedded"
	EmbeddedMasterMode = "embedded_master"
)

const (
	DegradedMode        = "degraded"
	triggerUploadQPName = "triggerUpload"
)

type (
	WorkerIdentifierT string
	JobID             int64
)

func Init4() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("warehouse")
}

func loadConfig() {
	// Port where WH is running
	config.RegisterIntConfigVariable(8082, &webPort, false, 1, "Warehouse.webPort")
	config.RegisterInt64ConfigVariable(1800, &uploadFreqInS, true, 1, "Warehouse.uploadFreqInS")
	lastProcessedMarkerMap = map[string]int64{}
	config.RegisterStringConfigVariable("embedded", &warehouseMode, false, "Warehouse.mode")
	host = config.GetString("WAREHOUSE_JOBS_DB_HOST", "localhost")
	user = config.GetString("WAREHOUSE_JOBS_DB_USER", "ubuntu")
	dbname = config.GetString("WAREHOUSE_JOBS_DB_DB_NAME", "ubuntu")
	port = config.GetInt("WAREHOUSE_JOBS_DB_PORT", 5432)
	password = config.GetString("WAREHOUSE_JOBS_DB_PASSWORD", "ubuntu") // Reading secrets from
	sslMode = config.GetString("WAREHOUSE_JOBS_DB_SSL_MODE", "disable")
	triggerUploadsMap = map[string]bool{}
	runningMode = config.GetString("Warehouse.runningMode", "")
	config.RegisterBoolConfigVariable(true, &ShouldForceSetLowerVersion, false, "SQLMigrator.forceSetLowerVersion")
	config.RegisterDurationConfigVariable(5, &dbHandleTimeout, true, time.Minute, []string{"Warehouse.dbHandleTimeout", "Warehouse.dbHanndleTimeoutInMin"}...)

	appName = misc.DefaultString("rudder-server").OnError(os.Hostname())
}

func getUploadFreqInS(syncFrequency string) int64 {
	freqInMin, err := strconv.ParseInt(syncFrequency, 10, 64)
	if err != nil {
		return uploadFreqInS
	}
	return freqInMin * 60
}

func uploadFrequencyExceeded(warehouse model.Warehouse, syncFrequency string) bool {
	freqInS := getUploadFreqInS(syncFrequency)
	lastProcessedMarkerMapLock.RLock()
	defer lastProcessedMarkerMapLock.RUnlock()
	if lastExecTime, ok := lastProcessedMarkerMap[warehouse.Identifier]; ok && timeutil.Now().Unix()-lastExecTime < freqInS {
		return true
	}
	return false
}

func setLastProcessedMarker(warehouse model.Warehouse, lastProcessedTime time.Time) {
	lastProcessedMarkerMapLock.Lock()
	defer lastProcessedMarkerMapLock.Unlock()
	lastProcessedMarkerMap[warehouse.Identifier] = lastProcessedTime.Unix()
	lastProcessedMarkerExp.Set(warehouse.Identifier, lastProcessedTime)
}

func getBucketFolder(batchID, tableName string) string {
	return fmt.Sprintf(`%v-%v`, batchID, tableName)
}

// Gets the config from config backend and extracts enabled write keys
func monitorDestRouters(ctx context.Context) error {
	dstToWhRouter := make(map[string]*router)

	ch := tenantManager.WatchConfig(ctx)
	for configData := range ch {
		err := onConfigDataEvent(ctx, configData, dstToWhRouter)
		if err != nil {
			return err
		}
	}

	g, _ := errgroup.WithContext(context.Background())
	for _, router := range dstToWhRouter {
		router := router
		g.Go(router.Shutdown)
	}
	return g.Wait()
}

func onConfigDataEvent(ctx context.Context, configMap map[string]backendconfig.ConfigT, dstToWhRouter map[string]*router) error {
	enabledDestinations := make(map[string]bool)
	for _, wConfig := range configMap {
		for _, source := range wConfig.Sources {
			for _, destination := range source.Destinations {
				enabledDestinations[destination.DestinationDefinition.Name] = true

				if !slices.Contains(warehouseutils.WarehouseDestinations, destination.DestinationDefinition.Name) {
					continue
				}

				router, ok := dstToWhRouter[destination.DestinationDefinition.Name]
				if ok {
					pkgLogger.Debug("Enabling existing Destination: ", destination.DestinationDefinition.Name)
					router.Enable()
					continue
				}

				pkgLogger.Info("Starting a new Warehouse Destination Router: ", destination.DestinationDefinition.Name)

				router, err := newRouter(
					ctx,
					application,
					destination.DestinationDefinition.Name,
					config.Default,
					pkgLogger,
					stats.Default,
					wrappedDBHandle,
					&notifier,
					tenantManager,
					controlPlaneClient,
					bcManager,
					encoding.NewFactory(config.Default),
				)
				if err != nil {
					return fmt.Errorf("setup warehouse %q: %w", destination.DestinationDefinition.Name, err)
				}
				dstToWhRouter[destination.DestinationDefinition.Name] = router
			}
		}
	}

	keys := lo.Keys(dstToWhRouter)
	for _, key := range keys {
		if _, ok := enabledDestinations[key]; !ok {
			if wh, ok := dstToWhRouter[key]; ok {
				pkgLogger.Info("Disabling a existing warehouse destination: ", key)
				wh.Disable()
			}
		}
	}

	return nil
}

func setupTables(dbHandle *sql.DB) error {
	m := &migrator.Migrator{
		Handle:                     dbHandle,
		MigrationsTable:            "wh_schema_migrations",
		ShouldForceSetLowerVersion: ShouldForceSetLowerVersion,
	}

	operation := func() error {
		return m.Migrate("warehouse")
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)
	err := backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		pkgLogger.Warnf("Failed to setup WH db tables: %v, retrying after %v", err, t)
	})
	if err != nil {
		return fmt.Errorf("could not run warehouse database migrations: %w", err)
	}
	return nil
}

func pendingEventsHandler(w http.ResponseWriter, r *http.Request) {
	// TODO : respond with errors in a common way
	pkgLogger.LogRequest(r)

	ctx := r.Context()
	// read body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		pkgLogger.Errorf("[WH]: Error reading body: %v", err)
		http.Error(w, "can't read body", http.StatusBadRequest)
		return
	}
	defer func() { _ = r.Body.Close() }()

	// unmarshall body
	var pendingEventsReq warehouseutils.PendingEventsRequest
	err = json.Unmarshal(body, &pendingEventsReq)
	if err != nil {
		pkgLogger.Errorf("[WH]: Error unmarshalling body: %v", err)
		http.Error(w, "can't unmarshall body", http.StatusBadRequest)
		return
	}

	sourceID, taskRunID := pendingEventsReq.SourceID, pendingEventsReq.TaskRunID
	// return error if source id is empty
	if sourceID == "" || taskRunID == "" {
		pkgLogger.Errorf("empty source_id or task_run_id in the pending events request")
		http.Error(w, "empty source_id or task_run_id", http.StatusBadRequest)
		return
	}

	workspaceID, err := tenantManager.SourceToWorkspace(ctx, sourceID)
	if err != nil {
		pkgLogger.Errorf("[WH]: Error checking if source is degraded: %v", err)
		http.Error(w, "workspaceID from sourceID not found", http.StatusBadRequest)
		return
	}

	if tenantManager.DegradedWorkspace(workspaceID) {
		pkgLogger.Infof("[WH]: Workspace (id: %q) is degraded: %v", workspaceID, err)
		http.Error(w, "workspace is in degraded mode", http.StatusServiceUnavailable)
		return
	}

	pendingEvents := false
	var (
		pendingStagingFileCount int64
		pendingUploadCount      int64
	)

	// check whether there are any pending staging files or uploads for the given source id
	// get pending staging files
	pendingStagingFileCount, err = repo.NewStagingFiles(wrappedDBHandle).CountPendingForSource(ctx, sourceID)
	if err != nil {
		err := fmt.Errorf("error getting pending staging file count : %v", err)
		pkgLogger.Errorf("[WH]: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	filters := []repo.FilterBy{
		{Key: "source_id", Value: sourceID},
		{Key: "metadata->>'source_task_run_id'", Value: taskRunID},
		{Key: "status", NotEquals: true, Value: model.ExportedData},
		{Key: "status", NotEquals: true, Value: model.Aborted},
	}

	pendingUploadCount, err = getFilteredCount(ctx, filters...)

	if err != nil {
		pkgLogger.Errorf("getting pending uploads count", "error", err)
		http.Error(w, fmt.Sprintf(
			"getting pending uploads count: %s", err.Error()),
			http.StatusInternalServerError)
		return
	}

	filters = []repo.FilterBy{
		{Key: "source_id", Value: sourceID},
		{Key: "metadata->>'source_task_run_id'", Value: pendingEventsReq.TaskRunID},
		{Key: "status", Value: "aborted"},
	}

	abortedUploadCount, err := getFilteredCount(ctx, filters...)
	if err != nil {
		pkgLogger.Errorf("getting aborted uploads count", "error", err.Error())
		http.Error(w, fmt.Sprintf("getting aborted uploads count: %s", err), http.StatusInternalServerError)
		return
	}

	// if there are any pending staging files or uploads, set pending events as true
	if (pendingStagingFileCount + pendingUploadCount) > int64(0) {
		pendingEvents = true
	}

	// read `triggerUpload` queryParam
	var triggerPendingUpload bool
	triggerUploadQP := r.URL.Query().Get(triggerUploadQPName)
	if triggerUploadQP != "" {
		triggerPendingUpload, _ = strconv.ParseBool(triggerUploadQP)
	}

	// trigger upload if there are pending events and triggerPendingUpload is true
	if pendingEvents && triggerPendingUpload {
		pkgLogger.Infof("[WH]: Triggering upload for all wh destinations connected to source '%s'", sourceID)

		wh := bcManager.WarehousesBySourceID(sourceID)

		// return error if no such destinations found
		if len(wh) == 0 {
			err := fmt.Errorf("no warehouse destinations found for source id '%s'", sourceID)
			pkgLogger.Errorf("[WH]: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		for _, warehouse := range wh {
			triggerUpload(warehouse)
		}
	}

	// create and write response
	res := warehouseutils.PendingEventsResponse{
		PendingEvents:            pendingEvents,
		PendingStagingFilesCount: pendingStagingFileCount,
		PendingUploadCount:       pendingUploadCount,
		AbortedEvents:            abortedUploadCount > 0,
	}

	resBody, err := json.Marshal(res)
	if err != nil {
		err := fmt.Errorf("failed to marshall pending events response : %v", err)
		pkgLogger.Errorf("[WH]: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(resBody)
}

func getFilteredCount(ctx context.Context, filters ...repo.FilterBy) (int64, error) {
	pkgLogger.Debugf("fetching filtered count")
	return repo.NewUploads(wrappedDBHandle).Count(ctx, filters...)
}

func getPendingUploadCount(filters ...warehouseutils.FilterBy) (uploadCount int64, err error) {
	pkgLogger.Debugf("Fetching pending upload count with filters: %v", filters)

	query := fmt.Sprintf(`
		SELECT
		  COUNT(*)
		FROM
		  %[1]s
		WHERE
		  %[1]s.status NOT IN ('%[2]s', '%[3]s')
	`,
		warehouseutils.WarehouseUploadsTable,
		model.ExportedData,
		model.Aborted,
	)

	args := make([]interface{}, 0)
	for i, filter := range filters {
		query += fmt.Sprintf(" AND %s=$%d", filter.Key, i+1)
		args = append(args, filter.Value)
	}

	err = wrappedDBHandle.QueryRow(query, args...).Scan(&uploadCount)
	if err != nil && err != sql.ErrNoRows {
		err = fmt.Errorf("query: %s failed with Error : %w", query, err)
		return
	}

	return uploadCount, nil
}

func triggerUploadHandler(w http.ResponseWriter, r *http.Request) {
	// TODO : respond with errors in a common way
	pkgLogger.LogRequest(r)

	ctx := r.Context()

	// read body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		pkgLogger.Errorf("[WH]: Error reading body: %v", err)
		http.Error(w, "can't read body", http.StatusBadRequest)
		return
	}
	defer func() { _ = r.Body.Close() }()

	// unmarshall body
	var triggerUploadReq warehouseutils.TriggerUploadRequest
	err = json.Unmarshal(body, &triggerUploadReq)
	if err != nil {
		pkgLogger.Errorf("[WH]: Error unmarshalling body: %v", err)
		http.Error(w, "can't unmarshall body", http.StatusBadRequest)
		return
	}

	workspaceID, err := tenantManager.SourceToWorkspace(ctx, triggerUploadReq.SourceID)
	if err != nil {
		pkgLogger.Errorf("[WH]: Error checking if source is degraded: %v", err)
		http.Error(w, "workspaceID from sourceID not found", http.StatusBadRequest)
		return
	}

	if tenantManager.DegradedWorkspace(workspaceID) {
		pkgLogger.Infof("[WH]: Workspace (id: %q) is degraded: %v", workspaceID, err)
		http.Error(w, "workspace is in degraded mode", http.StatusServiceUnavailable)
		return
	}

	err = TriggerUploadHandler(triggerUploadReq.SourceID, triggerUploadReq.DestinationID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func TriggerUploadHandler(sourceID, destID string) error {
	// return error if source id and dest id is empty
	if sourceID == "" && destID == "" {
		err := fmt.Errorf("empty source and destination id")
		pkgLogger.Errorf("[WH]: trigger upload : %v", err)
		return err
	}

	var wh []model.Warehouse
	if sourceID != "" && destID == "" {
		wh = bcManager.WarehousesBySourceID(sourceID)
	}
	if destID != "" {
		wh = bcManager.WarehousesByDestID(destID)
	}

	// return error if no such destinations found
	if len(wh) == 0 {
		err := fmt.Errorf("no warehouse destinations found for source id '%s'", sourceID)
		pkgLogger.Errorf("[WH]: %v", err)
		return err
	}

	// iterate over each wh destination and trigger upload
	for _, warehouse := range wh {
		triggerUpload(warehouse)
	}
	return nil
}

func fetchTablesHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	defer func() { _ = r.Body.Close() }()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		pkgLogger.Errorf("[WH]: Error reading body: %v", err)
		http.Error(w, "can't read body", http.StatusBadRequest)
		return
	}

	var connectionsTableRequest warehouseutils.FetchTablesRequest
	err = json.Unmarshal(body, &connectionsTableRequest)
	if err != nil {
		pkgLogger.Errorf("[WH]: Error unmarshalling body: %v", err)
		http.Error(w, "can't unmarshall body", http.StatusBadRequest)
		return
	}

	schemaRepo := repo.NewWHSchemas(wrappedDBHandle)
	tables, err := schemaRepo.GetTablesForConnection(ctx, connectionsTableRequest.Connections)
	if err != nil {
		pkgLogger.Errorf("[WH]: Error fetching tables: %v", err)
		http.Error(w, "can't fetch tables from schemas repo", http.StatusInternalServerError)
		return
	}
	resBody, err := json.Marshal(warehouseutils.FetchTablesResponse{
		ConnectionsTables: tables,
	})
	if err != nil {
		err := fmt.Errorf("failed to marshall tables to response : %v", err)
		pkgLogger.Errorf("[WH]: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(resBody)
}

func isUploadTriggered(wh model.Warehouse) bool {
	triggerUploadsMapLock.RLock()
	defer triggerUploadsMapLock.RUnlock()
	return triggerUploadsMap[wh.Identifier]
}

func triggerUpload(wh model.Warehouse) {
	triggerUploadsMapLock.Lock()
	defer triggerUploadsMapLock.Unlock()
	pkgLogger.Infof("[WH]: Upload triggered for warehouse '%s'", wh.Identifier)
	triggerUploadsMap[wh.Identifier] = true
}

func clearTriggeredUpload(wh model.Warehouse) {
	triggerUploadsMapLock.Lock()
	defer triggerUploadsMapLock.Unlock()
	delete(triggerUploadsMap, wh.Identifier)
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	var dbService, pgNotifierService string

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if runningMode != DegradedMode {
		if !CheckPGHealth(ctx, notifier.GetDBHandle()) {
			http.Error(w, "Cannot connect to pgNotifierService", http.StatusInternalServerError)
			return
		}
		pgNotifierService = "UP"
	}

	if isMaster() {
		if !CheckPGHealth(ctx, dbHandle) {
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
			"warehouseMode": %q,
			"goroutines": "%d"
		}
	`,
		dbService,
		pgNotifierService,
		strings.ToUpper(warehouseMode),
		runtime.NumGoroutine(),
	)

	_, _ = w.Write([]byte(healthVal))
}

func CheckPGHealth(ctx context.Context, db *sql.DB) bool {
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

func getConnectionString() string {
	if !CheckForWarehouseEnvVars() {
		return misc.GetConnectionString()
	}
	return fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=%s application_name=%s",
		host, port, user, password, dbname, sslMode, appName)
}

func startWebHandler(ctx context.Context) error {
	srvMux := chi.NewRouter()

	// do not register same endpoint when running embedded in rudder backend
	if isStandAlone() {
		srvMux.Get("/health", healthHandler)
	}
	if runningMode != DegradedMode {
		if isMaster() {
			pkgLogger.Infof("WH: Warehouse master service waiting for BackendConfig before starting on %d", webPort)
			backendconfig.DefaultBackendConfig.WaitForConfig(ctx)

			srvMux.Handle("/v1/process", (&api.WarehouseAPI{
				Logger:      pkgLogger,
				Stats:       stats.Default,
				Repo:        repo.NewStagingFiles(wrappedDBHandle),
				Multitenant: tenantManager,
			}).Handler())

			// triggers upload only when there are pending events and triggerUpload is sent for a sourceId
			srvMux.Post("/v1/warehouse/pending-events", pendingEventsHandler)
			// triggers uploads for a source
			srvMux.Post("/v1/warehouse/trigger-upload", triggerUploadHandler)

			// Warehouse Async Job end-points
			srvMux.Post("/v1/warehouse/jobs", asyncWh.AddWarehouseJobHandler)          // FIXME: add degraded mode
			srvMux.Get("/v1/warehouse/jobs/status", asyncWh.StatusWarehouseJobHandler) // FIXME: add degraded mode

			// fetch schema info
			// TODO: Remove this endpoint once sources change is released
			srvMux.Get("/v1/warehouse/fetch-tables", fetchTablesHandler)
			srvMux.Get("/internal/v1/warehouse/fetch-tables", fetchTablesHandler)

			pkgLogger.Infof("WH: Starting warehouse master service in %d", webPort)
		} else {
			pkgLogger.Infof("WH: Starting warehouse slave service in %d", webPort)
		}
	}

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", webPort),
		Handler:           bugsnag.Handler(srvMux),
		ReadHeaderTimeout: 3 * time.Second,
	}

	return kithttputil.ListenAndServe(ctx, srv)
}

// CheckForWarehouseEnvVars Checks if all the required Env Variables for Warehouse are present
func CheckForWarehouseEnvVars() bool {
	return config.IsSet("WAREHOUSE_JOBS_DB_HOST") &&
		config.IsSet("WAREHOUSE_JOBS_DB_USER") &&
		config.IsSet("WAREHOUSE_JOBS_DB_DB_NAME") &&
		config.IsSet("WAREHOUSE_JOBS_DB_PASSWORD")
}

// This checks if gateway is running or not
func isStandAlone() bool {
	return warehouseMode != EmbeddedMode && warehouseMode != EmbeddedMasterMode
}

func isMaster() bool {
	return warehouseMode == config.MasterMode ||
		warehouseMode == config.MasterSlaveMode ||
		warehouseMode == config.EmbeddedMode ||
		warehouseMode == config.EmbeddedMasterMode
}

func isSlave() bool {
	return warehouseMode == config.SlaveMode || warehouseMode == config.MasterSlaveMode || warehouseMode == config.EmbeddedMode
}

func isStandAloneSlave() bool {
	return warehouseMode == config.SlaveMode
}

func setupDB(ctx context.Context, connInfo string) error {
	var err error
	dbHandle, err = sql.Open("postgres", connInfo)
	if err != nil {
		return err
	}
	dbHandle.SetMaxOpenConns(config.GetInt("Warehouse.maxOpenConnections", 20))

	isDBCompatible, err := validators.IsPostgresCompatible(ctx, dbHandle)
	if err != nil {
		return err
	}

	if !isDBCompatible {
		err := errors.New("rudder Warehouse Service needs postgres version >= 10. Exiting")
		pkgLogger.Error(err)
		return err
	}

	if err = dbHandle.PingContext(ctx); err != nil {
		return fmt.Errorf("could not ping WH db: %w", err)
	}

	wrappedDBHandle = sqlquerywrapper.New(
		dbHandle,
		sqlquerywrapper.WithLogger(pkgLogger.Child("dbHandle")),
		sqlquerywrapper.WithQueryTimeout(dbHandleTimeout),
	)

	return setupTables(dbHandle)
}

// Setup prepares the database connection for warehouse service, verifies database compatibility and creates the required tables
func Setup(ctx context.Context) error {
	if !db.IsNormalMode() {
		return nil
	}
	psqlInfo := getConnectionString()
	if err := setupDB(ctx, psqlInfo); err != nil {
		return fmt.Errorf("cannot setup warehouse db: %w", err)
	}
	return nil
}

// Start starts the warehouse service
func Start(ctx context.Context, app app.App) error {
	application = app

	if dbHandle == nil && !isStandAloneSlave() {
		return errors.New("warehouse service cannot start, database connection is not setup")
	}
	// do not start warehouse service if rudder core is not in normal mode and warehouse is running in same process as rudder core
	if !isStandAlone() && !db.IsNormalMode() {
		pkgLogger.Infof("Skipping start of warehouse service...")
		return nil
	}

	pkgLogger.Infof("WH: Starting Warehouse service...")
	psqlInfo := getConnectionString()

	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Fatal(r)
			panic(r)
		}
	}()

	g, gCtx := errgroup.WithContext(ctx)

	tenantManager = &multitenant.Manager{
		BackendConfig: backendconfig.DefaultBackendConfig,
	}
	g.Go(func() error {
		tenantManager.Run(gCtx)
		return nil
	})

	bcManager = newBackendConfigManager(
		config.Default, wrappedDBHandle, tenantManager,
		pkgLogger.Child("wh_bc_manager"),
	)
	g.Go(func() error {
		bcManager.Start(gCtx)
		return nil
	})

	RegisterAdmin(bcManager, pkgLogger)

	runningMode := config.GetString("Warehouse.runningMode", "")
	if runningMode == DegradedMode {
		pkgLogger.Infof("WH: Running warehouse service in degraded mode...")
		if isMaster() {
			err := InitWarehouseAPI(dbHandle, bcManager, pkgLogger.Child("upload_api"))
			if err != nil {
				pkgLogger.Errorf("WH: Failed to start warehouse api: %v", err)
				return err
			}
		}
		return startWebHandler(ctx)
	}
	var err error
	workspaceIdentifier := fmt.Sprintf(`%s::%s`, config.GetKubeNamespace(), misc.GetMD5Hash(config.GetWorkspaceToken()))
	notifier, err = pgnotifier.New(workspaceIdentifier, psqlInfo)
	if err != nil {
		return fmt.Errorf("cannot setup pgnotifier: %w", err)
	}

	// Setting up reporting client only if standalone master or embedded connecting to different DB for warehouse
	// A different DB for warehouse is used when:
	// 1. MultiTenant (uses RDS)
	// 2. rudderstack-postgresql-warehouse pod in Hosted and Enterprise
	if (isStandAlone() && isMaster()) || (misc.GetConnectionString() != psqlInfo) {
		reporting := application.Features().Reporting.Setup(backendconfig.DefaultBackendConfig)

		g.Go(misc.WithBugsnagForWarehouse(func() error {
			reporting.AddClient(gCtx, types.Config{ConnInfo: psqlInfo, ClientName: types.WarehouseReportingClient})
			return nil
		}))
	}

	if isStandAlone() && isMaster() {
		// Report warehouse features
		g.Go(func() error {
			backendconfig.DefaultBackendConfig.WaitForConfig(gCtx)

			c := controlplane.NewClient(
				backendconfig.GetConfigBackendURL(),
				backendconfig.DefaultBackendConfig.Identity(),
			)

			err := c.SendFeatures(gCtx, info.WarehouseComponent.Name, info.WarehouseComponent.Features)
			if err != nil {
				pkgLogger.Errorf("error sending warehouse features: %v", err)
			}

			// We don't want to exit if we fail to send features
			return nil
		})
	}

	if isSlave() {
		pkgLogger.Infof("WH: Starting warehouse slave...")
		g.Go(misc.WithBugsnagForWarehouse(func() error {
			cm := newConstraintsManager(config.Default)
			ef := encoding.NewFactory(config.Default)

			slave := newSlave(config.Default, pkgLogger, stats.Default, &notifier, bcManager, cm, ef)
			return slave.setupSlave(gCtx)
		}))
	}

	if isMaster() {
		pkgLogger.Infof("[WH]: Starting warehouse master...")

		backendconfig.DefaultBackendConfig.WaitForConfig(ctx)

		region := config.GetString("region", "")

		controlPlaneClient = controlplane.NewClient(
			backendconfig.GetConfigBackendURL(),
			backendconfig.DefaultBackendConfig.Identity(),
			controlplane.WithRegion(region),
		)

		g.Go(misc.WithBugsnagForWarehouse(func() error {
			return notifier.ClearJobs(gCtx)
		}))

		g.Go(misc.WithBugsnagForWarehouse(func() error {
			return monitorDestRouters(gCtx)
		}))

		g.Go(misc.WithBugsnagForWarehouse(func() error {
			archive.CronArchiver(gCtx, archive.New(
				config.Default,
				pkgLogger,
				stats.Default,
				dbHandle,
				filemanager.New,
				tenantManager,
			))
			return nil
		}))

		err := InitWarehouseAPI(dbHandle, bcManager, pkgLogger.Child("upload_api"))
		if err != nil {
			pkgLogger.Errorf("WH: Failed to start warehouse api: %v", err)
			return err
		}
		asyncWh = jobs.InitWarehouseJobsAPI(gCtx, dbHandle, &notifier)
		jobs.WithConfig(asyncWh, config.Default)

		g.Go(misc.WithBugsnagForWarehouse(func() error {
			return asyncWh.InitAsyncJobRunner()
		}))
	}

	g.Go(func() error {
		return startWebHandler(gCtx)
	})

	return g.Wait()
}
