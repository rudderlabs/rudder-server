package warehouse

import (
	"context"
	"database/sql"
	"errors"
	"expvar"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/encoding"

	"github.com/cenkalti/backoff/v4"
	"github.com/samber/lo"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
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
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/jobs"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	application                app.App
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
	bcManager                  *backendConfigManager
	triggerUploadsMap          map[string]bool // `whType:sourceID:destinationID` -> boolean value representing if an upload was triggered or not
	triggerUploadsMapLock      sync.RWMutex
	pkgLogger                  logger.Logger
	ShouldForceSetLowerVersion bool
	asyncWh                    *jobs.AsyncJobWh
)

var (
	host, user, password, dbname, sslMode, appName string
	port                                           int
)

var defaultUploadPriority = 100

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
	config.RegisterInt64ConfigVariable(1800, &uploadFreqInS, true, 1, "Warehouse.uploadFreqInS")
	lastProcessedMarkerMap = map[string]int64{}
	host = config.GetString("WAREHOUSE_JOBS_DB_HOST", "localhost")
	user = config.GetString("WAREHOUSE_JOBS_DB_USER", "ubuntu")
	dbname = config.GetString("WAREHOUSE_JOBS_DB_DB_NAME", "ubuntu")
	port = config.GetInt("WAREHOUSE_JOBS_DB_PORT", 5432)
	password = config.GetString("WAREHOUSE_JOBS_DB_PASSWORD", "ubuntu") // Reading secrets from
	sslMode = config.GetString("WAREHOUSE_JOBS_DB_SSL_MODE", "disable")
	triggerUploadsMap = map[string]bool{}
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

func getConnectionString() string {
	if !CheckForWarehouseEnvVars() {
		return misc.GetConnectionString(config.Default)
	}
	return fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=%s application_name=%s",
		host, port, user, password, dbname, sslMode, appName)
}

// CheckForWarehouseEnvVars Checks if all the required Env Variables for Warehouse are present
func CheckForWarehouseEnvVars() bool {
	return config.IsSet("WAREHOUSE_JOBS_DB_HOST") &&
		config.IsSet("WAREHOUSE_JOBS_DB_USER") &&
		config.IsSet("WAREHOUSE_JOBS_DB_DB_NAME") &&
		config.IsSet("WAREHOUSE_JOBS_DB_PASSWORD")
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

	mode := config.GetString("Warehouse.mode", config.EmbeddedMode)

	if dbHandle == nil && !isStandAloneSlave(mode) {
		return errors.New("warehouse service cannot start, database connection is not setup")
	}
	// do not start warehouse service if rudder core is not in normal mode and warehouse is running in same process as rudder core
	if !isStandAlone(mode) && !db.IsNormalMode() {
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

	tenantManager = multitenant.New(config.Default, backendconfig.DefaultBackendConfig)

	g.Go(func() error {
		tenantManager.Run(gCtx)
		return nil
	})

	grpc, err := NewGRPC(config.Default, pkgLogger, stats.Default, wrappedDBHandle, bcManager)
	if err != nil {
		return fmt.Errorf("grpc server failed to start: %v", err)
	}

	bcManager = newBackendConfigManager(
		config.Default, wrappedDBHandle, tenantManager,
		pkgLogger.Child("wh_bc_manager"), grpc,
	)
	g.Go(func() error {
		bcManager.Start(gCtx)
		return nil
	})

	RegisterAdmin(bcManager, pkgLogger)

	runningMode := config.GetString("Warehouse.runningMode", "")
	if runningMode == DegradedMode {
		pkgLogger.Infof("WH: Running warehouse service in degraded mode...")
		if isMaster(mode) {
			err := InitWarehouseAPI(dbHandle, bcManager, pkgLogger.Child("upload_api"))
			if err != nil {
				pkgLogger.Errorf("WH: Failed to start warehouse api: %v", err)
				return err
			}
		}

		api := NewApi(
			mode, config.Default, pkgLogger, stats.Default,
			backendconfig.DefaultBackendConfig, wrappedDBHandle, nil, tenantManager,
			bcManager, nil,
		)
		return api.Start(ctx)
	}
	workspaceIdentifier := fmt.Sprintf(`%s::%s`, config.GetKubeNamespace(), misc.GetMD5Hash(config.GetWorkspaceToken()))
	notifier, err = pgnotifier.New(workspaceIdentifier, psqlInfo)
	if err != nil {
		return fmt.Errorf("cannot setup pgnotifier: %w", err)
	}

	// Setting up reporting client only if standalone master or embedded connecting to different DB for warehouse
	// A different DB for warehouse is used when:
	// 1. MultiTenant (uses RDS)
	// 2. rudderstack-postgresql-warehouse pod in Hosted and Enterprise
	if (isStandAlone(mode) && isMaster(mode)) || (misc.GetConnectionString(config.Default) != psqlInfo) {
		reporting := application.Features().Reporting.Setup(backendconfig.DefaultBackendConfig)

		g.Go(misc.WithBugsnagForWarehouse(func() error {
			reporting.AddClient(gCtx, types.Config{ConnInfo: psqlInfo, ClientName: types.WarehouseReportingClient})
			return nil
		}))
	}

	if isStandAlone(mode) && isMaster(mode) {
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

	if isSlave(mode) {
		pkgLogger.Infof("WH: Starting warehouse slave...")
		g.Go(misc.WithBugsnagForWarehouse(func() error {
			cm := newConstraintsManager(config.Default)
			ef := encoding.NewFactory(config.Default)

			slave := newSlave(config.Default, pkgLogger, stats.Default, &notifier, bcManager, cm, ef)
			return slave.setupSlave(gCtx)
		}))
	}

	if isMaster(mode) {
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
		api := NewApi(
			mode, config.Default, pkgLogger, stats.Default,
			backendconfig.DefaultBackendConfig, wrappedDBHandle, &notifier, tenantManager,
			bcManager, asyncWh,
		)
		return api.Start(gCtx)
	})

	return g.Wait()
}
