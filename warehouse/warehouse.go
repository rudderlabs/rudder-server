package warehouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/trigger"

	"github.com/rudderlabs/rudder-server/services/notifier"

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
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/services/validators"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/warehouse/archive"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/jobs"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type App struct {
	app                app.App
	conf               *config.Config
	logger             logger.Logger
	statsFactory       stats.Stats
	bcConfig           backendconfig.BackendConfig
	db                 *sqlmw.DB
	notifier           *notifier.Notifier
	tenantManager      *multitenant.Manager
	controlPlaneClient *controlplane.Client
	bcManager          *backendConfigManager
	api                *Api
	grpcServer         *GRPC
	constraintsManager *constraintsManager
	encodingFactory    *encoding.Factory
	fileManagerFactory filemanager.Factory
	jobsManager        *jobs.AsyncJobWh
	triggerStore       *trigger.Store

	appName string

	config struct {
		host     string
		user     string
		password string
		database string
		sslMode  string
		port     int

		mode                       string
		runningMode                string
		shouldForceSetLowerVersion bool
		dbQueryTimeout             time.Duration
		maxOpenConnections         int

		configBackendURL string
		region           string
	}
}

type (
	WorkerIdentifierT string
	JobID             int64
)

func NewApp(
	app app.App,
	conf *config.Config,
	log logger.Logger,
	statsFactory stats.Stats,
	bcConfig backendconfig.BackendConfig,
	fileManagerFactory filemanager.Factory,
) *App {
	a := &App{
		app:                app,
		conf:               conf,
		logger:             log.Child("warehouse"),
		statsFactory:       statsFactory,
		bcConfig:           bcConfig,
		fileManagerFactory: fileManagerFactory,
	}

	if a.conf.IsSet("Warehouse.dbHandleTimeout") {
		a.config.dbQueryTimeout = a.conf.GetDuration("Warehouse.dbHandleTimeout", 5, time.Minute)
	} else {
		a.config.dbQueryTimeout = a.conf.GetDuration("Warehouse.dbHandleTimeoutInMin", 5, time.Minute)
	}

	a.config.host = conf.GetString("WAREHOUSE_JOBS_DB_HOST", "localhost")
	a.config.user = conf.GetString("WAREHOUSE_JOBS_DB_USER", "ubuntu")
	a.config.password = conf.GetString("WAREHOUSE_JOBS_DB_PASSWORD", "ubuntu")
	a.config.database = conf.GetString("WAREHOUSE_JOBS_DB_DB_NAME", "ubuntu")
	a.config.sslMode = conf.GetString("WAREHOUSE_JOBS_DB_SSL_MODE", "disable")
	a.config.port = conf.GetInt("WAREHOUSE_JOBS_DB_PORT", 5432)
	a.config.mode = conf.GetString("Warehouse.mode", "embedded")
	a.config.runningMode = conf.GetString("Warehouse.runningMode", "")
	a.config.shouldForceSetLowerVersion = conf.GetBool("SQLMigrator.forceSetLowerVersion", true)
	a.config.maxOpenConnections = conf.GetInt("Warehouse.maxOpenConnections", 20)
	a.config.configBackendURL = conf.GetString("CONFIG_BACKEND_URL", "https://api.rudderstack.com")
	a.config.region = conf.GetString("region", "")

	a.appName = misc.DefaultString("rudder-server").OnError(os.Hostname())

	return a
}

func (a *App) Setup(ctx context.Context) error {
	if err := a.setupDatabase(ctx); err != nil {
		return fmt.Errorf("setting up database: %w", err)
	}

	a.triggerStore = trigger.NewStore()

	a.tenantManager = multitenant.New(
		a.conf,
		a.bcConfig,
	)
	a.controlPlaneClient = controlplane.NewClient(
		a.config.configBackendURL,
		a.bcConfig.Identity(),
		controlplane.WithRegion(a.config.region),
	)
	a.bcManager = newBackendConfigManager(
		a.conf,
		a.db,
		a.tenantManager,
		a.logger.Child("wh_bc_manager"),
	)
	a.constraintsManager = newConstraintsManager(
		a.conf,
	)
	a.encodingFactory = encoding.NewFactory(
		a.conf,
	)

	workspaceIdentifier := fmt.Sprintf(`%s::%s`,
		config.GetKubeNamespace(),
		misc.GetMD5Hash(config.GetWorkspaceToken()),
	)
	a.notifier = notifier.New(
		a.conf,
		a.logger,
		a.statsFactory,
		workspaceIdentifier,
	)
	if err := a.notifier.Setup(ctx, a.connectionString()); err != nil {
		return fmt.Errorf("cannot setup notifier: %w", err)
	}

	a.jobsManager = jobs.InitWarehouseJobsAPI(
		ctx,
		a.db.DB,
		a.notifier,
	)
	jobs.WithConfig(a.jobsManager, a.conf)

	var err error
	a.grpcServer, err = NewGRPCServer(
		a.conf,
		a.logger,
		a.db,
		a.tenantManager,
		a.bcManager,
		a.triggerStore,
	)
	if err != nil {
		return fmt.Errorf("cannot create grpc server: %w", err)
	}

	a.api = NewApi(
		a.config.mode,
		a.conf,
		a.logger,
		a.statsFactory,
		a.bcConfig,
		a.db,
		a.notifier,
		a.tenantManager,
		a.bcManager,
		a.jobsManager,
		a.triggerStore,
	)

	return nil
}

func (a *App) setupDatabase(ctx context.Context) error {
	dsn := a.connectionString()

	database, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("could not open: %w", err)
	}
	database.SetMaxOpenConns(a.config.maxOpenConnections)

	isCompatible, err := validators.IsPostgresCompatible(ctx, database)
	if err != nil {
		return fmt.Errorf("could not check compatibility: %w", err)
	} else if !isCompatible {
		return errors.New("warehouse Service needs postgres version >= 10. Exiting")
	}

	if err := database.PingContext(ctx); err != nil {
		return fmt.Errorf("could not ping: %w", err)
	}

	a.db = sqlmw.New(
		database,
		sqlmw.WithLogger(a.logger.Child("db")),
		sqlmw.WithQueryTimeout(a.config.dbQueryTimeout),
		sqlmw.WithStats(a.statsFactory),
	)

	err = a.setupTables()
	if err != nil {
		return fmt.Errorf("could not setup tables: %w", err)
	}

	return nil
}

func (a *App) connectionString() string {
	if !a.checkForWarehouseEnvVars() {
		return misc.GetConnectionString(a.conf)
	}

	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s application_name=%s",
		a.config.host,
		a.config.port,
		a.config.user,
		a.config.password,
		a.config.database,
		a.config.sslMode,
		a.appName,
	)
}

// checkForWarehouseEnvVars checks if the required database environment variables are set
func (a *App) checkForWarehouseEnvVars() bool {
	return a.conf.IsSet("WAREHOUSE_JOBS_DB_HOST") &&
		a.conf.IsSet("WAREHOUSE_JOBS_DB_USER") &&
		a.conf.IsSet("WAREHOUSE_JOBS_DB_DB_NAME") &&
		a.conf.IsSet("WAREHOUSE_JOBS_DB_PASSWORD")
}

func (a *App) setupTables() error {
	m := &migrator.Migrator{
		Handle:                     a.db.DB,
		MigrationsTable:            "wh_schema_migrations",
		ShouldForceSetLowerVersion: a.config.shouldForceSetLowerVersion,
	}

	operation := func() error {
		return m.Migrate("warehouse")
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)

	err := backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		a.logger.Warnf("retrying warehouse database migration in %s: %v", t, err)
	})
	if err != nil {
		return fmt.Errorf("could not migrate: %w", err)
	}

	return nil
}

// Start starts the warehouse service
func (a *App) Start(ctx context.Context) error {
	// do not start warehouse service if rudder core is not in normal mode and warehouse is running in same process as rudder core
	if !isStandAlone(a.config.mode) && !db.IsNormalMode() {
		a.logger.Infof("Skipping start of warehouse service...")
		return nil
	}

	a.logger.Infof("Starting Warehouse service...")

	defer func() {
		if r := recover(); r != nil {
			a.logger.Fatal(r)
			panic(r)
		}
	}()

	RegisterAdmin(a.bcManager, a.logger)

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		a.tenantManager.Run(gCtx)
		return nil
	})
	g.Go(func() error {
		a.bcManager.Start(gCtx)
		return nil
	})

	if isDegraded(a.config.runningMode) {
		a.logger.Infof("Running warehouse service in degraded mode...")

		if isMaster(a.config.mode) {
			g.Go(func() error {
				a.grpcServer.Start(gCtx)
				return nil
			})
		}
		g.Go(func() error {
			return a.api.Start(gCtx)
		})

		return g.Wait()
	}

	// Setting up reporting client only if standalone master or embedded connecting to different DB for warehouse
	// A different DB for warehouse is used when:
	// 1. MultiTenant (uses RDS)
	// 2. rudderstack-postgresql-warehouse pod in Hosted and Enterprise
	if (isStandAlone(a.config.mode) && isMaster(a.config.mode)) || (misc.GetConnectionString(a.conf) != a.connectionString()) {
		g.Go(misc.WithBugsnagForWarehouse(func() error {
			reportingClient := a.app.Features().Reporting.Setup(a.bcConfig)
			reportingClient.AddClient(gCtx, types.Config{
				ConnInfo:   a.connectionString(),
				ClientName: types.WarehouseReportingClient,
			})
			return nil
		}))
	}
	if isStandAlone(a.config.mode) && isMaster(a.config.mode) {
		// Report warehouse features
		g.Go(func() error {
			a.bcConfig.WaitForConfig(gCtx)

			err := a.controlPlaneClient.SendFeatures(
				gCtx,
				info.WarehouseComponent.Name,
				info.WarehouseComponent.Features,
			)
			if err != nil {
				a.logger.Errorf("sending warehouse features: %v", err)
			}

			// We don't want to exit if we fail to send features
			return nil
		})
	}
	if isSlave(a.config.mode) {
		a.logger.Infof("Starting warehouse slave...")

		g.Go(misc.WithBugsnagForWarehouse(func() error {
			slave := newSlave(
				a.conf,
				a.logger,
				a.statsFactory,
				a.notifier,
				a.bcManager,
				a.constraintsManager,
				a.encodingFactory,
			)
			return slave.setupSlave(gCtx)
		}))
	}
	if isMaster(a.config.mode) {
		a.logger.Infof("[WH]: Starting warehouse master...")

		a.bcConfig.WaitForConfig(ctx)

		g.Go(misc.WithBugsnagForWarehouse(func() error {
			if err := a.notifier.ClearJobs(gCtx); err != nil {
				return fmt.Errorf("unable to clear notifier jobs: %w", err)
			}

			return a.notifier.RunMaintenance(gCtx)
		}))

		g.Go(misc.WithBugsnagForWarehouse(func() error {
			return a.monitorDestRouters(gCtx)
		}))
		g.Go(misc.WithBugsnagForWarehouse(func() error {
			archive.CronArchiver(gCtx, archive.New(
				a.conf,
				a.logger,
				a.statsFactory,
				a.db.DB,
				a.fileManagerFactory,
				a.tenantManager,
			))
			return nil
		}))
		g.Go(func() error {
			a.grpcServer.Start(gCtx)
			return nil
		})
		g.Go(misc.WithBugsnagForWarehouse(func() error {
			return a.jobsManager.InitAsyncJobRunner()
		}))
	}

	g.Go(func() error {
		return a.api.Start(gCtx)
	})
	g.Go(func() error {
		<-gCtx.Done()
		return a.notifier.Shutdown()
	})

	return g.Wait()
}

// Gets the config from config backend and extracts enabled write keys
func (a *App) monitorDestRouters(ctx context.Context) error {
	dstToWhRouter := make(map[string]*router)

	ch := a.tenantManager.WatchConfig(ctx)
	for configData := range ch {
		err := a.onConfigDataEvent(ctx, configData, dstToWhRouter)
		if err != nil {
			return fmt.Errorf("config data event error: %v", err)
		}
	}

	g, _ := errgroup.WithContext(context.Background())
	for _, router := range dstToWhRouter {
		router := router
		g.Go(router.Shutdown)
	}
	return g.Wait()
}

func (a *App) onConfigDataEvent(ctx context.Context, configMap map[string]backendconfig.ConfigT, dstToWhRouter map[string]*router) error {
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
					a.logger.Debug("Enabling existing Destination: ", destination.DestinationDefinition.Name)
					router.Enable()
					continue
				}

				a.logger.Info("Starting a new Warehouse Destination Router: ", destination.DestinationDefinition.Name)

				router, err := newRouter(
					ctx,
					a.app,
					destination.DestinationDefinition.Name,
					a.conf,
					a.logger.Child("router"),
					a.statsFactory,
					a.db,
					a.notifier,
					a.tenantManager,
					a.controlPlaneClient,
					a.bcManager,
					a.encodingFactory,
					a.triggerStore,
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
				a.logger.Info("Disabling a existing warehouse destination: ", key)

				wh.Disable()
			}
		}
	}

	return nil
}
