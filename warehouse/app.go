package warehouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/sqlutil"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/collectors"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/app"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/info"
	"github.com/rudderlabs/rudder-server/services/controlplane"
	"github.com/rudderlabs/rudder-server/services/notifier"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/services/validators"
	"github.com/rudderlabs/rudder-server/utils/crash"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	whadmin "github.com/rudderlabs/rudder-server/warehouse/admin"
	"github.com/rudderlabs/rudder-server/warehouse/api"
	"github.com/rudderlabs/rudder-server/warehouse/archive"
	"github.com/rudderlabs/rudder-server/warehouse/bcm"
	"github.com/rudderlabs/rudder-server/warehouse/constraints"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/mode"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
	"github.com/rudderlabs/rudder-server/warehouse/router"
	"github.com/rudderlabs/rudder-server/warehouse/slave"
	"github.com/rudderlabs/rudder-server/warehouse/source"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type App struct {
	app                app.App
	reporting          types.Reporting
	conf               *config.Config
	logger             logger.Logger
	statsFactory       stats.Stats
	bcConfig           backendconfig.BackendConfig
	db                 *sqlquerywrapper.DB
	notifier           *notifier.Notifier
	tenantManager      *multitenant.Manager
	controlPlaneClient *controlplane.Client
	bcManager          *bcm.BackendConfigManager
	api                *api.Api
	grpcServer         *api.GRPC
	constraintsManager *constraints.Manager
	encodingFactory    *encoding.Factory
	fileManagerFactory filemanager.Factory
	sourcesManager     *source.Manager
	admin              *whadmin.Admin
	triggerStore       *sync.Map
	createUploadAlways *atomic.Bool

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

func New(
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

	a.config.host = conf.GetString("WAREHOUSE_JOBS_DB_HOST", "localhost")
	a.config.user = conf.GetString("WAREHOUSE_JOBS_DB_USER", "ubuntu")
	a.config.password = conf.GetString("WAREHOUSE_JOBS_DB_PASSWORD", "ubuntu")
	a.config.database = conf.GetString("WAREHOUSE_JOBS_DB_DB_NAME", "ubuntu")
	a.config.sslMode = conf.GetString("WAREHOUSE_JOBS_DB_SSL_MODE", "disable")
	a.config.port = conf.GetInt("WAREHOUSE_JOBS_DB_PORT", 5432)
	a.config.mode = conf.GetString("Warehouse.mode", "embedded")
	a.config.runningMode = conf.GetString("Warehouse.runningMode", "")
	a.config.shouldForceSetLowerVersion = conf.GetBoolVar(true, "SQLMigrator.forceSetLowerVersion")
	a.config.maxOpenConnections = conf.GetInt("Warehouse.maxOpenConnections", 20)
	a.config.configBackendURL = conf.GetString("CONFIG_BACKEND_URL", "https://api.rudderstack.com")
	a.config.region = conf.GetString("region", "")
	a.config.dbQueryTimeout = a.conf.GetDurationVar(5, time.Minute, "Warehouse.dbHandleTimeout", "Warehouse.dbHandleTimeoutInMin")

	a.appName = misc.DefaultString("rudder-server").OnError(os.Hostname())

	return a
}

func (a *App) Setup(ctx context.Context) error {
	if err := a.setupDatabase(ctx); err != nil {
		return fmt.Errorf("setting up database: %w", err)
	}

	a.createUploadAlways = &atomic.Bool{}
	a.triggerStore = &sync.Map{}
	a.tenantManager = multitenant.New(
		a.conf,
		a.bcConfig,
	)
	a.controlPlaneClient = controlplane.NewClient(
		a.config.configBackendURL,
		a.bcConfig.Identity(),
		controlplane.WithRegion(a.config.region),
	)
	a.bcManager = bcm.New(
		a.conf,
		a.db,
		a.tenantManager,
		a.logger.Child("wh_bc_manager"),
		a.statsFactory,
	)
	a.constraintsManager = constraints.New(
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
	err := a.notifier.Setup(ctx, a.connectionString("notifier"))
	if err != nil {
		return fmt.Errorf("cannot setup notifier: %w", err)
	}

	a.sourcesManager = source.New(
		a.conf,
		a.logger,
		a.db,
		a.notifier,
	)

	a.grpcServer, err = api.NewGRPCServer(
		a.conf,
		a.logger,
		a.statsFactory,
		a.db,
		a.tenantManager,
		a.bcManager,
		a.triggerStore,
	)
	if err != nil {
		return fmt.Errorf("cannot create grpc server: %w", err)
	}

	a.api = api.NewApi(
		a.config.mode,
		a.conf,
		a.logger,
		a.statsFactory,
		a.bcConfig,
		a.db,
		a.notifier,
		a.tenantManager,
		a.bcManager,
		a.sourcesManager,
		a.triggerStore,
	)
	a.admin = whadmin.New(
		a.bcManager,
		a.createUploadAlways,
		a.logger,
	)

	return nil
}

func (a *App) setupDatabase(ctx context.Context) error {
	dsn := a.connectionString("warehouse")

	database, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("could not open: %w", err)
	}
	database.SetMaxOpenConns(a.config.maxOpenConnections)

	isCompatible, err := validators.IsPostgresCompatible(ctx, database)
	if err != nil {
		return fmt.Errorf("could not check compatibility: %w", err)
	}
	if !isCompatible {
		return errors.New("warehouse Service needs postgres version >= 10. Exiting")
	}

	if err := database.PingContext(ctx); err != nil {
		return fmt.Errorf("could not ping: %w", err)
	}

	err = a.statsFactory.RegisterCollector(collectors.NewDatabaseSQLStats("warehouse", database))
	if err != nil {
		return fmt.Errorf("could not register collector: %w", err)
	}
	a.db = sqlquerywrapper.New(
		database,
		sqlquerywrapper.WithLogger(a.logger.Child("db")),
		sqlquerywrapper.WithQueryTimeout(a.config.dbQueryTimeout),
		sqlquerywrapper.WithStats(a.statsFactory),
	)

	err = a.migrate()
	if err != nil {
		return fmt.Errorf("could not migrate: %w", err)
	}

	return nil
}

func (a *App) connectionString(componentName string) string {
	if !a.checkForWarehouseEnvVars() {
		return misc.GetConnectionString(a.conf, componentName)
	}

	appName := fmt.Sprintf("%s-%s", componentName, a.appName)
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s application_name=%s",
		a.config.host,
		a.config.port,
		a.config.user,
		a.config.password,
		a.config.database,
		a.config.sslMode,
		appName,
	)
}

// checkForWarehouseEnvVars checks if the required database environment variables are set
func (a *App) checkForWarehouseEnvVars() bool {
	return a.conf.IsSet("WAREHOUSE_JOBS_DB_HOST") &&
		a.conf.IsSet("WAREHOUSE_JOBS_DB_USER") &&
		a.conf.IsSet("WAREHOUSE_JOBS_DB_DB_NAME") &&
		a.conf.IsSet("WAREHOUSE_JOBS_DB_PASSWORD")
}

func (a *App) migrate() error {
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

// Run runs the warehouse service
func (a *App) Run(ctx context.Context) error {
	a.logger.Info("Starting Warehouse service...")

	defer func() {
		if r := recover(); r != nil {
			a.logger.Fatal(r)
			panic(r)
		}
	}()

	admin.RegisterAdminHandler("Warehouse", a.admin)

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		a.tenantManager.Run(gCtx)
		return nil
	})
	g.Go(func() error {
		a.bcManager.Start(gCtx)
		return nil
	})
	g.Go(func() error {
		sqlutil.MonitorDatabase(
			gCtx,
			a.conf,
			a.statsFactory,
			a.db.DB,
			"warehouse",
		)
		return nil
	})

	if mode.IsDegraded(a.config.runningMode) {
		a.logger.Info("Running warehouse service in degraded mode...")

		if mode.IsMaster(a.config.mode) {
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

	if !mode.IsStandAloneSlave(a.config.mode) {
		a.reporting = a.app.Features().Reporting.Setup(gCtx, a.conf, a.bcConfig)
		defer a.reporting.Stop()
		syncer := a.reporting.DatabaseSyncer(types.SyncerConfig{ConnInfo: a.connectionString("reporting"), Label: types.WarehouseReportingLabel})
		g.Go(crash.NotifyWarehouse(func() error {
			syncer()
			return nil
		}))
	}
	if mode.IsStandAlone(a.config.mode) && mode.IsMaster(a.config.mode) {
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
	if mode.IsSlave(a.config.mode) {
		a.logger.Info("Starting warehouse slave...")

		g.Go(crash.NotifyWarehouse(func() error {
			s := slave.New(
				a.conf,
				a.logger,
				a.statsFactory,
				a.notifier,
				a.bcManager,
				a.constraintsManager,
				a.encodingFactory,
			)
			return s.SetupSlave(gCtx)
		}))
	}
	if mode.IsMaster(a.config.mode) {
		a.logger.Info("[WH]: Starting warehouse master...")

		a.bcConfig.WaitForConfig(ctx)

		g.Go(crash.NotifyWarehouse(func() error {
			return a.notifier.ClearJobs(gCtx)
		}))
		g.Go(crash.NotifyWarehouse(func() error {
			return a.monitorDestRouters(gCtx)
		}))
		g.Go(crash.NotifyWarehouse(func() error {
			archive.CronArchiver(gCtx, archive.New(
				a.conf,
				a.logger,
				a.statsFactory,
				a.db,
				a.fileManagerFactory,
				a.tenantManager,
			))
			return nil
		}))
		g.Go(func() error {
			a.grpcServer.Start(gCtx)
			return nil
		})
		g.Go(crash.NotifyWarehouse(func() error {
			return a.sourcesManager.Run(gCtx)
		}))
	}

	g.Go(func() error {
		return a.api.Start(gCtx)
	})
	g.Go(func() error {
		a.notifier.Monitor(gCtx)
		return nil
	})
	g.Go(func() error {
		<-gCtx.Done()
		return a.notifier.Shutdown()
	})

	return g.Wait()
}

// Gets the config from config backend and extracts enabled write keys
func (a *App) monitorDestRouters(ctx context.Context) error {
	dstToWhRouter := make(map[string]*router.Router)
	g, ctx := errgroup.WithContext(ctx)
	ch := a.tenantManager.WatchConfig(ctx)
	for configData := range ch {
		diffRouters := a.onConfigDataEvent(configData, dstToWhRouter)
		for _, r := range diffRouters {
			g.Go(func() error { return r.Start(ctx) })
		}
	}
	return g.Wait()
}

func (a *App) onConfigDataEvent(
	configMap map[string]backendconfig.ConfigT,
	dstToWhRouter map[string]*router.Router,
) map[string]*router.Router {
	enabledDestinations := make(map[string]bool)
	diffRouters := make(map[string]*router.Router)
	for _, wConfig := range configMap {
		for _, sConfig := range wConfig.Sources {
			for _, destination := range sConfig.Destinations {
				enabledDestinations[destination.DestinationDefinition.Name] = true

				if !slices.Contains(warehouseutils.WarehouseDestinations, destination.DestinationDefinition.Name) {
					continue
				}

				_, ok := dstToWhRouter[destination.DestinationDefinition.Name]
				if ok {
					a.logger.Debug("Enabling existing Destination: ", destination.DestinationDefinition.Name)
					continue
				}

				a.logger.Info("Starting a new Warehouse Destination Router: ", destination.DestinationDefinition.Name)

				r := router.New(
					a.reporting,
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
					a.createUploadAlways,
				)
				dstToWhRouter[destination.DestinationDefinition.Name] = r
				diffRouters[destination.DestinationDefinition.Name] = r
			}
		}
	}
	return diffRouters
}
