package apphandlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/rudderlabs/rudder-server/utils/types/deployment"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/app/cluster"
	"github.com/rudderlabs/rudder-server/app/cluster/state"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/gateway"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/jobsdb/prebackup"
	"github.com/rudderlabs/rudder-server/processor"
	ratelimiter "github.com/rudderlabs/rudder-server/rate-limiter"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	routerManager "github.com/rudderlabs/rudder-server/router/manager"
	"github.com/rudderlabs/rudder-server/services/db"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/debugger/source"
	transformationdebugger "github.com/rudderlabs/rudder-server/services/debugger/transformation"
	"github.com/rudderlabs/rudder-server/services/multitenant"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/utils/types/servermode"
)

// EmbeddedApp is the type for embedded type implementation
type EmbeddedApp struct {
	App            app.Interface
	VersionHandler func(w http.ResponseWriter, r *http.Request)
}

func (*EmbeddedApp) GetAppType() string {
	return fmt.Sprintf("rudder-server-%s", app.EMBEDDED)
}

func (embedded *EmbeddedApp) StartRudderCore(ctx context.Context, options *app.Options) error {
	pkgLogger.Info("Embedded mode: Starting Rudder Core")

	rudderCoreDBValidator()
	rudderCoreWorkSpaceTableSetup()
	rudderCoreNodeSetup()
	rudderCoreBaseSetup()

	g, ctx := errgroup.WithContext(ctx)

	deploymentType, err := deployment.GetFromEnv()
	if err != nil {
		return fmt.Errorf("failed to get deployment type: %w", err)
	}
	pkgLogger.Infof("Configured deployment type: %q", deploymentType)

	reporting := embedded.App.Features().Reporting.Setup(backendconfig.DefaultBackendConfig)

	g.Go(func() error {
		reporting.AddClient(ctx, types.Config{ConnInfo: jobsdb.GetConnectionString()})
		return nil
	})

	pkgLogger.Info("Clearing DB ", options.ClearDB)

	transformationdebugger.Setup()
	destinationdebugger.Setup(backendconfig.DefaultBackendConfig)
	sourcedebugger.Setup(backendconfig.DefaultBackendConfig)

	migrationMode := embedded.App.Options().MigrationMode
	reportingI := embedded.App.Features().Reporting.GetReportingInstance()
	transientSources := transientsource.NewService(ctx, backendconfig.DefaultBackendConfig)
	prebackupHandlers := []prebackup.Handler{
		prebackup.DropSourceIds(transientSources.SourceIdsSupplier()),
	}
	rsourcesService, err := NewRsourcesService(deploymentType)
	if err != nil {
		return err
	}

	// IMP NOTE: All the jobsdb setups must happen before migrator setup.
	// This gwDBForProcessor should only be used by processor as this is supposed to be stopped and started with the
	// Processor.
	gwDBForProcessor := jobsdb.NewForRead(
		"gw",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithMigrationMode(migrationMode),
		jobsdb.WithStatusHandler(),
		jobsdb.WithPreBackupHandlers(prebackupHandlers),
		jobsdb.WithDSLimit(&gatewayDSLimit),
	)
	defer gwDBForProcessor.Close()
	routerDB := jobsdb.NewForReadWrite(
		"rt",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithMigrationMode(migrationMode),
		jobsdb.WithStatusHandler(),
		jobsdb.WithPreBackupHandlers(prebackupHandlers),
		jobsdb.WithDSLimit(&routerDSLimit),
	)
	defer routerDB.Close()
	batchRouterDB := jobsdb.NewForReadWrite(
		"batch_rt",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithMigrationMode(migrationMode),
		jobsdb.WithStatusHandler(),
		jobsdb.WithPreBackupHandlers(prebackupHandlers),
		jobsdb.WithDSLimit(&batchRouterDSLimit),
	)
	defer batchRouterDB.Close()
	errDB := jobsdb.NewForReadWrite(
		"proc_error",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithMigrationMode(migrationMode),
		jobsdb.WithStatusHandler(),
		jobsdb.WithPreBackupHandlers(prebackupHandlers),
		jobsdb.WithDSLimit(&processorDSLimit),
	)

	var tenantRouterDB jobsdb.MultiTenantJobsDB
	var multitenantStats multitenant.MultiTenantI
	if misc.UseFairPickup() {
		tenantRouterDB = &jobsdb.MultiTenantHandleT{HandleT: routerDB}
		multitenantStats = multitenant.NewStats(map[string]jobsdb.MultiTenantJobsDB{
			"rt":       tenantRouterDB,
			"batch_rt": &jobsdb.MultiTenantLegacy{HandleT: batchRouterDB},
		})
	} else {
		tenantRouterDB = &jobsdb.MultiTenantLegacy{HandleT: routerDB}
		multitenantStats = multitenant.WithLegacyPickupJobs(multitenant.NewStats(map[string]jobsdb.MultiTenantJobsDB{
			"rt":       tenantRouterDB,
			"batch_rt": &jobsdb.MultiTenantLegacy{HandleT: batchRouterDB},
		}))
	}

	enableGateway := true
	if embedded.App.Features().Migrator != nil {
		if migrationMode == db.IMPORT || migrationMode == db.EXPORT || migrationMode == db.IMPORT_EXPORT {
			startProcessorFunc := func() {
				clearDB := false
				if enableProcessor {
					g.Go(misc.WithBugsnag(func() error {
						return StartProcessor(
							ctx, &clearDB, gwDBForProcessor, routerDB, batchRouterDB, errDB,
							reportingI, multitenant.NOOP, transientSources, rsourcesService,
						)
					}))
				}
			}
			startRouterFunc := func() {
				if enableRouter {
					g.Go(misc.WithBugsnag(func() error {
						StartRouter(ctx, tenantRouterDB, batchRouterDB, errDB, reportingI, multitenant.NOOP, transientSources, rsourcesService)
						return nil
					}))
				}
			}
			enableRouter = false
			enableProcessor = false
			enableGateway = migrationMode != db.EXPORT

			embedded.App.Features().Migrator.PrepareJobsdbsForImport(gwDBForProcessor, routerDB, batchRouterDB)

			g.Go(func() error {
				embedded.App.Features().Migrator.Run(ctx, gwDBForProcessor, routerDB, batchRouterDB, startProcessorFunc,
					startRouterFunc) // TODO
				return nil
			})
		}
	}

	var modeProvider cluster.ChangeEventProvider

	switch deploymentType {
	case deployment.MultiTenantType:
		pkgLogger.Info("using ETCD Based Dynamic Cluster Manager")
		modeProvider = state.NewETCDDynamicProvider()
	case deployment.DedicatedType:
		// FIXME: hacky way to determine server mode
		pkgLogger.Info("using Static Cluster Manager")
		if enableProcessor && enableRouter {
			modeProvider = state.NewStaticProvider(servermode.NormalMode)
		} else {
			modeProvider = state.NewStaticProvider(servermode.DegradedMode)
		}
	default:
		return fmt.Errorf("unsupported deployment type: %q", deploymentType)
	}

	proc := processor.New(ctx, &options.ClearDB, gwDBForProcessor, routerDB, batchRouterDB, errDB, multitenantStats, reportingI, transientSources, rsourcesService)
	rtFactory := &router.Factory{
		Reporting:        reportingI,
		Multitenant:      multitenantStats,
		BackendConfig:    backendconfig.DefaultBackendConfig,
		RouterDB:         tenantRouterDB,
		ProcErrorDB:      errDB,
		TransientSources: transientSources,
		RsourcesService:  rsourcesService,
	}
	brtFactory := &batchrouter.Factory{
		Reporting:        reportingI,
		Multitenant:      multitenantStats,
		BackendConfig:    backendconfig.DefaultBackendConfig,
		RouterDB:         batchRouterDB,
		ProcErrorDB:      errDB,
		TransientSources: transientSources,
		RsourcesService:  rsourcesService,
	}
	rt := routerManager.New(rtFactory, brtFactory, backendconfig.DefaultBackendConfig)

	dm := cluster.Dynamic{
		Provider:        modeProvider,
		GatewayDB:       gwDBForProcessor,
		RouterDB:        routerDB,
		BatchRouterDB:   batchRouterDB,
		ErrorDB:         errDB,
		Processor:       proc,
		Router:          rt,
		MultiTenantStat: multitenantStats,
	}

	if enableGateway {
		rateLimiter := ratelimiter.HandleT{}
		rateLimiter.SetUp()
		gw := gateway.HandleT{}
		// This separate gateway db is created just to be used with gateway because in case of degraded mode,
		// the earlier created gwDb (which was created to be used mainly with processor) will not be running, and it
		// will cause issues for gateway because gateway is supposed to receive jobs even in degraded mode.
		gatewayDB = jobsdb.NewForWrite(
			"gw",
			jobsdb.WithClearDB(options.ClearDB),
			jobsdb.WithMigrationMode(migrationMode),
			jobsdb.WithStatusHandler(),
		)
		defer gwDBForProcessor.Close()
		if err = gatewayDB.Start(); err != nil {
			return fmt.Errorf("could not start gateway: %w", err)
		}
		defer gatewayDB.Stop()

		gw.SetReadonlyDBs(&readonlyGatewayDB, &readonlyRouterDB, &readonlyBatchRouterDB)
		err = gw.Setup(
			embedded.App, backendconfig.DefaultBackendConfig, gatewayDB,
			&rateLimiter, embedded.VersionHandler, rsourcesService,
		)
		if err != nil {
			return fmt.Errorf("could not setup gateway: %w", err)
		}
		defer func() {
			if err := gw.Shutdown(); err != nil {
				pkgLogger.Warnf("Gateway shutdown error: %v", err)
			}
		}()

		g.Go(func() error {
			return gw.StartAdminHandler(ctx)
		})
		g.Go(func() error {
			return gw.StartWebHandler(ctx)
		})
	}
	if enableReplay {
		var replayDB jobsdb.HandleT
		err := replayDB.Setup(
			jobsdb.ReadWrite, options.ClearDB, "replay",
			migrationMode, true, prebackupHandlers,
		)
		if err != nil {
			return fmt.Errorf("could not setup replayDB: %w", err)
		}
		defer replayDB.TearDown()
		embedded.App.Features().Replay.Setup(ctx, &replayDB, gatewayDB, routerDB, batchRouterDB)
	}

	g.Go(func() error {
		// This should happen only after setupDatabaseTables() is called and journal table migrations are done
		// because if this start before that then there might be a case when ReadDB will try to read the owner table
		// which gets created after either Write or ReadWrite DB is created.
		return dm.Run(ctx)
	})

	g.Go(func() error {
		return rsourcesService.CleanupLoop(ctx)
	})

	return g.Wait()
}

func (*EmbeddedApp) HandleRecovery(options *app.Options) {
	db.HandleEmbeddedRecovery(options.NormalMode, options.DegradedMode, options.MigrationMode, misc.AppStartTime, app.EMBEDDED)
}
