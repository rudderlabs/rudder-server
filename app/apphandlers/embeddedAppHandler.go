package apphandlers

import (
	"context"
	"fmt"
	"github.com/rudderlabs/rudder-server/app/cluster"
	"github.com/rudderlabs/rudder-server/app/cluster/state"
	operationmanager "github.com/rudderlabs/rudder-server/operation-manager"
	"github.com/rudderlabs/rudder-server/processor"
	routerManager "github.com/rudderlabs/rudder-server/router/manager"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/debugger/source"
	transformationdebugger "github.com/rudderlabs/rudder-server/services/debugger/transformation"
	"github.com/rudderlabs/rudder-server/utils/types/servermode"
	"net/http"

	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/gateway"
	"github.com/rudderlabs/rudder-server/jobsdb"
	ratelimiter "github.com/rudderlabs/rudder-server/rate-limiter"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	"github.com/rudderlabs/rudder-server/services/db"
	"github.com/rudderlabs/rudder-server/services/multitenant"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-server/utils/types"

	// This is necessary for compatibility with enterprise features
	_ "github.com/rudderlabs/rudder-server/imports"
)

//EmbeddedApp is the type for embedded type implemention
type EmbeddedApp struct {
	App            app.Interface
	VersionHandler func(w http.ResponseWriter, r *http.Request)
}

func (embedded *EmbeddedApp) GetAppType() string {
	return fmt.Sprintf("rudder-server-%s", app.EMBEDDED)
}

func (embedded *EmbeddedApp) StartRudderCore(ctx context.Context, options *app.Options) error {
	pkgLogger.Info("Main starting")

	rudderCoreDBValidator()
	rudderCoreWorkSpaceTableSetup()
	rudderCoreNodeSetup()
	rudderCoreBaseSetup()

	g, ctx := errgroup.WithContext(ctx)

	//Setting up reporting client
	if embedded.App.Features().Reporting != nil {
		reporting := embedded.App.Features().Reporting.Setup(backendconfig.DefaultBackendConfig)

		g.Go(func() error {
			reporting.AddClient(ctx, types.Config{ConnInfo: jobsdb.GetConnectionString()})
			return nil
		})
	}

	pkgLogger.Info("Clearing DB ", options.ClearDB)

	transformationdebugger.Setup()
	destinationdebugger.Setup(backendconfig.DefaultBackendConfig)
	sourcedebugger.Setup(backendconfig.DefaultBackendConfig)

	migrationMode := embedded.App.Options().MigrationMode
	reportingI := embedded.App.Features().Reporting.GetReportingInstance()

	//IMP NOTE: All the jobsdb setups must happen before migrator setup.
	// This gwDBForProcessor should only be used by processor as this is supposed to be stopped and started with the
	//Processor.
	gwDBForProcessor := jobsdb.NewForRead(
		"gw",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithRetention(gwDBRetention),
		jobsdb.WithMigrationMode(migrationMode),
		jobsdb.WithStatusHandler(),
		jobsdb.WithQueryFilterKeys(jobsdb.QueryFiltersT{}),
	)
	defer gwDBForProcessor.Close()
	routerDB := jobsdb.NewForReadWrite(
		"rt",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithRetention(routerDBRetention),
		jobsdb.WithMigrationMode(migrationMode),
		jobsdb.WithStatusHandler(),
		jobsdb.WithQueryFilterKeys(router.QueryFilters),
	)
	defer routerDB.Close()
	batchRouterDB := jobsdb.NewForReadWrite(
		"batch_rt",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithRetention(routerDBRetention),
		jobsdb.WithMigrationMode(migrationMode),
		jobsdb.WithStatusHandler(),
		jobsdb.WithQueryFilterKeys(batchrouter.QueryFilters),
	)
	defer batchRouterDB.Close()
	errDB := jobsdb.NewForReadWrite(
		"proc_error",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithRetention(routerDBRetention),
		jobsdb.WithMigrationMode(migrationMode),
		jobsdb.WithStatusHandler(),
		jobsdb.WithQueryFilterKeys(jobsdb.QueryFiltersT{}),
	)

	var tenantRouterDB jobsdb.MultiTenantJobsDB
	var multitenantStats multitenant.MultiTenantI
	if config.GetBool("EnableMultitenancy", false) {
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
				g.Go(misc.WithBugsnag(func() error {
					StartProcessor(ctx, &clearDB, enableProcessor, gwDBForProcessor, routerDB, batchRouterDB, errDB,
						reportingI, multitenant.NOOP)
					return nil
				}))
			}
			startRouterFunc := func() {
				g.Go(misc.WithBugsnag(func() error {
					StartRouter(ctx, enableRouter, tenantRouterDB, batchRouterDB, errDB, reportingI, multitenant.NOOP)
					return nil
				}))
			}
			enableRouter = false
			enableProcessor = false
			enableGateway = (migrationMode != db.EXPORT)

			embedded.App.Features().Migrator.PrepareJobsdbsForImport(gwDBForProcessor, routerDB, batchRouterDB)

			g.Go(func() error {
				embedded.App.Features().Migrator.Run(ctx, gwDBForProcessor, routerDB, batchRouterDB, startProcessorFunc,
					startRouterFunc) //TODO
				return nil
			})
		}
	}

	var modeProvider state.StaticProvider
	// FIXME: hacky way to determine servermode
	if enableProcessor && enableRouter {
		modeProvider = state.StaticProvider{
			Mode: servermode.NormalMode,
		}
	} else {
		modeProvider = state.StaticProvider{
			Mode: servermode.DegradedMode,
		}
	}

	proc := processor.New(ctx, &options.ClearDB, gwDBForProcessor, routerDB, batchRouterDB, errDB, multitenantStats)
	rtFactory := &router.Factory{
		Reporting:     reportingI,
		Multitenant:   multitenantStats,
		BackendConfig: backendconfig.DefaultBackendConfig,
		RouterDB:      tenantRouterDB,
		ProcErrorDB:   errDB,
	}
	brtFactory := &batchrouter.Factory{
		Reporting:     reportingI,
		Multitenant:   multitenantStats,
		BackendConfig: backendconfig.DefaultBackendConfig,
		RouterDB:      batchRouterDB,
		ProcErrorDB:   errDB,
	}
	rt := routerManager.New(rtFactory, brtFactory, backendconfig.DefaultBackendConfig)

	dm := cluster.Dynamic{
		Provider:      &modeProvider,
		GatewayDB:     gwDBForProcessor,
		RouterDB:      routerDB,
		BatchRouterDB: batchRouterDB,
		ErrorDB:       errDB,
		Processor:     proc,
		Router:        rt,
		MultiTenantStat: multitenantStats,
	}

	if enableReplay && embedded.App.Features().Replay != nil {
		var replayDB jobsdb.HandleT
		replayDB.Setup(jobsdb.ReadWrite, options.ClearDB, "replay", routerDBRetention, migrationMode, true, jobsdb.QueryFiltersT{})
		defer replayDB.TearDown()
		embedded.App.Features().Replay.Setup(&replayDB, gwDBForProcessor, routerDB, batchRouterDB)
	}

	if enableGateway {
		rateLimiter := ratelimiter.HandleT{}
		rateLimiter.SetUp()
		gw := gateway.HandleT{}
		// This separate gateway db is created just to be used with gateway because in case of degraded mode,
		//the earlier created gwDb (which was created to be used mainly with processor) will not be running, and it
		//will cause issues for gateway because gateway is supposed to receive jobs even in degraded mode.
		gatewayDB = *jobsdb.NewForWrite(
			"gw",
			jobsdb.WithClearDB(options.ClearDB),
			jobsdb.WithRetention(gwDBRetention),
			jobsdb.WithMigrationMode(migrationMode),
			jobsdb.WithStatusHandler(),
			jobsdb.WithQueryFilterKeys(jobsdb.QueryFiltersT{}),
		)
		defer gwDBForProcessor.Close()
		gatewayDB.Start()
		defer gatewayDB.Stop()

		gw.SetReadonlyDBs(&readonlyGatewayDB, &readonlyRouterDB, &readonlyBatchRouterDB)
		gw.Setup(embedded.App, backendconfig.DefaultBackendConfig, &gatewayDB, &rateLimiter, embedded.VersionHandler)
		defer gw.Shutdown()

		g.Go(func() error {
			return gw.StartAdminHandler(ctx)
		})
		g.Go(func() error {
			return gw.StartWebHandler(ctx)
		})
	}

	g.Go(func() error {
		return dm.Run(ctx)
	})

	return g.Wait()
}

func (embedded *EmbeddedApp) HandleRecovery(options *app.Options) {
	db.HandleEmbeddedRecovery(options.NormalMode, options.DegradedMode, options.StandByMode, options.MigrationMode, misc.AppStartTime, app.EMBEDDED)
}

func (embedded *EmbeddedApp) LegacyStart(ctx context.Context, options *app.Options) error {
	pkgLogger.Info("Main starting")

	rudderCoreDBValidator()
	rudderCoreWorkSpaceTableSetup()
	rudderCoreNodeSetup()
	rudderCoreBaseSetup()

	g, ctx := errgroup.WithContext(ctx)

	//Setting up reporting client
	if embedded.App.Features().Reporting != nil {
		reporting := embedded.App.Features().Reporting.Setup(backendconfig.DefaultBackendConfig)

		g.Go(func() error {
			reporting.AddClient(ctx, types.Config{ConnInfo: jobsdb.GetConnectionString()})
			return nil
		})
	}

	var gatewayDB jobsdb.HandleT
	var routerDB jobsdb.HandleT
	var batchRouterDB jobsdb.HandleT
	var procErrorDB jobsdb.HandleT

	var tenantRouterDB jobsdb.MultiTenantJobsDB
	var multitenantStats multitenant.MultiTenantI

	pkgLogger.Info("Clearing DB ", options.ClearDB)

	transformationdebugger.Setup()
	destinationdebugger.Setup(backendconfig.DefaultBackendConfig)
	sourcedebugger.Setup(backendconfig.DefaultBackendConfig)

	migrationMode := embedded.App.Options().MigrationMode
	//IMP NOTE: All the jobsdb setups must happen before migrator setup.
	gatewayDB.Setup(jobsdb.ReadWrite, options.ClearDB, "gw", gwDBRetention, migrationMode, true, jobsdb.QueryFiltersT{})
	defer gatewayDB.TearDown()

	if enableProcessor || enableReplay {
		//setting up router, batch router, proc error DBs only if processor is enabled.
		routerDB.Setup(jobsdb.ReadWrite, options.ClearDB, "rt", routerDBRetention, migrationMode, true, router.QueryFilters)
		defer routerDB.TearDown()

		batchRouterDB.Setup(jobsdb.ReadWrite, options.ClearDB, "batch_rt", routerDBRetention, migrationMode, true, batchrouter.QueryFilters)
		defer batchRouterDB.TearDown()

		procErrorDB.Setup(jobsdb.ReadWrite, options.ClearDB, "proc_error", routerDBRetention, migrationMode, false, jobsdb.QueryFiltersT{})
		defer procErrorDB.TearDown()

		if config.GetBool("EnableMultitenancy", false) {
			tenantRouterDB = &jobsdb.MultiTenantHandleT{HandleT: &routerDB}
			multitenantStats = multitenant.NewStats(map[string]jobsdb.MultiTenantJobsDB{
				"rt":       tenantRouterDB,
				"batch_rt": &jobsdb.MultiTenantLegacy{HandleT: &batchRouterDB},
			})
		} else {
			tenantRouterDB = &jobsdb.MultiTenantLegacy{HandleT: &routerDB}
			multitenantStats = multitenant.WithLegacyPickupJobs(multitenant.NewStats(map[string]jobsdb.MultiTenantJobsDB{
				"rt":       tenantRouterDB,
				"batch_rt": &jobsdb.MultiTenantLegacy{HandleT: &batchRouterDB},
			}))
		}
	}

	enableGateway := true
	reportingI := embedded.App.Features().Reporting.GetReportingInstance()

	if embedded.App.Features().Migrator != nil {
		if migrationMode == db.IMPORT || migrationMode == db.EXPORT || migrationMode == db.IMPORT_EXPORT {
			startProcessorFunc := func() {
				clearDB := false
				g.Go(misc.WithBugsnag(func() error {
					StartProcessor(ctx, &clearDB, enableProcessor, &gatewayDB, &routerDB, &batchRouterDB, &procErrorDB, reportingI, multitenantStats)
					return nil
				}))
			}
			startRouterFunc := func() {
				g.Go(misc.WithBugsnag(func() error {
					StartRouter(ctx, enableRouter, tenantRouterDB, &batchRouterDB, &procErrorDB, reportingI, multitenantStats)
					return nil
				}))
			}
			enableRouter = false
			enableProcessor = false
			enableGateway = (migrationMode != db.EXPORT)

			embedded.App.Features().Migrator.PrepareJobsdbsForImport(&gatewayDB, &routerDB, &batchRouterDB)

			g.Go(func() error {
				embedded.App.Features().Migrator.Run(ctx, &gatewayDB, &routerDB, &batchRouterDB, startProcessorFunc, startRouterFunc) //TODO
				return nil
			})
		}
	}

	operationmanager.Setup(&gatewayDB, &routerDB, &batchRouterDB)

	g.Go(misc.WithBugsnag(func() error {
		return operationmanager.OperationManager.StartProcessLoop(ctx)
	}))

	g.Go(func() error {
		StartProcessor(ctx, &options.ClearDB, enableProcessor, &gatewayDB, &routerDB, &batchRouterDB, &procErrorDB, reportingI, multitenantStats)
		return nil
	})
	g.Go(func() error {
		StartRouter(ctx, enableRouter, tenantRouterDB, &batchRouterDB, &procErrorDB, reportingI, multitenantStats)
		return nil
	})

	if enableReplay && embedded.App.Features().Replay != nil {
		var replayDB jobsdb.HandleT
		replayDB.Setup(jobsdb.ReadWrite, options.ClearDB, "replay", routerDBRetention, migrationMode, true, jobsdb.QueryFiltersT{})
		defer replayDB.TearDown()
		embedded.App.Features().Replay.Setup(&replayDB, &gatewayDB, &routerDB, &batchRouterDB)
	}

	if enableGateway {
		var gateway gateway.HandleT
		var rateLimiter ratelimiter.HandleT

		rateLimiter.SetUp()
		gateway.SetReadonlyDBs(&readonlyGatewayDB, &readonlyRouterDB, &readonlyBatchRouterDB)
		gateway.Setup(embedded.App, backendconfig.DefaultBackendConfig, &gatewayDB, &rateLimiter, embedded.VersionHandler)
		defer gateway.Shutdown()

		g.Go(func() error {
			return gateway.StartAdminHandler(ctx)
		})
		g.Go(func() error {
			return gateway.StartWebHandler(ctx)
		})
	}

	return g.Wait()
}
