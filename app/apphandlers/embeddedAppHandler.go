package apphandlers

import (
	"context"
	"fmt"
	"github.com/rudderlabs/rudder-server/app/cluster"
	"github.com/rudderlabs/rudder-server/app/cluster/state"
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
	gwDB := jobsdb.NewForReadWrite("gw", jobsdb.WithRetention(gwDBRetention), jobsdb.WithMigrationMode(migrationMode),
		jobsdb.WithStatusHandler(), jobsdb.WithQueryFilterKeys(jobsdb.QueryFiltersT{}))
	defer gwDB.Close()
	gatewayDB = *gwDB
	rtDB := jobsdb.NewForReadWrite("rt", jobsdb.WithRetention(routerDBRetention),
		jobsdb.WithMigrationMode(migrationMode),
		jobsdb.WithStatusHandler(), jobsdb.WithQueryFilterKeys(router.QueryFilters))
	defer rtDB.Close()
	brtDB := jobsdb.NewForReadWrite("batch_rt", jobsdb.WithRetention(routerDBRetention),
		jobsdb.WithMigrationMode(migrationMode),
		jobsdb.WithStatusHandler(), jobsdb.WithQueryFilterKeys(batchrouter.QueryFilters))
	defer brtDB.Close()
	errDB := jobsdb.NewForReadWrite("proc_error", jobsdb.WithRetention(routerDBRetention),
		jobsdb.WithMigrationMode(migrationMode),
		jobsdb.WithStatusHandler(), jobsdb.WithQueryFilterKeys(jobsdb.QueryFiltersT{}))

	// TODO: Always initialize multi-tenant stats after PR#1736 gets merged.
	var tenantRouterDB jobsdb.MultiTenantJobsDB = &jobsdb.MultiTenantLegacy{HandleT: rtDB}
	var multitenantStats multitenant.MultiTenantI = multitenant.NOOP
	if config.GetBool("EnableMultitenancy", false) {
		tenantRouterDB = &jobsdb.MultiTenantHandleT{HandleT: rtDB}
		multitenantStats = multitenant.NewStats(tenantRouterDB)
	}

	enableGateway := true
	if embedded.App.Features().Migrator != nil {
		if migrationMode == db.IMPORT || migrationMode == db.EXPORT || migrationMode == db.IMPORT_EXPORT {
			startProcessorFunc := func() {
				clearDB := false
				g.Go(misc.WithBugsnag(func() error {
					StartProcessor(ctx, &clearDB, enableProcessor, gwDB, rtDB, brtDB, errDB,
						reportingI, multitenantStats)
					return nil
				}))
			}
			startRouterFunc := func() {
				g.Go(misc.WithBugsnag(func() error {
					StartRouter(ctx, enableRouter, tenantRouterDB, brtDB, errDB, reportingI, multitenantStats)
					return nil
				}))
			}
			enableRouter = false
			enableProcessor = false
			enableGateway = (migrationMode != db.EXPORT)

			embedded.App.Features().Migrator.PrepareJobsdbsForImport(gwDB, rtDB, brtDB)

			g.Go(func() error {
				embedded.App.Features().Migrator.Run(ctx, gwDB, rtDB, brtDB, startProcessorFunc,
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
	}

	proc := processor.New(ctx, &options.ClearDB, gwDB, rtDB, brtDB, errDB)
	rt := routerManager.New(ctx, brtDB, errDB, tenantRouterDB)

	dm := cluster.Dynamic{
		Provider:      &modeProvider,
		GatewayDB:     gwDB,
		RouterDB:      rtDB,
		BatchRouterDB: brtDB,
		ErrorDB:       errDB,
		Processor:     proc,
		Router:        rt,
	}
	dm.Setup()

	g.Go(func() error {
		return dm.Run(ctx)
	})

	if enableReplay && embedded.App.Features().Replay != nil {
		var replayDB jobsdb.HandleT
		replayDB.Setup(jobsdb.ReadWrite, options.ClearDB, "replay", routerDBRetention, migrationMode, true, jobsdb.QueryFiltersT{})
		defer replayDB.TearDown()
		embedded.App.Features().Replay.Setup(&replayDB, gwDB, rtDB, brtDB)
	}

	if enableGateway {
		var gw gateway.HandleT
		var rateLimiter ratelimiter.HandleT

		rateLimiter.SetUp()
		gw.SetReadonlyDBs(&readonlyGatewayDB, &readonlyRouterDB, &readonlyBatchRouterDB)
		gw.Setup(embedded.App, backendconfig.DefaultBackendConfig, gwDB, &rateLimiter, embedded.VersionHandler)
		defer gw.Shutdown()

		g.Go(func() error {
			return gw.StartAdminHandler(ctx)
		})
		g.Go(func() error {
			return gw.StartWebHandler(ctx)
		})
	}

	return g.Wait()
}

func (embedded *EmbeddedApp) HandleRecovery(options *app.Options) {
	db.HandleEmbeddedRecovery(options.NormalMode, options.DegradedMode, options.StandByMode, options.MigrationMode, misc.AppStartTime, app.EMBEDDED)
}
