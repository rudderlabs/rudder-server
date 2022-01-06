package apphandlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/gateway"
	"github.com/rudderlabs/rudder-server/jobsdb"
	operationmanager "github.com/rudderlabs/rudder-server/operation-manager"
	ratelimiter "github.com/rudderlabs/rudder-server/rate-limiter"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	"github.com/rudderlabs/rudder-server/services/db"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/debugger/source"
	transformationdebugger "github.com/rudderlabs/rudder-server/services/debugger/transformation"
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

	var gatewayDB jobsdb.HandleT
	var routerDB jobsdb.HandleT
	var batchRouterDB jobsdb.HandleT
	var procErrorDB jobsdb.HandleT

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
	}

	enableGateway := true
	var reportingI types.ReportingI
	if embedded.App.Features().Reporting != nil && config.GetBool("Reporting.enabled", types.DEFAULT_REPORTING_ENABLED) {
		reportingI = embedded.App.Features().Reporting.GetReportingInstance()
	}

	if embedded.App.Features().Migrator != nil {
		if migrationMode == db.IMPORT || migrationMode == db.EXPORT || migrationMode == db.IMPORT_EXPORT {
			startProcessorFunc := func() {
				clearDB := false
				g.Go(misc.WithBugsnag(func() error {
					StartProcessor(ctx, &clearDB, enableProcessor, &gatewayDB, &routerDB, &batchRouterDB, &procErrorDB, reportingI)
					return nil
				}))
			}
			startRouterFunc := func() {
				g.Go(misc.WithBugsnag(func() error {
					StartRouter(ctx, enableRouter, &routerDB, &batchRouterDB, &procErrorDB, reportingI)
					return nil
				}))
			}
			enableRouter = false
			enableProcessor = false
			enableGateway = (migrationMode != db.EXPORT)

			embedded.App.Features().Migrator.PrepareJobsdbsForImport(&gatewayDB, &routerDB, &batchRouterDB)

			g.Go(func() error {
				embedded.App.Features().Migrator.Run(ctx, &gatewayDB, &routerDB, &batchRouterDB, startProcessorFunc, startRouterFunc)
				return nil
			})
		}
	}

	operationmanager.Setup(&gatewayDB, &routerDB, &batchRouterDB)

	g.Go(misc.WithBugsnag(func() error {
		return operationmanager.OperationManager.StartProcessLoop(ctx)
	}))

	g.Go(func() error {
		StartProcessor(ctx, &options.ClearDB, enableProcessor, &gatewayDB, &routerDB, &batchRouterDB, &procErrorDB, reportingI)
		return nil
	})
	g.Go(func() error {
		StartRouter(ctx, enableRouter, &routerDB, &batchRouterDB, &procErrorDB, reportingI)
		return nil
	})

	if enableReplay && embedded.App.Features().Replay != nil {
		var replayDB jobsdb.HandleT
		replayDB.Setup(jobsdb.ReadWrite, options.ClearDB, "replay", routerDBRetention, migrationMode, true, jobsdb.QueryFiltersT{})
		defer replayDB.TearDown()
		embedded.App.Features().Replay.Setup(&replayDB, &gatewayDB, &routerDB)
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

func (embedded *EmbeddedApp) HandleRecovery(options *app.Options) {
	db.HandleEmbeddedRecovery(options.NormalMode, options.DegradedMode, options.StandByMode, options.MigrationMode, misc.AppStartTime, app.EMBEDDED)
}
