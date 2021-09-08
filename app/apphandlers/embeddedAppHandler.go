package apphandlers

import (
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
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/db"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/debugger/source"
	transformationdebugger "github.com/rudderlabs/rudder-server/services/debugger/transformation"
	"github.com/rudderlabs/rudder-server/utils/misc"
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

func (embedded *EmbeddedApp) StartRudderCore(options *app.Options) {
	pkgLogger.Info("Main starting")

	rudderCoreBaseSetup()

	//Setting up reporting client
	if embedded.App.Features().Reporting != nil {
		reporting := embedded.App.Features().Reporting.Setup(backendconfig.DefaultBackendConfig)
		reporting.AddClient(types.Config{ConnInfo: jobsdb.GetConnectionString()})
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
	if enableProcessor || enableReplay {
		//setting up router, batch router, proc error DBs only if processor is enabled.
		routerDB.Setup(jobsdb.ReadWrite, options.ClearDB, "rt", routerDBRetention, migrationMode, true, router.QueryFilters)
		batchRouterDB.Setup(jobsdb.ReadWrite, options.ClearDB, "batch_rt", routerDBRetention, migrationMode, true, batchrouter.QueryFilters)
		procErrorDB.Setup(jobsdb.ReadWrite, options.ClearDB, "proc_error", routerDBRetention, migrationMode, false, jobsdb.QueryFiltersT{})
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
				StartProcessor(&clearDB, enableProcessor, &gatewayDB, &routerDB, &batchRouterDB, &procErrorDB, reportingI)
			}
			startRouterFunc := func() {
				StartRouter(enableRouter, &routerDB, &batchRouterDB, &procErrorDB, reportingI)
			}
			enableRouter = false
			enableProcessor = false
			enableGateway = (migrationMode != db.EXPORT)

			embedded.App.Features().Migrator.PrepareJobsdbsForImport(&gatewayDB, &routerDB, &batchRouterDB)
			embedded.App.Features().Migrator.Setup(&gatewayDB, &routerDB, &batchRouterDB, startProcessorFunc, startRouterFunc)
		}
	}

	operationmanager.Setup(&gatewayDB, &routerDB, &batchRouterDB)
	rruntime.Go(func() {
		operationmanager.OperationManager.StartProcessLoop()
	})

	StartProcessor(&options.ClearDB, enableProcessor, &gatewayDB, &routerDB, &batchRouterDB, &procErrorDB, reportingI)
	StartRouter(enableRouter, &routerDB, &batchRouterDB, &procErrorDB, reportingI)

	if embedded.App.Features().Replay != nil {
		var replayDB jobsdb.HandleT
		replayDB.Setup(jobsdb.ReadWrite, options.ClearDB, "replay", routerDBRetention, migrationMode, true, jobsdb.QueryFiltersT{})
		embedded.App.Features().Replay.Setup(&replayDB, &gatewayDB, &routerDB)
	}

	if enableGateway {
		var gateway gateway.HandleT
		var rateLimiter ratelimiter.HandleT

		rateLimiter.SetUp()
		gateway.SetReadonlyDBs(&readonlyGatewayDB, &readonlyRouterDB, &readonlyBatchRouterDB)
		gateway.Setup(embedded.App, backendconfig.DefaultBackendConfig, &gatewayDB, &rateLimiter, embedded.VersionHandler)
		go gateway.StartAdminHandler()
		gateway.StartWebHandler()
	}
	//go readIOforResume(router) //keeping it as input from IO, to be replaced by UI
}

func (embedded *EmbeddedApp) HandleRecovery(options *app.Options) {
	db.HandleEmbeddedRecovery(options.NormalMode, options.DegradedMode, options.StandByMode, options.MigrationMode, misc.AppStartTime, app.EMBEDDED)
}
