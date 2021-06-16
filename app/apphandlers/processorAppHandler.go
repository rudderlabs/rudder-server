package apphandlers

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/bugsnag/bugsnag-go"
	"github.com/gorilla/mux"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	operationmanager "github.com/rudderlabs/rudder-server/operation-manager"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/db"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	transformationdebugger "github.com/rudderlabs/rudder-server/services/debugger/transformation"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"

	// This is necessary for compatibility with enterprise features
	_ "github.com/rudderlabs/rudder-server/imports"
)

//ProcessorApp is the type for Processor type implemention
type ProcessorApp struct {
	App            app.Interface
	VersionHandler func(w http.ResponseWriter, r *http.Request)
}

var (
	gatewayDB     jobsdb.HandleT
	routerDB      jobsdb.HandleT
	batchRouterDB jobsdb.HandleT
	procErrorDB   jobsdb.HandleT
)

func (processor *ProcessorApp) GetAppType() string {
	return fmt.Sprintf("rudder-server-%s", app.PROCESSOR)
}

func (processor *ProcessorApp) StartRudderCore(options *app.Options) {
	pkgLogger.Info("Processor starting")

	rudderCoreBaseSetup()

	//Setting up reporting client
	if processor.App.Features().Reporting != nil {
		reporting := processor.App.Features().Reporting.Setup(backendconfig.DefaultBackendConfig)
		reporting.AddClient(types.Config{ConnInfo: jobsdb.GetConnectionString()})
	}

	pkgLogger.Info("Clearing DB ", options.ClearDB)

	transformationdebugger.Setup()
	destinationdebugger.Setup()

	migrationMode := processor.App.Options().MigrationMode

	//IMP NOTE: All the jobsdb setups must happen before migrator setup.
	gatewayDB.Setup(jobsdb.Read, options.ClearDB, "gw", gwDBRetention, migrationMode, false, jobsdb.QueryFiltersT{})
	if enableProcessor || enableReplay {
		//setting up router, batch router, proc error DBs only if processor is enabled.
		routerDB.Setup(jobsdb.ReadWrite, options.ClearDB, "rt", routerDBRetention, migrationMode, true, router.QueryFilters)
		batchRouterDB.Setup(jobsdb.ReadWrite, options.ClearDB, "batch_rt", routerDBRetention, migrationMode, true, batchrouter.QueryFilters)
		procErrorDB.Setup(jobsdb.ReadWrite, options.ClearDB, "proc_error", routerDBRetention, migrationMode, false, jobsdb.QueryFiltersT{})
	}

	var reportingI types.ReportingI
	if processor.App.Features().Reporting != nil {
		reportingI = processor.App.Features().Reporting.GetReportingInstance()
	}

	if processor.App.Features().Migrator != nil {
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

			processor.App.Features().Migrator.PrepareJobsdbsForImport(nil, &routerDB, &batchRouterDB)
			processor.App.Features().Migrator.Setup(&gatewayDB, &routerDB, &batchRouterDB, startProcessorFunc, startRouterFunc)
		}
	}

	operationmanager.Setup(&gatewayDB, &routerDB, &batchRouterDB)
	rruntime.Go(func() {
		operationmanager.OperationManager.StartProcessLoop()
	})

	StartProcessor(&options.ClearDB, enableProcessor, &gatewayDB, &routerDB, &batchRouterDB, &procErrorDB, reportingI)
	StartRouter(enableRouter, &routerDB, &batchRouterDB, &procErrorDB, reportingI)

	if processor.App.Features().Replay != nil {
		var replayDB jobsdb.HandleT
		replayDB.Setup(jobsdb.ReadWrite, options.ClearDB, "replay", routerDBRetention, migrationMode, true, jobsdb.QueryFiltersT{})
		processor.App.Features().Replay.Setup(&replayDB, &gatewayDB, &routerDB)
	}

	startHealthWebHandler()
	//go readIOforResume(router) //keeping it as input from IO, to be replaced by UI
}

func (processor *ProcessorApp) HandleRecovery(options *app.Options) {
	db.HandleNullRecovery(options.NormalMode, options.DegradedMode, options.StandByMode, options.MigrationMode, misc.AppStartTime, app.PROCESSOR)
}

func startHealthWebHandler() {
	//Port where Processor health handler is running
	webPort := config.GetInt("Processor.webPort", 8086)
	pkgLogger.Infof("Starting in %d", webPort)
	srvMux := mux.NewRouter()
	srvMux.HandleFunc("/health", healthHandler)
	srvMux.HandleFunc("/", healthHandler)
	srv := &http.Server{
		Addr:              ":" + strconv.Itoa(webPort),
		Handler:           bugsnag.Handler(srvMux),
		ReadTimeout:       config.GetDuration("ReadTimeOutInSec", 0*time.Second),
		ReadHeaderTimeout: config.GetDuration("ReadHeaderTimeoutInSec", 0*time.Second),
		WriteTimeout:      config.GetDuration("WriteTimeOutInSec", 10*time.Second),
		IdleTimeout:       config.GetDuration("IdleTimeoutInSec", 720*time.Second),
		MaxHeaderBytes:    config.GetInt("MaxHeaderBytes", 524288),
	}
	pkgLogger.Fatal(srv.ListenAndServe())
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	app.HealthHandler(w, r, &gatewayDB)
}
