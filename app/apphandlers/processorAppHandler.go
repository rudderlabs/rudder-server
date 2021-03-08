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
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/db"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/destination-debugger"
	"github.com/rudderlabs/rudder-server/utils/misc"

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

	pkgLogger.Info("Clearing DB ", options.ClearDB)

	destinationdebugger.Setup()

	migrationMode := processor.App.Options().MigrationMode

	//IMP NOTE: All the jobsdb setups must happen before migrator setup.
	gatewayDB.Setup(jobsdb.Read, options.ClearDB, "gw", gwDBRetention, migrationMode, false)
	if enableProcessor {
		//setting up router, batch router, proc error DBs only if processor is enabled.
		routerDB.Setup(jobsdb.ReadWrite, options.ClearDB, "rt", routerDBRetention, migrationMode, true)
		batchRouterDB.Setup(jobsdb.ReadWrite, options.ClearDB, "batch_rt", routerDBRetention, migrationMode, true)
		procErrorDB.Setup(jobsdb.ReadWrite, options.ClearDB, "proc_error", routerDBRetention, migrationMode, false)
	}

	if processor.App.Features().Migrator != nil {
		if migrationMode == db.IMPORT || migrationMode == db.EXPORT || migrationMode == db.IMPORT_EXPORT {
			startProcessorFunc := func() {
				clearDB := false
				StartProcessor(&clearDB, enableProcessor, &gatewayDB, &routerDB, &batchRouterDB, &procErrorDB)
			}
			startRouterFunc := func() {
				StartRouter(enableRouter, &routerDB, &batchRouterDB, &procErrorDB)
			}
			enableRouter = false
			enableProcessor = false

			processor.App.Features().Migrator.PrepareJobsdbsForImport(nil, &routerDB, &batchRouterDB)
			processor.App.Features().Migrator.Setup(&gatewayDB, &routerDB, &batchRouterDB, startProcessorFunc, startRouterFunc)
		}
	}

	StartProcessor(&options.ClearDB, enableProcessor, &gatewayDB, &routerDB, &batchRouterDB, &procErrorDB)
	StartRouter(enableRouter, &routerDB, &batchRouterDB, &procErrorDB)

	startHealthWebHandler()
	//go readIOforResume(router) //keeping it as input from IO, to be replaced by UI
}

func (processor *ProcessorApp) HandleRecovery(options *app.Options) {
	db.HandleNullRecovery(options.NormalMode, options.DegradedMode, options.MigrationMode, misc.AppStartTime, app.PROCESSOR)
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
