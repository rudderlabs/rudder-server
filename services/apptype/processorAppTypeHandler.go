package apptype

import (
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"time"

	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/db"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/destination-debugger"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/validators"
	"github.com/rudderlabs/rudder-server/utils/misc"

	// This is necessary for compatibility with enterprise features
	_ "github.com/rudderlabs/rudder-server/imports"
)

//ProcessorAppType is the type for Processor type implemention
type ProcessorAppType struct {
	App            app.Interface
	VersionHandler func(w http.ResponseWriter, r *http.Request)
}

func (processor *ProcessorAppType) GetAppType() string {
	return "rudder-server-processor"
}

func (processor *ProcessorAppType) StartRudderCore(options *app.Options) {
	pkgLogger.Info("Main starting")

	if !validators.ValidateEnv() {
		panic(errors.New("Failed to start rudder-server"))
	}
	validators.InitializeEnv()

	// Check if there is a probable inconsistent state of Data
	if diagnostics.EnableServerStartMetric {
		Diagnostics.Track(diagnostics.ServerStart, map[string]interface{}{
			diagnostics.ServerStart: fmt.Sprint(time.Unix(misc.AppStartTime, 0)),
		})
	}

	//Reload Config
	loadConfig()

	var gatewayDB jobsdb.HandleT
	var routerDB jobsdb.HandleT
	var batchRouterDB jobsdb.HandleT
	var procErrorDB jobsdb.HandleT

	runtime.GOMAXPROCS(maxProcess)
	pkgLogger.Info("Clearing DB ", options.ClearDB)

	destinationdebugger.Setup()

	migrationMode := processor.App.Options().MigrationMode
	gatewayDB.Setup(jobsdb.Read, options.ClearDB, "gw", gwDBRetention, migrationMode, false)

	if processor.App.Features().Migrator != nil {
		if migrationMode == db.IMPORT || migrationMode == db.EXPORT || migrationMode == db.IMPORT_EXPORT {
			startProcessorFunc := func() {
				clearDBBool := false
				StartProcessor(&clearDBBool, migrationMode, enableProcessor, &gatewayDB, &routerDB, &batchRouterDB, &procErrorDB)
			}
			startRouterFunc := func() {
				StartRouter(enableRouter, &routerDB, &batchRouterDB)
			}
			enableRouter = false
			enableProcessor = false
			processor.App.Features().Migrator.Setup(&gatewayDB, &routerDB, &batchRouterDB, startProcessorFunc, startRouterFunc)
		}
	}

	StartProcessor(&options.ClearDB, migrationMode, enableProcessor, &gatewayDB, &routerDB, &batchRouterDB, &procErrorDB)
	StartRouter(enableRouter, &routerDB, &batchRouterDB)

	//go readIOforResume(router) //keeping it as input from IO, to be replaced by UI
}

func (processor *ProcessorAppType) HandleRecovery(options *app.Options) {
	db.HandleProcessorRecovery(options.NormalMode, options.DegradedMode, options.MigrationMode, misc.AppStartTime)
}
