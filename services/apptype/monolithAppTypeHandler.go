package apptype

import (
	"errors"
	"fmt"
	"net/http"

	"runtime"
	"time"

	"github.com/rudderlabs/rudder-server/app"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/gateway"
	"github.com/rudderlabs/rudder-server/jobsdb"
	ratelimiter "github.com/rudderlabs/rudder-server/rate-limiter"
	"github.com/rudderlabs/rudder-server/services/db"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/destination-debugger"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/source-debugger"
	"github.com/rudderlabs/rudder-server/services/validators"
	"github.com/rudderlabs/rudder-server/utils/misc"

	// This is necessary for compatibility with enterprise features
	_ "github.com/rudderlabs/rudder-server/imports"
)

//MonolithAppType is the type for monolith type implemention
type MonolithAppType struct {
	App            app.Interface
	VersionHandler func(w http.ResponseWriter, r *http.Request)
}

func (monolith *MonolithAppType) GetAppType() string {
	return "rudder-server"
}

func (monolith *MonolithAppType) StartRudderCore(options *app.Options) {
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
	sourcedebugger.Setup()

	migrationMode := monolith.App.Options().MigrationMode
	gatewayDB.Setup(jobsdb.ReadWrite, options.ClearDB, "gw", gwDBRetention, migrationMode, false)

	enableGateway := true

	if monolith.App.Features().Migrator != nil {
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
			enableGateway = (migrationMode != db.EXPORT)
			monolith.App.Features().Migrator.Setup(&gatewayDB, &routerDB, &batchRouterDB, startProcessorFunc, startRouterFunc)
		}
	}

	StartProcessor(&options.ClearDB, migrationMode, enableProcessor, &gatewayDB, &routerDB, &batchRouterDB, &procErrorDB)
	StartRouter(enableRouter, &routerDB, &batchRouterDB)

	if enableGateway {
		var gateway gateway.HandleT
		var rateLimiter ratelimiter.HandleT

		rateLimiter.SetUp()
		gateway.Setup(monolith.App, backendconfig.DefaultBackendConfig, &gatewayDB, &rateLimiter, &options.ClearDB, monolith.VersionHandler)
		gateway.StartWebHandler()
	}
	//go readIOforResume(router) //keeping it as input from IO, to be replaced by UI
}

func (monolith *MonolithAppType) HandleRecovery(options *app.Options) {
	db.HandleMonolithRecovery(options.NormalMode, options.DegradedMode, options.MigrationMode, misc.AppStartTime)
}
