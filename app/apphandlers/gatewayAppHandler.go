package apphandlers

import (
	"fmt"
	"net/http"

	"github.com/rudderlabs/rudder-server/app"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/gateway"
	"github.com/rudderlabs/rudder-server/jobsdb"
	ratelimiter "github.com/rudderlabs/rudder-server/rate-limiter"
	"github.com/rudderlabs/rudder-server/services/db"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/source-debugger"
	"github.com/rudderlabs/rudder-server/utils/misc"

	// This is necessary for compatibility with enterprise features
	_ "github.com/rudderlabs/rudder-server/imports"
)

//GatewayApp is the type for Gateway type implemention
type GatewayApp struct {
	App            app.Interface
	VersionHandler func(w http.ResponseWriter, r *http.Request)
}

func (gatewayApp *GatewayApp) GetAppType() string {
	return fmt.Sprintf("rudder-server-%s", app.GATEWAY)
}

func (gatewayApp *GatewayApp) StartRudderCore(options *app.Options) {
	pkgLogger.Info("Gateway starting")

	rudderCoreBaseSetup()

	var gatewayDB jobsdb.HandleT
	pkgLogger.Info("Clearing DB ", options.ClearDB)

	sourcedebugger.Setup()

	migrationMode := gatewayApp.App.Options().MigrationMode
	gatewayDB.Setup(jobsdb.Write, options.ClearDB, "gw", gwDBRetention, migrationMode, false)

	enableGateway := true

	if gatewayApp.App.Features().Migrator != nil {
		if migrationMode == db.IMPORT || migrationMode == db.EXPORT || migrationMode == db.IMPORT_EXPORT {
			enableGateway = (migrationMode != db.EXPORT)
		}

		gatewayApp.App.Features().Migrator.PrepareJobsdbsForImport(&gatewayDB, nil, nil)
	}

	if enableGateway {
		var gateway gateway.HandleT
		var rateLimiter ratelimiter.HandleT

		rateLimiter.SetUp()
		gateway.SetReadonlyDBs(&readonlyGatewayDB, &readonlyRouterDB, &readonlyBatchRouterDB)
		gateway.Setup(gatewayApp.App, backendconfig.DefaultBackendConfig, &gatewayDB, &rateLimiter, gatewayApp.VersionHandler)
		gateway.StartWebHandler()
	}
	//go readIOforResume(router) //keeping it as input from IO, to be replaced by UI
}

func (gateway *GatewayApp) HandleRecovery(options *app.Options) {
	db.HandleNullRecovery(options.NormalMode, options.DegradedMode, options.MigrationMode, misc.AppStartTime, app.GATEWAY)
}
