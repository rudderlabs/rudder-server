package apptype

import (
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

//GatewayAppType is the type for Gateway type implemention
type GatewayAppType struct {
	App            app.Interface
	VersionHandler func(w http.ResponseWriter, r *http.Request)
}

func (gatewayApp *GatewayAppType) GetAppType() string {
	return "rudder-server-gateway"
}

func (gatewayApp *GatewayAppType) StartRudderCore(options *app.Options) {
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
	}

	if enableGateway {
		var gateway gateway.HandleT
		var rateLimiter ratelimiter.HandleT

		rateLimiter.SetUp()
		gateway.Setup(gatewayApp.App, backendconfig.DefaultBackendConfig, &gatewayDB, &rateLimiter, &options.ClearDB, gatewayApp.VersionHandler)
		gateway.StartWebHandler()
	}
	//go readIOforResume(router) //keeping it as input from IO, to be replaced by UI
}

func (gateway *GatewayAppType) HandleRecovery(options *app.Options) {
	db.HandleMicroRecovery(options.NormalMode, options.DegradedMode, options.MigrationMode, misc.AppStartTime)
}
