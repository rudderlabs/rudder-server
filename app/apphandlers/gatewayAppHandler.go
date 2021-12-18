package apphandlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/rudderlabs/rudder-server/app"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/gateway"
	"github.com/rudderlabs/rudder-server/jobsdb"
	operationmanager "github.com/rudderlabs/rudder-server/operation-manager"
	ratelimiter "github.com/rudderlabs/rudder-server/rate-limiter"
	"github.com/rudderlabs/rudder-server/services/db"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/debugger/source"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"golang.org/x/sync/errgroup"

	// This is necessary for compatibility with enterprise features
	_ "github.com/rudderlabs/rudder-server/imports"
)

// GatewayApp is the type for Gateway type implemention
type GatewayApp struct {
	App            app.Interface
	VersionHandler func(w http.ResponseWriter, r *http.Request)
}

func (gatewayApp *GatewayApp) GetAppType() string {
	return fmt.Sprintf("rudder-server-%s", app.GATEWAY)
}

func (gatewayApp *GatewayApp) StartRudderCore(ctx context.Context, options *app.Options) error {
	pkgLogger.Info("Gateway starting")

	rudderCoreDBValidator()
	rudderCoreWorkSpaceTableSetup()
	rudderCoreBaseSetup()

	var gatewayDB jobsdb.HandleT
	pkgLogger.Info("Clearing DB ", options.ClearDB)

	sourcedebugger.Setup(backendconfig.DefaultBackendConfig)

	migrationMode := gatewayApp.App.Options().MigrationMode
	gatewayDB.Setup(jobsdb.Write, options.ClearDB, "gw", gwDBRetention, migrationMode, true, jobsdb.QueryFiltersT{})
	defer gatewayDB.TearDown()

	operationmanager.Setup(&gatewayDB, nil, nil)

	enableGateway := true

	if gatewayApp.App.Features().Migrator != nil {
		if migrationMode == db.IMPORT || migrationMode == db.EXPORT || migrationMode == db.IMPORT_EXPORT {
			enableGateway = (migrationMode != db.EXPORT)

			gatewayApp.App.Features().Migrator.PrepareJobsdbsForImport(&gatewayDB, nil, nil)
		}
	}

	g, ctx := errgroup.WithContext(ctx)

	if enableGateway {
		var gateway gateway.HandleT
		var rateLimiter ratelimiter.HandleT

		rateLimiter.SetUp()
		gateway.SetReadonlyDBs(&readonlyGatewayDB, &readonlyRouterDB, &readonlyBatchRouterDB)
		gateway.Setup(gatewayApp.App, backendconfig.DefaultBackendConfig, &gatewayDB, &rateLimiter, gatewayApp.VersionHandler)
		defer gateway.Shutdown()

		g.Go(func() error {
			return gateway.StartAdminHandler(ctx)
		})
		g.Go(func() error {
			return gateway.StartWebHandler(ctx)
		})
	}
	// go readIOforResume(router) //keeping it as input from IO, to be replaced by UI
	return g.Wait()
}

func (gateway *GatewayApp) HandleRecovery(options *app.Options) {
	db.HandleNullRecovery(options.NormalMode, options.DegradedMode, options.StandByMode, options.MigrationMode, misc.AppStartTime, app.GATEWAY)
}
