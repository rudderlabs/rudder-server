package apphandlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/rudderlabs/rudder-server/app/cluster"
	"github.com/rudderlabs/rudder-server/app/cluster/state"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
	"github.com/rudderlabs/rudder-server/utils/types/servermode"

	"github.com/rudderlabs/rudder-server/app"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/gateway"
	"github.com/rudderlabs/rudder-server/jobsdb"
	ratelimiter "github.com/rudderlabs/rudder-server/rate-limiter"
	"github.com/rudderlabs/rudder-server/services/db"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/debugger/source"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"golang.org/x/sync/errgroup"

	// This is necessary for compatibility with enterprise features
	_ "github.com/rudderlabs/rudder-server/imports"
)

//GatewayApp is the type for Gateway type implemention
type GatewayApp struct {
	App            app.Interface
	VersionHandler func(w http.ResponseWriter, r *http.Request)
}

func (*GatewayApp) GetAppType() string {
	return fmt.Sprintf("rudder-server-%s", app.GATEWAY)
}

func (gatewayApp *GatewayApp) StartRudderCore(ctx context.Context, options *app.Options) error {
	pkgLogger.Info("Gateway starting")

	rudderCoreDBValidator()
	rudderCoreWorkSpaceTableSetup()
	rudderCoreBaseSetup()

	pkgLogger.Info("Clearing DB ", options.ClearDB)

	sourcedebugger.Setup(backendconfig.DefaultBackendConfig)

	migrationMode := gatewayApp.App.Options().MigrationMode

	gatewayDB := jobsdb.NewForWrite(
		"gw",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithRetention(gwDBRetention),
		jobsdb.WithMigrationMode(migrationMode),
		jobsdb.WithStatusHandler(),
		jobsdb.WithQueryFilterKeys(jobsdb.QueryFiltersT{}),
	)
	defer gatewayDB.Close()
	gatewayDB.Start()
	defer gatewayDB.Stop()

	enableGateway := true
	if gatewayApp.App.Features().Migrator != nil {
		if migrationMode == db.IMPORT || migrationMode == db.EXPORT || migrationMode == db.IMPORT_EXPORT {
			enableGateway = (migrationMode != db.EXPORT)

			gatewayApp.App.Features().Migrator.PrepareJobsdbsForImport(gatewayDB, nil, nil)
		}
	}

	g, ctx := errgroup.WithContext(ctx)

	var modeProvider cluster.ChangeEventProvider

	deploymentType, err := deployment.GetFromEnv()
	if err != nil {
		return fmt.Errorf("failed to get deployment type: %v", err)
	}
	pkgLogger.Infof("Configured deployment type: %q", deploymentType)

	switch deploymentType {
	case deployment.MultiTenantType:
		pkgLogger.Info("using ETCD Based Dynamic Cluster Manager")
		modeProvider = state.NewETCDDynamicProvider()
	case deployment.HostedType, deployment.DedicatedType:
		pkgLogger.Info("using Static Cluster Manager")
		if enableProcessor && enableRouter {
			modeProvider = state.NewStaticProvider(servermode.NormalMode)
		} else {
			modeProvider = state.NewStaticProvider(servermode.DegradedMode)
		}
	default:
		return fmt.Errorf("unsupported deployment type: %q", deploymentType)
	}

	dm := cluster.Dynamic{
		Provider:         modeProvider,
		GatewayComponent: true,
	}
	g.Go(func() error {
		return dm.Run(ctx)
	})

	if enableGateway {
		var gw gateway.HandleT
		var rateLimiter ratelimiter.HandleT

		rateLimiter.SetUp()
		gw.SetReadonlyDBs(&readonlyGatewayDB, &readonlyRouterDB, &readonlyBatchRouterDB)
		gw.Setup(gatewayApp.App, backendconfig.DefaultBackendConfig, gatewayDB, &rateLimiter, gatewayApp.VersionHandler, rsources.NewNoOpService())
		defer gw.Shutdown()

		g.Go(func() error {
			return gw.StartAdminHandler(ctx)
		})
		g.Go(func() error {
			return gw.StartWebHandler(ctx)
		})
	}
	//go readIOforResume(router) //keeping it as input from IO, to be replaced by UI
	return g.Wait()
}

func (*GatewayApp) HandleRecovery(options *app.Options) {
	db.HandleNullRecovery(options.NormalMode, options.DegradedMode, options.MigrationMode, misc.AppStartTime, app.GATEWAY)
}
