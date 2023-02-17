package apphandlers

import (
	"context"
	"fmt"
	"net/http"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/app/cluster"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/gateway"
	"github.com/rudderlabs/rudder-server/jobsdb"
	ratelimiter "github.com/rudderlabs/rudder-server/rate-limiter"
	"github.com/rudderlabs/rudder-server/services/db"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/debugger/source"
	fileuploader "github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

// gatewayApp is the type for Gateway type implementation
type gatewayApp struct {
	setupDone      bool
	app            app.App
	versionHandler func(w http.ResponseWriter, r *http.Request)
	log            logger.Logger
	config         struct {
		gatewayDSLimit int
	}
}

func (a *gatewayApp) loadConfiguration() {
	config.RegisterIntConfigVariable(0, &a.config.gatewayDSLimit, true, 1, "Gateway.jobsDB.dsLimit", "JobsDB.dsLimit")
}

func (a *gatewayApp) Setup(options *app.Options) error {
	a.loadConfiguration()
	if err := db.HandleNullRecovery(options.NormalMode, options.DegradedMode, misc.AppStartTime, app.GATEWAY); err != nil {
		return err
	}
	if err := rudderCoreDBValidator(); err != nil {
		return err
	}
	if err := rudderCoreWorkSpaceTableSetup(); err != nil {
		return err
	}
	a.setupDone = true
	return nil
}

func (a *gatewayApp) StartRudderCore(ctx context.Context, options *app.Options) error {
	if !a.setupDone {
		return fmt.Errorf("gateway cannot start, database is not setup")
	}
	a.log.Info("Gateway starting")

	readonlyGatewayDB, err := setupReadonlyDBs()
	if err != nil {
		return err
	}

	deploymentType, err := deployment.GetFromEnv()
	if err != nil {
		return fmt.Errorf("failed to get deployment type: %v", err)
	}

	a.log.Infof("Configured deployment type: %q", deploymentType)
	a.log.Info("Clearing DB ", options.ClearDB)

	sourceHandle, err := sourcedebugger.NewHandle(backendconfig.DefaultBackendConfig)
	if err != nil {
		return err
	}
	defer sourceHandle.Stop()

	fileUploaderProvider := fileuploader.NewProvider(ctx, backendconfig.DefaultBackendConfig)

	gatewayDB := jobsdb.NewForWrite(
		"gw",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithStatusHandler(),
		jobsdb.WithDSLimit(&a.config.gatewayDSLimit),
		jobsdb.WithFileUploaderProvider(fileUploaderProvider),
	)
	defer gatewayDB.Close()
	if err := gatewayDB.Start(); err != nil {
		return fmt.Errorf("could not start gatewayDB: %w", err)
	}
	defer gatewayDB.Stop()

	g, ctx := errgroup.WithContext(ctx)

	modeProvider, err := resolveModeProvider(a.log, deploymentType)
	if err != nil {
		return err
	}

	dm := cluster.Dynamic{
		Provider:         modeProvider,
		GatewayComponent: true,
	}
	g.Go(func() error {
		return dm.Run(ctx)
	})

	var gw gateway.HandleT
	var rateLimiter ratelimiter.HandleT

	rateLimiter.SetUp()
	gw.SetReadonlyDB(readonlyGatewayDB)
	rsourcesService, err := NewRsourcesService(deploymentType)
	if err != nil {
		return err
	}
	err = gw.Setup(
		ctx,
		a.app, backendconfig.DefaultBackendConfig, gatewayDB,
		&rateLimiter, a.versionHandler, rsourcesService, sourceHandle,
	)
	if err != nil {
		return fmt.Errorf("failed to setup gateway: %w", err)
	}
	defer func() {
		if err := gw.Shutdown(); err != nil {
			a.log.Warnf("Gateway shutdown error: %v", err)
		}
	}()

	g.Go(func() error {
		return gw.StartAdminHandler(ctx)
	})
	g.Go(func() error {
		return gw.StartWebHandler(ctx)
	})
	return g.Wait()
}
