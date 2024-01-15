package apphandlers

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/app/cluster"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/gateway"
	gwThrottler "github.com/rudderlabs/rudder-server/gateway/throttler"
	drain_config "github.com/rudderlabs/rudder-server/internal/drain-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/db"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/debugger/source"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/services/transformer"
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
		gatewayDSLimit misc.ValueLoader[int]
	}
}

func (a *gatewayApp) Setup(options *app.Options) error {
	a.config.gatewayDSLimit = config.GetReloadableIntVar(0, 1, "Gateway.jobsDB.dsLimit", "JobsDB.dsLimit")
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
	config := config.Default
	if !a.setupDone {
		return fmt.Errorf("gateway cannot start, database is not setup")
	}
	a.log.Info("Gateway starting")

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
		jobsdb.WithDSLimit(a.config.gatewayDSLimit),
		jobsdb.WithSkipMaintenanceErr(config.GetBool("Gateway.jobsDB.skipMaintenanceError", true)),
		jobsdb.WithFileUploaderProvider(fileUploaderProvider),
	)
	defer gatewayDB.Close()

	if err := gatewayDB.Start(); err != nil {
		return fmt.Errorf("could not start gatewayDB: %w", err)
	}
	defer gatewayDB.Stop()

	errDB := jobsdb.NewForWrite(
		"proc_error",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithSkipMaintenanceErr(config.GetBool("Gateway.jobsDB.skipMaintenanceError", true)),
	)
	defer errDB.Close()

	if err := errDB.Start(); err != nil {
		return fmt.Errorf("could not start errDB: %w", err)
	}
	defer errDB.Stop()

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

	var gw gateway.Handle
	rateLimiter, err := gwThrottler.New(stats.Default)
	if err != nil {
		return fmt.Errorf("failed to create rate limiter: %w", err)
	}
	rsourcesService, err := NewRsourcesService(deploymentType, false)
	if err != nil {
		return err
	}
	transformerFeaturesService := transformer.NewFeaturesService(ctx, transformer.FeaturesServiceConfig{
		PollInterval:             config.GetDuration("Transformer.pollInterval", 1, time.Second),
		TransformerURL:           config.GetString("DEST_TRANSFORM_URL", "http://localhost:9090"),
		FeaturesRetryMaxAttempts: 10,
	})
	drainConfigManager, err := drain_config.NewDrainConfigManager(config, a.log.Child("drain-config"))
	if err != nil {
		a.log.Errorw("drain config manager setup failed while starting gateway", "error", err)
	}

	drainConfigHttpHandler := drain_config.ErrorResponder("unable to start drain config http handler")
	if drainConfigManager != nil {
		defer drainConfigManager.Stop()
		drainConfigHttpHandler = drainConfigManager.DrainConfigHttpHandler()
	}

	err = gw.Setup(
		ctx,
		config, logger.NewLogger().Child("gateway"), stats.Default,
		a.app, backendconfig.DefaultBackendConfig, gatewayDB, errDB,
		rateLimiter, a.versionHandler, rsourcesService, transformerFeaturesService, sourceHandle,
		gateway.WithInternalHttpHandlers(
			map[string]http.Handler{
				"/drain": drainConfigHttpHandler,
			},
		),
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
		return gw.StartWebHandler(ctx)
	})
	return g.Wait()
}
