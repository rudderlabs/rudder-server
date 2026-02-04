package apphandlers

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"time"

	"github.com/rudderlabs/rudder-schemas/go/stream"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/app/cluster"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/gateway"
	gwThrottler "github.com/rudderlabs/rudder-server/gateway/throttler"
	drain_config "github.com/rudderlabs/rudder-server/internal/drain-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/debugger/source"
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
}

func (a *gatewayApp) Setup() error {
	if err := rudderCoreDBValidator(); err != nil {
		return err
	}
	if err := rudderCoreNodeSetup(); err != nil {
		return err
	}
	a.setupDone = true
	return nil
}

func (a *gatewayApp) StartRudderCore(ctx context.Context, _ func(), options *app.Options) error {
	config := config.Default
	statsFactory := stats.Default
	if !a.setupDone {
		return fmt.Errorf("gateway cannot start, database is not setup")
	}
	a.log.Infon("Gateway starting")

	deploymentType, err := deployment.GetFromEnv()
	if err != nil {
		return fmt.Errorf("failed to get deployment type: %v", err)
	}

	a.log.Infon("Configured deployment type", logger.NewStringField("deploymentType", string(deploymentType)))
	a.log.Infon("Clearing DB", logger.NewBoolField("clearDB", options.ClearDB))

	sourceHandle, err := sourcedebugger.NewHandle(backendconfig.DefaultBackendConfig)
	if err != nil {
		return err
	}
	defer sourceHandle.Stop()

	var jobsdbPool *sql.DB
	if config.GetBoolVar(true, "DB.gateway.Pool.enabled", "DB.Pool.enabled") {
		jobsdbPool, err = misc.NewDatabaseConnectionPool(ctx, "gateway", misc.DatabaseConnectionPoolConfig{
			MaxOpenConns:    config.GetReloadableIntVar(20, 1, "DB.gateway.Pool.maxOpenConnections", "DB.Pool.maxOpenConnections"),
			MaxIdleConns:    config.GetReloadableIntVar(5, 1, "DB.gateway.Pool.maxIdleConnections", "DB.Pool.maxIdleConnections"),
			ConnMaxIdleTime: config.GetReloadableDurationVar(15, time.Minute, "DB.gateway.Pool.maxIdleTime", "DB.Pool.maxIdleTime"),
			ConnMaxLifetime: config.GetReloadableDurationVar(0, time.Second, "DB.gateway.Pool.maxConnLifetime", "DB.Pool.maxConnLifetime"),
			UpdateInterval:  config.GetDurationVar(60, time.Second, "DB.gateway.Pool.updateInterval", "DB.Pool.updateInterval"),
		}, config, statsFactory)
		if err != nil {
			return err
		}
		defer jobsdbPool.Close()
	}
	partitionCount := config.GetIntVar(0, 1, "JobsDB.partitionCount")

	gwWOHandle := jobsdb.NewForWrite(
		"gw",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithSkipMaintenanceErr(config.GetBool("Gateway.jobsDB.skipMaintenanceError", true)),
		jobsdb.WithStats(statsFactory),
		jobsdb.WithDBHandle(jobsdbPool),
		jobsdb.WithNumPartitions(partitionCount),
	)
	defer gwWOHandle.Close()
	var gwWODB jobsdb.JobsDB = gwWOHandle

	if err := gwWODB.Start(); err != nil {
		return fmt.Errorf("could not start gatewayDB: %w", err)
	}
	defer func() {
		// wrapping Stop call in an anonymous function
		// so that we can decorate gwWODB later for partition migrations
		// and call Stop on the decorated instance
		gwWODB.Stop()
	}()

	g, ctx := errgroup.WithContext(ctx)

	modeProvider, err := resolveModeProvider(a.log, deploymentType)
	if err != nil {
		return fmt.Errorf("resolving mode provider: %w", err)
	}
	partitionMigrator, gwDB, err := setupGatewayPartitionMigrator(ctx, jobsdbPool, config, statsFactory, gwWODB, modeProvider.EtcdClient)
	if err != nil {
		return fmt.Errorf("setting up partition migrator: %w", err)
	}
	gwWODB = gwDB
	if err := partitionMigrator.Start(); err != nil {
		return fmt.Errorf("starting partition migrator: %w", err)
	}
	defer partitionMigrator.Stop()

	dm := cluster.Dynamic{Provider: modeProvider, GatewayComponent: true}
	g.Go(func() error {
		return dm.Run(ctx)
	})

	var gw gateway.Handle
	rateLimiter, err := gwThrottler.New(statsFactory)
	if err != nil {
		return fmt.Errorf("failed to create rate limiter: %w", err)
	}
	rsourcesService, err := NewRsourcesService(deploymentType, false, statsFactory)
	if err != nil {
		return err
	}
	transformerFeaturesService := transformer.NewFeaturesService(ctx, config, transformer.FeaturesServiceOptions{
		PollInterval:             config.GetDuration("Transformer.pollInterval", 10, time.Second),
		TransformerURL:           config.GetString("DEST_TRANSFORM_URL", "http://localhost:9090"),
		FeaturesRetryMaxAttempts: 10,
	})
	drainConfigManager, err := drain_config.NewDrainConfigManager(config, a.log.Child("drain-config"), statsFactory)
	if err != nil {
		a.log.Errorn("drain config manager setup failed while starting gateway", obskit.Error(err))
	}

	drainConfigHttpHandler := drain_config.ErrorResponder("unable to start drain config http handler")
	if drainConfigManager != nil {
		defer drainConfigManager.Stop()
		drainConfigHttpHandler = drainConfigManager.DrainConfigHttpHandler()
	}
	streamMsgValidator := stream.NewMessageValidator()
	err = gw.Setup(ctx, config, logger.NewLogger().Child("gateway"), statsFactory, a.app, backendconfig.DefaultBackendConfig,
		gwWODB, rateLimiter, a.versionHandler, rsourcesService, transformerFeaturesService, sourceHandle,
		streamMsgValidator, gateway.WithInternalHttpHandlers(
			map[string]http.Handler{
				"/drain": drainConfigHttpHandler,
			},
		))
	if err != nil {
		return fmt.Errorf("failed to setup gateway: %w", err)
	}
	defer func() {
		if err := gw.Shutdown(); err != nil {
			a.log.Warnn("Gateway shutdown error", obskit.Error(err))
		}
	}()

	g.Go(func() error {
		return gw.StartWebHandler(ctx)
	})
	return g.Wait()
}
