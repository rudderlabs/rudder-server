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
	"github.com/rudderlabs/rudder-server/archiver"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/gateway"
	gwThrottler "github.com/rudderlabs/rudder-server/gateway/throttler"
	drain_config "github.com/rudderlabs/rudder-server/internal/drain-config"
	"github.com/rudderlabs/rudder-server/internal/pulsar"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/jobsdb/bench"
	"github.com/rudderlabs/rudder-server/processor"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	routerManager "github.com/rudderlabs/rudder-server/router/manager"
	rtThrottler "github.com/rudderlabs/rudder-server/router/throttler"
	schema_forwarder "github.com/rudderlabs/rudder-server/schema-forwarder"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/debugger/source"
	transformationdebugger "github.com/rudderlabs/rudder-server/services/debugger/transformation"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/services/rmetrics"
	"github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/crash"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/payload"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

// embeddedApp is the type for embedded type implementation
type embeddedApp struct {
	setupDone      bool
	app            app.App
	versionHandler func(w http.ResponseWriter, r *http.Request)
	log            logger.Logger
	config         struct {
		eschDSLimit    config.ValueLoader[int]
		arcDSLimit     config.ValueLoader[int]
		rtDSLimit      config.ValueLoader[int]
		batchrtDSLimit config.ValueLoader[int]
		gwDSLimit      config.ValueLoader[int]
	}
}

func (a *embeddedApp) Setup() error {
	a.config.gwDSLimit = config.GetReloadableIntVar(0, 1, "JobsDB.gw.dsLimit", "Gateway.jobsDB.dsLimit", "JobsDB.dsLimit")
	a.config.rtDSLimit = config.GetReloadableIntVar(0, 1, "JobsDB.rt.dsLimit", "Router.jobsDB.dsLimit", "JobsDB.dsLimit")
	a.config.batchrtDSLimit = config.GetReloadableIntVar(0, 1, "JobsDB.batch_rt.dsLimit", "BatchRouter.jobsDB.dsLimit", "JobsDB.dsLimit")
	a.config.eschDSLimit = config.GetReloadableIntVar(0, 1, "JobsDB.esch.dsLimit", "Processor.jobsDB.dsLimit", "JobsDB.dsLimit")
	a.config.arcDSLimit = config.GetReloadableIntVar(0, 1, "JobsDB.arc.dsLimit", "Processor.jobsDB.dsLimit", "JobsDB.dsLimit")
	if err := rudderCoreDBValidator(); err != nil {
		return err
	}
	if err := rudderCoreNodeSetup(); err != nil {
		return err
	}
	a.setupDone = true
	return nil
}

func (a *embeddedApp) StartRudderCore(ctx context.Context, shutdownFn func(), options *app.Options) error {
	config := config.Default
	statsFactory := stats.Default

	if !a.setupDone {
		return fmt.Errorf("embedded rudder core cannot start, database is not setup")
	}
	a.log.Infon("Embedded mode: Starting Rudder Core")
	g, ctx := errgroup.WithContext(ctx)
	terminalErrFn := terminalErrorFunction(ctx, g)

	deploymentType, err := deployment.GetFromEnv()
	if err != nil {
		return fmt.Errorf("failed to get deployment type: %w", err)
	}
	a.log.Infon("Configured deployment type", logger.NewStringField("deploymentType", string(deploymentType)))

	trackedUsersReporter, err := a.app.Features().TrackedUsers.Setup(config)
	if err != nil {
		return fmt.Errorf("could not setup tracked users: %w", err)
	}
	err = trackedUsersReporter.MigrateDatabase(misc.GetConnectionString(config, "tracked_users"), config)
	if err != nil {
		return fmt.Errorf("could not run tracked users database migration: %w", err)
	}
	reporting := a.app.Features().Reporting.Setup(ctx, config, backendconfig.DefaultBackendConfig)
	defer reporting.Stop()
	syncer := reporting.DatabaseSyncer(types.SyncerConfig{ConnInfo: misc.GetConnectionString(config, "reporting")})
	g.Go(func() error {
		syncer()
		return nil
	})

	a.log.Infon("Clearing DB", logger.NewBoolField("clearDB", options.ClearDB))

	transformationhandle, err := transformationdebugger.NewHandle(backendconfig.DefaultBackendConfig)
	if err != nil {
		return err
	}
	defer transformationhandle.Stop()
	destinationHandle, err := destinationdebugger.NewHandle(backendconfig.DefaultBackendConfig)
	if err != nil {
		return err
	}
	defer destinationHandle.Stop()
	sourceHandle, err := sourcedebugger.NewHandle(backendconfig.DefaultBackendConfig)
	if err != nil {
		return err
	}
	defer sourceHandle.Stop()

	transientSources := transientsource.NewService(ctx, backendconfig.DefaultBackendConfig)

	fileUploaderProvider := fileuploader.NewProvider(ctx, backendconfig.DefaultBackendConfig)

	rsourcesService, err := NewRsourcesService(deploymentType, true, statsFactory)
	if err != nil {
		return err
	}

	transformerFeaturesService := transformer.NewFeaturesService(ctx, config, transformer.FeaturesServiceOptions{
		PollInterval:             config.GetDuration("Transformer.pollInterval", 10, time.Second),
		TransformerURL:           config.GetString("DEST_TRANSFORM_URL", "http://localhost:9090"),
		FeaturesRetryMaxAttempts: 10,
	})

	var dbPool *sql.DB
	if config.GetBoolVar(true, "db.pool.shared") {
		dbPool, err = misc.NewDatabaseConnectionPool(ctx, config, statsFactory, "embedded-app")
		if err != nil {
			return err
		}
		defer dbPool.Close()
	}
	partitionCount := config.GetIntVar(0, 1, "JobsDB.partitionCount")

	pendingEventsRegistry := rmetrics.NewPendingEventsRegistry()

	// This separate gateway db is created just to be used with gateway because in case of degraded mode,
	// the earlier created gwDb (which was created to be used mainly with processor) will not be running, and it
	// will cause issues for gateway because gateway is supposed to receive jobs even in degraded mode.
	gwWOHandle := jobsdb.NewForWrite(
		"gw",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithStats(statsFactory),
		jobsdb.WithDBHandle(dbPool),
		jobsdb.WithNumPartitions(partitionCount),
	)
	defer gwWOHandle.Close()
	if err = gwWOHandle.Start(); err != nil {
		return fmt.Errorf("could not start gateway: %w", err)
	}
	defer gwWOHandle.Stop()
	var gwWODB jobsdb.JobsDB = gwWOHandle

	gwROHandle := jobsdb.NewForRead(
		"gw",
		jobsdb.WithDSLimit(a.config.gwDSLimit),
		jobsdb.WithSkipMaintenanceErr(config.GetBool("Gateway.jobsDB.skipMaintenanceError", true)),
		jobsdb.WithStats(statsFactory),
		jobsdb.WithDBHandle(dbPool),
		jobsdb.WithNumPartitions(partitionCount),
	)
	defer gwROHandle.Close()
	var gwRODB jobsdb.JobsDB = gwROHandle

	rtRWHandle := jobsdb.NewForReadWrite(
		"rt",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithDSLimit(a.config.rtDSLimit),
		jobsdb.WithSkipMaintenanceErr(config.GetBool("Router.jobsDB.skipMaintenanceError", false)),
		jobsdb.WithStats(statsFactory),
		jobsdb.WithDBHandle(dbPool),
		jobsdb.WithNumPartitions(partitionCount),
	)
	defer rtRWHandle.Close()
	rtRWDB := jobsdb.NewPendingEventsJobsDB(rtRWHandle, pendingEventsRegistry)

	brtRWHandle := jobsdb.NewForReadWrite(
		"batch_rt",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithDSLimit(a.config.batchrtDSLimit),
		jobsdb.WithSkipMaintenanceErr(config.GetBool("BatchRouter.jobsDB.skipMaintenanceError", false)),
		jobsdb.WithStats(statsFactory),
		jobsdb.WithDBHandle(dbPool),
		jobsdb.WithNumPartitions(partitionCount),
	)
	defer brtRWHandle.Close()
	brtRWDB := jobsdb.NewPendingEventsJobsDB(brtRWHandle, pendingEventsRegistry)

	eschRWDB := jobsdb.NewForReadWrite(
		"esch",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithDSLimit(a.config.eschDSLimit),
		jobsdb.WithSkipMaintenanceErr(config.GetBool("Processor.jobsDB.skipMaintenanceError", false)),
		jobsdb.WithStats(statsFactory),
		jobsdb.WithDBHandle(dbPool),
	)
	defer eschRWDB.Close()

	arcRWDB := jobsdb.NewForReadWrite(
		"arc",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithDSLimit(a.config.arcDSLimit),
		jobsdb.WithSkipMaintenanceErr(config.GetBool("Processor.jobsDB.skipMaintenanceError", false)),
		jobsdb.WithStats(statsFactory),
		jobsdb.WithJobMaxAge(config.GetReloadableDurationVar(24, time.Hour, "archival.jobRetention")),
		jobsdb.WithDBHandle(dbPool),
	)
	defer arcRWDB.Close()

	var schemaForwarder schema_forwarder.Forwarder
	if config.GetBool("EventSchemas2.enabled", false) {
		client, err := pulsar.NewClient(config)
		if err != nil {
			return err
		}
		defer client.Close()
		schemaForwarder = schema_forwarder.NewForwarder(terminalErrFn, eschRWDB, &client, backendconfig.DefaultBackendConfig, logger.NewLogger().Child("jobs_forwarder"), config, statsFactory)
	} else {
		schemaForwarder = schema_forwarder.NewAbortingForwarder(terminalErrFn, eschRWDB, logger.NewLogger().Child("jobs_forwarder"), config, statsFactory)
	}

	modeProvider, err := resolveModeProvider(a.log, deploymentType)
	if err != nil {
		return err
	}

	// setup partition migrator
	ppmSetup, err := setupProcessorPartitionMigrator(ctx, shutdownFn, dbPool,
		config, statsFactory,
		gwRODB, gwWODB,
		rtRWDB, brtRWDB,
		modeProvider.EtcdClient,
	)
	defer ppmSetup.Finally() // always run finally to clean up resources regardless of error
	if err != nil {
		return fmt.Errorf("setting up partition migrator: %w", err)
	}
	partitionMigrator := ppmSetup.PartitionMigrator
	gwWODB = ppmSetup.GwDB
	rtRWDB = ppmSetup.RtDB
	brtRWDB = ppmSetup.BrtDB

	adaptiveLimit := payload.SetupAdaptiveLimiter(ctx, g)

	enrichers, err := setupPipelineEnrichers(config, a.log, statsFactory)
	if err != nil {
		return fmt.Errorf("setting up pipeline enrichers: %w", err)
	}

	defer func() {
		for _, enricher := range enrichers {
			_ = enricher.Close()
		}
	}()

	proc := processor.New(
		ctx,
		&options.ClearDB,
		gwRODB,
		rtRWDB,
		brtRWDB,
		eschRWDB,
		arcRWDB,
		reporting,
		transientSources,
		fileUploaderProvider,
		rsourcesService,
		transformerFeaturesService,
		destinationHandle,
		transformationhandle,
		enrichers,
		trackedUsersReporter,
		pendingEventsRegistry,
		processor.WithAdaptiveLimit(adaptiveLimit),
	)
	routerLogger := logger.NewLogger().Child("router")
	throttlerFactory, err := rtThrottler.NewFactory(config, statsFactory, routerLogger.Child("throttler"))
	if err != nil {
		return fmt.Errorf("failed to create rt throttler factory: %w", err)
	}
	rtFactory := &router.Factory{
		Logger:        routerLogger,
		Reporting:     reporting,
		BackendConfig: backendconfig.DefaultBackendConfig,
		RouterDB: jobsdb.NewCachingDistinctParameterValuesJobsdb( // using a cache so that multiple routers can share the same cache and not hit the DB every time
			config.GetReloadableDurationVar(1, time.Second, "JobsDB.rt.parameterValuesCacheTtl", "JobsDB.parameterValuesCacheTtl"),
			rtRWDB,
		),
		TransientSources:           transientSources,
		RsourcesService:            rsourcesService,
		TransformerFeaturesService: transformerFeaturesService,
		ThrottlerFactory:           throttlerFactory,
		Debugger:                   destinationHandle,
		AdaptiveLimit:              adaptiveLimit,
	}
	brtFactory := &batchrouter.Factory{
		Reporting:     reporting,
		BackendConfig: backendconfig.DefaultBackendConfig,
		RouterDB: jobsdb.NewCachingDistinctParameterValuesJobsdb( // using a cache so that multiple batch routers can share the same cache and not hit the DB every time
			config.GetReloadableDurationVar(1, time.Second, "JobsDB.rt.parameterValuesCacheTtl", "JobsDB.parameterValuesCacheTtl"),
			brtRWDB,
		),
		TransientSources: transientSources,
		RsourcesService:  rsourcesService,
		Debugger:         destinationHandle,
		AdaptiveLimit:    adaptiveLimit,
	}
	rt := routerManager.New(rtFactory, brtFactory, backendconfig.DefaultBackendConfig, logger.NewLogger())

	dm := cluster.Dynamic{
		Provider:          modeProvider,
		GatewayDB:         gwRODB,
		RouterDB:          rtRWDB,
		BatchRouterDB:     brtRWDB,
		EventSchemaDB:     eschRWDB,
		ArchivalDB:        arcRWDB,
		PartitionMigrator: partitionMigrator,
		Processor:         proc,
		Router:            rt,
		SchemaForwarder:   schemaForwarder,
		Archiver: archiver.New(
			arcRWDB,
			fileUploaderProvider,
			config,
			statsFactory,
			archiver.WithAdaptiveLimit(adaptiveLimit),
		),
	}

	rateLimiter, err := gwThrottler.New(statsFactory)
	if err != nil {
		return fmt.Errorf("failed to create gw rate limiter: %w", err)
	}
	drainConfigManager, err := drain_config.NewDrainConfigManager(config, a.log.Child("drain-config"), statsFactory)
	if err != nil {
		return fmt.Errorf("drain config manager setup: %v", err)
	}
	defer drainConfigManager.Stop()
	g.Go(crash.Wrapper(func() (err error) {
		return drainConfigManager.DrainConfigRoutine(ctx)
	}))
	g.Go(crash.Wrapper(func() (err error) {
		return drainConfigManager.CleanupRoutine(ctx)
	}))
	streamMsgValidator := stream.NewMessageValidator()
	gw := gateway.Handle{}
	err = gw.Setup(ctx, config, logger.NewLogger().Child("gateway"), statsFactory, a.app, backendconfig.DefaultBackendConfig,
		gwWODB, rateLimiter, a.versionHandler, rsourcesService, transformerFeaturesService, sourceHandle,
		streamMsgValidator, gateway.WithInternalHttpHandlers(
			map[string]http.Handler{
				"/drain": drainConfigManager.DrainConfigHttpHandler(),
			},
		))
	if err != nil {
		return fmt.Errorf("could not setup gateway: %w", err)
	}
	defer func() {
		if err := gw.Shutdown(); err != nil {
			a.log.Warnn("Gateway shutdown error", obskit.Error(err))
		}
	}()

	g.Go(func() error {
		return gw.StartWebHandler(ctx)
	})

	g.Go(func() error {
		// This should happen only after setupDatabaseTables() is called and journal table migrations are done
		// because if this start before that then there might be a case when ReadDB will try to read the owner table
		// which gets created after either Write or ReadWrite DB is created.
		return dm.Run(ctx)
	})

	g.Go(func() error {
		return rsourcesService.CleanupLoop(ctx)
	})

	g.Go(func() error {
		replicationLagStat := statsFactory.NewStat("rsources_log_replication_lag", stats.GaugeType)
		replicationSlotStat := statsFactory.NewStat("rsources_log_replication_slot", stats.GaugeType)
		rsourcesService.Monitor(ctx, replicationLagStat, replicationSlotStat)
		return nil
	})

	if config.GetBool("JobsDB.Bench.enabled", false) {
		g.Go(func() error {
			b, err := bench.New(config, statsFactory, a.log.Child("jobsdb.benchmark"), dbPool)
			if err != nil {
				return fmt.Errorf("creating jobsdb benchmarker: %w", err)
			}
			return b.Run(ctx)
		})
	}
	return g.Wait()
}
