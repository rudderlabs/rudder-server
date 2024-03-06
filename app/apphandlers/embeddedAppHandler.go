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
	"github.com/rudderlabs/rudder-server/archiver"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/gateway"
	gwThrottler "github.com/rudderlabs/rudder-server/gateway/throttler"
	drain_config "github.com/rudderlabs/rudder-server/internal/drain-config"
	"github.com/rudderlabs/rudder-server/internal/pulsar"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/jobsdb/prebackup"
	"github.com/rudderlabs/rudder-server/processor"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	routerManager "github.com/rudderlabs/rudder-server/router/manager"
	rtThrottler "github.com/rudderlabs/rudder-server/router/throttler"
	schema_forwarder "github.com/rudderlabs/rudder-server/schema-forwarder"
	"github.com/rudderlabs/rudder-server/services/db"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/debugger/source"
	transformationdebugger "github.com/rudderlabs/rudder-server/services/debugger/transformation"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/services/transientsource"
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
	conf           *config.Config
	config         struct {
		enableReplay       bool
		processorDSLimit   misc.ValueLoader[int]
		routerDSLimit      misc.ValueLoader[int]
		batchRouterDSLimit misc.ValueLoader[int]
		gatewayDSLimit     misc.ValueLoader[int]
	}
}

func (a *embeddedApp) Setup(options *app.Options) error {
	a.config.enableReplay = a.conf.GetBoolVar(types.DefaultReplayEnabled, "Replay.enabled")
	a.config.processorDSLimit = a.conf.GetReloadableIntVar(0, 1, "Processor.jobsDB.dsLimit", "JobsDB.dsLimit")
	a.config.gatewayDSLimit = a.conf.GetReloadableIntVar(0, 1, "Gateway.jobsDB.dsLimit", "JobsDB.dsLimit")
	a.config.routerDSLimit = a.conf.GetReloadableIntVar(0, 1, "Router.jobsDB.dsLimit", "JobsDB.dsLimit")
	a.config.batchRouterDSLimit = a.conf.GetReloadableIntVar(0, 1, "BatchRouter.jobsDB.dsLimit", "JobsDB.dsLimit")

	if a.conf.GetBool("recovery.enabled", true) {
		if err := db.HandleEmbeddedRecovery(options.NormalMode, options.DegradedMode, misc.AppStartTime, app.EMBEDDED); err != nil {
			return err
		}
	}

	if err := rudderCoreDBValidator(a.conf); err != nil {
		return err
	}
	if err := rudderCoreWorkSpaceTableSetup(a.conf); err != nil {
		return err
	}
	if err := rudderCoreNodeSetup(a.conf); err != nil {
		return err
	}
	a.setupDone = true
	return nil
}

func (a *embeddedApp) StartRudderCore(ctx context.Context, options *app.Options) error {
	if !a.setupDone {
		return fmt.Errorf("embedded rudder core cannot start, database is not setup")
	}
	a.log.Info("Embedded mode: Starting Rudder Core")
	g, ctx := errgroup.WithContext(ctx)
	terminalErrFn := terminalErrorFunction(ctx, g)

	deploymentType, err := deployment.GetType(a.conf)
	if err != nil {
		return fmt.Errorf("failed to get deployment type: %w", err)
	}
	a.log.Infof("Configured deployment type: %q", deploymentType)

	reporting := a.app.Features().Reporting.Setup(ctx, backendconfig.DefaultBackendConfig)
	defer reporting.Stop()
	syncer := reporting.DatabaseSyncer(types.SyncerConfig{ConnInfo: misc.GetConnectionString(a.conf, "reporting")})
	g.Go(func() error {
		syncer()
		return nil
	})

	a.log.Info("Clearing DB ", options.ClearDB)

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
	prebackupHandlers := []prebackup.Handler{
		prebackup.DropSourceIds(transientSources.SourceIdsSupplier()),
	}

	fileUploaderProvider := fileuploader.NewProvider(ctx, backendconfig.DefaultBackendConfig)

	rsourcesService, err := NewRsourcesService(a.conf, deploymentType, true)
	if err != nil {
		return err
	}

	transformerFeaturesService := transformer.NewFeaturesService(ctx, transformer.FeaturesServiceConfig{
		PollInterval:             a.conf.GetDuration("Transformer.pollInterval", 1, time.Second),
		TransformerURL:           a.conf.GetString("DEST_TRANSFORM_URL", "http://localhost:9090"),
		FeaturesRetryMaxAttempts: 10,
	})

	// This separate gateway db is created just to be used with gateway because in case of degraded mode,
	// the earlier created gwDb (which was created to be used mainly with processor) will not be running, and it
	// will cause issues for gateway because gateway is supposed to receive jobs even in degraded mode.
	gatewayDB := jobsdb.NewForWrite(
		"gw",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithConfig(a.conf),
	)
	if err = gatewayDB.Start(); err != nil {
		return fmt.Errorf("could not start gateway: %w", err)
	}
	defer gatewayDB.Stop()

	// This gwDBForProcessor should only be used by processor as this is supposed to be stopped and started with the
	// Processor.
	gwDBForProcessor := jobsdb.NewForRead(
		"gw",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithPreBackupHandlers(prebackupHandlers),
		jobsdb.WithDSLimit(a.config.gatewayDSLimit),
		jobsdb.WithFileUploaderProvider(fileUploaderProvider),
		jobsdb.WithSkipMaintenanceErr(a.conf.GetBool("Gateway.jobsDB.skipMaintenanceError", true)),
		jobsdb.WithConfig(a.conf),
	)
	defer gwDBForProcessor.Close()
	routerDB := jobsdb.NewForReadWrite(
		"rt",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithPreBackupHandlers(prebackupHandlers),
		jobsdb.WithDSLimit(a.config.routerDSLimit),
		jobsdb.WithFileUploaderProvider(fileUploaderProvider),
		jobsdb.WithSkipMaintenanceErr(a.conf.GetBool("Router.jobsDB.skipMaintenanceError", false)),
		jobsdb.WithConfig(a.conf),
	)
	defer routerDB.Close()
	batchRouterDB := jobsdb.NewForReadWrite(
		"batch_rt",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithPreBackupHandlers(prebackupHandlers),
		jobsdb.WithDSLimit(a.config.batchRouterDSLimit),
		jobsdb.WithFileUploaderProvider(fileUploaderProvider),
		jobsdb.WithSkipMaintenanceErr(a.conf.GetBool("BatchRouter.jobsDB.skipMaintenanceError", false)),
		jobsdb.WithConfig(a.conf),
	)
	defer batchRouterDB.Close()

	// We need two errorDBs, one in read & one in write mode to support separate gateway to store failures
	errDBForRead := jobsdb.NewForRead(
		"proc_error",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithPreBackupHandlers(prebackupHandlers),
		jobsdb.WithDSLimit(a.config.processorDSLimit),
		jobsdb.WithFileUploaderProvider(fileUploaderProvider),
		jobsdb.WithSkipMaintenanceErr(a.conf.GetBool("Processor.jobsDB.skipMaintenanceError", false)),
		jobsdb.WithConfig(a.conf),
	)
	defer errDBForRead.Close()
	errDBForWrite := jobsdb.NewForWrite(
		"proc_error",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithSkipMaintenanceErr(a.conf.GetBool("Processor.jobsDB.skipMaintenanceError", true)),
		jobsdb.WithConfig(a.conf),
	)
	if err = errDBForWrite.Start(); err != nil {
		return fmt.Errorf("could not start errDBForWrite: %w", err)
	}
	defer errDBForWrite.Stop()

	schemaDB := jobsdb.NewForReadWrite(
		"esch",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithDSLimit(a.config.processorDSLimit),
		jobsdb.WithSkipMaintenanceErr(a.conf.GetBool("Processor.jobsDB.skipMaintenanceError", false)),
		jobsdb.WithConfig(a.conf),
	)
	defer schemaDB.Close()

	archivalDB := jobsdb.NewForReadWrite(
		"arc",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithDSLimit(a.config.processorDSLimit),
		jobsdb.WithSkipMaintenanceErr(a.conf.GetBool("Processor.jobsDB.skipMaintenanceError", false)),
		jobsdb.WithJobMaxAge(
			func() time.Duration {
				return a.conf.GetDuration("archival.jobRetention", 24, time.Hour)
			},
		),
		jobsdb.WithConfig(a.conf),
	)
	defer archivalDB.Close()

	var schemaForwarder schema_forwarder.Forwarder
	if a.conf.GetBool("EventSchemas2.enabled", false) {
		client, err := pulsar.NewClient(a.conf)
		if err != nil {
			return err
		}
		defer client.Close()
		schemaForwarder = schema_forwarder.NewForwarder(terminalErrFn, schemaDB, &client, backendconfig.DefaultBackendConfig, logger.NewLogger().Child("jobs_forwarder"), a.conf, stats.Default)
	} else {
		schemaForwarder = schema_forwarder.NewAbortingForwarder(terminalErrFn, schemaDB, logger.NewLogger().Child("jobs_forwarder"), a.conf, stats.Default)
	}

	modeProvider, err := resolveModeProvider(a.conf, a.log, deploymentType)
	if err != nil {
		return err
	}

	adaptiveLimit := payload.SetupAdaptiveLimiter(ctx, g, a.conf)

	enrichers, err := setupPipelineEnrichers(a.conf, a.log, stats.Default)
	if err != nil {
		return fmt.Errorf("setting up pipeline enrichers: %w", err)
	}

	defer func() {
		for _, enricher := range enrichers {
			enricher.Close()
		}
	}()

	proc := processor.New(
		ctx,
		a.conf,
		&options.ClearDB,
		gwDBForProcessor,
		routerDB,
		batchRouterDB,
		errDBForRead,
		errDBForWrite,
		schemaDB,
		archivalDB,
		reporting,
		transientSources,
		fileUploaderProvider,
		rsourcesService,
		transformerFeaturesService,
		destinationHandle,
		transformationhandle,
		enrichers,
		processor.WithAdaptiveLimit(adaptiveLimit),
	)
	throttlerFactory, err := rtThrottler.NewFactory(a.conf, stats.Default)
	if err != nil {
		return fmt.Errorf("failed to create rt throttler factory: %w", err)
	}
	rtFactory := &router.Factory{
		Logger:                     logger.NewLogger().Child("router"),
		Conf:                       a.conf,
		Reporting:                  reporting,
		BackendConfig:              backendconfig.DefaultBackendConfig,
		RouterDB:                   routerDB,
		ProcErrorDB:                errDBForWrite,
		TransientSources:           transientSources,
		RsourcesService:            rsourcesService,
		TransformerFeaturesService: transformerFeaturesService,
		ThrottlerFactory:           throttlerFactory,
		Debugger:                   destinationHandle,
		AdaptiveLimit:              adaptiveLimit,
	}
	brtFactory := &batchrouter.Factory{
		Conf:             a.conf,
		Reporting:        reporting,
		BackendConfig:    backendconfig.DefaultBackendConfig,
		RouterDB:         batchRouterDB,
		ProcErrorDB:      errDBForWrite,
		TransientSources: transientSources,
		RsourcesService:  rsourcesService,
		Debugger:         destinationHandle,
		AdaptiveLimit:    adaptiveLimit,
	}
	rt := routerManager.New(rtFactory, brtFactory, backendconfig.DefaultBackendConfig, logger.NewLogger())

	dm := cluster.Dynamic{
		Provider:        modeProvider,
		GatewayDB:       gwDBForProcessor,
		RouterDB:        routerDB,
		BatchRouterDB:   batchRouterDB,
		ErrorDB:         errDBForRead,
		EventSchemaDB:   schemaDB,
		ArchivalDB:      archivalDB,
		Processor:       proc,
		Router:          rt,
		SchemaForwarder: schemaForwarder,
		Archiver: archiver.New(
			archivalDB,
			fileUploaderProvider,
			a.conf,
			stats.Default,
			archiver.WithAdaptiveLimit(adaptiveLimit),
		),
	}

	rateLimiter, err := gwThrottler.New(stats.Default)
	if err != nil {
		return fmt.Errorf("failed to create gw rate limiter: %w", err)
	}
	drainConfigManager, err := drain_config.NewDrainConfigManager(a.conf, a.log.Child("drain-config"))
	if err != nil {
		return fmt.Errorf("drain config manager setup: %v", err)
	}
	defer drainConfigManager.Stop()
	g.Go(misc.WithBugsnag(func() (err error) {
		return drainConfigManager.DrainConfigRoutine(ctx)
	}))
	g.Go(misc.WithBugsnag(func() (err error) {
		return drainConfigManager.CleanupRoutine(ctx)
	}))
	gw := gateway.Handle{}
	err = gw.Setup(
		ctx,
		a.conf, logger.NewLogger().Child("gateway"), stats.Default,
		a.app, backendconfig.DefaultBackendConfig, gatewayDB, errDBForWrite,
		rateLimiter, a.versionHandler, rsourcesService, transformerFeaturesService, sourceHandle,
		gateway.WithInternalHttpHandlers(
			map[string]http.Handler{
				"/drain": drainConfigManager.DrainConfigHttpHandler(),
			},
		),
	)
	if err != nil {
		return fmt.Errorf("could not setup gateway: %w", err)
	}
	defer func() {
		if err := gw.Shutdown(); err != nil {
			a.log.Warnf("Gateway shutdown error: %v", err)
		}
	}()

	g.Go(func() error {
		return gw.StartWebHandler(ctx)
	})
	if a.config.enableReplay {
		var replayDB jobsdb.Handle
		err := replayDB.Setup(
			a.conf,
			jobsdb.ReadWrite, options.ClearDB, "replay",
			prebackupHandlers, fileUploaderProvider,
		)
		if err != nil {
			return fmt.Errorf("could not setup replayDB: %w", err)
		}
		replay, err := a.app.Features().Replay.Setup(ctx, a.conf, &replayDB, gatewayDB, routerDB, batchRouterDB)
		if err != nil {
			return err
		}
		if err := replay.Start(); err != nil {
			return fmt.Errorf("could not start replay: %w", err)
		}
		defer func() { _ = replay.Stop() }()
	}

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
		replicationLagStat := stats.Default.NewStat("rsources_log_replication_lag", stats.GaugeType)
		replicationSlotStat := stats.Default.NewStat("rsources_log_replication_slot", stats.GaugeType)
		rsourcesService.Monitor(ctx, replicationLagStat, replicationSlotStat)
		return nil
	})

	return g.Wait()
}
