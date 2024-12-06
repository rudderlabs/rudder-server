package apphandlers

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/app/cluster"
	"github.com/rudderlabs/rudder-server/archiver"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	drain_config "github.com/rudderlabs/rudder-server/internal/drain-config"
	"github.com/rudderlabs/rudder-server/internal/pulsar"
	"github.com/rudderlabs/rudder-server/jobsdb"
	proc "github.com/rudderlabs/rudder-server/processor"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	routerManager "github.com/rudderlabs/rudder-server/router/manager"
	"github.com/rudderlabs/rudder-server/router/throttler"
	schema_forwarder "github.com/rudderlabs/rudder-server/schema-forwarder"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	transformationdebugger "github.com/rudderlabs/rudder-server/services/debugger/transformation"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/crash"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/payload"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

// processorApp is the type for Processor type implementation
type processorApp struct {
	setupDone      bool
	app            app.App
	versionHandler func(w http.ResponseWriter, r *http.Request)
	log            logger.Logger
	config         struct {
		processorDSLimit   config.ValueLoader[int]
		routerDSLimit      config.ValueLoader[int]
		batchRouterDSLimit config.ValueLoader[int]
		gatewayDSLimit     config.ValueLoader[int]
		http               struct {
			ReadTimeout       time.Duration
			ReadHeaderTimeout time.Duration
			WriteTimeout      time.Duration
			IdleTimeout       time.Duration
			webPort           int
			MaxHeaderBytes    int
		}
	}
}

func (a *processorApp) Setup() error {
	a.config.http.ReadTimeout = config.GetDurationVar(0, time.Second, []string{"ReadTimeout", "ReadTimeOutInSec"}...)
	a.config.http.ReadHeaderTimeout = config.GetDurationVar(0, time.Second, []string{"ReadHeaderTimeout", "ReadHeaderTimeoutInSec"}...)
	a.config.http.WriteTimeout = config.GetDurationVar(10, time.Second, []string{"WriteTimeout", "WriteTimeOutInSec"}...)
	a.config.http.IdleTimeout = config.GetDurationVar(720, time.Second, []string{"IdleTimeout", "IdleTimeoutInSec"}...)
	a.config.http.webPort = config.GetIntVar(8086, 1, "Processor.webPort")
	a.config.http.MaxHeaderBytes = config.GetIntVar(524288, 1, "MaxHeaderBytes")
	a.config.processorDSLimit = config.GetReloadableIntVar(0, 1, "Processor.jobsDB.dsLimit", "JobsDB.dsLimit")
	a.config.gatewayDSLimit = config.GetReloadableIntVar(0, 1, "Gateway.jobsDB.dsLimit", "JobsDB.dsLimit")
	a.config.routerDSLimit = config.GetReloadableIntVar(0, 1, "Router.jobsDB.dsLimit", "JobsDB.dsLimit")
	a.config.batchRouterDSLimit = config.GetReloadableIntVar(0, 1, "BatchRouter.jobsDB.dsLimit", "JobsDB.dsLimit")
	if err := rudderCoreDBValidator(); err != nil {
		return err
	}
	if err := rudderCoreNodeSetup(); err != nil {
		return err
	}
	a.setupDone = true
	return nil
}

func (a *processorApp) StartRudderCore(ctx context.Context, options *app.Options) error {
	config := config.Default
	statsFactory := stats.Default
	if !a.setupDone {
		return fmt.Errorf("processor service cannot start, database is not setup")
	}
	a.log.Info("Processor starting")

	g, ctx := errgroup.WithContext(ctx)
	terminalErrFn := terminalErrorFunction(ctx, g)

	deploymentType, err := deployment.GetFromEnv()
	if err != nil {
		return fmt.Errorf("failed to get deployment type: %w", err)
	}
	a.log.Infof("Configured deployment type: %q", deploymentType)

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
	g.Go(crash.Wrapper(func() error {
		syncer()
		return nil
	}))

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
	if config.GetBoolVar(true, "db.processor.pool.shared", "db.pool.shared") {
		dbPool, err = misc.NewDatabaseConnectionPool(ctx, config, statsFactory, "processor-app")
		if err != nil {
			return err
		}
		defer dbPool.Close()
	}

	gwDBForProcessor := jobsdb.NewForRead(
		"gw",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithDSLimit(a.config.gatewayDSLimit),
		jobsdb.WithSkipMaintenanceErr(config.GetBool("Gateway.jobsDB.skipMaintenanceError", true)),
		jobsdb.WithStats(statsFactory),
		jobsdb.WithDBHandle(dbPool),
	)
	defer gwDBForProcessor.Close()
	routerDB := jobsdb.NewForReadWrite(
		"rt",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithDSLimit(a.config.routerDSLimit),
		jobsdb.WithSkipMaintenanceErr(config.GetBool("Router.jobsDB.skipMaintenanceError", false)),
		jobsdb.WithStats(statsFactory),
		jobsdb.WithDBHandle(dbPool),
	)
	defer routerDB.Close()
	batchRouterDB := jobsdb.NewForReadWrite(
		"batch_rt",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithDSLimit(a.config.batchRouterDSLimit),
		jobsdb.WithSkipMaintenanceErr(config.GetBool("BatchRouter.jobsDB.skipMaintenanceError", false)),
		jobsdb.WithStats(statsFactory),
		jobsdb.WithDBHandle(dbPool),
	)
	defer batchRouterDB.Close()
	errDBForRead := jobsdb.NewForRead(
		"proc_error",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithDSLimit(a.config.processorDSLimit),
		jobsdb.WithSkipMaintenanceErr(config.GetBool("Processor.jobsDB.skipMaintenanceError", false)),
		jobsdb.WithStats(statsFactory),
		jobsdb.WithDBHandle(dbPool),
	)
	defer errDBForRead.Close()
	errDBForWrite := jobsdb.NewForWrite(
		"proc_error",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithSkipMaintenanceErr(config.GetBool("Processor.jobsDB.skipMaintenanceError", true)),
		jobsdb.WithStats(statsFactory),
		jobsdb.WithDBHandle(dbPool),
	)
	errDBForWrite.Close()
	if err = errDBForWrite.Start(); err != nil {
		return fmt.Errorf("could not start errDBForWrite: %w", err)
	}
	defer errDBForWrite.Stop()
	schemaDB := jobsdb.NewForReadWrite(
		"esch",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithDSLimit(a.config.processorDSLimit),
		jobsdb.WithStats(statsFactory),
		jobsdb.WithDBHandle(dbPool),
	)
	defer schemaDB.Close()

	archivalDB := jobsdb.NewForReadWrite(
		"arc",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithDSLimit(a.config.processorDSLimit),
		jobsdb.WithSkipMaintenanceErr(config.GetBool("Processor.jobsDB.skipMaintenanceError", false)),
		jobsdb.WithStats(statsFactory),
		jobsdb.WithJobMaxAge(
			func() time.Duration {
				return config.GetDuration("archival.jobRetention", 24, time.Hour)
			},
		),
		jobsdb.WithDBHandle(dbPool),
	)
	defer archivalDB.Close()

	var schemaForwarder schema_forwarder.Forwarder
	if config.GetBool("EventSchemas2.enabled", false) {
		client, err := pulsar.NewClient(config)
		if err != nil {
			return err
		}
		defer client.Close()
		schemaForwarder = schema_forwarder.NewForwarder(terminalErrFn, schemaDB, &client, backendconfig.DefaultBackendConfig, logger.NewLogger().Child("jobs_forwarder"), config, statsFactory)
	} else {
		schemaForwarder = schema_forwarder.NewAbortingForwarder(terminalErrFn, schemaDB, logger.NewLogger().Child("jobs_forwarder"), config, statsFactory)
	}

	modeProvider, err := resolveModeProvider(a.log, deploymentType)
	if err != nil {
		return err
	}

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

	p := proc.New(
		ctx,
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
		trackedUsersReporter,
		proc.WithAdaptiveLimit(adaptiveLimit),
	)
	throttlerFactory, err := throttler.NewFactory(config, statsFactory)
	if err != nil {
		return fmt.Errorf("failed to create throttler factory: %w", err)
	}
	rtFactory := &router.Factory{
		Logger:                     logger.NewLogger().Child("router"),
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
		Provider:         modeProvider,
		GatewayComponent: false,
		GatewayDB:        gwDBForProcessor,
		RouterDB:         routerDB,
		BatchRouterDB:    batchRouterDB,
		ErrorDB:          errDBForRead,
		SchemaForwarder:  schemaForwarder,
		EventSchemaDB:    schemaDB,
		ArchivalDB:       archivalDB,
		Processor:        p,
		Router:           rt,
		Archiver: archiver.New(
			archivalDB,
			fileUploaderProvider,
			config,
			statsFactory,
			archiver.WithAdaptiveLimit(adaptiveLimit),
		),
	}

	g.Go(func() error {
		return a.startHealthWebHandler(ctx, gwDBForProcessor)
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

	return g.Wait()
}

func (a *processorApp) startHealthWebHandler(ctx context.Context, db *jobsdb.Handle) error {
	// Port where Processor health handler is running
	a.log.Infof("Starting in %d", a.config.http.webPort)
	srvMux := chi.NewMux()
	srvMux.HandleFunc("/health", app.LivenessHandler(db))
	srvMux.HandleFunc("/", app.LivenessHandler(db))
	srv := &http.Server{
		Addr:              ":" + strconv.Itoa(a.config.http.webPort),
		Handler:           crash.Handler(srvMux),
		ReadTimeout:       a.config.http.ReadTimeout,
		ReadHeaderTimeout: a.config.http.ReadHeaderTimeout,
		WriteTimeout:      a.config.http.WriteTimeout,
		IdleTimeout:       a.config.http.IdleTimeout,
		MaxHeaderBytes:    a.config.http.MaxHeaderBytes,
	}

	return kithttputil.ListenAndServe(ctx, srv)
}
