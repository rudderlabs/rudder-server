package apphandlers

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/bugsnag/bugsnag-go/v2"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/app/cluster"
	"github.com/rudderlabs/rudder-server/archiver"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/internal/pulsar"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/jobsdb/prebackup"
	proc "github.com/rudderlabs/rudder-server/processor"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	routerManager "github.com/rudderlabs/rudder-server/router/manager"
	"github.com/rudderlabs/rudder-server/router/throttler"
	schema_forwarder "github.com/rudderlabs/rudder-server/schema-forwarder"
	"github.com/rudderlabs/rudder-server/services/db"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	transformationdebugger "github.com/rudderlabs/rudder-server/services/debugger/transformation"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/services/transientsource"
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
		processorDSLimit   misc.ValueLoader[int]
		routerDSLimit      misc.ValueLoader[int]
		batchRouterDSLimit misc.ValueLoader[int]
		gatewayDSLimit     misc.ValueLoader[int]
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

func (a *processorApp) Setup(options *app.Options) error {
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
	if err := db.HandleNullRecovery(options.NormalMode, options.DegradedMode, misc.AppStartTime, app.PROCESSOR); err != nil {
		return err
	}
	if err := rudderCoreDBValidator(); err != nil {
		return err
	}
	if err := rudderCoreWorkSpaceTableSetup(); err != nil {
		return err
	}
	if err := rudderCoreNodeSetup(); err != nil {
		return err
	}
	a.setupDone = true
	return nil
}

func (a *processorApp) StartRudderCore(ctx context.Context, options *app.Options) error {
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

	reporting := a.app.Features().Reporting.Setup(ctx, backendconfig.DefaultBackendConfig)
	defer reporting.Stop()
	syncer := reporting.DatabaseSyncer(types.SyncerConfig{ConnInfo: misc.GetConnectionString(config.Default)})
	g.Go(misc.WithBugsnag(func() error {
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
	prebackupHandlers := []prebackup.Handler{
		prebackup.DropSourceIds(transientSources.SourceIdsSupplier()),
	}

	fileUploaderProvider := fileuploader.NewProvider(ctx, backendconfig.DefaultBackendConfig)

	rsourcesService, err := NewRsourcesService(deploymentType)
	if err != nil {
		return err
	}

	transformerFeaturesService := transformer.NewFeaturesService(ctx, transformer.FeaturesServiceConfig{
		PollInterval:             config.GetDuration("Transformer.pollInterval", 1, time.Second),
		TransformerURL:           config.GetString("DEST_TRANSFORM_URL", "http://localhost:9090"),
		FeaturesRetryMaxAttempts: 10,
	})

	gwDBForProcessor := jobsdb.NewForRead(
		"gw",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithPreBackupHandlers(prebackupHandlers),
		jobsdb.WithDSLimit(a.config.gatewayDSLimit),
		jobsdb.WithFileUploaderProvider(fileUploaderProvider),
		jobsdb.WithSkipMaintenanceErr(config.GetBool("Gateway.jobsDB.skipMaintenanceError", true)),
	)
	defer gwDBForProcessor.Close()
	routerDB := jobsdb.NewForReadWrite(
		"rt",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithPreBackupHandlers(prebackupHandlers),
		jobsdb.WithDSLimit(a.config.routerDSLimit),
		jobsdb.WithFileUploaderProvider(fileUploaderProvider),
		jobsdb.WithSkipMaintenanceErr(config.GetBool("Router.jobsDB.skipMaintenanceError", false)),
	)
	defer routerDB.Close()
	batchRouterDB := jobsdb.NewForReadWrite(
		"batch_rt",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithPreBackupHandlers(prebackupHandlers),
		jobsdb.WithDSLimit(a.config.batchRouterDSLimit),
		jobsdb.WithFileUploaderProvider(fileUploaderProvider),
		jobsdb.WithSkipMaintenanceErr(config.GetBool("BatchRouter.jobsDB.skipMaintenanceError", false)),
	)
	defer batchRouterDB.Close()
	errDBForRead := jobsdb.NewForRead(
		"proc_error",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithPreBackupHandlers(prebackupHandlers),
		jobsdb.WithDSLimit(a.config.processorDSLimit),
		jobsdb.WithFileUploaderProvider(fileUploaderProvider),
		jobsdb.WithSkipMaintenanceErr(config.GetBool("Processor.jobsDB.skipMaintenanceError", false)),
	)
	defer errDBForRead.Close()
	errDBForWrite := jobsdb.NewForWrite(
		"proc_error",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithSkipMaintenanceErr(config.GetBool("Processor.jobsDB.skipMaintenanceError", true)),
	)
	if err = errDBForWrite.Start(); err != nil {
		return fmt.Errorf("could not start errDBForWrite: %w", err)
	}
	defer errDBForWrite.Stop()
	schemaDB := jobsdb.NewForReadWrite(
		"esch",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithDSLimit(a.config.processorDSLimit),
		jobsdb.WithFileUploaderProvider(fileUploaderProvider),
	)
	defer schemaDB.Close()

	archivalDB := jobsdb.NewForReadWrite(
		"arc",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithDSLimit(a.config.processorDSLimit),
		jobsdb.WithSkipMaintenanceErr(config.GetBool("Processor.jobsDB.skipMaintenanceError", false)),
		jobsdb.WithJobMaxAge(
			func() time.Duration {
				return config.GetDuration("archival.jobRetention", 24, time.Hour)
			},
		),
	)
	defer archivalDB.Close()

	var schemaForwarder schema_forwarder.Forwarder
	if config.GetBool("EventSchemas2.enabled", false) {
		client, err := pulsar.NewClient(config.Default)
		if err != nil {
			return err
		}
		defer client.Close()
		schemaForwarder = schema_forwarder.NewForwarder(terminalErrFn, schemaDB, &client, backendconfig.DefaultBackendConfig, logger.NewLogger().Child("jobs_forwarder"), config.Default, stats.Default)
	} else {
		schemaForwarder = schema_forwarder.NewAbortingForwarder(terminalErrFn, schemaDB, logger.NewLogger().Child("jobs_forwarder"), config.Default, stats.Default)
	}

	modeProvider, err := resolveModeProvider(a.log, deploymentType)
	if err != nil {
		return err
	}

	adaptiveLimit := payload.SetupAdaptiveLimiter(ctx, g)

	enrichers, err := setupPipelineEnrichers(config.Default, a.log, stats.Default)
	if err != nil {
		return fmt.Errorf("setting up pipeline enrichers: %w", err)
	}

	defer func() {
		for _, enricher := range enrichers {
			enricher.Close()
		}
	}()

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
		proc.WithAdaptiveLimit(adaptiveLimit),
	)
	throttlerFactory, err := throttler.NewFactory(config.Default, stats.Default)
	if err != nil {
		return fmt.Errorf("failed to create throttler factory: %w", err)
	}
	rtFactory := &router.Factory{
		Logger:           logger.NewLogger().Child("router"),
		Reporting:        reporting,
		BackendConfig:    backendconfig.DefaultBackendConfig,
		RouterDB:         routerDB,
		ProcErrorDB:      errDBForWrite,
		TransientSources: transientSources,
		RsourcesService:  rsourcesService,
		ThrottlerFactory: throttlerFactory,
		Debugger:         destinationHandle,
		AdaptiveLimit:    adaptiveLimit,
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
			config.Default,
			stats.Default,
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
		replicationLagStat := stats.Default.NewStat("rsources_log_replication_lag", stats.GaugeType)
		replicationSlotStat := stats.Default.NewStat("rsources_log_replication_slot", stats.GaugeType)
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
		Handler:           bugsnag.Handler(srvMux),
		ReadTimeout:       a.config.http.ReadTimeout,
		ReadHeaderTimeout: a.config.http.ReadHeaderTimeout,
		WriteTimeout:      a.config.http.WriteTimeout,
		IdleTimeout:       a.config.http.IdleTimeout,
		MaxHeaderBytes:    a.config.http.MaxHeaderBytes,
	}

	return kithttputil.ListenAndServe(ctx, srv)
}
