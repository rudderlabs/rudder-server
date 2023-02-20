package apphandlers

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/router/throttler"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/payload"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"

	"golang.org/x/sync/errgroup"

	"github.com/bugsnag/bugsnag-go/v2"
	"github.com/gorilla/mux"

	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/app/cluster"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/jobsdb/prebackup"
	proc "github.com/rudderlabs/rudder-server/processor"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	routerManager "github.com/rudderlabs/rudder-server/router/manager"
	"github.com/rudderlabs/rudder-server/services/db"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	transformationdebugger "github.com/rudderlabs/rudder-server/services/debugger/transformation"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/services/multitenant"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
)

// processorApp is the type for Processor type implemention
type processorApp struct {
	setupDone      bool
	app            app.App
	versionHandler func(w http.ResponseWriter, r *http.Request)
	log            logger.Logger
	config         struct {
		processorDSLimit   int
		routerDSLimit      int
		batchRouterDSLimit int
		gatewayDSLimit     int
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

func (a *processorApp) loadConfiguration() {
	config.RegisterDurationConfigVariable(0, &a.config.http.ReadTimeout, false, time.Second, []string{"ReadTimeout", "ReadTimeOutInSec"}...)
	config.RegisterDurationConfigVariable(0, &a.config.http.ReadHeaderTimeout, false, time.Second, []string{"ReadHeaderTimeout", "ReadHeaderTimeoutInSec"}...)
	config.RegisterDurationConfigVariable(10, &a.config.http.WriteTimeout, false, time.Second, []string{"WriteTimeout", "WriteTimeOutInSec"}...)
	config.RegisterDurationConfigVariable(720, &a.config.http.IdleTimeout, false, time.Second, []string{"IdleTimeout", "IdleTimeoutInSec"}...)
	config.RegisterIntConfigVariable(8086, &a.config.http.webPort, false, 1, "Processor.webPort")
	config.RegisterIntConfigVariable(524288, &a.config.http.MaxHeaderBytes, false, 1, "MaxHeaderBytes")
	config.RegisterIntConfigVariable(0, &a.config.processorDSLimit, true, 1, "Processor.jobsDB.dsLimit", "JobsDB.dsLimit")
	config.RegisterIntConfigVariable(0, &a.config.gatewayDSLimit, true, 1, "Gateway.jobsDB.dsLimit", "JobsDB.dsLimit")
	config.RegisterIntConfigVariable(0, &a.config.routerDSLimit, true, 1, "Router.jobsDB.dsLimit", "JobsDB.dsLimit")
	config.RegisterIntConfigVariable(0, &a.config.batchRouterDSLimit, true, 1, "BatchRouter.jobsDB.dsLimit", "JobsDB.dsLimit")
}

func (a *processorApp) Setup(options *app.Options) error {
	a.loadConfiguration()
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

	if _, err := setupReadonlyDBs(); err != nil {
		return err
	}
	g, ctx := errgroup.WithContext(ctx)

	deploymentType, err := deployment.GetFromEnv()
	if err != nil {
		return fmt.Errorf("failed to get deployment type: %w", err)
	}
	a.log.Infof("Configured deployment type: %q", deploymentType)

	reporting := a.app.Features().Reporting.Setup(backendconfig.DefaultBackendConfig)

	g.Go(misc.WithBugsnag(func() error {
		reporting.AddClient(ctx, types.Config{ConnInfo: misc.GetConnectionString()})
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

	reportingI := a.app.Features().Reporting.GetReportingInstance()
	transientSources := transientsource.NewService(ctx, backendconfig.DefaultBackendConfig)
	prebackupHandlers := []prebackup.Handler{
		prebackup.DropSourceIds(transientSources.SourceIdsSupplier()),
	}

	fileUploaderProvider := fileuploader.NewProvider(ctx, backendconfig.DefaultBackendConfig)

	rsourcesService, err := NewRsourcesService(deploymentType)
	if err != nil {
		return err
	}

	gwDBForProcessor := jobsdb.NewForRead(
		"gw",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithStatusHandler(),
		jobsdb.WithPreBackupHandlers(prebackupHandlers),
		jobsdb.WithDSLimit(&a.config.gatewayDSLimit),
		jobsdb.WithFileUploaderProvider(fileUploaderProvider),
	)
	defer gwDBForProcessor.Close()
	routerDB := jobsdb.NewForReadWrite(
		"rt",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithStatusHandler(),
		jobsdb.WithPreBackupHandlers(prebackupHandlers),
		jobsdb.WithDSLimit(&a.config.routerDSLimit),
		jobsdb.WithFileUploaderProvider(fileUploaderProvider),
	)
	defer routerDB.Close()
	batchRouterDB := jobsdb.NewForReadWrite(
		"batch_rt",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithStatusHandler(),
		jobsdb.WithPreBackupHandlers(prebackupHandlers),
		jobsdb.WithDSLimit(&a.config.batchRouterDSLimit),
		jobsdb.WithFileUploaderProvider(fileUploaderProvider),
	)
	defer batchRouterDB.Close()
	errDB := jobsdb.NewForReadWrite(
		"proc_error",
		jobsdb.WithClearDB(options.ClearDB),
		jobsdb.WithStatusHandler(),
		jobsdb.WithPreBackupHandlers(prebackupHandlers),
		jobsdb.WithDSLimit(&a.config.processorDSLimit),
		jobsdb.WithFileUploaderProvider(fileUploaderProvider),
	)
	var tenantRouterDB jobsdb.MultiTenantJobsDB
	var multitenantStats multitenant.MultiTenantI
	if misc.UseFairPickup() {
		tenantRouterDB = &jobsdb.MultiTenantHandleT{HandleT: routerDB}
		multitenantStats = multitenant.NewStats(map[string]jobsdb.MultiTenantJobsDB{
			"rt":       tenantRouterDB,
			"batch_rt": &jobsdb.MultiTenantLegacy{HandleT: batchRouterDB},
		})
	} else {
		tenantRouterDB = &jobsdb.MultiTenantLegacy{HandleT: routerDB}
		multitenantStats = multitenant.WithLegacyPickupJobs(multitenant.NewStats(map[string]jobsdb.MultiTenantJobsDB{
			"rt":       tenantRouterDB,
			"batch_rt": &jobsdb.MultiTenantLegacy{HandleT: batchRouterDB},
		}))
	}

	modeProvider, err := resolveModeProvider(a.log, deploymentType)
	if err != nil {
		return err
	}

	adaptiveLimit := payload.SetupAdaptiveLimiter(ctx, g)

	p := proc.New(
		ctx,
		&options.ClearDB,
		gwDBForProcessor,
		routerDB,
		batchRouterDB,
		errDB,
		multitenantStats,
		reportingI,
		transientSources,
		fileUploaderProvider,
		rsourcesService,
		destinationHandle,
		transformationhandle,
		proc.WithAdaptiveLimit(adaptiveLimit),
	)
	throttlerFactory, err := throttler.New(stats.Default)
	if err != nil {
		return fmt.Errorf("failed to create throttler factory: %w", err)
	}
	rtFactory := &router.Factory{
		Reporting:        reportingI,
		Multitenant:      multitenantStats,
		BackendConfig:    backendconfig.DefaultBackendConfig,
		RouterDB:         tenantRouterDB,
		ProcErrorDB:      errDB,
		TransientSources: transientSources,
		RsourcesService:  rsourcesService,
		ThrottlerFactory: throttlerFactory,
		Debugger:         destinationHandle,
		AdaptiveLimit:    adaptiveLimit,
	}
	brtFactory := &batchrouter.Factory{
		Reporting:        reportingI,
		Multitenant:      multitenantStats,
		BackendConfig:    backendconfig.DefaultBackendConfig,
		RouterDB:         batchRouterDB,
		ProcErrorDB:      errDB,
		TransientSources: transientSources,
		RsourcesService:  rsourcesService,
		Debugger:         destinationHandle,
		AdaptiveLimit:    adaptiveLimit,
	}
	rt := routerManager.New(rtFactory, brtFactory, backendconfig.DefaultBackendConfig)

	dm := cluster.Dynamic{
		Provider:         modeProvider,
		GatewayComponent: false,
		GatewayDB:        gwDBForProcessor,
		RouterDB:         routerDB,
		BatchRouterDB:    batchRouterDB,
		ErrorDB:          errDB,
		Processor:        p,
		Router:           rt,
		MultiTenantStat:  multitenantStats,
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

func (a *processorApp) startHealthWebHandler(ctx context.Context, db *jobsdb.HandleT) error {
	// Port where Processor health handler is running
	a.log.Infof("Starting in %d", a.config.http.webPort)
	srvMux := mux.NewRouter()
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

	return httputil.ListenAndServe(ctx, srv)
}
