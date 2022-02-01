package apphandlers

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/bugsnag/bugsnag-go"
	"github.com/gorilla/mux"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	operationmanager "github.com/rudderlabs/rudder-server/operation-manager"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	"github.com/rudderlabs/rudder-server/services/db"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	transformationdebugger "github.com/rudderlabs/rudder-server/services/debugger/transformation"
	"github.com/rudderlabs/rudder-server/services/multitenant"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	"golang.org/x/sync/errgroup"

	// This is necessary for compatibility with enterprise features
	_ "github.com/rudderlabs/rudder-server/imports"
)

//ProcessorApp is the type for Processor type implemention
type ProcessorApp struct {
	App            app.Interface
	VersionHandler func(w http.ResponseWriter, r *http.Request)
}

var (
	gatewayDB         jobsdb.HandleT
	batchRouterDB     jobsdb.HandleT
	procErrorDB       jobsdb.HandleT
	ReadTimeout       time.Duration
	ReadHeaderTimeout time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	webPort           int
	MaxHeaderBytes    int
)

func (processor *ProcessorApp) GetAppType() string {
	return fmt.Sprintf("rudder-server-%s", app.PROCESSOR)
}

func Init() {
	loadConfigHandler()
}

func loadConfigHandler() {
	config.RegisterDurationConfigVariable(time.Duration(0), &ReadTimeout, false, time.Second, []string{"ReadTimeout", "ReadTimeOutInSec"}...)
	config.RegisterDurationConfigVariable(time.Duration(0), &ReadHeaderTimeout, false, time.Second, []string{"ReadHeaderTimeout", "ReadHeaderTimeoutInSec"}...)
	config.RegisterDurationConfigVariable(time.Duration(10), &WriteTimeout, false, time.Second, []string{"WriteTimeout", "WriteTimeOutInSec"}...)
	config.RegisterDurationConfigVariable(time.Duration(720), &IdleTimeout, false, time.Second, []string{"IdleTimeout", "IdleTimeoutInSec"}...)
	config.RegisterIntConfigVariable(8086, &webPort, false, 1, "Processor.webPort")
	config.RegisterIntConfigVariable(524288, &MaxHeaderBytes, false, 1, "MaxHeaderBytes")
}

func (processor *ProcessorApp) StartRudderCore(ctx context.Context, options *app.Options) error {
	pkgLogger.Info("Processor starting")

	rudderCoreDBValidator()
	rudderCoreWorkSpaceTableSetup()
	rudderCoreNodeSetup()
	rudderCoreBaseSetup()
	g, ctx := errgroup.WithContext(ctx)

	//Setting up reporting client
	if processor.App.Features().Reporting != nil {
		reporting := processor.App.Features().Reporting.Setup(backendconfig.DefaultBackendConfig)

		g.Go(misc.WithBugsnag(func() error {
			reporting.AddClient(ctx, types.Config{ConnInfo: jobsdb.GetConnectionString()})
			return nil
		}))
	}

	pkgLogger.Info("Clearing DB ", options.ClearDB)

	transformationdebugger.Setup()
	destinationdebugger.Setup(backendconfig.DefaultBackendConfig)

	migrationMode := processor.App.Options().MigrationMode
	//IMP NOTE: All the jobsdb setups must happen before migrator setup.
	gatewayDB.Setup(jobsdb.Read, options.ClearDB, "gw", gwDBRetention, migrationMode, true, jobsdb.QueryFiltersT{})
	defer gatewayDB.TearDown()

	var routerDB jobsdb.HandleT = jobsdb.HandleT{}
	if enableProcessor || enableReplay {
		//setting up router, batch router, proc error DBs only if processor is enabled.
		routerDB.Setup(jobsdb.ReadWrite, options.ClearDB, "rt", routerDBRetention, migrationMode, true, router.QueryFilters)
		defer routerDB.TearDown()

		batchRouterDB.Setup(jobsdb.ReadWrite, options.ClearDB, "batch_rt", routerDBRetention, migrationMode, true, batchrouter.QueryFilters)
		defer batchRouterDB.TearDown()

		procErrorDB.Setup(jobsdb.ReadWrite, options.ClearDB, "proc_error", routerDBRetention, migrationMode, false, jobsdb.QueryFiltersT{})
		defer procErrorDB.TearDown()
	}

	var tenantRouterDB jobsdb.MultiTenantJobsDB = &jobsdb.MultiTenantLegacy{HandleT: &routerDB} //FIXME copy locks ?
	var multitenantStats multitenant.MultiTenantI = multitenant.NOOP

	if config.GetBool("EnableMultitenancy", false) {
		tenantRouterDB = &jobsdb.MultiTenantHandleT{HandleT: &routerDB}
		multitenantStats = multitenant.NewStats(tenantRouterDB)
	}

	reportingI := processor.App.Features().Reporting.GetReportingInstance()

	if processor.App.Features().Migrator != nil {
		if migrationMode == db.IMPORT || migrationMode == db.EXPORT || migrationMode == db.IMPORT_EXPORT {
			startProcessorFunc := func() {
				g.Go(func() error {
					clearDB := false
					StartProcessor(ctx, &clearDB, enableProcessor, &gatewayDB, &routerDB, &batchRouterDB, &procErrorDB, reportingI, multitenantStats)

					return nil
				})
			}
			startRouterFunc := func() {
				g.Go(func() error {
					StartRouter(ctx, enableRouter, tenantRouterDB, &batchRouterDB, &procErrorDB, reportingI, multitenantStats)
					return nil
				})
			}
			enableRouter = false
			enableProcessor = false

			processor.App.Features().Migrator.PrepareJobsdbsForImport(nil, &routerDB, &batchRouterDB)
			g.Go(func() error {
				processor.App.Features().Migrator.Run(ctx, &gatewayDB, &routerDB, &batchRouterDB, startProcessorFunc, startRouterFunc) //TODO
				return nil
			})
		}
	}

	operationmanager.Setup(&gatewayDB, &routerDB, &batchRouterDB)

	g.Go(misc.WithBugsnag(func() error {
		return operationmanager.OperationManager.StartProcessLoop(ctx)
	}))

	g.Go(func() error {
		StartProcessor(ctx, &options.ClearDB, enableProcessor, &gatewayDB, &routerDB, &batchRouterDB, &procErrorDB, reportingI, multitenantStats)
		return nil
	})
	g.Go(func() error {
		StartRouter(ctx, enableRouter, tenantRouterDB, &batchRouterDB, &procErrorDB, reportingI, multitenantStats)
		return nil
	})

	if enableReplay && processor.App.Features().Replay != nil {
		var replayDB jobsdb.HandleT
		replayDB.Setup(jobsdb.ReadWrite, options.ClearDB, "replay", routerDBRetention, migrationMode, true, jobsdb.QueryFiltersT{})
		defer replayDB.TearDown()
		processor.App.Features().Replay.Setup(&replayDB, &gatewayDB, &routerDB)
	}

	g.Go(func() error {
		return startHealthWebHandler(ctx)
	})
	err := g.Wait()

	return err
	//go readIOforResume(router) //keeping it as input from IO, to be replaced by UI
}

func (processor *ProcessorApp) HandleRecovery(options *app.Options) {
	db.HandleNullRecovery(options.NormalMode, options.DegradedMode, options.StandByMode, options.MigrationMode, misc.AppStartTime, app.PROCESSOR)
}

func startHealthWebHandler(ctx context.Context) error {
	//Port where Processor health handler is running
	pkgLogger.Infof("Starting in %d", webPort)
	srvMux := mux.NewRouter()
	srvMux.HandleFunc("/health", healthHandler)
	srvMux.HandleFunc("/", healthHandler)
	srv := &http.Server{
		Addr:              ":" + strconv.Itoa(webPort),
		Handler:           bugsnag.Handler(srvMux),
		ReadTimeout:       ReadTimeout,
		ReadHeaderTimeout: ReadHeaderTimeout,
		WriteTimeout:      WriteTimeout,
		IdleTimeout:       IdleTimeout,
		MaxHeaderBytes:    MaxHeaderBytes,
	}
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		<-ctx.Done()
		return srv.Shutdown(context.Background())
	})
	g.Go(func() error {
		return srv.ListenAndServe()
	})

	return g.Wait()
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	app.HealthHandler(w, r, &gatewayDB)
}
