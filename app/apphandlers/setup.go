package apphandlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"time"

	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/validators"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	utilsync "github.com/rudderlabs/rudder-server/utils/sync"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-server/utils/types"
)

var (
	maxProcess                                                 int
	gwDBRetention, routerDBRetention                           time.Duration
	enableProcessor, enableRouter, enableReplay                bool
	objectStorageDestinations                                  []string
	asyncDestinations                                          []string
	warehouseDestinations                                      []string
	routerLoaded                                               utilsync.First
	processorLoaded                                            utilsync.First
	pkgLogger                                                  logger.LoggerI
	Diagnostics                                                diagnostics.DiagnosticsI = diagnostics.Diagnostics
	readonlyGatewayDB, readonlyRouterDB, readonlyBatchRouterDB jobsdb.ReadonlyHandleT
	readonlyProcErrorDB                                        jobsdb.ReadonlyHandleT
)

//AppHandler to be implemented by different app type objects.
type AppHandler interface {
	GetAppType() string
	HandleRecovery(*app.Options)
	StartRudderCore(context.Context, *app.Options) error
}

func GetAppHandler(application app.Interface, appType string, versionHandler func(w http.ResponseWriter, r *http.Request)) AppHandler {
	var handler AppHandler
	switch appType {
	case app.GATEWAY:
		handler = &GatewayApp{App: application, VersionHandler: versionHandler}
	case app.PROCESSOR:
		handler = &ProcessorApp{App: application, VersionHandler: versionHandler}
	case app.EMBEDDED:
		handler = &EmbeddedApp{App: application, VersionHandler: versionHandler}
	default:
		panic(errors.New("invalid app type"))
	}

	return handler
}

func init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("apphandlers")
}

func loadConfig() {
	config.RegisterIntConfigVariable(12, &maxProcess, false, 1, "maxProcess")
	config.RegisterDurationConfigVariable(time.Duration(0), &gwDBRetention, false, time.Hour, []string{"gwDBRetention", "gwDBRetentionInHr"}...)
	config.RegisterDurationConfigVariable(time.Duration(0), &routerDBRetention, false, time.Hour, "routerDBRetention")
	config.RegisterBoolConfigVariable(true, &enableProcessor, false, "enableProcessor")
	config.RegisterBoolConfigVariable(false, &enableReplay, false, "Replay.enabled")
	config.RegisterBoolConfigVariable(true, &enableRouter, false, "enableRouter")
	objectStorageDestinations = []string{"S3", "GCS", "AZURE_BLOB", "MINIO", "DIGITAL_OCEAN_SPACES"}
	asyncDestinations = []string{"MARKETO_BULK_UPLOAD"}
	warehouseDestinations = []string{"RS", "BQ", "SNOWFLAKE", "POSTGRES", "CLICKHOUSE", "MSSQL", "AZURE_SYNAPSE", "S3_DATALAKE"}
}

func rudderCoreBaseSetup() {

	if !validators.ValidateEnv() {
		panic(errors.New("Failed to start rudder-server"))
	}
	validators.InitializeEnv()

	// Check if there is a probable inconsistent state of Data
	if diagnostics.EnableServerStartMetric {
		Diagnostics.Track(diagnostics.ServerStart, map[string]interface{}{
			diagnostics.ServerStart: fmt.Sprint(time.Unix(misc.AppStartTime, 0)),
		})
	}

	//Reload Config
	loadConfig()

	readonlyGatewayDB.Setup("gw")
	readonlyRouterDB.Setup("rt")
	readonlyBatchRouterDB.Setup("batch_rt")
	readonlyProcErrorDB.Setup("proc_error")

	processor.RegisterAdminHandlers(&readonlyProcErrorDB)
	router.RegisterAdminHandlers(&readonlyRouterDB, &readonlyBatchRouterDB)

	runtime.GOMAXPROCS(maxProcess)
}

//StartProcessor atomically starts processor process if not already started
func StartProcessor(ctx context.Context, clearDB *bool, enableProcessor bool, gatewayDB, routerDB, batchRouterDB *jobsdb.HandleT, procErrorDB *jobsdb.HandleT, reporting types.ReportingI) {
	if !enableProcessor {
		return
	}

	if !processorLoaded.First() {
		pkgLogger.Debug("processor started by an other go routine")
		return
	}

	var processorInstance = processor.NewProcessor()
	processor.ProcessorManagerSetup(processorInstance)
	processorInstance.Setup(backendconfig.DefaultBackendConfig, gatewayDB, routerDB, batchRouterDB, procErrorDB, clearDB, reporting)
	processorInstance.Start(ctx)
}

//StartRouter atomically starts router process if not already started
func StartRouter(ctx context.Context, enableRouter bool, routerDB, batchRouterDB, procErrorDB *jobsdb.HandleT, reporting types.ReportingI) {
	if !enableRouter {
		return
	}

	if !routerLoaded.First() {
		pkgLogger.Debug("processor started by an other go routine")
		return
	}

	router.RoutersManagerSetup()
	batchrouter.BatchRoutersManagerSetup()
	monitorDestRouters(ctx, routerDB, batchRouterDB, procErrorDB, reporting)
}

// Gets the config from config backend and extracts enabled writekeys
func monitorDestRouters(ctx context.Context, routerDB, batchRouterDB, procErrorDB *jobsdb.HandleT, reporting types.ReportingI) {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch, backendconfig.TopicBackendConfig)
	dstToRouter := make(map[string]*router.HandleT)
	dstToBatchRouter := make(map[string]*batchrouter.HandleT)
	// dstToWhRouter := make(map[string]*warehouse.HandleT)

	cleanup := make([]func(), 0)

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case config := <-ch:
			sources := config.Data.(backendconfig.ConfigT)
			enabledDestinations := make(map[string]bool)
			for _, source := range sources.Sources {
				for _, destination := range source.Destinations {
					enabledDestinations[destination.DestinationDefinition.Name] = true
					//For batch router destinations
					if misc.Contains(objectStorageDestinations, destination.DestinationDefinition.Name) || misc.Contains(warehouseDestinations, destination.DestinationDefinition.Name) || misc.Contains(asyncDestinations, destination.DestinationDefinition.Name) {
						_, ok := dstToBatchRouter[destination.DestinationDefinition.Name]
						if !ok {
							pkgLogger.Info("Starting a new Batch Destination Router ", destination.DestinationDefinition.Name)
							var brt batchrouter.HandleT
							brt.Setup(backendconfig.DefaultBackendConfig, batchRouterDB, procErrorDB, destination.DestinationDefinition.Name, reporting)
							brt.Start()
							// TODO cleanup = append(cleanup, router.Shutdown)
							cleanup = append(cleanup, brt.Shutdown)
							dstToBatchRouter[destination.DestinationDefinition.Name] = &brt
						}
					} else {
						_, ok := dstToRouter[destination.DestinationDefinition.Name]
						if !ok {
							pkgLogger.Info("Starting a new Destination ", destination.DestinationDefinition.Name)
							var router router.HandleT
							router.Setup(backendconfig.DefaultBackendConfig, routerDB, procErrorDB, destination.DestinationDefinition, reporting)
							router.Start()
							cleanup = append(cleanup, router.Shutdown)
							dstToRouter[destination.DestinationDefinition.Name] = &router
						}
					}
				}
			}

			rm, err := router.GetRoutersManager()
			if rm != nil && err == nil {
				rm.SetRoutersReady()
			}

			brm, err := batchrouter.GetBatchRoutersManager()
			if brm != nil && err == nil {
				brm.SetBatchRoutersReady()
			}
		}
	}

	g, _ := errgroup.WithContext(context.Background())
	for _, f := range cleanup {
		f := f
		g.Go(func() error {
			f()
			return nil
		})
	}
	g.Wait()
}
