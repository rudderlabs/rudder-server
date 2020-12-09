package apptype

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

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
)

var (
	maxProcess                       int
	gwDBRetention, routerDBRetention time.Duration
	enableProcessor, enableRouter    bool
	objectStorageDestinations        []string
	warehouseDestinations            []string
	moduleLoadLock                   sync.Mutex
	routerLoaded                     bool
	processorLoaded                  bool
	pkgLogger                        logger.LoggerI
	Diagnostics                      diagnostics.DiagnosticsI = diagnostics.Diagnostics
)

func init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("apptype")
}

func loadConfig() {
	maxProcess = config.GetInt("maxProcess", 12)
	gwDBRetention = config.GetDuration("gwDBRetentionInHr", 0) * time.Hour
	routerDBRetention = config.GetDuration("routerDBRetention", 0)
	enableProcessor = config.GetBool("enableProcessor", true)
	enableRouter = config.GetBool("enableRouter", true)
	objectStorageDestinations = []string{"S3", "GCS", "AZURE_BLOB", "MINIO", "DIGITAL_OCEAN_SPACES"}
	warehouseDestinations = []string{"RS", "BQ", "SNOWFLAKE", "POSTGRES", "CLICKHOUSE"}
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

	runtime.GOMAXPROCS(maxProcess)
}

//NOTE: StartProcessor and StartRouter should be called in order and from the same thread.
//StartProcessor sets up router, batch router, proc error DBs, which router also depends on.
//enableRouter will be true only if enableProcessor is true. This is a must.
//If otherwise, server may crash because setup of router and batch router DB is not called.

//StartProcessor atomically starts processor process if not already started
func StartProcessor(clearDB *bool, migrationMode string, enableProcessor bool, gatewayDB, routerDB, batchRouterDB *jobsdb.HandleT, procErrorDB *jobsdb.HandleT) {
	moduleLoadLock.Lock()
	defer moduleLoadLock.Unlock()

	if processorLoaded {
		return
	}

	if enableProcessor {
		//setting up router, batch router, proc error DBs only if processor is enabled.
		routerDB.Setup(jobsdb.ReadWrite, *clearDB, "rt", routerDBRetention, migrationMode, true)
		batchRouterDB.Setup(jobsdb.ReadWrite, *clearDB, "batch_rt", routerDBRetention, migrationMode, true)
		procErrorDB.Setup(jobsdb.ReadWrite, *clearDB, "proc_error", routerDBRetention, migrationMode, false)

		var processor = processor.NewProcessor()
		processor.Setup(backendconfig.DefaultBackendConfig, gatewayDB, routerDB, batchRouterDB, procErrorDB)
		processor.Start()

		processorLoaded = true
	}
}

//StartRouter atomically starts router process if not already started
func StartRouter(enableRouter bool, routerDB, batchRouterDB *jobsdb.HandleT) {
	moduleLoadLock.Lock()
	defer moduleLoadLock.Unlock()

	if routerLoaded {
		return
	}

	if enableRouter {
		go monitorDestRouters(routerDB, batchRouterDB)
		routerLoaded = true
	}
}

// Gets the config from config backend and extracts enabled writekeys
func monitorDestRouters(routerDB, batchRouterDB *jobsdb.HandleT) {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch, backendconfig.TopicBackendConfig)
	dstToRouter := make(map[string]*router.HandleT)
	dstToBatchRouter := make(map[string]*batchrouter.HandleT)
	// dstToWhRouter := make(map[string]*warehouse.HandleT)

	for {
		config := <-ch
		sources := config.Data.(backendconfig.SourcesT)
		enabledDestinations := make(map[string]bool)
		for _, source := range sources.Sources {
			for _, destination := range source.Destinations {
				enabledDestinations[destination.DestinationDefinition.Name] = true
				//For batch router destinations
				if misc.Contains(objectStorageDestinations, destination.DestinationDefinition.Name) || misc.Contains(warehouseDestinations, destination.DestinationDefinition.Name) {
					_, ok := dstToBatchRouter[destination.DestinationDefinition.Name]
					if !ok {
						pkgLogger.Info("Starting a new Batch Destination Router ", destination.DestinationDefinition.Name)
						var brt batchrouter.HandleT
						brt.Setup(batchRouterDB, destination.DestinationDefinition.Name)
						dstToBatchRouter[destination.DestinationDefinition.Name] = &brt
					}
				} else {
					_, ok := dstToRouter[destination.DestinationDefinition.Name]
					if !ok {
						pkgLogger.Info("Starting a new Destination ", destination.DestinationDefinition.Name)
						var router router.HandleT
						router.Setup(routerDB, destination.DestinationDefinition.Name)
						dstToRouter[destination.DestinationDefinition.Name] = &router
					}
				}
			}
		}
	}
}
