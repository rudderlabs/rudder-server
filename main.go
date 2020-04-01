package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"

	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"sync"
	"syscall"
	"time"

	"github.com/bugsnag/bugsnag-go"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/gateway"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor"
	ratelimiter "github.com/rudderlabs/rudder-server/rate-limiter"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/db"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/source-debugger"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/services/validators"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse"
)

var (
	maxProcess                                  int
	gwDBRetention, routerDBRetention            time.Duration
	enableProcessor, enableRouter, enableBackup bool
	isReplayServer                              bool
	enabledDestinations                         []backendconfig.DestinationT
	configSubscriberLock                        sync.RWMutex
	objectStorageDestinations                   []string
	warehouseDestinations                       []string
	warehouseMode                               string
)

var version = "Not an official release. Get the latest release from the github repo."
var major, minor, commit, buildDate, builtBy, gitURL, patch string

func loadConfig() {
	maxProcess = config.GetInt("maxProcess", 12)
	gwDBRetention = config.GetDuration("gwDBRetentionInHr", 0) * time.Hour
	routerDBRetention = config.GetDuration("routerDBRetention", 0)
	enableProcessor = config.GetBool("enableProcessor", true)
	enableRouter = config.GetBool("enableRouter", true)
	enableBackup = config.GetBool("JobsDB.enableBackup", true)
	isReplayServer = config.GetEnvAsBool("IS_REPLAY_SERVER", false)
	objectStorageDestinations = []string{"S3", "GCS", "AZURE_BLOB", "MINIO"}
	warehouseDestinations = []string{"RS", "BQ", "SNOWFLAKE"}
	warehouseMode = config.GetString("Warehouse.mode", "embedded")
}

// Test Function
func readIOforResume(router router.HandleT) {
	for {
		var u string
		_, err := fmt.Scanf("%v", &u)
		fmt.Println("from stdin ", u)
		if err != nil {
			panic(err)
		}
		router.ResetSleep()
	}
}

// Gets the config from config backend and extracts enabled writekeys
func monitorDestRouters(routerDB, batchRouterDB *jobsdb.HandleT) {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch, "backendConfig")
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
						logger.Info("Starting a new Batch Destination Router", destination.DestinationDefinition.Name)
						var brt batchrouter.HandleT
						brt.Setup(batchRouterDB, destination.DestinationDefinition.Name)
						dstToBatchRouter[destination.DestinationDefinition.Name] = &brt
					}
				} else {
					_, ok := dstToRouter[destination.DestinationDefinition.Name]
					if !ok {
						logger.Info("Starting a new Destination", destination.DestinationDefinition.Name)
						var router router.HandleT
						router.Setup(routerDB, destination.DestinationDefinition.Name)
						dstToRouter[destination.DestinationDefinition.Name] = &router
					}
				}
			}
		}
	}
}

func init() {
	config.Initialize()
	loadConfig()
}

func versionInfo() map[string]interface{} {
	return map[string]interface{}{"Version": version, "Major": major, "Minor": minor, "Patch": patch, "Commit": commit, "BuildDate": buildDate, "BuiltBy": builtBy, "GitUrl": gitURL}
}

func versionHandler(w http.ResponseWriter, r *http.Request) {
	var version = versionInfo()
	versionFormatted, _ := json.Marshal(&version)
	w.Write(versionFormatted)
}

func printVersion() {
	version := versionInfo()
	versionFormatted, _ := json.MarshalIndent(&version, "", " ")
	fmt.Printf("Version Info %s\n", versionFormatted)
}

func startWarehouseService() {
	warehouse.Start()
}

func startRudderCore(clearDB *bool, normalMode bool, degradedMode bool, maintenanceMode bool) {
	logger.Info("Main starting")

	if !validators.ValidateEnv() {
		panic(errors.New("Failed to start rudder-server"))
	}

	// Check if there is a probable inconsistent state of Data
	misc.AppStartTime = time.Now().Unix()
	db.HandleRecovery(normalMode, degradedMode, maintenanceMode, misc.AppStartTime)
	//Reload Config
	loadConfig()

	var gatewayDB jobsdb.HandleT
	var routerDB jobsdb.HandleT
	var batchRouterDB jobsdb.HandleT

	runtime.GOMAXPROCS(maxProcess)
	logger.Info("Clearing DB ", *clearDB)

	sourcedebugger.Setup()
	backendconfig.Setup()

	//Forcing enableBackup false if this server is for handling replayed events
	if isReplayServer {
		enableBackup = false
	}

	gatewayDB.Setup(*clearDB, "gw", gwDBRetention, enableBackup)
	routerDB.Setup(*clearDB, "rt", routerDBRetention, false)
	batchRouterDB.Setup(*clearDB, "batch_rt", routerDBRetention, false)

	//Setup the three modules, the gateway, the router and the processor

	if enableRouter {
		go monitorDestRouters(&routerDB, &batchRouterDB)
	}

	if enableProcessor {
		var processor processor.HandleT
		processor.Setup(&gatewayDB, &routerDB, &batchRouterDB)
	}

	var gateway gateway.HandleT
	var rateLimiter ratelimiter.HandleT
	rateLimiter.SetUp()
	gateway.Setup(&gatewayDB, &rateLimiter, clearDB)
	//go readIOforResume(router) //keeping it as input from IO, to be replaced by UI
}

func canStartServer() bool {
	return warehouseMode == config.EmbeddedMode || warehouseMode == config.OffMode
}

func canStartWarehouse() bool {
	return warehouseMode != config.OffMode
}

func main() {
	version := versionInfo()

	bugsnag.Configure(bugsnag.Configuration{
		APIKey:       config.GetEnv("BUGSNAG_KEY", ""),
		ReleaseStage: config.GetEnv("GO_ENV", "development"),
		// The import paths for the Go packages containing your source files
		ProjectPackages: []string{"main", "github.com/rudderlabs/rudder-server"},
		// more configuration options
		AppType:      "rudder-server",
		AppVersion:   version["Version"].(string),
		PanicHandler: func() {},
	})
	ctx := bugsnag.StartSession(context.Background())
	defer func() {
		if r := recover(); r != nil {
			defer bugsnag.AutoNotify(ctx, bugsnag.SeverityError, bugsnag.MetaData{
				"GoRoutines": {
					"Number": runtime.NumGoroutine(),
				}})

			misc.RecordAppError(fmt.Errorf("%v", r))
			logger.Fatal(r)
			panic(r)
		}
	}()

	logger.Setup()

	//Creating Stats Client should be done right after setting up logger and before setting up other modules.
	stats.Setup()

	normalMode := flag.Bool("normal-mode", false, "a bool")
	degradedMode := flag.Bool("degraded-mode", false, "a bool")
	maintenanceMode := flag.Bool("maintenance-mode", false, "a bool")

	clearDB := flag.Bool("cleardb", false, "a bool")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to `file`")
	memprofile := flag.String("memprofile", "", "write memory profile to `file`")
	versionFlag := flag.Bool("v", false, "Print the current version and exit")

	flag.Parse()
	if *versionFlag {
		printVersion()
		return
	}
	http.HandleFunc("/version", versionHandler)

	var f *os.File
	if *cpuprofile != "" {
		var err error
		f, err = os.Create(*cpuprofile)
		if err != nil {
			panic(err)
		}
		runtime.SetBlockProfileRate(1)
		err = pprof.StartCPUProfile(f)
		if err != nil {
			panic(err)
		}
	}

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		if *cpuprofile != "" {
			logger.Info("Stopping CPU profile")
			pprof.StopCPUProfile()
			f.Close()
		}
		if *memprofile != "" {
			f, err := os.Create(*memprofile)
			if err != nil {
				panic(err)
			}
			defer f.Close()
			runtime.GC() // get up-to-date statistics
			err = pprof.WriteHeapProfile(f)
			if err != nil {
				panic(err)
			}
		}
		// clearing zap Log buffer to std output
		if logger.Log != nil {
			logger.Fatal("SIGTERM called. Process exiting")
		}
		stats.StopRuntimeStats()
		os.Exit(1)
	}()

	if canStartServer() {
		rruntime.Go(func() {
			startRudderCore(clearDB, *normalMode, *degradedMode, *maintenanceMode)
		})
	}

	// initialize warehouse service after core to handle non-normal recovery modes
	if canStartWarehouse() {
		rruntime.Go(func() {
			startWarehouseService()
		})
	}

	misc.KeepProcessAlive()
}
