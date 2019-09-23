package main

import (
	"flag"
	"fmt"
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
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	"github.com/rudderlabs/rudder-server/services/db"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/source-debugger"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	maxProcess                                  int
	gwDBRetention, routerDBRetention            time.Duration
	enableProcessor, enableRouter, enableBackup bool
	enabledDestinations                         []backendconfig.DestinationT
	configSubscriberLock                        sync.RWMutex
	rawDataDestinations                         []string
)

func loadConfig() {
	maxProcess = config.GetInt("maxProcess", 12)
	gwDBRetention = config.GetDuration("gwDBRetention", time.Duration(1)) * time.Hour
	routerDBRetention = config.GetDuration("routerDBRetention", 0)
	enableProcessor = config.GetBool("enableProcessor", true)
	enableRouter = config.GetBool("enableRouter", true)
	enableBackup = config.GetBool("JobsDB.enableBackup", true)
	rawDataDestinations = []string{"S3"}
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
	backendconfig.Eb.Subscribe("backendconfig", ch)
	dstToRouter := make(map[string]*router.HandleT)
	isBatchRouterSetup := false
	var brt batchrouter.HandleT

	for {
		config := <-ch
		logger.Debug("Got config from config-backend", config)
		sources := config.Data.(backendconfig.SourcesT)
		enabledDestinations = enabledDestinations[:0]
		enableBatchRouter := false
		for _, source := range sources.Sources {
			if source.Enabled {
				for _, destination := range source.Destinations {
					if destination.Enabled {
						enabledDestinations = append(enabledDestinations, destination)
						if misc.Contains(rawDataDestinations, destination.DestinationDefinition.Name) {
							enableBatchRouter = true
							brt.Enable()
							if !isBatchRouterSetup {
								isBatchRouterSetup = true
								brt.Setup(batchRouterDB)
							}
						} else {
							rt, ok := dstToRouter[destination.DestinationDefinition.Name]
							if !ok {
								logger.Info("Starting a new Destination", destination.DestinationDefinition.Name)
								var router router.HandleT
								router.Setup(routerDB, destination.DestinationDefinition.Name)
								dstToRouter[destination.DestinationDefinition.Name] = &router
							} else {
								logger.Info("Enabling existing Destination", destination.DestinationDefinition.Name)
								rt.Enable()
							}

						}

					}
				}
			}
		}
		for destID, rtHandle := range dstToRouter {
			found := false
			for _, dst := range enabledDestinations {
				if destID == dst.DestinationDefinition.Name {
					found = true
					break
				}
			}
			//Router is not in enabled list. Disable it
			if !found {
				logger.Info("Disabling a existing destination", destID)
				rtHandle.Disable()
			}
		}
		if !enableBatchRouter {
			brt.Disable()
		}
	}
}

func init() {
	config.Initialize()
	loadConfig()
}

func main() {
	bugsnag.Configure(bugsnag.Configuration{
		APIKey:       config.GetEnv("BUGSNAG_KEY", "a82c3193aa5914abe2cfb66557f1cc2b"),
		ReleaseStage: config.GetEnv("GO_ENV", "development"),
		// The import paths for the Go packages containing your source files
		ProjectPackages: []string{"main", "github.com/rudderlabs/rudder-server"},
		// more configuration options
		AppType: "rudder-server",
	})
	logger.Setup()
	logger.Info("Main starting")

	normalMode := flag.Bool("normal-mode", false, "a bool")
	degradedMode := flag.Bool("degraded-mode", false, "a bool")
	maintenanceMode := flag.Bool("maintenance-mode", false, "a bool")

	clearDB := flag.Bool("cleardb", false, "a bool")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to `file`")
	memprofile := flag.String("memprofile", "", "write memory profile to `file`")

	flag.Parse()

	// Check if there is a probable inconsistent state of Data
	db.HandleRecovery(*normalMode, *degradedMode, *maintenanceMode)
	//Reload Config
	loadConfig()

	var f *os.File
	if *cpuprofile != "" {
		var err error
		f, err = os.Create(*cpuprofile)
		misc.AssertError(err)
		runtime.SetBlockProfileRate(1)
		err = pprof.StartCPUProfile(f)
		misc.AssertError(err)
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
			misc.AssertError(err)
			defer f.Close()
			runtime.GC() // get up-to-date statistics
			err = pprof.WriteHeapProfile(f)
			misc.AssertError(err)
		}
		os.Exit(1)
	}()

	var gatewayDB jobsdb.HandleT
	var routerDB jobsdb.HandleT
	var batchRouterDB jobsdb.HandleT

	runtime.GOMAXPROCS(maxProcess)
	logger.Info("Clearing DB", *clearDB)

	sourcedebugger.Setup()
	backendconfig.Setup()
	gatewayDB.Setup(*clearDB, "gw", gwDBRetention, enableBackup && true)
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
	gateway.Setup(&gatewayDB)
	//go readIOforResume(router) //keeping it as input from IO, to be replaced by UI
}
