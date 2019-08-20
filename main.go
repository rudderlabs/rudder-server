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
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/gateway"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/misc"
	"github.com/rudderlabs/rudder-server/processor"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/services/db"
	"github.com/rudderlabs/rudder-server/utils"
)

var (
	maxProcess                                  int
	gwDBRetention, routerDBRetention            time.Duration
	enableProcessor, enableRouter, enableBackup bool
	enabledDestinations                         []backendconfig.DestinationT
	configSubscriberLock                        sync.RWMutex
)

func loadConfig() {
	maxProcess = config.GetInt("maxProcess", 12)
	gwDBRetention = config.GetDuration("gwDBRetention", time.Duration(1)) * time.Hour
	routerDBRetention = config.GetDuration("routerDBRetention", 0)
	enableProcessor = config.GetBool("enableProcessor", true)
	enableRouter = config.GetBool("enableRouter", true)
	enableBackup = config.GetBool("JobsDB.enableBackup", true)
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
func monitorDestRouters(routeDb *jobsdb.HandleT) {
	ch := make(chan utils.DataEvent)
	backendconfig.Eb.Subscribe("backendconfig", ch)
	dstToRouter := make(map[string]*router.HandleT)
	for {
		config := <-ch
		fmt.Println("XXX Got config", config)
		sources := config.Data.(backendconfig.SourcesT)
		enabledDestinations = enabledDestinations[:0]
		for _, source := range sources.Sources {
			if source.Enabled {
				for _, destination := range source.Destinations {
					if destination.Enabled {
						enabledDestinations = append(enabledDestinations, destination)
						rt, ok := dstToRouter[destination.DestinationDefinition.Name]
						if !ok {
							fmt.Println("Starting a new Destination", destination.DestinationDefinition.Name)
							var router router.HandleT
							router.Setup(routeDb, destination.DestinationDefinition.Name)
							dstToRouter[destination.DestinationDefinition.Name] = &router
						} else {
							fmt.Println("Enabling existing Destination", destination.DestinationDefinition.Name)
							rt.Enable()
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
				fmt.Println("Disabling a existing destination", destID)
				rtHandle.Disable()
			}
		}
	}
}

func init() {
	config.Initialize()
	loadConfig()
}

func main() {
	fmt.Println("Main starting")
	bugsnag.Configure(bugsnag.Configuration{
		APIKey:       config.GetString("apiKey", "a82c3193aa5914abe2cfb66557f1cc2b"),
		ReleaseStage: config.GetString("releaseStage", "development"),
		// The import paths for the Go packages containing your source files
		ProjectPackages: []string{"main", "github.com/rudderlabs/rudder-server"},
		// more configuration options
		AppType: "rudder-server",
	})

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
			fmt.Println("Stopping CPU profile")
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

	misc.SetupLogger()

	runtime.GOMAXPROCS(maxProcess)
	fmt.Println("Clearing DB", *clearDB)

	backendconfig.Setup()
	gatewayDB.Setup(*clearDB, "gw", gwDBRetention, enableBackup && true)
	routerDB.Setup(*clearDB, "rt", routerDBRetention, false)
	//Setup the three modules, the gateway, the router and the processor

	if enableRouter {
		go monitorDestRouters(&routerDB)
	}

	if enableProcessor {
		var processor processor.HandleT
		processor.Setup(&gatewayDB, &routerDB)
	}

	var gateway gateway.HandleT
	gateway.Setup(&gatewayDB)
	//go readIOforResume(router) //keeping it as input from IO, to be replaced by UI
}
