package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/gateway"
	"github.com/rudderlabs/rudder-server/integrations"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/misc"
	"github.com/rudderlabs/rudder-server/processor"
	"github.com/rudderlabs/rudder-server/router"
)

var (
	maxProcess                       int
	gwDBRetention, routerDBRetention time.Duration
	enableProcessor, enableRouter    bool
)

func loadConfig() {
	maxProcess = config.GetInt("maxProcess", 12)
	gwDBRetention = config.GetDuration("gwDBRetention", time.Duration(1)) * time.Hour
	routerDBRetention = config.GetDuration("routerDBRetention", 0)
	enableProcessor = config.GetBool("enableProcessor", true)
	enableRouter = config.GetBool("enableRouter", true)
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

func enableMonitorDestRouters(routeDb *jobsdb.HandleT) {
	dstToRouter := make(map[string]*router.HandleT)
	for {
		//Get all the destinations and start the rotuer if
		//not already enabled
		for _, dest := range integrations.GetAllDestinations() {
			rt, ok := dstToRouter[dest]
			if !ok {
				fmt.Println("Enabling Destination", dest)
				var router router.HandleT
				router.Setup(routeDb, dest)
				dstToRouter[dest] = &router
			} else {
				rt.Enable()
			}
		}

		//Iterate through the existing routers and disable
		//which have been removed from config
		for d, rtHandle := range dstToRouter {
			found := false
			for _, dstID := range integrations.GetAllDestinations() {
				if d == dstID {
					found = true
					break
				}
			}
			//Router is not in enabled list. Disable it
			if !found {
				rtHandle.Disable()
			}
		}
		//Sleep before the next round
		<-time.After(5 * time.Second)
	}
}

func init() {
	if err := godotenv.Load(); err != nil {
		fmt.Println("No .env file found")
	}
	config.Initialize()
	loadConfig()
}

func main() {
	fmt.Println("Main starting")
	clearDB := flag.Bool("cleardb", false, "a bool")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to `file`")
	memprofile := flag.String("memprofile", "", "write memory profile to `file`")

	flag.Parse()

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
	gatewayDB.Setup(*clearDB, "gw", gwDBRetention, true)
	routerDB.Setup(*clearDB, "rt", routerDBRetention, false)

	//Setup the three modules, the gateway, the router and the processor

	if enableRouter {
		go enableMonitorDestRouters(&routerDB)
	}

	if enableProcessor {
		var processor processor.HandleT
		processor.Setup(&gatewayDB, &routerDB)
	}

	var gateway gateway.HandleT
	gateway.Setup(&gatewayDB)
	//go readIOforResume(router) //keeping it as input from IO, to be replaced by UI
}
