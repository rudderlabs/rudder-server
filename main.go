package main

import (
	"flag"
	"fmt"
	"runtime"
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
)

func loadConfig() {
	maxProcess = config.GetInt("maxProcess", 12)
	gwDBRetention = config.GetDuration("gwDBRetention", time.Duration(1)) * time.Hour
	routerDBRetention = config.GetDuration("routerDBRetention", 0)
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
		router.MakeSleepToZero()
	}
}

func init() {
	if err := godotenv.Load(); err != nil {
		fmt.Println("No .env file found")
	}
	config.Initialize()
}

func main() {
	loadConfig()
	misc.SetupLogger()
	fmt.Println("Main starting")
	var gatewayDB jobsdb.HandleT
	var routerDB jobsdb.HandleT
	runtime.GOMAXPROCS(maxProcess)

	//Flag determines if we reset the databases
	clearDB := flag.Bool("cleardb", false, "a bool")
	flag.Parse()
	fmt.Println("Clearing DB", *clearDB)
	gatewayDB.Setup(*clearDB, "gw", gwDBRetention)
	routerDB.Setup(*clearDB, "rt", routerDBRetention)

	//Setup the three modules, the gateway, the router and the processor
	var gateway gateway.HandleT

	var processor processor.HandleT

	//The router module should be setup for
	//all the enabled destinations
	for _, dest := range integrations.GetAllDestinations() {
		var router router.HandleT
		fmt.Println("Enabling Destination", dest)
		router.Setup(&routerDB, dest)
	}

	//go readIOforResume(router) //keeping it as input from IO, to be replaced by UI

	processor.Setup(&gatewayDB, &routerDB)
	gateway.Setup(&gatewayDB)
}
