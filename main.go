package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/pprof"
	"strconv"
	"strings"

	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/bugsnag/bugsnag-go"
	"github.com/gorilla/mux"
	_ "go.uber.org/automaxprocs"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-server/gateway"
	"github.com/rudderlabs/rudder-server/gateway/webhook"
	"github.com/rudderlabs/rudder-server/jobsdb"
	operationmanager "github.com/rudderlabs/rudder-server/operation-manager"
	"github.com/rudderlabs/rudder-server/processor"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/processor/stash"
	"github.com/rudderlabs/rudder-server/processor/transformer"

	ratelimiter "github.com/rudderlabs/rudder-server/rate-limiter"

	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager"
	"github.com/rudderlabs/rudder-server/router/customdestinationmanager"
	routertransformer "github.com/rudderlabs/rudder-server/router/transformer"
	batchrouterutils "github.com/rudderlabs/rudder-server/router/utils"

	event_schema "github.com/rudderlabs/rudder-server/event-schema"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/app/apphandlers"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/alert"
	"github.com/rudderlabs/rudder-server/services/archiver"
	"github.com/rudderlabs/rudder-server/services/db"

	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/debugger/source"
	transformationdebugger "github.com/rudderlabs/rudder-server/services/debugger/transformation"

	"github.com/rudderlabs/rudder-server/services/dedup"
	"github.com/rudderlabs/rudder-server/services/streammanager/kafka"

	destination_connection_tester "github.com/rudderlabs/rudder-server/services/destination-connection-tester"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	"github.com/rudderlabs/rudder-server/services/stats"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	azuresynapse "github.com/rudderlabs/rudder-server/warehouse/azure-synapse"
	"github.com/rudderlabs/rudder-server/warehouse/bigquery"
	"github.com/rudderlabs/rudder-server/warehouse/mssql"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/redshift"
	"github.com/rudderlabs/rudder-server/warehouse/snowflake"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	"github.com/rudderlabs/rudder-server/warehouse"
	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"

	// This is necessary for compatibility with enterprise features
	_ "github.com/rudderlabs/rudder-server/imports"
)

var (
	application               app.Interface
	warehouseMode             string
	enableSuppressUserFeature bool
	pkgLogger                 logger.LoggerI
	appHandler                apphandlers.AppHandler
	ReadTimeout               time.Duration
	ReadHeaderTimeout         time.Duration
	WriteTimeout              time.Duration
	IdleTimeout               time.Duration
	gracefulShutdownTimeout   time.Duration
	MaxHeaderBytes            int
)

var version = "Not an official release. Get the latest release from the github repo."
var major, minor, commit, buildDate, builtBy, gitURL, patch string

func loadConfig() {
	config.RegisterStringConfigVariable("embedded", &warehouseMode, false, "Warehouse.mode")
	config.RegisterBoolConfigVariable(true, &enableSuppressUserFeature, false, "Gateway.enableSuppressUserFeature")
	config.RegisterDurationConfigVariable(time.Duration(0), &ReadTimeout, false, time.Second, []string{"ReadTimeOut", "ReadTimeOutInSec"}...)
	config.RegisterDurationConfigVariable(time.Duration(0), &ReadHeaderTimeout, false, time.Second, []string{"ReadHeaderTimeout", "ReadHeaderTimeoutInSec"}...)
	config.RegisterDurationConfigVariable(time.Duration(10), &WriteTimeout, false, time.Second, []string{"WriteTimeout", "WriteTimeOutInSec"}...)
	config.RegisterDurationConfigVariable(time.Duration(720), &IdleTimeout, false, time.Second, []string{"IdleTimeout", "IdleTimeoutInSec"}...)
	config.RegisterDurationConfigVariable(time.Duration(15), &gracefulShutdownTimeout, false, time.Second, "GracefulShutdownTimeout")
	config.RegisterIntConfigVariable(524288, &MaxHeaderBytes, false, 1, "MaxHeaderBytes")

}

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("main")
}

func versionInfo() map[string]interface{} {
	return map[string]interface{}{"Version": version, "Major": major, "Minor": minor, "Patch": patch, "Commit": commit, "BuildDate": buildDate, "BuiltBy": builtBy, "GitUrl": gitURL, "TransformerVersion": transformer.GetVersion()}
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

func startWarehouseService(ctx context.Context, application app.Interface) error {
	return warehouse.Start(ctx, application)
}

func canStartServer() bool {
	pkgLogger.Info("warehousemode ", warehouseMode)
	return warehouseMode == config.EmbeddedMode || warehouseMode == config.OffMode || warehouseMode == config.PooledWHSlaveMode
}

func canStartWarehouse() bool {
	return warehouseMode != config.OffMode
}

func runAllInit() {
	config.Load()
	admin.Init()
	app.Init()
	logger.Init()
	misc.Init()
	stats.Init()
	db.Init()
	diagnostics.Init()
	backendconfig.Init()
	warehouseutils.Init()
	bigquery.Init()
	clickhouse.Init()
	archiver.Init()
	destinationdebugger.Init()
	pgnotifier.Init()
	jobsdb.Init()
	jobsdb.Init2()
	jobsdb.Init3()
	destination_connection_tester.Init()
	warehouse.Init()
	warehouse.Init2()
	warehouse.Init3()
	warehouse.Init4()
	warehouse.Init5()
	azuresynapse.Init()
	mssql.Init()
	postgres.Init()
	redshift.Init()
	snowflake.Init()
	transformer.Init()
	webhook.Init()
	batchrouter.Init()
	batchrouter.Init2()
	asyncdestinationmanager.Init()
	batchrouterutils.Init()
	dedup.Init()
	event_schema.Init()
	event_schema.Init2()
	stash.Init()
	transformationdebugger.Init()
	processor.Init()
	kafka.Init()
	customdestinationmanager.Init()
	routertransformer.Init()
	router.Init()
	router.Init2()
	operationmanager.Init()
	operationmanager.Init2()
	ratelimiter.Init()
	sourcedebugger.Init()
	gateway.Init()
	apphandlers.Init()
	apphandlers.Init2()
	rruntime.Init()
	integrations.Init()
	alert.Init()
	Init()

}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		<-c
		cancel()
	}()

	Run(ctx)
}

func Run(ctx context.Context) {
	runAllInit()

	options := app.LoadOptions()
	if options.VersionFlag {
		printVersion()
		return
	}

	application = app.New(options)

	//application & backend setup should be done before starting any new goroutines.
	application.Setup()

	appTypeStr := strings.ToUpper(config.GetEnv("APP_TYPE", app.EMBEDDED))
	appHandler = apphandlers.GetAppHandler(application, appTypeStr, versionHandler)

	version := versionInfo()
	bugsnag.Configure(bugsnag.Configuration{
		APIKey:       config.GetEnv("BUGSNAG_KEY", ""),
		ReleaseStage: config.GetEnv("GO_ENV", "development"),
		// The import paths for the Go packages containing your source files
		ProjectPackages: []string{"main", "github.com/rudderlabs/rudder-server"},
		// more configuration options
		AppType:      appHandler.GetAppType(),
		AppVersion:   version["Version"].(string),
		PanicHandler: func() {},
	})
	ctx = bugsnag.StartSession(ctx)
	defer func() {
		if r := recover(); r != nil {
			defer bugsnag.AutoNotify(ctx, bugsnag.SeverityError, bugsnag.MetaData{
				"GoRoutines": {
					"Number": runtime.NumGoroutine(),
				}})

			misc.RecordAppError(fmt.Errorf("%v", r))
			pkgLogger.Fatal(r)
			panic(r)
		}
	}()

	//Creating Stats Client should be done right after setting up logger and before setting up other modules.
	stats.Setup()

	var pollRegulations bool
	if enableSuppressUserFeature {
		if application.Features().SuppressUser != nil {
			pollRegulations = true
		} else {
			pkgLogger.Info("Suppress User feature is enterprise only. Unable to poll regulations.")
		}
	}

	var configEnvHandler types.ConfigEnvI
	if application.Features().ConfigEnv != nil {
		configEnvHandler = application.Features().ConfigEnv.Setup()
	}

	backendconfig.Setup(pollRegulations, configEnvHandler)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return admin.StartServer(ctx)
	})

	misc.AppStartTime = time.Now().Unix()
	//If the server is standby mode, then no major services (gateway, processor, routers...) run
	if options.StandByMode {
		appHandler.HandleRecovery(options)
		g.Go(func() error {
			return startStandbyWebHandler(ctx)
		})
	} else {
		if canStartServer() {
			appHandler.HandleRecovery(options)
			g.Go(misc.WithBugsnag(func() error {
				return appHandler.StartRudderCore(ctx, options)
			}))
		}

		// initialize warehouse service after core to handle non-normal recovery modes
		if appTypeStr != app.GATEWAY && canStartWarehouse() {
			g.Go(misc.WithBugsnag(func() error {
				return startWarehouseService(ctx, application)
			}))
		}
	}

	var ctxDoneTime time.Time
	g.Go(func() error {
		<-ctx.Done()
		ctxDoneTime = time.Now()
		return nil
	})

	go func() {
		<-ctx.Done()
		<-time.After(gracefulShutdownTimeout)
		// Assume graceful shutdown failed, log remain goroutines and force kill
		pkgLogger.Errorf(
			"Graceful termination failed after %s, goroutine dump:\n",
			gracefulShutdownTimeout,
		)

		fmt.Print("\n\n")
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		fmt.Print("\n\n")

		application.Stop()
		if logger.Log != nil {
			logger.Log.Sync()
		}
		stats.StopRuntimeStats()

		os.Exit(1)
	}()

	err := g.Wait()
	if err != nil && err != context.Canceled {
		pkgLogger.Error(err)
	}

	application.Stop()

	pkgLogger.Infof(
		"Graceful terminal after %s, with %d go-routines",
		time.Since(ctxDoneTime),
		runtime.NumGoroutine(),
	)
	// clearing zap Log buffer to std output
	if logger.Log != nil {
		logger.Log.Sync()
	}
	stats.StopRuntimeStats()
}

func startStandbyWebHandler(ctx context.Context) error {
	webPort := getWebPort()
	srvMux := mux.NewRouter()
	srvMux.HandleFunc("/health", standbyHealthHandler)
	srvMux.HandleFunc("/", standbyHealthHandler)
	srvMux.HandleFunc("/version", versionHandler)

	// route everything else to defaultHandler:
	srvMux.PathPrefix("/").HandlerFunc(standbyDefaultHandler)

	srv := &http.Server{
		Addr:              ":" + strconv.Itoa(webPort),
		Handler:           bugsnag.Handler(srvMux),
		ReadTimeout:       ReadTimeout,
		ReadHeaderTimeout: ReadHeaderTimeout,
		WriteTimeout:      WriteTimeout,
		IdleTimeout:       IdleTimeout,
		MaxHeaderBytes:    MaxHeaderBytes,
	}
	func() {
		<-ctx.Done()
		srv.Shutdown(context.Background())
	}()

	if err := srv.ListenAndServe(); err != nil {
		return fmt.Errorf("web server: %w", err)
	}
	return nil
}

func getWebPort() int {
	appTypeStr := strings.ToUpper(config.GetEnv("APP_TYPE", app.EMBEDDED))
	switch appTypeStr {
	case app.GATEWAY:
		return config.GetInt("Gateway.webPort", 8080)
	case app.PROCESSOR:
		return config.GetInt("Processor.webPort", 8086)
	case app.EMBEDDED:
		return config.GetInt("Gateway.webPort", 8080)
	}

	panic(errors.New("invalid app type"))
}

//StandbyHealthHandler is the http handler for health endpoint
func standbyHealthHandler(w http.ResponseWriter, r *http.Request) {
	appTypeStr := strings.ToUpper(config.GetEnv("APP_TYPE", app.EMBEDDED))
	healthVal := fmt.Sprintf(`{"appType": "%s", "mode":"%s"}`, appTypeStr, strings.ToUpper(db.CurrentMode))
	w.Write([]byte(healthVal))
}

//StandbyDefaultHandler is the http handler for health endpoint
func standbyDefaultHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Server is in standby mode. Please retry after sometime", 500)
}
