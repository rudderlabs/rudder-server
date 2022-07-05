package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
	"syscall"
	"time"

	"github.com/bugsnag/bugsnag-go/v2"
	_ "go.uber.org/automaxprocs"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/admin/profiler"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/app/apphandlers"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	eventschema "github.com/rudderlabs/rudder-server/event-schema"
	"github.com/rudderlabs/rudder-server/gateway"
	"github.com/rudderlabs/rudder-server/gateway/webhook"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/processor/stash"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	ratelimiter "github.com/rudderlabs/rudder-server/rate-limiter"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager"
	"github.com/rudderlabs/rudder-server/router/customdestinationmanager"
	oauth "github.com/rudderlabs/rudder-server/router/oauthResponseHandler"
	routertransformer "github.com/rudderlabs/rudder-server/router/transformer"
	batchrouterutils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/services/alert"
	"github.com/rudderlabs/rudder-server/services/archiver"
	"github.com/rudderlabs/rudder-server/services/db"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/debugger/source"
	transformationdebugger "github.com/rudderlabs/rudder-server/services/debugger/transformation"
	"github.com/rudderlabs/rudder-server/services/dedup"
	destinationconnectiontester "github.com/rudderlabs/rudder-server/services/destination-connection-tester"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/multitenant"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/services/streammanager/kafka"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse"
	azuresynapse "github.com/rudderlabs/rudder-server/warehouse/azure-synapse"
	"github.com/rudderlabs/rudder-server/warehouse/bigquery"
	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/configuration_testing"
	"github.com/rudderlabs/rudder-server/warehouse/deltalake"
	"github.com/rudderlabs/rudder-server/warehouse/mssql"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/redshift"
	"github.com/rudderlabs/rudder-server/warehouse/snowflake"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

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

var (
	version                                                                  = "Not an official release. Get the latest release from the github repo."
	major, minor, commit, buildDate, builtBy, gitURL, patch, enterpriseToken string
)

func loadConfig() {
	config.RegisterStringConfigVariable("embedded", &warehouseMode, false, "Warehouse.mode")
	config.RegisterBoolConfigVariable(true, &enableSuppressUserFeature, false, "Gateway.enableSuppressUserFeature")
	config.RegisterDurationConfigVariable(0, &ReadTimeout, false, time.Second, []string{"ReadTimeOut", "ReadTimeOutInSec"}...)
	config.RegisterDurationConfigVariable(0, &ReadHeaderTimeout, false, time.Second, []string{"ReadHeaderTimeout", "ReadHeaderTimeoutInSec"}...)
	config.RegisterDurationConfigVariable(10, &WriteTimeout, false, time.Second, []string{"WriteTimeout", "WriteTimeOutInSec"}...)
	config.RegisterDurationConfigVariable(720, &IdleTimeout, false, time.Second, []string{"IdleTimeout", "IdleTimeoutInSec"}...)
	config.RegisterDurationConfigVariable(15, &gracefulShutdownTimeout, false, time.Second, "GracefulShutdownTimeout")
	config.RegisterIntConfigVariable(524288, &MaxHeaderBytes, false, 1, "MaxHeaderBytes")

	enterpriseToken = config.GetEnv("ENTERPRISE_TOKEN", enterpriseToken)
}

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("main")
}

func versionInfo() map[string]interface{} {
	return map[string]interface{}{"Version": version, "Major": major, "Minor": minor, "Patch": patch, "Commit": commit, "BuildDate": buildDate, "BuiltBy": builtBy, "GitUrl": gitURL, "TransformerVersion": transformer.GetVersion(), "DatabricksVersion": misc.GetDatabricksVersion()}
}

func versionHandler(w http.ResponseWriter, _ *http.Request) {
	version := versionInfo()
	versionFormatted, _ := json.Marshal(&version)
	_, _ = w.Write(versionFormatted)
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
	stats.Setup()
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
	destinationconnectiontester.Init()
	warehouse.Init()
	warehouse.Init2()
	warehouse.Init3()
	warehouse.Init4()
	warehouse.Init5()
	warehouse.Init6()
	configuration_testing.Init()
	azuresynapse.Init()
	mssql.Init()
	postgres.Init()
	redshift.Init()
	snowflake.Init()
	deltalake.Init()
	transformer.Init()
	webhook.Init()
	batchrouter.Init()
	batchrouter.Init2()
	asyncdestinationmanager.Init()
	batchrouterutils.Init()
	dedup.Init()
	eventschema.Init()
	eventschema.Init2()
	stash.Init()
	transformationdebugger.Init()
	processor.Init()
	kafka.Init()
	customdestinationmanager.Init()
	routertransformer.Init()
	router.Init()
	router.InitRouterAdmin()
	ratelimiter.Init()
	sourcedebugger.Init()
	gateway.Init()
	apphandlers.Init()
	apphandlers.Init2()
	integrations.Init()
	alert.Init()
	multitenant.Init()
	oauth.Init()
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

	options.EnterpriseToken = enterpriseToken

	application = app.New(options)

	// application & backend setup should be done before starting any new goroutines.
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
	defer misc.BugsnagNotify(ctx, "Core")()

	configEnvHandler := application.Features().ConfigEnv.Setup()

	if config.GetEnv("RSERVER_WAREHOUSE_MODE", "") != "slave" {
		if err := backendconfig.Setup(configEnvHandler); err != nil {
			pkgLogger.Errorf("Unable to setup backend config: %s", err)
			return
		}

		backendconfig.DefaultBackendConfig.StartWithIDs(ctx, backendconfig.DefaultBackendConfig.AccessToken())
	}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return admin.StartServer(ctx)
	})

	g.Go(func() error {
		p := &profiler.Profiler{}
		return p.StartServer(ctx)
	})

	misc.AppStartTime = time.Now().Unix()
	if canStartServer() {
		appHandler.HandleRecovery(options)
		g.Go(misc.WithBugsnag(func() error {
			return appHandler.StartRudderCore(ctx, options)
		}))
	}

	// initialize warehouse service after core to handle non-normal recovery modes
	if appTypeStr != app.GATEWAY && canStartWarehouse() {
		g.Go(misc.WithBugsnagForWarehouse(func() error {
			return startWarehouseService(ctx, application)
		}))
	}

	var ctxDoneTime time.Time
	g.Go(func() error {
		<-ctx.Done()
		ctxDoneTime = time.Now()
		return nil
	})

	g.Go(func() error {
		<-ctx.Done()
		backendconfig.DefaultBackendConfig.Stop()
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
		_ = pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		fmt.Print("\n\n")

		application.Stop()
		if logger.Log != nil {
			_ = logger.Log.Sync()
		}
		stats.StopPeriodicStats()
		if config.GetEnvAsBool("RUDDER_GRACEFUL_SHUTDOWN_TIMEOUT_EXIT", true) {
			os.Exit(1)
		}
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
		_ = logger.Log.Sync()
	}
	stats.StopPeriodicStats()
}
