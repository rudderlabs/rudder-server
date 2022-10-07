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

	"github.com/rudderlabs/rudder-server/info"
	"github.com/rudderlabs/rudder-server/warehouse/datalake"

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
	"github.com/rudderlabs/rudder-server/services/controlplane/features"
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
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
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
)

var (
	application               app.App
	warehouseMode             string
	enableSuppressUserFeature bool
	pkgLogger                 logger.Logger
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

	enterpriseToken = config.GetString("ENTERPRISE_TOKEN", enterpriseToken)
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

func startWarehouseService(ctx context.Context, application app.App) error {
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
	admin.Init()
	misc.Init()
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
	datalake.Init()
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
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	exitCode := Run(ctx)
	cancel()
	os.Exit(exitCode)
}

func Run(ctx context.Context) int {
	runAllInit()

	options := app.LoadOptions()
	if options.VersionFlag {
		printVersion()
		return 0
	}

	options.EnterpriseToken = enterpriseToken

	application = app.New(options)

	// application & backend setup should be done before starting any new goroutines.
	application.Setup()

	appTypeStr := strings.ToUpper(config.GetString("APP_TYPE", app.EMBEDDED))
	appHandler = apphandlers.GetAppHandler(application, appTypeStr, versionHandler)

	versionDetail := versionInfo()
	bugsnag.Configure(bugsnag.Configuration{
		APIKey:       config.GetString("BUGSNAG_KEY", ""),
		ReleaseStage: config.GetString("GO_ENV", "development"),
		// The import paths for the Go packages containing your source files
		ProjectPackages: []string{"main", "github.com/rudderlabs/rudder-server"},
		// more configuration options
		AppType:      appHandler.GetAppType(),
		AppVersion:   versionDetail["Version"].(string),
		PanicHandler: func() {},
	})
	ctx = bugsnag.StartSession(ctx)
	defer misc.BugsnagNotify(ctx, "Core")()

	deploymentType, err := deployment.GetFromEnv()
	if err != nil {
		pkgLogger.Errorf("failed to get deployment type: %w", err)
		return 1
	}
	// TODO: remove as soon as we update the configuration with statsExcludedTags where necessary
	if !config.IsSet("statsExcludedTags") && deploymentType == deployment.MultiTenantType && (!config.IsSet("WORKSPACE_NAMESPACE") || strings.Contains(config.GetString("WORKSPACE_NAMESPACE", ""), "free")) {
		config.Set("statsExcludedTags", []string{"workspaceId"})
	}
	stats.Default.Start(ctx)
	stats.Default.NewTaggedStat("rudder_server_config", stats.GaugeType, stats.Tags{"version": version, "major": major, "minor": minor, "patch": patch, "commit": commit, "buildDate": buildDate, "builtBy": builtBy, "gitUrl": gitURL, "TransformerVersion": transformer.GetVersion(), "DatabricksVersion": misc.GetDatabricksVersion()}).Gauge(1)

	configEnvHandler := application.Features().ConfigEnv.Setup()

	if config.GetString("Warehouse.mode", "") != "slave" {
		if err := backendconfig.Setup(configEnvHandler); err != nil {
			pkgLogger.Errorf("Unable to setup backend config: %s", err)
			return 1
		}

		backendconfig.DefaultBackendConfig.StartWithIDs(ctx, "")
	}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := admin.StartServer(ctx); err != nil {
			return fmt.Errorf("admin server routine: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		p := &profiler.Profiler{}
		if err := p.StartServer(ctx); err != nil {
			return fmt.Errorf("profiler server routine: %w", err)
		}
		return nil
	})

	misc.AppStartTime = time.Now().Unix()
	if canStartServer() {
		appHandler.HandleRecovery(options)
		g.Go(misc.WithBugsnag(func() (err error) {
			if err := appHandler.StartRudderCore(ctx, options); err != nil {
				return fmt.Errorf("rudder core: %w", err)
			}
			return nil
		}))
		g.Go(misc.WithBugsnag(func() error {
			backendconfig.DefaultBackendConfig.WaitForConfig(ctx)

			c := features.NewClient(
				config.GetString("CONFIG_BACKEND_URL", "https://api.rudderlabs.com"),
				backendconfig.DefaultBackendConfig.Identity(),
			)

			err := c.Send(ctx, info.ServerComponent.Name, info.ServerComponent.Features)
			if err != nil {
				pkgLogger.Errorf("error sending server features: %v", err)
			}

			// we don't want to exit if we can't send server features
			return nil
		}))
	}

	// initialize warehouse service after core to handle non-normal recovery modes
	if appTypeStr != app.GATEWAY && canStartWarehouse() {
		g.Go(misc.WithBugsnagForWarehouse(func() error {
			if err := startWarehouseService(ctx, application); err != nil {
				return fmt.Errorf("warehouse service routine: %w", err)
			}
			return nil
		}))
	}

	shutdownDone := make(chan struct{})
	go func() {
		err := g.Wait()
		if err != nil {
			pkgLogger.Errorf("Terminal error: %v", err)
		}

		pkgLogger.Info("Attempting to shutdown gracefully")
		backendconfig.DefaultBackendConfig.Stop()
		close(shutdownDone)
	}()

	<-ctx.Done()
	ctxDoneTime := time.Now()

	select {
	case <-shutdownDone:
		application.Stop()
		pkgLogger.Infof(
			"Graceful terminal after %s, with %d go-routines",
			time.Since(ctxDoneTime),
			runtime.NumGoroutine(),
		)
		// clearing zap Log buffer to std output
		logger.Sync()
		stats.Default.Stop()
	case <-time.After(gracefulShutdownTimeout):
		// Assume graceful shutdown failed, log remain goroutines and force kill
		pkgLogger.Errorf(
			"Graceful termination failed after %s, goroutine dump:\n",
			time.Since(ctxDoneTime),
		)

		fmt.Print("\n\n")
		_ = pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		fmt.Print("\n\n")

		application.Stop()
		logger.Sync()
		stats.Default.Stop()
		if config.GetBool("RUDDER_GRACEFUL_SHUTDOWN_TIMEOUT_EXIT", true) {
			return 1
		}
	}

	return 0
}
