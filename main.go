package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/bugsnag/bugsnag-go"

	"github.com/rudderlabs/rudder-server/processor/transformer"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/app/apphandlers"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/warehouse"

	// This is necessary for compatibility with enterprise features
	_ "github.com/rudderlabs/rudder-server/imports"
)

var (
	application               app.Interface
	warehouseMode             string
	enableSuppressUserFeature bool
	pkgLogger                 logger.LoggerI
	appHandler                apphandlers.AppHandler
)

var version = "Not an official release. Get the latest release from the github repo."
var major, minor, commit, buildDate, builtBy, gitURL, patch string

func loadConfig() {
	warehouseMode = config.GetString("Warehouse.mode", "embedded")
	enableSuppressUserFeature = config.GetBool("Gateway.enableSuppressUserFeature", false)
}

func init() {
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

func startWarehouseService() {
	warehouse.Start()
}

func canStartServer() bool {
	pkgLogger.Info("warehousemode ", warehouseMode)
	return warehouseMode == config.EmbeddedMode || warehouseMode == config.OffMode || warehouseMode == config.PooledWHSlaveMode
}

func canStartWarehouse() bool {
	return warehouseMode != config.OffMode
}

func main() {
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
	ctx := bugsnag.StartSession(context.Background())
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

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		application.Stop()
		// clearing zap Log buffer to std output
		if logger.Log != nil {
			logger.Log.Sync()
		}
		stats.StopRuntimeStats()
		os.Exit(1)
	}()

	misc.AppStartTime = time.Now().Unix()
	if canStartServer() {
		appHandler.HandleRecovery(options)
		rruntime.Go(func() {
			appHandler.StartRudderCore(options)
		})
	}

	// initialize warehouse service after core to handle non-normal recovery modes
	if appTypeStr != app.GATEWAY && canStartWarehouse() {
		rruntime.Go(func() {
			startWarehouseService()
		})
	}

	rruntime.Go(admin.StartServer)

	misc.KeepProcessAlive()
}
