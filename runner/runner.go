package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	_ "go.uber.org/automaxprocs"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/profiler"
	"github.com/rudderlabs/rudder-go-kit/stats"
	svcMetric "github.com/rudderlabs/rudder-go-kit/stats/metric"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/app/apphandlers"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/info"
	"github.com/rudderlabs/rudder-server/router/customdestinationmanager"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/alert"
	"github.com/rudderlabs/rudder-server/services/controlplane"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/oauth"
	"github.com/rudderlabs/rudder-server/services/streammanager/kafka"
	"github.com/rudderlabs/rudder-server/utils/crash"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
	"github.com/rudderlabs/rudder-server/warehouse"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

// ReleaseInfo holds the release information
type ReleaseInfo struct {
	Version         string
	Commit          string
	BuildDate       string
	BuiltBy         string
	EnterpriseToken string
}

// Runner is responsible for running the application
type Runner struct {
	appType                   string
	application               app.App
	releaseInfo               ReleaseInfo
	warehouseMode             string
	warehouseApp              *warehouse.App
	enableSuppressUserFeature bool
	logger                    logger.Logger
	appHandler                apphandlers.AppHandler
	gracefulShutdownTimeout   time.Duration
}

// New creates and initializes a new Runner
func New(releaseInfo ReleaseInfo) *Runner {
	return &Runner{
		appType:                   strings.ToUpper(config.GetString("APP_TYPE", app.EMBEDDED)),
		releaseInfo:               releaseInfo,
		logger:                    logger.NewLogger().Child("runner"),
		warehouseMode:             config.GetString("Warehouse.mode", "embedded"),
		enableSuppressUserFeature: config.GetBool("Gateway.enableSuppressUserFeature", true),
		gracefulShutdownTimeout:   config.GetDuration("GracefulShutdownTimeout", 15, time.Second),
	}
}

// Run runs the application and returns the exit code
func (r *Runner) Run(ctx context.Context, args []string) int {
	// Start stats
	deploymentType, err := deployment.GetFromEnv()
	if err != nil {
		r.logger.Errorf("failed to get deployment type: %v", err)
		return 1
	}

	path, err := config.Default.ConfigFileUsed()
	if err != nil {
		r.logger.Warnf("Config: Failed to parse config file from path %q, using default values: %v", path, err)
	} else {
		r.logger.Infof("Config: Using config file: %s", path)
	}

	if err := config.Default.DotEnvLoaded(); err != nil {
		r.logger.Infof("Config: No .env file loaded: %v", err)
	} else {
		r.logger.Infof("Config: Loaded .env file")
	}

	// TODO: remove as soon as we update the configuration with statsExcludedTags where necessary
	if !config.IsSet("statsExcludedTags") && deploymentType == deployment.MultiTenantType &&
		(!config.IsSet("WORKSPACE_NAMESPACE") || strings.Contains(config.GetString("WORKSPACE_NAMESPACE", ""), "free")) {
		config.Set("statsExcludedTags", []string{"workspaceId", "sourceID", "destId"})
	}
	statsOptions := []stats.Option{
		stats.WithServiceName(r.appType),
		stats.WithServiceVersion(r.releaseInfo.Version),
	}
	if r.canStartWarehouse() {
		statsOptions = append(statsOptions, stats.WithDefaultHistogramBuckets(defaultWarehouseHistogramBuckets))
		for histogramName, buckets := range customBucketsWarehouse {
			statsOptions = append(statsOptions, stats.WithHistogramBuckets(histogramName, buckets))
		}
	} else {
		statsOptions = append(statsOptions, stats.WithDefaultHistogramBuckets(defaultHistogramBuckets))
		for histogramName, buckets := range customBucketsServer {
			statsOptions = append(statsOptions, stats.WithHistogramBuckets(histogramName, buckets))
		}
	}
	for histogramName, buckets := range customBuckets {
		statsOptions = append(statsOptions, stats.WithHistogramBuckets(histogramName, buckets))
	}
	stats.Default = stats.NewStats(config.Default, logger.Default, svcMetric.Instance, statsOptions...)
	if err := stats.Default.Start(ctx, rruntime.GoRoutineFactory); err != nil {
		r.logger.Errorf("Failed to start stats: %v", err)
		return 1
	}

	runAllInit()

	options := app.LoadOptions(args)
	if options.VersionFlag {
		r.printVersion()
		return 0
	}

	options.EnterpriseToken = r.releaseInfo.EnterpriseToken

	r.application = app.New(options)

	// application & backend setup should be done before starting any new goroutines.
	r.application.Setup()

	r.appHandler, err = apphandlers.GetAppHandler(r.application, r.appType, r.versionHandler)
	if err != nil {
		r.logger.Errorf("Failed to get app handler: %v", err)
		return 1
	}

	crash.Configure(r.logger, crash.PanicWrapperOpts{
		ReleaseStage: config.GetString("GO_ENV", "development"),
		AppType:      fmt.Sprintf("rudder-server-%s", r.appType),
		AppVersion:   r.releaseInfo.Version,
	})
	defer crash.Notify("Core")()

	stats.Default.NewTaggedStat("rudder_server_config",
		stats.GaugeType,
		stats.Tags{
			"version":   r.releaseInfo.Version,
			"commit":    r.releaseInfo.Commit,
			"buildDate": r.releaseInfo.BuildDate,
			"builtBy":   r.releaseInfo.BuiltBy,
		}).Gauge(1)

	configEnvHandler := r.application.Features().ConfigEnv.Setup()

	if err := backendconfig.Setup(configEnvHandler); err != nil {
		r.logger.Errorf("Unable to setup backend config: %s", err)
		return 1
	}
	backendconfig.DefaultBackendConfig.StartWithIDs(ctx, "")

	// Prepare databases in sequential order, so that failure in one doesn't affect others (leaving dirty schema migration state)
	if r.canStartServer() {
		if err := r.appHandler.Setup(); err != nil {
			r.logger.Errorf("Unable to prepare rudder-core database: %s", err)
			return 1
		}
	}
	if r.canStartWarehouse() {
		r.warehouseApp = warehouse.New(
			r.application,
			config.Default,
			r.logger,
			stats.Default,
			backendconfig.DefaultBackendConfig,
			filemanager.New,
		)

		if err := r.warehouseApp.Setup(ctx); err != nil {
			r.logger.Errorf("Unable to prepare warehouse database: %s", err)
			return 1
		}
	}
	g, ctx := errgroup.WithContext(ctx)

	// Start admin server
	if config.GetBool("AdminServer.enabled", true) {
		g.Go(func() error {
			if err := admin.StartServer(ctx); err != nil {
				return fmt.Errorf("admin server routine: %w", err)
			}
			return nil
		})
	}

	if config.GetBool("Profiler.Enabled", true) {
		g.Go(func() error {
			return profiler.StartServer(ctx, config.GetInt("Profiler.Port", 7777))
		})
	}

	// Start rudder core
	if r.canStartServer() {
		g.Go(crash.Wrapper(func() (err error) {
			if err := r.appHandler.StartRudderCore(ctx, options); err != nil {
				return fmt.Errorf("rudder core: %w", err)
			}
			return nil
		}))
		g.Go(crash.Wrapper(func() error {
			backendconfig.DefaultBackendConfig.WaitForConfig(ctx)

			c := controlplane.NewClient(
				config.GetString("CONFIG_BACKEND_URL", "https://api.rudderstack.com"),
				backendconfig.DefaultBackendConfig.Identity(),
			)

			err := c.SendFeatures(ctx, info.ServerComponent.Name, info.ServerComponent.Features)
			if err != nil {
				r.logger.Errorf("error sending server features: %v", err)
			}

			// we don't want to exit if we can't send server features
			return nil
		}))
	}

	// Start warehouse
	// initialize warehouse service after core to handle non-normal recovery modes
	if r.canStartWarehouse() {
		g.Go(crash.NotifyWarehouse(func() error {
			if err := r.warehouseApp.Run(ctx); err != nil {
				return fmt.Errorf("warehouse service routine: %w", err)
			}
			return nil
		}))
	}

	shutdownDone := make(chan struct{})
	go func() {
		err := g.Wait()
		if err != nil {
			r.logger.Errorf("Terminal error: %v", err)
		}

		r.logger.Info("Attempting to shutdown gracefully")
		backendconfig.DefaultBackendConfig.Stop()
		close(shutdownDone)
	}()

	<-ctx.Done()
	ctxDoneTime := time.Now()

	select {
	case <-shutdownDone:
		r.application.Stop()
		r.logger.Infof(
			"Graceful termination after %s, with %d go-routines",
			time.Since(ctxDoneTime),
			runtime.NumGoroutine(),
		)
		// clearing zap Log buffer to std output
		logger.Sync()
		stats.Default.Stop()
	case <-time.After(r.gracefulShutdownTimeout):
		// Assume graceful shutdown failed, log remain goroutines and force kill
		r.logger.Errorf(
			"Graceful termination failed after %s, goroutine dump:\n",
			time.Since(ctxDoneTime),
		)

		fmt.Print("\n\n")
		_ = pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		fmt.Print("\n\n")

		r.application.Stop()
		logger.Sync()
		stats.Default.Stop()
		if config.GetBool("RUDDER_GRACEFUL_SHUTDOWN_TIMEOUT_EXIT", true) {
			return 1
		}
	}

	return 0
}

func runAllInit() {
	admin.Init()
	misc.Init()
	diagnostics.Init()
	backendconfig.Init()
	warehouseutils.Init()
	validations.Init()
	kafka.Init()
	customdestinationmanager.Init()
	alert.Init()
	oauth.Init()
}

func (r *Runner) versionInfo() map[string]interface{} {
	return map[string]interface{}{
		"Version":   r.releaseInfo.Version,
		"Commit":    r.releaseInfo.Commit,
		"BuildDate": r.releaseInfo.BuildDate,
		"BuiltBy":   r.releaseInfo.BuiltBy,
		"Features":  info.ServerComponent.Features,
	}
}

func (r *Runner) versionHandler(w http.ResponseWriter, _ *http.Request) {
	version := r.versionInfo()
	versionFormatted, _ := json.Marshal(&version)
	_, _ = w.Write(versionFormatted)
}

func (r *Runner) printVersion() {
	version := r.versionInfo()
	versionFormatted, _ := json.MarshalIndent(&version, "", " ")
	fmt.Printf("Version Info %s\n", versionFormatted)
}

func (r *Runner) canStartServer() bool {
	r.logger.Info("warehousemode ", r.warehouseMode)
	return r.warehouseMode == config.EmbeddedMode || r.warehouseMode == config.OffMode || r.warehouseMode == config.EmbeddedMasterMode
}

func (r *Runner) canStartWarehouse() bool {
	return r.appType != app.GATEWAY && r.warehouseMode != config.OffMode
}
