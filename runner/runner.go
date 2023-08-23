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

	"github.com/bugsnag/bugsnag-go/v2"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/profiler"
	"github.com/rudderlabs/rudder-go-kit/stats"
	svcMetric "github.com/rudderlabs/rudder-go-kit/stats/metric"
	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/app/apphandlers"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	eventschema "github.com/rudderlabs/rudder-server/event-schema"
	"github.com/rudderlabs/rudder-server/gateway/webhook"
	"github.com/rudderlabs/rudder-server/info"
	"github.com/rudderlabs/rudder-server/processor/stash"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager"
	"github.com/rudderlabs/rudder-server/router/customdestinationmanager"
	routertransformer "github.com/rudderlabs/rudder-server/router/transformer"
	batchrouterutils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/alert"
	"github.com/rudderlabs/rudder-server/services/controlplane"
	"github.com/rudderlabs/rudder-server/services/db"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/oauth"
	"github.com/rudderlabs/rudder-server/services/streammanager/kafka"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
	"github.com/rudderlabs/rudder-server/warehouse"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

var (
	defaultHistogramBuckets = []float64{
		0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60,
		300 /* 5 mins */, 600 /* 10 mins */, 1800, /* 30 mins */
	}
	defaultWarehouseHistogramBuckets = []float64{
		0.1, 0.25, 0.5, 1, 2.5, 5, 10, 60,
		300 /* 5 mins */, 600 /* 10 mins */, 1800 /* 30 mins */, 10800 /* 3 hours */, 36000, /* 10 hours */
		86400 /* 1 day */, 259200 /* 3 days */, 604800 /* 7 days */, 1209600, /* 2 weeks */
	}
)

// ReleaseInfo holds the release information
type ReleaseInfo struct {
	Version         string
	Major           string
	Minor           string
	Patch           string
	Commit          string
	BuildDate       string
	BuiltBy         string
	GitURL          string
	EnterpriseToken string
}

// Runner is responsible for running the application
type Runner struct {
	appType                   string
	application               app.App
	releaseInfo               ReleaseInfo
	warehouseMode             string
	enableSuppressUserFeature bool
	logger                    logger.Logger
	appHandler                apphandlers.AppHandler
	readTimeout               time.Duration
	readHeaderTimeout         time.Duration
	writeTimeout              time.Duration
	idleTimeout               time.Duration
	gracefulShutdownTimeout   time.Duration
	maxHeaderBytes            int
}

// New creates and initializes a new Runner
func New(releaseInfo ReleaseInfo) *Runner {
	getConfigDuration := func(defaultValueInTimescaleUnits int64, timeScale time.Duration, keys ...string) time.Duration {
		for i, key := range keys {
			if config.IsSet(key) || i == len(keys)-1 {
				return config.GetDuration(key, defaultValueInTimescaleUnits, timeScale)
			}
		}
		return 0
	}
	return &Runner{
		appType:                   strings.ToUpper(config.GetString("APP_TYPE", app.EMBEDDED)),
		releaseInfo:               releaseInfo,
		logger:                    logger.NewLogger().Child("runner"),
		warehouseMode:             config.GetString("Warehouse.mode", "embedded"),
		enableSuppressUserFeature: config.GetBool("Gateway.enableSuppressUserFeature", true),
		readTimeout:               getConfigDuration(0, time.Second, "ReadTimeOut", "ReadTimeOutInSec"),
		readHeaderTimeout:         getConfigDuration(0, time.Second, "ReadHeaderTimeout", "ReadHeaderTimeoutInSec"),
		writeTimeout:              getConfigDuration(10, time.Second, "WriteTimeout", "WriteTimeOutInSec"),
		idleTimeout:               getConfigDuration(720, time.Second, "IdleTimeout", "IdleTimeoutInSec"),
		gracefulShutdownTimeout:   config.GetDuration("GracefulShutdownTimeout", 15, time.Second),
		maxHeaderBytes:            config.GetInt("MaxHeaderBytes", 524288),
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
	} else {
		statsOptions = append(statsOptions, stats.WithDefaultHistogramBuckets(defaultHistogramBuckets))
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

	// Start bugsnag
	bugsnag.Configure(bugsnag.Configuration{
		APIKey:       config.GetString("BUGSNAG_KEY", ""),
		ReleaseStage: config.GetString("GO_ENV", "development"),
		// The import paths for the Go packages containing your source files
		ProjectPackages: []string{"main", "github.com/rudderlabs/rudder-server"},
		// more configuration options
		AppType:      fmt.Sprintf("rudder-server-%s", r.appType),
		AppVersion:   r.releaseInfo.Version,
		PanicHandler: func() {},
	})
	ctx = bugsnag.StartSession(ctx)
	defer misc.BugsnagNotify(ctx, "Core")()

	stats.Default.NewTaggedStat("rudder_server_config",
		stats.GaugeType,
		stats.Tags{
			"version":            r.releaseInfo.Version,
			"major":              r.releaseInfo.Major,
			"minor":              r.releaseInfo.Minor,
			"patch":              r.releaseInfo.Patch,
			"commit":             r.releaseInfo.Commit,
			"buildDate":          r.releaseInfo.BuildDate,
			"builtBy":            r.releaseInfo.BuiltBy,
			"gitUrl":             r.releaseInfo.GitURL,
			"TransformerVersion": transformer.GetVersion(),
		}).Gauge(1)

	configEnvHandler := r.application.Features().ConfigEnv.Setup()

	if err := backendconfig.Setup(configEnvHandler); err != nil {
		r.logger.Errorf("Unable to setup backend config: %s", err)
		return 1
	}
	backendconfig.DefaultBackendConfig.StartWithIDs(ctx, "")

	// Prepare databases in sequential order, so that failure in one doesn't affect others (leaving dirty schema migration state)
	if r.canStartServer() {
		if err := r.appHandler.Setup(options); err != nil {
			r.logger.Errorf("Unable to prepare rudder-core database: %s", err)
			return 1
		}
	}
	if r.canStartWarehouse() {
		if err := warehouse.Setup(ctx); err != nil {
			r.logger.Errorf("Unable to prepare warehouse database: %s", err)
			return 1
		}
	}
	g, ctx := errgroup.WithContext(ctx)

	// Start admin server
	g.Go(func() error {
		if err := admin.StartServer(ctx); err != nil {
			return fmt.Errorf("admin server routine: %w", err)
		}
		return nil
	})

	if config.GetBool("Profiler.Enabled", true) {
		g.Go(func() error {
			return profiler.StartServer(ctx, config.GetInt("Profiler.Port", 7777))
		})
	}

	misc.AppStartTime = time.Now().Unix()

	// Start rudder core
	if r.canStartServer() {
		g.Go(misc.WithBugsnag(func() (err error) {
			if err := r.appHandler.StartRudderCore(ctx, options); err != nil {
				return fmt.Errorf("rudder core: %w", err)
			}
			return nil
		}))
		g.Go(misc.WithBugsnag(func() error {
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
		g.Go(misc.WithBugsnagForWarehouse(func() error {
			if err := warehouse.Start(ctx, r.application); err != nil {
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
			"Graceful terminal after %s, with %d go-routines",
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
	db.Init()
	diagnostics.Init()
	backendconfig.Init()
	warehouseutils.Init()
	warehouse.Init4()
	validations.Init()
	webhook.Init()
	asyncdestinationmanager.Init()
	batchrouterutils.Init()
	eventschema.Init()
	eventschema.Init2()
	stash.Init()
	kafka.Init()
	customdestinationmanager.Init()
	routertransformer.Init()
	alert.Init()
	oauth.Init()
}

func (r *Runner) versionInfo() map[string]interface{} {
	return map[string]interface{}{
		"Version":            r.releaseInfo.Version,
		"Major":              r.releaseInfo.Major,
		"Minor":              r.releaseInfo.Minor,
		"Patch":              r.releaseInfo.Patch,
		"Commit":             r.releaseInfo.Commit,
		"BuildDate":          r.releaseInfo.BuildDate,
		"BuiltBy":            r.releaseInfo.BuiltBy,
		"GitUrl":             r.releaseInfo.GitURL,
		"TransformerVersion": transformer.GetVersion(),
		"Features":           info.ServerComponent.Features,
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
