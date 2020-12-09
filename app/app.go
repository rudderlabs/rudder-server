package app

//go:generate mockgen -destination=../mocks/app/mock_app.go -package=mock_app github.com/rudderlabs/rudder-server/app Interface

import (
	"fmt"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/db"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

const (
	GATEWAY   = "GATEWAY"
	PROCESSOR = "PROCESSOR"
	EMBEDDED  = "EMBEDDED"
)

// App holds the main application's configuration and state
type App struct {
	options  *Options
	features *Features // Enterprise features, if available

	cpuprofileOutput *os.File
}

// Interface of a rudder-server application
type Interface interface {
	Setup()              // Initializes application
	Stop()               // Stop application
	Options() *Options   // Get this application's options
	Features() *Features // Get this application's enterprise features
}

var pkgLogger logger.LoggerI

func init() {
	pkgLogger = logger.NewLogger().Child("app")
}

// Setup initializes application
func (a *App) Setup() {
	// If cpuprofile flag is present, setup cpu profiling
	if a.options.Cpuprofile != "" {
		a.initCPUProfiling()
	}

	// initialize enterprise features, if available
	a.initEnterpriseFeatures()
}

func (a *App) initCPUProfiling() {
	var err error
	a.cpuprofileOutput, err = os.Create(a.options.Cpuprofile)
	if err != nil {
		panic(err)
	}
	runtime.SetBlockProfileRate(1)
	err = pprof.StartCPUProfile(a.cpuprofileOutput)
	if err != nil {
		panic(err)
	}

}

func (a *App) initEnterpriseFeatures() {
	a.features = &Features{}

	if migratorFeatureSetup != nil {
		a.features.Migrator = migratorFeatureSetup(a)
	}

	if suppressUserFeatureSetup != nil {
		a.features.SuppressUser = suppressUserFeatureSetup(a)
	}

	if configEnvFeatureSetup != nil {
		a.features.ConfigEnv = configEnvFeatureSetup(a)
	}
}

// Options returns this application's options
func (a *App) Options() *Options {
	return a.options
}

// Features returns this application's enterprise features
func (a *App) Features() *Features {
	return a.features
}

// Stop stops application
func (a *App) Stop() {
	if a.options.Cpuprofile != "" {
		pkgLogger.Info("Stopping CPU profile")
		pprof.StopCPUProfile()
		a.cpuprofileOutput.Close()
	}

	if a.options.Memprofile != "" {
		f, err := os.Create(a.options.Memprofile)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		err = pprof.WriteHeapProfile(f)
		if err != nil {
			panic(err)
		}
	}
}

// New creates a new application instance
func New(options *Options) Interface {
	return &App{
		options: options,
	}
}

//HealthHandler is the http handler for health endpoint
func HealthHandler(w http.ResponseWriter, r *http.Request, jobsDB jobsdb.JobsDB) {
	var dbService string = "UP"
	var enabledRouter string = "TRUE"
	var backendConfigMode string = "API"
	if !jobsDB.CheckPGHealth() {
		dbService = "DOWN"
	}
	if !config.GetBool("enableRouter", true) {
		enabledRouter = "FALSE"
	}
	if config.GetBool("BackendConfig.configFromFile", false) {
		backendConfigMode = "JSON"
	}

	appTypeStr := strings.ToUpper(config.GetEnv("APP_TYPE", EMBEDDED))
	healthVal := fmt.Sprintf(`{"appType": "%s", "server":"UP", "db":"%s","acceptingEvents":"TRUE","routingEvents":"%s","mode":"%s","goroutines":"%d", "backendConfigMode": "%s", "lastSync":"%s", "lastRegulationSync":"%s"}`, appTypeStr, dbService, enabledRouter, strings.ToUpper(db.CurrentMode), runtime.NumGoroutine(), backendConfigMode, backendconfig.LastSync, backendconfig.LastRegulationSync)
	w.Write([]byte(healthVal))
}
