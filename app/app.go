package app

//go:generate mockgen -destination=../mocks/app/mock_app.go -package=mock_app github.com/rudderlabs/rudder-server/app App

import (
	"fmt"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"

	"github.com/rudderlabs/rudder-server/enterprise/trackedusers"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	configenv "github.com/rudderlabs/rudder-server/enterprise/config-env"
	"github.com/rudderlabs/rudder-server/enterprise/reporting"
	suppression "github.com/rudderlabs/rudder-server/enterprise/suppress-user"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

const (
	GATEWAY   = "GATEWAY"
	PROCESSOR = "PROCESSOR"
	EMBEDDED  = "EMBEDDED"
)

// App represents a rudder-server application
type App interface {
	Setup()              // Initializes application
	Stop()               // Stop application
	Options() *Options   // Get this application's options
	Features() *Features // Get this application's enterprise features
}

// app holds the main application's configuration and state
type app struct {
	log      logger.Logger
	options  *Options
	features *Features // Enterprise features, if available

	cpuprofileOutput *os.File
}

// Setup initializes application
func (a *app) Setup() {
	// If cpuprofile flag is present, setup cpu profiling
	if a.options.Cpuprofile != "" {
		a.initCPUProfiling()
	}

	if a.options.EnterpriseToken == "" {
		a.log.Info("Open source version of rudder-server")
	} else {
		a.log.Info("Enterprise version of rudder-server")
	}

	a.initFeatures()
}

func (a *app) initCPUProfiling() {
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

func (a *app) initFeatures() {
	enterpriseLogger := logger.NewLogger().Child("enterprise")
	a.features = &Features{
		SuppressUser: &suppression.Factory{
			EnterpriseToken: a.options.EnterpriseToken,
			Log:             enterpriseLogger.Child("suppress-user"),
		},
		Reporting: &reporting.Factory{
			EnterpriseToken: a.options.EnterpriseToken,
			Log:             enterpriseLogger.Child("reporting"),
		},
		ConfigEnv: &configenv.Factory{
			EnterpriseToken: a.options.EnterpriseToken,
			Log:             enterpriseLogger.Child("config-env"),
		},
		TrackedUsers: &trackedusers.Factory{
			Log: enterpriseLogger.Child("tracked-users"),
		},
	}
}

// Options returns this application's options
func (a *app) Options() *Options {
	return a.options
}

// Features returns this application's enterprise features
func (a *app) Features() *Features {
	return a.features
}

// Stop stops application
func (a *app) Stop() {
	if a.options.Cpuprofile != "" {
		a.log.Info("Stopping CPU profile")
		pprof.StopCPUProfile()
		_ = a.cpuprofileOutput.Close()
	}

	if a.options.Memprofile != "" {
		f, err := os.Create(a.options.Memprofile)
		if err != nil {
			panic(err)
		}
		defer func() { _ = f.Close() }()
		runtime.GC() // get up-to-date statistics
		err = pprof.WriteHeapProfile(f)
		if err != nil {
			panic(err)
		}
	}
}

// New creates a new application instance
func New(options *Options) App {
	return &app{
		log:     logger.NewLogger().Child("app"),
		options: options,
	}
}

// LivenessHandler is the http handler for the Kubernetes liveness probe
func LivenessHandler(jobsDB jobsdb.JobsDB) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		healthy, responsePayload := getHealthVal(jobsDB)
		if !healthy {
			http.Error(w, "Cannot connect to db", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(responsePayload))
	}
}

func getHealthVal(jobsDB jobsdb.JobsDB) (bool, string) {
	dbService := "UP"
	healthy := true
	if jobsDB.Ping() != nil {
		dbService = "DOWN"
		healthy = false
	}
	enabledRouter := "TRUE"
	if !config.GetBool("enableRouter", true) {
		enabledRouter = "FALSE"
	}
	backendConfigMode := "API"
	if config.GetBool("BackendConfig.configFromFile", false) {
		backendConfigMode = "JSON"
	}

	appTypeStr := strings.ToUpper(config.GetString("APP_TYPE", EMBEDDED))
	return healthy, fmt.Sprintf(
		`{"appType":"%s","server":"UP","db":"%s","acceptingEvents":"TRUE","routingEvents":"%s","mode":"NORMAL",`+
			`"backendConfigMode":"%s","lastSync":"%s","lastRegulationSync":"%s"}`,
		appTypeStr, dbService, enabledRouter,
		backendConfigMode, backendconfig.LastSync, backendconfig.LastRegulationSync,
	)
}
