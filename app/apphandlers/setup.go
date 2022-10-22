package apphandlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/validators"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

var (
	enableProcessor, enableRouter, enableReplay                bool
	pkgLogger                                                  logger.Logger
	Diagnostics                                                diagnostics.DiagnosticsI
	readonlyGatewayDB, readonlyRouterDB, readonlyBatchRouterDB jobsdb.ReadonlyHandleT
	readonlyProcErrorDB                                        jobsdb.ReadonlyHandleT
)

// AppHandler to be implemented by different app type objects.
type AppHandler interface {
	GetAppType() string
	HandleRecovery(*app.Options)
	StartRudderCore(context.Context, *app.Options) error
}

func GetAppHandler(application app.App, appType string, versionHandler func(w http.ResponseWriter, r *http.Request)) AppHandler {
	var handler AppHandler
	switch appType {
	case app.GATEWAY:
		handler = &GatewayApp{App: application, VersionHandler: versionHandler}
	case app.PROCESSOR:
		handler = &ProcessorApp{App: application, VersionHandler: versionHandler}
	case app.EMBEDDED:
		handler = &EmbeddedApp{App: application, VersionHandler: versionHandler}
	default:
		panic(errors.New("invalid app type"))
	}

	return handler
}

func Init2() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("apphandlers")
	Diagnostics = diagnostics.Diagnostics
}

func loadConfig() {
	config.RegisterBoolConfigVariable(true, &enableProcessor, false, "enableProcessor")
	config.RegisterBoolConfigVariable(types.DEFAULT_REPLAY_ENABLED, &enableReplay, false, "Replay.enabled")
	config.RegisterBoolConfigVariable(true, &enableRouter, false, "enableRouter")
}

func rudderCoreDBValidator() {
	validators.ValidateEnv()
}

func rudderCoreNodeSetup() {
	validators.InitializeNodeMigrations()
}

func rudderCoreWorkSpaceTableSetup() {
	validators.CheckAndValidateWorkspaceToken()
}

func rudderCoreBaseSetup() {
	// Check if there is a probable inconsistent state of Data
	if diagnostics.EnableServerStartMetric {
		Diagnostics.Track(diagnostics.ServerStart, map[string]interface{}{
			diagnostics.ServerStart: fmt.Sprint(time.Unix(misc.AppStartTime, 0)),
		})
	}

	// Reload Config
	loadConfig()

	readonlyGatewayDB.Setup("gw")
	readonlyRouterDB.Setup("rt")
	readonlyBatchRouterDB.Setup("batch_rt")
	readonlyProcErrorDB.Setup("proc_error")

	processor.RegisterAdminHandlers(&readonlyProcErrorDB)
	router.RegisterAdminHandlers(&readonlyRouterDB, &readonlyBatchRouterDB)
}

// NewRsourcesService produces a rsources.JobService through environment configuration (env variables & config file)
func NewRsourcesService(deploymentType deployment.Type) (rsources.JobService, error) {
	var rsourcesConfig rsources.JobServiceConfig
	rsourcesConfig.MaxPoolSize = config.GetInt("Rsources.PoolSize", 5)
	rsourcesConfig.LocalConn = misc.GetConnectionString()
	rsourcesConfig.LocalHostname = config.GetString("DB.host", "localhost")
	rsourcesConfig.SharedConn = config.GetString("SharedDB.dsn", "")
	rsourcesConfig.SkipFailedRecordsCollection = !config.GetBool("Router.failedKeysEnabled", false)

	if deploymentType == deployment.MultiTenantType {
		// For multitenant deployment type we shall require the existence of a SHARED_DB
		// TODO: change default value of Rsources.FailOnMissingSharedDB to true, when shared DB is provisioned
		if rsourcesConfig.SharedConn == "" && config.GetBool("Rsources.FailOnMissingSharedDB", false) {
			return nil, fmt.Errorf("deployment type %s requires SharedDB.dsn to be provided", deploymentType)
		}
	}

	return rsources.NewJobService(rsourcesConfig)
}
