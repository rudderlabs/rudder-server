package apphandlers

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/app/cluster"
	"github.com/rudderlabs/rudder-server/app/cluster/state"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/validators"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
	"github.com/rudderlabs/rudder-server/utils/types/servermode"
)

// AppHandler starts the app
type AppHandler interface {
	// Setup to be called only once before starting the app.
	Setup(*app.Options) error
	// StartRudderCore starts the app
	StartRudderCore(context.Context, *app.Options) error
}

func GetAppHandler(application app.App, appType string, versionHandler func(w http.ResponseWriter, r *http.Request)) (AppHandler, error) {
	log := logger.NewLogger().Child("apphandlers").Child(appType)
	switch appType {
	case app.GATEWAY:
		return &gatewayApp{app: application, versionHandler: versionHandler, log: log}, nil
	case app.PROCESSOR:
		return &processorApp{app: application, versionHandler: versionHandler, log: log}, nil
	case app.EMBEDDED:
		return &embeddedApp{app: application, versionHandler: versionHandler, log: log}, nil
	default:
		return nil, fmt.Errorf("unsupported app type %s", appType)
	}
}

func rudderCoreDBValidator() error {
	return validators.ValidateEnv()
}

func rudderCoreNodeSetup() error {
	return validators.InitializeNodeMigrations()
}

func rudderCoreWorkSpaceTableSetup() error {
	return validators.CheckAndValidateWorkspaceToken()
}

func setupReadonlyDBs() (gw *jobsdb.ReadonlyHandleT, err error) {
	if diagnostics.EnableServerStartMetric {
		diagnostics.Diagnostics.Track(diagnostics.ServerStart, map[string]interface{}{
			diagnostics.ServerStart: fmt.Sprint(time.Unix(misc.AppStartTime, 0)),
		})
	}
	var gwDB, rtDB, batchrtDB, procerrDB jobsdb.ReadonlyHandleT

	if err := gwDB.Setup("gw"); err != nil {
		return nil, fmt.Errorf("setting up gw readonly db: %w", err)
	}
	gw = &gwDB

	if err := rtDB.Setup("rt"); err != nil {
		return nil, fmt.Errorf("setting up gw readonly db: %w", err)
	}
	if err := batchrtDB.Setup("batch_rt"); err != nil {
		return nil, fmt.Errorf("setting up batch_rt readonly db: %w", err)
	}
	router.RegisterAdminHandlers(&rtDB, &batchrtDB)

	if err := procerrDB.Setup("proc_error"); err != nil {
		return nil, fmt.Errorf("setting up proc_error readonly db: %w", err)
	}
	processor.RegisterAdminHandlers(&procerrDB)

	return
}

// NewRsourcesService produces a rsources.JobService through environment configuration (env variables & config file)
func NewRsourcesService(deploymentType deployment.Type) (rsources.JobService, error) {
	var rsourcesConfig rsources.JobServiceConfig
	rsourcesConfig.MaxPoolSize = config.GetInt("Rsources.PoolSize", 5)
	rsourcesConfig.LocalConn = misc.GetConnectionString()
	rsourcesConfig.LocalHostname = config.GetString("DB.host", "localhost")
	rsourcesConfig.SharedConn = config.GetString("SharedDB.dsn", "")
	rsourcesConfig.SkipFailedRecordsCollection = !config.GetBool("Router.failedKeysEnabled", true)

	if deploymentType == deployment.MultiTenantType {
		// For multitenant deployment type we shall require the existence of a SHARED_DB
		// TODO: change default value of Rsources.FailOnMissingSharedDB to true, when shared DB is provisioned
		if rsourcesConfig.SharedConn == "" && config.GetBool("Rsources.FailOnMissingSharedDB", false) {
			return nil, fmt.Errorf("deployment type %s requires SharedDB.dsn to be provided", deploymentType)
		}
	}

	return rsources.NewJobService(rsourcesConfig)
}

func resolveModeProvider(log logger.Logger, deploymentType deployment.Type) (cluster.ChangeEventProvider, error) {
	enableProcessor := config.GetBool("enableProcessor", true)
	enableRouter := config.GetBool("enableRouter", true)
	forceStaticMode := config.GetBool("forceStaticModeProvider", false)

	var modeProvider cluster.ChangeEventProvider

	staticModeProvider := func() cluster.ChangeEventProvider {
		// FIXME: hacky way to determine server mode
		if enableProcessor && enableRouter {
			return state.NewStaticProvider(servermode.NormalMode)
		}
		return state.NewStaticProvider(servermode.DegradedMode)
	}

	if forceStaticMode {
		log.Info("forcing the use of Static Cluster Manager")
		modeProvider = staticModeProvider()
	} else {
		switch deploymentType {
		case deployment.MultiTenantType:
			log.Info("using ETCD Based Dynamic Cluster Manager")
			modeProvider = state.NewETCDDynamicProvider()
		case deployment.DedicatedType:
			log.Info("using Static Cluster Manager")
			modeProvider = staticModeProvider()
		default:
			return modeProvider, fmt.Errorf("unsupported deployment type: %q", deploymentType)
		}
	}
	return modeProvider, nil
}
