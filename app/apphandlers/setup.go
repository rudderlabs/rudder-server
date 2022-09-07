package apphandlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	"github.com/rudderlabs/rudder-server/router/manager"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/multitenant"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/services/validators"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	utilsync "github.com/rudderlabs/rudder-server/utils/sync"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

var (
	enableProcessor, enableRouter, enableReplay                bool
	objectStorageDestinations                                  []string
	asyncDestinations                                          []string
	routerLoaded                                               utilsync.First
	processorLoaded                                            utilsync.First
	pkgLogger                                                  logger.LoggerI
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

func GetAppHandler(application app.Interface, appType string, versionHandler func(w http.ResponseWriter, r *http.Request)) AppHandler {
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
	objectStorageDestinations = []string{"S3", "GCS", "AZURE_BLOB", "MINIO", "DIGITAL_OCEAN_SPACES"}
	asyncDestinations = []string{"MARKETO_BULK_UPLOAD"}
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

// StartProcessor atomically starts processor process if not already started
func StartProcessor(
	ctx context.Context, clearDB *bool, gatewayDB, routerDB, batchRouterDB,
	procErrorDB *jobsdb.HandleT, reporting types.ReportingI, multitenantStat multitenant.MultiTenantI,
	transientSources transientsource.Service, rsourcesService rsources.JobService,
) error {
	if !processorLoaded.First() {
		pkgLogger.Debug("processor started by another go routine")
		return nil
	}

	processorInstance := processor.NewProcessor()
	processorInstance.Setup(
		backendconfig.DefaultBackendConfig, gatewayDB, routerDB, batchRouterDB, procErrorDB,
		clearDB, reporting, multitenantStat, transientSources, rsourcesService,
	)
	defer processorInstance.Shutdown()
	return processorInstance.Start(ctx)
}

// StartRouter atomically starts router process if not already started
func StartRouter(
	ctx context.Context, routerDB jobsdb.MultiTenantJobsDB, batchRouterDB *jobsdb.HandleT,
	procErrorDB *jobsdb.HandleT, reporting types.ReportingI, multitenantStat multitenant.MultiTenantI,
	transientSources transientsource.Service, rsourcesService rsources.JobService,
) {
	if !routerLoaded.First() {
		pkgLogger.Debug("processor started by an other go routine")
		return
	}

	routerFactory := router.Factory{
		BackendConfig:    backendconfig.DefaultBackendConfig,
		Reporting:        reporting,
		Multitenant:      multitenantStat,
		RouterDB:         routerDB,
		ProcErrorDB:      procErrorDB,
		TransientSources: transientSources,
		RsourcesService:  rsourcesService,
	}

	batchRouterFactory := batchrouter.Factory{
		BackendConfig:    backendconfig.DefaultBackendConfig,
		Reporting:        reporting,
		Multitenant:      multitenantStat,
		ProcErrorDB:      procErrorDB,
		RouterDB:         batchRouterDB,
		TransientSources: transientSources,
		RsourcesService:  rsourcesService,
	}

	m := manager.New(&routerFactory, &batchRouterFactory, backendconfig.DefaultBackendConfig)

	err := m.Start()
	if err != nil {
		panic(err)
	}
	defer m.Stop()

	<-ctx.Done()
}

// NewRsourcesService produces a rsources.JobService through environment configuration (env variables & config file)
func NewRsourcesService(deploymentType deployment.Type) (rsources.JobService, error) {
	var rsourcesConfig rsources.JobServiceConfig
	rsourcesConfig.MaxPoolSize = config.GetInt("Rsources.PoolSize", 5)
	rsourcesConfig.LocalConn = jobsdb.GetConnectionString()
	rsourcesConfig.LocalHostname = config.GetEnv("JOBS_DB_HOST", "localhost")
	rsourcesConfig.SharedConn = config.GetEnv("SHARED_DB_DSN", "")
	rsourcesConfig.SkipFailedRecordsCollection = !config.GetBool("Router.failedKeysEnabled", false)

	if deploymentType == deployment.MultiTenantType {
		// For multitenant deployment type we shall require the existence of a SHARED_DB
		// TODO: change default value of Rsources.FailOnMissingSharedDB to true, when shared DB is provisioned
		if rsourcesConfig.SharedConn == "" && config.GetBool("Rsources.FailOnMissingSharedDB", false) {
			return nil, fmt.Errorf("deployment type %s requires SHARED_DB_DSN to be provided", deploymentType)
		}
	}

	return rsources.NewJobService(rsourcesConfig)
}
