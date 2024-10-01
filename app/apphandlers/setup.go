package apphandlers

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/app/cluster"
	"github.com/rudderlabs/rudder-server/app/cluster/state"
	"github.com/rudderlabs/rudder-server/internal/enricher"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/validators"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
	"github.com/rudderlabs/rudder-server/utils/types/servermode"
)

// AppHandler starts the app
type AppHandler interface {
	// Setup to be called only once before starting the app.
	Setup() error
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

// NewRsourcesService produces a rsources.JobService through environment configuration (env variables & config file)
func NewRsourcesService(deploymentType deployment.Type, shouldSetupSharedDB bool, stats stats.Stats) (rsources.JobService, error) {
	var rsourcesConfig rsources.JobServiceConfig
	rsourcesConfig.MaxPoolSize = config.GetInt("Rsources.MaxPoolSize", 3)
	rsourcesConfig.MinPoolSize = config.GetInt("Rsources.MinPoolSize", 1)
	rsourcesConfig.LocalConn = misc.GetConnectionString(config.Default, "rsources")
	rsourcesConfig.LocalHostname = config.GetString("DB.host", "localhost")
	sharedDBConnUrl := config.GetString("SharedDB.dsn", "")
	if len(sharedDBConnUrl) != 0 {
		var err error
		sharedDBConnUrl, err = misc.SetAppNameInDBConnURL(sharedDBConnUrl, "rsources")
		if err != nil {
			return nil, fmt.Errorf("failed to set application name in dns: %w", err)
		}
	}
	rsourcesConfig.SharedConn = sharedDBConnUrl
	rsourcesConfig.SkipFailedRecordsCollection = !config.GetBool("Router.failedKeysEnabled", true)

	if deploymentType == deployment.MultiTenantType {
		// For multitenant deployment type we shall require the existence of a SHARED_DB
		// TODO: change default value of Rsources.FailOnMissingSharedDB to true, when shared DB is provisioned
		if rsourcesConfig.SharedConn == "" && config.GetBool("Rsources.FailOnMissingSharedDB", false) {
			return nil, fmt.Errorf("deployment type %s requires SharedDB.dsn to be provided", deploymentType)
		}
	}

	rsourcesConfig.ShouldSetupSharedDB = shouldSetupSharedDB

	return rsources.NewJobService(rsourcesConfig, stats)
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

// terminalErrorFunction returns a function that cancels the errgroup g with an error when the returned function is called.
func terminalErrorFunction(ctx context.Context, g *errgroup.Group) func(error) {
	cancelChannel := make(chan error)
	var cancelOnce sync.Once
	cancel := func(err error) {
		cancelOnce.Do(func() {
			cancelChannel <- err
			close(cancelChannel)
		})
	}
	g.Go(func() error {
		select {
		case <-ctx.Done():
			return nil
		case err := <-cancelChannel:
			return err
		}
	})
	return cancel
}

func setupPipelineEnrichers(conf *config.Config, log logger.Logger, stats stats.Stats) ([]enricher.PipelineEnricher, error) {
	var enrichers []enricher.PipelineEnricher

	if conf.GetBool("GeoEnrichment.enabled", false) {
		log.Infof("Setting up the geolocation pipeline enricher")

		geoEnricher, err := enricher.NewGeoEnricher(conf, log, stats)
		if err != nil {
			return nil, fmt.Errorf("starting geo enrichment process for pipeline: %w", err)
		}
		enrichers = append(enrichers, geoEnricher)
	}

	return enrichers, nil
}
