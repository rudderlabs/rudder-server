package router

import (
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/throttler"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

type Factory struct {
	Reporting        reporter
	Multitenant      tenantStats
	BackendConfig    backendconfig.BackendConfig
	RouterDB         jobsdb.MultiTenantJobsDB
	ProcErrorDB      jobsdb.JobsDB
	TransientSources transientsource.Service
	RsourcesService  rsources.JobService
	Logger           logger.Logger
	Stats            stats.Stats
}

func (f *Factory) New(destination *backendconfig.DestinationT, identifier string) *HandleT {
	throttlerFactory, err := throttler.New()
	if err != nil {
		f.Logger.Errorf("[Router Factory] Failed to initialize throttler factory: %v", err)
		f.Stats.NewTaggedStat("init_throttler_factory_error", stats.CountType, nil).Increment()
	}

	r := &HandleT{
		Reporting:        f.Reporting,
		MultitenantI:     f.Multitenant,
		throttlerFactory: throttlerFactory,
	}
	destConfig := getRouterConfig(destination, identifier)
	r.Setup(f.BackendConfig, f.RouterDB, f.ProcErrorDB, destConfig, f.TransientSources, f.RsourcesService)
	return r
}

type destinationConfig struct {
	name          string
	responseRules map[string]interface{}
	config        map[string]interface{}
	destinationID string
}

func getRouterConfig(destination *backendconfig.DestinationT, identifier string) destinationConfig {
	return destinationConfig{
		name:          destination.DestinationDefinition.Name,
		destinationID: identifier,
		config:        destination.DestinationDefinition.Config,
		responseRules: destination.DestinationDefinition.ResponseRules,
	}
}
