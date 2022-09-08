package router

import (
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transientsource"
)

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

type Factory struct {
	Reporting        reporter
	Multitenant      tenantStats
	BackendConfig    backendconfig.BackendConfig
	RouterDB         jobsdb.MultiTenantJobsDB
	ProcErrorDB      jobsdb.JobsDB
	TransientSources transientsource.Service
	RsourcesService  rsources.JobService
}

func (f *Factory) New(destination *backendconfig.DestinationT, identifier string) *HandleT {
	r := &HandleT{
		Reporting:    f.Reporting,
		MultitenantI: f.Multitenant,
	}
	destConfig := getRouterConfig(destination, identifier)
	r.Setup(f.BackendConfig, f.RouterDB, f.ProcErrorDB, destConfig, f.TransientSources, f.RsourcesService)
	return r
}
