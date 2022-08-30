package router

import (
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type destinationConfig struct {
	Name          string
	ResponseRules map[string]interface{}
	Config        map[string]interface{}
	DestinationID string
}

func getRouterConfig(destination *backendconfig.DestinationT) destinationConfig {
	var destConfig destinationConfig

	destConfig.Name = destination.DestinationDefinition.Name
	destConfig.DestinationID = misc.GetRouterIdentifier(destination.ID, destination.DestinationDefinition.Name)
	destConfig.Config = destination.DestinationDefinition.Config
	destConfig.ResponseRules = destination.DestinationDefinition.ResponseRules
	return destConfig
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

func (f *Factory) New(destination *backendconfig.DestinationT) (*HandleT, string) {
	r := &HandleT{
		Reporting:    f.Reporting,
		MultitenantI: f.Multitenant,
	}
	destConfig := getRouterConfig(destination)
	r.Setup(f.BackendConfig, f.RouterDB, f.ProcErrorDB, destConfig, f.TransientSources, f.RsourcesService)
	return r, destConfig.DestinationID
}
