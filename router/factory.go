package router

import (
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

type Factory struct {
	Reporting     reporter
	Multitenant   tenantStats
	BackendConfig backendconfig.BackendConfig
	RouterDB      jobsdb.MultiTenantJobsDB
	ProcErrorDB   jobsdb.JobsDB
}

func (f *Factory) New(destinationDefinition backendconfig.DestinationDefinitionT) *HandleT {
	r := &HandleT{
		Reporting:    f.Reporting,
		MultitenantI: f.Multitenant,
	}
	r.Setup(f.BackendConfig, f.RouterDB, f.ProcErrorDB, destinationDefinition)
	return r
}
