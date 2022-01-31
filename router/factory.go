package router

import (
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/multitenant"
)

type Factory struct {
	Reporting     reporter
	BackendConfig backendconfig.BackendConfig
	ProcErrorDB   jobsdb.JobsDB
	RouterDB      *jobsdb.HandleT
	Multitenant   bool
}

func (f *Factory) New(destinationDefinition backendconfig.DestinationDefinitionT) *HandleT {
	var tenantRouterDB jobsdb.MultiTenantJobsDB = &jobsdb.MultiTenantLegacy{HandleT: f.RouterDB}
	var multitenantStats multitenant.MultiTenantI = multitenant.NOOP

	if f.Multitenant {
		tenantRouterDB = &jobsdb.MultiTenantHandleT{HandleT: f.RouterDB}
		multitenantStats = multitenant.NewStats(tenantRouterDB)
	}

	r := &HandleT{
		Reporting:    f.Reporting,
		MultitenantI: multitenantStats,
	}

	r.Setup(f.BackendConfig, tenantRouterDB, f.ProcErrorDB, destinationDefinition)
	return r
}
