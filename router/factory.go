package router

import (
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/objectdb"
	"github.com/rudderlabs/rudder-server/services/transientsource"
)

type Factory struct {
	Reporting        reporter
	Multitenant      tenantStats
	BackendConfig    backendconfig.BackendConfig
	RouterDB         jobsdb.MultiTenantJobsDB
	ProcErrorDB      jobsdb.JobsDB
	TransientSources transientsource.Service
	ObjectBox        *objectdb.Box
}

func (f *Factory) New(destinationDefinition backendconfig.DestinationDefinitionT) *HandleT {
	r := &HandleT{
		Reporting:    f.Reporting,
		MultitenantI: f.Multitenant,
	}
	// customVal, err := f.ObjectBox.GetOrCreateCustomVal(destinationDefinition.Name)
	// if err != nil {
	// 	panic(err)
	// }
	// r.customVal = customVal
	r.box = f.ObjectBox
	r.Setup(f.BackendConfig, f.RouterDB, f.ProcErrorDB, destinationDefinition, f.TransientSources)
	return r
}
