package batchrouter

import (
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/multitenant"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type Factory struct {
	Reporting     types.ReportingI
	BackendConfig backendconfig.BackendConfig
	RouterDB      *jobsdb.HandleT
	ProcErrorDB   jobsdb.JobsDB
	Multitenant   bool
}

func (f *Factory) New(destType string) *HandleT {
	var multitenantStats multitenant.MultiTenantI = multitenant.NOOP

	if f.Multitenant {
		multitenantStats = multitenant.NewStats(&jobsdb.MultiTenantHandleT{HandleT: f.RouterDB})
	}

	r := &HandleT{}

	r.Setup(f.BackendConfig, f.RouterDB, f.ProcErrorDB, destType, f.Reporting, multitenantStats)
	return r
}
