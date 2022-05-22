package batchrouter

import (
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/multitenant"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type Factory struct {
	Reporting        types.ReportingI
	Multitenant      multitenant.MultiTenantI
	BackendConfig    backendconfig.BackendConfig
	RouterDB         jobsdb.JobsDB
	ProcErrorDB      jobsdb.JobsDB
	TransientSources transientsource.Service
}

func (f *Factory) New(destType string) *HandleT {
	r := &HandleT{}

	r.Setup(f.BackendConfig, f.RouterDB, f.ProcErrorDB, destType, f.Reporting, f.Multitenant, f.TransientSources)
	return r
}
