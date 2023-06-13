package batchrouter

import (
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/services/multitenant"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type Factory struct {
	Reporting        types.Reporting
	Multitenant      multitenant.MultiTenantI
	BackendConfig    backendconfig.BackendConfig
	RouterDB         jobsdb.JobsDB
	ProcErrorDB      jobsdb.JobsDB
	TransientSources transientsource.Service
	RsourcesService  rsources.JobService
	Debugger         destinationdebugger.DestinationDebugger
	AdaptiveLimit    func(int64) int64
}

func (f *Factory) New(destination *backendconfig.DestinationT) *Handle {
	r := &Handle{
		adaptiveLimit: f.AdaptiveLimit,
	}

	r.Setup(
		destination,
		f.BackendConfig,
		f.RouterDB,
		f.ProcErrorDB,
		f.Reporting,
		f.Multitenant,
		f.TransientSources,
		f.RsourcesService,
		f.Debugger,
	)
	return r
}
