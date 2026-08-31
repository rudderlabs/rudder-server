package batchrouter

import (
	"github.com/rudderlabs/rudder-go-kit/config"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type Factory struct {
	Reporting        types.Reporting
	BackendConfig    backendconfig.BackendConfig
	RouterDB         jobsdb.JobsDB
	TransientSources transientsource.Service
	RsourcesService  rsources.JobService
	SyncSettings     rsources.SyncSettingDelegate
	Debugger         destinationdebugger.DestinationDebugger
	AdaptiveLimit    func(int64) int64
}

func (f *Factory) New(destType string) *Handle {
	if f.SyncSettings == nil {
		// Dropping this field would not break the build, and the batch router would
		// then decide error capture per record at runtime instead - so fail here.
		panic("batchrouter: Factory.SyncSettings is required")
	}
	r := &Handle{
		adaptiveLimit: f.AdaptiveLimit,
	}

	r.Setup(
		destType,
		f.BackendConfig,
		f.RouterDB,
		f.Reporting,
		f.TransientSources,
		f.RsourcesService,
		f.SyncSettings,
		f.Debugger,
		config.Default,
	)
	return r
}
