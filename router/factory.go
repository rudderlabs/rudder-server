package router

import (
	"context"
	"database/sql"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/throttler"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	utilTypes "github.com/rudderlabs/rudder-server/utils/types"
)

type Factory struct {
	Logger           logger.Logger
	Reporting        reporter
	BackendConfig    backendconfig.BackendConfig
	RouterDB         jobsdb.JobsDB
	ProcErrorDB      jobsdb.JobsDB
	TransientSources transientsource.Service
	RsourcesService  rsources.JobService
	ThrottlerFactory *throttler.Factory
	Debugger         destinationdebugger.DestinationDebugger
	AdaptiveLimit    func(int64) int64
}

func (f *Factory) New(destination *backendconfig.DestinationT) *Handle {
	r := &Handle{
		Reporting:        f.Reporting,
		throttlerFactory: f.ThrottlerFactory,
		adaptiveLimit:    f.AdaptiveLimit,
	}
	r.Setup(
		destination.DestinationDefinition,
		f.Logger,
		config.Default,
		f.BackendConfig,
		f.RouterDB,
		f.ProcErrorDB,
		f.TransientSources,
		f.RsourcesService,
		f.Debugger,
	)
	return r
}

type reporter interface {
	WaitForSetup(ctx context.Context, clientName string) error
	Report(metrics []*utilTypes.PUReportedMetric, txn *sql.Tx)
}
