package router

import (
	"context"
	"database/sql"
	"time"

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
	Multitenant      tenantStats
	BackendConfig    backendconfig.BackendConfig
	RouterDB         jobsdb.MultiTenantJobsDB
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
		MultitenantI:     f.Multitenant,
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

type tenantStats interface {
	CalculateSuccessFailureCounts(workspace, destType string, isSuccess, isDrained bool)
	GetRouterPickupJobs(
		destType string, noOfWorkers int, routerTimeOut time.Duration, jobQueryBatchSize int,
	) map[string]int
	ReportProcLoopAddStats(stats map[string]map[string]int, tableType string)
	UpdateWorkspaceLatencyMap(destType, workspaceID string, val float64)
}
