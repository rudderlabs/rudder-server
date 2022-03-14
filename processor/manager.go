package processor

import (
	"context"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/services/multitenant"
	"github.com/rudderlabs/rudder-server/utils/types"
	"golang.org/x/sync/errgroup"
)

type LifecycleManager struct {
	*HandleT
	mainCtx          context.Context
	currentCancel    context.CancelFunc
	waitGroup        *errgroup.Group
	DBs              *jobsdb.DBs
	multitenantStats multitenant.MultiTenantI // need not initialize again
	reportingI       types.ReportingI         // need not initialize again
	backendConfig    backendconfig.BackendConfig
}

func (proc *LifecycleManager) Run(ctx context.Context) error {
	return nil
}

// StartNew starts a processor, this is not a blocking call.
//If the processor is not completely started and the data started coming then also it will not be problematic as we
//are assuming that the DBs will be up.
func (proc *LifecycleManager) StartNew() {
	proc.HandleT.Setup(proc.backendConfig, &proc.DBs.GatewayDB, &proc.DBs.RouterDB, &proc.DBs.BatchRouterDB,
		&proc.DBs.ProcErrDB, &proc.DBs.ClearDB, proc.reporting, proc.multitenantStats)

	currentCtx, cancel := context.WithCancel(context.Background())
	proc.currentCancel = cancel

	g, ctx := errgroup.WithContext(currentCtx)
	proc.waitGroup = g
	g.Go(func() error {
		proc.Start(ctx)
		return nil
	})
}

// Stop stops the processor, this is a blocking call.
func (proc *LifecycleManager) Stop() {
	proc.currentCancel()
	proc.Shutdown()
	proc.waitGroup.Wait()
}

// New creates a new Processor instance
func New(ctx context.Context, dbs *jobsdb.DBs) *LifecycleManager {
	proc := &LifecycleManager{
		HandleT:          &HandleT{transformer: transformer.NewTransformer()},
		mainCtx:          ctx,
		DBs:              dbs,
		multitenantStats: multitenant.NOOP,
		backendConfig:    backendconfig.DefaultBackendConfig,
	}
	return proc
}
