package processor

import (
	"context"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/services/multitenant"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type Processor struct {
	*HandleT
	mainCtx          context.Context
	currentCancel    context.CancelFunc
	DBs              *jobsdb.DBs
	multitenantStats multitenant.MultiTenantI // need not initialize again
	reportingI       types.ReportingI         // need not initialize again
	backendConfig    backendconfig.BackendConfig
}

func (proc *Processor) Run(ctx context.Context) error {
	return nil
}

func (proc *Processor) StartNew() {
	proc.DBs.Start()
	proc.HandleT.Setup(proc.backendConfig, &proc.DBs.GatewayDB, &proc.DBs.RouterDB, &proc.DBs.BatchRouterDB,
		&proc.DBs.ProcErrDB, &proc.DBs.ClearDB, proc.reporting, proc.multitenantStats)

	currentCtx, cancel := context.WithCancel(context.Background())
	proc.currentCancel = cancel
	proc.Start(currentCtx)
}

func (proc *Processor) Stop() {
	proc.currentCancel()
	proc.DBs.Halt()
	proc.Shutdown()
}

// New creates a new Processor instance
func New(ctx context.Context, dbs *jobsdb.DBs) *Processor {
	proc := &Processor{
		HandleT:          &HandleT{transformer: transformer.NewTransformer()},
		mainCtx:          ctx,
		DBs:              dbs,
		multitenantStats: multitenant.NOOP,
		backendConfig:    backendconfig.DefaultBackendConfig,
	}
	return proc
}
