package processor

import (
	"context"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	"github.com/rudderlabs/rudder-server/services/multitenant"
	"github.com/rudderlabs/rudder-server/utils/types"
	"time"
)

type Processor struct {
	*HandleT
	mainCtx           context.Context
	gatewayDB         *jobsdb.HandleT
	routerDB          *jobsdb.HandleT
	batchRouterDB     *jobsdb.HandleT
	procErrorDB       *jobsdb.HandleT
	gwDBRetention     *time.Duration
	routerDBRetention *time.Duration
	clearDB           *bool
	migrationMode     *string
	multitenantStats  multitenant.MultiTenantI // need not initialize again
	reportingI        types.ReportingI         // need not initialize again
	backendConfig     backendconfig.BackendConfig
	currentCancel     context.CancelFunc
}

func (proc *Processor) Run(ctx context.Context) error {
	return nil
}

func (proc *Processor) StartNew() {
	proc.gatewayDB = &jobsdb.HandleT{}
	proc.routerDB = &jobsdb.HandleT{}
	proc.batchRouterDB = &jobsdb.HandleT{}
	proc.procErrorDB = &jobsdb.HandleT{}
	proc.gatewayDB.Setup(jobsdb.Read, *proc.clearDB, "gw", *proc.gwDBRetention, *proc.migrationMode, true,
		jobsdb.QueryFiltersT{})
	proc.routerDB.Setup(jobsdb.Write, *proc.clearDB, "rt", *proc.routerDBRetention, *proc.migrationMode, true,
		router.QueryFilters)
	proc.batchRouterDB.Setup(jobsdb.Write, *proc.clearDB, "batch_rt", *proc.routerDBRetention, *proc.migrationMode, true,
		batchrouter.QueryFilters)
	proc.procErrorDB.Setup(jobsdb.Write, *proc.clearDB, "proc_error", *proc.routerDBRetention, *proc.migrationMode,
		false, jobsdb.QueryFiltersT{})
	proc.HandleT.Setup(proc.backendConfig, proc.gatewayDB, proc.routerDB, proc.batchRouterDB, proc.procErrorDB,
		proc.clearDB, proc.reporting, proc.multitenantStats)

	currentCtx, cancel := context.WithCancel(context.Background())
	proc.currentCancel = cancel
	proc.Start(currentCtx)
}

func (proc *Processor) Stop() {
	proc.currentCancel()
	proc.gatewayDB.TearDown()
	proc.routerDB.TearDown()
	proc.batchRouterDB.TearDown()
	proc.errorDB.TearDown()
	proc.Shutdown()
}

// NewProcessor creates a new Processor intanstace
func New(ctx context.Context) *Processor {
	dbRetentionTime := 0 * time.Hour //take these from env
	clearDb := false // take this from caller function
	migrationMode := "import" // take this from caller function
	proc := &Processor{
		HandleT: &HandleT{transformer: transformer.NewTransformer()},
		mainCtx: ctx,
		gatewayDB:         &jobsdb.HandleT{},
		routerDB:          &jobsdb.HandleT{},
		batchRouterDB:     &jobsdb.HandleT{},
		procErrorDB:       &jobsdb.HandleT{},
		gwDBRetention:     &dbRetentionTime,
		routerDBRetention: &dbRetentionTime,
		clearDB:           &clearDb,
		migrationMode:     &migrationMode,
		multitenantStats:  multitenant.NOOP,
		backendConfig:     backendconfig.DefaultBackendConfig,
	}
	return proc
}
