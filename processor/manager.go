package processor

import (
	"context"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/dedup"
	"github.com/rudderlabs/rudder-server/services/multitenant"
	"github.com/rudderlabs/rudder-server/utils/types"
)

var (
	gatewayDB        *jobsdb.HandleT
	routerDB         *jobsdb.HandleT
	batchRouterDB    *jobsdb.HandleT
	procErrorDB      *jobsdb.HandleT
	multitenantStats multitenant.MultiTenantI
	reportingI       types.ReportingI
	clearDB          *bool
)

func (proc *HandleT) Run(ctx context.Context) error {
	proc.Setup(backendconfig.DefaultBackendConfig, gatewayDB, routerDB, batchRouterDB, procErrorDB, clearDB,
		reportingI, multitenantStats)
	return nil
}

func (proc *HandleT) StartNew() {
	multitenantStats = multitenant.NOOP
	// Need to setup all the above variables, problem is those are dependent on Applications
	proc.gatewayDB = gatewayDB
	proc.routerDB = routerDB
	proc.batchRouterDB = batchRouterDB
	proc.errorDB = procErrorDB
	proc.reporting = reportingI
	proc.multitenantI = multitenantStats
	if enableDedup {
		proc.dedupHandler = dedup.GetInstance(clearDB)
	}
	currentCtx, cancel := context.WithCancel(context.Background())
	proc.currentCancel = cancel
	proc.Start(currentCtx)
}

func (proc *HandleT) Stop() {
	proc.currentCancel()
	proc.gatewayDB.TearDown()
	proc.routerDB.TearDown()
	proc.batchRouterDB.TearDown()
	proc.errorDB.TearDown()
	proc.Shutdown()
}
