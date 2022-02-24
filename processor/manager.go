package processor

import (
	"context"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
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
	clearDb          *bool
)

func (proc *HandleT) Run(ctx context.Context) error {
	return nil
}

func (proc *HandleT) StartNew() {
	multitenantStats = multitenant.NOOP
	// Need to setup all the above variables, problem is those are dependent on Applications
	proc.Setup(backendconfig.DefaultBackendConfig, gatewayDB, routerDB, batchRouterDB, procErrorDB, clearDb,
		reportingI, multitenantStats)
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
