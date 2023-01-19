package processor

import (
	"context"
	"sync"

	transformationdebugger "github.com/rudderlabs/rudder-server/services/debugger/transformation"

	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/services/multitenant"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type LifecycleManager struct {
	HandleT          *HandleT
	mainCtx          context.Context
	currentCancel    context.CancelFunc
	waitGroup        interface{ Wait() }
	gatewayDB        *jobsdb.HandleT
	routerDB         *jobsdb.HandleT
	batchRouterDB    *jobsdb.HandleT
	errDB            *jobsdb.HandleT
	clearDB          *bool
	MultitenantStats multitenant.MultiTenantI // need not initialize again
	ReportingI       types.ReportingI         // need not initialize again
	BackendConfig    backendconfig.BackendConfig
	Transformer      transformer.Transformer
	transientSources transientsource.Service
	fileuploader     fileuploader.Provider
	rsourcesService  rsources.JobService
	destDebugger     destinationdebugger.DestinationDebugger
	transDebugger    transformationdebugger.TransformationDebugger
}

// Start starts a processor, this is not a blocking call.
// If the processor is not completely started and the data started coming then also it will not be problematic as we
// are assuming that the DBs will be up.
func (proc *LifecycleManager) Start() error {
	if proc.Transformer != nil {
		proc.HandleT.transformer = proc.Transformer
	}

	proc.HandleT.Setup(
		proc.BackendConfig, proc.gatewayDB, proc.routerDB, proc.batchRouterDB, proc.errDB,
		proc.clearDB, proc.ReportingI, proc.MultitenantStats, proc.transientSources, proc.fileuploader, proc.rsourcesService, proc.destDebugger, proc.transDebugger,
	)

	currentCtx, cancel := context.WithCancel(context.Background())
	proc.currentCancel = cancel

	var wg sync.WaitGroup
	proc.waitGroup = &wg

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := proc.HandleT.Start(currentCtx); err != nil {
			proc.HandleT.logger.Errorf("Error starting processor: %v", err)
		}
	}()
	return nil
}

// Stop stops the processor, this is a blocking call.
func (proc *LifecycleManager) Stop() {
	proc.currentCancel()
	proc.HandleT.Shutdown()
	proc.waitGroup.Wait()
}

// New creates a new Processor instance
func New(ctx context.Context, clearDb *bool, gwDb, rtDb, brtDb, errDb *jobsdb.HandleT,
	tenantDB multitenant.MultiTenantI, reporting types.ReportingI, transientSources transientsource.Service, fileuploader fileuploader.Provider,
	rsourcesService rsources.JobService, destDebugger destinationdebugger.DestinationDebugger, transDebugger transformationdebugger.TransformationDebugger,
) *LifecycleManager {
	proc := &LifecycleManager{
		HandleT:          &HandleT{transformer: transformer.NewTransformer()},
		mainCtx:          ctx,
		gatewayDB:        gwDb,
		routerDB:         rtDb,
		batchRouterDB:    brtDb,
		errDB:            errDb,
		clearDB:          clearDb,
		MultitenantStats: tenantDB,
		BackendConfig:    backendconfig.DefaultBackendConfig,
		ReportingI:       reporting,
		transientSources: transientSources,
		fileuploader:     fileuploader,
		rsourcesService:  rsourcesService,
		destDebugger:     destDebugger,
		transDebugger:    transDebugger,
	}
	return proc
}
