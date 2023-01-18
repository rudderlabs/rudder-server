package processor

import (
	"context"
	"fmt"
	"sync"

	"github.com/rudderlabs/rudder-go-kit/stats/metric"
	transformationdebugger "github.com/rudderlabs/rudder-server/services/debugger/transformation"
	"github.com/rudderlabs/rudder-server/services/rmetrics"

	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/services/multitenant"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/samber/lo"
)

type LifecycleManager struct {
	Handle           *Handle
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
		proc.Handle.transformer = proc.Transformer
	}

	proc.Handle.Setup(
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
		if err := proc.Handle.Start(currentCtx); err != nil {
			proc.Handle.logger.Errorf("Error starting processor: %v", err)
		}
	}()
	return nil
}

// Stop stops the processor, this is a blocking call.
func (proc *LifecycleManager) Stop() {
	proc.currentCancel()
	proc.Handle.Shutdown()
	proc.waitGroup.Wait()
}

func WithFeaturesRetryMaxAttempts(maxAttempts int) func(l *LifecycleManager) {
	return func(l *LifecycleManager) {
		l.Handle.config.featuresRetryMaxAttempts = maxAttempts
	}
}

// New creates a new Processor instance
func New(ctx context.Context, clearDb *bool, gwDb, rtDb, brtDb, errDb *jobsdb.HandleT,
	tenantDB multitenant.MultiTenantI, reporting types.ReportingI, transientSources transientsource.Service, fileuploader fileuploader.Provider,
	rsourcesService rsources.JobService, destDebugger destinationdebugger.DestinationDebugger, transDebugger transformationdebugger.TransformationDebugger,
	opts ...Opts,
) *LifecycleManager {
	proc := &LifecycleManager{
		Handle:           NewHandle(transformer.NewTransformer()),
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
	for _, opt := range opts {
		opt(proc)
	}
	return proc
}

type Opts func(l *LifecycleManager)

func WithAdaptiveLimit(adaptiveLimitFunction func(int64) int64) Opts {
	return func(l *LifecycleManager) {
		l.Handle.adaptiveLimit = adaptiveLimitFunction
	}
}
func (proc *Handle) cleanUpRetiredJobs(ctx context.Context) {
	ch := proc.backendConfig.Subscribe(ctx, backendconfig.TopicProcessConfig)
	for {
		select {
		case <-ctx.Done():
			return
		case data := <-ch:
			config := data.Data.(map[string]backendconfig.ConfigT)
			workspaceMap := make(map[string]struct{})
			for workspace := range config {
				workspaceMap[workspace] = struct{}{}
			}
			proc.doCleanupRetiredJobs(ctx, workspaceMap)
		}
	}
}

func (proc *Handle) doCleanupRetiredJobs(ctx context.Context, workspaceMap map[string]struct{}) {
	// router
	routerPendingEventMetrics := metric.Instance.
		GetRegistry(metric.PublishedMetrics).
		GetMetricsByName(fmt.Sprintf(rmetrics.JobsdbPendingEventsCount, "rt"))
	retiredRouterWorkspaces := lo.Reject(
		lo.Map(
			routerPendingEventMetrics,
			func(gaugeMetric metric.TagsWithValue, _ int) string {
				return gaugeMetric.Tags["workspace"]
			},
		),
		func(workspace string, _ int) bool {
			_, ok := workspaceMap[workspace]
			return !ok
		},
	)
	if len(retiredRouterWorkspaces) > 0 {
		if err := proc.routerDB.CleanUpRetiredJobs(
			ctx,
			retiredRouterWorkspaces,
		); err != nil {
			proc.logger.Errorf("Error cleaning up retired jobs for router: %v", err)
		}
	}

	// batch router
	batchRouterPendingEventMetrics := metric.Instance.
		GetRegistry(metric.PublishedMetrics).
		GetMetricsByName(fmt.Sprintf(rmetrics.JobsdbPendingEventsCount, "brt"))

	retiredBatchRouterWorkspaces := lo.Reject(
		lo.Map(
			batchRouterPendingEventMetrics,
			func(gaugeMetric metric.TagsWithValue, _ int) string {
				return gaugeMetric.Tags["workspace"]
			},
		),
		func(workspace string, _ int) bool {
			_, ok := workspaceMap[workspace]
			return !ok
		},
	)

	if len(retiredBatchRouterWorkspaces) > 0 {
		if err := proc.batchRouterDB.CleanUpRetiredJobs(
			ctx,
			retiredBatchRouterWorkspaces,
		); err != nil {
			proc.logger.Errorf("Error cleaning up retired jobs for batch router: %v", err)
		}
	}
}
