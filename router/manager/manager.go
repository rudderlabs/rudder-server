package manager

import (
	"context"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	"github.com/rudderlabs/rudder-server/services/multitenant"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	"golang.org/x/sync/errgroup"
)

var (
	objectStorageDestinations []string
	asyncDestinations         []string
	warehouseDestinations     []string
	pkgLogger                 = logger.NewLogger().Child("router")
)

type LifecycleManager struct {
	rt               *router.Factory
	brt              *batchrouter.Factory
	mainCtx          context.Context
	currentCancel    context.CancelFunc
	waitGroup        *errgroup.Group
	DBs              *jobsdb.DBs
	multitenantStats multitenant.MultiTenantI
	reportingI       types.ReportingI
	backendConfig    backendconfig.BackendConfig
}

func (r *LifecycleManager) Run(ctx context.Context) error {
	return nil
}

// StartNew starts a Router, this is not a blocking call.
//If the router is not completely started and the data started coming then also it will not be problematic as we
//are assuming that the DBs will be up.
func (r *LifecycleManager) StartNew() {
	r.rt = &router.Factory{
		Reporting:     r.reportingI,
		Multitenant:   multitenant.NOOP,
		BackendConfig: r.backendConfig,
		RouterDB:      &r.DBs.TenantRouterDB,
		ProcErrorDB:   &r.DBs.ProcErrDB,
	}
	r.brt = &batchrouter.Factory{
		Reporting:     r.reportingI,
		Multitenant:   multitenant.NOOP,
		BackendConfig: r.backendConfig,
		RouterDB:      &r.DBs.BatchRouterDB,
		ProcErrorDB:   &r.DBs.ProcErrDB,
	}

	currentCtx, cancel := context.WithCancel(context.Background())
	r.currentCancel = cancel
	g, _ := errgroup.WithContext(context.Background())
	r.waitGroup = g
	g.Go(func() error {
		r.monitorDestRouters(currentCtx, *r.rt, *r.brt)
		return nil
	})
}

// Stop stops the Router, this is a blocking call.
func (r *LifecycleManager) Stop() {
	r.currentCancel()
	r.waitGroup.Wait()
}

// NewRouterManager creates a new Router instance
func NewRouterManager(ctx context.Context, dbs *jobsdb.DBs) *LifecycleManager {
	router.RoutersManagerSetup()
	batchrouter.BatchRoutersManagerSetup()

	return &LifecycleManager{
		rt:               &router.Factory{},
		brt:              &batchrouter.Factory{},
		mainCtx:          ctx,
		DBs:              dbs,
		multitenantStats: multitenant.NOOP,
		backendConfig:    backendconfig.DefaultBackendConfig,
	}
}

// Gets the config from config backend and extracts enabled writekeys
func (r *LifecycleManager) monitorDestRouters(ctx context.Context, routerFactory router.Factory,
	batchrouterFactory batchrouter.Factory) {
	ch := make(chan utils.DataEvent)
	r.backendConfig.Subscribe(ch, backendconfig.TopicBackendConfig)
	dstToRouter := make(map[string]*router.HandleT)
	dstToBatchRouter := make(map[string]*batchrouter.HandleT)
	cleanup := make([]func(), 0)

	//Crash recover routerDB, batchRouterDB
	//Note: The following cleanups can take time if there are too many
	//rt / batch_rt tables and there would be a delay readin from channel `ch`
	//However, this shouldn't be the problem since backend config pushes config
	//to its subscribers in separate goroutines to prevent blocking.
	routerFactory.RouterDB.DeleteExecuting(jobsdb.GetQueryParamsT{JobCount: -1})
	batchrouterFactory.RouterDB.DeleteExecuting(jobsdb.GetQueryParamsT{JobCount: -1})

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case config := <-ch:
			sources := config.Data.(backendconfig.ConfigT)
			enabledDestinations := make(map[string]bool)
			for _, source := range sources.Sources {
				for _, destination := range source.Destinations {
					enabledDestinations[destination.DestinationDefinition.Name] = true
					//For batch router destinations
					if misc.ContainsString(objectStorageDestinations, destination.DestinationDefinition.Name) || misc.ContainsString(warehouseDestinations, destination.DestinationDefinition.Name) || misc.ContainsString(asyncDestinations, destination.DestinationDefinition.Name) {
						_, ok := dstToBatchRouter[destination.DestinationDefinition.Name]
						if !ok {
							pkgLogger.Info("Starting a new Batch Destination Router ", destination.DestinationDefinition.Name)
							brt := batchrouterFactory.New(destination.DestinationDefinition.Name)
							brt.Start()
							cleanup = append(cleanup, brt.Shutdown)
							dstToBatchRouter[destination.DestinationDefinition.Name] = brt
						}
					} else {
						_, ok := dstToRouter[destination.DestinationDefinition.Name]
						if !ok {
							pkgLogger.Info("Starting a new Destination ", destination.DestinationDefinition.Name)
							rt := routerFactory.New(destination.DestinationDefinition)
							rt.Start()
							cleanup = append(cleanup, rt.Shutdown)
							dstToRouter[destination.DestinationDefinition.Name] = rt
						}
					}
				}
			}

			rm, err := router.GetRoutersManager()
			if rm != nil && err == nil {
				rm.SetRoutersReady()
			}

			brm, err := batchrouter.GetBatchRoutersManager()
			if brm != nil && err == nil {
				brm.SetBatchRoutersReady()
			}
		}
	}

	g, _ := errgroup.WithContext(context.Background())
	for _, f := range cleanup {
		f := f
		g.Go(func() error {
			f()
			return nil
		})
	}
	g.Wait()
}
