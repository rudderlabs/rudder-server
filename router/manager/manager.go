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
	"time"
)

var (
	objectStorageDestinations []string
	asyncDestinations         []string
	warehouseDestinations     []string
	pkgLogger = logger.NewLogger().Child("router")
)

type Router struct {
	rt                *router.Factory
	brt               *batchrouter.Factory
	mainCtx           context.Context
	routerDB          *jobsdb.HandleT
	tenantDB          jobsdb.MultiTenantJobsDB
	batchRouterDB     *jobsdb.HandleT
	procErrorDB       *jobsdb.HandleT
	clearDB           bool
	multitenantStats  multitenant.MultiTenantI
	reportingI        types.ReportingI
	backendConfig     backendconfig.BackendConfig
	currentCancel     context.CancelFunc
	routerDBRetention time.Duration
	migrationMode     string
}

func (r *Router) Run(ctx context.Context) error {
	return nil
}

func (r *Router) StartNew() {
	r.routerDB = &jobsdb.HandleT{}
	r.tenantDB = &jobsdb.MultiTenantLegacy{HandleT: r.routerDB}
	r.batchRouterDB = &jobsdb.HandleT{}
	r.procErrorDB = &jobsdb.HandleT{}

	r.routerDB.Setup(jobsdb.ReadWrite, r.clearDB, "rt", r.routerDBRetention, r.migrationMode, true,
		router.QueryFilters)
	r.procErrorDB.Setup(jobsdb.Write, r.clearDB, "proc_error", r.routerDBRetention, r.migrationMode,
		false, jobsdb.QueryFiltersT{})
	r.batchRouterDB.Setup(jobsdb.ReadWrite, r.clearDB, "batch_rt", r.routerDBRetention, r.migrationMode, true,
		batchrouter.QueryFilters)

	r.rt = &router.Factory{
		Multitenant:   multitenant.NOOP,
		BackendConfig: r.backendConfig,
		RouterDB:      r.tenantDB,
		ProcErrorDB:   r.procErrorDB,
	}
	r.brt = &batchrouter.Factory{
		Multitenant:   multitenant.NOOP,
		BackendConfig: r.backendConfig,
		RouterDB:      r.batchRouterDB,
		ProcErrorDB:   r.procErrorDB,
	}

	currentCtx, cancel := context.WithCancel(context.Background())
	r.currentCancel = cancel
	monitorDestRouters(currentCtx, *r.rt, *r.brt)
}

func (r *Router) Stop() {

}

func NewRouterManager(ctx context.Context) *Router {
	router.RoutersManagerSetup()
	batchrouter.BatchRoutersManagerSetup()
	dbRetentionTime := 0 * time.Hour //take these from env
	clearDb := false                 // take this from caller function
	migrationMode := "import"        // take this from caller function
	return &Router{
		rt:                &router.Factory{},
		brt:               &batchrouter.Factory{},
		mainCtx:           ctx,
		tenantDB:          &jobsdb.MultiTenantHandleT{},
		batchRouterDB:     &jobsdb.HandleT{},
		procErrorDB:       &jobsdb.HandleT{},
		multitenantStats:  multitenant.NOOP,
		backendConfig:     backendconfig.DefaultBackendConfig,
		routerDBRetention: dbRetentionTime,
		clearDB:           clearDb,
		migrationMode:     migrationMode,
	}
}

// Gets the config from config backend and extracts enabled writekeys
func monitorDestRouters(ctx context.Context, routerFactory router.Factory, batchrouterFactory batchrouter.Factory) {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch, backendconfig.TopicBackendConfig)
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
