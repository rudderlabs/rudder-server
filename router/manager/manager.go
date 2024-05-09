package manager

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type LifecycleManager struct {
	logger        logger.Logger
	rt            *router.Factory
	brt           *batchrouter.Factory
	backendConfig backendconfig.BackendConfig
	currentCancel context.CancelFunc
	waitGroup     *errgroup.Group
}

// Start starts a Router, this is not a blocking call.
// If the router is not completely started and the data started coming then also it will not be problematic as we
// are assuming that the DBs will be up.
func (r *LifecycleManager) Start() error {
	currentCtx, cancel := context.WithCancel(context.Background())
	r.currentCancel = cancel
	g, _ := errgroup.WithContext(context.Background())
	r.waitGroup = g
	g.Go(func() error {
		r.monitorDestRouters(currentCtx, r.rt, r.brt)
		return nil
	})
	return nil
}

// Stop stops the Router, this is a blocking call.
func (r *LifecycleManager) Stop() {
	r.currentCancel()
	_ = r.waitGroup.Wait()
}

// New creates a new Router instance
func New(rtFactory *router.Factory, brtFactory *batchrouter.Factory,
	backendConfig backendconfig.BackendConfig, logger logger.Logger,
) *LifecycleManager {
	return &LifecycleManager{
		logger:        logger,
		rt:            rtFactory,
		brt:           brtFactory,
		backendConfig: backendConfig,
	}
}

func cleanUpAsyncDestinationsLogsDir() {
	localTmpDirName := fmt.Sprintf(`/%s/`, misc.RudderAsyncDestinationLogs)

	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		return
	}

	_ = misc.RemoveContents(fmt.Sprintf("%v%v", tmpDirPath, localTmpDirName))
}

// Gets the config from config backend and extracts enabled write-keys
func (r *LifecycleManager) monitorDestRouters(
	ctx context.Context, routerFactory *router.Factory, batchrouterFactory *batchrouter.Factory,
) {
	ch := r.backendConfig.Subscribe(ctx, backendconfig.TopicBackendConfig)
	dstToRouter := make(map[string]*router.Handle)
	dstToBatchRouter := make(map[string]*batchrouter.Handle)
	cleanup := make([]func(), 0)

	// Crash recover routerDB, batchRouterDB
	// Note: The following cleanups can take time if there are too many
	// rt / batch_rt tables and there would be a delay reading from the 'ch' channel
	// However, this shouldn't be the problem since backend config pushes config
	// to its subscribers in separate goroutines to prevent blocking.
	routerFactory.RouterDB.FailExecuting()
	batchrouterFactory.RouterDB.FailExecuting()

	// Remove all contents of aysnc destinations logs directory
	cleanUpAsyncDestinationsLogsDir()

loop:
	for {
		select {
		case <-ctx.Done():
			r.logger.Infof("Router monitor stopped Context Cancelled")
			break loop
		case data, open := <-ch:
			if !open {
				r.logger.Infof("Router monitor stopped, Config Channel Closed")
				break loop
			}
			config := data.Data.(map[string]backendconfig.ConfigT)
			for _, wConfig := range config {
				for i := range wConfig.Sources {
					source := &wConfig.Sources[i]
					for k := range source.Destinations {
						destination := &source.Destinations[k]
						// For batch router destinations
						if batchrouter.IsBatchRouterDestination(destination.DestinationDefinition.Name) {
							_, ok := dstToBatchRouter[destination.DestinationDefinition.Name]
							if !ok {
								r.logger.Infof("Starting a new Batch Destination Router: %s", destination.DestinationDefinition.Name)
								brt := batchrouterFactory.New(destination.DestinationDefinition.Name)
								brt.Start()
								cleanup = append(cleanup, brt.Shutdown)
								dstToBatchRouter[destination.DestinationDefinition.Name] = brt
							}
						} else {
							_, ok := dstToRouter[destination.DestinationDefinition.Name]
							if !ok {
								r.logger.Infof("Starting a new Destination: %s", destination.DestinationDefinition.Name)
								rt := routerFactory.New(destination)
								rt.Start()
								cleanup = append(cleanup, rt.Shutdown)
								dstToRouter[destination.DestinationDefinition.Name] = rt
							}
						}
					}
				}
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
	_ = g.Wait()
}
