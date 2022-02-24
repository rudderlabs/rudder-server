package cluster

import (
	"context"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	operationmanager "github.com/rudderlabs/rudder-server/operation-manager"
	"github.com/rudderlabs/rudder-server/processor"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"golang.org/x/sync/errgroup"
)

type ManagerStatic struct {
}

func (m ManagerStatic) Run(ctx context.Context) error {


	g, ctx := errgroup.WithContext(ctx)

	operationmanager.Setup(&gatewayDB, &routerDB, &batchRouterDB)

	g.Go(misc.WithBugsnag(func() error {
		return operationmanager.OperationManager.StartProcessLoop(ctx)
	}))

	g.Go(func() error {
		var processorInstance = processor.NewProcessor()
		processor.ProcessorManagerSetup(processorInstance)
		processorInstance.Setup(backendconfig.DefaultBackendConfig, gatewayDB, routerDB, batchRouterDB, procErrorDB, clearDB, reporting, multitenantStat)
		defer processorInstance.Shutdown()
		processorInstance.Start(ctx)
		return nil
	})

	return g.Wait()
}
