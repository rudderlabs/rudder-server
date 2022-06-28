package gateway

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/rudderlabs/rudder-server/app/cluster"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

type Dynamic struct {
	Provider cluster.ChangeEventProvider

	Gateway *HandleT

	currentWorkspaceIDs string

	logger logger.LoggerI

	once sync.Once
}

func (dm *Dynamic) init() {
	dm.logger = logger.NewLogger().Child("GW Manager")
}

func (dm *Dynamic) Run(ctx context.Context) error {
	dm.once.Do(dm.init)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	workspaceIDsChan := dm.Provider.WorkspaceIDs(ctx)
	for {
		select {
		case <-ctx.Done():
			err := dm.Gateway.Shutdown()
			if err != nil {
				pkgLogger.Warnf("Gateway shutdown error: %v", err)
			}
		case change := <-workspaceIDsChan:
			if change.Err() != nil {
				return change.Err()
			}
			ids := strings.Join(change.WorkspaceIDs(), ",")

			pkgLogger.Infof("Got trigger to change workspaceIDs: %q", ids)
			err := dm.HandleWorkspaceChange(ctx, ids)
			if err != nil {
				return err
			}
			pkgLogger.Debugf("Acknowledging the workspaceIDs change")

			if err := change.Ack(ctx); err != nil {
				return fmt.Errorf("ack workspaceIDs change: %w", err)
			}
		}
	}
}

func (dm *Dynamic) HandleWorkspaceChange(ctx context.Context, workspaces string) error {
	if dm.Gateway.currentWorkspaceIDs == workspaces {
		return nil
	}
	dm.Gateway.backendConfig.Stop()
	dm.Gateway.backendConfig.StartWithIDs(workspaces)
	dm.Gateway.currentWorkspaceIDs = workspaces
	return dm.Gateway.backendConfig.WaitForConfig(ctx)
}

func NewGatewayManager(provider cluster.ChangeEventProvider, gateway *HandleT) *Dynamic {
	return &Dynamic{
		Provider:            provider,
		Gateway:             gateway,
		currentWorkspaceIDs: "",
	}
}
