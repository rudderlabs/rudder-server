package dynamic

import (
	"context"
	"github.com/rudderlabs/rudder-server/app/apphandlers"
)

type ClusterManager struct {
	application *apphandlers.CommonAppInterface
	Ctx context.Context
}

func (d *ClusterManager) Init() {
	// initiate the dynamic cluster manager
}

func (d *ClusterManager) Run(ctx context.Context) error {
	// This will call the methods which will are responsible for two things:
	// 1. Get the expected state updates for the server
	// 2. Use the lifecycle managers of all the apps and will call the start/stop methods for those apps
	return nil
}

func (d *ClusterManager) GetExpectedState() (string, error) {
	return "", nil
}
