package legacy

import (
	"context"
	"github.com/rudderlabs/rudder-server/app/apphandlers"
)

type ClusterManager struct {
	application *apphandlers.CommonAppInterface
	Ctx context.Context
}

func (l *ClusterManager) Init() {
	// initiate the cluster manager
	// Assign the apps lifecycle
}

func (l *ClusterManager) GetExpectedState() (string, error) {
	return "", nil
}

func (l *ClusterManager) Run(ctx context.Context) error {

	// This will call the method which will be responsible for two things:
	// 1. Get the expected state updates for the server
	// 2. Use the lifecycle managers of all the apps and will call the start/stop methods for those apps
	return nil
}
