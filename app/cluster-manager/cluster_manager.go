package clustermanager
// seperate out legacy and dynamic one

import (
	"context"
	"github.com/rudderlabs/rudder-server/app/apphandlers"
)

type ClusterManager interface {
	GetExpectedState() (string, error)
	Run(ctx context.Context) error
}

type LegacyClusterManager struct {
	apphandlers.Apps
	Ctx context.Context
}

type DynamicClusterManager struct {
	apphandlers.Apps
	Ctx context.Context
}

func (lcm *LegacyClusterManager) Init() {
	// initiate the cluster manager
	// Assign the apps lifecycle
}

func (dcm *DynamicClusterManager) Init() {
	// initiate the dynamic cluster manager
}

func NewClusterManager(ctx context.Context, apps *apphandlers.Apps) (ClusterManager, error) {
	// create new cluster manager based on environment variable
	return &LegacyClusterManager{Apps: *apps, Ctx: ctx}, nil
	// Decide which cluster manager needs to be used
	// Initiate that cluster manager
	// do dependency stuff for that cluster manager
}

func (lcm *LegacyClusterManager) GetExpectedState() (string, error) {
	return "", nil
}

func (lcm *LegacyClusterManager) Run(ctx context.Context) error {

	// This will call the method which will be responsible for two things:
	// 1. Get the expected state updates for the server
	// 2. Use the lifecycle managers of all the apps and will call the start/stop methods for those apps
	return nil
}

func (dcm *DynamicClusterManager) Run(ctx context.Context) error {
	// This will call the methods which will are responsible for two things:
	// 1. Get the expected state updates for the server
	// 2. Use the lifecycle managers of all the apps and will call the start/stop methods for those apps
	return nil
}

func (dcm *DynamicClusterManager) GetExpectedState() (string, error) {
	return "", nil
}
