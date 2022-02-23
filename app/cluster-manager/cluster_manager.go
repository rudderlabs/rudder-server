package clustermanager
// seperate out legacy and dynamic one

import (
	"context"
)

type ClusterManager interface {
	GetExpectedState() (string, error)
	Run(ctx context.Context) error
}

func NewClusterManager(ctx context.Context) (ClusterManager, error) {
	// create new cluster manager based on environment variable
	// Decide which cluster manager needs to be used
	// Initiate that cluster manager
	// do dependency stuff for that cluster manager
	return nil, nil
}
