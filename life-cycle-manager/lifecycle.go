package life_cycle_manager

import "context"

type LifeCycleManager interface {
	Run(ctx context.Context) error
	StartNew()
	Stop()
}
