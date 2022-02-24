package lifecyclemanager

import "context"

type LifeCycleManager interface {
	Run(ctx context.Context) error
	StartNew()
	Stop()
}
