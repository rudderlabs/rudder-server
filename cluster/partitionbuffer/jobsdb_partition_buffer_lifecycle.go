package partitionbuffer

import "context"

func (b *jobsDBPartitionBuffer) Start() error {
	for _, db := range b.lifecycleJobsDBs {
		if err := db.Start(); err != nil {
			return err
		}
	}
	b.lifecycleCtx, b.lifecycleCancel = context.WithCancel(context.Background())
	b.startBufferWatchdog()
	return nil
}

func (b *jobsDBPartitionBuffer) Stop() {
	b.lifecycleCancel()
	b.lifecycleWG.Wait()
	for _, db := range b.lifecycleJobsDBs {
		db.Stop()
	}
}
