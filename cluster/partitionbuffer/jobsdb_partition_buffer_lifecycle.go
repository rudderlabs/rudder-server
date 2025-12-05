package partitionbuffer

func (b *jobsDBPartitionBuffer) Start() error {
	for _, db := range b.lifecycleJobsDBs {
		if err := db.Start(); err != nil {
			return err
		}
	}
	return nil
}

func (b *jobsDBPartitionBuffer) Stop() {
	for _, db := range b.lifecycleJobsDBs {
		db.Stop()
	}
}
