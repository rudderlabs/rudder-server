package batchrouter

import (
	"context"
	"sync"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

type PartitionWorker struct {
	ctx            context.Context
	cancel         context.CancelFunc
	logger         logger.Logger
	channel        chan *jobsdb.JobT
	partitionMutex sync.RWMutex
	active         *config.Reloadable[int]
}

func NewPartitionWorker(config *config.Config, logger logger.Logger, partition string) *PartitionWorker {
	ctx, cancel := context.WithCancel(context.Background())
	maxJobsToBuffer := config.GetInt("BatchRouter.partitionWorker.maxJobsToBuffer", 100000)
	return &PartitionWorker{
		ctx:     ctx,
		cancel:  cancel,
		channel: make(chan *jobsdb.JobT, maxJobsToBuffer),
		logger:  logger.With("partition", partition),
		active:  config.GetReloadableIntVar(1, 1, "BatchRouter.partitionWorker.active"),
	}
}

func (pw *PartitionWorker) AddJob(job *jobsdb.JobT) {
	pw.channel <- job
}

func (pw *PartitionWorker) Start() {

}
