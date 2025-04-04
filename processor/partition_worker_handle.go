package processor

import (
	"context"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/rsources"
)

// workerHandle is the interface trying to abstract processor's [Handle] implementation from the worker
type workerHandle interface {
	logger() logger.Logger
	config() workerHandleConfig
	rsourcesService() rsources.JobService
	handlePendingGatewayJobs(key string) bool
	stats() *processorStats

	getJobsStage(ctx context.Context, partition string) jobsdb.JobsResult
	markExecuting(ctx context.Context, partition string, jobs []*jobsdb.JobT) error
	jobSplitter(ctx context.Context, jobs []*jobsdb.JobT, rsourcesStats rsources.StatsCollector) []subJob
	preprocessStage(partition string, subJobs subJob) (*preTransformationMessage, error)
	pretransformStage(partition string, preTrans *preTransformationMessage) (*transformationMessage, error)
	userTransformStage(partition string, in *transformationMessage) *userTransformData
	destinationTransformStage(partition string, in *userTransformData) *storeMessage
	storeStage(partition string, in *storeMessage)
}

// workerHandleConfig is a struct containing the processor.Handle configuration relevant for workers
type workerHandleConfig struct {
	maxEventsToProcess config.ValueLoader[int]

	enablePipelining      bool
	pipelineBufferedItems int
	subJobSize            int
	pipelinesPerPartition int

	readLoopSleep config.ValueLoader[time.Duration]
	maxLoopSleep  config.ValueLoader[time.Duration]
}
