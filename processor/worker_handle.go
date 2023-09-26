package processor

import (
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// workerHandle is the interface trying to abstract processor's [Handle] implememtation from the worker
type workerHandle interface {
	logger() logger.Logger
	config() workerHandleConfig
	rsourcesService() rsources.JobService
	handlePendingGatewayJobs(key string) bool
	stats() *processorStats

	getJobs(partition string) jobsdb.JobsResult
	markExecuting(jobs []*jobsdb.JobT) error
	jobSplitter(jobs []*jobsdb.JobT, rsourcesStats rsources.StatsCollector) []subJob
	processJobsForDest(partition string, subJobs subJob) *transformationMessage
	transformations(partition string, in *transformationMessage) *storeMessage
	Store(partition string, in *storeMessage)
}

// workerHandleConfig is a struct containing the processor.Handle configuration relevant for workers
type workerHandleConfig struct {
	maxEventsToProcess misc.ValueLoader[int]

	enablePipelining      bool
	pipelineBufferedItems int
	subJobSize            int

	readLoopSleep misc.ValueLoader[time.Duration]
	maxLoopSleep  misc.ValueLoader[time.Duration]
}
