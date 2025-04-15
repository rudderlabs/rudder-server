package batchrouter

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	asynccommon "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type PartitionWorker struct {
	ctx            context.Context
	cancel         context.CancelFunc
	logger         logger.Logger
	brt            *Handle
	channel        chan *JobEntry
	partitionMutex sync.RWMutex
	active         *config.Reloadable[int]
}

type JobEntry struct {
	job      *jobsdb.JobT
	sourceID string
	destID   string
}

func NewPartitionWorker(logger logger.Logger, partition string, brt *Handle) *PartitionWorker {
	ctx, cancel := context.WithCancel(context.Background())
	maxJobsToBuffer := brt.conf.GetInt("BatchRouter.partitionWorker.maxJobsToBuffer", 100000)
	return &PartitionWorker{
		ctx:     ctx,
		cancel:  cancel,
		channel: make(chan *JobEntry, maxJobsToBuffer),
		logger:  logger.With("partition", partition),
		active:  brt.conf.GetReloadableIntVar(1, 1, "BatchRouter.partitionWorker.active"),
		brt:     brt,
	}
}

func (pw *PartitionWorker) AddJob(job *jobsdb.JobT, sourceID, destID string) {
	pw.channel <- &JobEntry{
		job:      job,
		sourceID: sourceID,
		destID:   destID,
	}
}

func (pw *PartitionWorker) Start() {
	uploadFreq := pw.brt.uploadFreq.Load()
	jobBatch := make([]*JobEntry, 0)
	active := 0
	timer := time.NewTimer(uploadFreq)
	// Helper function to safely stop and reset the timer
	resetTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(uploadFreq)
	}

	processBatch := func(jobBatch []*JobEntry) {
		jobsPerConnection := make(map[string]map[string][]*jobsdb.JobT)
		for _, job := range jobBatch {
			if _, ok := jobsPerConnection[job.sourceID]; !ok {
				jobsPerConnection[job.sourceID] = make(map[string][]*jobsdb.JobT)
			}
			jobsPerConnection[job.sourceID][job.destID] = append(jobsPerConnection[job.sourceID][job.destID], job.job)
		}
		for sourceID, destJobs := range jobsPerConnection {
			for destID, jobs := range destJobs {
				pw.processAndUploadBatch(sourceID, destID, jobs)
			}
		}
	}

	go func() {
		for {
			select {
			case job, ok := <-pw.channel:
				if !ok {
					if len(jobBatch) > 0 {
						processBatch(jobBatch)
					}
					return
				}
				jobBatch = append(jobBatch, job)
				if len(jobBatch) > pw.brt.maxEventsInABatch {
					pw.partitionMutex.Lock()
					if active <= pw.active.Load() {
						active++
						go func() {
							defer func() {
								pw.partitionMutex.Lock()
								active--
								resetTimer()
								pw.partitionMutex.Unlock()
							}()
							processBatch(jobBatch)
						}()
					}
					pw.partitionMutex.Unlock()
				}
			case <-timer.C:
				pw.partitionMutex.Lock()
				if active <= pw.active.Load() {
					active++
					go func() {
						defer func() {
							pw.partitionMutex.Lock()
							active--
							resetTimer()
							pw.partitionMutex.Unlock()
						}()
					}()
				}
				pw.partitionMutex.Unlock()
				resetTimer()
			case <-pw.ctx.Done():
				return
			}
		}
	}()
}

func (pw *PartitionWorker) processAndUploadBatch(sourceID, destID string, jobs []*jobsdb.JobT) {
	if len(jobs) == 0 {
		return
	}
	// Get destination and source details
	pw.brt.configSubscriberMu.RLock()
	destWithSources, ok := pw.brt.destinationsMap[destID]
	pw.brt.configSubscriberMu.RUnlock()

	if !ok {
		// Handle destination not found
		pw.logger.Errorf("Destination not found for ID: %s", destID)
		return
	}

	// Find the source
	var source backendconfig.SourceT
	sourceFound := false
	for _, s := range destWithSources.Sources {
		if s.ID == sourceID {
			source = s
			sourceFound = true
			break
		}
	}

	if !sourceFound {
		// Handle source not found
		pw.logger.Errorf("Source not found for ID: %s", sourceID)
		return
	}

	batchedJobs := BatchedJobs{
		Jobs: jobs,
		Connection: &Connection{
			Destination: destWithSources.Destination,
			Source:      source,
		},
	}

	// Use the existing upload logic
	defer pw.brt.limiter.upload.Begin("")()

	// Helper function for standard object storage upload process
	processObjectStorageUpload := func(destType string, isWarehouse bool) {
		output := pw.brt.upload(destType, &batchedJobs, isWarehouse)
		pw.brt.recordDeliveryStatus(*batchedJobs.Connection, output, isWarehouse)
		pw.brt.updateJobStatus(&batchedJobs, isWarehouse, output.Error, false)
		misc.RemoveFilePaths(output.LocalFilePaths...)
		if output.JournalOpID > 0 {
			pw.brt.jobsDB.JournalDeleteEntry(output.JournalOpID)
		}
		if output.Error == nil {
			pw.brt.recordUploadStats(*batchedJobs.Connection, output)
		}
	}

	switch {
	case IsObjectStorageDestination(pw.brt.destType):
		processObjectStorageUpload(pw.brt.destType, false)
	case IsWarehouseDestination(pw.brt.destType):
		useRudderStorage := misc.IsConfiguredToUseRudderObjectStorage(batchedJobs.Connection.Destination.Config)
		objectStorageType := warehouseutils.ObjectStorageType(pw.brt.destType, batchedJobs.Connection.Destination.Config, useRudderStorage)
		splitBatchJobs := pw.brt.splitBatchJobsOnTimeWindow(batchedJobs)
		for _, batchJob := range splitBatchJobs {
			output := pw.brt.upload(objectStorageType, batchJob, true)
			notifyWarehouseErr := false
			if output.Error == nil && output.Key != "" {
				output.Error = pw.brt.pingWarehouse(batchJob, output)
				if output.Error != nil {
					notifyWarehouseErr = true
				}
				warehouseutils.DestStat(stats.CountType, "generate_staging_files", batchJob.Connection.Destination.ID).Count(1)
				warehouseutils.DestStat(stats.CountType, "staging_file_batch_size", batchJob.Connection.Destination.ID).Count(len(batchJob.Jobs))
			}
			pw.brt.recordDeliveryStatus(*batchJob.Connection, output, true)
			pw.brt.updateJobStatus(batchJob, true, output.Error, notifyWarehouseErr)
			misc.RemoveFilePaths(output.LocalFilePaths...)
		}
	case asynccommon.IsAsyncDestination(pw.brt.destType):
		pw.brt.sendJobsToStorage(batchedJobs)
	default:
		// Handle any other destination types
		pw.logger.Warnf("Unsupported destination type %s for job buffer. Attempting generic processing.", pw.brt.destType)
		panic(fmt.Sprintf("Unsupported destination type %s for job buffer. Attempting generic processing.", pw.brt.destType))
	}
}

func (pw *PartitionWorker) Stop() {
	close(pw.channel)
	pw.cancel()
}
