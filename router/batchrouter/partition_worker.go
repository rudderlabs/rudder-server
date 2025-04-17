package batchrouter

import (
	"context"
	"fmt"
	"sync"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	asynccommon "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/batchrouter/circuitbreaker"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type PartitionWorker struct {
	wg      sync.WaitGroup
	logger  logger.Logger
	brt     *Handle
	channel chan *JobEntry
	active  *config.Reloadable[int]
	cb      circuitbreaker.CircuitBreaker
	limiter kitsync.Limiter
}

type JobEntry struct {
	job      *jobsdb.JobT
	sourceID string
	destID   string
}

func NewPartitionWorker(logger logger.Logger, partition string, brt *Handle, cb circuitbreaker.CircuitBreaker) *PartitionWorker {
	maxJobsToBuffer := brt.conf.GetInt("BatchRouter.partitionWorker.maxJobsToBuffer", 100000)
	pw := &PartitionWorker{
		channel: make(chan *JobEntry, maxJobsToBuffer),
		logger:  logger.With("partition", partition),
		active:  brt.conf.GetReloadableIntVar(1, 1, "BatchRouter.partitionWorker.active"),
		brt:     brt,
		cb:      cb,
		limiter: kitsync.NewLimiter(
			context.Background(), &sync.WaitGroup{}, "batchrouter_partition_worker",
			brt.conf.GetInt("BatchRouter.partitionWorker.concurrency", 10),
			stats.Default,
		),
	}
	return pw
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
	pw.logger.Infof("Starting partition worker with upload frequency %s", uploadFreq)
	processJobs := func(jobs []*JobEntry) {
		pw.wg.Add(1)
		done := pw.limiter.Begin("")
		go func() {
			defer done()
			defer pw.wg.Done()
			jobsPerConnection := lo.GroupBy(jobs, func(job *JobEntry) lo.Tuple2[string, string] {
				return lo.Tuple2[string, string]{A: job.sourceID, B: job.destID}
			})
			var wg sync.WaitGroup
			for connection, jobs := range jobsPerConnection {
				wg.Add(1)
				go func() {
					defer wg.Done()
					pw.processAndUploadBatch(connection.A, connection.B, lo.Map(jobs, func(job *JobEntry, _ int) *jobsdb.JobT { return job.job }))
				}()
			}
			wg.Wait()
		}()
	}
	pw.wg.Add(1)
	go func() {
		defer pw.wg.Done()
		for {
			jobs, jobsLength, _, ok := lo.BufferWithTimeout(pw.channel, pw.brt.maxEventsInABatch, uploadFreq)
			if jobsLength > 0 {
				processJobs(jobs)
			}
			if !ok {
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
		batchedJobs := BatchedJobs{
			Jobs: jobs,
			Connection: &Connection{
				Destination: backendconfig.DestinationT{
					ID: destID,
				},
				Source: backendconfig.SourceT{
					ID: sourceID,
				},
			},
		}
		err := fmt.Errorf("BRT: Batch destination not found in config for destID: %s", destID)
		pw.brt.updateJobStatus(&batchedJobs, false, err, false)
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
		batchedJobs := BatchedJobs{
			Jobs: jobs,
			Connection: &Connection{
				Destination: backendconfig.DestinationT{
					ID: destID,
				},
				Source: backendconfig.SourceT{
					ID: sourceID,
				},
			},
		}
		err := fmt.Errorf("BRT: Batch destination source not found in config for sourceID: %s", sourceID)
		pw.brt.updateJobStatus(&batchedJobs, false, err, false)
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
			pw.cb.Success()
		} else {
			pw.cb.Failure()
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

			if output.Error == nil {
				pw.cb.Success()
			} else {
				pw.cb.Failure()
			}
		}
	case asynccommon.IsAsyncDestination(pw.brt.destType):
		err := pw.brt.sendJobsToStorage(batchedJobs)
		if err != nil {
			pw.cb.Failure()
		} else {
			pw.cb.Success()
		}
	default:
		// Handle any other destination types
		pw.logger.Warnf("Unsupported destination type %s for job buffer. Attempting generic processing.", pw.brt.destType)
		panic(fmt.Sprintf("Unsupported destination type %s for job buffer. Attempting generic processing.", pw.brt.destType))
	}
}

func (pw *PartitionWorker) Stop() {
	close(pw.channel)
	pw.wg.Wait()
}
