package batchrouter

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	asynccommon "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/batchrouter/circuitbreaker"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type PartitionWorker struct {
	wg      *sync.WaitGroup
	cancel  context.CancelFunc
	logger  logger.Logger
	brt     *Handle
	channel chan *JobEntry
	cb      circuitbreaker.CircuitBreaker
	limiter kitsync.Limiter

	delayedJobAdditionTimerFrequency time.Duration
}

type JobEntry struct {
	job      *jobsdb.JobT
	sourceID string
	destID   string
}

func NewPartitionWorker(logger logger.Logger, partition string, brt *Handle, cb circuitbreaker.CircuitBreaker) *PartitionWorker {
	maxJobsToBuffer := brt.conf.GetIntVar(100000, 1, "BatchRouter."+brt.destType+".partitionWorker.maxJobsToBuffer", "BatchRouter.partitionWorker.maxJobsToBuffer")
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	pw := &PartitionWorker{
		wg:      wg,
		cancel:  cancel,
		channel: make(chan *JobEntry, maxJobsToBuffer),
		logger:  logger.With("partition", partition),
		brt:     brt,
		cb:      cb,
		limiter: kitsync.NewReloadableLimiter(
			ctx, wg, "batchrouter_partition_worker",
			brt.conf.GetReloadableIntVar(10, 1, "BatchRouter."+brt.destType+".partitionWorker.concurrency", "BatchRouter.partitionWorker.concurrency"),
			stats.Default,
		),
		delayedJobAdditionTimerFrequency: brt.conf.GetDurationVar(5, time.Second, "BatchRouter."+brt.destType+".partitionWorker.addJobMaxDelaySeconds", "BatchRouter.partitionWorker.addJobMaxDelaySeconds"),
	}
	return pw
}

func (pw *PartitionWorker) AddJob(job *jobsdb.JobT, sourceID, destID string) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go pw.monitorDelayedJobAddition(ctx, &wg, stats.Tags{"source_id": sourceID, "destination_id": destID})
	pw.channel <- &JobEntry{
		job:      job,
		sourceID: sourceID,
		destID:   destID,
	}
	cancel()
	wg.Wait()
}

// monitorDelayedJobAddition runs in a goroutine to periodically report delayed job additions.
func (pw *PartitionWorker) monitorDelayedJobAddition(ctx context.Context, wg *sync.WaitGroup, statTags stats.Tags) {
	defer wg.Done()
	start := time.Now()
	initialTimer := time.NewTimer(pw.delayedJobAdditionTimerFrequency)
	defer initialTimer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-initialTimer.C:
		stats.Default.NewTaggedStat("batchrouter_partition_worker_add_job_delay", stats.TimerType, statTags).SendTiming(time.Since(start))
	}
}

func (pw *PartitionWorker) Start() {
	// Correct Start method implementation
	uploadFreq := pw.brt.uploadFreq
	pw.logger.Infof("Starting partition worker with upload frequency %s", uploadFreq.Load())
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
			jobs, jobsLength, _, ok := lo.BufferWithTimeout(pw.channel, pw.brt.maxEventsInABatch, uploadFreq.Load())
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

	// getSource is a helper function to get the source from the config
	getSourceAndDestination := func(destID, sourceID string) (backendconfig.SourceT, backendconfig.DestinationT, error) {
		pw.brt.configSubscriberMu.RLock()
		destWithSources, ok := pw.brt.destinationsMap[destID]
		pw.brt.configSubscriberMu.RUnlock()
		if !ok {
			return backendconfig.SourceT{}, backendconfig.DestinationT{}, fmt.Errorf("destination not found in config for id: %s", destID)
		}
		s, found := lo.Find(destWithSources.Sources, func(s backendconfig.SourceT) bool {
			return s.ID == sourceID
		})
		if !found {
			return backendconfig.SourceT{}, backendconfig.DestinationT{
				ID: destID,
			}, fmt.Errorf("source not found in config for id: %s", sourceID)
		}
		return s, destWithSources.Destination, nil
	}

	source, destination, err := getSourceAndDestination(destID, sourceID)
	if err != nil {
		batchedJobs := BatchedJobs{
			Jobs: jobs,
			Connection: &Connection{
				Destination: destination,
				Source:      source,
			},
		}
		pw.brt.updateJobStatus(&batchedJobs, false, err, false)
		pw.logger.Errorn("Error while getting source and destination", obskit.Error(err))
		return
	}

	batchedJobs := BatchedJobs{
		Jobs: jobs,
		Connection: &Connection{
			Destination: destination,
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
	pw.cancel()
	pw.wg.Wait()
}
