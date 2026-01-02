// Migration Job Executor, i.e. moving a set of partitions from one node's jobsdb to another
package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/jobsdb"
	proto "github.com/rudderlabs/rudder-server/proto/cluster"
)

type Opt func(*migrationJobExecutor)

// WithConfig sets the config to be used by the executor
func WithConfig(conf *config.Config) Opt {
	return func(mpe *migrationJobExecutor) {
		mpe.conf = conf
	}
}

// WithLogger sets the logger to be used by the executor
func WithLogger(logger logger.Logger) Opt {
	return func(mpe *migrationJobExecutor) {
		mpe.logger = logger
	}
}

// WithStats sets the stats instance to be used by the executor
func WithStats(stats stats.Stats) Opt {
	return func(mpe *migrationJobExecutor) {
		mpe.stats = stats
	}
}

// NewMigrationJobExecutor creates a new MigrationJobExecutor
func NewMigrationJobExecutor(migrationJobID string, partitionIDs []string, sourceDB jobsdb.JobsDB, target string, opts ...Opt) MigrationJobExecutor {
	mpe := &migrationJobExecutor{
		migrationJobID: migrationJobID,
		partitionIDs:   partitionIDs,
		sourceDB:       sourceDB,
		target:         target,
	}
	for _, opt := range opts {
		opt(mpe)
	}
	if mpe.conf == nil {
		mpe.conf = config.Default
	}
	if mpe.logger == nil {
		mpe.logger = logger.NewLogger().Child("partitionmigration")
	}
	if mpe.stats == nil {
		mpe.stats = stats.Default
	}
	mpe.logger = mpe.logger.Withn(
		logger.NewStringField("migrationJobID", mpe.migrationJobID),
		logger.NewStringField("tablePrefix", mpe.sourceDB.Identifier()),
		logger.NewStringField("partitions", strings.Join(mpe.partitionIDs, ",")),
		logger.NewStringField("target", mpe.target),
	)
	mpe.batchSize = mpe.conf.GetReloadableIntVar(10000, 1, "PartitionMigration.Executor.BatchSize")
	mpe.chunkSize = mpe.conf.GetReloadableIntVar(1000, 1, "PartitionMigration.Executor.ChunkSize")
	mpe.progressPeriod = mpe.conf.GetReloadableDurationVar(30, time.Second, "PartitionMigration.Executor.ProgressPeriod")
	mpe.sendTimeout = mpe.conf.GetReloadableDurationVar(10, time.Minute, "PartitionMigration.Executor.SendTimeout", "PartitionMigration.Executor.StreamTimeout")
	mpe.receiveTimeout = mpe.conf.GetReloadableDurationVar(10, time.Minute, "PartitionMigration.Executor.ReceiveTimeout", "PartitionMigration.Executor.StreamTimeout")
	return mpe
}

type MigrationJobExecutor interface {
	// Run executes the migration job
	Run(ctx context.Context) error
}

type migrationJobExecutor struct {
	migrationJobID string   // unique identifier for the migration job
	partitionIDs   []string // partitions to be migrated

	sourceDB jobsdb.JobsDB // source jobsdb
	target   string        // target address of the partition migrator server

	conf   *config.Config
	logger logger.Logger
	stats  stats.Stats

	batchSize      config.ValueLoader[int]           // number of jobs to fetch in each batch
	chunkSize      config.ValueLoader[int]           // number of jobs to send in each chunk to the remote server
	progressPeriod config.ValueLoader[time.Duration] // period for logging progress
	sendTimeout    config.ValueLoader[time.Duration] // timeout for stream send operations
	receiveTimeout config.ValueLoader[time.Duration] // timeout for stream receive operations
}

// Run executes the migration job
func (mpe *migrationJobExecutor) Run(ctx context.Context) error {
	defer mpe.stats.NewTaggedStat("partition_mig_jobexec_run_time", stats.TimerType, mpe.statsTags()).RecordDuration()()
	mpe.logger.Infon("Starting partition migration")

	// mark any executing jobs as failed to handle previous interrupted migrations
	if err := mpe.markExecutingJobsAsFailed(ctx); err != nil {
		return fmt.Errorf("marking executing jobs as failed: %w", err)
	}

	// create a partition migrator client
	client, err := NewPartitionMigrationClient(mpe.target, mpe.conf)
	if err != nil {
		return fmt.Errorf("creating partition migrator client: %w", err)
	}
	defer func() { // close the client on exit
		if err := client.Close(); err != nil {
			mpe.logger.Errorn("closing partition migrator client", obskit.Error(err))
		}
	}()

	streamGroup, streamCtx := errgroup.WithContext(ctx) // group for sender and receiver goroutines
	// open the stream
	mpe.logger.Debugn("Opening the grpc stream")
	stream, err := client.StreamJobs(streamCtx)
	if err != nil {
		return fmt.Errorf("initiating stream jobs: %w", err)
	}

	var totalSent atomic.Int64  // counter for total sent jobs
	var totalAcked atomic.Int64 // counter for total acknowledged jobs

	var unackedBatchesMu sync.Mutex
	unackedBatches := make(map[int][]*jobsdb.JobT)            // map of batch index to unacked jobs in case of failure, for marking them as failed.
	batchesToAck := make(chan lo.Tuple2[int, []*jobsdb.JobT]) // channel for sending unacked batch info to the receiver goroutine to acknowledge (one at a time). channel is closed when sender is done

	// sender goroutine
	streamGroup.Go(func() error {
		mpe.logger.Infon("Starting to send jobs for migration")
		defer mpe.logger.Infon("Finished sending jobs for migration")

		// start by sending the metadata
		mpe.logger.Debugn("Sending metadata")
		if err := mpe.sendWithTimeout(ctx, stream, &proto.StreamJobsRequest{Payload: &proto.StreamJobsRequest_Metadata{
			Metadata: &proto.JobStreamMetadata{
				MigrationJobId: mpe.migrationJobID,
				TablePrefix:    mpe.sourceDB.Identifier(),
				PartitionIds:   mpe.partitionIDs,
			},
		}}); err != nil {
			return fmt.Errorf("sending metadata: %w", err)
		}
		mpe.logger.Debugn("Metadata was sent")

		// send job batches in chunks
		batchIndex := -1

		var noMoreJobs bool // signals the end of jobs to be sent
		// As the last batch we are always sending an empty chunk, in order to avoid a race condition in deduplication, if this is enabled on server:
		// The server, after sending an acknowledgement for the last batch, it then resets its deduplication state for this migration job id.
		// If client crashes before marking the jobs as migrated, upon restart it will resend the last batch which will not be deduplicated by server.
		for {
			batchIndex++
			if noMoreJobs {
				// we sent an empty chunk in the previous iteration to signal end of jobs
				// we are done sending
				_ = stream.CloseSend()
				close(batchesToAck)
				return nil
			}
			// get next batch of jobs to process
			mpe.logger.Debugn("Getting next batch of jobs to process",
				logger.NewIntField("batchIndex", int64(batchIndex)),
			)
			jobs, err := mpe.sourceDB.GetToProcess(streamCtx, jobsdb.GetQueryParams{
				PartitionFilters: mpe.partitionIDs,
				JobsLimit:        mpe.batchSize.Load(),
			}, nil)
			if err != nil {
				return fmt.Errorf("getting batch %d of jobs to process: %w", batchIndex, err)
			}
			if len(jobs.Jobs) == 0 && !jobs.DSLimitsReached {
				// no more jobs to process, we are done sending
				noMoreJobs = true
				mpe.logger.Debugn("No more jobs to process, sending empty chunk to signal end of jobs",
					logger.NewIntField("batchIndex", int64(batchIndex)),
				)
			}
			mpe.logger.Debugn("About to send batch of jobs",
				logger.NewIntField("batchIndex", int64(batchIndex)),
				logger.NewIntField("jobCount", int64(len(jobs.Jobs))),
			)
			mpe.logger.Debugn("Marking jobs as executing",
				logger.NewIntField("batchIndex", int64(batchIndex)),
			)
			// mark them as executing
			if err := mpe.updateJobStatus(streamCtx, jobs.Jobs, jobsdb.Executing.State, nil); err != nil {
				return fmt.Errorf("marking batch %d of jobs as executing: %w", batchIndex, err)
			}
			mpe.logger.Debugn("Marked jobs as executing",
				logger.NewIntField("batchIndex", int64(batchIndex)),
			)
			// add to unacked batches
			unackedBatchesMu.Lock()
			unackedBatches[batchIndex] = jobs.Jobs
			unackedBatchesMu.Unlock()

			// split jobs into chunks and send them
			chunks := lo.Chunk(jobs.Jobs, mpe.chunkSize.Load())
			if len(chunks) == 0 {
				chunks = [][]*jobsdb.JobT{{}} // we will send one empty chunk if there are no jobs
			}
			mpe.logger.Debugn("Sending batch in chunks",
				logger.NewIntField("batchIndex", int64(batchIndex)),
				logger.NewIntField("chunkCount", int64(len(chunks))),
			)
			for i, chunk := range chunks {
				mpe.logger.Debugn("Sending chunk",
					logger.NewIntField("batchIndex", int64(batchIndex)),
					logger.NewIntField("chunk", int64(i+1)),
				)
				lastChunk := i == len(chunks)-1
				if lastChunk { // send batch info to receiver only just before the last chunk
					mpe.logger.Debugn("Sending batch info to the receiver goroutine",
						logger.NewIntField("batchIndex", int64(batchIndex)),
					)
					select {
					case batchesToAck <- lo.T2(batchIndex, jobs.Jobs):
						// added successfully
					case <-streamCtx.Done():
						return streamCtx.Err()
					}
				}
				if err := mpe.sendWithTimeout(streamCtx, stream, &proto.StreamJobsRequest{
					Payload: &proto.StreamJobsRequest_Chunk{
						Chunk: &proto.JobsBatchChunk{
							BatchIndex: int64(batchIndex),
							LastChunk:  lastChunk,
							Jobs:       lo.Map(chunk, proto.JobFromJobsdbJob),
						},
					},
				}); err != nil {
					return fmt.Errorf("streaming jobs chunk %d for batch %d: %w", i+1, batchIndex, err)
				}
				mpe.logger.Debugn("Sent chunk",
					logger.NewIntField("batchIndex", int64(batchIndex)),
					logger.NewIntField("chunk", int64(i+1)),
				)
				totalSent.Add(int64(len(chunk)))
				mpe.stats.NewTaggedStat("partition_mig_jobexec_jobs_sent", stats.CountType, mpe.statsTags()).Count(len(chunk))
			}
		}
	})

	// receiver goroutine
	streamGroup.Go(func() error {
		mpe.logger.Infon("Starting to receive acks for migrated jobs")
		defer mpe.logger.Infon("Finished receiving acks for migrated jobs")
		for {
			select {
			case <-streamCtx.Done(): // context cancelled
				return streamCtx.Err()
			case batchToAck, ok := <-batchesToAck: // new unacked batch to ack
				if !ok { // channel is closed, just wait for an eof from the server
					mpe.logger.Debugn("Receiver waiting for stream to be closed by server")
					_, err = mpe.receiveWithTimeout(streamCtx, stream)
					if !errors.Is(err, io.EOF) {
						return fmt.Errorf("received unexpected error while waiting for server stream to end: %w", err)
					}
					mpe.logger.Debugn("Stream was closed by server")
					return nil
				}
				start := time.Now()
				// wait for the acknowledgement
				mpe.logger.Debugn("Waiting for ack from server",
					logger.NewIntField("batchIndex", int64(batchToAck.A)),
				)
				ack, err := mpe.receiveWithTimeout(streamCtx, stream)
				if err != nil {
					return fmt.Errorf("receiving ack from server: %w", err)
				}
				if ack.BatchIndex != int64(batchToAck.A) {
					return fmt.Errorf("received ack for batch %d, but expected ack for batch %d", ack.BatchIndex, batchToAck.A)
				}
				mpe.logger.Debugn("Received ack from server",
					logger.NewIntField("batchIndex", int64(batchToAck.A)),
				)
				// mark jobs as migrated
				mpe.logger.Debugn("Marking batch as migrated",
					logger.NewIntField("batchIndex", int64(batchToAck.A)),
				)
				if err := mpe.updateJobStatus(streamCtx, batchToAck.B, jobsdb.Migrated.State, nil); err != nil {
					return fmt.Errorf("marking batch %d as migrated: %w", batchToAck.A, err)
				}
				mpe.logger.Debugn("Marked batch as migrated",
					logger.NewIntField("batchIndex", int64(batchToAck.A)),
				)
				// remove from unacked batches
				unackedBatchesMu.Lock()
				delete(unackedBatches, batchToAck.A)
				unackedBatchesMu.Unlock()
				totalAcked.Add(int64(len(batchToAck.B)))
				mpe.stats.NewTaggedStat("partition_mig_jobexec_jobs_acked_time", stats.TimerType, mpe.statsTags()).SendTiming(time.Since(start))
				mpe.stats.NewTaggedStat("partition_mig_jobexec_jobs_acked", stats.CountType, mpe.statsTags()).Count(len(batchToAck.B))
			}
		}
	})

	g := &errgroup.Group{}
	done := make(chan struct{})
	// periodic logger
	g.Go(func() error {
		for {
			select {
			case <-done:
				return nil
			case <-time.After(mpe.progressPeriod.Load()):
				mpe.logger.Infon("Partition migration in progress",
					logger.NewIntField("sent", totalSent.Load()),
					logger.NewIntField("acked", totalAcked.Load()),
				)
			}
		}
	})
	// wait for sender and receiver to finish
	g.Go(func() error {
		defer close(done)
		if err := streamGroup.Wait(); err != nil {
			if ctx.Err() == nil {
				mpe.logger.Errorn("Partition migration failed", obskit.Error(err))
				for index, jobs := range unackedBatches { // no need to lock unackedBatches, we are done with sender and receiver goroutines
					if err := mpe.updateJobStatus(ctx, jobs, jobsdb.Failed.State, fmt.Errorf("job migration interrupted: %w", err)); err != nil {
						mpe.logger.Warnn("Could not mark non-migrated jobs as failed",
							logger.NewIntField("batchIndex", int64(index)),
							logger.NewStringField("tablePrefix", mpe.sourceDB.Identifier()),
							logger.NewStringField("migrationJobID", mpe.migrationJobID),
							logger.NewStringField("partitions", strings.Join(mpe.partitionIDs, ",")),
							obskit.Error(err),
						)
					}
				}
			}
			return fmt.Errorf("migrating partitions: %w", err)
		}
		mpe.logger.Infon("Partition migration completed successfully",
			logger.NewIntField("total", totalAcked.Load()),
		)
		return nil
	})
	err = g.Wait()
	mpe.logger.Infon("Partition migration progress final status",
		logger.NewIntField("sent", totalSent.Load()),
		logger.NewIntField("acked", totalAcked.Load()),
		obskit.Error(err),
	)
	return err
}

// markExecutingJobsAsFailed marks any executing jobs as failed to handle previous interrupted migrations
func (mpe *migrationJobExecutor) markExecutingJobsAsFailed(ctx context.Context) error {
	defer mpe.stats.NewTaggedStat("partition_mig_jobexec_fail_executing_time", stats.TimerType, mpe.statsTags()).RecordDuration()()
	var total int
	for done := false; !done; {
		executingJobsResult, err := mpe.sourceDB.GetJobs(ctx, []string{jobsdb.Executing.State}, jobsdb.GetQueryParams{
			PartitionFilters: mpe.partitionIDs,
			JobsLimit:        mpe.batchSize.Load(),
		})
		if err != nil {
			return fmt.Errorf("getting executing jobs: %w", err)
		}
		if len(executingJobsResult.Jobs) > 0 {
			if err := mpe.updateJobStatus(ctx, executingJobsResult.Jobs, jobsdb.Failed.State, fmt.Errorf("job migration interrupted")); err != nil {
				return fmt.Errorf("marking executing jobs as failed: %w", err)
			}
			total += len(executingJobsResult.Jobs)
		}
		// if there are no more executing jobs and DS limits are not reached, we are done
		done = len(executingJobsResult.Jobs) == 0 && !executingJobsResult.DSLimitsReached
	}
	if total > 0 {
		mpe.logger.Infon("Marked executing jobs as failed for partition migration",
			logger.NewIntField("count", int64(total)),
		)
	}
	return nil
}

// updateJobStatus updates the status of the given jobs to the given state
func (mpe *migrationJobExecutor) updateJobStatus(ctx context.Context, jobs []*jobsdb.JobT, state string, err error) error {
	if len(jobs) == 0 {
		return nil
	}
	errorCode := ""
	errorResponse := []byte("{}")
	switch state {
	case jobsdb.Succeeded.State:
		errorCode = "200"
	case jobsdb.Failed.State:
		errorCode = "500"
		if err != nil {
			errorResponse = []byte(fmt.Sprintf(`{"error": %q}`, err.Error()))
		}
	default:
	}
	now := time.Now()
	statusList := make([]*jobsdb.JobStatusT, len(jobs))
	for i, job := range jobs {
		params := job.LastJobStatus.Parameters
		if len(params) == 0 {
			params = []byte("{}")
		}
		statusList[i] = &jobsdb.JobStatusT{
			JobID:         job.JobID,
			JobState:      state,
			AttemptNum:    job.LastJobStatus.AttemptNum,
			ExecTime:      now,
			RetryTime:     now,
			ErrorCode:     errorCode,
			ErrorResponse: errorResponse,
			Parameters:    params,
			JobParameters: job.Parameters,
			WorkspaceId:   job.WorkspaceId,
			PartitionID:   job.PartitionID,
		}
	}
	customValFilters := []string{"migrated"} // TODO: this is fine for now, but we'll need adaptations as soon as we start worrying about pending events
	return mpe.sourceDB.UpdateJobStatus(ctx, statusList, customValFilters, nil)
}

// receiveWithTimeout wraps stream.Recv with a timeout and context cancellation support
func (mpe *migrationJobExecutor) receiveWithTimeout(ctx context.Context, stream proto.PartitionMigration_StreamJobsClient) (*proto.JobsBatchAck, error) {
	recvCh := make(chan lo.Tuple2[*proto.JobsBatchAck, error], 1)
	go func() { // known issue - goroutine leaks on timeout - acceptable.
		recvCh <- lo.T2(stream.Recv())
	}()
	select {
	case rec := <-recvCh:
		return rec.A, rec.B
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(mpe.receiveTimeout.Load()):
		return nil, status.Errorf(codes.DeadlineExceeded, "stream receive operation timed out after %s", mpe.receiveTimeout.Load())
	}
}

// sendWithTimeout wraps stream.Send with a timeout and context cancellation support
func (mpe *migrationJobExecutor) sendWithTimeout(ctx context.Context, stream proto.PartitionMigration_StreamJobsClient, request *proto.StreamJobsRequest) error {
	sendCh := make(chan error, 1)
	go func() {
		sendCh <- stream.Send(request)
	}()
	select {
	case err := <-sendCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(mpe.sendTimeout.Load()):
		return status.Errorf(codes.DeadlineExceeded, "stream send operation timed out after %s", mpe.sendTimeout.Load())
	}
}

func (mpe *migrationJobExecutor) statsTags() stats.Tags {
	return stats.Tags{
		"tablePrefix": mpe.sourceDB.Identifier(),
		"target":      mpe.target,
	}
}
