package partitionbuffer

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// The same integration test runs for 2 modes,
//
//	(a) readwrite buffer and
//	(b) read-only with write-only buffer.
//
// Each scenario runs a sequence storing data for multiple different customVal values and partitions, buffering and flushing partitions using a short delay between them to allow some overlap.
// Each sequence runs the following steps:
//
//  1. For each partition and customVal, start one goroutine that stores data in writer jobsdb with the customVal & partition assigned at a given rate, e.g. 100 items/sec. Each job has an incrementing sequence number.
//
//  2. For each customVal, start one goroutine that reads data from the reader jobsdb and marks them as succeeded. It also verifies that jobs are being read in order.
//
//  3. After a short delay (e.g. 5 seconds plus some jitter), another set of goroutines, one for each partition will:
//     - Mark the partition as buffered in the reader jobsdb & triggers a refresh in the writer jobsdb.
//     - Wait another 10 seconds + jitter and then call flush on the reader buffer for the given partition.
//     - Wait another 10 seconds + jitter and then trigger the store goroutine to stop.
//
//  4. The store goroutines mark the last sequence number in the donePartitions map.
//
//  5. The read goroutines check the donePartitions map to know when to stop reading, i.e. when all store goroutines have finished.
//
// Test configuration constants - adjust these to modify test behavior
const (
	itBatchSize             = 300              // jobs per batch for store/read operations
	itFlushMoveTimeout      = 10 * time.Second // timeout for move operations during flush
	itTotalPartitionsNum    = 64               // total partitions in the test setup
	itBufferedPartitionsNum = 5                // number of partitions to buffer (must be < totalPartitionsNum)
	itBufferedDelay         = 5 * time.Second  // delay before marking partition as buffered
	itFlushDelay            = 10 * time.Second // delay before flushing buffer
	itEndDelay              = 5 * time.Second  // delay before stopping store goroutines
	itTimeout               = 10 * time.Minute // overall test timeout
)

func TestPartitionBufferIntegrationTest(t *testing.T) {
	m := testPartitionBufferIntegrationTestMethods{
		batchSize:             itBatchSize,
		flushMoveTimeout:      itFlushMoveTimeout,
		totalPartitionsNum:    itTotalPartitionsNum,
		bufferedPartitionsNum: itBufferedPartitionsNum,
		bufferedDelay:         itBufferedDelay,
		flushDelay:            itFlushDelay,
		endDelay:              itEndDelay,
	}

	t.Logf("Running integration test with config: batchSize=%d, totalPartitions=%d, bufferedPartitions=%d",
		m.batchSize, m.totalPartitionsNum, m.bufferedPartitionsNum)

	t.Run("readwrite", func(t *testing.T) {
		t.Logf("Testing readwrite mode: single partition buffer instance handles both reading and writing")
		reader, writer := m.ReadWriteSetup(t)
		m.RunScenario(t, reader, writer)
	})

	t.Run("readonly and writeonly buffers", func(t *testing.T) {
		t.Logf("Testing readonly with write buffer mode: separate instances for reading and writing")
		reader, writer := m.ReadOnlyWithWriteBufferSetup(t)
		m.RunScenario(t, reader, writer)
	})
}

type testPartitionBufferIntegrationTestMethods struct {
	batchSize             int           // number of records to store/read/flush at a time
	flushMoveTimeout      time.Duration // timeout for move operation, before forcing switchover
	totalPartitionsNum    int           // number of partitions used in the test
	bufferedPartitionsNum int           // number of partitions to be buffered
	bufferedDelay         time.Duration // time to wait before marking partition as buffered
	flushDelay            time.Duration // time to wait before flushing the buffer after marking the partition as buffered
	endDelay              time.Duration // time to wait before stopping the store goroutine, after flush has completed
}

// ReadWriteSetup sets up a JobsDBPartitionBuffer instance with readwrite capabilities for both reading and writing.
func (m testPartitionBufferIntegrationTestMethods) ReadWriteSetup(t *testing.T) (reader, writer JobsDBPartitionBuffer) {
	t.Helper()
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pg, err := postgres.Setup(pool, t)
	require.NoError(t, err)
	runNodeMigration(t, pg.DB)

	c := config.New()
	c.Set("JobsDB.maxDSSize", m.batchSize)
	c.Set("JobsDB.addNewDSLoopSleepDuration", "1s")

	primary := jobsdb.NewForReadWrite("primary",
		jobsdb.WithDBHandle(pg.DB),
		jobsdb.WithNumPartitions(m.totalPartitionsNum),
		jobsdb.WithSkipMaintenanceErr(true),
		jobsdb.WithDSLimit(config.SingleValueLoader(2)),
		jobsdb.WithConfig(c),
	)
	require.NoError(t, primary.Start(), "it should be able to start JobsDB")
	t.Cleanup(func() {
		primary.Stop()
	})
	buffer := jobsdb.NewForReadWrite("buf",
		jobsdb.WithDBHandle(pg.DB),
		jobsdb.WithNumPartitions(m.totalPartitionsNum),
		jobsdb.WithSkipMaintenanceErr(true),
		jobsdb.WithDSLimit(config.SingleValueLoader(10)),
		jobsdb.WithConfig(c),
	)
	require.NoError(t, buffer.Start(), "it should be able to start buffer JobsDB")
	t.Cleanup(func() {
		buffer.Stop()
	})

	pb, err := NewJobsDBPartitionBuffer(t.Context(),
		WithReadWriteJobsDBs(primary, buffer),
		WithNumPartitions(m.totalPartitionsNum),
		WithFlushBatchSize(config.SingleValueLoader(m.batchSize*10)),
		WithFlushMoveTimeout(config.SingleValueLoader(m.flushMoveTimeout)),
	)
	require.NoError(t, err)
	return pb, pb
}

// ReadOnlyWithWriteBufferSetup sets up a JobsDBPartitionBuffer instance with read-only capabilities for reading and write-only capabilities for writing.
func (m testPartitionBufferIntegrationTestMethods) ReadOnlyWithWriteBufferSetup(t *testing.T) (reader, writer JobsDBPartitionBuffer) {
	t.Helper()
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pg, err := postgres.Setup(pool, t)
	require.NoError(t, err)
	runNodeMigration(t, pg.DB)

	c := config.New()
	c.Set("JobsDB.maxDSSize", m.batchSize)
	c.Set("JobsDB.addNewDSLoopSleepDuration", "1s")

	// We need two writer jobsdb instances, one for the writer partition buffer and another one for the reader partition buffer to be able to flush.
	primaryWO1 := jobsdb.NewForWrite("primary",
		jobsdb.WithDBHandle(pg.DB),
		jobsdb.WithNumPartitions(m.totalPartitionsNum),
		jobsdb.WithSkipMaintenanceErr(true),
		jobsdb.WithDSLimit(config.SingleValueLoader(2)),
		jobsdb.WithConfig(c),
	)
	require.NoError(t, primaryWO1.Start(), "it should be able to start JobsDB")
	t.Cleanup(func() {
		primaryWO1.Stop()
	})
	primaryWO2 := jobsdb.NewForWrite("primary",
		jobsdb.WithDBHandle(pg.DB),
		jobsdb.WithNumPartitions(m.totalPartitionsNum),
		jobsdb.WithSkipMaintenanceErr(true),
		jobsdb.WithDSLimit(config.SingleValueLoader(2)),
		jobsdb.WithConfig(c),
	)
	require.NoError(t, primaryWO2.Start(), "it should be able to start JobsDB")
	t.Cleanup(func() {
		primaryWO2.Stop()
	})

	primaryRO := jobsdb.NewForRead("primary",
		jobsdb.WithDBHandle(pg.DB),
		jobsdb.WithNumPartitions(m.totalPartitionsNum),
		jobsdb.WithSkipMaintenanceErr(true),
		jobsdb.WithDSLimit(config.SingleValueLoader(2)),
		jobsdb.WithConfig(c),
	)
	require.NoError(t, primaryRO.Start(), "it should be able to start JobsDB")
	t.Cleanup(func() {
		primaryRO.Stop()
	})

	bufferWO := jobsdb.NewForWrite("buf",
		jobsdb.WithDBHandle(pg.DB),
		jobsdb.WithNumPartitions(m.totalPartitionsNum),
		jobsdb.WithSkipMaintenanceErr(true),
		jobsdb.WithDSLimit(config.SingleValueLoader(10)),
		jobsdb.WithConfig(c),
	)
	require.NoError(t, bufferWO.Start(), "it should be able to start buffer JobsDB")
	t.Cleanup(func() {
		bufferWO.Stop()
	})

	bufferRO := jobsdb.NewForRead("buf",
		jobsdb.WithDBHandle(pg.DB),
		jobsdb.WithNumPartitions(m.totalPartitionsNum),
		jobsdb.WithSkipMaintenanceErr(true),
		jobsdb.WithDSLimit(config.SingleValueLoader(10)),
		jobsdb.WithConfig(c),
	)
	require.NoError(t, bufferRO.Start(), "it should be able to start buffer JobsDB")
	t.Cleanup(func() {
		bufferRO.Stop()
	})

	pbW, err := NewJobsDBPartitionBuffer(t.Context(),
		WithWriterOnlyJobsDBs(primaryWO1, bufferWO),
	)
	require.NoError(t, err)

	pbR, err := NewJobsDBPartitionBuffer(t.Context(),
		WithReaderOnlyAndFlushJobsDBs(primaryRO, bufferRO, primaryWO2),
		WithNumPartitions(m.totalPartitionsNum),
		WithFlushBatchSize(config.SingleValueLoader(m.batchSize*10)),
		WithFlushMoveTimeout(config.SingleValueLoader(m.flushMoveTimeout)),
	)
	require.NoError(t, err)
	return pbR, pbW
}

// RunScenario runs the partition buffer integration test scenario with the provided reader & writer JobsDBPartitionBuffer instances.
// It runs a sequence storing data for multiple different customVal values and partitions, buffering and flushing partitions using a short delay between them to allow some overlap.
func (m testPartitionBufferIntegrationTestMethods) RunScenario(t *testing.T, reader, writer JobsDBPartitionBuffer) {
	t.Helper()
	start := time.Now()
	ctx, cancel := context.WithTimeout(t.Context(), itTimeout)
	defer cancel()
	wg, ctx := errgroup.WithContext(ctx)

	// Run sequence for multiple customVal values with some delay between them to allow overlap
	m.RunSequence(t, ctx, wg, reader, writer, []string{"cv1", "cv2", "cv3"})

	err := wg.Wait()
	t.Logf("All sequences completed in %v", time.Since(start))
	require.NoError(t, err, "integration test scenario should complete without errors")
}

// RunSequence runs the partition buffer integration test sequence:
//
//  1. For each partition and customVal, start one goroutine that stores data in writer jobsdb with the customVal & partition assigned at a given rate, e.g. 100 items/sec. Each job has an incrementing sequence number.
//
//  2. For each customVal, start one goroutine that reads data from the reader jobsdb and marks them as succeeded. It also verifies that jobs are being read in order.
//
//  3. After a short delay (e.g. 5 seconds plus some jitter), another set of goroutines, one for each partition will:
//     - Mark the partition as buffered in the reader jobsdb & triggers a refresh in the writer jobsdb.
//     - Wait another 10 seconds + jitter and then call flush on the reader buffer for the given partition.
//     - Wait another 10 seconds + jitter and then trigger the store goroutine to stop.
//
//  4. The store goroutines mark the last sequence number in the donePartitions map.
//
//  5. The read goroutines check the donePartitions map to know when to stop reading, i.e. when all store goroutines have finished.
func (m testPartitionBufferIntegrationTestMethods) RunSequence(t *testing.T, ctx context.Context, wg *errgroup.Group, reader, writer JobsDBPartitionBuffer, customVals []string) {
	t.Helper()
	wg.Go(func() error {
		seqG, ctx := errgroup.WithContext(ctx)

		partitionIDFunc := func(i int) string {
			return "p-" + strconv.Itoa(i)
		}
		var donePartitionsMu sync.RWMutex
		donePartitions := map[string]map[string]int{}
		for _, customVal := range customVals {
			donePartitions[customVal] = map[string]int{}
		}
		stopStore := make(map[string]chan struct{}) // signal to stop the store goroutine
		for i := range m.bufferedPartitionsNum {
			stopStore[partitionIDFunc(i)] = make(chan struct{})
		}

		for _, customVal := range customVals {
			for i := range m.bufferedPartitionsNum {
				partitionID := partitionIDFunc(i)
				stop := stopStore[partitionID]

				// Store goroutine (one per partition and customVal)
				seqG.Go(func() error {
					if err := misc.SleepCtx(ctx, time.Duration(5*i)*time.Millisecond); err != nil {
						return fmt.Errorf("sleeping before starting store goroutine for %q (%s): %w", partitionID, customVal, err)
					}
					sequence := 0
					totalJobsStored := 0
					sendJobs := func() (time.Duration, error) {
						start := time.Now()
						var jobs []*jobsdb.JobT
						for i := 0; i < m.batchSize; i++ {
							sequence++
							jobs = append(jobs, &jobsdb.JobT{
								UUID:         uuid.New(),
								UserID:       "user-1",
								CreatedAt:    start,
								ExpireAt:     start,
								CustomVal:    customVal,
								EventCount:   1,
								EventPayload: []byte(`{"sequence":` + strconv.Itoa(sequence) + `}`),
								Parameters:   []byte(`{}`),
								WorkspaceId:  "workspace-1",
								PartitionID:  partitionID,
							})
						}
						ctx, cancel := context.WithTimeout(ctx, time.Minute)
						defer cancel()
						err := writer.Store(ctx, jobs)
						if err != nil {
							return 0, fmt.Errorf("storing batch for %q (%s), sequence range %d-%d: %w",
								partitionID, customVal, sequence-m.batchSize+1, sequence, err)
						}
						totalJobsStored += len(jobs)
						return time.Since(start), nil
					}
					sleepFor := time.Second
					for {
						select {
						case <-stop: // stop storing, notify about the last sequence number and exit
							t.Logf("Store goroutine for %q (%s) finished: stored %d jobs, final sequence: %d",
								partitionID, customVal, totalJobsStored, sequence)
							donePartitionsMu.Lock()
							donePartitions[customVal][partitionID] = sequence
							donePartitionsMu.Unlock()
							return nil
						case <-time.After(sleepFor):
							elapsed, err := sendJobs()
							if err != nil {
								return err
							}
							t.Logf("Stored jobs for %q (%s), sequence range %d-%d", partitionID, customVal, sequence-m.batchSize+1, sequence)
							sleepFor = time.Second - elapsed
						case <-ctx.Done():
							return nil
						}
					}
				})
			}

			seqG.Go(func() error { // Read goroutine (one per customVal)
				partitionOffsets := map[string]int{}
				history := map[string][]int{}
				for i := range m.bufferedPartitionsNum {
					partitionID := partitionIDFunc(i)
					partitionOffsets[partitionID] = 0
				}
				totalJobsProcessed := 0
				lastLogTime := time.Now()
				for {
					select {
					case <-ctx.Done():
						return fmt.Errorf("context canceled before verifying all jobs for custom val %s (processed %d jobs)",
							customVal, totalJobsProcessed)
					default:
						now := time.Now()
						getCtx, cancel := context.WithTimeout(ctx, 1*time.Minute)
						jobs, err := reader.GetUnprocessed(getCtx, jobsdb.GetQueryParams{CustomValFilters: []string{customVal}, JobsLimit: m.batchSize * 10})
						cancel()
						if err != nil {
							return fmt.Errorf("getting unprocessed jobs for %s: %w", customVal, err)
						}
						var statusList []*jobsdb.JobStatusT
						currentBatch := map[string][]int{}
						for _, job := range jobs.Jobs {
							statusList = append(statusList, &jobsdb.JobStatusT{
								JobID:         job.JobID,
								JobState:      jobsdb.Succeeded.State,
								AttemptNum:    1,
								ExecTime:      now,
								RetryTime:     now,
								ErrorCode:     "200",
								ErrorResponse: []byte("{}"),
								Parameters:    []byte("{}"),
								JobParameters: job.Parameters,
								WorkspaceId:   job.WorkspaceId,
								PartitionID:   job.PartitionID,
							})
							seqNum := gjson.GetBytes(job.EventPayload, "sequence").Int()
							history[job.PartitionID] = append(history[job.PartitionID], int(seqNum))
							currentBatch[job.PartitionID] = append(currentBatch[job.PartitionID], int(seqNum))
							if int(seqNum) != partitionOffsets[job.PartitionID]+1 {
								return fmt.Errorf("out of order job received for %q (%s), last: %d, current: %d, batch: %v, history: %v", job.PartitionID, customVal, partitionOffsets[job.PartitionID], seqNum, currentBatch[job.PartitionID], history[job.PartitionID])
							}
							partitionOffsets[job.PartitionID] = int(seqNum)
						}
						if len(statusList) > 0 {
							updateCtx, cancel := context.WithTimeout(ctx, 1*time.Minute)
							err := reader.UpdateJobStatus(updateCtx, statusList, []string{customVal}, nil)
							cancel()
							if err != nil {
								return fmt.Errorf("updating job statuses for custom val %s: %w", customVal, err)
							}
							totalJobsProcessed += len(statusList)
						}

						// check if we have reached the end condition
						readDone := true
						for partitionID, offset := range partitionOffsets {
							donePartitionsMu.RLock()
							endSequence, done := donePartitions[customVal][partitionID]
							donePartitionsMu.RUnlock()
							if !done || offset != endSequence {
								if time.Since(lastLogTime) > 5*time.Second {
									t.Logf("Partition %q (%s) not done yet: current offset %d, end sequence %d (done=%v)", partitionID, customVal, offset, endSequence, done)
								}
								readDone = false
								break
							}
						}
						// Log progress every 5 seconds
						if time.Since(lastLogTime) > 5*time.Second {
							t.Logf("Read progress for %q: processed %d jobs, partition offsets: %v", customVal, totalJobsProcessed, partitionOffsets)
							lastLogTime = time.Now()
						}
						if readDone {
							t.Logf("Read goroutine for %q completed: processed %d jobs total", customVal, totalJobsProcessed)
							return nil
						}
					}
				}
			})
		}

		// Control goroutines (one per partition)
		for i := range m.bufferedPartitionsNum {
			partitionID := partitionIDFunc(i)
			stop := stopStore[partitionID]
			seqG.Go(func() error {
				if err := misc.SleepCtx(ctx, m.bufferedDelay+time.Duration(i)*100*time.Millisecond); err != nil {
					return ctx.Err()
				}
				// Mark partition as buffered
				t.Logf("Marking %q as buffered", partitionID)
				if err := reader.BufferPartitions(ctx, []string{partitionID}); err != nil {
					return fmt.Errorf("marking partition %q as buffered: %w", partitionID, err)
				}
				// Refresh the writer jobsdb
				t.Logf("Refreshing buffered partitions for %q", partitionID)
				if err := writer.RefreshBufferedPartitions(ctx); err != nil {
					return fmt.Errorf("refreshing buffered partitions for partition %q: %w", partitionID, err)
				}
				// Wait before flushing
				t.Logf("Waiting before flushing for %q", partitionID)
				if err := misc.SleepCtx(ctx, m.flushDelay+time.Duration(i)*200*time.Millisecond); err != nil {
					return ctx.Err()
				}
				// Flush the buffer
				t.Logf("Flushing buffer for %q", partitionID)
				flushCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
				if err := reader.FlushBufferedPartitions(flushCtx, []string{partitionID}); err != nil {
					cancel()
					return fmt.Errorf("flushing buffer for %q: %w", partitionID, err)
				}
				cancel()
				// Wait before stopping the store goroutine
				t.Logf("Waiting before stopping the store goroutines for %q", partitionID)
				if err := misc.SleepCtx(ctx, m.endDelay+time.Duration(i)*100*time.Millisecond); err != nil {
					return ctx.Err()
				}

				// Stop the store goroutine
				t.Logf("Stopping store goroutines for %q", partitionID)
				close(stop)
				return nil
			})
		}

		return seqG.Wait()
	})
}
