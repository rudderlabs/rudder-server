// GRPC service API implementation for partition migration server
package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/samber/lo"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rudderlabs/rudder-go-kit/config"
	kitctx "github.com/rudderlabs/rudder-go-kit/context"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/jobsdb"
	proto "github.com/rudderlabs/rudder-server/proto/cluster"
	"github.com/rudderlabs/rudder-server/utils/tx"
)

type Opt func(*partitionMigrationServer)

// WithLogger sets the logger for the PartitionMigrationServer.
func WithLogger(logger logger.Logger) Opt {
	return func(s *partitionMigrationServer) {
		s.logger = logger
	}
}

// WithStats sets the stats for the PartitionMigrationServer.
func WithStats(stats stats.Stats) Opt {
	return func(s *partitionMigrationServer) {
		s.stats = stats
	}
}

// WithDedupEnabled sets whether deduplication is enabled for the PartitionMigrationServer (enabled by default).
func WithDedupEnabled(dedupEnabled bool) Opt {
	return func(s *partitionMigrationServer) {
		s.dedupEnabled = dedupEnabled
	}
}

// WithStreamTimeout sets the timeout for stream receive/send operations (default: 5 minutes).
func WithStreamTimeout(timeout config.ValueLoader[time.Duration]) Opt {
	return func(s *partitionMigrationServer) {
		s.streamTimeout = timeout
	}
}

// NewPartitionMigrationServer creates a new PartitionMigrationServer instance.
func NewPartitionMigrationServer(ctx context.Context, jobsdbs []jobsdb.JobsDB, opts ...Opt) proto.PartitionMigrationServer {
	s := &partitionMigrationServer{
		lifecycleCtx:     ctx,
		activeMigrations: make(map[string]struct{}),
		jobsdbs:          lo.SliceToMap(jobsdbs, func(db jobsdb.JobsDB) (string, jobsdb.JobsDB) { return db.Identifier(), db }),
		dedupEnabled:     true,
		streamTimeout:    config.SingleValueLoader(10 * time.Minute),
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.logger == nil {
		s.logger = logger.NewLogger().Child("partitionmigration")
	}
	if s.stats == nil {
		s.stats = stats.Default
	}
	return s
}

type partitionMigrationServer struct {
	proto.UnimplementedPartitionMigrationServer
	lifecycleCtx  context.Context
	logger        logger.Logger
	stats         stats.Stats
	jobsdbs       map[string]jobsdb.JobsDB          // map of table prefix to jobsdb instance
	dedupEnabled  bool                              // whether deduplication is enabled when receiving jobs
	streamTimeout config.ValueLoader[time.Duration] // timeout for stream receive/send operations

	activeMigrationsMu sync.Mutex          // protects activeMigrations
	activeMigrations   map[string]struct{} // set of active migration keys (migrationJobID_tablePrefix)
}

// StreamJobs handles the streaming of jobs from the client for migration
// to the target jobsdb:
//
//   - The client first sends a metadata message, followed by multiple job chunk messages.
//   - The server receives the job chunks, reconstructs the job batches, and stores them in the target jobsdb.
//   - It acknowledges each batch back to the client after successful storage.
//   - While a batch is being stored, the server continues to receive subsequent chunks to maximize throughput.
//   - At most 2 batches are kept in memory at any time to control memory usage.
//   - Deduplication is performed based on job IDs to avoid storing duplicate jobs.
func (s *partitionMigrationServer) StreamJobs(stream proto.PartitionMigration_StreamJobsServer) (streamErr error) {
	streamErr = func() error {
		s.logger.Infon("Opening new streaming session")
		// merge the stream context with the server lifecycle context
		ctx, cancel := kitctx.MergedContext(stream.Context(), s.lifecycleCtx)
		defer cancel()

		// receive the first message containing metadata
		r, err := s.receiveWithTimeout(ctx, stream)
		if err != nil {
			s.logger.Errorn("Error receiving stream metadata from client", obskit.Error(err))
			return fmt.Errorf("receiving stream metadata: %w", err)
		}
		s.logger.Infon("Received first message from client")
		metadata := r.GetMetadata()
		if metadata == nil {
			s.logger.Errorn("First message from client did not contain metadata")
			return status.New(codes.FailedPrecondition, "first message must be metadata").Err()
		}
		log := s.logger.Withn(
			logger.NewStringField("migrationJobID", metadata.MigrationJobId),
			logger.NewStringField("tablePrefix", metadata.TablePrefix),
			logger.NewStringField("partitions", strings.Join(metadata.PartitionIds, ",")),
		)
		statTags := stats.Tags{
			"tablePrefix": metadata.TablePrefix,
		}
		log.Infon("Starting job stream for partition migration")
		defer log.Infon("Ending job stream for partition migration")
		defer s.stats.NewTaggedStat("partition_mig_server_stream_time", stats.TimerType, statTags).RecordDuration()()

		// resolve the jobsdb for the given table prefix
		db := s.jobsdbs[metadata.TablePrefix]
		if db == nil {
			s.logger.Errorn("No jobsdb found for the given table prefix")
			return status.New(codes.FailedPrecondition, "no jobsdb found for the given table prefix").Err()
		}

		// construct the migration key, which is a combination of migration job ID and table prefix
		migrationKey := metadata.MigrationJobId + "_" + metadata.TablePrefix

		// only allow one active migration per migration key
		s.activeMigrationsMu.Lock()
		if _, exists := s.activeMigrations[migrationKey]; exists {
			s.activeMigrationsMu.Unlock()
			log.Errorn("Detected attempt to start a migration that is already in progress")
			return status.New(codes.Aborted, "a migration for the same migration job id and table prefix is already in progress").Err()
		}
		s.activeMigrations[migrationKey] = struct{}{}
		s.activeMigrationsMu.Unlock()
		defer func() { // clean up active migration entry
			s.activeMigrationsMu.Lock()
			delete(s.activeMigrations, migrationKey)
			s.activeMigrationsMu.Unlock()
		}()

		// Deduplication, if enabled, will keep deduplicating jobs until it finds a job with JobID > lastJobID.
		// This allows us to resume interrupted migrations without re-storing already stored jobs.
		// Once we find a job with JobID > lastJobID, we disable deduplication for the rest of the migration,
		// as we assume that the client will not resend any previously sent jobs after that point.
		dedupActive := s.dedupEnabled
		var lastJobID int64
		if s.dedupEnabled {
			// Load lastJobID from persistent storage to resume interrupted migrations
			if err := db.WithTx(func(tx *tx.Tx) error {
				return tx.QueryRowContext(ctx, "SELECT COALESCE((SELECT last_job_id FROM partition_migration_dedup WHERE key = $1), -1)", migrationKey).Scan(&lastJobID)
			}); err != nil {
				log.Errorn("Failed to load lastJobID for migration", obskit.Error(err))
				return fmt.Errorf("loading lastJobID for migration %s: %w", migrationKey, err)
			}
			log.Infon("Loaded lastJobID for deduplication", logger.NewIntField("lastJobID", lastJobID))
		}

		batches := make(chan streamingBatch) // unbuffered channel for backpressure

		// goroutine for storing complete batches and sending acknowledgments
		storeErr := make(chan error, 1)
		getStoreErr := func() error { // helper to get the store error if any
			select {
			case err := <-storeErr:
				return err
			default:
				return nil
			}
		}

		var storeGroup sync.WaitGroup
		storeGroup.Go(func() {
			log.Infon("Starting store and ack goroutine")
			defer log.Infon("Stopped store and ack goroutine")
			for batch := range batches {
				log.Debugn("Received batch", logger.NewIntField("batchIndex", batch.index))
				if len(batch.jobs) > 0 { // store only non-empty batches (we may have empty batches after deduplication)
					log.Debugn("Storing batch", logger.NewIntField("batchIndex", batch.index))
					newLastJobID := batch.jobs[len(batch.jobs)-1].JobID
					// store the batch
					if err := db.WithStoreSafeTx(ctx, func(tx jobsdb.StoreSafeTx) error {
						// store jobs
						if err := db.StoreInTx(ctx, tx, batch.jobs); err != nil {
							return fmt.Errorf("storing jobs in %s jobsdb: %w", metadata.TablePrefix, err)
						}
						if s.dedupEnabled {
							// update lastJobID for deduplication
							if _, err := tx.SqlTx().ExecContext(ctx,
								"INSERT INTO partition_migration_dedup (key, last_job_id) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET last_job_id = EXCLUDED.last_job_id;",
								migrationKey,
								newLastJobID,
							); err != nil {
								return fmt.Errorf("updating last_job_id for migration %s: %w", migrationKey, err)
							}
						}
						return nil
					}); err != nil {
						storeErr <- fmt.Errorf("storing jobs batch %d: %w", batch.index, err)
						close(storeErr)
						cancel()
						return
					}
				}
				// acknowledge the batch
				log.Debugn("Acknowledging batch", logger.NewIntField("batchIndex", batch.index))
				if err := s.sendWithTimeout(ctx, stream, &proto.JobsBatchAck{BatchIndex: batch.index}); err != nil {
					log.Debugn("Error acknowledging batch", logger.NewIntField("batchIndex", batch.index), obskit.Error(err))
					storeErr <- fmt.Errorf("sending ack for jobs batch %d: %w", batch.index, err)
					close(storeErr)
					cancel()
					return
				}
				s.stats.NewTaggedStat("partition_mig_server_ack_jobs", stats.CountType, statTags).Count(len(batch.jobs))
			}
			// done with for loop, close storeErr to signal completion
			close(storeErr)
			// if context is not cancelled (i.e. successful completion), cleanup dedup state
			if ctx.Err() == nil && s.dedupEnabled {
				log.Infon("Deleting deduplication state for completed migration")
				if err := db.WithTx(func(tx *tx.Tx) error {
					if _, err := tx.ExecContext(ctx, "DELETE FROM partition_migration_dedup WHERE key = $1", migrationKey); err != nil {
						return fmt.Errorf("deleting deduplication state for migration %s: %w", migrationKey, err)
					}
					return nil
				}); err != nil {
					s.logger.Warnn("Failed to delete deduplication state for completed migration",
						obskit.Error(err),
					)
				}
			}
		})

		// helper to stop the store goroutine, wait for it to finish and return any errors
		stopStoreAndReturnError := func(err error) error {
			// figure out the cause (either the passed error or the store error),
			// we give priority to any store error that may have occurred before the passed error
			var cause error
			cause = getStoreErr()
			if cause == nil {
				cause = err
			}

			// cancel the context in case of an error to abort ongoing operations
			if err != nil {
				cancel()
			}

			// close the batches channel to stop the storing goroutine
			close(batches)
			log.Infon("Waiting for store goroutine to finish")
			// wait for the store goroutine to finish
			storeGroup.Wait()

			// return the cause error if any
			if cause != nil {
				return cause
			}
			// even if there was no cause error, after we closed the channel, store might have failed, thus we need to check again
			// no need to lock again as the goroutine has finished
			return getStoreErr()
		}

		// process incoming stream, collect job chunks into batches and send them to the store goroutine
		log.Infon("Starting receiving job chunks from client")
		var batch streamingBatch
		err = func() error {
			for {
				r, err := s.receiveWithTimeout(ctx, stream)

				if errors.Is(err, io.EOF) { // client has finished sending
					log.Infon("Client has finished sending job chunks")
					return nil
				}
				if err != nil { // any other error should be considered fatal
					return fmt.Errorf("receiving from client stream: %w", err)
				}

				chunk := r.GetChunk()
				if chunk == nil {
					log.Warnn("Client sent empty job chunk")
					return status.New(codes.FailedPrecondition, "received message without job batch").Err()
				}
				if len(batch.jobs) == 0 { // first chunk of a new batch
					batch.index = chunk.BatchIndex
				}
				jobs, err := chunk.GetJobsdbJobs()
				if err != nil {
					return fmt.Errorf("converting to jobsdb jobs: %w", err)
				}

				// deduplicate jobs if needed, i.e. ignore jobs with JobID <= lastJobID
				if dedupActive {
					jobs = lo.Filter(jobs, func(j *jobsdb.JobT, _ int) bool {
						return j.JobID > lastJobID
					})
					dedupedJobsCount := len(chunk.Jobs) - len(jobs)
					if dedupedJobsCount > 0 {
						s.logger.Infon("Deduplicated jobs",
							logger.NewIntField("dedupCount", int64(dedupedJobsCount)),
						)
					}
					if len(jobs) > 0 { // disable dedup as soon as we find at least one job in the batch that is not a duplicate
						dedupActive = false
					}
					s.stats.NewTaggedStat("partition_mig_server_dedup_jobs", stats.CountType, statTags).Count(dedupedJobsCount)
				}
				batch.jobs = append(batch.jobs, jobs...)
				if chunk.LastChunk {
					select {
					case batches <- batch:
						log.Debugn("Sent batch for storing",
							logger.NewIntField("batchIndex", batch.index),
							logger.NewIntField("jobCount", int64(len(batch.jobs))),
						)
						// batch sent for storing
						batch = streamingBatch{}
					case <-ctx.Done(): // context cancelled
						return fmt.Errorf("context done: %w", ctx.Err())
					}
				}
			}
		}()
		err = stopStoreAndReturnError(err)
		if err != nil {
			log.Errorn("Error processing job chunks from client", obskit.Error(err))
			return err
		}
		log.Infon("Finished processing job chunks from client")
		return nil
	}()
	if streamErr != nil {
		s.logger.Errorn("StreamJobs ended with error", obskit.Error(streamErr))
	}
	return streamErr
}

// streamingBatch represents a batch of jobs received from the client for migration
type streamingBatch struct {
	index int64          // index of the batch, used for acknowledgments
	jobs  []*jobsdb.JobT // jobs contained in the batch
}

// receiveWithTimeout wraps stream.Recv with a timeout and context cancellation support
func (s *partitionMigrationServer) receiveWithTimeout(ctx context.Context, stream proto.PartitionMigration_StreamJobsServer) (*proto.StreamJobsRequest, error) {
	recvCh := make(chan lo.Tuple2[*proto.StreamJobsRequest, error], 1)
	go func() { // known issue - goroutine leaks on timeout - acceptable.
		recvCh <- lo.T2(stream.Recv())
	}()
	select {
	case rec := <-recvCh:
		return rec.A, rec.B
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(s.streamTimeout.Load()):
		return nil, status.Errorf(codes.DeadlineExceeded, "stream receive operation timed out after %s", s.streamTimeout.Load())
	}
}

// sendWithTimeout wraps stream.Send with a timeout and context cancellation support
func (s *partitionMigrationServer) sendWithTimeout(ctx context.Context, stream proto.PartitionMigration_StreamJobsServer, ack *proto.JobsBatchAck) error {
	sendCh := make(chan error, 1)
	go func() {
		sendCh <- stream.Send(ack)
	}()
	select {
	case err := <-sendCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(s.streamTimeout.Load()):
		return status.Errorf(codes.DeadlineExceeded, "stream send operation timed out after %s", s.streamTimeout.Load())
	}
}
