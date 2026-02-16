package sourcenode

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/etcdwatcher"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	etcdtypes "github.com/rudderlabs/rudder-schemas/go/cluster"
	"github.com/rudderlabs/rudder-server/cluster/migrator/etcdclient"
	"github.com/rudderlabs/rudder-server/cluster/migrator/etcdkeys"
	"github.com/rudderlabs/rudder-server/cluster/migrator/partitionmigration/client"
	"github.com/rudderlabs/rudder-server/cluster/migrator/retry"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// Migrator defines the interface for a source node migrator
type Migrator interface {
	// Handle prepares the source node for starting a new partition migration
	Handle(ctx context.Context, migration *etcdtypes.PartitionMigration) error

	// Run watches for new migration jobs assigned to this source node
	Run(ctx context.Context, wg *errgroup.Group) error
}

type migrator struct {
	nodeIndex int
	nodeName  string

	etcdClient        etcdclient.Client
	readerJobsDBs     []jobsdb.JobsDB // reader jobsdbs for this source node
	config            *config.Config
	logger            logger.Logger
	stats             stats.Stats
	shutdown          func() // function to trigger a shutdown of the node
	targetURLProvider func(targetNodeIndex int) (string, error)

	c struct {
		readExcludeSleep         *config.Reloadable[time.Duration] // duration to wait after marking partitions as read-excluded
		waitForInProgressTimeout *config.Reloadable[time.Duration] // timeout for waiting for in-progress jobs to complete
		inProgressPollSleep      *config.Reloadable[time.Duration] // sleep duration between checks for in-progress jobs
	}

	// state
	pendingMigrationJobsMu sync.Mutex
	pendingMigrationJobs   map[string]struct{}
}

// Handle prepares the source node for starting a new partition migration
// by marking the source partitions as read-excluded and waiting for in-progress jobs to complete
// on those partitions.
//
// If there are no source partitions assigned to this node, it returns immediately.
// Otherwise, it performs the following steps:
//  1. Marks the source partitions as read-excluded in all jobsdbs.
//  2. Waits for a configured sleep duration to allow in-flight queries to complete.
//  3. Waits until there are no in-progress job statuses in all jobsdbs for the source partitions.
//
// If any step above fails, it returns an error. If the wait for in-progress jobs times out, it triggers a shutdown of the node before returning an error.
func (m *migrator) Handle(ctx context.Context, migration *etcdtypes.PartitionMigration) error {
	defer m.stats.NewTaggedStat("partition_mig_src_handle", stats.TimerType, m.statsTags()).RecordDuration()()

	sourcePartitions := getSourcePartitions(migration, m.nodeIndex)
	if len(sourcePartitions) == 0 {
		return nil // no partitions to handle
	}
	if err := m.addReadExcludedPartitions(ctx, sourcePartitions); err != nil {
		return fmt.Errorf("adding read excluded partitions: %w", err)
	}
	m.logger.Infon("Marked source partitions as read-excluded",
		logger.NewStringField("partitions", strings.Join(sourcePartitions, ",")),
	)
	// wait for sleep time so that in-flight queries can complete
	if err := misc.SleepCtx(ctx, m.c.readExcludeSleep.Load()); err != nil {
		return fmt.Errorf("waiting for read exclude sleep: %w", err)
	}
	// wait until there are no in-progress job statuses in all jobsdbs for the source partitions
	return m.waitForNoInProgressJobs(ctx, sourcePartitions)
}

// addReadExcludedPartitions marks the given partitions as excluded from reading in all jobsdbs
func (m *migrator) addReadExcludedPartitions(ctx context.Context, sourcePartitions []string) error {
	g, ctx := errgroup.WithContext(ctx)
	for _, readerJobsDB := range m.readerJobsDBs {
		g.Go(func() error {
			if err := readerJobsDB.AddReadExcludedPartitionIDs(ctx, sourcePartitions); err != nil {
				return fmt.Errorf("adding read excluded partitions to %q jobsdb: %w", readerJobsDB.Identifier(), err)
			}
			return nil
		})
	}
	return g.Wait()
}

// removeReadExcludedPartitions unmarks the given partitions as excluded from reading in all jobsdbs
func (m *migrator) removeReadExcludedPartitions(ctx context.Context, sourcePartitions []string) error {
	g, ctx := errgroup.WithContext(ctx)
	for _, readerJobsDB := range m.readerJobsDBs {
		g.Go(func() error {
			if err := readerJobsDB.RemoveReadExcludedPartitionIDs(ctx, sourcePartitions); err != nil {
				return fmt.Errorf("removing read excluded partitions from %q jobsdb: %w", readerJobsDB.Identifier(), err)
			}
			return nil
		})
	}
	return g.Wait()
}

// waitForNoInProgressJobs waits until there are no in-progress job statuses in all jobsdbs for the given source partitions.
func (m *migrator) waitForNoInProgressJobs(ctx context.Context, sourcePartitions []string) error {
	defer m.stats.NewTaggedStat("partition_mig_src_wait_inprogress", stats.TimerType, m.statsTags()).RecordDuration()()
	pollTimeoutCtx, cancel := context.WithTimeout(ctx, m.c.waitForInProgressTimeout.Load())
	defer cancel()
	pollGroup, pollCtx := errgroup.WithContext(pollTimeoutCtx)

	getInProgressJobs := func(ctx context.Context, readerJobsDB jobsdb.JobsDB) (bool, error) {
		for {
			r, err := readerJobsDB.GetJobs(
				ctx,
				[]string{jobsdb.Executing.State, jobsdb.Importing.State},
				jobsdb.GetQueryParams{
					PartitionFilters: sourcePartitions,
					JobsLimit:        1,
				},
			)
			if err != nil {
				return false, fmt.Errorf("getting in-progress jobs from %q jobsdb: %w", readerJobsDB.Identifier(), err)
			}
			if len(r.Jobs) > 0 { // found in-progress jobs
				return true, nil
			}
			if len(r.Jobs) == 0 && !r.DSLimitsReached { // no in-progress jobs and no more jobs to fetch
				return false, nil
			}
		}
	}
	for _, readerJobsDB := range m.readerJobsDBs {
		pollGroup.Go(func() error {
			start := time.Now()
			hasInProgressJobs := true
			for hasInProgressJobs {
				select {
				case <-pollCtx.Done():
					return pollCtx.Err()
				default:
					var err error
					hasInProgressJobs, err = getInProgressJobs(pollCtx, readerJobsDB)
					if err != nil {
						return err
					}
					if hasInProgressJobs {
						m.logger.Infon("Waiting for in-progress jobs to complete",
							logger.NewStringField("jobsdb", readerJobsDB.Identifier()),
							logger.NewStringField("partitions", fmt.Sprintf("%v", sourcePartitions)),
							logger.NewDurationField("elapsed", time.Since(start)),
						)
						// sleep for a short duration before checking again
						if err := misc.SleepCtx(pollCtx, m.c.inProgressPollSleep.Load()); err != nil {
							return fmt.Errorf("sleeping while waiting for no in-progress jobs in %q jobsdb: %w", readerJobsDB.Identifier(), err)
						}
					}
				}
			}
			return nil
		})
	}
	err := pollGroup.Wait()
	// if timed out waiting for no in-progress jobs, trigger a shutdown
	if err != nil && pollTimeoutCtx.Err() != nil && ctx.Err() == nil {
		m.shutdown()
		return fmt.Errorf("timeout waiting for no in-progress jobs: %w", pollTimeoutCtx.Err())
	}
	return err
}

// Run watches for new migration jobs assigned to this source node and handles them asynchronously.
// All go routines are added to the provided errgroup.Group. It returns an error if the watcher cannot be created.
func (m *migrator) Run(ctx context.Context, wg *errgroup.Group) error {
	ctx = jobsdb.WithPriorityPool(ctx)                 // use priority pool for migration job handling
	m.pendingMigrationJobs = make(map[string]struct{}) // reset pending migration jobs map

	// create a watcher for partition migration jobs
	jobWatcher, err := etcdwatcher.NewBuilder[*etcdtypes.PartitionMigrationJob](m.etcdClient,
		etcdkeys.MigrationJobKeyPrefix(m.config)).
		WithPrefix().
		WithWatchEventType(etcdwatcher.PutWatchEventType).
		WithWatchMode(etcdwatcher.AllMode).
		WithFilter(func(event *etcdwatcher.Event[*etcdtypes.PartitionMigrationJob]) bool {
			// only watch for new migration jobs
			if event.Value.Status != etcdtypes.PartitionMigrationJobStatusNew {
				return false
			}
			// where this node is a source node
			return event.Value.SourceNode == m.nodeIndex
		}).
		Build()
	if err != nil {
		return fmt.Errorf("creating etcd watcher: %w", err)
	}

	wg.Go(func() error {
		// It keeps retrying on errors using an exponential backoff until the context is done.
		if err := retry.PerpetualExponentialBackoffWithNotify(ctx, m.config,
			func() error {
				// Watch for new partition migration job events and handle them
				values, leave := jobWatcher.Watch(ctx)
				defer leave()
				for value := range values {
					if value.Error != nil {
						return fmt.Errorf("watching partition migration job events: %w", value.Error)
					}
					// skip if migration job is already being processed,
					// otherwise add it to pending migration jobs
					m.pendingMigrationJobsMu.Lock()
					_, exists := m.pendingMigrationJobs[value.Event.Value.JobID]
					if !exists {
						m.pendingMigrationJobs[value.Event.Value.JobID] = struct{}{}
					}
					m.pendingMigrationJobsMu.Unlock()
					if exists {
						m.logger.Warnn("Received partition migration job event for a job that is already being processed, skipping",
							logger.NewStringField("jobId", value.Event.Value.JobID),
						)
						continue
					}
					// handle new migration event asynchronously
					wg.Go(func() error {
						m.onNewJob(ctx, value.Event.Key, value.Event.Value)
						return nil
					})
				}
				return nil
			},
			func(err error, d time.Duration) {
				m.logger.Errorn("Error watching for new partition migration jobs, retrying",
					logger.NewDurationField("retryIn", d),
					obskit.Error(err),
				)
			},
		); err != nil {
			m.logger.Errorn("Failed to watch new partition migration job events, context done",
				obskit.Error(err),
			)
		}
		return nil
	})
	return nil
}

// onNewJob handles a new partition migration job assigned to this source node:
//
// 1. Moves jobs for the specified partitions from this source node to the target node.
// 2. Removes the read-excluded status for the migrated partitions.
// 3. Marks the migration job status as "moved" in etcd.
// 4. Removes the job from the pending migration jobs map.
//
// It retries on errors with an exponential backoff until the context is done.
func (m *migrator) onNewJob(ctx context.Context, key string, job *etcdtypes.PartitionMigrationJob) {
	start := time.Now()
	log := m.logger.Withn(
		logger.NewStringField("migrationID", job.MigrationID),
		logger.NewStringField("jobID", job.JobID),
		logger.NewIntField("sourceNode", int64(job.SourceNode)),
		logger.NewIntField("targetNode", int64(job.TargetNode)),
	)
	log.Infon("Received new partition migration job",
		logger.NewStringField("partitions", fmt.Sprintf("%v", job.Partitions)),
	)

	// Keep retrying errors with a backoff until context is done
	if err := retry.PerpetualExponentialBackoffWithNotify(ctx, m.config,
		func() error {
			target, err := m.targetURLProvider(job.TargetNode)
			if err != nil {
				return fmt.Errorf("getting target URL: %w", err)
			}
			// move jobs from all source jobsdbs concurrently
			jobG, jobCtx := errgroup.WithContext(ctx)
			for _, db := range m.readerJobsDBs {
				jobG.Go(func() error {
					return client.NewMigrationJobExecutor(
						job.JobID, m.nodeIndex, job.Partitions, db, target,
						client.WithConfig(m.config),
						client.WithLogger(m.logger),
						client.WithStats(m.stats),
					).Run(jobCtx)
				})
			}
			if err := jobG.Wait(); err != nil {
				log.Errorn("Error while moving jobs to target node for partition migration job",
					obskit.Error(err),
				)
				return fmt.Errorf("moving jobs to target node for partition migration job: %w", err)
			}

			// remove read-excluded partitions from all jobsdbs
			if err := m.removeReadExcludedPartitions(ctx, job.Partitions); err != nil {
				return fmt.Errorf("removing read excluded partitions after migration: %w", err)
			}

			// mark partition migration job status as [moved] in etcd
			job.Status = etcdtypes.PartitionMigrationJobStatusMoved
			v, err := jsonrs.Marshal(job)
			if err != nil {
				return fmt.Errorf("marshaling migration job to mark its status as moved: %w", err)
			}
			res, err := m.etcdClient.Put(ctx, key, string(v))
			if err != nil {
				return fmt.Errorf("marking partition migration job status as moved in etcd: %w", err)
			}

			// remove from pending migration jobs
			m.pendingMigrationJobsMu.Lock()
			delete(m.pendingMigrationJobs, job.JobID)
			m.pendingMigrationJobsMu.Unlock()

			log.Infon("Partition migration job status marked as moved in etcd successfully",
				logger.NewIntField("revision", res.Header.Revision),
			)
			m.stats.NewTaggedStat("partition_mig_src_job", stats.TimerType, m.statsTags()).SendTiming(time.Since(start))
			return nil
		},
		func(err error, d time.Duration) {
			log.Errorn("Error handling new partition migration job, retrying",
				logger.NewDurationField("retryIn", d),
				obskit.Error(err),
			)
		},
	); err != nil {
		log.Errorn("Failed to handle new partition migration job, context done",
			obskit.Error(err),
		)
	}
}

func (m *migrator) statsTags() stats.Tags {
	return stats.Tags{
		"nodeIndex": strconv.Itoa(m.nodeIndex),
		"component": "processor",
	}
}

// getSourcePartitions returns the list of partitions assigned to the given source node index in the migration jobs
func getSourcePartitions(migration *etcdtypes.PartitionMigration, nodeIndex int) []string {
	var sourcePartitions []string
	for _, job := range migration.Jobs {
		if job.SourceNode == nodeIndex {
			sourcePartitions = append(sourcePartitions, job.Partitions...)
		}
	}
	return sourcePartitions
}
