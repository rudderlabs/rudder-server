package targetnode

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/etcdwatcher"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	etcdtypes "github.com/rudderlabs/rudder-schemas/go/cluster"
	"github.com/rudderlabs/rudder-server/cluster/migrator/etcdclient"
	"github.com/rudderlabs/rudder-server/cluster/migrator/etcdkeys"
	"github.com/rudderlabs/rudder-server/cluster/migrator/partitionmigration/server"
	"github.com/rudderlabs/rudder-server/cluster/migrator/retry"
	"github.com/rudderlabs/rudder-server/cluster/partitionbuffer"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

// Migrator defines the interface for a target node migrator
type Migrator interface {
	// Handle prepares the target node for starting a new partition migration
	Handle(ctx context.Context, migration *etcdtypes.PartitionMigration) error

	// Run starts a gRPC server for accepting jobs from source nodes and watches for moved migration jobs assigned to this target node
	Run(ctx context.Context, wg *errgroup.Group) error
}

type migrator struct {
	nodeIndex int
	nodeName  string

	etcdClient        etcdclient.Client
	bufferedJobsDBs   [][]partitionbuffer.JobsDBPartitionBuffer // hierarchy of partition buffer jobsdbs
	unbufferedJobsDBs []jobsdb.JobsDB                           // jobsdbs used for writing migrated jobs to (unbuffered)
	config            *config.Config
	logger            logger.Logger
	stats             stats.Stats

	// state
	pendingMigrationJobsMu sync.Mutex
	pendingMigrationJobs   map[string]struct{}
}

// Handle marks the partitions assigned to this target node as buffered in all buffered jobsDBs.
func (m *migrator) Handle(ctx context.Context, migration *etcdtypes.PartitionMigration) error {
	defer m.stats.NewTaggedStat("partition_mig_target_handle", stats.TimerType, m.statsTags()).RecordDuration()()
	targetPartitions := getTargetPartitions(migration, m.nodeIndex)
	if len(targetPartitions) == 0 {
		return nil // no partitions assigned to this target node
	}

	// Mark the partitions as buffered in all bufferedJobsDBs
	g, ctx := errgroup.WithContext(ctx)
	allBufferedDBs := lo.FlatMap(m.bufferedJobsDBs, func(x []partitionbuffer.JobsDBPartitionBuffer, _ int) []partitionbuffer.JobsDBPartitionBuffer {
		return x
	})
	for _, pb := range allBufferedDBs {
		g.Go(func() error {
			if err := pb.BufferPartitions(ctx, targetPartitions); err != nil {
				return fmt.Errorf("marking partitions as buffered in in %q jobsdb: %w", pb.Identifier(), err)
			}
			return nil
		})
	}
	return g.Wait()
}

// Run watches for moved migration jobs assigned to this target node and handles them asynchronously.
// It also starts a gRPC server for accepting jobs from source nodes.
// All go routines are added to the provided errgroup.Group.
// It returns an error if the watcher cannot be created, or if the gRPC server fails to start.
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
			// only watch for moved migration jobs
			if event.Value.Status != etcdtypes.PartitionMigrationJobStatusMoved {
				return false
			}
			// where this node is a target node
			pmj := event.Value
			if pmj.TargetNode != m.nodeIndex {
				return false
			}
			// skip if migration job is already being processed,
			// otherwise add it to pending migration jobs
			m.pendingMigrationJobsMu.Lock()
			_, exists := m.pendingMigrationJobs[event.Value.JobID]
			if !exists {
				m.pendingMigrationJobs[event.Value.JobID] = struct{}{}
			}
			m.pendingMigrationJobsMu.Unlock()
			return !exists
		}).
		Build()
	if err != nil {
		return fmt.Errorf("creating etcd watcher: %w", err)
	}
	// start gRPC server for accepting jobs from source nodes
	pms := server.NewPartitionMigrationServer(ctx, m.unbufferedJobsDBs,
		server.WithDedupEnabled(m.config.GetBoolVar(true, "PartitionMigration.Grpc.Server.dedupEnabled")),
		server.WithStreamTimeout(m.config.GetReloadableDurationVar(10, time.Minute, "PartitionMigration.Grpc.Server.streamTimeout")),
		server.WithLogger(m.logger),
		server.WithStats(m.stats),
	)
	grpcServer := server.NewGRPCServer(m.config, pms)
	if err := grpcServer.Start(); err != nil {
		return fmt.Errorf("starting gRPC server: %w", err)
	}

	// start watching for moved migration jobs
	wg.Go(func() error {
		// It keeps retrying on errors using an exponential backoff until the context is done.
		if err := retry.PerpetualExponentialBackoffWithNotify(ctx, m.config,
			func() error {
				// Watch for moved partition migration job events and handle them
				values, leave := jobWatcher.Watch(ctx)
				defer leave()
				for value := range values {
					if value.Error != nil {
						return fmt.Errorf("watching partition migration job events: %w", value.Error)
					}
					// handle moved migration event asynchronously
					wg.Go(func() error {
						m.onNewJob(ctx, value.Event.Key, value.Event.Value)
						return nil
					})
				}
				return nil
			},
			func(err error, d time.Duration) {
				m.logger.Errorn("Error watching for moved partition migration jobs, retrying",
					logger.NewDurationField("retryIn", d),
					obskit.Error(err),
				)
			},
		); err != nil {
			m.logger.Errorn("Failed to watch moved partition migration job events, context done",
				obskit.Error(err),
			)
		}
		return nil
	})

	// stop gRPC server when context is done
	wg.Go(func() error {
		<-ctx.Done()
		grpcServer.Stop()
		return nil
	})
	return nil
}

// onNewJob handles a moved partition migration job assigned to this target node:
// For each group of buffered JobsDBPartitionBuffers, it flushes the buffered partitions for
// this job concurrently in all partition buffers of that group. Then it marks the job as completed
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
	log.Infon("Received moved partition migration job",
		logger.NewStringField("partitions", fmt.Sprintf("%v", job.Partitions)),
	)

	// Keep retrying errors with a backoff until context is done
	if err := retry.PerpetualExponentialBackoffWithNotify(ctx, m.config,
		func() error {
			// flush jobs in sequence
			for pbGroupIndex, pbGroup := range m.bufferedJobsDBs {
				// flush buffered partitions for this job in all partition buffers of this group concurrently
				g, jobCtx := errgroup.WithContext(ctx)
				groupIdentifiers := lo.Map(pbGroup, func(p partitionbuffer.JobsDBPartitionBuffer, _ int) string { return p.Identifier() })
				log.Infon("Flushing buffered partitions for group",
					logger.NewIntField("groupIndex", int64(pbGroupIndex)),
					logger.NewStringField("groupMembers", strings.Join(groupIdentifiers, ",")),
				)
				for _, pb := range pbGroup {
					pb := pb
					g.Go(func() error {
						if err := pb.FlushBufferedPartitions(jobCtx, job.Partitions); err != nil {
							return fmt.Errorf("flushing buffered partitions of %q: %w", pb.Identifier(), err)
						}
						return nil
					})
				}
				if err := g.Wait(); err != nil {
					return fmt.Errorf("flushing buffered partitions for group index %d: %w", pbGroupIndex, err)
				}
				log.Infon("Flushed buffered partitions for group successfully",
					logger.NewIntField("groupIndex", int64(pbGroupIndex)),
					logger.NewStringField("groupMembers", strings.Join(groupIdentifiers, ",")),
				)
			}

			// mark partition migration job status as [completed] in etcd
			job.Status = etcdtypes.PartitionMigrationJobStatusCompleted
			v, err := jsonrs.Marshal(job)
			if err != nil {
				return fmt.Errorf("marshaling migration job to mark its status as completed: %w", err)
			}
			res, err := m.etcdClient.Put(ctx, key, string(v))
			if err != nil {
				return fmt.Errorf("marking partition migration job status as completed in etcd: %w", err)
			}

			// remove from pending migration jobs
			m.pendingMigrationJobsMu.Lock()
			delete(m.pendingMigrationJobs, job.JobID)
			m.pendingMigrationJobsMu.Unlock()

			log.Infon("Partition migration job status marked as completed in etcd successfully",
				logger.NewIntField("revision", res.Header.Revision),
			)
			m.stats.NewTaggedStat("partition_mig_target_job", stats.TimerType, m.statsTags()).SendTiming(time.Since(start))
			return nil
		},
		func(err error, d time.Duration) {
			log.Errorn("Error handling moved partition migration job, retrying",
				logger.NewDurationField("retryIn", d),
				obskit.Error(err),
			)
		},
	); err != nil {
		log.Errorn("Failed to handle moved partition migration job, context done",
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

// getTargetPartitions returns the list of partitions assigned to the given target node index in the migration jobs
func getTargetPartitions(migration *etcdtypes.PartitionMigration, nodeIndex int) []string {
	var targetPartitions []string
	for _, job := range migration.Jobs {
		if job.TargetNode == nodeIndex {
			targetPartitions = append(targetPartitions, job.Partitions...)
		}
	}
	return targetPartitions
}
