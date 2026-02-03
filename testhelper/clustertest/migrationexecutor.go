package clustertest

import (
	"context"
	"fmt"
	"path"
	"slices"
	"time"

	"github.com/samber/lo"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/rudderlabs/rudder-go-kit/etcdwatcher"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	etcdtypes "github.com/rudderlabs/rudder-schemas/go/cluster"
	"github.com/rudderlabs/rudder-server/cluster/migrator/etcdclient"
)

type PartitionMigrationExecutor interface {
	// Run executes the migration process.
	Run(ctx context.Context) error
}

type Logger interface {
	Logf(format string, args ...any)
}

type PartitionChangedListener func(paritionID string, newNodeIndex int)

// NewPartitionMigrationExecutor creates a new PartitionMigrationExecutor instance.
func NewPartitionMigrationExecutor(
	namespace string, // namespace to use in etcd keys
	migration etcdtypes.PartitionMigration, // the migration to execute
	separateGateway bool, // whether gateways are separate from processors
	client etcdclient.Client, // etcd client to use
	listener PartitionChangedListener, // listener to notify partition changes
	logger Logger, // logger to use
) PartitionMigrationExecutor {
	return &migrationExecutor{
		namespace:               namespace,
		migration:               migration,
		separateGateway:         separateGateway,
		client:                  client,
		partitionChangeListener: listener,
		logger:                  logger,
	}
}

type migrationExecutor struct {
	namespace               string
	migration               etcdtypes.PartitionMigration
	separateGateway         bool
	client                  etcdclient.Client
	partitionChangeListener PartitionChangedListener
	logger                  Logger
}

func (me *migrationExecutor) Run(ctx context.Context) error {
	var (
		migrationKeyPrefix    = "/" + me.namespace + "/migration/request/"
		migrationAckKeyPrefix = "/" + me.namespace + "/migration/ack/"
		migrationJobKeyPrefix = "/" + me.namespace + "/migration/job/"
		reloadGwKeyPrefix     = "/" + me.namespace + "/reload/gateway/request/"
		reloadGwAckKeyPrefix  = "/" + me.namespace + "/reload/gateway/ack/"
	)

	putMigration := func() error {
		v, err := jsonrs.Marshal(me.migration)
		if err != nil {
			return fmt.Errorf("marshalling migration request: %w", err)
		}
		_, err = me.client.Put(ctx, path.Join(migrationKeyPrefix, me.migration.ID), string(v))
		if err != nil {
			return fmt.Errorf("putting migration request to etcd: %w", err)
		}
		return nil
	}

	// prepare migration
	me.migration.Status = etcdtypes.PartitionMigrationStatusNew
	me.migration.AckKeyPrefix = path.Join(migrationAckKeyPrefix, me.migration.ID)
	me.logger.Logf("migrationexecutor: putting new migration in etcd")
	if err := putMigration(); err != nil {
		return fmt.Errorf("putting new migration in etcd: %w", err)
	}

	requiredAcks := lo.Uniq(slices.Concat(me.migration.SourceNodes(), me.migration.TargetNodes()))

	me.logger.Logf("migrationexecutor: waiting for acks from all involved nodes")
	migrationAckWatcher, err := etcdwatcher.NewBuilder[etcdtypes.PartitionMigrationAck](me.client, me.migration.AckKeyPrefix).
		WithPrefix().
		WithWatchEventType(etcdwatcher.PutWatchEventType).
		WithWatchMode(etcdwatcher.AllMode).
		Build()
	if err != nil {
		return fmt.Errorf("building etcd watcher for migration acks: %w", err)
	}
	acksReceived := make(map[int]struct{})
	acks, leave := migrationAckWatcher.Watch(ctx)
	for ack := range acks {
		if ack.Error != nil {
			leave()
			return fmt.Errorf("watching for migration acks: %w", ack.Error)
		}
		acksReceived[ack.Event.Value.NodeIndex] = struct{}{}
		if lo.ElementsMatch(lo.Keys(acksReceived), requiredAcks) {
			break
		}
	}
	leave()

	if me.separateGateway {
		me.logger.Logf("migrationexecutor: updating migration status to reloading gw in etcd")
		me.migration.Status = etcdtypes.PartitionMigrationStatusReloadingGW
		if err := putMigration(); err != nil {
			return fmt.Errorf("updating migration status to reloading gw in etcd: %w", err)
		}

		me.logger.Logf("migrationexecutor: reloading gateways")
		rgc := etcdtypes.ReloadGatewayCommand{
			Nodes:        me.migration.TargetNodes(),
			AckKeyPrefix: path.Join(reloadGwAckKeyPrefix, me.migration.ID),
		}
		v, err := jsonrs.Marshal(rgc)
		if err != nil {
			return fmt.Errorf("marshalling reload gateway command: %w", err)
		}
		_, err = me.client.Put(ctx, path.Join(reloadGwKeyPrefix, me.migration.ID), string(v))
		if err != nil {
			return fmt.Errorf("putting reload gateway command to etcd: %w", err)
		}
		me.logger.Logf("migrationexecutor: waiting for acks from all involved gateway nodes")
		reloadAckWatcher, err := etcdwatcher.NewBuilder[etcdtypes.ReloadGatewayAck](me.client, rgc.AckKeyPrefix).
			WithPrefix().
			WithWatchEventType(etcdwatcher.PutWatchEventType).
			WithWatchMode(etcdwatcher.AllMode).
			Build()
		if err != nil {
			return fmt.Errorf("building etcd watcher for migration acks: %w", err)
		}
		acksReceived := make(map[int]struct{})
		acks, leave := reloadAckWatcher.Watch(ctx)
		for ack := range acks {
			if ack.Error != nil {
				leave()
				return fmt.Errorf("watching for migration acks: %w", ack.Error)
			}
			acksReceived[ack.Event.Value.NodeIndex] = struct{}{}
			if lo.ElementsMatch(lo.Keys(acksReceived), rgc.Nodes) {
				break
			}
		}
		leave()
		me.logger.Logf("migrationexecutor: gateways reloaded")
	}

	me.logger.Logf("migrationexecutor: updating migration status to reloading src router in etcd")
	me.migration.Status = etcdtypes.PartitionMigrationStatusReloadingSrcRouter
	if err := putMigration(); err != nil {
		return fmt.Errorf("updating migration status to reloading src router in etcd: %w", err)
	}
	me.logger.Logf("migrationexecutor: notifying partition switches to target nodes")
	for _, job := range me.migration.Jobs {
		for _, partitionID := range job.Partitions {
			me.partitionChangeListener(partitionID, job.TargetNode)
		}
	}

	me.logger.Logf("migrationexecutor: updating migration status to migrating in etcd")
	me.migration.Status = etcdtypes.PartitionMigrationStatusMigrating
	if err := putMigration(); err != nil {
		return fmt.Errorf("updating migration status to migrating in etcd: %w", err)
	}

	// sleep to allow for some jobsdb jobs to be buffered before starting the migration jobs
	time.Sleep(2 * time.Second)

	me.logger.Logf("migrationexecutor: creating migration jobs in etcd")
	for _, jobHeader := range me.migration.Jobs {
		j := etcdtypes.PartitionMigrationJob{
			PartitionMigrationJobHeader: *jobHeader,
			MigrationID:                 me.migration.ID,
			Status:                      etcdtypes.PartitionMigrationJobStatusNew,
		}
		v, err := jsonrs.Marshal(j)
		if err != nil {
			return fmt.Errorf("marshalling migration job: %w", err)
		}
		_, err = me.client.Put(ctx, path.Join(migrationJobKeyPrefix, me.migration.ID, j.JobID), string(v))
		if err != nil {
			return fmt.Errorf("putting migration job to etcd: %w", err)
		}
	}

	pendingJobs := lo.SliceToMap(me.migration.Jobs, func(job *etcdtypes.PartitionMigrationJobHeader) (string, struct{}) {
		return job.JobID, struct{}{}
	})

	me.logger.Logf("migrationexecutor: waiting for migration jobs to complete")
	jobStatusWatcher, err := etcdwatcher.NewBuilder[etcdtypes.PartitionMigrationJob](me.client, path.Join(migrationJobKeyPrefix, me.migration.ID)).
		WithPrefix().
		WithWatchMode(etcdwatcher.AllMode).
		WithFilter(func(e *etcdwatcher.Event[etcdtypes.PartitionMigrationJob]) bool {
			return e.Value.Status == etcdtypes.PartitionMigrationJobStatusCompleted
		}).
		Build()
	if err != nil {
		return fmt.Errorf("building etcd watcher for job status updates: %w", err)
	}
	jobs, leave := jobStatusWatcher.Watch(ctx)
	for job := range jobs {
		if job.Error != nil {
			leave()
			return fmt.Errorf("watching for job status updates: %w", job.Error)
		}
		delete(pendingJobs, job.Event.Value.JobID)
		if len(pendingJobs) == 0 {
			break
		}
	}
	leave()

	me.logger.Logf("migrationexecutor: updating migration status to completed in etcd")
	me.migration.Status = etcdtypes.PartitionMigrationStatusCompleted
	if err := putMigration(); err != nil {
		return fmt.Errorf("updating migration status to completed in etcd: %w", err)
	}
	me.logger.Logf("migrationexecutor: deleting migration jobs")
	if _, err := me.client.Delete(ctx, path.Join(migrationJobKeyPrefix, me.migration.ID), clientv3.WithPrefix()); err != nil {
		return fmt.Errorf("deleting migration jobs: %w", err)
	}
	me.logger.Logf("migrationexecutor: deleting migration acks")
	if _, err := me.client.Delete(ctx, me.migration.AckKeyPrefix, clientv3.WithPrefix()); err != nil {
		return fmt.Errorf("deleting migration acks: %w", err)
	}
	me.logger.Logf("migrationexecutor: deleting migration request")
	if _, err := me.client.Delete(ctx, path.Join(migrationKeyPrefix, me.migration.ID)); err != nil {
		return fmt.Errorf("deleting migration request: %w", err)
	}

	return nil
}
