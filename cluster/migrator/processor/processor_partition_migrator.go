// Migrator component for rudder-server operating in processor mode
package processor

import (
	"context"
	"fmt"
	"slices"
	"strconv"
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
	"github.com/rudderlabs/rudder-server/cluster/migrator/processor/sourcenode"
	"github.com/rudderlabs/rudder-server/cluster/migrator/processor/targetnode"
	"github.com/rudderlabs/rudder-server/cluster/migrator/retry"
)

// PartitionMigrator handles partition migrations for a processor node
type PartitionMigrator interface {
	// Start starts the partition migrator
	Start() error

	// Stop stops the partition migrator
	Stop()
}

type processorPartitionMigrator struct {
	nodeIndex int
	nodeName  string

	// dependencies
	config         *config.Config
	logger         logger.Logger
	stats          stats.Stats
	etcdClient     etcdclient.Client
	sourceMigrator sourcenode.Migrator
	targetMigrator targetnode.Migrator

	// component lifecycle
	wg              *errgroup.Group
	lifecycleCtx    context.Context
	lifecycleCancel func()

	// state
	pendingMigrationsMu sync.Mutex
	pendingMigrations   map[string]struct{}
}

func (ppm *processorPartitionMigrator) Start() error {
	ppm.lifecycleCtx, ppm.lifecycleCancel = context.WithCancel(context.Background())
	wg, ctx := errgroup.WithContext(ppm.lifecycleCtx)
	ppm.wg = wg
	ppm.pendingMigrations = make(map[string]struct{})

	// start watching for new partition migrations
	wg.Go(func() error {
		ppm.watchNewMigrations(ctx)
		return nil
	})

	// start running source migrator
	if err := ppm.sourceMigrator.Run(ctx, wg); err != nil {
		return fmt.Errorf("starting source migrator: %w", err)
	}

	// start running target migrator
	if err := ppm.targetMigrator.Run(ctx, wg); err != nil {
		return fmt.Errorf("starting target migrator: %w", err)
	}
	return nil
}

// watchNewMigrations watches for new partition migration requests and handles them
// It keeps retrying on errors using an exponential backoff until the context is done.
func (ppm *processorPartitionMigrator) watchNewMigrations(ctx context.Context) {
	if err := retry.PerpetualExponentialBackoffWithNotify(ctx, ppm.config,
		func() error {
			// create a watcher for partition migration requests
			migrationWatcher, err := etcdwatcher.NewBuilder[*etcdtypes.PartitionMigration](ppm.etcdClient,
				etcdkeys.MigrationRequestKeyPrefix(ppm.config)).
				WithPrefix().
				WithWatchEventType(etcdwatcher.PutWatchEventType).
				WithWatchMode(etcdwatcher.AllMode).
				WithFilter(func(event *etcdwatcher.Event[*etcdtypes.PartitionMigration]) bool {
					// only watch for new migrations
					if event.Value.Status != etcdtypes.PartitionMigrationStatusNew {
						return false
					}
					// where this node is a source node, a target node, or both
					pm := event.Value
					if !slices.Contains(pm.SourceNodes(), ppm.nodeIndex) && !slices.Contains(pm.TargetNodes(), ppm.nodeIndex) {
						return false
					}

					// skip if migration is already being processed,
					// otherwise add it to pending migrations
					ppm.pendingMigrationsMu.Lock()
					_, exists := ppm.pendingMigrations[event.Value.ID]
					if !exists {
						ppm.pendingMigrations[event.Value.ID] = struct{}{}
					}
					ppm.pendingMigrationsMu.Unlock()
					return !exists
				}).
				Build()
			if err != nil {
				return fmt.Errorf("creating etcd watcher: %w", err)
			}

			// watch for new partition migration events and handle them
			values, leave := migrationWatcher.Watch(ctx)
			defer leave()
			for value := range values {
				if value.Error != nil {
					return fmt.Errorf("watching partition migration events: %w", value.Error)
				}
				// handle new migration event asynchronously
				ppm.wg.Go(func() error {
					ppm.onNewMigration(ctx, value.Event.Value)
					return nil
				})
			}
			return nil
		},
		func(err error, d time.Duration) {
			ppm.logger.Errorn("Error watching for new partition migrations, retrying",
				logger.NewDurationField("retryIn", d),
				obskit.Error(err),
			)
		},
	); err != nil {
		ppm.logger.Errorn("Failed to watch partition migration events, context done",
			obskit.Error(err),
		)
	}
}

// onNewMigration handles a new partition migration event. It keeps retrying until the migration is successfully started or the context is done.
func (ppm *processorPartitionMigrator) onNewMigration(ctx context.Context, pm *etcdtypes.PartitionMigration) {
	start := time.Now()
	ackKey := pm.AckKey(ppm.nodeName)
	ppm.logger.Infon("Received partition migration start event",
		logger.NewStringField("migrationId", pm.ID),
		logger.NewStringField("ackKey", ackKey),
	)
	// keep retrying until context is done
	if err := retry.PerpetualExponentialBackoffWithNotify(ctx, ppm.config,
		func() error {
			// use errgroup to run source and target migrations concurrently
			mg, mCtx := errgroup.WithContext(ctx)

			// if this node is a source node, start source migration
			if slices.Contains(pm.SourceNodes(), ppm.nodeIndex) {
				mg.Go(func() error {
					return ppm.sourceMigrator.Handle(mCtx, pm)
				})
			}
			// if this node is a target node, start target migration
			if slices.Contains(pm.TargetNodes(), ppm.nodeIndex) {
				mg.Go(func() error {
					return ppm.targetMigrator.Handle(mCtx, pm)
				})
			}
			if err := mg.Wait(); err != nil {
				return err
			}
			// send ack for migration started
			v, err := jsonrs.Marshal(pm.Ack(ppm.nodeIndex, ppm.nodeName))
			if err != nil {
				return fmt.Errorf("marshalling ack value: %w", err)
			}
			resp, err := ppm.etcdClient.Put(ctx, ackKey, string(v))
			if err != nil {
				return fmt.Errorf("putting ack key in etcd: %w", err)
			}
			ppm.logger.Infon("Acknowledged partition migration start event",
				logger.NewStringField("migrationId", pm.ID),
				logger.NewStringField("ackKey", ackKey),
				logger.NewIntField("revision", resp.Header.Revision),
				logger.NewDurationField("duration", time.Since(start)),
			)
			// remove from pending migrations
			ppm.pendingMigrationsMu.Lock()
			delete(ppm.pendingMigrations, pm.ID)
			ppm.pendingMigrationsMu.Unlock()
			ppm.stats.NewTaggedStat("partition_mig_handle", stats.TimerType, ppm.statsTags()).SendTiming(time.Since(start))
			return nil
		},
		func(err error, d time.Duration) {
			ppm.logger.Errorn("Error processing new partition migration event, retrying",
				logger.NewStringField("migrationId", pm.ID),
				logger.NewDurationField("retryIn", d),
				obskit.Error(err),
			)
		}); err != nil {
		ppm.logger.Errorn("Failed to process new partition migration event, context done",
			logger.NewStringField("migrationId", pm.ID),
			obskit.Error(err),
		)
	}
}

func (ppm *processorPartitionMigrator) Stop() {
	ppm.lifecycleCancel()
	_ = ppm.wg.Wait()
}

func (ppm *processorPartitionMigrator) statsTags() stats.Tags {
	return stats.Tags{
		"nodeIndex": strconv.Itoa(ppm.nodeIndex),
		"component": "processor",
	}
}
