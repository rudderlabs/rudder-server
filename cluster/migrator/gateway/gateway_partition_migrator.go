// Migrator component for rudder-server operating in gateway mode
package migrator

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
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
	"github.com/rudderlabs/rudder-server/cluster/migrator/retry"
)

// PartitionRefresher is the interface for refreshing buffered partitions
type PartitionRefresher interface {
	// RefreshBufferedPartitions refreshes the list of buffered partitions from the database
	RefreshBufferedPartitions(ctx context.Context) error
}

// PartitionMigrator handles partition migrations for a gateway node
type PartitionMigrator interface {
	// Start starts the partition migrator
	Start() error

	// Stop stops the partition migrator
	Stop()
}

type gatewayPartitionMigrator struct {
	nodeIndex int
	nodeName  string

	// dependencies
	config             *config.Config
	logger             logger.Logger
	stats              stats.Stats
	etcdClient         etcdclient.Client
	partitionRefresher PartitionRefresher

	// component lifecycle
	wg              *errgroup.Group
	lifecycleCtx    context.Context
	lifecycleCancel func()

	// state
	pendingReloadsMu sync.Mutex
	pendingReloads   map[string]struct{}
}

func (gpm *gatewayPartitionMigrator) Start() error {
	gpm.lifecycleCtx, gpm.lifecycleCancel = context.WithCancel(context.Background())
	wg, ctx := errgroup.WithContext(gpm.lifecycleCtx)
	gpm.wg = wg
	gpm.pendingReloads = make(map[string]struct{})

	// start watching for reload requests
	wg.Go(func() error {
		gpm.watchReloadRequests(ctx)
		return nil
	})

	return nil
}

// watchReloadRequests watches for gateway reload requests and handles them.
// It keeps retrying on errors using an exponential backoff until the context is done.
func (gpm *gatewayPartitionMigrator) watchReloadRequests(ctx context.Context) {
	if err := retry.PerpetualExponentialBackoffWithNotify(ctx, gpm.config,
		func() error {
			// create a watcher for gateway reload requests
			reloadWatcher, err := etcdwatcher.NewBuilder[*etcdtypes.ReloadGatewayCommand](gpm.etcdClient,
				etcdkeys.ReloadGatewayRequestKeyPrefix(gpm.config)).
				WithPrefix().
				WithWatchEventType(etcdwatcher.PutWatchEventType).
				WithWatchMode(etcdwatcher.AllMode).
				WithFilter(func(event *etcdwatcher.Event[*etcdtypes.ReloadGatewayCommand]) bool {
					// only handle reload commands for this node
					return slices.Contains(event.Value.Nodes, gpm.nodeIndex)
				}).
				WithTxnGate(func(rgc *etcdtypes.ReloadGatewayCommand) []clientv3.Cmp {
					// if ack key already exists, no need to process again
					ackKey := rgc.AckKey(gpm.nodeName)
					return []clientv3.Cmp{
						clientv3.Compare(clientv3.Version(ackKey), "=", 0),
					}
				}).
				Build()
			if err != nil {
				return fmt.Errorf("creating etcd watcher: %w", err)
			}

			// watch for reload events and handle them
			values, leave := reloadWatcher.Watch(ctx)
			defer leave()
			for value := range values {
				if value.Error != nil {
					return fmt.Errorf("watching reload events: %w", value.Error)
				}
				// skip if reload is already being processed,
				// otherwise add it to pending reloads
				gpm.pendingReloadsMu.Lock()
				_, exists := gpm.pendingReloads[value.Event.Value.AckKeyPrefix]
				if !exists {
					gpm.pendingReloads[value.Event.Value.AckKeyPrefix] = struct{}{}
				}
				gpm.pendingReloadsMu.Unlock()
				if exists {
					gpm.logger.Infon("Received duplicate gateway reload request, already being processed",
						logger.NewStringField("ackKeyPrefix", value.Event.Value.AckKeyPrefix),
					)
					continue
				}

				// handle reload event asynchronously
				gpm.wg.Go(func() error {
					gpm.onReloadRequest(ctx, value.Event.Value)
					return nil
				})
			}
			return nil
		},
		func(err error, d time.Duration) {
			gpm.logger.Errorn("Error watching for gateway reload requests, retrying",
				logger.NewDurationField("retryIn", d),
				obskit.Error(err),
			)
		},
	); err != nil {
		gpm.logger.Errorn("Failed to watch gateway reload events, context done",
			obskit.Error(err),
		)
	}
}

// onReloadRequest handles a gateway reload request. It keeps retrying until the reload is successful or the context is done.
func (gpm *gatewayPartitionMigrator) onReloadRequest(ctx context.Context, cmd *etcdtypes.ReloadGatewayCommand) {
	start := time.Now()
	ackKey := cmd.AckKey(gpm.nodeName)
	gpm.logger.Infon("Received gateway reload request",
		logger.NewStringField("ackKeyPrefix", cmd.AckKeyPrefix),
		logger.NewStringField("ackKey", ackKey),
	)
	// keep retrying until context is done
	if err := retry.PerpetualExponentialBackoffWithNotify(ctx, gpm.config,
		func() error {
			// refresh buffered partitions from database
			if err := gpm.partitionRefresher.RefreshBufferedPartitions(ctx); err != nil {
				return fmt.Errorf("refreshing buffered partitions: %w", err)
			}

			// send ack for reload completed
			v, err := jsonrs.Marshal(cmd.Ack(gpm.nodeIndex, gpm.nodeName))
			if err != nil {
				return fmt.Errorf("marshalling ack value: %w", err)
			}
			resp, err := gpm.etcdClient.Put(ctx, ackKey, string(v))
			if err != nil {
				return fmt.Errorf("putting ack key in etcd: %w", err)
			}
			gpm.logger.Infon("Acknowledged gateway reload request",
				logger.NewStringField("ackKeyPrefix", cmd.AckKeyPrefix),
				logger.NewStringField("ackKey", ackKey),
				logger.NewIntField("revision", resp.Header.Revision),
				logger.NewDurationField("duration", time.Since(start)),
			)
			// remove from pending reloads
			gpm.pendingReloadsMu.Lock()
			delete(gpm.pendingReloads, cmd.AckKeyPrefix)
			gpm.pendingReloadsMu.Unlock()
			gpm.stats.NewTaggedStat("partition_mig_gw_reload", stats.TimerType, gpm.statsTags()).SendTiming(time.Since(start))
			return nil
		},
		func(err error, d time.Duration) {
			gpm.logger.Errorn("Error processing gateway reload request, retrying",
				logger.NewStringField("ackKeyPrefix", cmd.AckKeyPrefix),
				logger.NewDurationField("retryIn", d),
				obskit.Error(err),
			)
		}); err != nil {
		gpm.logger.Errorn("Failed to process gateway reload request, context done",
			logger.NewStringField("ackKeyPrefix", cmd.AckKeyPrefix),
			obskit.Error(err),
		)
	}
}

func (gpm *gatewayPartitionMigrator) Stop() {
	gpm.lifecycleCancel()
	_ = gpm.wg.Wait()
}

func (gpm *gatewayPartitionMigrator) statsTags() stats.Tags {
	return stats.Tags{
		"nodeIndex": strconv.Itoa(gpm.nodeIndex),
		"component": "gateway",
	}
}
