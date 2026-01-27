package migrator

import (
	"context"
	"database/sql"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/etcd"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	etcdtypes "github.com/rudderlabs/rudder-schemas/go/cluster"
	"github.com/rudderlabs/rudder-server/cluster/migrator/etcdkeys"
	"github.com/rudderlabs/rudder-server/cluster/partitionbuffer"
	"github.com/rudderlabs/rudder-server/jobsdb"
	sqlmigrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
)

func TestGatewayPartitionMigrator(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	etcdResource, err := etcd.Setup(pool, t)
	require.NoError(t, err)

	pg, err := postgres.Setup(pool, t)
	require.NoError(t, err)

	// Run node migrations to create required tables (e.g., buffered_partitions_versions)
	runNodeMigration(t, pg.DB)

	t.Run("handle reload request successfully", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 1*time.Minute)
		defer cancel()

		// Setup unique namespace for this test
		namespace := rand.String(10)
		conf := config.New()
		conf.Set("WORKSPACE_NAMESPACE", namespace)

		nodeIndex := 0
		nodeName := "test-gw-node-0"

		// Create gw jobsdb for write with partitioning enabled
		gwJobsDB := jobsdb.NewForReadWrite("gw_"+rand.String(5),
			jobsdb.WithDBHandle(pg.DB),
			jobsdb.WithConfig(conf),
			jobsdb.WithSkipMaintenanceErr(true),
			jobsdb.WithNumPartitions(64),
		)
		require.NoError(t, gwJobsDB.Start(), "it should be able to start gw JobsDB")
		defer gwJobsDB.Stop()

		// Create buffer jobsdb for write
		bufferJobsDB := jobsdb.NewForWrite("gw_buf_"+rand.String(5),
			jobsdb.WithDBHandle(pg.DB),
			jobsdb.WithConfig(conf),
			jobsdb.WithSkipMaintenanceErr(true),
		)
		require.NoError(t, bufferJobsDB.Start(), "it should be able to start buffer JobsDB")
		defer bufferJobsDB.Stop()

		// Create partition buffer with writer-only jobsdbs
		pb, err := partitionbuffer.NewJobsDBPartitionBuffer(ctx,
			partitionbuffer.WithWriterOnlyJobsDBs(gwJobsDB, bufferJobsDB),
			partitionbuffer.WithStats(stats.NOP),
		)
		require.NoError(t, err)
		require.NoError(t, pb.Start())
		defer pb.Stop()

		// Wrap the partition buffer to track refresh calls
		trackedPB := &partitionBufferRefreshTracker{PartitionRefresher: pb}

		// Build and start the migrator with the tracked partition buffer as the refresher
		migrator, err := NewGatewayPartitionMigratorBuilder(nodeIndex, nodeName).
			WithConfig(conf).
			WithStats(stats.NOP).
			WithEtcdClient(etcdResource.Client).
			WithPartitionRefresher(trackedPB).
			Build()
		require.NoError(t, err)

		err = migrator.Start()
		require.NoError(t, err)
		defer migrator.Stop()

		// Create a reload command for this node
		reloadID := rand.String(10)
		ackKeyPrefix := "/" + namespace + "/reload/gateway/ack/" + reloadID
		reloadCmd := &etcdtypes.ReloadGatewayCommand{
			Nodes:        []int{nodeIndex},
			AckKeyPrefix: ackKeyPrefix,
		}

		// Put the reload command in etcd
		reloadKey := etcdkeys.ReloadGatewayRequestKeyPrefix(conf) + reloadID
		reloadValue, err := jsonrs.Marshal(reloadCmd)
		require.NoError(t, err)

		_, err = etcdResource.Client.Put(ctx, reloadKey, string(reloadValue))
		require.NoError(t, err)

		// Wait for the ack event
		require.Eventually(t, func() bool {
			resp, err := etcdResource.Client.Get(ctx, ackKeyPrefix, clientv3.WithPrefix())
			if err != nil {
				return false
			}
			return len(resp.Kvs) > 0
		}, 30*time.Second, 100*time.Millisecond, "ack event not received")

		// Verify the ack content
		resp, err := etcdResource.Client.Get(ctx, ackKeyPrefix, clientv3.WithPrefix())
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)

		require.Equal(t, path.Join(ackKeyPrefix, nodeName), string(resp.Kvs[0].Key))
		var ack etcdtypes.ReloadGatewayAck
		err = jsonrs.Unmarshal(resp.Kvs[0].Value, &ack)
		require.NoError(t, err)
		require.Equal(t, nodeIndex, ack.NodeIndex)
		require.Equal(t, nodeName, ack.NodeName)

		// Verify that RefreshBufferedPartitions was called on the partition buffer
		require.Equal(t, 1, trackedPB.refreshCalls, "RefreshBufferedPartitions should be called once")
	})

	t.Run("ignore reload requests for other nodes", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 1*time.Minute)
		defer cancel()

		namespace := rand.String(10)
		conf := config.New()
		conf.Set("WORKSPACE_NAMESPACE", namespace)

		nodeIndex := 0
		nodeName := "test-gw-node-0"

		// Create mock partition refresher to track calls
		mockRefresher := &mockPartitionRefresher{}

		// Build and start the migrator
		migrator, err := NewGatewayPartitionMigratorBuilder(nodeIndex, nodeName).
			WithConfig(conf).
			WithStats(stats.NOP).
			WithEtcdClient(etcdResource.Client).
			WithPartitionRefresher(mockRefresher).
			Build()
		require.NoError(t, err)

		err = migrator.Start()
		require.NoError(t, err)
		defer migrator.Stop()

		// Create a reload command for a different node (node 1)
		reloadID := rand.String(10)
		ackKeyPrefix := "/" + namespace + "/reload/gateway/ack/" + reloadID
		reloadCmd := &etcdtypes.ReloadGatewayCommand{
			Nodes:        []int{1, 2}, // not for node 0
			AckKeyPrefix: ackKeyPrefix,
		}

		// Put the reload command in etcd
		reloadKey := etcdkeys.ReloadGatewayRequestKeyPrefix(conf) + reloadID
		reloadValue, err := jsonrs.Marshal(reloadCmd)
		require.NoError(t, err)

		_, err = etcdResource.Client.Put(ctx, reloadKey, string(reloadValue))
		require.NoError(t, err)

		// Verify that no refresh was triggered (no ack should be sent)
		require.Never(t, func() bool {
			return mockRefresher.refreshCalls > 0
		}, 2*time.Second, 100*time.Millisecond, "refresh should not be called for other nodes")

		// Verify no ack was sent
		resp, err := etcdResource.Client.Get(ctx, ackKeyPrefix, clientv3.WithPrefix())
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 0, "no ack should be sent for reload commands targeting other nodes")
	})

	t.Run("handle multiple nodes in reload command", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 1*time.Minute)
		defer cancel()

		namespace := rand.String(10)
		conf := config.New()
		conf.Set("WORKSPACE_NAMESPACE", namespace)

		nodeIndex := 1
		nodeName := "test-gw-node-1"

		// Create mock partition refresher
		mockRefresher := &mockPartitionRefresher{}

		// Build and start the migrator
		migrator, err := NewGatewayPartitionMigratorBuilder(nodeIndex, nodeName).
			WithConfig(conf).
			WithStats(stats.NOP).
			WithEtcdClient(etcdResource.Client).
			WithPartitionRefresher(mockRefresher).
			Build()
		require.NoError(t, err)

		err = migrator.Start()
		require.NoError(t, err)
		defer migrator.Stop()

		// Create a reload command that includes this node among others
		reloadID := rand.String(10)
		ackKeyPrefix := "/" + namespace + "/reload/gateway/ack/" + reloadID
		reloadCmd := &etcdtypes.ReloadGatewayCommand{
			Nodes:        []int{0, 1, 2}, // includes node 1
			AckKeyPrefix: ackKeyPrefix,
		}

		// Put the reload command in etcd
		reloadKey := etcdkeys.ReloadGatewayRequestKeyPrefix(conf) + reloadID
		reloadValue, err := jsonrs.Marshal(reloadCmd)
		require.NoError(t, err)

		_, err = etcdResource.Client.Put(ctx, reloadKey, string(reloadValue))
		require.NoError(t, err)

		// Wait for the ack event
		require.Eventually(t, func() bool {
			resp, err := etcdResource.Client.Get(ctx, ackKeyPrefix, clientv3.WithPrefix())
			if err != nil {
				return false
			}
			return len(resp.Kvs) > 0
		}, 30*time.Second, 100*time.Millisecond, "ack event not received")

		// Verify refresh was called
		require.Equal(t, 1, mockRefresher.refreshCalls, "refresh should be called once")

		// Verify the ack content
		resp, err := etcdResource.Client.Get(ctx, ackKeyPrefix, clientv3.WithPrefix())
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)

		require.Equal(t, path.Join(ackKeyPrefix, nodeName), string(resp.Kvs[0].Key))
	})

	t.Run("ignore duplicate reload requests", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 1*time.Minute)
		defer cancel()

		namespace := rand.String(10)
		conf := config.New()
		conf.Set("WORKSPACE_NAMESPACE", namespace)

		nodeIndex := 0
		nodeName := "test-gw-node-0"

		// Create gw jobsdb for write with partitioning enabled
		gwJobsDB := jobsdb.NewForReadWrite("gw_"+rand.String(5),
			jobsdb.WithDBHandle(pg.DB),
			jobsdb.WithConfig(conf),
			jobsdb.WithSkipMaintenanceErr(true),
			jobsdb.WithNumPartitions(64),
		)
		require.NoError(t, gwJobsDB.Start(), "it should be able to start gw JobsDB")
		defer gwJobsDB.Stop()

		// Create buffer jobsdb for write
		bufferJobsDB := jobsdb.NewForWrite("gw_buf_"+rand.String(5),
			jobsdb.WithDBHandle(pg.DB),
			jobsdb.WithConfig(conf),
			jobsdb.WithSkipMaintenanceErr(true),
		)
		require.NoError(t, bufferJobsDB.Start(), "it should be able to start buffer JobsDB")
		defer bufferJobsDB.Stop()

		// Create partition buffer with writer-only jobsdbs
		pb, err := partitionbuffer.NewJobsDBPartitionBuffer(ctx,
			partitionbuffer.WithWriterOnlyJobsDBs(gwJobsDB, bufferJobsDB),
			partitionbuffer.WithStats(stats.NOP),
		)
		require.NoError(t, err)
		require.NoError(t, pb.Start())
		defer pb.Stop()

		// Wrap the partition buffer with a delay to simulate processing time
		// This ensures duplicate events arrive while the first is still being processed
		trackedPB := &partitionBufferRefreshTracker{
			PartitionRefresher: pb,
			delay:              2 * time.Second,
		}

		// Build and start the migrator
		migrator, err := NewGatewayPartitionMigratorBuilder(nodeIndex, nodeName).
			WithConfig(conf).
			WithStats(stats.NOP).
			WithEtcdClient(etcdResource.Client).
			WithPartitionRefresher(trackedPB).
			Build()
		require.NoError(t, err)

		err = migrator.Start()
		require.NoError(t, err)
		defer migrator.Stop()

		// Create a reload command
		reloadID := rand.String(10)
		ackKeyPrefix := "/" + namespace + "/reload/gateway/ack/" + reloadID
		reloadCmd := &etcdtypes.ReloadGatewayCommand{
			Nodes:        []int{nodeIndex},
			AckKeyPrefix: ackKeyPrefix,
		}

		// Put the reload command in etcd twice in quick succession
		reloadKey := etcdkeys.ReloadGatewayRequestKeyPrefix(conf) + reloadID
		reloadValue, err := jsonrs.Marshal(reloadCmd)
		require.NoError(t, err)

		_, err = etcdResource.Client.Put(ctx, reloadKey, string(reloadValue))
		require.NoError(t, err)
		_, err = etcdResource.Client.Put(ctx, reloadKey, string(reloadValue))
		require.NoError(t, err)

		// Wait for the ack event
		require.Eventually(t, func() bool {
			resp, err := etcdResource.Client.Get(ctx, ackKeyPrefix, clientv3.WithPrefix())
			if err != nil {
				return false
			}
			return len(resp.Kvs) > 0
		}, 30*time.Second, 100*time.Millisecond, "ack event not received")

		migrator.Stop()

		// Verify refresh was only called once despite duplicate events
		require.Equal(t, 1, trackedPB.refreshCalls, "refresh should be called only once despite duplicate events")
	})

	t.Run("retry on refresh failure then succeed", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 1*time.Minute)
		defer cancel()

		namespace := rand.String(10)
		conf := config.New()
		conf.Set("WORKSPACE_NAMESPACE", namespace)
		conf.Set("Migrator.retryInitialInterval", "100ms")
		conf.Set("Migrator.retryMaxInterval", "500ms")

		nodeIndex := 0
		nodeName := "test-gw-node-0"

		// Create mock partition refresher that fails the first 2 times, then succeeds
		callCount := 0
		mockRefresher := &mockPartitionRefresher{
			refreshFn: func(ctx context.Context) error {
				callCount++
				if callCount <= 2 {
					return fmt.Errorf("refresh failed (attempt %d)", callCount)
				}
				return nil
			},
		}

		// Build and start the migrator
		migrator, err := NewGatewayPartitionMigratorBuilder(nodeIndex, nodeName).
			WithConfig(conf).
			WithStats(stats.NOP).
			WithEtcdClient(etcdResource.Client).
			WithPartitionRefresher(mockRefresher).
			Build()
		require.NoError(t, err)

		err = migrator.Start()
		require.NoError(t, err)
		defer migrator.Stop()

		// Create a reload command
		reloadID := rand.String(10)
		ackKeyPrefix := "/" + namespace + "/reload/gateway/ack/" + reloadID
		reloadCmd := &etcdtypes.ReloadGatewayCommand{
			Nodes:        []int{nodeIndex},
			AckKeyPrefix: ackKeyPrefix,
		}

		// Put the reload command in etcd
		reloadKey := etcdkeys.ReloadGatewayRequestKeyPrefix(conf) + reloadID
		reloadValue, err := jsonrs.Marshal(reloadCmd)
		require.NoError(t, err)

		_, err = etcdResource.Client.Put(ctx, reloadKey, string(reloadValue))
		require.NoError(t, err)

		// Wait for the ack event (should succeed after retries)
		require.Eventually(t, func() bool {
			resp, err := etcdResource.Client.Get(ctx, ackKeyPrefix, clientv3.WithPrefix())
			if err != nil {
				return false
			}
			return len(resp.Kvs) > 0
		}, 30*time.Second, 100*time.Millisecond, "ack event not received")

		// Verify the ack content
		resp, err := etcdResource.Client.Get(ctx, ackKeyPrefix, clientv3.WithPrefix())
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)

		require.Equal(t, path.Join(ackKeyPrefix, nodeName), string(resp.Kvs[0].Key))

		migrator.Stop()

		// Verify refresh was called 3 times (2 failures + 1 success)
		require.Equal(t, 3, mockRefresher.refreshCalls, "refresh should be called 3 times (2 failures + 1 success)")
	})

	t.Run("etcd client initially returning errors", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 1*time.Minute)
		defer cancel()

		namespace := rand.String(10)
		conf := config.New()
		conf.Set("WORKSPACE_NAMESPACE", namespace)
		conf.Set("Migrator.retryInitialInterval", "100ms")
		conf.Set("Migrator.retryMaxInterval", "500ms")

		nodeIndex := 0
		nodeName := "test-gw-node-0"

		// Create mock partition refresher
		mockRefresher := &mockPartitionRefresher{}

		// Wrap the etcd client to simulate errors initially
		mockClient := &mockEtcdClient{
			Client:       etcdResource.Client,
			getFailCount: 3,
			getErr:       fmt.Errorf("etcd get error: connection refused"),
		}

		// Build and start the migrator with the mock client
		migrator, err := NewGatewayPartitionMigratorBuilder(nodeIndex, nodeName).
			WithConfig(conf).
			WithStats(stats.NOP).
			WithEtcdClient(mockClient).
			WithPartitionRefresher(mockRefresher).
			Build()
		require.NoError(t, err)

		err = migrator.Start()
		require.NoError(t, err)
		defer migrator.Stop()

		// Create a reload command
		reloadID := rand.String(10)
		ackKeyPrefix := "/" + namespace + "/reload/gateway/ack/" + reloadID
		reloadCmd := &etcdtypes.ReloadGatewayCommand{
			Nodes:        []int{nodeIndex},
			AckKeyPrefix: ackKeyPrefix,
		}

		// Put the reload command in etcd (use the real client to ensure data is there)
		reloadKey := etcdkeys.ReloadGatewayRequestKeyPrefix(conf) + reloadID
		reloadValue, err := jsonrs.Marshal(reloadCmd)
		require.NoError(t, err)

		_, err = etcdResource.Client.Put(ctx, reloadKey, string(reloadValue))
		require.NoError(t, err)

		// Wait for the ack event - this proves the migrator recovered from initial errors
		require.Eventually(t, func() bool {
			resp, err := etcdResource.Client.Get(ctx, ackKeyPrefix, clientv3.WithPrefix())
			if err != nil {
				return false
			}
			return len(resp.Kvs) > 0
		}, 30*time.Second, 100*time.Millisecond, "ack event not received")

		// Verify the ack content
		resp, err := etcdResource.Client.Get(ctx, ackKeyPrefix, clientv3.WithPrefix())
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)

		require.Equal(t, path.Join(ackKeyPrefix, nodeName), string(resp.Kvs[0].Key))

		migrator.Stop()

		// Verify the mock client was called multiple times (at least 3 times: 2 failures + 1 success)
		require.GreaterOrEqual(t, mockClient.getCount, 3, "etcd Get should be called at least 3 times")

		// Verify refresh was called
		require.Equal(t, 1, mockRefresher.refreshCalls, "refresh should be called once after recovery")
	})
}

// partitionBufferRefreshTracker wraps a PartitionRefresher and tracks calls to RefreshBufferedPartitions
type partitionBufferRefreshTracker struct {
	PartitionRefresher
	refreshCalls int
	delay        time.Duration // optional delay before calling the underlying RefreshBufferedPartitions
}

func (p *partitionBufferRefreshTracker) RefreshBufferedPartitions(ctx context.Context) error {
	p.refreshCalls++
	if p.delay > 0 {
		time.Sleep(p.delay)
	}
	return p.PartitionRefresher.RefreshBufferedPartitions(ctx)
}

type mockPartitionRefresher struct {
	refreshCalls int
	refreshFn    func(ctx context.Context) error
	refreshErr   error
}

func (m *mockPartitionRefresher) RefreshBufferedPartitions(ctx context.Context) error {
	m.refreshCalls++
	if m.refreshFn != nil {
		return m.refreshFn(ctx)
	}
	return m.refreshErr
}

type mockEtcdClient struct {
	*clientv3.Client
	getCount     int
	getFailCount int // number of times Get should fail before succeeding
	getErr       error
}

func (mec *mockEtcdClient) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	mec.getCount++
	if mec.getErr != nil && (mec.getFailCount == 0 || mec.getCount <= mec.getFailCount) {
		return nil, mec.getErr
	}
	return mec.Client.Get(ctx, key, opts...)
}

func runNodeMigration(t *testing.T, db *sql.DB) {
	t.Helper()
	m := &sqlmigrator.Migrator{
		Handle:          db,
		MigrationsTable: "node_migrations",
	}
	require.NoError(t, m.Migrate("node"))
}
