package processor

import (
	"context"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/etcd"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	etcdtypes "github.com/rudderlabs/rudder-schemas/go/cluster"
	"github.com/rudderlabs/rudder-server/cluster/migrator/etcdkeys"
)

func TestProcessorPartitionMigrator(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	etcdResource, err := etcd.Setup(pool, t)
	require.NoError(t, err)

	t.Run("handle new migration successfully", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 1*time.Minute)
		defer cancel()

		// Setup unique namespace for this test
		namespace := rand.String(10)
		conf := config.New()
		conf.Set("WORKSPACE_NAMESPACE", namespace)

		nodeIndex := 0
		nodeName := "test-node-0"

		// Create mock migrators
		sourceMigrator := &mockSourceMigrator{}
		targetMigrator := &mockTargetMigrator{}

		// Build and start the migrator
		migrator, err := NewProcessorPartitionMigratorBuilder(nodeIndex, nodeName).
			WithConfig(conf).
			WithEtcdClient(etcdResource.Client).
			WithSourceMigrator(sourceMigrator).
			WithTargetMigrator(targetMigrator).
			Build()
		require.NoError(t, err)

		err = migrator.Start()
		require.NoError(t, err)
		defer migrator.Stop()

		// Create a migration where this node is both source and target
		migrationID := rand.String(10)
		ackKeyPrefix := "/" + namespace + "/migration/ack/" + migrationID
		migration := &etcdtypes.PartitionMigration{
			ID:     migrationID,
			Status: etcdtypes.PartitionMigrationStatusNew,
			Jobs: []*etcdtypes.PartitionMigrationJobHeader{
				{
					JobID:      "job-1",
					SourceNode: nodeIndex,
					TargetNode: 1,
					Partitions: []string{"partition-1"},
				},
				{
					JobID:      "job-2",
					SourceNode: 1,
					TargetNode: nodeIndex,
					Partitions: []string{"partition-2"},
				},
			},
			AckKeyPrefix: ackKeyPrefix,
		}

		// Put the migration event in etcd
		migrationKey := etcdkeys.MigrationRequestKeyPrefix(conf) + migrationID
		migrationValue, err := jsonrs.Marshal(migration)
		require.NoError(t, err)

		_, err = etcdResource.Client.Put(ctx, migrationKey, string(migrationValue))
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
		var ack etcdtypes.PartitionMigrationAck
		err = jsonrs.Unmarshal(resp.Kvs[0].Value, &ack)
		require.NoError(t, err)
		require.Equal(t, nodeIndex, ack.NodeIndex)
		require.Equal(t, nodeName, ack.NodeName)

		migrator.Stop()

		// Verify method calls on mocks
		require.Equal(t, 1, sourceMigrator.runCalls, "source migrator Run should be called once")
		require.Equal(t, 1, sourceMigrator.handleCalls, "source migrator NewSourceMigration should be called once")
		require.Equal(t, 1, targetMigrator.runCalls, "target migrator Run should be called once")
		require.Equal(t, 1, targetMigrator.handleCalls, "target migrator NewTargetMigration should be called once")
	})

	t.Run("start with error", func(t *testing.T) {
		t.Run("source migrator failing", func(t *testing.T) {
			namespace := rand.String(10)
			conf := config.New()
			conf.Set("WORKSPACE_NAMESPACE", namespace)

			nodeIndex := 0
			nodeName := "test-node-0"

			// Create mock migrators where source migrator fails on Run
			sourceMigrator := &mockSourceMigrator{
				runError: fmt.Errorf("source migrator run failed"),
			}
			targetMigrator := &mockTargetMigrator{}

			// Build the migrator
			migrator, err := NewProcessorPartitionMigratorBuilder(nodeIndex, nodeName).
				WithConfig(conf).
				WithEtcdClient(etcdResource.Client).
				WithSourceMigrator(sourceMigrator).
				WithTargetMigrator(targetMigrator).
				Build()
			require.NoError(t, err)

			// Start should fail because source migrator's Run fails
			err = migrator.Start()
			require.Error(t, err)
			require.Contains(t, err.Error(), "starting source migrator")
			require.Contains(t, err.Error(), "source migrator run failed")

			// Verify source migrator Run was called, but target migrator Run was not
			require.Equal(t, 1, sourceMigrator.runCalls, "source migrator Run should be called once")
			require.Equal(t, 0, targetMigrator.runCalls, "target migrator Run should not be called")
		})

		t.Run("target migrator failing", func(t *testing.T) {
			namespace := rand.String(10)
			conf := config.New()
			conf.Set("WORKSPACE_NAMESPACE", namespace)

			nodeIndex := 0
			nodeName := "test-node-0"

			// Create mock migrators where target migrator fails on Run
			sourceMigrator := &mockSourceMigrator{}
			targetMigrator := &mockTargetMigrator{
				runError: fmt.Errorf("target migrator run failed"),
			}

			// Build the migrator
			migrator, err := NewProcessorPartitionMigratorBuilder(nodeIndex, nodeName).
				WithConfig(conf).
				WithEtcdClient(etcdResource.Client).
				WithSourceMigrator(sourceMigrator).
				WithTargetMigrator(targetMigrator).
				Build()
			require.NoError(t, err)

			// Start should fail because target migrator's Run fails
			err = migrator.Start()
			require.Error(t, err)
			require.Contains(t, err.Error(), "starting target migrator")
			require.Contains(t, err.Error(), "target migrator run failed")

			// Verify both migrator Runs were called
			require.Equal(t, 1, sourceMigrator.runCalls, "source migrator Run should be called once")
			require.Equal(t, 1, targetMigrator.runCalls, "target migrator Run should be called once")
		})
	})

	t.Run("ignore irrelevant events", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 1*time.Minute)
		defer cancel()

		namespace := rand.String(10)
		conf := config.New()
		conf.Set("WORKSPACE_NAMESPACE", namespace)

		nodeIndex := 0
		nodeName := "test-node-0"

		// Create mock migrators with a delay to simulate processing time
		// This ensures duplicate events arrive while the first is still being processed
		processingDelay := 2 * time.Second
		sourceMigrator := &mockSourceMigrator{
			handleFn: func(ctx context.Context, migration *etcdtypes.PartitionMigration) error {
				time.Sleep(processingDelay)
				return nil
			},
		}
		targetMigrator := &mockTargetMigrator{
			handleFn: func(ctx context.Context, migration *etcdtypes.PartitionMigration) error {
				time.Sleep(processingDelay)
				return nil
			},
		}

		// Build and start the migrator
		migrator, err := NewProcessorPartitionMigratorBuilder(nodeIndex, nodeName).
			WithConfig(conf).
			WithEtcdClient(etcdResource.Client).
			WithSourceMigrator(sourceMigrator).
			WithTargetMigrator(targetMigrator).
			Build()
		require.NoError(t, err)

		err = migrator.Start()
		require.NoError(t, err)
		defer migrator.Stop()

		// Test 1: Migration not assigned to this node (node indices 2 and 3)
		migrationNotForThisNode := &etcdtypes.PartitionMigration{
			ID:     rand.String(10),
			Status: etcdtypes.PartitionMigrationStatusNew,
			Jobs: []*etcdtypes.PartitionMigrationJobHeader{
				{
					JobID:      "job-1",
					SourceNode: 2,
					TargetNode: 3,
					Partitions: []string{"partition-1"},
				},
			},
			AckKeyPrefix: "/" + namespace + "/migration/ack/" + rand.String(10),
		}
		migrationValue, err := jsonrs.Marshal(migrationNotForThisNode)
		require.NoError(t, err)
		_, err = etcdResource.Client.Put(ctx, etcdkeys.MigrationRequestKeyPrefix(conf)+migrationNotForThisNode.ID, string(migrationValue))
		require.NoError(t, err)

		// Test 2: Migration with status != new (status = in_progress)
		migrationNotNew := &etcdtypes.PartitionMigration{
			ID:     rand.String(10),
			Status: etcdtypes.PartitionMigrationStatusMigrating,
			Jobs: []*etcdtypes.PartitionMigrationJobHeader{
				{
					JobID:      "job-1",
					SourceNode: nodeIndex,
					TargetNode: 1,
					Partitions: []string{"partition-1"},
				},
			},
			AckKeyPrefix: "/" + namespace + "/migration/ack/" + rand.String(10),
		}
		migrationValue, err = jsonrs.Marshal(migrationNotNew)
		require.NoError(t, err)
		_, err = etcdResource.Client.Put(ctx, etcdkeys.MigrationRequestKeyPrefix(conf)+migrationNotNew.ID, string(migrationValue))
		require.NoError(t, err)

		require.Never(t, func() bool {
			return sourceMigrator.handleCalls > 0 || targetMigrator.handleCalls > 0
		}, 2*time.Second, 100*time.Millisecond)

		// Test 3: Send duplicate migration events - same migration sent twice
		duplicateMigrationID := rand.String(10)
		duplicateAckKeyPrefix := "/" + namespace + "/migration/ack/" + duplicateMigrationID
		duplicateMigration := &etcdtypes.PartitionMigration{
			ID:     duplicateMigrationID,
			Status: etcdtypes.PartitionMigrationStatusNew,
			Jobs: []*etcdtypes.PartitionMigrationJobHeader{
				{
					JobID:      "job-1",
					SourceNode: nodeIndex,
					TargetNode: 1,
					Partitions: []string{"partition-1"},
				},
			},
			AckKeyPrefix: duplicateAckKeyPrefix,
		}
		migrationValue, err = jsonrs.Marshal(duplicateMigration)
		require.NoError(t, err)

		// Send the same migration twice in quick succession
		_, err = etcdResource.Client.Put(ctx, etcdkeys.MigrationRequestKeyPrefix(conf)+duplicateMigrationID, string(migrationValue))
		require.NoError(t, err)
		_, err = etcdResource.Client.Put(ctx, etcdkeys.MigrationRequestKeyPrefix(conf)+duplicateMigrationID, string(migrationValue))
		require.NoError(t, err)

		// Wait for the ack event
		require.Eventually(t, func() bool {
			resp, err := etcdResource.Client.Get(ctx, duplicateAckKeyPrefix, clientv3.WithPrefix())
			if err != nil {
				return false
			}
			return len(resp.Kvs) > 0
		}, 30*time.Second, 100*time.Millisecond, "ack event not received for duplicate test")

		// Stop the migrator and verify call counts
		migrator.Stop()

		// Should only have one set of method calls despite sending duplicate events
		require.Equal(t, 1, sourceMigrator.runCalls, "source migrator Run should be called once")
		require.Equal(t, 1, sourceMigrator.handleCalls, "source migrator NewSourceMigration should be called only once despite duplicate events")
		require.Equal(t, 1, targetMigrator.runCalls, "target migrator Run should be called once")
		require.Equal(t, 0, targetMigrator.handleCalls, "target migrator NewTargetMigration should not be called (node is only source)")
	})

	t.Run("new source migration failing then succeeding", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 1*time.Minute)
		defer cancel()

		namespace := rand.String(10)
		conf := config.New()
		conf.Set("WORKSPACE_NAMESPACE", namespace)

		nodeIndex := 0
		nodeName := "test-node-0"

		// Create mock source migrator that fails the first 2 times, then succeeds
		callCount := 0
		sourceMigrator := &mockSourceMigrator{
			handleFn: func(ctx context.Context, migration *etcdtypes.PartitionMigration) error {
				callCount++
				if callCount <= 2 {
					return fmt.Errorf("source migration failed (attempt %d)", callCount)
				}
				return nil
			},
		}
		targetMigrator := &mockTargetMigrator{}

		// Build and start the migrator
		migrator, err := NewProcessorPartitionMigratorBuilder(nodeIndex, nodeName).
			WithConfig(conf).
			WithEtcdClient(etcdResource.Client).
			WithSourceMigrator(sourceMigrator).
			WithTargetMigrator(targetMigrator).
			Build()
		require.NoError(t, err)

		err = migrator.Start()
		require.NoError(t, err)
		defer migrator.Stop()

		// Create a migration where this node is the source
		migrationID := rand.String(10)
		ackKeyPrefix := "/" + namespace + "/migration/ack/" + migrationID
		migration := &etcdtypes.PartitionMigration{
			ID:     migrationID,
			Status: etcdtypes.PartitionMigrationStatusNew,
			Jobs: []*etcdtypes.PartitionMigrationJobHeader{
				{
					JobID:      "job-1",
					SourceNode: nodeIndex,
					TargetNode: 1,
					Partitions: []string{"partition-1"},
				},
			},
			AckKeyPrefix: ackKeyPrefix,
		}

		// Put the migration event in etcd
		migrationKey := etcdkeys.MigrationRequestKeyPrefix(conf) + migrationID
		migrationValue, err := jsonrs.Marshal(migration)
		require.NoError(t, err)

		_, err = etcdResource.Client.Put(ctx, migrationKey, string(migrationValue))
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
		var ack etcdtypes.PartitionMigrationAck
		err = jsonrs.Unmarshal(resp.Kvs[0].Value, &ack)
		require.NoError(t, err)
		require.Equal(t, nodeIndex, ack.NodeIndex)
		require.Equal(t, nodeName, ack.NodeName)

		migrator.Stop()

		// Verify method calls on mocks
		// NewSourceMigration should be called 3 times (2 failures + 1 success)
		require.Equal(t, 1, sourceMigrator.runCalls, "source migrator Run should be called once")
		require.Equal(t, 3, sourceMigrator.handleCalls, "source migrator NewSourceMigration should be called 3 times (2 failures + 1 success)")
		require.Equal(t, 1, targetMigrator.runCalls, "target migrator Run should be called once")
		require.Equal(t, 0, targetMigrator.handleCalls, "target migrator NewTargetMigration should not be called (node is only source)")
	})

	t.Run("new target migration failing then succeeding", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 1*time.Minute)
		defer cancel()

		namespace := rand.String(10)
		conf := config.New()
		conf.Set("WORKSPACE_NAMESPACE", namespace)

		nodeIndex := 0
		nodeName := "test-node-0"

		// Create mock target migrator that fails the first 2 times, then succeeds
		callCount := 0
		sourceMigrator := &mockSourceMigrator{}
		targetMigrator := &mockTargetMigrator{
			handleFn: func(ctx context.Context, migration *etcdtypes.PartitionMigration) error {
				callCount++
				if callCount <= 2 {
					return fmt.Errorf("target migration failed (attempt %d)", callCount)
				}
				return nil
			},
		}

		// Build and start the migrator
		migrator, err := NewProcessorPartitionMigratorBuilder(nodeIndex, nodeName).
			WithConfig(conf).
			WithEtcdClient(etcdResource.Client).
			WithSourceMigrator(sourceMigrator).
			WithTargetMigrator(targetMigrator).
			Build()
		require.NoError(t, err)

		err = migrator.Start()
		require.NoError(t, err)
		defer migrator.Stop()

		// Create a migration where this node is the target
		migrationID := rand.String(10)
		ackKeyPrefix := "/" + namespace + "/migration/ack/" + migrationID
		migration := &etcdtypes.PartitionMigration{
			ID:     migrationID,
			Status: etcdtypes.PartitionMigrationStatusNew,
			Jobs: []*etcdtypes.PartitionMigrationJobHeader{
				{
					JobID:      "job-1",
					SourceNode: 1,
					TargetNode: nodeIndex,
					Partitions: []string{"partition-1"},
				},
			},
			AckKeyPrefix: ackKeyPrefix,
		}

		// Put the migration event in etcd
		migrationKey := etcdkeys.MigrationRequestKeyPrefix(conf) + migrationID
		migrationValue, err := jsonrs.Marshal(migration)
		require.NoError(t, err)

		_, err = etcdResource.Client.Put(ctx, migrationKey, string(migrationValue))
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
		var ack etcdtypes.PartitionMigrationAck
		err = jsonrs.Unmarshal(resp.Kvs[0].Value, &ack)
		require.NoError(t, err)
		require.Equal(t, nodeIndex, ack.NodeIndex)
		require.Equal(t, nodeName, ack.NodeName)

		migrator.Stop()

		// Verify method calls on mocks
		// NewTargetMigration should be called 3 times (2 failures + 1 success)
		require.Equal(t, 1, sourceMigrator.runCalls, "source migrator Run should be called once")
		require.Equal(t, 0, sourceMigrator.handleCalls, "source migrator NewSourceMigration should not be called (node is only target)")
		require.Equal(t, 1, targetMigrator.runCalls, "target migrator Run should be called once")
		require.Equal(t, 3, targetMigrator.handleCalls, "target migrator NewTargetMigration should be called 3 times (2 failures + 1 success)")
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
		nodeName := "test-node-0"

		// Create mock migrators
		sourceMigrator := &mockSourceMigrator{}
		targetMigrator := &mockTargetMigrator{}

		// Wrap the etcd client to simulate errors initially
		// The Get call (used by etcdwatcher to get initial state) will fail 3 times then succeed
		mockClient := &mockEtcdClient{
			Client:       etcdResource.Client,
			getFailCount: 3,
			getErr:       fmt.Errorf("etcd get error: connection refused"),
		}

		// Build and start the migrator with the mock client
		migrator, err := NewProcessorPartitionMigratorBuilder(nodeIndex, nodeName).
			WithConfig(conf).
			WithEtcdClient(mockClient).
			WithSourceMigrator(sourceMigrator).
			WithTargetMigrator(targetMigrator).
			Build()
		require.NoError(t, err)

		err = migrator.Start()
		require.NoError(t, err)
		defer migrator.Stop()

		// Create a migration where this node is the source
		migrationID := rand.String(10)
		ackKeyPrefix := "/" + namespace + "/migration/ack/" + migrationID
		migration := &etcdtypes.PartitionMigration{
			ID:     migrationID,
			Status: etcdtypes.PartitionMigrationStatusNew,
			Jobs: []*etcdtypes.PartitionMigrationJobHeader{
				{
					JobID:      "job-1",
					SourceNode: nodeIndex,
					TargetNode: 1,
					Partitions: []string{"partition-1"},
				},
			},
			AckKeyPrefix: ackKeyPrefix,
		}

		// Put the migration event in etcd (use the real client to ensure data is there)
		migrationKey := etcdkeys.MigrationRequestKeyPrefix(conf) + migrationID
		migrationValue, err := jsonrs.Marshal(migration)
		require.NoError(t, err)

		_, err = etcdResource.Client.Put(ctx, migrationKey, string(migrationValue))
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
		var ack etcdtypes.PartitionMigrationAck
		err = jsonrs.Unmarshal(resp.Kvs[0].Value, &ack)
		require.NoError(t, err)
		require.Equal(t, nodeIndex, ack.NodeIndex)
		require.Equal(t, nodeName, ack.NodeName)

		migrator.Stop()

		// Verify the mock client was called multiple times (at least 3 times: 2 failures + 1 success)
		require.GreaterOrEqual(t, mockClient.getCount, 3, "etcd Get should be called at least 3 times (2 failures + successful calls)")

		// Verify migrators were called successfully after recovery
		require.Equal(t, 1, sourceMigrator.runCalls, "source migrator Run should be called once")
		require.Equal(t, 1, sourceMigrator.handleCalls, "source migrator NewSourceMigration should be called once")
		require.Equal(t, 1, targetMigrator.runCalls, "target migrator Run should be called once")
		require.Equal(t, 0, targetMigrator.handleCalls, "target migrator NewTargetMigration should not be called (node is only source)")
	})
}

type mockSourceMigrator struct {
	runCalls int
	runFn    func(ctx context.Context, wg *errgroup.Group) error
	runError error

	handleCalls int
	handleFn    func(ctx context.Context, migration *etcdtypes.PartitionMigration) error
	handleError error
}

func (msm *mockSourceMigrator) Run(ctx context.Context, wg *errgroup.Group) error {
	msm.runCalls++
	if msm.runFn != nil {
		return msm.runFn(ctx, wg)
	}
	return msm.runError
}

func (msm *mockSourceMigrator) Handle(ctx context.Context, migration *etcdtypes.PartitionMigration) error {
	msm.handleCalls++
	if msm.handleFn != nil {
		return msm.handleFn(ctx, migration)
	}
	return msm.handleError
}

type mockTargetMigrator struct {
	runCalls    int
	runFn       func(ctx context.Context, wg *errgroup.Group) error
	runError    error
	handleCalls int
	handleFn    func(ctx context.Context, migration *etcdtypes.PartitionMigration) error
	handleError error
}

func (mtm *mockTargetMigrator) Run(ctx context.Context, wg *errgroup.Group) error {
	mtm.runCalls++
	if mtm.runFn != nil {
		return mtm.runFn(ctx, wg)
	}
	return mtm.runError
}

func (mtm *mockTargetMigrator) Handle(ctx context.Context, migration *etcdtypes.PartitionMigration) error {
	mtm.handleCalls++
	if mtm.handleFn != nil {
		return mtm.handleFn(ctx, migration)
	}
	return mtm.handleError
}

type mockEtcdClient struct {
	*clientv3.Client
	getCount     int
	getFailCount int // number of times Get should fail before succeeding (0 = always fail if getErr is set)
	getErr       error
}

func (mec *mockEtcdClient) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	mec.getCount++
	if mec.getErr != nil && (mec.getFailCount == 0 || mec.getCount <= mec.getFailCount) {
		return nil, mec.getErr
	}
	return mec.Client.Get(ctx, key, opts...)
}
