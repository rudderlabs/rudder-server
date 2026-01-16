package processor_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	etcdtypes "github.com/rudderlabs/rudder-schemas/go/cluster"
	"github.com/rudderlabs/rudder-server/cluster/migrator/processor"
)

func TestProcessorPartitionMigratorBuilder(t *testing.T) {
	t.Run("Build with all dependencies provided", func(t *testing.T) {
		migrator, err := processor.NewProcessorPartitionMigratorBuilder(0, "test-node").
			WithConfig(config.New()).
			WithLogger(logger.NOP).
			WithStats(stats.NOP).
			WithEtcdClient(&mockEtcdClient{}).
			WithSourceMigrator(&mockSourceMigrator{}).
			WithTargetMigrator(&mockTargetMigrator{}).
			Build()

		require.NoError(t, err)
		require.NotNil(t, migrator)
	})

	t.Run("Build with minimum required dependencies", func(t *testing.T) {
		// config, logger, and stats should default when not provided
		migrator, err := processor.NewProcessorPartitionMigratorBuilder(1, "test-node-1").
			WithEtcdClient(&mockEtcdClient{}).
			WithSourceMigrator(&mockSourceMigrator{}).
			WithTargetMigrator(&mockTargetMigrator{}).
			Build()

		require.NoError(t, err)
		require.NotNil(t, migrator)
	})

	t.Run("Build fails without etcd client", func(t *testing.T) {
		_, err := processor.NewProcessorPartitionMigratorBuilder(0, "test-node").
			WithSourceMigrator(&mockSourceMigrator{}).
			WithTargetMigrator(&mockTargetMigrator{}).
			Build()

		require.Error(t, err)
		require.Contains(t, err.Error(), "etcd client not provided")
	})

	t.Run("Build fails without source migrator", func(t *testing.T) {
		_, err := processor.NewProcessorPartitionMigratorBuilder(0, "test-node").
			WithEtcdClient(&mockEtcdClient{}).
			WithTargetMigrator(&mockTargetMigrator{}).
			Build()

		require.Error(t, err)
		require.Contains(t, err.Error(), "source migrator not provided")
	})

	t.Run("Build fails without target migrator", func(t *testing.T) {
		_, err := processor.NewProcessorPartitionMigratorBuilder(0, "test-node").
			WithEtcdClient(&mockEtcdClient{}).
			WithSourceMigrator(&mockSourceMigrator{}).
			Build()

		require.Error(t, err)
		require.Contains(t, err.Error(), "target migrator not provided")
	})

	t.Run("Builder methods return builder for chaining", func(t *testing.T) {
		builder := processor.NewProcessorPartitionMigratorBuilder(0, "test-node")

		// Each method should return the same builder for chaining
		b1 := builder.WithConfig(config.New())
		require.Same(t, builder, b1)

		b2 := builder.WithLogger(logger.NOP)
		require.Same(t, builder, b2)

		b3 := builder.WithStats(stats.NOP)
		require.Same(t, builder, b3)

		b4 := builder.WithEtcdClient(&mockEtcdClient{})
		require.Same(t, builder, b4)

		b5 := builder.WithSourceMigrator(&mockSourceMigrator{})
		require.Same(t, builder, b5)

		b6 := builder.WithTargetMigrator(&mockTargetMigrator{})
		require.Same(t, builder, b6)
	})
}

type mockEtcdClient struct{}

func (m *mockEtcdClient) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return &clientv3.GetResponse{}, nil
}

func (m *mockEtcdClient) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	return nil
}

func (m *mockEtcdClient) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	return &clientv3.PutResponse{}, nil
}

type mockSourceMigrator struct{}

func (m *mockSourceMigrator) Run(ctx context.Context, wg *errgroup.Group) error {
	return nil
}

func (m *mockSourceMigrator) Handle(ctx context.Context, migration *etcdtypes.PartitionMigration) error {
	return nil
}

type mockTargetMigrator struct{}

func (m *mockTargetMigrator) Run(ctx context.Context, wg *errgroup.Group) error {
	return nil
}

func (m *mockTargetMigrator) Handle(ctx context.Context, migration *etcdtypes.PartitionMigration) error {
	return nil
}
