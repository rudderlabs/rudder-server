package migrator_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	migrator "github.com/rudderlabs/rudder-server/cluster/migrator/gateway"
)

func TestGatewayPartitionMigratorBuilder(t *testing.T) {
	t.Run("Build with all dependencies provided", func(t *testing.T) {
		m, err := migrator.NewGatewayPartitionMigratorBuilder(0, "test-node").
			WithConfig(config.New()).
			WithLogger(logger.NOP).
			WithStats(stats.NOP).
			WithEtcdClient(&mockEtcdClient{}).
			WithPartitionRefresher(&mockPartitionRefresher{}).
			Build()

		require.NoError(t, err)
		require.NotNil(t, m)
	})

	t.Run("Build with minimum required dependencies", func(t *testing.T) {
		// config, logger, and stats should default when not provided
		m, err := migrator.NewGatewayPartitionMigratorBuilder(1, "test-node-1").
			WithEtcdClient(&mockEtcdClient{}).
			WithPartitionRefresher(&mockPartitionRefresher{}).
			Build()

		require.NoError(t, err)
		require.NotNil(t, m)
	})

	t.Run("Build fails without etcd client", func(t *testing.T) {
		_, err := migrator.NewGatewayPartitionMigratorBuilder(0, "test-node").
			WithPartitionRefresher(&mockPartitionRefresher{}).
			Build()

		require.Error(t, err)
		require.Contains(t, err.Error(), "etcd client not provided")
	})

	t.Run("Build fails without partition refresher", func(t *testing.T) {
		_, err := migrator.NewGatewayPartitionMigratorBuilder(0, "test-node").
			WithEtcdClient(&mockEtcdClient{}).
			Build()

		require.Error(t, err)
		require.Contains(t, err.Error(), "partition refresher not provided")
	})

	t.Run("Builder methods return builder for chaining", func(t *testing.T) {
		builder := migrator.NewGatewayPartitionMigratorBuilder(0, "test-node")

		// Each method should return the same builder for chaining
		b1 := builder.WithConfig(config.New())
		require.Same(t, builder, b1)

		b2 := builder.WithLogger(logger.NOP)
		require.Same(t, builder, b2)

		b3 := builder.WithStats(stats.NOP)
		require.Same(t, builder, b3)

		b4 := builder.WithEtcdClient(&mockEtcdClient{})
		require.Same(t, builder, b4)

		b5 := builder.WithPartitionRefresher(&mockPartitionRefresher{})
		require.Same(t, builder, b5)
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

func (m *mockEtcdClient) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return &clientv3.DeleteResponse{}, nil
}

func (m *mockEtcdClient) Txn(ctx context.Context) clientv3.Txn {
	return nil
}

type mockPartitionRefresher struct{}

func (m *mockPartitionRefresher) RefreshBufferedPartitions(ctx context.Context) error {
	return nil
}
