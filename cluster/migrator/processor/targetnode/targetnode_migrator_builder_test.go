package targetnode_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/cluster/migrator/processor/targetnode"
	"github.com/rudderlabs/rudder-server/cluster/partitionbuffer"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

func TestMigratorBuilder(t *testing.T) {
	t.Run("Build with all dependencies provided", func(t *testing.T) {
		migrator, err := targetnode.NewMigratorBuilder(0, "test-node").
			WithConfig(config.New()).
			WithLogger(logger.NOP).
			WithStats(stats.NOP).
			WithEtcdClient(&mockEtcdClient{}).
			WithBufferedJobsDBs([][]partitionbuffer.JobsDBPartitionBuffer{
				{&mockBufferedJobsDB{identifier: "gw"}},
			}).
			WithUnbufferedJobsDBs([]jobsdb.JobsDB{&mockJobsDB{identifier: "gw"}}).
			Build()

		require.NoError(t, err)
		require.NotNil(t, migrator)
	})

	t.Run("Build with minimum required dependencies", func(t *testing.T) {
		// config, logger, and stats should default when not provided
		migrator, err := targetnode.NewMigratorBuilder(1, "test-node-1").
			WithEtcdClient(&mockEtcdClient{}).
			WithBufferedJobsDBs([][]partitionbuffer.JobsDBPartitionBuffer{
				{&mockBufferedJobsDB{identifier: "gw"}},
			}).
			WithUnbufferedJobsDBs([]jobsdb.JobsDB{&mockJobsDB{identifier: "gw"}}).
			Build()

		require.NoError(t, err)
		require.NotNil(t, migrator)
	})

	t.Run("Build fails without etcd client", func(t *testing.T) {
		_, err := targetnode.NewMigratorBuilder(0, "test-node").
			WithBufferedJobsDBs([][]partitionbuffer.JobsDBPartitionBuffer{
				{&mockBufferedJobsDB{identifier: "gw"}},
			}).
			WithUnbufferedJobsDBs([]jobsdb.JobsDB{&mockJobsDB{identifier: "gw"}}).
			Build()

		require.Error(t, err)
		require.Contains(t, err.Error(), "etcd client not provided")
	})

	t.Run("Build fails without buffered jobsdbs", func(t *testing.T) {
		_, err := targetnode.NewMigratorBuilder(0, "test-node").
			WithEtcdClient(&mockEtcdClient{}).
			WithUnbufferedJobsDBs([]jobsdb.JobsDB{&mockJobsDB{identifier: "gw"}}).
			Build()

		require.Error(t, err)
		require.Contains(t, err.Error(), "buffered jobsdbs not provided")
	})

	t.Run("Build fails without unbuffered jobsdbs", func(t *testing.T) {
		_, err := targetnode.NewMigratorBuilder(0, "test-node").
			WithEtcdClient(&mockEtcdClient{}).
			WithBufferedJobsDBs([][]partitionbuffer.JobsDBPartitionBuffer{
				{&mockBufferedJobsDB{identifier: "gw"}},
			}).
			Build()

		require.Error(t, err)
		require.Contains(t, err.Error(), "unbuffered jobsdbs not provided")
	})

	t.Run("Build fails with empty buffered jobsdbs group", func(t *testing.T) {
		_, err := targetnode.NewMigratorBuilder(0, "test-node").
			WithEtcdClient(&mockEtcdClient{}).
			WithBufferedJobsDBs([][]partitionbuffer.JobsDBPartitionBuffer{
				{}, // empty group
			}).
			WithUnbufferedJobsDBs([]jobsdb.JobsDB{&mockJobsDB{identifier: "gw"}}).
			Build()

		require.Error(t, err)
		require.Contains(t, err.Error(), "buffered jobsdbs group 0 is empty")
	})

	t.Run("Build fails when buffered jobsdb has no matching unbuffered", func(t *testing.T) {
		_, err := targetnode.NewMigratorBuilder(0, "test-node").
			WithEtcdClient(&mockEtcdClient{}).
			WithBufferedJobsDBs([][]partitionbuffer.JobsDBPartitionBuffer{
				{&mockBufferedJobsDB{identifier: "gw"}},
			}).
			WithUnbufferedJobsDBs([]jobsdb.JobsDB{&mockJobsDB{identifier: "rt"}}). // different identifier
			Build()

		require.Error(t, err)
		require.Contains(t, err.Error(), "buffered jobsdb \"gw\" has no matching unbuffered jobsdb")
	})

	t.Run("Build fails when buffered and unbuffered counts mismatch", func(t *testing.T) {
		_, err := targetnode.NewMigratorBuilder(0, "test-node").
			WithEtcdClient(&mockEtcdClient{}).
			WithBufferedJobsDBs([][]partitionbuffer.JobsDBPartitionBuffer{
				{&mockBufferedJobsDB{identifier: "gw"}},
			}).
			WithUnbufferedJobsDBs([]jobsdb.JobsDB{
				&mockJobsDB{identifier: "gw"},
				&mockJobsDB{identifier: "rt"},
			}).
			Build()

		require.Error(t, err)
		require.Contains(t, err.Error(), "buffered and unbuffered jobsdbs count mismatch")
	})

	t.Run("Build fails when unbuffered jobsdb implements JobsDBPartitionBuffer", func(t *testing.T) {
		_, err := targetnode.NewMigratorBuilder(0, "test-node").
			WithEtcdClient(&mockEtcdClient{}).
			WithBufferedJobsDBs([][]partitionbuffer.JobsDBPartitionBuffer{
				{&mockBufferedJobsDB{identifier: "gw"}},
			}).
			WithUnbufferedJobsDBs([]jobsdb.JobsDB{&mockBufferedJobsDB{identifier: "gw"}}). // buffered passed as unbuffered
			Build()

		require.Error(t, err)
		require.Contains(t, err.Error(), "unbuffered jobsdb \"gw\" should not implement the JobsDBPartitionBuffer interface")
	})

	t.Run("Build succeeds with multiple buffered jobsdbs groups", func(t *testing.T) {
		migrator, err := targetnode.NewMigratorBuilder(0, "test-node").
			WithEtcdClient(&mockEtcdClient{}).
			WithBufferedJobsDBs([][]partitionbuffer.JobsDBPartitionBuffer{
				{&mockBufferedJobsDB{identifier: "gw"}},
				{&mockBufferedJobsDB{identifier: "rt"}},
			}).
			WithUnbufferedJobsDBs([]jobsdb.JobsDB{
				&mockJobsDB{identifier: "gw"},
				&mockJobsDB{identifier: "rt"},
			}).
			Build()

		require.NoError(t, err)
		require.NotNil(t, migrator)
	})

	t.Run("Builder methods return builder for chaining", func(t *testing.T) {
		builder := targetnode.NewMigratorBuilder(0, "test-node")

		// Each method should return the same builder for chaining
		b1 := builder.WithConfig(config.New())
		require.Same(t, builder, b1)

		b2 := builder.WithLogger(logger.NOP)
		require.Same(t, builder, b2)

		b3 := builder.WithStats(stats.NOP)
		require.Same(t, builder, b3)

		b4 := builder.WithEtcdClient(&mockEtcdClient{})
		require.Same(t, builder, b4)

		b5 := builder.WithBufferedJobsDBs([][]partitionbuffer.JobsDBPartitionBuffer{
			{&mockBufferedJobsDB{identifier: "gw"}},
		})
		require.Same(t, builder, b5)

		b6 := builder.WithUnbufferedJobsDBs([]jobsdb.JobsDB{&mockJobsDB{identifier: "gw"}})
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

type mockJobsDB struct {
	jobsdb.JobsDB
	identifier string
}

func (m *mockJobsDB) Identifier() string {
	return m.identifier
}

type mockBufferedJobsDB struct {
	jobsdb.JobsDB
	identifier string
}

func (m *mockBufferedJobsDB) Identifier() string {
	return m.identifier
}

func (m *mockBufferedJobsDB) BufferPartitions(ctx context.Context, partitionIds []string) error {
	return nil
}

func (m *mockBufferedJobsDB) RefreshBufferedPartitions(ctx context.Context) error {
	return nil
}

func (m *mockBufferedJobsDB) FlushBufferedPartitions(ctx context.Context, partitionIds []string) error {
	return nil
}
