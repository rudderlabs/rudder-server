package targetnode

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/etcd"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	etcdtypes "github.com/rudderlabs/rudder-schemas/go/cluster"
	"github.com/rudderlabs/rudder-server/cluster/migrator/partitionmigration/client"
	"github.com/rudderlabs/rudder-server/cluster/partitionbuffer"
	"github.com/rudderlabs/rudder-server/jobsdb"
	sqlmigrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
)

func TestMigrator(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	etcdResource, err := etcd.Setup(pool, t)
	require.NoError(t, err)

	type handleEnv struct {
		db *sql.DB

		gwJobsDB    jobsdb.JobsDB
		gwBufJobsDB jobsdb.JobsDB
		gwBuffer    partitionbuffer.JobsDBPartitionBuffer

		rtJobsDB    jobsdb.JobsDB
		rtBufJobsDB jobsdb.JobsDB
		rtBuffer    partitionbuffer.JobsDBPartitionBuffer

		conf       *config.Config
		nodeIndex  int
		namespace  string
		etcdClient *clientv3.Client
	}

	newHandleEnv := func(t *testing.T) *handleEnv {
		t.Helper()
		pg, err := postgres.Setup(pool, t)
		require.NoError(t, err)
		runNodeMigration(t, pg.DB)

		namespace := rand.String(10)
		conf := config.New()
		conf.Set("WORKSPACE_NAMESPACE", namespace)

		// Create primary gw jobsdb
		gwJobsDB := jobsdb.NewForReadWrite("gw",
			jobsdb.WithDBHandle(pg.DB),
			jobsdb.WithNumPartitions(64),
			jobsdb.WithSkipMaintenanceErr(true),
			jobsdb.WithConfig(conf),
		)
		require.NoError(t, gwJobsDB.Start())
		t.Cleanup(gwJobsDB.Stop)

		// Create buffer gw jobsdb
		gwBufJobsDB := jobsdb.NewForReadWrite("gw_buf",
			jobsdb.WithDBHandle(pg.DB),
			jobsdb.WithNumPartitions(64),
			jobsdb.WithSkipMaintenanceErr(true),
			jobsdb.WithConfig(conf),
		)
		require.NoError(t, gwBufJobsDB.Start())
		t.Cleanup(gwBufJobsDB.Stop)

		// Create gw partition buffer
		gwBuffer, err := partitionbuffer.NewJobsDBPartitionBuffer(t.Context(),
			partitionbuffer.WithReadWriteJobsDBs(gwJobsDB, gwBufJobsDB),
		)
		require.NoError(t, err)

		// Create primary rt jobsdb
		rtJobsDB := jobsdb.NewForReadWrite("rt",
			jobsdb.WithDBHandle(pg.DB),
			jobsdb.WithNumPartitions(64),
			jobsdb.WithSkipMaintenanceErr(true),
			jobsdb.WithConfig(conf),
		)
		require.NoError(t, rtJobsDB.Start())
		t.Cleanup(rtJobsDB.Stop)

		// Create buffer rt jobsdb
		rtBufJobsDB := jobsdb.NewForReadWrite("rt_buf",
			jobsdb.WithDBHandle(pg.DB),
			jobsdb.WithNumPartitions(64),
			jobsdb.WithSkipMaintenanceErr(true),
			jobsdb.WithConfig(conf),
		)
		require.NoError(t, rtBufJobsDB.Start())
		t.Cleanup(rtBufJobsDB.Stop)

		// Create rt partition buffer
		rtBuffer, err := partitionbuffer.NewJobsDBPartitionBuffer(t.Context(),
			partitionbuffer.WithReadWriteJobsDBs(rtJobsDB, rtBufJobsDB),
		)
		require.NoError(t, err)

		return &handleEnv{
			db:          pg.DB,
			gwJobsDB:    gwJobsDB,
			gwBufJobsDB: gwBufJobsDB,
			gwBuffer:    gwBuffer,
			rtJobsDB:    rtJobsDB,
			rtBufJobsDB: rtBufJobsDB,
			rtBuffer:    rtBuffer,
			conf:        conf,
			nodeIndex:   1, // target node
			namespace:   namespace,
			etcdClient:  etcdResource.Client,
		}
	}

	generateJobs := func(t *testing.T, partitionID string, count int) []*jobsdb.JobT {
		t.Helper()
		jobs := make([]*jobsdb.JobT, 0, count)
		for range count {
			jobs = append(jobs, &jobsdb.JobT{
				UUID:         uuid.New(),
				UserID:       "user-1",
				CustomVal:    "test",
				EventCount:   1,
				EventPayload: []byte(`{"key": "value"}`),
				Parameters:   []byte(`{"source_id":"source-1", "destination_id":"dest-1"}`),
				WorkspaceId:  "workspace-1",
				PartitionID:  partitionID,
			})
		}
		return jobs
	}

	t.Run("Handle", func(t *testing.T) {
		t.Run("success", func(t *testing.T) {
			env := newHandleEnv(t)

			// Create a migration job where this node (nodeIndex=1) is the target
			migration := &etcdtypes.PartitionMigration{
				ID:     "migration-1",
				Status: etcdtypes.PartitionMigrationStatusNew,
				Jobs: []*etcdtypes.PartitionMigrationJobHeader{
					{
						JobID:      "job-1",
						SourceNode: 0,
						TargetNode: env.nodeIndex,
						Partitions: []string{"partition-1", "partition-2"},
					},
				},
			}

			// Create migrator
			m, err := NewMigratorBuilder(env.nodeIndex, "test-target-node").
				WithConfig(env.conf).
				WithEtcdClient(env.etcdClient).
				WithBufferedJobsDBs([][]partitionbuffer.JobsDBPartitionBuffer{
					{env.rtBuffer},
					{env.gwBuffer},
				}).
				WithUnbufferedJobsDBs([]jobsdb.JobsDB{env.gwJobsDB, env.rtJobsDB}).
				Build()
			require.NoError(t, err)

			// Call Handle
			err = m.Handle(t.Context(), migration)
			require.NoError(t, err)

			// Verify that partitions are in the buffered list for both buffers
			gwBufferedPartitions := getBufferedPartitions(t, env.db, "gw")
			require.ElementsMatch(t, []string{"partition-1", "partition-2"}, gwBufferedPartitions)

			rtBufferedPartitions := getBufferedPartitions(t, env.db, "rt")
			require.ElementsMatch(t, []string{"partition-1", "partition-2"}, rtBufferedPartitions)

			// Store some jobs via the partition buffer - they should go to the buffer db
			require.NoError(t, env.gwBuffer.Store(t.Context(), generateJobs(t, "partition-1", 10)))
			require.NoError(t, env.rtBuffer.Store(t.Context(), generateJobs(t, "partition-2", 10)))
			require.NoError(t, env.gwBuffer.Store(t.Context(), generateJobs(t, "partition-3", 10)))
			require.NoError(t, env.rtBuffer.Store(t.Context(), generateJobs(t, "partition-3", 10)))

			// Verify that jobs are added to the buffer jobsdb (not the primary)
			gwBufCount := countJobsInTable(t, env.db, "gw_buf", "partition-1")
			require.Equal(t, 10, gwBufCount, "jobs for partition-1 should be in gw buffer")

			rtBufCount := countJobsInTable(t, env.db, "rt_buf", "partition-2")
			require.Equal(t, 10, rtBufCount, "jobs for partition-2 should be in rt buffer")

			// Verify that primary jobsdbs have no jobs for these partitions
			gwPrimaryCount := countJobsInTable(t, env.db, "gw", "partition-1")
			require.Equal(t, 0, gwPrimaryCount, "no jobs should be in primary gw for partition-1")

			rtPrimaryCount := countJobsInTable(t, env.db, "rt", "partition-2")
			require.Equal(t, 0, rtPrimaryCount, "no jobs should be in primary rt for partition-2")
		})

		t.Run("buffer initial error", func(t *testing.T) {
			env := newHandleEnv(t)

			// Configure fast retry for testing
			env.conf.Set("PerpetualExponentialBackoff.minBackoff", "10ms")
			env.conf.Set("PerpetualExponentialBackoff.maxBackoff", "50ms")

			// Create a migration job where this node (nodeIndex=1) is the target
			migration := &etcdtypes.PartitionMigration{
				ID:     "migration-1",
				Status: etcdtypes.PartitionMigrationStatusNew,
				Jobs: []*etcdtypes.PartitionMigrationJobHeader{
					{
						JobID:      "job-1",
						SourceNode: 0,
						TargetNode: env.nodeIndex,
						Partitions: []string{"partition-1", "partition-2"},
					},
				},
			}

			// Create mock buffers that fail BufferPartitions initially
			bufferErr := errors.New("buffer error: connection refused")
			mockGwBuffer := &mockJobsDBPartitionBuffer{
				JobsDBPartitionBuffer:  env.gwBuffer,
				bufferPartitionsErr:    bufferErr,
				bufferPartitionsFailAt: 2, // fail first 2 calls
			}
			mockRtBuffer := &mockJobsDBPartitionBuffer{
				JobsDBPartitionBuffer:  env.rtBuffer,
				bufferPartitionsErr:    bufferErr,
				bufferPartitionsFailAt: 4, // fail first 4 calls
			}

			// Create migrator with the mock buffers
			m, err := NewMigratorBuilder(env.nodeIndex, "test-target-node").
				WithConfig(env.conf).
				WithEtcdClient(env.etcdClient).
				WithBufferedJobsDBs([][]partitionbuffer.JobsDBPartitionBuffer{
					{mockRtBuffer},
					{mockGwBuffer},
				}).
				WithUnbufferedJobsDBs([]jobsdb.JobsDB{env.gwJobsDB, env.rtJobsDB}).
				Build()
			require.NoError(t, err)

			// Call Handle - should succeed after retries
			require.Eventually(t, func() bool {
				err = m.Handle(t.Context(), migration)
				return err == nil
			}, 10*time.Second, 10*time.Millisecond, "Handle should succeed after retries")

			// Verify that the mock buffers were called multiple times (retries happened)
			require.GreaterOrEqual(t, int(mockGwBuffer.bufferPartitionsCount.Load()), 4,
				"gw buffer BufferPartitions should be called at least 4 times due to retries")
			require.GreaterOrEqual(t, int(mockRtBuffer.bufferPartitionsCount.Load()), 4,
				"rt buffer BufferPartitions should be called at least 4 times due to retries")

			// Verify that partitions are in the buffered list for both buffers
			gwBufferedPartitions := getBufferedPartitions(t, env.db, "gw")
			require.ElementsMatch(t, []string{"partition-1", "partition-2"}, gwBufferedPartitions)

			rtBufferedPartitions := getBufferedPartitions(t, env.db, "rt")
			require.ElementsMatch(t, []string{"partition-1", "partition-2"}, rtBufferedPartitions)
		})
	})

	// runEnv extends handleEnv with a source node environment for testing gRPC migration
	type runEnv struct {
		*handleEnv

		sourceDB       *sql.DB
		sourceGWJobsDB jobsdb.JobsDB
		sourceRTJobsDB jobsdb.JobsDB
	}

	newRunEnv := func(t *testing.T) *runEnv {
		t.Helper()
		env := newHandleEnv(t)

		// Setup source postgres
		sourcePG, err := postgres.Setup(pool, t)
		require.NoError(t, err)
		runNodeMigration(t, sourcePG.DB)

		// Create source gw jobsdb
		sourceGWJobsDB := jobsdb.NewForReadWrite("gw",
			jobsdb.WithDBHandle(sourcePG.DB),
			jobsdb.WithNumPartitions(64),
			jobsdb.WithSkipMaintenanceErr(true),
			jobsdb.WithConfig(env.conf),
		)
		require.NoError(t, sourceGWJobsDB.Start())
		t.Cleanup(sourceGWJobsDB.Stop)

		// Create source rt jobsdb
		sourceRTJobsDB := jobsdb.NewForReadWrite("rt",
			jobsdb.WithDBHandle(sourcePG.DB),
			jobsdb.WithNumPartitions(64),
			jobsdb.WithSkipMaintenanceErr(true),
			jobsdb.WithConfig(env.conf),
		)
		require.NoError(t, sourceRTJobsDB.Start())
		t.Cleanup(sourceRTJobsDB.Stop)

		return &runEnv{
			handleEnv:      env,
			sourceDB:       sourcePG.DB,
			sourceGWJobsDB: sourceGWJobsDB,
			sourceRTJobsDB: sourceRTJobsDB,
		}
	}

	t.Run("Run", func(t *testing.T) {
		t.Run("flush moved job", func(t *testing.T) {
			env := newHandleEnv(t)

			// Configure gRPC server port
			port, err := testhelper.GetFreePort()
			require.NoError(t, err)
			env.conf.Set("PartitionMigration.Grpc.Server.Port", port)

			// Mark the partitions as buffered using Handle
			migration := &etcdtypes.PartitionMigration{
				ID:     "migration-1",
				Status: etcdtypes.PartitionMigrationStatusNew,
				Jobs: []*etcdtypes.PartitionMigrationJobHeader{
					{
						JobID:      "job-1",
						SourceNode: 0,
						TargetNode: env.nodeIndex,
						Partitions: []string{"partition-1", "partition-2"},
					},
				},
			}

			// Create migrator
			m, err := NewMigratorBuilder(env.nodeIndex, "test-target-node").
				WithConfig(env.conf).
				WithEtcdClient(env.etcdClient).
				WithBufferedJobsDBs([][]partitionbuffer.JobsDBPartitionBuffer{
					{env.rtBuffer},
					{env.gwBuffer},
				}).
				WithUnbufferedJobsDBs([]jobsdb.JobsDB{env.gwJobsDB, env.rtJobsDB}).
				Build()
			require.NoError(t, err)

			// Call Handle to mark partitions as buffered
			err = m.Handle(t.Context(), migration)
			require.NoError(t, err)

			// Store jobs to the buffer (simulating jobs that arrived during migration)
			require.NoError(t, env.gwBuffer.Store(t.Context(), generateJobs(t, "partition-1", 50)))
			require.NoError(t, env.gwBuffer.Store(t.Context(), generateJobs(t, "partition-2", 50)))
			require.NoError(t, env.gwBuffer.Store(t.Context(), generateJobs(t, "partition-3", 50)))
			require.NoError(t, env.rtBuffer.Store(t.Context(), generateJobs(t, "partition-1", 50)))
			require.NoError(t, env.rtBuffer.Store(t.Context(), generateJobs(t, "partition-2", 50)))
			require.NoError(t, env.rtBuffer.Store(t.Context(), generateJobs(t, "partition-3", 50)))

			// Verify jobs are in buffer dbs
			require.Equal(t, 50, countJobsInTable(t, env.db, "gw_buf", "partition-1"))
			require.Equal(t, 50, countJobsInTable(t, env.db, "gw_buf", "partition-2"))
			require.Equal(t, 50, countJobsInTable(t, env.db, "gw", "partition-3"))
			require.Equal(t, 50, countJobsInTable(t, env.db, "rt_buf", "partition-1"))
			require.Equal(t, 50, countJobsInTable(t, env.db, "rt_buf", "partition-2"))
			require.Equal(t, 50, countJobsInTable(t, env.db, "rt", "partition-3"))

			// Start Run
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			wg, ctx := errgroup.WithContext(ctx)
			err = m.Run(ctx, wg)
			require.NoError(t, err)

			// Put a migration job in etcd with status "moved"
			migrationJob := &etcdtypes.PartitionMigrationJob{
				PartitionMigrationJobHeader: etcdtypes.PartitionMigrationJobHeader{
					JobID:      "job-1",
					SourceNode: 0,
					TargetNode: env.nodeIndex,
					Partitions: []string{"partition-1", "partition-2"},
				},
				MigrationID: "migration-1",
				Status:      etcdtypes.PartitionMigrationJobStatusMoved,
			}
			jobKey := "/" + env.namespace + "/migration/job/" + migrationJob.JobID
			jobValue, err := jsonrs.Marshal(migrationJob)
			require.NoError(t, err)
			_, err = env.etcdClient.Put(t.Context(), jobKey, string(jobValue))
			require.NoError(t, err)

			// Wait until the job is marked as "completed"
			require.Eventually(t, func() bool {
				resp, err := env.etcdClient.Get(t.Context(), jobKey)
				if err != nil || len(resp.Kvs) == 0 {
					return false
				}
				var job etcdtypes.PartitionMigrationJob
				if err := jsonrs.Unmarshal(resp.Kvs[0].Value, &job); err != nil {
					return false
				}
				return job.Status == etcdtypes.PartitionMigrationJobStatusCompleted
			}, 30*time.Second, 100*time.Millisecond, "job status should be marked as completed")

			// Cancel context to stop Run
			cancel()
			require.NoError(t, wg.Wait())

			// Verify that partitions are removed from the buffered list
			gwBufferedPartitions := getBufferedPartitions(t, env.db, "gw")
			require.Empty(t, gwBufferedPartitions, "gw buffered partitions should be empty after flush")

			rtBufferedPartitions := getBufferedPartitions(t, env.db, "rt")
			require.Empty(t, rtBufferedPartitions, "rt buffered partitions should be empty after flush")

			// Verify that jobs are flushed from buffer to primary
			require.Equal(t, 0, countJobsInTable(t, env.db, "gw_buf", "partition-1"), "gw_buf should be empty for partition-1")
			require.Equal(t, 0, countJobsInTable(t, env.db, "gw_buf", "partition-2"), "gw_buf should be empty for partition-2")
			require.Equal(t, 0, countJobsInTable(t, env.db, "rt_buf", "partition-1"), "rt_buf should be empty for partition-1")
			require.Equal(t, 0, countJobsInTable(t, env.db, "rt_buf", "partition-2"), "rt_buf should be empty for partition-2")

			// Verify that jobs are now in primary jobsdbs
			require.Equal(t, 50, countJobsInTable(t, env.db, "gw", "partition-1"), "gw should have jobs for partition-1")
			require.Equal(t, 50, countJobsInTable(t, env.db, "gw", "partition-2"), "gw should have jobs for partition-2")
			require.Equal(t, 50, countJobsInTable(t, env.db, "rt", "partition-1"), "rt should have jobs for partition-1")
			require.Equal(t, 50, countJobsInTable(t, env.db, "rt", "partition-2"), "rt should have jobs for partition-2")
		})

		t.Run("flush initial error", func(t *testing.T) {
			env := newHandleEnv(t)

			// Configure gRPC server port and fast retry
			port, err := testhelper.GetFreePort()
			require.NoError(t, err)
			env.conf.Set("PartitionMigration.Grpc.Server.Port", port)
			env.conf.Set("PerpetualExponentialBackoff.minBackoff", "10ms")
			env.conf.Set("PerpetualExponentialBackoff.maxBackoff", "50ms")

			// Mark the partitions as buffered using Handle
			migration := &etcdtypes.PartitionMigration{
				ID:     "migration-1",
				Status: etcdtypes.PartitionMigrationStatusNew,
				Jobs: []*etcdtypes.PartitionMigrationJobHeader{
					{
						JobID:      "job-1",
						SourceNode: 0,
						TargetNode: env.nodeIndex,
						Partitions: []string{"partition-1", "partition-2"},
					},
				},
			}

			// Create mock buffers that fail FlushBufferedPartitions initially
			// Both buffers are in the same group, so they're flushed concurrently
			// We fail both for the first 3 attempts, then they succeed together
			flushErr := errors.New("flush error: connection refused")
			mockGwBuffer := &mockJobsDBPartitionBuffer{
				JobsDBPartitionBuffer:         env.gwBuffer,
				flushBufferedPartitionsErr:    flushErr,
				flushBufferedPartitionsFailAt: 2, // fail first 2 calls
			}
			mockRtBuffer := &mockJobsDBPartitionBuffer{
				JobsDBPartitionBuffer:         env.rtBuffer,
				flushBufferedPartitionsErr:    flushErr,
				flushBufferedPartitionsFailAt: 2, // fail first 2 calls
			}

			// Create migrator with mock buffers - put both in the same group
			// so they are flushed concurrently and either both succeed or both fail
			m, err := NewMigratorBuilder(env.nodeIndex, "test-target-node").
				WithConfig(env.conf).
				WithEtcdClient(env.etcdClient).
				WithBufferedJobsDBs([][]partitionbuffer.JobsDBPartitionBuffer{
					{mockRtBuffer},
					{mockGwBuffer},
				}).
				WithUnbufferedJobsDBs([]jobsdb.JobsDB{env.gwJobsDB, env.rtJobsDB}).
				Build()
			require.NoError(t, err)

			// Call Handle to mark partitions as buffered
			err = m.Handle(t.Context(), migration)
			require.NoError(t, err)

			// Store jobs to the buffer
			require.NoError(t, env.gwBuffer.Store(t.Context(), generateJobs(t, "partition-1", 50)))
			require.NoError(t, env.rtBuffer.Store(t.Context(), generateJobs(t, "partition-1", 50)))

			// Start Run
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			wg, ctx := errgroup.WithContext(ctx)
			err = m.Run(ctx, wg)
			require.NoError(t, err)

			// Put a migration job in etcd with status "moved"
			migrationJob := &etcdtypes.PartitionMigrationJob{
				PartitionMigrationJobHeader: etcdtypes.PartitionMigrationJobHeader{
					JobID:      "job-1",
					SourceNode: 0,
					TargetNode: env.nodeIndex,
					Partitions: []string{"partition-1", "partition-2"},
				},
				MigrationID: "migration-1",
				Status:      etcdtypes.PartitionMigrationJobStatusMoved,
			}
			jobKey := "/" + env.namespace + "/migration/job/" + migrationJob.JobID
			jobValue, err := jsonrs.Marshal(migrationJob)
			require.NoError(t, err)
			_, err = env.etcdClient.Put(t.Context(), jobKey, string(jobValue))
			require.NoError(t, err)

			// Wait until the job is marked as "completed" (after retries succeed)
			require.Eventually(t, func() bool {
				resp, err := env.etcdClient.Get(t.Context(), jobKey)
				if err != nil || len(resp.Kvs) == 0 {
					return false
				}
				var job etcdtypes.PartitionMigrationJob
				if err := jsonrs.Unmarshal(resp.Kvs[0].Value, &job); err != nil {
					return false
				}
				return job.Status == etcdtypes.PartitionMigrationJobStatusCompleted
			}, 40*time.Second, 100*time.Millisecond, "job status should be marked as completed after retries")

			// Cancel context to stop Run
			cancel()
			require.NoError(t, wg.Wait())

			// Verify that the mock buffers were called multiple times (retries happened)
			require.GreaterOrEqual(t, int(mockRtBuffer.flushBufferedPartitionsCount.Load()), 3,
				"rt buffer FlushBufferedPartitions should be called at least 3 times due to retries")
			require.GreaterOrEqual(t, int(mockGwBuffer.flushBufferedPartitionsCount.Load()), 3,
				"gw buffer FlushBufferedPartitions should be called at least 3 times due to retries")

			// Verify that jobs are flushed successfully after retries
			require.Equal(t, 0, countJobsInTable(t, env.db, "gw_buf", "partition-1"), "gw_buf should be empty")
			require.Equal(t, 0, countJobsInTable(t, env.db, "rt_buf", "partition-1"), "rt_buf should be empty")
			require.Equal(t, 50, countJobsInTable(t, env.db, "gw", "partition-1"), "gw should have jobs")
			require.Equal(t, 50, countJobsInTable(t, env.db, "rt", "partition-1"), "rt should have jobs")
		})

		t.Run("etcd client initial error", func(t *testing.T) {
			// This test simulates etcd client errors during Run's interaction with etcd
			// using a mock etcd client that fails the first few calls
			// fail 2 Gets and 3 Puts
			env := newHandleEnv(t)

			// Configure gRPC server port and fast retry
			port, err := testhelper.GetFreePort()
			require.NoError(t, err)
			env.conf.Set("PartitionMigration.Grpc.Server.Port", port)
			env.conf.Set("PerpetualExponentialBackoff.minBackoff", "10ms")
			env.conf.Set("PerpetualExponentialBackoff.maxBackoff", "50ms")

			// Mark the partitions as buffered using Handle
			migration := &etcdtypes.PartitionMigration{
				ID:     "migration-1",
				Status: etcdtypes.PartitionMigrationStatusNew,
				Jobs: []*etcdtypes.PartitionMigrationJobHeader{
					{
						JobID:      "job-1",
						SourceNode: 0,
						TargetNode: env.nodeIndex,
						Partitions: []string{"partition-1", "partition-2"},
					},
				},
			}

			// Create mock etcd client that fails initially
			mockEtcdClient := &mockEtcdClient{
				Client:       env.etcdClient,
				getErr:       fmt.Errorf("get error"),
				getFailCount: 2,
				putErr:       fmt.Errorf("put error"),
				putFailCount: 3,
			}

			// Create migrator with mock buffers - put both in the same group
			// so they are flushed concurrently and either both succeed or both fail
			m, err := NewMigratorBuilder(env.nodeIndex, "test-target-node").
				WithConfig(env.conf).
				WithEtcdClient(mockEtcdClient).
				WithBufferedJobsDBs([][]partitionbuffer.JobsDBPartitionBuffer{
					{env.rtBuffer},
					{env.gwBuffer},
				}).
				WithUnbufferedJobsDBs([]jobsdb.JobsDB{env.gwJobsDB, env.rtJobsDB}).
				Build()
			require.NoError(t, err)

			// Call Handle to mark partitions as buffered
			err = m.Handle(t.Context(), migration)
			require.NoError(t, err)

			// Store jobs to the buffer
			require.NoError(t, env.gwBuffer.Store(t.Context(), generateJobs(t, "partition-1", 50)))
			require.NoError(t, env.rtBuffer.Store(t.Context(), generateJobs(t, "partition-1", 50)))

			// Start Run
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			wg, ctx := errgroup.WithContext(ctx)
			err = m.Run(ctx, wg)
			require.NoError(t, err)

			// Put a migration job in etcd with status "moved"
			migrationJob := &etcdtypes.PartitionMigrationJob{
				PartitionMigrationJobHeader: etcdtypes.PartitionMigrationJobHeader{
					JobID:      "job-1",
					SourceNode: 0,
					TargetNode: env.nodeIndex,
					Partitions: []string{"partition-1", "partition-2"},
				},
				MigrationID: "migration-1",
				Status:      etcdtypes.PartitionMigrationJobStatusMoved,
			}
			jobKey := "/" + env.namespace + "/migration/job/" + migrationJob.JobID
			jobValue, err := jsonrs.Marshal(migrationJob)
			require.NoError(t, err)
			_, err = env.etcdClient.Put(t.Context(), jobKey, string(jobValue))
			require.NoError(t, err)

			// Wait until the job is marked as "completed" (after retries succeed)
			require.Eventually(t, func() bool {
				resp, err := env.etcdClient.Get(t.Context(), jobKey)
				if err != nil || len(resp.Kvs) == 0 {
					return false
				}
				var job etcdtypes.PartitionMigrationJob
				if err := jsonrs.Unmarshal(resp.Kvs[0].Value, &job); err != nil {
					return false
				}
				return job.Status == etcdtypes.PartitionMigrationJobStatusCompleted
			}, 40*time.Second, 100*time.Millisecond, "job status should be marked as completed after retries")

			// Cancel context to stop Run
			cancel()
			require.NoError(t, wg.Wait())

			// Verify that the mock buffers were called multiple times (retries happened)
			require.Greater(t, int(mockEtcdClient.getCount.Load()), mockEtcdClient.getFailCount,
				"etcd client get should be called multiple times due to retries")
			require.GreaterOrEqual(t, int(mockEtcdClient.putCount.Load()), mockEtcdClient.putFailCount,
				"etcd client put should be called multiple times due to retries")

			// Verify that jobs are flushed successfully after retries
			require.Equal(t, 0, countJobsInTable(t, env.db, "gw_buf", "partition-1"), "gw_buf should be empty")
			require.Equal(t, 0, countJobsInTable(t, env.db, "rt_buf", "partition-1"), "rt_buf should be empty")
			require.Equal(t, 50, countJobsInTable(t, env.db, "gw", "partition-1"), "gw should have jobs")
			require.Equal(t, 50, countJobsInTable(t, env.db, "rt", "partition-1"), "rt should have jobs")
		})

		t.Run("grpc then flush", func(t *testing.T) {
			env := newRunEnv(t)

			// Configure gRPC server port
			port, err := testhelper.GetFreePort()
			require.NoError(t, err)
			env.conf.Set("PartitionMigration.Grpc.Server.Port", port)
			targetAddr := "127.0.0.1:" + strconv.Itoa(port)

			// Mark the partitions as buffered using Handle
			migration := &etcdtypes.PartitionMigration{
				ID:     "migration-1",
				Status: etcdtypes.PartitionMigrationStatusNew,
				Jobs: []*etcdtypes.PartitionMigrationJobHeader{
					{
						JobID:      "job-1",
						SourceNode: 0,
						TargetNode: env.nodeIndex,
						Partitions: []string{"partition-1", "partition-2"},
					},
				},
			}

			// Create migrator
			m, err := NewMigratorBuilder(env.nodeIndex, "test-target-node").
				WithConfig(env.conf).
				WithEtcdClient(env.etcdClient).
				WithBufferedJobsDBs([][]partitionbuffer.JobsDBPartitionBuffer{
					{env.rtBuffer},
					{env.gwBuffer},
				}).
				WithUnbufferedJobsDBs([]jobsdb.JobsDB{env.gwJobsDB, env.rtJobsDB}).
				Build()
			require.NoError(t, err)

			// Call Handle to mark partitions as buffered
			err = m.Handle(t.Context(), migration)
			require.NoError(t, err)

			// Store jobs to the buffer (simulating jobs that arrived during migration)
			require.NoError(t, env.gwBuffer.Store(t.Context(), generateJobs(t, "partition-1", 30)))
			require.NoError(t, env.gwBuffer.Store(t.Context(), generateJobs(t, "partition-2", 30)))
			require.NoError(t, env.rtBuffer.Store(t.Context(), generateJobs(t, "partition-1", 30)))
			require.NoError(t, env.rtBuffer.Store(t.Context(), generateJobs(t, "partition-2", 30)))

			// Store jobs in source node (to be migrated via gRPC)
			require.NoError(t, env.sourceGWJobsDB.Store(t.Context(), generateJobs(t, "partition-1", 50)))
			require.NoError(t, env.sourceGWJobsDB.Store(t.Context(), generateJobs(t, "partition-2", 50)))
			require.NoError(t, env.sourceRTJobsDB.Store(t.Context(), generateJobs(t, "partition-1", 50)))
			require.NoError(t, env.sourceRTJobsDB.Store(t.Context(), generateJobs(t, "partition-2", 50)))

			// Verify jobs are in buffer dbs (not primary)
			require.Equal(t, 30, countJobsInTable(t, env.db, "gw_buf", "partition-1"))
			require.Equal(t, 30, countJobsInTable(t, env.db, "gw_buf", "partition-2"))
			require.Equal(t, 0, countJobsInTable(t, env.db, "gw", "partition-1"))
			require.Equal(t, 0, countJobsInTable(t, env.db, "gw", "partition-2"))

			// Start Run (starts gRPC server)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			wg, ctx := errgroup.WithContext(ctx)
			err = m.Run(ctx, wg)
			require.NoError(t, err)

			// Give gRPC server a moment to start
			time.Sleep(200 * time.Millisecond)

			// Use MigrationJobExecutor to send jobs from source to target via gRPC
			gwExecutor := client.NewMigrationJobExecutor(
				"job-1",
				0, // source node index
				[]string{"partition-1", "partition-2"},
				env.sourceGWJobsDB,
				targetAddr,
				client.WithConfig(env.conf),
			)
			rtExecutor := client.NewMigrationJobExecutor(
				"job-1",
				0, // source node index
				[]string{"partition-1", "partition-2"},
				env.sourceRTJobsDB,
				targetAddr,
				client.WithConfig(env.conf),
			)

			// Run executors to send jobs via gRPC
			execWg, execCtx := errgroup.WithContext(ctx)
			execWg.Go(func() error {
				return gwExecutor.Run(execCtx)
			})
			execWg.Go(func() error {
				return rtExecutor.Run(execCtx)
			})
			require.NoError(t, execWg.Wait(), "executors should complete successfully")

			// Verify that jobs sent via gRPC are in primary jobsdbs (not buffer)
			require.Equal(t, 50, countJobsInTable(t, env.db, "gw", "partition-1"), "gRPC jobs should be in primary gw")
			require.Equal(t, 50, countJobsInTable(t, env.db, "gw", "partition-2"), "gRPC jobs should be in primary gw")
			require.Equal(t, 50, countJobsInTable(t, env.db, "rt", "partition-1"), "gRPC jobs should be in primary rt")
			require.Equal(t, 50, countJobsInTable(t, env.db, "rt", "partition-2"), "gRPC jobs should be in primary rt")

			// Verify that buffer still has the buffered jobs
			require.Equal(t, 30, countJobsInTable(t, env.db, "gw_buf", "partition-1"), "buffer should still have jobs")
			require.Equal(t, 30, countJobsInTable(t, env.db, "gw_buf", "partition-2"), "buffer should still have jobs")

			// Put migration job in etcd with status "moved"
			migrationJob := &etcdtypes.PartitionMigrationJob{
				PartitionMigrationJobHeader: etcdtypes.PartitionMigrationJobHeader{
					JobID:      "job-1",
					SourceNode: 0,
					TargetNode: env.nodeIndex,
					Partitions: []string{"partition-1", "partition-2"},
				},
				MigrationID: "migration-1",
				Status:      etcdtypes.PartitionMigrationJobStatusMoved,
			}
			jobKey := "/" + env.namespace + "/migration/job/" + migrationJob.JobID
			jobValue, err := jsonrs.Marshal(migrationJob)
			require.NoError(t, err)
			_, err = env.etcdClient.Put(t.Context(), jobKey, string(jobValue))
			require.NoError(t, err)

			// Wait until the job is marked as "completed"
			require.Eventually(t, func() bool {
				resp, err := env.etcdClient.Get(t.Context(), jobKey)
				if err != nil || len(resp.Kvs) == 0 {
					return false
				}
				var job etcdtypes.PartitionMigrationJob
				if err := jsonrs.Unmarshal(resp.Kvs[0].Value, &job); err != nil {
					return false
				}
				return job.Status == etcdtypes.PartitionMigrationJobStatusCompleted
			}, 30*time.Second, 100*time.Millisecond, "job status should be marked as completed")

			// Cancel context to stop Run
			cancel()
			require.NoError(t, wg.Wait())

			// Verify that partitions are removed from the buffered list
			gwBufferedPartitions := getBufferedPartitions(t, env.db, "gw")
			require.Empty(t, gwBufferedPartitions, "gw buffered partitions should be empty after flush")

			rtBufferedPartitions := getBufferedPartitions(t, env.db, "rt")
			require.Empty(t, rtBufferedPartitions, "rt buffered partitions should be empty after flush")

			// Verify that buffer is flushed to primary (buffer empty, primary has all jobs)
			require.Equal(t, 0, countJobsInTable(t, env.db, "gw_buf", "partition-1"), "gw_buf should be empty")
			require.Equal(t, 0, countJobsInTable(t, env.db, "gw_buf", "partition-2"), "gw_buf should be empty")
			require.Equal(t, 0, countJobsInTable(t, env.db, "rt_buf", "partition-1"), "rt_buf should be empty")
			require.Equal(t, 0, countJobsInTable(t, env.db, "rt_buf", "partition-2"), "rt_buf should be empty")

			// Primary should have both gRPC jobs (50) and flushed buffer jobs (30) = 80 total
			require.Equal(t, 80, countJobsInTable(t, env.db, "gw", "partition-1"), "gw should have all jobs")
			require.Equal(t, 80, countJobsInTable(t, env.db, "gw", "partition-2"), "gw should have all jobs")
			require.Equal(t, 80, countJobsInTable(t, env.db, "rt", "partition-1"), "rt should have all jobs")
			require.Equal(t, 80, countJobsInTable(t, env.db, "rt", "partition-2"), "rt should have all jobs")

			// Verify that source jobs are marked as migrated (no unprocessed jobs left)
			require.Equal(t, 0, countJobsInTable(t, env.sourceDB, "gw", "partition-1"), "source gw should have no unprocessed jobs")
			require.Equal(t, 0, countJobsInTable(t, env.sourceDB, "gw", "partition-2"), "source gw should have no unprocessed jobs")
			require.Equal(t, 0, countJobsInTable(t, env.sourceDB, "rt", "partition-1"), "source rt should have no unprocessed jobs")
			require.Equal(t, 0, countJobsInTable(t, env.sourceDB, "rt", "partition-2"), "source rt should have no unprocessed jobs")
		})

		t.Run("grpc cannot start", func(t *testing.T) {
			env := newHandleEnv(t)

			// Get a free port and start a listener on it to block the gRPC server
			port, err := testhelper.GetFreePort()
			require.NoError(t, err)
			env.conf.Set("PartitionMigration.Grpc.Server.Port", port)

			// Start a listener on the same port to block gRPC server startup
			listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
			require.NoError(t, err)
			defer listener.Close()

			// Create migrator
			m, err := NewMigratorBuilder(env.nodeIndex, "test-target-node").
				WithConfig(env.conf).
				WithEtcdClient(env.etcdClient).
				WithBufferedJobsDBs([][]partitionbuffer.JobsDBPartitionBuffer{
					{env.rtBuffer},
					{env.gwBuffer},
				}).
				WithUnbufferedJobsDBs([]jobsdb.JobsDB{env.gwJobsDB, env.rtJobsDB}).
				Build()
			require.NoError(t, err)

			// Call Run - should return an error because gRPC server cannot start
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			wg, ctx := errgroup.WithContext(ctx)
			err = m.Run(ctx, wg)
			require.Error(t, err)
			require.Contains(t, err.Error(), "starting gRPC server")
		})
	})
}

// getBufferedPartitions retrieves the buffered partition IDs from the database
func getBufferedPartitions(t *testing.T, db *sql.DB, prefix string) []string {
	t.Helper()
	rows, err := db.Query(`SELECT partition_id FROM ` + prefix + `_buffered_partitions`)
	require.NoError(t, err)
	defer rows.Close()

	var partitions []string
	for rows.Next() {
		var partitionID string
		require.NoError(t, rows.Scan(&partitionID))
		partitions = append(partitions, partitionID)
	}
	require.NoError(t, rows.Err())
	return partitions
}

// countJobsInTable counts the number of unprocessed jobs for a specific partition in a jobsdb table
func countJobsInTable(t *testing.T, db *sql.DB, prefix, partitionID string) int {
	t.Helper()
	var count int
	err := db.QueryRowContext(
		t.Context(),
		`SELECT COUNT(*)
		FROM unionjobsdbmetadata('`+prefix+`', 10)
		WHERE (
			job_state IS NULL
			OR
			job_state NOT IN ('succeeded', 'aborted', 'migrated', 'filtered')
		) AND partition_id = $1`, partitionID).Scan(&count)
	require.NoError(t, err)
	return count
}

// runNodeMigration runs the node migrations on the provided database handle.
func runNodeMigration(t *testing.T, db *sql.DB) {
	t.Helper()
	m := &sqlmigrator.Migrator{
		Handle:          db,
		MigrationsTable: "node_migrations",
	}
	require.NoError(t, m.Migrate("node"))
}

// mockJobsDBPartitionBuffer wraps a real JobsDBPartitionBuffer and can simulate errors
type mockJobsDBPartitionBuffer struct {
	partitionbuffer.JobsDBPartitionBuffer

	bufferPartitionsErr    error
	bufferPartitionsFailAt int // number of times to fail before succeeding
	bufferPartitionsCount  atomic.Int32

	flushBufferedPartitionsErr    error
	flushBufferedPartitionsFailAt int // number of times to fail before succeeding
	flushBufferedPartitionsCount  atomic.Int32
}

func (m *mockJobsDBPartitionBuffer) BufferPartitions(ctx context.Context, partitionIds []string) error {
	count := m.bufferPartitionsCount.Add(1)
	if m.bufferPartitionsErr != nil && int(count) <= m.bufferPartitionsFailAt {
		return m.bufferPartitionsErr
	}
	return m.JobsDBPartitionBuffer.BufferPartitions(ctx, partitionIds)
}

func (m *mockJobsDBPartitionBuffer) FlushBufferedPartitions(ctx context.Context, partitionIds []string) error {
	count := m.flushBufferedPartitionsCount.Add(1)
	if m.flushBufferedPartitionsErr != nil && int(count) <= m.flushBufferedPartitionsFailAt {
		return m.flushBufferedPartitionsErr
	}
	return m.JobsDBPartitionBuffer.FlushBufferedPartitions(ctx, partitionIds)
}

type mockEtcdClient struct {
	*clientv3.Client
	getCount     atomic.Int32
	getFailCount int   // number of times Get should fail before succeeding
	getErr       error // error to return when failing

	putCount     atomic.Int32
	putFailCount int   // number of times Put should fail before succeeding
	putErr       error // error to return when failing
}

func (m *mockEtcdClient) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	count := m.getCount.Add(1)
	if m.getErr != nil && int(count) <= m.getFailCount {
		return nil, m.getErr
	}
	return m.Client.Get(ctx, key, opts...)
}

func (m *mockEtcdClient) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	count := m.putCount.Add(1)
	if m.putErr != nil && int(count) <= m.putFailCount {
		return nil, m.putErr
	}
	return m.Client.Put(ctx, key, val, opts...)
}
