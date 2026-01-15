package sourcenode

import (
	"context"
	"database/sql"
	"fmt"
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
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/etcd"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	etcdtypes "github.com/rudderlabs/rudder-schemas/go/cluster"
	"github.com/rudderlabs/rudder-server/cluster/migrator/partitionmigration/server"
	"github.com/rudderlabs/rudder-server/jobsdb"
	sqlmigrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
)

func TestMigrator(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	etcdResource, err := etcd.Setup(pool, t)
	require.NoError(t, err)

	type handleEnv struct {
		db         *sql.DB
		gwJobsDB   jobsdb.JobsDB
		rtJobsDB   jobsdb.JobsDB
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
		conf.Set("PartitionMigration.Processor.SourceNode.readExcludeSleep", "10ms")
		conf.Set("PartitionMigration.Processor.SourceNode.waitForInProgressTimeout", "10s")

		gwJobsDB := jobsdb.NewForReadWrite("gw",
			jobsdb.WithDBHandle(pg.DB),
			jobsdb.WithNumPartitions(64),
			jobsdb.WithSkipMaintenanceErr(true),
			jobsdb.WithConfig(conf),
		)
		err = gwJobsDB.Start()
		require.NoError(t, err)
		t.Cleanup(func() {
			gwJobsDB.Stop()
		})

		rtJobsDB := jobsdb.NewForReadWrite("rt",
			jobsdb.WithDBHandle(pg.DB),
			jobsdb.WithNumPartitions(64),
			jobsdb.WithSkipMaintenanceErr(true),
			jobsdb.WithConfig(conf),
		)
		err = rtJobsDB.Start()
		require.NoError(t, err)
		t.Cleanup(func() {
			rtJobsDB.Stop()
		})

		return &handleEnv{
			db:         pg.DB,
			gwJobsDB:   gwJobsDB,
			rtJobsDB:   rtJobsDB,
			conf:       conf,
			nodeIndex:  0,
			namespace:  namespace,
			etcdClient: etcdResource.Client,
		}
	}

	generateJobs := func(t *testing.T, partitionID string, count int) []*jobsdb.JobT {
		t.Helper()
		jobs := make([]*jobsdb.JobT, 0, count)
		for range count {
			jobs = append(jobs, &jobsdb.JobT{
				UUID:         uuid.New(),
				UserID:       "user-1",
				CreatedAt:    time.Now(),
				ExpireAt:     time.Now(),
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
		t.Run("no in-progress jobs", func(t *testing.T) {
			env := newHandleEnv(t)

			// Generate some jobs in both jobsdbs
			require.NoError(t, env.gwJobsDB.Store(t.Context(), generateJobs(t, "partition-1", 100)))
			require.NoError(t, env.gwJobsDB.Store(t.Context(), generateJobs(t, "partition-2", 100)))
			require.NoError(t, env.gwJobsDB.Store(t.Context(), generateJobs(t, "partition-3", 100)))
			require.NoError(t, env.rtJobsDB.Store(t.Context(), generateJobs(t, "partition-1", 100)))
			require.NoError(t, env.rtJobsDB.Store(t.Context(), generateJobs(t, "partition-2", 100)))
			require.NoError(t, env.rtJobsDB.Store(t.Context(), generateJobs(t, "partition-3", 100)))

			// Create a migration job where this node is the source
			migration := &etcdtypes.PartitionMigration{
				ID:     "migration-1",
				Status: etcdtypes.PartitionMigrationStatusNew,
				Jobs: []*etcdtypes.PartitionMigrationJobHeader{
					{
						JobID:      "job-1",
						SourceNode: env.nodeIndex,
						TargetNode: 1,
						Partitions: []string{"partition-1", "partition-2"},
					},
				},
			}

			shutdownCalled := false
			m, err := NewMigratorBuilder(env.nodeIndex, "test-node").
				WithConfig(env.conf).
				WithReaderJobsDBs([]jobsdb.JobsDB{env.gwJobsDB, env.rtJobsDB}).
				WithShutdown(func() { shutdownCalled = true }).
				WithTargetURLProvider(func(targetNodeIndex int) (string, error) {
					return "localhost:1234", nil
				}).
				WithEtcdClient(env.etcdClient).
				Build()
			require.NoError(t, err)

			// Call Handle - should complete successfully since there are no in-progress jobs
			err = m.Handle(t.Context(), migration)
			require.NoError(t, err)

			// Verify that partitions are in the read exclusion list for both jobsdbs
			gwExcluded := getReadExcludedPartitions(t, env.db, "gw")
			require.ElementsMatch(t, []string{"partition-1", "partition-2"}, gwExcluded)

			rtExcluded := getReadExcludedPartitions(t, env.db, "rt")
			require.ElementsMatch(t, []string{"partition-1", "partition-2"}, rtExcluded)

			// Verify shutdown was not called
			require.False(t, shutdownCalled, "shutdown should not be called when there are no in-progress jobs")
		})

		t.Run("with in-progress jobs", func(t *testing.T) {
			env := newHandleEnv(t)

			// Generate some jobs in both jobsdbs
			require.NoError(t, env.gwJobsDB.Store(t.Context(), generateJobs(t, "partition-1", 10)))
			require.NoError(t, env.gwJobsDB.Store(t.Context(), generateJobs(t, "partition-2", 10)))
			require.NoError(t, env.gwJobsDB.Store(t.Context(), generateJobs(t, "partition-3", 10)))
			require.NoError(t, env.rtJobsDB.Store(t.Context(), generateJobs(t, "partition-1", 10)))
			require.NoError(t, env.rtJobsDB.Store(t.Context(), generateJobs(t, "partition-2", 10)))
			require.NoError(t, env.rtJobsDB.Store(t.Context(), generateJobs(t, "partition-3", 10)))

			// Get unprocessed jobs so we can mark some as in-progress
			gwUnprocessed, err := env.gwJobsDB.GetUnprocessed(t.Context(), jobsdb.GetQueryParams{
				JobsLimit:        100,
				PartitionFilters: []string{"partition-1"},
			})
			require.NoError(t, err)
			require.Greater(t, len(gwUnprocessed.Jobs), 0, "should have unprocessed jobs in gw")

			rtUnprocessed, err := env.rtJobsDB.GetUnprocessed(t.Context(), jobsdb.GetQueryParams{
				JobsLimit:        100,
				PartitionFilters: []string{"partition-1"},
			})
			require.NoError(t, err)
			require.Greater(t, len(rtUnprocessed.Jobs), 0, "should have unprocessed jobs in rt")

			// Mark some jobs as Executing (in-progress) in both jobsdbs
			gwStatuses := make([]*jobsdb.JobStatusT, 0, len(gwUnprocessed.Jobs))
			for _, job := range gwUnprocessed.Jobs[:5] { // mark first 5 as executing
				gwStatuses = append(gwStatuses, &jobsdb.JobStatusT{
					JobID:         job.JobID,
					JobState:      jobsdb.Executing.State,
					AttemptNum:    1,
					ExecTime:      time.Now(),
					RetryTime:     time.Now(),
					ErrorCode:     "",
					ErrorResponse: []byte(`{}`),
					Parameters:    []byte(`{}`),
					WorkspaceId:   job.WorkspaceId,
					PartitionID:   job.PartitionID,
					CustomVal:     job.CustomVal,
				})
			}
			require.NoError(t, env.gwJobsDB.UpdateJobStatus(t.Context(), gwStatuses))

			rtStatuses := make([]*jobsdb.JobStatusT, 0, len(rtUnprocessed.Jobs))
			for _, job := range rtUnprocessed.Jobs[:5] { // mark first 5 as executing
				rtStatuses = append(rtStatuses, &jobsdb.JobStatusT{
					JobID:         job.JobID,
					JobState:      jobsdb.Executing.State,
					AttemptNum:    1,
					ExecTime:      time.Now(),
					RetryTime:     time.Now(),
					ErrorCode:     "",
					ErrorResponse: []byte(`{}`),
					Parameters:    []byte(`{}`),
					WorkspaceId:   job.WorkspaceId,
					PartitionID:   job.PartitionID,
					CustomVal:     job.CustomVal,
				})
			}
			require.NoError(t, env.rtJobsDB.UpdateJobStatus(t.Context(), rtStatuses))

			// Create a migration job where this node is the source
			migration := &etcdtypes.PartitionMigration{
				ID:     "migration-2",
				Status: etcdtypes.PartitionMigrationStatusNew,
				Jobs: []*etcdtypes.PartitionMigrationJobHeader{
					{
						JobID:      "job-2",
						SourceNode: env.nodeIndex,
						TargetNode: 1,
						Partitions: []string{"partition-1", "partition-2"},
					},
				},
			}

			shutdownCalled := false
			m, err := NewMigratorBuilder(env.nodeIndex, "test-node").
				WithConfig(env.conf).
				WithReaderJobsDBs([]jobsdb.JobsDB{env.gwJobsDB, env.rtJobsDB}).
				WithShutdown(func() { shutdownCalled = true }).
				WithTargetURLProvider(func(targetNodeIndex int) (string, error) {
					return "localhost:1234", nil
				}).
				WithEtcdClient(env.etcdClient).
				Build()
			require.NoError(t, err)

			// Run Handle in a goroutine since it will block waiting for in-progress jobs
			handleDone := make(chan error, 1)
			go func() {
				handleDone <- m.Handle(t.Context(), migration)
			}()

			// Wait a bit for Handle to start polling
			time.Sleep(500 * time.Millisecond)

			// Now mark the in-progress jobs as failed (terminal state) so Handle can complete
			for i := range gwStatuses {
				gwStatuses[i].JobState = jobsdb.Failed.State
				gwStatuses[i].AttemptNum = 2
			}
			require.NoError(t, env.gwJobsDB.UpdateJobStatus(t.Context(), gwStatuses))

			for i := range rtStatuses {
				rtStatuses[i].JobState = jobsdb.Failed.State
				rtStatuses[i].AttemptNum = 2
			}
			require.NoError(t, env.rtJobsDB.UpdateJobStatus(t.Context(), rtStatuses))

			// Wait for Handle to complete
			select {
			case err := <-handleDone:
				require.NoError(t, err, "Handle should complete successfully after in-progress jobs finish")
			case <-time.After(10 * time.Second):
				t.Fatal("Handle did not complete within timeout")
			}

			// Verify that partitions are in the read exclusion list for both jobsdbs
			gwExcluded := getReadExcludedPartitions(t, env.db, "gw")
			require.ElementsMatch(t, []string{"partition-1", "partition-2"}, gwExcluded)

			rtExcluded := getReadExcludedPartitions(t, env.db, "rt")
			require.ElementsMatch(t, []string{"partition-1", "partition-2"}, rtExcluded)

			// Verify shutdown was not called since jobs completed before timeout
			require.False(t, shutdownCalled, "shutdown should not be called when in-progress jobs complete")
		})

		t.Run("with in-progress jobs and a timeout", func(t *testing.T) {
			env := newHandleEnv(t)

			// Set a very short timeout for this test
			env.conf.Set("PartitionMigration.Processor.SourceNode.waitForInProgressTimeout", "500ms")

			// Generate some jobs in both jobsdbs
			require.NoError(t, env.gwJobsDB.Store(t.Context(), generateJobs(t, "partition-1", 10)))
			require.NoError(t, env.rtJobsDB.Store(t.Context(), generateJobs(t, "partition-1", 10)))

			// Get unprocessed jobs so we can mark some as in-progress
			gwUnprocessed, err := env.gwJobsDB.GetUnprocessed(t.Context(), jobsdb.GetQueryParams{
				JobsLimit:        100,
				PartitionFilters: []string{"partition-1"},
			})
			require.NoError(t, err)
			require.Greater(t, len(gwUnprocessed.Jobs), 0, "should have unprocessed jobs in gw")

			rtUnprocessed, err := env.rtJobsDB.GetUnprocessed(t.Context(), jobsdb.GetQueryParams{
				JobsLimit:        100,
				PartitionFilters: []string{"partition-1"},
			})
			require.NoError(t, err)
			require.Greater(t, len(rtUnprocessed.Jobs), 0, "should have unprocessed jobs in rt")

			// Mark some jobs as Executing (in-progress) in both jobsdbs
			gwStatuses := make([]*jobsdb.JobStatusT, 0)
			for _, job := range gwUnprocessed.Jobs[:5] {
				gwStatuses = append(gwStatuses, &jobsdb.JobStatusT{
					JobID:         job.JobID,
					JobState:      jobsdb.Executing.State,
					AttemptNum:    1,
					ExecTime:      time.Now(),
					RetryTime:     time.Now(),
					ErrorCode:     "",
					ErrorResponse: []byte(`{}`),
					Parameters:    []byte(`{}`),
					WorkspaceId:   job.WorkspaceId,
					PartitionID:   job.PartitionID,
					CustomVal:     job.CustomVal,
				})
			}
			require.NoError(t, env.gwJobsDB.UpdateJobStatus(t.Context(), gwStatuses))

			rtStatuses := make([]*jobsdb.JobStatusT, 0)
			for _, job := range rtUnprocessed.Jobs[:5] {
				rtStatuses = append(rtStatuses, &jobsdb.JobStatusT{
					JobID:         job.JobID,
					JobState:      jobsdb.Executing.State,
					AttemptNum:    1,
					ExecTime:      time.Now(),
					RetryTime:     time.Now(),
					ErrorCode:     "",
					ErrorResponse: []byte(`{}`),
					Parameters:    []byte(`{}`),
					WorkspaceId:   job.WorkspaceId,
					PartitionID:   job.PartitionID,
					CustomVal:     job.CustomVal,
				})
			}
			require.NoError(t, env.rtJobsDB.UpdateJobStatus(t.Context(), rtStatuses))

			// Create a migration job where this node is the source
			migration := &etcdtypes.PartitionMigration{
				ID:     "migration-3",
				Status: etcdtypes.PartitionMigrationStatusNew,
				Jobs: []*etcdtypes.PartitionMigrationJobHeader{
					{
						JobID:      "job-3",
						SourceNode: env.nodeIndex,
						TargetNode: 1,
						Partitions: []string{"partition-1"},
					},
				},
			}

			shutdownCalled := false
			m, err := NewMigratorBuilder(env.nodeIndex, "test-node").
				WithConfig(env.conf).
				WithReaderJobsDBs([]jobsdb.JobsDB{env.gwJobsDB, env.rtJobsDB}).
				WithShutdown(func() { shutdownCalled = true }).
				WithTargetURLProvider(func(targetNodeIndex int) (string, error) {
					return "localhost:1234", nil
				}).
				WithEtcdClient(env.etcdClient).
				Build()
			require.NoError(t, err)

			// Call Handle - should timeout because in-progress jobs never complete
			err = m.Handle(t.Context(), migration)
			require.Error(t, err, "Handle should fail due to timeout")
			require.Contains(t, err.Error(), "timeout waiting for no in-progress jobs")

			// Verify shutdown was called due to timeout
			require.True(t, shutdownCalled, "shutdown should be called when timeout occurs")

			// Verify that partitions are still in the read exclusion list even though migration failed
			// (partitions are marked as excluded before waiting for in-progress jobs)
			gwExcluded := getReadExcludedPartitions(t, env.db, "gw")
			require.ElementsMatch(t, []string{"partition-1"}, gwExcluded)

			rtExcluded := getReadExcludedPartitions(t, env.db, "rt")
			require.ElementsMatch(t, []string{"partition-1"}, rtExcluded)
		})

		t.Run("with jobsdb intermittent failures", func(t *testing.T) {
			env := newHandleEnv(t)

			// Generate some jobs in both jobsdbs
			require.NoError(t, env.gwJobsDB.Store(t.Context(), generateJobs(t, "partition-1", 100)))
			require.NoError(t, env.gwJobsDB.Store(t.Context(), generateJobs(t, "partition-2", 100)))
			require.NoError(t, env.rtJobsDB.Store(t.Context(), generateJobs(t, "partition-1", 100)))
			require.NoError(t, env.rtJobsDB.Store(t.Context(), generateJobs(t, "partition-2", 100)))

			// Create a migration job where this node is the source
			migration := &etcdtypes.PartitionMigration{
				ID:     "migration-4",
				Status: etcdtypes.PartitionMigrationStatusNew,
				Jobs: []*etcdtypes.PartitionMigrationJobHeader{
					{
						JobID:      "job-4",
						SourceNode: env.nodeIndex,
						TargetNode: 1,
						Partitions: []string{"partition-1", "partition-2"},
					},
				},
			}

			// Create mock jobsdbs that fail AddReadExcludedPartitionIDs 3 times before succeeding
			mockGWJobsDB := &mockJobsDB{
				JobsDB:                               env.gwJobsDB,
				addReadExcludedPartitionIDsFailCount: 3,
				addReadExcludedPartitionIDsErr:       fmt.Errorf("jobsdb error: connection refused"),
			}
			mockRTJobsDB := &mockJobsDB{
				JobsDB:                               env.rtJobsDB,
				addReadExcludedPartitionIDsFailCount: 3,
				addReadExcludedPartitionIDsErr:       fmt.Errorf("jobsdb error: connection refused"),
			}

			shutdownCalled := false
			m, err := NewMigratorBuilder(env.nodeIndex, "test-node").
				WithConfig(env.conf).
				WithReaderJobsDBs([]jobsdb.JobsDB{mockGWJobsDB, mockRTJobsDB}).
				WithShutdown(func() { shutdownCalled = true }).
				WithTargetURLProvider(func(targetNodeIndex int) (string, error) {
					return "localhost:1234", nil
				}).
				WithEtcdClient(env.etcdClient).
				Build()
			require.NoError(t, err)

			// Call Handle - should eventually succeed after retrying AddReadExcludedPartitionIDs
			require.Eventually(t, func() bool {
				err = m.Handle(t.Context(), migration)
				return err == nil
			}, 10*time.Second, 10*time.Millisecond, "Handle should eventually succeed after retries")

			// Verify the mock jobsdbs had AddReadExcludedPartitionIDs called multiple times (retries happened)
			require.GreaterOrEqual(t, int(mockGWJobsDB.addReadExcludedPartitionIDsCount.Load()), 4,
				"gw AddReadExcludedPartitionIDs should be called at least 4 times (3 failures + 1 success)")
			require.GreaterOrEqual(t, int(mockRTJobsDB.addReadExcludedPartitionIDsCount.Load()), 4,
				"rt AddReadExcludedPartitionIDs should be called at least 4 times (3 failures + 1 success)")

			// Verify that partitions are in the read exclusion list for both jobsdbs
			gwExcluded := getReadExcludedPartitions(t, env.db, "gw")
			require.ElementsMatch(t, []string{"partition-1", "partition-2"}, gwExcluded)

			rtExcluded := getReadExcludedPartitions(t, env.db, "rt")
			require.ElementsMatch(t, []string{"partition-1", "partition-2"}, rtExcluded)

			// Verify shutdown was not called
			require.False(t, shutdownCalled, "shutdown should not be called when migration succeeds after retries")
		})
	})

	type runEnv struct {
		sourceDB       *sql.DB
		sourceGWJobsDB jobsdb.JobsDB
		sourceRTJobsDB jobsdb.JobsDB

		targetDB       *sql.DB
		targetGWJobsDB jobsdb.JobsDB
		targetRTJobsDB jobsdb.JobsDB

		conf       *config.Config
		targetAddr string
		namespace  string
		etcdClient *clientv3.Client
	}

	newRunEnv := func(t *testing.T) *runEnv {
		t.Helper()

		setupSqlDB := func(t *testing.T) *sql.DB {
			t.Helper()
			pg, err := postgres.Setup(pool, t)
			require.NoError(t, err)
			runNodeMigration(t, pg.DB)
			return pg.DB
		}

		namespace := rand.String(10)
		conf := config.New()
		conf.Set("WORKSPACE_NAMESPACE", namespace)
		conf.Set("PartitionMigration.Processor.SourceNode.readExcludeSleep", "10ms")
		conf.Set("PartitionMigration.Processor.SourceNode.waitForInProgressTimeout", "10s")
		conf.Set("LOG_LEVEL", "DEBUG")

		setupJobsDB := func(t *testing.T, prefix string, sqlDB *sql.DB) jobsdb.JobsDB {
			t.Helper()
			db := jobsdb.NewForReadWrite(prefix,
				jobsdb.WithDBHandle(sqlDB),
				jobsdb.WithNumPartitions(64),
				jobsdb.WithSkipMaintenanceErr(true),
				jobsdb.WithConfig(conf),
			)
			err := db.Start()
			require.NoError(t, err)
			t.Cleanup(func() {
				db.Stop()
			})
			return db
		}

		// Setup source postgres and jobsdbs
		sourceDB := setupSqlDB(t)
		sourceGWJobsDB := setupJobsDB(t, "gw", sourceDB)
		sourceRTJobsDB := setupJobsDB(t, "rt", sourceDB)

		// Setup target postgres and jobsdbs
		targetDB := setupSqlDB(t)
		targetGWJobsDB := setupJobsDB(t, "gw", targetDB)
		targetRTJobsDB := setupJobsDB(t, "rt", targetDB)

		// Setup partition migration gRPC server
		port, err := testhelper.GetFreePort()
		require.NoError(t, err)
		conf.Set("PartitionMigration.Grpc.Server.Port", port)
		conf.Set("PartitionMigration.Grpc.Server.WriteBufferSize", -1)
		conf.Set("PartitionMigration.Grpc.Server.ReadBufferSize", -1)

		pms := server.NewPartitionMigrationServer(t.Context(), []jobsdb.JobsDB{targetGWJobsDB, targetRTJobsDB},
			server.WithLogger(logger.NewFactory(conf).NewLogger()))
		grpcServer := server.NewGRPCServer(conf, pms)
		require.NoError(t, grpcServer.Start(), "it should be able to start the grpc server")
		t.Cleanup(grpcServer.Stop)

		return &runEnv{
			sourceDB:       sourceDB,
			sourceGWJobsDB: sourceGWJobsDB,
			sourceRTJobsDB: sourceRTJobsDB,
			targetDB:       targetDB,
			targetGWJobsDB: targetGWJobsDB,
			targetRTJobsDB: targetRTJobsDB,
			conf:           conf,
			targetAddr:     "127.0.0.1:" + strconv.Itoa(port),
			namespace:      namespace,
			etcdClient:     etcdResource.Client,
		}
	}

	countJobsForPartition := func(t *testing.T, db *sql.DB, prefix, partitionID string) int {
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
			) AND partition_id = '`+partitionID+`'`).Scan(&count)
		require.NoError(t, err)
		return count
	}

	t.Run("Run", func(t *testing.T) {
		t.Run("new job successful move", func(t *testing.T) {
			env := newRunEnv(t)

			// Generate jobs in source db (both jobsdbs)
			require.NoError(t, env.sourceGWJobsDB.Store(t.Context(), generateJobs(t, "partition-1", 100)))
			require.NoError(t, env.sourceGWJobsDB.Store(t.Context(), generateJobs(t, "partition-2", 100)))
			require.NoError(t, env.sourceGWJobsDB.Store(t.Context(), generateJobs(t, "partition-3", 100)))
			require.NoError(t, env.sourceRTJobsDB.Store(t.Context(), generateJobs(t, "partition-1", 100)))
			require.NoError(t, env.sourceRTJobsDB.Store(t.Context(), generateJobs(t, "partition-2", 100)))
			require.NoError(t, env.sourceRTJobsDB.Store(t.Context(), generateJobs(t, "partition-3", 100)))

			// Create migration job in etcd
			migrationJob := &etcdtypes.PartitionMigrationJob{
				PartitionMigrationJobHeader: etcdtypes.PartitionMigrationJobHeader{
					JobID:      "job-1",
					SourceNode: 0, // this node
					TargetNode: 1,
					Partitions: []string{"partition-1", "partition-2"},
				},
				MigrationID: "migration-1",
				Status:      etcdtypes.PartitionMigrationJobStatusNew,
			}
			jobKey := "/" + env.namespace + "/migration/job/" + migrationJob.JobID
			jobValue, err := jsonrs.Marshal(migrationJob)
			require.NoError(t, err)
			_, err = env.etcdClient.Put(t.Context(), jobKey, string(jobValue))
			require.NoError(t, err)

			// Create a migration (for Handle)
			migration := &etcdtypes.PartitionMigration{
				ID:     "migration-1",
				Status: etcdtypes.PartitionMigrationStatusNew,
				Jobs: []*etcdtypes.PartitionMigrationJobHeader{
					&migrationJob.PartitionMigrationJobHeader,
				},
			}

			// Create migrator
			m, err := NewMigratorBuilder(0, "test-node").
				WithConfig(env.conf).
				WithReaderJobsDBs([]jobsdb.JobsDB{env.sourceGWJobsDB, env.sourceRTJobsDB}).
				WithShutdown(func() {}).
				WithTargetURLProvider(func(targetNodeIndex int) (string, error) {
					return env.targetAddr, nil
				}).
				WithEtcdClient(env.etcdClient).
				Build()
			require.NoError(t, err)

			// Call Handle for the migration (marks partitions as read-excluded)
			err = m.Handle(t.Context(), migration)
			require.NoError(t, err)

			// Call Run
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			wg, ctx := errgroup.WithContext(ctx)
			err = m.Run(ctx, wg)
			require.NoError(t, err)

			// Wait for migration to complete (job status changes to "moved")
			require.Eventually(t, func() bool {
				resp, err := env.etcdClient.Get(t.Context(), jobKey)
				if err != nil || len(resp.Kvs) == 0 {
					return false
				}
				var job etcdtypes.PartitionMigrationJob
				if err := jsonrs.Unmarshal(resp.Kvs[0].Value, &job); err != nil {
					return false
				}
				return job.Status == etcdtypes.PartitionMigrationJobStatusMoved
			}, 30*time.Second, 100*time.Millisecond, "job status should be marked as moved")

			// Cancel context to stop watcher
			cancel()
			require.NoError(t, wg.Wait(), "Run should complete without error")

			// Verify that data is moved to target db
			p1TargetGW := countJobsForPartition(t, env.targetDB, "gw", "partition-1")
			require.Equal(t, 100, p1TargetGW, "all jobs for partition-1 should be migrated to target gw")
			p2TargetGW := countJobsForPartition(t, env.targetDB, "gw", "partition-2")
			require.Equal(t, 100, p2TargetGW, "all jobs for partition-2 should be migrated to target gw")
			p1TargetRT := countJobsForPartition(t, env.targetDB, "rt", "partition-1")
			require.Equal(t, 100, p1TargetRT, "all jobs for partition-1 should be migrated to target rt")
			p2TargetRT := countJobsForPartition(t, env.targetDB, "rt", "partition-2")
			require.Equal(t, 100, p2TargetRT, "all jobs for partition-2 should be migrated to target rt")

			// Verify that jobs are removed from source db
			p1SourceGW := countJobsForPartition(t, env.sourceDB, "gw", "partition-1")
			require.Equal(t, 0, p1SourceGW, "all jobs for partition-1 should be removed from source gw")
			p2SourceGW := countJobsForPartition(t, env.sourceDB, "gw", "partition-2")
			require.Equal(t, 0, p2SourceGW, "all jobs for partition-2 should be removed from source gw")
			p1SourceRT := countJobsForPartition(t, env.sourceDB, "rt", "partition-1")
			require.Equal(t, 0, p1SourceRT, "all jobs for partition-1 should be removed from source rt")
			p2SourceRT := countJobsForPartition(t, env.sourceDB, "rt", "partition-2")
			require.Equal(t, 0, p2SourceRT, "all jobs for partition-2 should be removed from source rt")

			// Verify that read exclusions are removed after migration
			gwExcluded := getReadExcludedPartitions(t, env.sourceDB, "gw")
			require.Empty(t, gwExcluded, "read exclusions should be removed from gw after migration")
			rtExcluded := getReadExcludedPartitions(t, env.sourceDB, "rt")
			require.Empty(t, rtExcluded, "read exclusions should be removed from rt after migration")
		})

		t.Run("etcd client intermittent failures", func(t *testing.T) {
			env := newRunEnv(t)

			// Generate jobs in source db (both jobsdbs)
			require.NoError(t, env.sourceGWJobsDB.Store(t.Context(), generateJobs(t, "partition-1", 100)))
			require.NoError(t, env.sourceGWJobsDB.Store(t.Context(), generateJobs(t, "partition-2", 100)))
			require.NoError(t, env.sourceGWJobsDB.Store(t.Context(), generateJobs(t, "partition-3", 100)))
			require.NoError(t, env.sourceRTJobsDB.Store(t.Context(), generateJobs(t, "partition-1", 100)))
			require.NoError(t, env.sourceRTJobsDB.Store(t.Context(), generateJobs(t, "partition-2", 100)))
			require.NoError(t, env.sourceRTJobsDB.Store(t.Context(), generateJobs(t, "partition-3", 100)))

			// Create migration job in etcd (use real client to ensure data is there)
			migrationJob := &etcdtypes.PartitionMigrationJob{
				PartitionMigrationJobHeader: etcdtypes.PartitionMigrationJobHeader{
					JobID:      "job-1",
					SourceNode: 0, // this node
					TargetNode: 1,
					Partitions: []string{"partition-1", "partition-2"},
				},
				MigrationID: "migration-1",
				Status:      etcdtypes.PartitionMigrationJobStatusNew,
			}
			jobKey := "/" + env.namespace + "/migration/job/" + migrationJob.JobID
			jobValue, err := jsonrs.Marshal(migrationJob)
			require.NoError(t, err)
			_, err = env.etcdClient.Put(t.Context(), jobKey, string(jobValue))
			require.NoError(t, err)

			// Create a migration (for Handle)
			migration := &etcdtypes.PartitionMigration{
				ID:     "migration-1",
				Status: etcdtypes.PartitionMigrationStatusNew,
				Jobs: []*etcdtypes.PartitionMigrationJobHeader{
					&migrationJob.PartitionMigrationJobHeader,
				},
			}

			// Create a mock etcd client that wraps the real client but fails Get initially
			mockClient := &mockEtcdClient{
				Client:       env.etcdClient,
				getFailCount: 3, // fail 3 times before succeeding
				getErr:       fmt.Errorf("etcd get error: connection refused"),
			}

			// Create migrator with mock client
			m, err := NewMigratorBuilder(0, "test-node").
				WithConfig(env.conf).
				WithReaderJobsDBs([]jobsdb.JobsDB{env.sourceGWJobsDB, env.sourceRTJobsDB}).
				WithShutdown(func() {}).
				WithTargetURLProvider(func(targetNodeIndex int) (string, error) {
					return env.targetAddr, nil
				}).
				WithEtcdClient(mockClient).
				Build()
			require.NoError(t, err)

			// Call Handle for the migration (marks partitions as read-excluded)
			err = m.Handle(t.Context(), migration)
			require.NoError(t, err)

			// Call Run
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			wg, ctx := errgroup.WithContext(ctx)
			err = m.Run(ctx, wg)
			require.NoError(t, err)

			// Wait for migration to complete (job status changes to "moved")
			// Use real client to check status
			require.Eventually(t, func() bool {
				resp, err := env.etcdClient.Get(t.Context(), jobKey)
				if err != nil || len(resp.Kvs) == 0 {
					return false
				}
				var job etcdtypes.PartitionMigrationJob
				if err := jsonrs.Unmarshal(resp.Kvs[0].Value, &job); err != nil {
					return false
				}
				return job.Status == etcdtypes.PartitionMigrationJobStatusMoved
			}, 30*time.Second, 100*time.Millisecond, "job status should be marked as moved")

			// Cancel context to stop watcher
			cancel()
			require.NoError(t, wg.Wait(), "Run should complete without error")

			// Verify the mock client was called multiple times (retries happened)
			require.GreaterOrEqual(t, int(mockClient.getCount.Load()), 3, "etcd Get should be called at least 3 times (failures + successful calls)")

			// Verify that data is moved to target db
			p1TargetGW := countJobsForPartition(t, env.targetDB, "gw", "partition-1")
			require.Equal(t, 100, p1TargetGW, "all jobs for partition-1 should be migrated to target gw")
			p2TargetGW := countJobsForPartition(t, env.targetDB, "gw", "partition-2")
			require.Equal(t, 100, p2TargetGW, "all jobs for partition-2 should be migrated to target gw")
			p1TargetRT := countJobsForPartition(t, env.targetDB, "rt", "partition-1")
			require.Equal(t, 100, p1TargetRT, "all jobs for partition-1 should be migrated to target rt")
			p2TargetRT := countJobsForPartition(t, env.targetDB, "rt", "partition-2")
			require.Equal(t, 100, p2TargetRT, "all jobs for partition-2 should be migrated to target rt")

			// Verify that jobs are removed from source db
			p1SourceGW := countJobsForPartition(t, env.sourceDB, "gw", "partition-1")
			require.Equal(t, 0, p1SourceGW, "all jobs for partition-1 should be removed from source gw")
			p2SourceGW := countJobsForPartition(t, env.sourceDB, "gw", "partition-2")
			require.Equal(t, 0, p2SourceGW, "all jobs for partition-2 should be removed from source gw")
			p1SourceRT := countJobsForPartition(t, env.sourceDB, "rt", "partition-1")
			require.Equal(t, 0, p1SourceRT, "all jobs for partition-1 should be removed from source rt")
			p2SourceRT := countJobsForPartition(t, env.sourceDB, "rt", "partition-2")
			require.Equal(t, 0, p2SourceRT, "all jobs for partition-2 should be removed from source rt")

			// Verify that read exclusions are removed after migration
			gwExcluded := getReadExcludedPartitions(t, env.sourceDB, "gw")
			require.Empty(t, gwExcluded, "read exclusions should be removed from gw after migration")
			rtExcluded := getReadExcludedPartitions(t, env.sourceDB, "rt")
			require.Empty(t, rtExcluded, "read exclusions should be removed from rt after migration")
		})

		t.Run("jobsdb intermittent failures", func(t *testing.T) {
			env := newRunEnv(t)

			// Generate jobs in source db (both jobsdbs)
			require.NoError(t, env.sourceGWJobsDB.Store(t.Context(), generateJobs(t, "partition-1", 100)))
			require.NoError(t, env.sourceGWJobsDB.Store(t.Context(), generateJobs(t, "partition-2", 100)))
			require.NoError(t, env.sourceGWJobsDB.Store(t.Context(), generateJobs(t, "partition-3", 100)))
			require.NoError(t, env.sourceRTJobsDB.Store(t.Context(), generateJobs(t, "partition-1", 100)))
			require.NoError(t, env.sourceRTJobsDB.Store(t.Context(), generateJobs(t, "partition-2", 100)))
			require.NoError(t, env.sourceRTJobsDB.Store(t.Context(), generateJobs(t, "partition-3", 100)))

			// Create migration job in etcd
			migrationJob := &etcdtypes.PartitionMigrationJob{
				PartitionMigrationJobHeader: etcdtypes.PartitionMigrationJobHeader{
					JobID:      "job-1",
					SourceNode: 0, // this node
					TargetNode: 1,
					Partitions: []string{"partition-1", "partition-2"},
				},
				MigrationID: "migration-1",
				Status:      etcdtypes.PartitionMigrationJobStatusNew,
			}
			jobKey := "/" + env.namespace + "/migration/job/" + migrationJob.JobID
			jobValue, err := jsonrs.Marshal(migrationJob)
			require.NoError(t, err)
			_, err = env.etcdClient.Put(t.Context(), jobKey, string(jobValue))
			require.NoError(t, err)

			// Create a migration (for Handle)
			migration := &etcdtypes.PartitionMigration{
				ID:     "migration-1",
				Status: etcdtypes.PartitionMigrationStatusNew,
				Jobs: []*etcdtypes.PartitionMigrationJobHeader{
					&migrationJob.PartitionMigrationJobHeader,
				},
			}

			// Create a mock jobsdb wrapper for GW that fails GetToProcess initially
			mockGWJobsDB := &mockJobsDB{
				JobsDB:                env.sourceGWJobsDB,
				getToProcessFailCount: 3, // fail 3 times before succeeding
				getToProcessErr:       fmt.Errorf("jobsdb error: connection refused"),
			}

			// Create migrator with mock jobsdb (gw uses mock, rt uses real)
			m, err := NewMigratorBuilder(0, "test-node").
				WithConfig(env.conf).
				WithReaderJobsDBs([]jobsdb.JobsDB{mockGWJobsDB, env.sourceRTJobsDB}).
				WithShutdown(func() {}).
				WithTargetURLProvider(func(targetNodeIndex int) (string, error) {
					return env.targetAddr, nil
				}).
				WithEtcdClient(env.etcdClient).
				Build()
			require.NoError(t, err)

			// Call Handle for the migration (marks partitions as read-excluded)
			err = m.Handle(t.Context(), migration)
			require.NoError(t, err)

			// Call Run
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			wg, ctx := errgroup.WithContext(ctx)
			err = m.Run(ctx, wg)
			require.NoError(t, err)

			// Wait for migration to complete (job status changes to "moved")
			require.Eventually(t, func() bool {
				resp, err := env.etcdClient.Get(t.Context(), jobKey)
				if err != nil || len(resp.Kvs) == 0 {
					return false
				}
				var job etcdtypes.PartitionMigrationJob
				if err := jsonrs.Unmarshal(resp.Kvs[0].Value, &job); err != nil {
					return false
				}
				return job.Status == etcdtypes.PartitionMigrationJobStatusMoved
			}, 30*time.Second, 100*time.Millisecond, "job status should be marked as moved")

			// Cancel context to stop watcher
			cancel()
			require.NoError(t, wg.Wait(), "Run should complete without error")

			// Verify the mock jobsdb was called multiple times (retries happened)
			require.GreaterOrEqual(t, int(mockGWJobsDB.getToProcessCount.Load()), 3, "jobsdb GetToProcess should be called at least 3 times (failures + successful calls)")

			// Verify that data is moved to target db
			p1TargetGW := countJobsForPartition(t, env.targetDB, "gw", "partition-1")
			require.Equal(t, 100, p1TargetGW, "all jobs for partition-1 should be migrated to target gw")
			p2TargetGW := countJobsForPartition(t, env.targetDB, "gw", "partition-2")
			require.Equal(t, 100, p2TargetGW, "all jobs for partition-2 should be migrated to target gw")
			p1TargetRT := countJobsForPartition(t, env.targetDB, "rt", "partition-1")
			require.Equal(t, 100, p1TargetRT, "all jobs for partition-1 should be migrated to target rt")
			p2TargetRT := countJobsForPartition(t, env.targetDB, "rt", "partition-2")
			require.Equal(t, 100, p2TargetRT, "all jobs for partition-2 should be migrated to target rt")

			// Verify that jobs are removed from source db
			p1SourceGW := countJobsForPartition(t, env.sourceDB, "gw", "partition-1")
			require.Equal(t, 0, p1SourceGW, "all jobs for partition-1 should be removed from source gw")
			p2SourceGW := countJobsForPartition(t, env.sourceDB, "gw", "partition-2")
			require.Equal(t, 0, p2SourceGW, "all jobs for partition-2 should be removed from source gw")
			p1SourceRT := countJobsForPartition(t, env.sourceDB, "rt", "partition-1")
			require.Equal(t, 0, p1SourceRT, "all jobs for partition-1 should be removed from source rt")
			p2SourceRT := countJobsForPartition(t, env.sourceDB, "rt", "partition-2")
			require.Equal(t, 0, p2SourceRT, "all jobs for partition-2 should be removed from source rt")

			// Verify that read exclusions are removed after migration
			gwExcluded := getReadExcludedPartitions(t, env.sourceDB, "gw")
			require.Empty(t, gwExcluded, "read exclusions should be removed from gw after migration")
			rtExcluded := getReadExcludedPartitions(t, env.sourceDB, "rt")
			require.Empty(t, rtExcluded, "read exclusions should be removed from rt after migration")
		})
	})
}

// getReadExcludedPartitions retrieves the read-excluded partition IDs from the database
func getReadExcludedPartitions(t *testing.T, db *sql.DB, prefix string) []string {
	t.Helper()
	rows, err := db.Query(`SELECT partition_id FROM ` + prefix + `_read_excluded_partitions`)
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

// runNodeMigration runs the node migrations on the provided database handle.
func runNodeMigration(t *testing.T, db *sql.DB) {
	t.Helper()
	m := &sqlmigrator.Migrator{
		Handle:          db,
		MigrationsTable: "node_migrations",
	}
	require.NoError(t, m.Migrate("node"))
}

// mockEtcdClient wraps a real etcd client and simulates intermittent failures
type mockEtcdClient struct {
	*clientv3.Client
	getCount     atomic.Int32
	getFailCount int   // number of times Get should fail before succeeding
	getErr       error // error to return when failing
}

func (m *mockEtcdClient) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	count := m.getCount.Add(1)
	if m.getErr != nil && int(count) <= m.getFailCount {
		return nil, m.getErr
	}
	return m.Client.Get(ctx, key, opts...)
}

// mockJobsDB wraps a real jobsdb and simulates intermittent failures for GetToProcess
type mockJobsDB struct {
	jobsdb.JobsDB

	getToProcessCount     atomic.Int32
	getToProcessFailCount int   // number of times GetToProcess should fail before succeeding
	getToProcessErr       error // error to return when failing

	addReadExcludedPartitionIDsCount     atomic.Int32
	addReadExcludedPartitionIDsFailCount int   // number of times AddReadExcludedPartitionIDs should fail before succeeding
	addReadExcludedPartitionIDsErr       error // error to return when failing
}

func (m *mockJobsDB) GetToProcess(ctx context.Context, params jobsdb.GetQueryParams, more jobsdb.MoreToken) (*jobsdb.MoreJobsResult, error) {
	count := m.getToProcessCount.Add(1)
	if m.getToProcessErr != nil && int(count) <= m.getToProcessFailCount {
		return nil, m.getToProcessErr
	}
	return m.JobsDB.GetToProcess(ctx, params, more)
}

func (m *mockJobsDB) AddReadExcludedPartitionIDs(ctx context.Context, partitionIDs []string) error {
	count := m.addReadExcludedPartitionIDsCount.Add(1)
	if m.addReadExcludedPartitionIDsErr != nil && int(count) <= m.addReadExcludedPartitionIDsFailCount {
		return m.addReadExcludedPartitionIDsErr
	}
	return m.JobsDB.AddReadExcludedPartitionIDs(ctx, partitionIDs)
}
