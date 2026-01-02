package client

import (
	"context"
	"database/sql"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/cluster/migrator/partitionmigration/server"
	"github.com/rudderlabs/rudder-server/jobsdb"
	proto "github.com/rudderlabs/rudder-server/proto/cluster"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/tx"
)

func TestMigrationJobExecutor(t *testing.T) {
	// newEnv initializes the test environment with a running gRPC server and a jobsdb instance.
	newEnv := func(t *testing.T) *testMigrationJobExecutorEnv {
		t.Helper()
		pool, err := dockertest.NewPool("")
		require.NoError(t, err)

		setupDB := func(t *testing.T) (*sql.DB, jobsdb.JobsDB) {
			pg, err := postgres.Setup(pool, t)
			require.NoError(t, err)
			runNodeMigration(t, pg.DB)
			db := jobsdb.NewForReadWrite("rt",
				jobsdb.WithDBHandle(pg.DB),
				jobsdb.WithNumPartitions(64),
				jobsdb.WithSkipMaintenanceErr(true),
			)
			err = db.Start()
			require.NoError(t, err)
			t.Cleanup(func() {
				db.Stop()
			})
			return pg.DB, db
		}
		sourceDB, sourceJobsDB := setupDB(t)
		targetDB, targetJobsDB := setupDB(t)

		port, err := testhelper.GetFreePort()
		require.NoError(t, err)
		c := config.New()
		c.Set("PartitionMigration.Grpc.Server.Port", port)
		c.Set("PartitionMigration.Grpc.Server.WriteBufferSize", -1) // disable write buffer
		c.Set("PartitionMigration.Grpc.Server.ReadBufferSize", -1)  // disable read buffer
		c.Set("LOG_LEVEL", "ERROR")
		pms := server.NewPartitionMigrationServer(t.Context(), []jobsdb.JobsDB{targetJobsDB}, server.WithLogger(logger.NewFactory(c).NewLogger()))
		mockPMS := &mockPartitionMigrationServer{
			PartitionMigrationServer: pms,
		}
		grpcServer := server.NewGRPCServer(c, mockPMS)
		require.NoError(t, grpcServer.Start(), "it should be able to start the grpc server")
		t.Cleanup(grpcServer.Stop)

		return &testMigrationJobExecutorEnv{
			sourceDB:     sourceDB,
			sourceJobsDB: &mockJobsDB{JobsDB: sourceJobsDB},
			targetDB:     targetDB,
			targetJobsDB: &mockJobsDB{JobsDB: targetJobsDB},

			target: "127.0.0.1:" + strconv.Itoa(port),
			pms:    mockPMS,
		}
	}

	t.Run("successful migration", func(t *testing.T) {
		env := newEnv(t)

		require.NoError(t, env.sourceJobsDB.Store(t.Context(), env.generateJobs(t, "1", 1000)))
		require.NoError(t, env.sourceJobsDB.Store(t.Context(), env.generateJobs(t, "2", 1000)))
		require.NoError(t, env.sourceJobsDB.Store(t.Context(), env.generateJobs(t, "3", 1000)))

		c := config.New()
		c.Set("LOG_LEVEL", "DEBUG")
		c.Set("PartitionMigration.Executor.BatchSize", 100)
		c.Set("PartitionMigration.Executor.ChunkSize", 10)
		c.Set("PartitionMigration.Executor.StreamTimeout", "1m")
		c.Set("PartitionMigration.Executor.ProgressPeriod", "100ms")

		mpe := NewMigrationJobExecutor("job1", []string{"1", "2"}, env.sourceJobsDB, env.target, WithConfig(c), WithLogger(logger.NewFactory(c).NewLogger()))
		err := mpe.Run(t.Context())
		require.NoError(t, err, "migration job should complete successfully")

		// verify jobs in target
		p1Target := env.countJobsForPartition(t, env.targetDB, env.targetJobsDB.Identifier(), "1")
		require.Equal(t, 1000, p1Target, "all jobs for partition 1 should be migrated to target")
		p2Target := env.countJobsForPartition(t, env.targetDB, env.targetJobsDB.Identifier(), "2")
		require.Equal(t, 1000, p2Target, "all jobs for partition 2 should be migrated to target")

		// verify jobs in source
		p1Source := env.countJobsForPartition(t, env.sourceDB, env.sourceJobsDB.Identifier(), "1")
		require.Equal(t, 0, p1Source, "all jobs for partition 1 should be removed from source")
		p2Source := env.countJobsForPartition(t, env.sourceDB, env.sourceJobsDB.Identifier(), "2")
		require.Equal(t, 0, p2Source, "all jobs for partition 2 should be removed from source")
		p3Source := env.countJobsForPartition(t, env.sourceDB, env.sourceJobsDB.Identifier(), "3")
		require.Equal(t, 1000, p3Source, "jobs for partition 3 should remain in source")
	})

	t.Run("successful migration with some jobs already in executing state", func(t *testing.T) {
		env := newEnv(t)
		require.NoError(t, env.sourceJobsDB.Store(t.Context(), env.generateJobs(t, "1", 1000)))
		// mark some jobs as executing
		jobs, err := env.sourceJobsDB.GetUnprocessed(t.Context(), jobsdb.GetQueryParams{JobsLimit: 100})
		require.NoError(t, err)
		require.Len(t, jobs.Jobs, 100)
		err = env.sourceJobsDB.UpdateJobStatus(t.Context(), env.generateJobStatuses(t, jobs.Jobs, jobsdb.Executing.State), []string{"test"}, nil)
		require.NoError(t, err)

		c := config.New()
		c.Set("LOG_LEVEL", "DEBUG")
		c.Set("PartitionMigration.Executor.BatchSize", 100)
		c.Set("PartitionMigration.Executor.ChunkSize", 10)
		c.Set("PartitionMigration.Executor.StreamTimeout", "1m")
		c.Set("PartitionMigration.Executor.ProgressPeriod", "100ms")

		mpe := NewMigrationJobExecutor("job1", []string{"1"}, env.sourceJobsDB, env.target, WithConfig(c), WithLogger(logger.NewFactory(c).NewLogger()))
		err = mpe.Run(t.Context())
		require.NoError(t, err, "migration job should complete successfully")

		// verify jobs in target
		p1Target := env.countJobsForPartition(t, env.targetDB, env.targetJobsDB.Identifier(), "1")
		require.Equal(t, 1000, p1Target, "all jobs for partition 1 should be migrated to target")

		// verify jobs in source
		p1Source := env.countJobsForPartition(t, env.sourceDB, env.sourceJobsDB.Identifier(), "1")
		require.Equal(t, 0, p1Source, "all jobs for partition 1 should be removed from source")
	})

	t.Run("migration getting cancelled and resumed", func(t *testing.T) {
		env := newEnv(t)
		require.NoError(t, env.sourceJobsDB.Store(t.Context(), env.generateJobs(t, "1", 5000)))

		c := config.New()
		c.Set("LOG_LEVEL", "DEBUG")
		c.Set("PartitionMigration.Executor.BatchSize", 50)
		c.Set("PartitionMigration.Executor.ChunkSize", 10)
		c.Set("PartitionMigration.Executor.StreamTimeout", "1m")
		c.Set("PartitionMigration.Executor.ProgressPeriod", "100ms")

		mpe := NewMigrationJobExecutor("job1", []string{"1"}, env.sourceJobsDB, env.target, WithConfig(c), WithLogger(logger.NewFactory(c).NewLogger()))

		// run migration with a timeout context to simulate cancellation
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
		err := mpe.Run(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded, "migration job should be cancelled due to context deadline")

		// resume migration
		err = mpe.Run(t.Context())
		require.NoError(t, err, "migration job should complete successfully")

		// verify jobs in target
		p1Target := env.countJobsForPartition(t, env.targetDB, env.targetJobsDB.Identifier(), "1")
		require.Equal(t, 5000, p1Target, "all jobs for partition 1 should be migrated to target")

		// verify jobs in source
		p1Source := env.countJobsForPartition(t, env.sourceDB, env.sourceJobsDB.Identifier(), "1")
		require.Equal(t, 0, p1Source, "all jobs for partition 1 should be removed from source")
	})

	t.Run("send timeout", func(t *testing.T) {
		env := newEnv(t)
		env.pms.mock = true
		env.pms.receive = false // server will not receive any messages

		err := env.sourceJobsDB.Store(t.Context(), env.generateJobs(t, "1", 1000))
		require.NoError(t, err)

		c := config.New()
		c.Set("LOG_LEVEL", "DEBUG")
		c.Set("PartitionMigration.Executor.BatchSize", 500)
		c.Set("PartitionMigration.Executor.ChunkSize", 200)
		c.Set("PartitionMigration.Executor.SendTimeout", "1s")
		c.Set("PartitionMigration.Executor.ProgressPeriod", "1s")

		mpe := NewMigrationJobExecutor("job1", []string{"1"}, env.sourceJobsDB, env.target, WithConfig(c), WithLogger(logger.NewFactory(c).NewLogger()))
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		err = mpe.Run(ctx)
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.DeadlineExceeded, st.Code())
		require.ErrorContains(t, err, "stream send operation timed out after 1s")
	})

	t.Run("receive timeout", func(t *testing.T) {
		env := newEnv(t)
		env.pms.mock = true
		env.pms.receive = true
		env.pms.send = false // server will not send acks back

		err := env.sourceJobsDB.Store(t.Context(), env.generateJobs(t, "1", 1000))
		require.NoError(t, err)

		c := config.New()
		c.Set("LOG_LEVEL", "DEBUG")
		c.Set("PartitionMigration.Executor.BatchSize", 100)
		c.Set("PartitionMigration.Executor.ChunkSize", 10)
		c.Set("PartitionMigration.Executor.ReceiveTimeout", "1s")
		c.Set("PartitionMigration.Executor.ProgressPeriod", "1s")

		mpe := NewMigrationJobExecutor("job1", []string{"1"}, env.sourceJobsDB, env.target, WithConfig(c), WithLogger(logger.NewFactory(c).NewLogger()))
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		err = mpe.Run(ctx)
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.DeadlineExceeded, st.Code())
		require.ErrorContains(t, err, "stream receive operation timed out after 1s")
	})

	t.Run("server not listening", func(t *testing.T) {
		env := newEnv(t)
		err := env.sourceJobsDB.Store(t.Context(), env.generateJobs(t, "1", 1))
		require.NoError(t, err)

		c := config.New()
		c.Set("LOG_LEVEL", "DEBUG")
		c.Set("PartitionMigration.Executor.BatchSize", 500)
		c.Set("PartitionMigration.Executor.ChunkSize", 200)
		c.Set("PartitionMigration.Executor.SendTimeout", "1s")
		c.Set("PartitionMigration.Executor.ProgressPeriod", "1s")

		port, err := testhelper.GetFreePort()
		require.NoError(t, err)
		invalidTarget := "127.0.0.1:" + strconv.Itoa(port)
		mpe := NewMigrationJobExecutor("job1", []string{"1"}, env.sourceJobsDB, invalidTarget, WithConfig(c), WithLogger(logger.NewFactory(c).NewLogger()))
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		err = mpe.Run(ctx)
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Unavailable, st.Code())
		require.ErrorContains(t, err, "Error while dialing")
	})
}

type testMigrationJobExecutorEnv struct {
	target       string      // address of the target grpc server
	sourceDB     *sql.DB     // underlying sql.DB
	sourceJobsDB *mockJobsDB // jobsdb instance used by the grpc server, with mocking capabilities for error reproduction

	targetDB     *sql.DB     // underlying sql.DB of the target
	targetJobsDB *mockJobsDB // jobsdb instance used by the grpc server, with mocking capabilities for error reproduction

	pms *mockPartitionMigrationServer
}

// generateJobs creates a list of jobsdb.JobT for testing purposes with increasing JobIDs.
func (testMigrationJobExecutorEnv) generateJobs(t *testing.T, partitionID string, count int) []*jobsdb.JobT {
	t.Helper()
	jobs := make([]*jobsdb.JobT, 0, count)
	for range count {
		jobs = append(jobs, &jobsdb.JobT{
			UUID:       uuid.New(),
			UserID:     "user-1",
			CreatedAt:  time.Now(),
			ExpireAt:   time.Now(),
			CustomVal:  "test",
			EventCount: 1,
			EventPayload: []byte(`{
  "requestId": "c2b7a1d4-9e34-4c88-a9a3-71b2c5f0e921",
  "timestamp": "2025-12-22T10:48:12Z",
  "service": "example-api",
  "environment": "test",
  "user": {
    "id": "user_10293",
    "email": "user_10293@example.com",
    "roles": ["viewer", "contributor"],
    "active": true
  },
  "metrics": {
    "cpu": 0.73,
    "memory": 512,
    "latencyMs": 183
  },
  "data": [
    {
      "id": "a1",
      "value": "X7qM2F9aK4T8B0WcPZJrN5U3H"
    },
    {
      "id": "b2",
      "value": "QW8R7Y6Z5P4A3S2D1F0GHJKL"
    },
    {
      "id": "c3",
      "value": "mN7ZxA9H3FJ6D0qK4R8P5T"
    }
  ],
  "notes": "Generated payload for testing migration job executor with payloads that can fill the grpc buffers."
}
`),
			Parameters:  []byte(`{"source_id":"source-1", "destination_id":"dest-1"}`),
			WorkspaceId: "workspace-1",
			PartitionID: partitionID,
		})
	}
	return jobs
}

func (testMigrationJobExecutorEnv) generateJobStatuses(t *testing.T, jobs []*jobsdb.JobT, status string) []*jobsdb.JobStatusT {
	t.Helper()
	now := time.Now()
	return lo.Map(jobs, func(job *jobsdb.JobT, _ int) *jobsdb.JobStatusT {
		return &jobsdb.JobStatusT{
			JobID:         job.JobID,
			JobState:      status,
			AttemptNum:    1,
			ExecTime:      now,
			RetryTime:     now,
			ErrorCode:     "200",
			ErrorResponse: []byte("{}"),
			Parameters:    []byte("{}"),
			JobParameters: job.Parameters,
			WorkspaceId:   job.WorkspaceId,
			PartitionID:   job.PartitionID,
		}
	})
}

func (testMigrationJobExecutorEnv) countJobsForPartition(t *testing.T, db *sql.DB, prefix, partitionID string) int {
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

// mockJobsDB is a wrapper around jobsdb.JobsDB that allows simulating errors during operations.
type mockJobsDB struct {
	jobsdb.JobsDB
	storeErr error
	txErr    error
}

// WithTx overrides the JobsDB's WithTx method to simulate a transaction begin error if txErr is set.
func (m *mockJobsDB) WithTx(fn func(tx *tx.Tx) error) error {
	if m.txErr != nil {
		return m.txErr
	}
	return m.JobsDB.WithTx(fn)
}

// StoreInTx overrides the JobsDB's StoreInTx method to simulate a store error if storeErr is set.
func (m *mockJobsDB) StoreInTx(ctx context.Context, tx jobsdb.StoreSafeTx, jobList []*jobsdb.JobT) error {
	if m.storeErr != nil {
		return m.storeErr
	}
	return m.JobsDB.StoreInTx(ctx, tx, jobList)
}

// runNodeMigration runs the node migrations on the provided database handle.
func runNodeMigration(t *testing.T, db *sql.DB) {
	t.Helper()
	m := &migrator.Migrator{
		Handle:          db,
		MigrationsTable: "node_migrations",
	}
	require.NoError(t, m.Migrate("node"))
}

type mockPartitionMigrationServer struct {
	proto.PartitionMigrationServer
	mock    bool  // whether to use the mock behavior
	err     error // error to return from StreamJobs if mock is true
	receive bool  // whether to receive messages from the stream if mock is true
	send    bool  // whether to send acks back to the client if mock is true
}

func (m *mockPartitionMigrationServer) StreamJobs(stream proto.PartitionMigration_StreamJobsServer) error {
	if !m.mock {
		return m.PartitionMigrationServer.StreamJobs(stream)
	}
	if m.err != nil {
		return m.err
	}
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-time.After(10 * time.Millisecond):
		}
		if m.receive {
			r, err := stream.Recv()
			if err != nil {
				return err
			}
			if r.GetChunk() != nil && m.send {
				err := stream.Send(&proto.JobsBatchAck{BatchIndex: r.GetChunk().GetBatchIndex()})
				if err != nil {
					return err
				}
			}
		}
	}
}
