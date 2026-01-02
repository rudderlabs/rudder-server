package partitionmigration_test

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/cluster/migrator/partitionmigration/client"
	"github.com/rudderlabs/rudder-server/cluster/migrator/partitionmigration/server"
	"github.com/rudderlabs/rudder-server/jobsdb"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
)

// Test scenario:
//   - Start a grpc server setup with to 2 jobsdbs. We will be restarting this server every [restartServerEvery] seconds to simulate failures and pod restarts.
//   - Seed both source jobsdbs with jobs for [numPartitions] different partitions. Each partition will have [jobsPerPartition] jobs and their payload will contain an incremental job index, starting from 1 to [jobsPerPartition].
//   - Start multiple different migration job executors from each source, each responsible for migrating a different set of partitions [partitionsToMigrate]. We will be restarting these executors every [restartExecutorEvery] seconds to simulate failures and pod restarts.
//   - Start another goroutine for each target jobsdb that will be querying for jobs from all partitions and validating that the jobs are being migrated correctly and in order.
//   - Wait up to [executionTimeout] until all jobs are migrated and verified.
func TestMigrationJobExecutorIntegrationTest(t *testing.T) {
	const (
		restartServerEvery   = 3 * time.Second // how often to restart the grpc server
		restartExecutorEvery = 2 * time.Second // how often to restart each migration executor
		executionTimeout     = 5 * time.Minute // overall timeout for the executors and verifiers
		numPartitions        = 10              // total number of partitions to seed
		jobsPerPartition     = 20000           // number of jobs to seed per partition
		chunkSize            = 20              // seed jobs in chunks of this size
	)
	partitionsToMigrate := [][]string{{"4", "5"}, {"7"}, {"9", "10"}} // partitions to be migrated grouped by executor

	env := func(t *testing.T) *itMigrationJobExecutorEnv {
		env := &itMigrationJobExecutorEnv{}
		pool, err := dockertest.NewPool("")
		require.NoError(t, err)

		setupSqlDB := func(t *testing.T) *sql.DB { // setup a new postgres instance and run node migrations
			t.Helper()
			pg, err := postgres.Setup(pool, t)
			require.NoError(t, err)
			runNodeMigration(t, pg.DB)
			return pg.DB
		}

		setupJobsDB := func(t *testing.T, prefix string, sqlDB *sql.DB) jobsdb.JobsDB { // setup a new jobsdb instance
			t.Helper()
			c := config.New()
			c.Set("JobsDB.maxDSSize", 10000)
			db := jobsdb.NewForReadWrite(prefix,
				jobsdb.WithDBHandle(sqlDB),
				jobsdb.WithNumPartitions(64),
				jobsdb.WithSkipMaintenanceErr(true),
				jobsdb.WithConfig(c),
				jobsdb.WithDSLimit(config.SingleValueLoader(2)),
			)
			err = db.Start()
			require.NoError(t, err)
			t.Cleanup(func() {
				db.Stop()
			})
			return db
		}

		// setup source
		env.sourceDB = setupSqlDB(t)
		env.sourceJobsDB1 = setupJobsDB(t, "rt", env.sourceDB)
		env.sourceJobsDB2 = setupJobsDB(t, "batch_rt", env.sourceDB)

		// setup target
		env.targetDB = setupSqlDB(t)
		env.targetJobsDB1 = setupJobsDB(t, "rt", env.targetDB)
		env.targetJobsDB2 = setupJobsDB(t, "batch_rt", env.targetDB)

		// setup server config
		env.serverPort, err = testhelper.GetFreePort()
		require.NoError(t, err)
		env.target = "127.0.0.1:" + strconv.Itoa(env.serverPort)
		env.serverConf = config.New()
		env.serverConf.Set("PartitionMigration.Grpc.Server.Port", env.serverPort)
		env.serverConf.Set("PartitionMigration.Grpc.Server.StopTimeout", "2s")
		env.serverConf.Set("LOG_LEVEL", "ERROR")

		return env
	}(t)

	t.Logf("seeding jobsdbs with data...")
	seedWg, ctx := errgroup.WithContext(t.Context())
	for p := 1; p <= numPartitions; p++ {
		partitionID := strconv.Itoa(p)
		seedWg.Go(func() error { // seed jobsdb1
			t.Logf("Seeding jobs for partition %q and tablePrefix %q", partitionID, env.sourceJobsDB1.Identifier())
			return env.seedJobs(ctx, env.sourceJobsDB1, partitionID, jobsPerPartition, chunkSize)
		})
		seedWg.Go(func() error { // seed jobsdb2
			partitionID := strconv.Itoa(p)
			t.Logf("Seeding jobs for partition %q and tablePrefix %q", partitionID, env.sourceJobsDB2.Identifier())
			return env.seedJobs(ctx, env.sourceJobsDB2, partitionID, jobsPerPartition, chunkSize)
		})
	}
	err := seedWg.Wait()
	require.NoError(t, err, "it should be able to seed jobsdbs with data")
	t.Logf("done seeding jobsdbs with data...")

	t.Logf("starting gRPC server")
	env.runServer(t, restartServerEvery)

	ctx, cancel := context.WithTimeout(t.Context(), executionTimeout) // overall timeout for the executors and verifiers
	defer cancel()

	t.Logf("starting migration executors...")
	executorWG, executorCtx := errgroup.WithContext(ctx)
	for _, partitions := range partitionsToMigrate {
		executorWG.Go(func() error { // executor for sourceJobsDB1
			return env.runMigrationExecutor(t, executorCtx, partitions, env.sourceJobsDB1, restartExecutorEvery)
		})
		executorWG.Go(func() error { // executor for sourceJobsDB2
			return env.runMigrationExecutor(t, executorCtx, partitions, env.sourceJobsDB2, restartExecutorEvery)
		})
	}

	t.Logf("starting verifiers")
	verifierWG, verifierCtx := errgroup.WithContext(ctx)
	for _, db := range []jobsdb.JobsDB{env.targetJobsDB1, env.targetJobsDB2} {
		verifierWG.Go(func() error {
			return env.runVerifier(t, verifierCtx, db, jobsPerPartition, len(lo.Flatten(partitionsToMigrate)))
		})
	}

	t.Logf("waiting for executors to finish...")
	err = executorWG.Wait()
	require.NoError(t, err, "executors should finish without error")

	t.Logf("waiting for verifiers to finish...")
	err = verifierWG.Wait()
	require.NoError(t, err, "verifiers should finish without error")

	t.Logf("gRPC server restarts: %d", env.serverRestarts.Load())
	t.Logf("Migration executor restarts: %d", env.executorRestarts.Load())
}

type itMigrationJobExecutorEnv struct {
	target        string  // address of the target grpc server
	sourceDB      *sql.DB // underlying sql.DB
	sourceJobsDB1 jobsdb.JobsDB
	sourceJobsDB2 jobsdb.JobsDB

	targetDB      *sql.DB // underlying sql.DB of the target
	targetJobsDB1 jobsdb.JobsDB
	targetJobsDB2 jobsdb.JobsDB

	serverPort int
	serverConf *config.Config

	serverRestarts   atomic.Int64
	executorRestarts atomic.Int64
}

// seedJobs seeds [num] jobs into the provided jobsdb for the given partitionID in chunks of [chunkSize].
func (*itMigrationJobExecutorEnv) seedJobs(ctx context.Context, db jobsdb.JobsDB, partitionID string, num, chunkSize int) error {
	var jobs []*jobsdb.JobT
	for i := 1; i <= num; i++ {
		jobs = append(jobs, &jobsdb.JobT{
			UUID:         uuid.New(),
			UserID:       "user-1",
			CustomVal:    "test",
			EventCount:   1,
			Parameters:   []byte(`{"source_id": "source-1", "destination_id": "dest-2"}`),
			EventPayload: []byte(`{"idx": ` + strconv.Itoa(i) + `}`),
			WorkspaceId:  "workspace-1",
			PartitionID:  partitionID,
		})
	}
	for _, chunk := range lo.Chunk(jobs, chunkSize) {
		time.Sleep(time.Millisecond)
		if err := db.Store(ctx, chunk); err != nil {
			return err
		}
	}
	return nil
}

// runServer starts a grpc server that will be restarted every [restartEvery] duration to simulate
// failures and pod restarts.
func (env *itMigrationJobExecutorEnv) runServer(t *testing.T, restartEvery time.Duration) {
	t.Helper()
	pms := server.NewPartitionMigrationServer(t.Context(), []jobsdb.JobsDB{env.targetJobsDB1, env.targetJobsDB2}, server.WithLogger(logger.NewFactory(env.serverConf).NewLogger()))
	var wg sync.WaitGroup
	stop := make(chan struct{})
	t.Cleanup(func() {
		close(stop)
		wg.Wait()
	})
	wg.Go(func() {
		ticker := time.NewTicker(restartEvery)
		defer ticker.Stop()
		for {
			grpcServer := server.NewGRPCServer(env.serverConf, pms)
			require.NoError(t, grpcServer.Start(), "it should be able to start the grpc server")
			t.Logf("started grpc server")
			select {
			case <-ticker.C:
				grpcServer.Stop()
				t.Logf("stopped grpc server for restart")
			case <-t.Context().Done():
				grpcServer.Stop()
				t.Logf("stopped grpc server due to test context done")
				return
			case <-stop:
				grpcServer.Stop()
				t.Logf("stopped grpc server due to test cleanup")
				return
			}
			env.serverRestarts.Add(1)
		}
	})
}

// runMigrationExecutor runs a migration executor for the provided partitions and source jobsdb. It
// restarts the executor every [restartEvery] duration to simulate failures and pod restarts.
func (env *itMigrationJobExecutorEnv) runMigrationExecutor(t *testing.T, ctx context.Context, partitions []string, sourceJobsDB jobsdb.JobsDB, restartEvery time.Duration) error {
	t.Logf("Starting partition migration executor for migrating %+v from %q", partitions, sourceJobsDB.Identifier())
	t.Helper()
	c := config.New()
	c.Set("LOG_LEVEL", "INFO")
	c.Set("PartitionMigration.Executor.BatchSize", 100)
	c.Set("PartitionMigration.Executor.ChunkSize", 20)
	jobID := "job-" + uuid.New().String()
	mpe := client.NewMigrationJobExecutor(jobID, partitions, sourceJobsDB, env.target, client.WithConfig(c), client.WithLogger(logger.NewFactory(c).NewLogger()))
	for {
		restartingCtx, cancel := context.WithTimeout(ctx, restartEvery)
		t.Logf("Running partition migration executor for migrating %+v from %q", partitions, sourceJobsDB.Identifier())
		err := mpe.Run(restartingCtx)
		cancel()
		if err == nil {
			t.Logf("Partition migration executor for migrating %+v from %q completed successfully", partitions, sourceJobsDB.Identifier())
			return nil
		}
		if ctx.Err() != nil {
			t.Logf("Partition migration executor for migrating %+v from %q failed: %v", partitions, sourceJobsDB.Identifier(), err)
			return err
		}
		env.executorRestarts.Add(1)
		t.Logf("Partition migration executor for migrating %+v from %q restarting after error: %v", partitions, sourceJobsDB.Identifier(), err)
	}
}

// runVerifier validates migration correctness by:
// 1. Consuming jobs from target JobsDB as they arrive
// 2. Verifying job ordering within each partition (idx must be sequential)
// 3. Detecting duplicates (same idx seen twice would indicate dedup failure)
// 4. Completing when all partitions reach maxIndex
func (env *itMigrationJobExecutorEnv) runVerifier(t *testing.T, ctx context.Context, db jobsdb.JobsDB, maxIndex, partitions int) error {
	t.Logf("Starting jobsdb verifier for %q", db.Identifier())
	t.Helper()
	indexesMap := make(map[string]int64) // partitionID -> last seen index
	lastLog := time.Now()
	for {
		toProcess, err := db.GetToProcess(ctx, jobsdb.GetQueryParams{JobsLimit: 100, CustomValFilters: []string{"test"}}, nil)
		if err != nil {
			return fmt.Errorf("getting to process: %w", err)
		}

		if len(toProcess.Jobs) > 0 {
			// mark jobs as processed
			err := db.UpdateJobStatus(ctx, lo.Map(toProcess.Jobs, func(job *jobsdb.JobT, _ int) *jobsdb.JobStatusT {
				return &jobsdb.JobStatusT{
					JobID:         job.JobID,
					JobState:      jobsdb.Succeeded.State,
					AttemptNum:    1,
					ExecTime:      time.Now(),
					ErrorCode:     "200",
					ErrorResponse: []byte("{}"),
					Parameters:    []byte("{}"),
					JobParameters: job.Parameters,
					WorkspaceId:   job.WorkspaceId,
					PartitionID:   job.PartitionID,
				}
			}), []string{"test"}, nil)
			if err != nil {
				return fmt.Errorf("updating job statuses: %w", err)
			}
		}
		for _, job := range toProcess.Jobs {
			partition := job.PartitionID
			idx := gjson.GetBytes(job.EventPayload, "idx").Int()
			lastSeenIdx, ok := indexesMap[partition]
			if ok && idx != lastSeenIdx+1 {
				return fmt.Errorf("verifier for %q: expected next index for partition %q to be %d, got %d", db.Identifier(), partition, lastSeenIdx+1, idx)
			}
			indexesMap[partition] = idx
		}
		if len(indexesMap) == partitions && lo.EveryBy(lo.Values(indexesMap), func(idx int64) bool {
			return idx == int64(maxIndex)
		}) {
			t.Logf("Jobsdb verifier for %q completed successfully", db.Identifier())
			return nil
		} else if time.Since(lastLog) > 10*time.Second {
			t.Logf("Jobsdb verifier for %q still running...\nmaxIndex: %d\npartitions: %d\nindexesMap:%+v", db.Identifier(), maxIndex, partitions, indexesMap)

			lastLog = time.Now()
		}
	}
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
