package rsources

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"testing"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
)

var (
	DB_DSN = "root@tcp(127.0.0.1:3306)/service"
	db     *sql.DB
)

func TestMain(m *testing.M) {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	database := "jobsdb"
	// pulls an image, creates a container based on it and runs it
	resourcePostgres, err := pool.Run("postgres", "11-alpine", []string{
		"POSTGRES_PASSWORD=password",
		"POSTGRES_DB=" + database,
		"POSTGRES_USER=rudder",
	})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	port := resourcePostgres.GetPort("5432/tcp")
	DB_DSN = fmt.Sprintf("postgres://rudder:password@localhost:%s/%s?sslmode=disable", port, database)
	fmt.Println("DB_DSN:", DB_DSN)

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		var err error
		db, err = sql.Open("postgres", fmt.Sprintf(
			"host=localhost port=%s user=rudder password=password dbname=jobsdb sslmode=disable",
			port))
		if err != nil {
			log.Println(err)
			return err
		}
		return db.Ping()
	}); err != nil {
		log.Fatalf("Could not connect to docker after backoff: %s", err)
	}

	code := m.Run()
	// blockOnHold()
	defer os.Exit(code)
	defer func() {
		if err := pool.Purge(resourcePostgres); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()
}

// Scenario 1: statusFromQueryResult for
// 1. one task with one source with one destination, all completed
// 2. one task with one source with one destination, destination not completed
// 3. one task with one source with one destination, source not completed
// 4. two tasks with one source each (same id) and one destination each (same id), one task completed, other not
// task1 -> source1(completed) -> dest1(completed)
// task2 -> source1(incomplete) -> dest1(incomplete)

// Scenario 2: IncrementStats a) once, b) twice -> verify using GetStatus

// Scenario 3: GetStatus filtering a) TaskRunId (1 & 2) b) SourceId (1 & 2)
// task1 -> source1 & source2
// task2 -> source2 & source3

// Scenario 4: Delete

// func blockOnHold() {
// 	fmt.Println("Test on hold, before cleanup")
// 	fmt.Println("Press Ctrl+C to exit")

// 	c := make(chan os.Signal, 1)
// 	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

// 	<-c
// }

func TestSourcesHandler(t *testing.T) {
	sh := NewJobService(db)
	ctx := context.Background()
	key := JobTargetKey{
		SourceId:      "source_id",
		DestinationId: "destination_id",
		TaskRunId:     "task_run_id",
	}
	stats := Stats{
		In:     10,
		Out:    4,
		Failed: 6,
	}
	tx, _ := db.Begin()

	sh.IncrementStats(ctx, tx, "jobRunId", key, stats)
	err := tx.Commit()
	if err != nil {
		t.Errorf("Error committing transaction: %s", err)
	}
	t.Run("gets status", func(t *testing.T) {
		jobFilters := JobFilter{
			SourceId:  []string{"source_id"},
			TaskRunId: []string{"task_run_id"},
		}
		status, err := sh.GetStatus(ctx, "jobRunId", jobFilters)
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, status.ID, "jobRunId")
		require.Equal(t, len(status.TasksStatus), 1)
		require.Equal(t, len(status.TasksStatus[0].SourcesStatus), 1)
		require.Equal(t,
			len(status.TasksStatus[0].SourcesStatus[0].DestinationsStatus),
			1)
		require.Equal(t,
			status.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].ID,
			"destination_id")
		require.Equal(t,
			status.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Completed,
			true)
		require.Equal(t,
			status.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Stats.In,
			uint(10))
		require.Equal(t,
			status.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Stats.Out,
			uint(4))
		require.Equal(t,
			status.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Stats.Failed,
			uint(6))
	})

	t.Run("Delete clears all the pertinent tables(for now the stats table only)", func(t *testing.T) {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		_ = sh.Delete(ctx, "jobRunId")
		err = tx.Commit()
		if err != nil {
			t.Fatal(err)
		}
		jobFilters := JobFilter{
			SourceId:  []string{"source_id"},
			TaskRunId: []string{"task_run_id"},
		}
		status, err := sh.GetStatus(ctx, "jobRunId", jobFilters)
		require.Equal(t, status, JobStatus{})
		pqErr := err.(*pq.Error)
		require.Equal(t, pqErr.Code, pq.ErrorCode("42P01")) // relation does not exist
	})

	t.Run("A table(per jobrunid) is created only once if two goroutines compete", func(t *testing.T) {
		// TODO: test two goroutines competing here
		sh := NewJobService(db)
		wg := &sync.WaitGroup{}
		wg.Add(3)
		go increment(ctx, db, JobTargetKey{
			SourceId:      "source_id1",
			DestinationId: "destination_id1",
			TaskRunId:     "task_run_id",
		}, Stats{
			In:     10,
			Out:    4,
			Failed: 6,
		}, sh, wg)
		go increment(ctx, db, JobTargetKey{
			SourceId:      "source_id1",
			DestinationId: "destination_id2",
			TaskRunId:     "task_run_id",
		}, Stats{
			In:     12,
			Out:    4,
			Failed: 6,
		}, sh, wg)
		go increment(ctx, db, JobTargetKey{
			SourceId:      "source_id2",
			DestinationId: "destination_id1",
			TaskRunId:     "task_run_id",
		}, Stats{
			In:     12,
			Out:    6,
			Failed: 6,
		}, sh, wg)

		wg.Wait()
		res, err := sh.GetStatus(ctx, "jobRunId", JobFilter{})
		sort.Slice(res.TasksStatus[0].SourcesStatus, func(i, j int) bool {
			return res.TasksStatus[0].SourcesStatus[i].ID < res.TasksStatus[0].SourcesStatus[j].ID
		})
		require.NoError(t, err)
		require.Equal(t, len(res.TasksStatus), 1)
		require.Equal(t, len(res.TasksStatus[0].SourcesStatus), 2)
		sort.Slice(res.TasksStatus[0].SourcesStatus[0].DestinationsStatus, func(i, j int) bool {
			return res.TasksStatus[0].SourcesStatus[0].DestinationsStatus[i].ID < res.TasksStatus[0].SourcesStatus[0].DestinationsStatus[j].ID
		})
		require.Equal(t, len(res.TasksStatus[0].SourcesStatus[0].DestinationsStatus), 2)
		require.Equal(t, len(res.TasksStatus[0].SourcesStatus[1].DestinationsStatus), 1)
		require.Equal(t, res.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Completed, true)
		require.Equal(t, res.TasksStatus[0].SourcesStatus[0].DestinationsStatus[1].Completed, false)
		require.Equal(t, res.TasksStatus[0].SourcesStatus[0].Completed, false)
		require.Equal(t, res.TasksStatus[0].SourcesStatus[1].DestinationsStatus[0].Completed, true)
		require.Equal(t, res.TasksStatus[0].SourcesStatus[1].Completed, true)
		_ = sh.Delete(ctx, "jobRunId")
	})

	t.Run("GetStatus", func(t *testing.T) {
		sh := NewJobService(db)
		wg := &sync.WaitGroup{}
		wg.Add(4)
		increment(ctx, db, JobTargetKey{
			SourceId:      "source_id1",
			DestinationId: "destination_id",
			TaskRunId:     "task_run_id1",
		}, Stats{
			In:     10,
			Out:    4,
			Failed: 6,
		}, sh, wg,
		)
		increment(ctx, db, JobTargetKey{
			SourceId:      "source_id2",
			DestinationId: "destination_id",
			TaskRunId:     "task_run_id1",
		}, Stats{
			In:     10,
			Out:    2,
			Failed: 6,
		}, sh, wg,
		)
		increment(ctx, db, JobTargetKey{
			SourceId:      "source_id2",
			DestinationId: "destination_id",
			TaskRunId:     "task_run_id2",
		}, Stats{
			In:     10,
			Out:    2,
			Failed: 6,
		}, sh, wg,
		)
		increment(ctx, db, JobTargetKey{
			SourceId:      "source_id3",
			DestinationId: "destination_id",
			TaskRunId:     "task_run_id2",
		}, Stats{
			In:     10,
			Out:    2,
			Failed: 6,
		}, sh, wg,
		)
		wg.Wait()

		res, err := sh.GetStatus(ctx, "jobRunId", JobFilter{
			SourceId:  []string{"source_id1", "source_id2"},
			TaskRunId: []string{"task_run_id1", "task_run_id2"},
		})
		sort.Slice(res.TasksStatus, func(i, j int) bool {
			return res.TasksStatus[i].ID < res.TasksStatus[j].ID
		})
		require.NoError(t, err)
		require.Equal(t, len(res.TasksStatus), 2)

		sort.Slice(res.TasksStatus[0].SourcesStatus, func(i, j int) bool {
			return res.TasksStatus[0].SourcesStatus[i].ID < res.TasksStatus[0].SourcesStatus[j].ID
		})
		require.Equal(t, len(res.TasksStatus[0].SourcesStatus), 2)
		require.Equal(t, len(res.TasksStatus[0].SourcesStatus[0].DestinationsStatus), 1)
		require.Equal(t, len(res.TasksStatus[0].SourcesStatus[1].DestinationsStatus), 1)
		require.Equal(t, res.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Completed, true)
		require.Equal(t, res.TasksStatus[0].SourcesStatus[0].Completed, true)
		require.Equal(t, res.TasksStatus[0].SourcesStatus[1].Completed, false)

		sort.Slice(res.TasksStatus[1].SourcesStatus, func(i, j int) bool {
			return res.TasksStatus[1].SourcesStatus[i].ID < res.TasksStatus[1].SourcesStatus[j].ID
		})
		require.Equal(t, len(res.TasksStatus[1].SourcesStatus), 1)
		require.Equal(t, len(res.TasksStatus[1].SourcesStatus[0].DestinationsStatus), 1)
		require.Equal(t, res.TasksStatus[1].SourcesStatus[0].DestinationsStatus[0].Completed, false)
	})
}

func increment(ctx context.Context, db *sql.DB, key JobTargetKey, stat Stats, sh JobService, wg *sync.WaitGroup) {
	tx, _ := db.Begin()
	sh.IncrementStats(ctx, tx, "jobRunId", key, stat)
	_ = tx.Commit()
	wg.Done()
}

func TestStatusFromQueryResult(t *testing.T) {
	t.Run("one task with one source with one destination, all completed", func(t *testing.T) {
		res := statusFromQueryResult("jobRunId", map[JobTargetKey]Stats{
			{
				SourceId:      "source_id",
				DestinationId: "destination_id",
				TaskRunId:     "task_run_id",
			}: {
				In:     10,
				Out:    4,
				Failed: 6,
			},
		},
		)

		require.Equal(t, res.ID, "jobRunId")
		require.Equal(t, len(res.TasksStatus), 1)
		require.Equal(t, len(res.TasksStatus[0].SourcesStatus), 1)
		require.Equal(t, len(res.TasksStatus[0].SourcesStatus[0].DestinationsStatus), 1)
		require.Equal(t, res.TasksStatus[0].SourcesStatus[0].Completed, true)
		require.Equal(t, res.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Completed, true)
	})

	t.Run("one task with one source with one destination, destination not completed", func(t *testing.T) {
		res := statusFromQueryResult("jobRunId", map[JobTargetKey]Stats{
			{
				SourceId:      "source_id",
				DestinationId: "destination_id",
				TaskRunId:     "task_run_id",
			}: {
				In:     10,
				Out:    3,
				Failed: 6,
			},
		})

		require.Equal(t, res.ID, "jobRunId")
		require.Equal(t, len(res.TasksStatus), 1)
		require.Equal(t, len(res.TasksStatus[0].SourcesStatus), 1)
		require.Equal(t, len(res.TasksStatus[0].SourcesStatus[0].DestinationsStatus), 1)
		require.Equal(t, res.TasksStatus[0].SourcesStatus[0].Completed, false)
		require.Equal(t, res.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Completed, false)
	})

	t.Run("one task with one source with one destination, source not completed", func(t *testing.T) {
		res := statusFromQueryResult("jobRunId", map[JobTargetKey]Stats{
			{
				SourceId:      "source_id",
				DestinationId: "destination_id",
				TaskRunId:     "task_run_id",
			}: {
				In:     10,
				Out:    4,
				Failed: 6,
			},
			{
				SourceId:  "source_id",
				TaskRunId: "task_run_id",
			}: {
				In:     10,
				Out:    3,
				Failed: 6,
			},
		})

		require.Equal(t, res.ID, "jobRunId")
		require.Equal(t, len(res.TasksStatus), 1)
		require.Equal(t, len(res.TasksStatus[0].SourcesStatus), 1)
		require.Equal(t, len(res.TasksStatus[0].SourcesStatus[0].DestinationsStatus), 1)
		require.Equal(t, res.TasksStatus[0].SourcesStatus[0].Completed, false)
		require.Equal(t, res.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Completed, true)
	})

	t.Run("two tasks with one source each (same id) and one destination each (same id), one task completed, other not", func(t *testing.T) {
		res := statusFromQueryResult("jobRunId", map[JobTargetKey]Stats{
			{
				SourceId:      "source_id",
				DestinationId: "destination_id",
				TaskRunId:     "task_run_id1",
			}: {
				In:     10,
				Out:    4,
				Failed: 6,
			},
			{
				SourceId:      "source_id",
				DestinationId: "destination_id",
				TaskRunId:     "task_run_id2",
			}: {
				In:     10,
				Out:    3,
				Failed: 6,
			},
		})

		sort.Slice(res.TasksStatus, func(i, j int) bool {
			return res.TasksStatus[i].ID < res.TasksStatus[j].ID
		})
		require.Equal(t, res.ID, "jobRunId")
		require.Equal(t, len(res.TasksStatus), 2)
		require.Equal(t, len(res.TasksStatus[0].SourcesStatus), 1)
		require.Equal(t, len(res.TasksStatus[0].SourcesStatus[0].DestinationsStatus), 1)
		require.Equal(t, res.TasksStatus[0].SourcesStatus[0].Completed, true)
		require.Equal(t, res.TasksStatus[0].SourcesStatus[0].DestinationsStatus[0].Completed, true)
		require.Equal(t, len(res.TasksStatus[1].SourcesStatus), 1)
		require.Equal(t, len(res.TasksStatus[1].SourcesStatus[0].DestinationsStatus), 1)
		require.Equal(t, res.TasksStatus[1].SourcesStatus[0].Completed, false)
		require.Equal(t, res.TasksStatus[1].SourcesStatus[0].DestinationsStatus[0].Completed, false)
	})
}
