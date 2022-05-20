package rsources

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"testing"

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
	tx, err := db.Begin()
	require.NoError(t, err, "it should be able to begin the transaction")

	require.NoError(t, sh.IncrementStats(ctx, tx, "jobRunId", key, stats), "it should be able to increment stats")

	require.NoError(t, tx.Commit(), "it should be able to commit the transaction")

	t.Run("Get Status", func(t *testing.T) {
		jobFilters := JobFilter{
			SourceId:  []string{"source_id"},
			TaskRunId: []string{"task_run_id"},
		}
		status, err := sh.GetStatus(ctx, "jobRunId", jobFilters)
		if err != nil {
			t.Fatal(err)
		}
		expected := JobStatus{
			ID: "jobRunId",
			TasksStatus: []TaskStatus{
				{
					ID: "task_run_id",
					SourcesStatus: []SourceStatus{
						{
							ID:        "source_id",
							Stats:     Stats{},
							Completed: true,
							DestinationsStatus: []DestinationStatus{
								{
									ID:        "destination_id",
									Completed: true,
									Stats:     stats,
								},
							},
						},
					},
				},
			},
		}
		require.Equal(t, expected, status)
	})

	t.Run("Delete clears all the pertinent tables(for now the stats table only)", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err, "it should be able to begin the transaction")
		err = sh.Delete(ctx, "jobRunId")
		require.NoError(t, err, "it should be able to delete")
		err = tx.Commit()
		require.NoError(t, err, "it should be able to commit the transaction")
		jobFilters := JobFilter{
			SourceId:  []string{"source_id"},
			TaskRunId: []string{"task_run_id"},
		}
		status, err := sh.GetStatus(ctx, "jobRunId", jobFilters)
		require.NotNil(t, err)
		require.Equal(t, status, JobStatus{})
		require.True(t, errors.Is(err, StatusNotFoundError), "it should return a StatusNotFoundError")

	})

	t.Run("GetStatus with filtering", func(t *testing.T) {
		sh := NewJobService(db)
		wg := &sync.WaitGroup{}
		wg.Add(5)
		go increment(t, ctx, db, JobTargetKey{
			TaskRunId:     "task_run_id1",
			SourceId:      "source_id1",
			DestinationId: "destination_id",
		}, Stats{
			In:     10,
			Out:    0,
			Failed: 0,
		}, sh, wg,
		)
		go increment(t, ctx, db, JobTargetKey{
			TaskRunId:     "task_run_id1",
			SourceId:      "source_id1",
			DestinationId: "destination_id",
		}, Stats{
			In:     0,
			Out:    4,
			Failed: 6,
		}, sh, wg,
		)
		go increment(t, ctx, db, JobTargetKey{
			TaskRunId:     "task_run_id1",
			SourceId:      "source_id2",
			DestinationId: "destination_id",
		}, Stats{
			In:     10,
			Out:    2,
			Failed: 6,
		}, sh, wg,
		)
		go increment(t, ctx, db, JobTargetKey{
			TaskRunId:     "task_run_id2",
			SourceId:      "source_id2",
			DestinationId: "destination_id",
		}, Stats{
			In:     10,
			Out:    2,
			Failed: 6,
		}, sh, wg,
		)
		go increment(t, ctx, db, JobTargetKey{
			TaskRunId:     "task_run_id2",
			SourceId:      "source_id3",
			DestinationId: "destination_id",
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
		for _, ts := range res.TasksStatus {
			sort.Slice(ts.SourcesStatus, func(i, j int) bool {
				return ts.SourcesStatus[i].ID < ts.SourcesStatus[j].ID
			})
		}
		require.NoError(t, err)

		expected := JobStatus{
			ID: "jobRunId",
			TasksStatus: []TaskStatus{
				{
					ID: "task_run_id1",
					SourcesStatus: []SourceStatus{
						{
							ID:        "source_id1",
							Completed: true,
							DestinationsStatus: []DestinationStatus{
								{
									ID:        "destination_id",
									Completed: true,
									Stats: Stats{
										In:     10,
										Out:    4,
										Failed: 6,
									},
								},
							},
						},
						{
							ID:        "source_id2",
							Completed: false,
							DestinationsStatus: []DestinationStatus{
								{
									ID:        "destination_id",
									Completed: false,
									Stats: Stats{
										In:     10,
										Out:    2,
										Failed: 6,
									},
								},
							},
						},
					},
				},
				{
					ID: "task_run_id2",
					SourcesStatus: []SourceStatus{
						{
							ID:        "source_id2",
							Completed: false,
							DestinationsStatus: []DestinationStatus{
								{
									ID:        "destination_id",
									Completed: false,
									Stats: Stats{
										In:     10,
										Out:    2,
										Failed: 6,
									},
								},
							},
						},
					},
				},
			},
		}

		require.Equal(t, expected, res)
	})
}

func increment(t *testing.T, ctx context.Context, db *sql.DB, key JobTargetKey, stat Stats, sh JobService, wg *sync.WaitGroup) {
	tx, err := db.Begin()
	require.NoError(t, err, "it should be able to begin the transaction")
	err = sh.IncrementStats(ctx, tx, "jobRunId", key, stat)
	require.NoError(t, err, "it should be able to increment stats")
	err = tx.Commit()
	require.NoError(t, err, "it should be able to commit the transaction")
	wg.Done()
}
