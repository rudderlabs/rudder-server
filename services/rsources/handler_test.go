package rsources

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	assert "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	DB_DSN = "root@tcp(127.0.0.1:3306)/service"
	db     *sql.DB
)

func TestMain(m *testing.M) {
	code := m.Run()
	// blockOnHold()
	defer os.Exit(code)
}

func TestMultitenantSourcesHandler(t *testing.T) {

	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}
	// Create 3 postgresql using wal_level logical (pgA, pgB, pgC)
	pgA, pgA_DSN := dbResource(pool)
	pgB, pgB_DSN := dbResource(pool)
	pgC, pgC_DSN := dbResource(pool)
	defer purgeResource(pool, pgA, pgB, pgC)

	// Start 2 JobServices
	// 1. js1 with local=pgA, remote=pgC
	// 2. js2 with local=pgB, remote=pgC
	// Increment the same jobRunId from both services
	ctx := context.Background()
	shA, jobRunIdA := prepareService(ctx, Stats{
		In:     10,
		Out:    4,
		Failed: 6,
	}, t, pgA_DSN, pgC_DSN, "job_run_id")
	shB, jobRunIdB := prepareService(ctx, Stats{
		In:     10,
		Out:    4,
		Failed: 6,
	}, t, pgB_DSN, pgC_DSN, "job_run_id")

	require.Equal(t, jobRunIdA, jobRunIdB, "jobRunIds should be the same")
	// Query both services for the jobRunId and verify that same results are generated (use require.Eventually)
	t.Run("Status from both services should be same", func(t *testing.T) {
		require.Eventually(t, func() bool {
			statsA, err := shA.GetStatus(ctx, jobRunIdA, JobFilter{
				SourceID:  []string{"source_id"},
				TaskRunID: []string{"task_run_id"},
			})
			require.NoError(t, err, "it should be able to get stats from JobServiceA")
			statsB, err := shB.GetStatus(ctx, jobRunIdB, JobFilter{
				SourceID:  []string{"source_id"},
				TaskRunID: []string{"task_run_id"},
			})
			require.NoError(t, err, "it should be able to get stats from JobServiceA")
			return assert.Equal(t, statsA, statsB)
		}, 10*time.Second, 10*time.Millisecond, "Status from both services should be same")
	})

}

func dbResource(pool *dockertest.Pool) (*dockertest.Resource, string) {
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
	return resourcePostgres, DB_DSN
}

func purgeResource(pool *dockertest.Pool, resources ...*dockertest.Resource) {
	for _, resource := range resources {
		err := pool.Purge(resource)
		if err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}
}

func prepareService(ctx context.Context, stats Stats, t *testing.T, localDSN, sharedDSN, jobRunId string) (JobService, string) {
	if jobRunId == "" {
		jobRunId = strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
	}
	config := JobServiceConfig{
		LocalHostname:   "localhost",
		MaxPoolSize:     1,
		LocalConnection: DB_DSN,
	}
	sh, err := NewJobService(config)
	require.NoError(t, err, "it should be able to create the service")

	key := JobTargetKey{
		SourceID:      "source_id",
		DestinationID: "destination_id",
		TaskRunID:     "task_run_id",
	}

	tx, err := db.Begin()
	require.NoError(t, err, "it should be able to begin the transaction")

	require.NoError(t, sh.IncrementStats(ctx, tx, jobRunId, key, stats), "it should be able to increment stats")

	require.NoError(t, tx.Commit(), "it should be able to commit the transaction")
	return sh, jobRunId
}

func TestSourcesHandler(t *testing.T) {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	resourcePostgres, DB_DSN := dbResource(pool)
	defer func() {
		purgeResource(pool, resourcePostgres)
	}()

	ctx := context.Background()
	stats := Stats{
		In:     10,
		Out:    4,
		Failed: 6,
	}

	t.Run("Get Status", func(t *testing.T) {
		sh, jobRunId := prepareService(ctx, stats, t, DB_DSN, "", "")
		jobFilters := JobFilter{
			SourceID:  []string{"source_id"},
			TaskRunID: []string{"task_run_id"},
		}
		status, err := sh.GetStatus(ctx, jobRunId, jobFilters)
		if err != nil {
			t.Fatal(err)
		}
		expected := JobStatus{
			ID: jobRunId,
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
		sh, jobRunId := prepareService(ctx, stats, t, DB_DSN, "", "")
		tx, err := db.Begin()
		require.NoError(t, err, "it should be able to begin the transaction")
		err = sh.Delete(ctx, jobRunId)
		require.NoError(t, err, "it should be able to delete")
		err = tx.Commit()
		require.NoError(t, err, "it should be able to commit the transaction")
		jobFilters := JobFilter{
			SourceID:  []string{"source_id"},
			TaskRunID: []string{"task_run_id"},
		}
		status, err := sh.GetStatus(ctx, jobRunId, jobFilters)
		require.NotNil(t, err)
		require.Equal(t, status, JobStatus{})
		require.True(t, errors.Is(err, StatusNotFoundError), "it should return a StatusNotFoundError")

	})

	t.Run("GetStatus with filtering", func(t *testing.T) {
		sh, jobRunId := prepareService(ctx, stats, t, DB_DSN, "", "")
		wg := &sync.WaitGroup{}
		wg.Add(5)
		go increment(ctx, t, db, jobRunId, JobTargetKey{
			TaskRunID:     "task_run_id1",
			SourceID:      "source_id1",
			DestinationID: "destination_id",
		}, Stats{
			In:     10,
			Out:    0,
			Failed: 0,
		}, sh, wg,
		)
		go increment(ctx, t, db, jobRunId, JobTargetKey{
			TaskRunID:     "task_run_id1",
			SourceID:      "source_id1",
			DestinationID: "destination_id",
		}, Stats{
			In:     0,
			Out:    4,
			Failed: 6,
		}, sh, wg,
		)
		go increment(ctx, t, db, jobRunId, JobTargetKey{
			TaskRunID:     "task_run_id1",
			SourceID:      "source_id2",
			DestinationID: "destination_id",
		}, Stats{
			In:     10,
			Out:    2,
			Failed: 6,
		}, sh, wg,
		)
		go increment(ctx, t, db, jobRunId, JobTargetKey{
			TaskRunID:     "task_run_id2",
			SourceID:      "source_id2",
			DestinationID: "destination_id",
		}, Stats{
			In:     10,
			Out:    2,
			Failed: 6,
		}, sh, wg,
		)
		go increment(ctx, t, db, jobRunId, JobTargetKey{
			TaskRunID:     "task_run_id2",
			SourceID:      "source_id3",
			DestinationID: "destination_id",
		}, Stats{
			In:     10,
			Out:    2,
			Failed: 6,
		}, sh, wg,
		)
		wg.Wait()

		res, err := sh.GetStatus(ctx, jobRunId, JobFilter{
			SourceID:  []string{"source_id1", "source_id2"},
			TaskRunID: []string{"task_run_id1", "task_run_id2"},
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
			ID: jobRunId,
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

	t.Run("Cleanup loop", func(t *testing.T) {
		sh, _ := prepareService(ctx, stats, t, DB_DSN, "", "")
		ts := time.Now().Add(-48 * time.Hour)
		stmt, err := db.Prepare(`update "rsources_stats" set ts = $1`)
		require.NoError(t, err)
		_, err = stmt.Exec(ts)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		go func() { _ = sh.CleanupLoop(ctx) }()

		for {
			select {
			case <-ctx.Done():
				t.Error("it should cleanup all tables")
				return
			case <-time.After(1 * time.Second):
				sqlStatement := `select count(*) from "rsources_stats"`
				var count int
				err = db.QueryRow(sqlStatement).Scan(&count)
				require.NoError(t, err)
				if count == 0 {
					return
				}
			}
		}

	})
}

func increment(ctx context.Context, t *testing.T, db *sql.DB, jobRunId string, key JobTargetKey, stat Stats, sh JobService, wg *sync.WaitGroup) {
	tx, err := db.Begin()
	require.NoError(t, err, "it should be able to begin the transaction")
	err = sh.IncrementStats(ctx, tx, jobRunId, key, stat)
	require.NoError(t, err, "it should be able to increment stats")
	err = tx.Commit()
	require.NoError(t, err, "it should be able to commit the transaction")
	wg.Done()
}
