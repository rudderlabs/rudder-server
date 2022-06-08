package rsources

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
)

type postgresResource struct {
	resource    *dockertest.Resource
	db          *sql.DB
	internalDSN string
	externalDSN string
}

var defaultJobTargetKey = JobTargetKey{
	TaskRunID:     "task_run_id",
	SourceID:      "source_id",
	DestinationID: "destination_id",
}

func TestSourcesHandler(t *testing.T) {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	resource := newDBResource(pool, "", "postgres")
	defer func() {
		purgeResource(pool, resource.resource)
	}()

	stats := Stats{
		In:     10,
		Out:    4,
		Failed: 6,
	}
	config := JobServiceConfig{
		LocalHostname: "postgres",
		MaxPoolSize:   1,
		LocalConn:     resource.externalDSN,
	}
	sh := createService(t, config)

	t.Run("Get Status", func(t *testing.T) {
		jobRunId := newJobRunId()
		increment(t, resource.db, jobRunId, defaultJobTargetKey, stats, sh, nil)
		jobFilters := JobFilter{
			SourceID:  []string{"source_id"},
			TaskRunID: []string{"task_run_id"},
		}
		status, err := sh.GetStatus(context.Background(), jobRunId, jobFilters)
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
		jobRunId := newJobRunId()
		increment(t, resource.db, jobRunId, defaultJobTargetKey, stats, sh, nil)
		tx, err := resource.db.Begin()
		require.NoError(t, err, "it should be able to begin the transaction")
		err = sh.Delete(context.Background(), jobRunId)
		require.NoError(t, err, "it should be able to delete")
		err = tx.Commit()
		require.NoError(t, err, "it should be able to commit the transaction")
		jobFilters := JobFilter{
			SourceID:  []string{"source_id"},
			TaskRunID: []string{"task_run_id"},
		}
		status, err := sh.GetStatus(context.Background(), jobRunId, jobFilters)
		require.NotNil(t, err)
		require.Equal(t, status, JobStatus{})
		require.True(t, errors.Is(err, StatusNotFoundError), "it should return a StatusNotFoundError")

	})

	t.Run("GetStatus with filtering", func(t *testing.T) {
		jobRunId := newJobRunId()
		increment(t, resource.db, jobRunId, defaultJobTargetKey, stats, sh, nil)
		wg := &sync.WaitGroup{}
		wg.Add(5)
		go increment(t, resource.db, jobRunId, JobTargetKey{
			TaskRunID:     "task_run_id1",
			SourceID:      "source_id1",
			DestinationID: "destination_id",
		}, Stats{
			In:     10,
			Out:    0,
			Failed: 0,
		}, sh, wg,
		)
		go increment(t, resource.db, jobRunId, JobTargetKey{
			TaskRunID:     "task_run_id1",
			SourceID:      "source_id1",
			DestinationID: "destination_id",
		}, Stats{
			In:     0,
			Out:    4,
			Failed: 6,
		}, sh, wg,
		)
		go increment(t, resource.db, jobRunId, JobTargetKey{
			TaskRunID:     "task_run_id1",
			SourceID:      "source_id2",
			DestinationID: "destination_id",
		}, Stats{
			In:     10,
			Out:    2,
			Failed: 6,
		}, sh, wg,
		)
		go increment(t, resource.db, jobRunId, JobTargetKey{
			TaskRunID:     "task_run_id2",
			SourceID:      "source_id2",
			DestinationID: "destination_id",
		}, Stats{
			In:     10,
			Out:    2,
			Failed: 6,
		}, sh, wg,
		)
		go increment(t, resource.db, jobRunId, JobTargetKey{
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

		res, err := sh.GetStatus(context.Background(), jobRunId, JobFilter{
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
		jobRunId := newJobRunId()
		increment(t, resource.db, jobRunId, defaultJobTargetKey, stats, sh, nil)
		ts := time.Now().Add(-48 * time.Hour)
		stmt, err := resource.db.Prepare(`update "rsources_stats" set ts = $1`)
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
				err = resource.db.QueryRow(sqlStatement).Scan(&count)
				require.NoError(t, err)
				if count == 0 {
					return
				}
			}
		}

	})
}

func TestMultitenantSourcesHandler(t *testing.T) {

	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}
	const networkId = "postgres-network"
	network, _ := pool.Client.NetworkInfo(networkId)
	if network == nil {
		network, err = pool.Client.CreateNetwork(docker.CreateNetworkOptions{Name: networkId})
		if err != nil {
			log.Fatalf("could not create a network: %s", err)
		}
	}

	defer func() {
		_ = pool.Client.RemoveNetwork(network.ID)
	}()

	// Create 3 postgresql using wal_level logical (pgA, pgB, pgC)
	pgA := newDBResource(pool, network.ID, "postgres-1", "wal_level=logical")
	pgB := newDBResource(pool, network.ID, "postgres-2", "wal_level=logical")
	pgC := newDBResource(pool, network.ID, "postgres-3")
	defer purgeResource(pool, pgA.resource, pgB.resource, pgC.resource)

	var serviceA JobService
	configA := JobServiceConfig{
		LocalHostname:          "postgres-1",
		MaxPoolSize:            1,
		LocalConn:              pgA.externalDSN,
		SharedConn:             pgC.externalDSN,
		SubscriptionTargetConn: pgA.internalDSN,
	}

	var serviceB JobService
	configB := JobServiceConfig{
		LocalHostname:          "postgres-2",
		MaxPoolSize:            1,
		LocalConn:              pgB.externalDSN,
		SharedConn:             pgC.externalDSN,
		SubscriptionTargetConn: pgB.internalDSN,
	}

	t.Run("It should be able to create two services", func(t *testing.T) {
		// Start 2 JobServices
		// 1. js1 with local=pgA, remote=pgC
		// 2. js2 with local=pgB, remote=pgC
		// Increment the same jobRunId from both services
		serviceA = createService(t, configA)
		serviceB = createService(t, configB)

	})

	// Query both services for the jobRunId and verify that same results are generated (use require.Eventually)
	t.Run("Status from both services should be same", func(t *testing.T) {
		jobRunId := newJobRunId()
		statsA := Stats{
			In:     5,
			Out:    4,
			Failed: 0,
		}
		increment(t, pgA.db, jobRunId, defaultJobTargetKey, statsA, serviceA, nil)

		statsB := Stats{
			In:     3,
			Out:    2,
			Failed: 1,
		}
		increment(t, pgB.db, jobRunId, defaultJobTargetKey, statsB, serviceB, nil)

		require.Eventually(t, func() bool {
			totalStatsA, err := serviceA.GetStatus(context.Background(), jobRunId, JobFilter{
				SourceID:  []string{"source_id"},
				TaskRunID: []string{"task_run_id"},
			})
			require.NoError(t, err, "it should be able to get stats from JobServiceA")
			totalStatsB, err := serviceB.GetStatus(context.Background(), jobRunId, JobFilter{
				SourceID:  []string{"source_id"},
				TaskRunID: []string{"task_run_id"},
			})
			require.NoError(t, err, "it should be able to get stats from JobServiceB")

			expected := JobStatus{
				ID: jobRunId,
				TasksStatus: []TaskStatus{
					{
						ID: defaultJobTargetKey.TaskRunID,
						SourcesStatus: []SourceStatus{
							{
								ID:        defaultJobTargetKey.SourceID,
								Stats:     Stats{},
								Completed: false,
								DestinationsStatus: []DestinationStatus{
									{
										ID:        defaultJobTargetKey.DestinationID,
										Completed: false,
										Stats: Stats{
											In:     8,
											Out:    6,
											Failed: 1,
										},
									},
								},
							},
						},
					},
				},
			}
			sameResults := reflect.DeepEqual(totalStatsA, totalStatsB)
			expectedResults := reflect.DeepEqual(expected, totalStatsB)
			return sameResults && expectedResults

		}, 20*time.Second, 10*time.Millisecond, "Status from both services should be same and representing the sum")
	})

	t.Run("Creating the same services again should be possible and not affect publications and subscriptions", func(t *testing.T) {
		createService(t, configA)
		createService(t, configB)
	})

}

func createService(t *testing.T, config JobServiceConfig) JobService {
	service, err := NewJobService(config)
	require.NoError(t, err, "it should be able to create the service")
	return service
}

func increment(t *testing.T, db *sql.DB, jobRunId string, key JobTargetKey, stat Stats, sh JobService, wg *sync.WaitGroup) {
	tx, err := db.Begin()
	require.NoError(t, err, "it should be able to begin the transaction")
	err = sh.IncrementStats(context.Background(), tx, jobRunId, key, stat)
	require.NoError(t, err, "it should be able to increment stats")
	err = tx.Commit()
	require.NoError(t, err, "it should be able to commit the transaction")
	if wg != nil {
		wg.Done()
	}
}

func newJobRunId() string {
	return strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
}

func newDBResource(pool *dockertest.Pool, networkId string, hostname string, params ...string) postgresResource {
	database := "jobsdb"
	cmd := []string{"postgres"}
	if len(params) > 0 {
		cmd = append(cmd, "-c")
		cmd = append(cmd, params...)
	}
	// pulls an image, creates a container based on it and runs it
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "11-alpine",
		NetworkID:  networkId,
		Hostname:   hostname,
		Env: []string{
			"POSTGRES_PASSWORD=password",
			"POSTGRES_DB=" + database,
			"POSTGRES_USER=rudder",
		},
		Cmd: cmd,
	})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	port := resource.GetPort("5432/tcp")
	externalDSN := fmt.Sprintf("postgres://rudder:password@localhost:%s/%s?sslmode=disable", port, database)
	internalDSN := fmt.Sprintf("postgres://rudder:password@%s:5432/%s?sslmode=disable", hostname, database)
	var db *sql.DB
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
	return postgresResource{
		db:          db,
		resource:    resource,
		internalDSN: internalDSN,
		externalDSN: externalDSN,
	}
}

func purgeResource(pool *dockertest.Pool, resources ...*dockertest.Resource) {
	for _, resource := range resources {
		err := pool.Purge(resource)
		if err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}
}
