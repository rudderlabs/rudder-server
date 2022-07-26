package rsources

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/uuid"
	_ "github.com/lib/pq"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
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

var _ = Describe("Using sources handler", func() {
	Context("single-tenant setup with a single local datasource", Ordered, func() {
		var (
			pool     *dockertest.Pool
			resource postgresResource
			sh       JobService
		)
		stats := Stats{
			In:     10,
			Out:    4,
			Failed: 6,
		}
		BeforeAll(func() {
			var err error
			pool, err = dockertest.NewPool("")
			Expect(err).NotTo(HaveOccurred())
			resource = newDBResource(pool, "", "postgres")
			config := JobServiceConfig{
				LocalHostname: "postgres",
				MaxPoolSize:   1,
				LocalConn:     resource.externalDSN,
			}
			sh = createService(config)
		})

		AfterAll(func() {
			if resource.resource != nil {
				purgeResource(pool, resource.resource)
			}
		})

		It("should be able to add and get failed records", func() {
			jobRunId := newJobRunId()
			addFailedRecords(resource.db, jobRunId, defaultJobTargetKey, sh, []json.RawMessage{
				[]byte(`{"record-1": "id-1"}`),
				[]byte(`{"record-2": "id-2"}`),
			})
			jobFilters := JobFilter{
				SourceID:  []string{"source_id"},
				TaskRunID: []string{"task_run_id"},
			}

			failedRecords, err := sh.GetFailedRecords(context.Background(), jobRunId, jobFilters)
			Expect(err).NotTo(HaveOccurred(), "it should be able to get failed records")
			expcetedRecords := FailedRecords{
				{
					JobRunID:      jobRunId,
					TaskRunID:     "task_run_id",
					SourceID:      "source_id",
					DestinationID: "destination_id",
					RecordID:      json.RawMessage(`{"record-1": "id-1"}`),
				},
				{
					JobRunID:      jobRunId,
					TaskRunID:     "task_run_id",
					SourceID:      "source_id",
					DestinationID: "destination_id",
					RecordID:      json.RawMessage(`{"record-2": "id-2"}`),
				},
			}
			Expect(failedRecords).To(Equal(expcetedRecords), "it should be able to get failed records")
		})

		It("should be able to get the status", func() {
			jobRunId := newJobRunId()
			increment(resource.db, jobRunId, defaultJobTargetKey, stats, sh, nil)
			jobFilters := JobFilter{
				SourceID:  []string{"source_id"},
				TaskRunID: []string{"task_run_id"},
			}
			status, err := sh.GetStatus(context.Background(), jobRunId, jobFilters)
			Expect(err).NotTo(HaveOccurred())
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
			Expect(status).To(Equal(expected))
		})

		It("should be able to delete stats and failed keys", func() {
			jobRunId := newJobRunId()
			increment(resource.db, jobRunId, defaultJobTargetKey, stats, sh, nil)
			addFailedRecords(resource.db, jobRunId, defaultJobTargetKey, sh, []json.RawMessage{
				[]byte(`{"record-1": "id-1"}`),
				[]byte(`{"record-2": "id-2"}`),
			})
			tx, err := resource.db.Begin()
			Expect(err).NotTo(HaveOccurred(), "it should be able to begin the transaction")
			err = sh.Delete(context.Background(), jobRunId)
			Expect(err).NotTo(HaveOccurred(), "it should be able to delete stats, failed keys for the jobrunid")
			err = tx.Commit()
			Expect(err).NotTo(HaveOccurred(), "it should be able to commit the transaction")
			jobFilters := JobFilter{
				SourceID:  []string{"source_id"},
				TaskRunID: []string{"task_run_id"},
			}
			status, err := sh.GetStatus(context.Background(), jobRunId, jobFilters)
			Expect(err).To(HaveOccurred())
			Expect(status).To(Equal(JobStatus{}))
			Expect(errors.Is(err, StatusNotFoundError)).To(BeTrue(), "it should return a StatusNotFoundError")
			failedRecords, err := sh.GetFailedRecords(context.Background(), jobRunId, jobFilters)
			Expect(err).NotTo(HaveOccurred())
			Expect(failedRecords).To(Equal(FailedRecords{}))
		})

		It("should be able to get failed records by filtering", func() {
			jobRunId := newJobRunId()

			addFailedRecords(resource.db, jobRunId, JobTargetKey{
				TaskRunID:     "task_run_id1",
				SourceID:      "source_id1",
				DestinationID: "destination_id",
			}, sh, []json.RawMessage{
				[]byte(`{"record-1": "id-1"}`),
				[]byte(`{"record-2": "id-2"}`),
			})
			addFailedRecords(resource.db, jobRunId, JobTargetKey{
				TaskRunID:     "task_run_id1",
				SourceID:      "source_id1",
				DestinationID: "destination_id",
			}, sh, []json.RawMessage{
				[]byte(`{"record-11": "id-112"}`),
				[]byte(`{"record-22": "id-222"}`),
			})
			addFailedRecords(resource.db, jobRunId, JobTargetKey{
				TaskRunID:     "task_run_id1",
				SourceID:      "source_id2",
				DestinationID: "destination_id",
			}, sh, []json.RawMessage{
				[]byte(`{"record-12": "id-12"}`),
				[]byte(`{"record-21": "id-21"}`),
			})
			addFailedRecords(resource.db, jobRunId, JobTargetKey{
				TaskRunID:     "task_run_id2",
				SourceID:      "source_id2",
				DestinationID: "destination_id",
			}, sh, []json.RawMessage{
				[]byte(`{"record-11": "id-11"}`),
				[]byte(`{"record-32": "id-32"}`),
			})
			addFailedRecords(resource.db, jobRunId, JobTargetKey{
				TaskRunID:     "task_run_id2",
				SourceID:      "source_id3",
				DestinationID: "destination_id",
			}, sh, []json.RawMessage{
				[]byte(`{"record-13": "id-13"}`),
				[]byte(`{"record-23": "id-23"}`),
			})
			expected := FailedRecords{
				{
					JobRunID:      jobRunId,
					TaskRunID:     "task_run_id1",
					SourceID:      "source_id1",
					DestinationID: "destination_id",
					RecordID:      json.RawMessage(`{"record-1": "id-1"}`),
				},
				{
					JobRunID:      jobRunId,
					TaskRunID:     "task_run_id1",
					SourceID:      "source_id1",
					DestinationID: "destination_id",
					RecordID:      json.RawMessage("{\"record-2\": \"id-2\"}"),
				},
				{
					JobRunID:      jobRunId,
					TaskRunID:     "task_run_id1",
					SourceID:      "source_id1",
					DestinationID: "destination_id",
					RecordID:      json.RawMessage("{\"record-11\": \"id-112\"}"),
				},
				{
					JobRunID:      jobRunId,
					TaskRunID:     "task_run_id1",
					SourceID:      "source_id1",
					DestinationID: "destination_id",
					RecordID:      json.RawMessage("{\"record-22\": \"id-222\"}"),
				},
				{
					JobRunID:      jobRunId,
					TaskRunID:     "task_run_id1",
					SourceID:      "source_id2",
					DestinationID: "destination_id",
					RecordID:      json.RawMessage("{\"record-12\": \"id-12\"}"),
				},
				{
					JobRunID:      jobRunId,
					TaskRunID:     "task_run_id1",
					SourceID:      "source_id2",
					DestinationID: "destination_id",
					RecordID:      json.RawMessage("{\"record-21\": \"id-21\"}"),
				},
				{
					JobRunID:      jobRunId,
					TaskRunID:     "task_run_id2",
					SourceID:      "source_id2",
					DestinationID: "destination_id",
					RecordID:      json.RawMessage("{\"record-11\": \"id-11\"}"),
				},
				{
					JobRunID:      jobRunId,
					TaskRunID:     "task_run_id2",
					SourceID:      "source_id2",
					DestinationID: "destination_id",
					RecordID:      json.RawMessage("{\"record-32\": \"id-32\"}"),
				},
			}
			failedRecords, err := sh.GetFailedRecords(context.Background(), jobRunId, JobFilter{
				SourceID:  []string{"source_id1", "source_id2"},
				TaskRunID: []string{"task_run_id1", "task_run_id2"},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(expected).To(Equal(failedRecords), "it should return the failed records for the jobrunid based on the filtering")
		})

		It("should be able to get the status with filtering", func() {
			jobRunId := newJobRunId()
			increment(resource.db, jobRunId, defaultJobTargetKey, stats, sh, nil)
			wg := &sync.WaitGroup{}
			wg.Add(5)
			go increment(resource.db, jobRunId, JobTargetKey{
				TaskRunID:     "task_run_id1",
				SourceID:      "source_id1",
				DestinationID: "destination_id",
			}, Stats{
				In:     10,
				Out:    0,
				Failed: 0,
			}, sh, wg,
			)
			go increment(resource.db, jobRunId, JobTargetKey{
				TaskRunID:     "task_run_id1",
				SourceID:      "source_id1",
				DestinationID: "destination_id",
			}, Stats{
				In:     0,
				Out:    4,
				Failed: 6,
			}, sh, wg,
			)
			go increment(resource.db, jobRunId, JobTargetKey{
				TaskRunID:     "task_run_id1",
				SourceID:      "source_id2",
				DestinationID: "destination_id",
			}, Stats{
				In:     10,
				Out:    2,
				Failed: 6,
			}, sh, wg,
			)
			go increment(resource.db, jobRunId, JobTargetKey{
				TaskRunID:     "task_run_id2",
				SourceID:      "source_id2",
				DestinationID: "destination_id",
			}, Stats{
				In:     10,
				Out:    2,
				Failed: 6,
			}, sh, wg,
			)
			go increment(resource.db, jobRunId, JobTargetKey{
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
			Expect(err).NotTo(HaveOccurred())

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
			Expect(res).To(Equal(expected))
		})

		It("should be able to execute the cleanup loop", func() {
			jobRunId := newJobRunId()
			increment(resource.db, jobRunId, defaultJobTargetKey, stats, sh, nil)
			addFailedRecords(resource.db, jobRunId, defaultJobTargetKey, sh, []json.RawMessage{
				[]byte(`{"record-1": "id-1"}`),
				[]byte(`{"record-2": "id-2"}`),
			})
			ts := time.Now().Add(-48 * time.Hour)
			stmt, err := resource.db.Prepare(`update "rsources_stats" set ts = $1`)
			Expect(err).NotTo(HaveOccurred())
			_, err = stmt.Exec(ts)
			Expect(err).NotTo(HaveOccurred())
			defer stmt.Close()
			stmt2, err := resource.db.Prepare(`update "rsources_failed_keys" set ts = $1`)
			Expect(err).NotTo(HaveOccurred())
			_, err = stmt2.Exec(ts)
			defer stmt2.Close()
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
			defer cancel()
			go func() { _ = sh.CleanupLoop(ctx) }()

			for {
				select {
				case <-ctx.Done():
					Fail("it should cleanup all tables")
					return
				case <-time.After(1 * time.Second):
					sqlStatement := `select count(*) from "rsources_stats" where job_run_id = $1`
					var statCount int
					err = resource.db.QueryRow(sqlStatement, jobRunId).Scan(&statCount)
					Expect(err).NotTo(HaveOccurred())

					sqlStatement = `select count(*) from "rsources_failed_keys" where job_run_id = $1`
					var failedRecordCount int
					err = resource.db.QueryRow(sqlStatement, jobRunId).Scan(&failedRecordCount)
					Expect(err).NotTo(HaveOccurred())

					if statCount == 0 && failedRecordCount == 0 {
						return
					}
				}
			}
		})
	})

	Context("multitenant setup with local & shared datasources", Ordered, func() {
		var (
			pool               *dockertest.Pool
			network            *docker.Network
			pgA, pgB, pgC      postgresResource
			configA, configB   JobServiceConfig
			serviceA, serviceB JobService
		)

		BeforeAll(func() {
			var err error
			pool, err = dockertest.NewPool("")
			Expect(err).NotTo(HaveOccurred())
			const networkId = "TestMultitenantSourcesHandler"
			network, _ = pool.Client.NetworkInfo(networkId)
			if network == nil {
				network, err = pool.Client.CreateNetwork(docker.CreateNetworkOptions{Name: networkId})
				Expect(err).NotTo(HaveOccurred())
			}
			for containerID := range network.Containers { // Remove any containers left from previous runs
				_ = pool.Client.RemoveContainer(docker.RemoveContainerOptions{ID: containerID, Force: true, RemoveVolumes: true})
			}

			pgA = newDBResource(pool, network.ID, "postgres-1", "wal_level=logical")
			pgB = newDBResource(pool, network.ID, "postgres-2", "wal_level=logical")
			pgC = newDBResource(pool, network.ID, "postgres-3")

			configA = JobServiceConfig{
				LocalHostname:          "postgres-1",
				MaxPoolSize:            1,
				LocalConn:              pgA.externalDSN,
				SharedConn:             pgC.externalDSN,
				SubscriptionTargetConn: pgA.internalDSN,
			}

			configB = JobServiceConfig{
				LocalHostname:          "postgres-2",
				MaxPoolSize:            1,
				LocalConn:              pgB.externalDSN,
				SharedConn:             pgC.externalDSN,
				SubscriptionTargetConn: pgB.internalDSN,
			}
		})

		AfterAll(func() {
			if network != nil {
				_ = pool.Client.RemoveNetwork(network.ID)
			}
			if pgA.resource != nil {
				purgeResource(pool, pgA.resource, pgB.resource, pgC.resource)
			}
		})
		It("should be able to create two services", func() {
			// Start 2 JobServices
			// 1. js1 with local=pgA, remote=pgC
			// 2. js2 with local=pgB, remote=pgC
			// Increment the same jobRunId from both services
			serviceA = createService(configA)
			serviceB = createService(configB)
		})

		It("should be able to query both services for the same jobRunId and receive same stats", func() {
			// Status from both services should be same
			jobRunId := newJobRunId()
			statsA := Stats{
				In:     5,
				Out:    4,
				Failed: 0,
			}
			increment(pgA.db, jobRunId, defaultJobTargetKey, statsA, serviceA, nil)

			statsB := Stats{
				In:     3,
				Out:    2,
				Failed: 1,
			}
			increment(pgB.db, jobRunId, defaultJobTargetKey, statsB, serviceB, nil)
			Eventually(func() bool {
				totalStatsA, err := serviceA.GetStatus(context.Background(), jobRunId, JobFilter{
					SourceID:  []string{"source_id"},
					TaskRunID: []string{"task_run_id"},
				})
				Expect(err).NotTo(HaveOccurred(), "it should be able to get stats from JobServiceA")
				totalStatsB, err := serviceB.GetStatus(context.Background(), jobRunId, JobFilter{
					SourceID:  []string{"source_id"},
					TaskRunID: []string{"task_run_id"},
				})
				Expect(err).NotTo(HaveOccurred(), "it should be able to get stats from JobServiceB")

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
			}, "20s", "100ms").Should(BeTrue(), "Status from both services should be same and representing the sum")
		})

		It("should be able to query both services for the same jobRunId and receive same failed Records", func() {
			jobRunId := newJobRunId()
			addFailedRecords(pgA.db, jobRunId, defaultJobTargetKey, serviceA, []json.RawMessage{
				json.RawMessage(`{"id": "1"}`),
				json.RawMessage(`{"id": "2"}`),
			})
			addFailedRecords(pgB.db, jobRunId, defaultJobTargetKey, serviceB, []json.RawMessage{
				json.RawMessage(`{"id": "2"}`),
				json.RawMessage(`{"id": "3"}`),
			})
			expected := FailedRecords{
				{
					JobRunID:      jobRunId,
					TaskRunID:     defaultJobTargetKey.TaskRunID,
					SourceID:      defaultJobTargetKey.SourceID,
					DestinationID: defaultJobTargetKey.DestinationID,
					RecordID:      json.RawMessage(`{"id": "1"}`),
				},
				{
					JobRunID:      jobRunId,
					TaskRunID:     defaultJobTargetKey.TaskRunID,
					SourceID:      defaultJobTargetKey.SourceID,
					DestinationID: defaultJobTargetKey.DestinationID,
					RecordID:      json.RawMessage(`{"id": "2"}`),
				},
				{
					JobRunID:      jobRunId,
					TaskRunID:     defaultJobTargetKey.TaskRunID,
					SourceID:      defaultJobTargetKey.SourceID,
					DestinationID: defaultJobTargetKey.DestinationID,
					RecordID:      json.RawMessage(`{"id": "2"}`),
				},
				{
					JobRunID:      jobRunId,
					TaskRunID:     defaultJobTargetKey.TaskRunID,
					SourceID:      defaultJobTargetKey.SourceID,
					DestinationID: defaultJobTargetKey.DestinationID,
					RecordID:      json.RawMessage(`{"id": "3"}`),
				},
			}
			Eventually(func() bool {
				failedKeysA, err := serviceA.GetFailedRecords(context.Background(), jobRunId, JobFilter{
					SourceID:  []string{"source_id"},
					TaskRunID: []string{"task_run_id"},
				})
				Expect(err).NotTo(HaveOccurred(), "it should be able to get failed keys from JobServiceA")
				failedKeysB, err := serviceB.GetFailedRecords(context.Background(), jobRunId, JobFilter{
					SourceID:  []string{"source_id"},
					TaskRunID: []string{"task_run_id"},
				})
				Expect(err).NotTo(HaveOccurred(), "it should be able to get failed keys from JobServiceB")
				return reflect.DeepEqual(failedKeysA, failedKeysB) && reflect.DeepEqual(failedKeysA, expected)
			}, "20s", "100ms").Should(BeTrue(), "Failed Records from both services should be the same")
		})

		It("should be able to create the same services again and not affect publications and subscriptions", func() {
			createService(configA)
			createService(configB)
		})

		It("shouldn't be able to create a service when wal_level=logical is not set on the local db", func() {
			pgD := newDBResource(pool, network.ID, "postgres-4")
			defer purgeResource(pool, pgD.resource)
			badConfig := JobServiceConfig{
				LocalHostname:          "postgres-4",
				MaxPoolSize:            1,
				LocalConn:              pgD.externalDSN,
				SharedConn:             pgC.externalDSN,
				SubscriptionTargetConn: pgD.internalDSN,
			}
			_, err := NewJobService(badConfig)
			Expect(err).To(HaveOccurred(), "it shouldn't able to create the service")
		})

		It("shouldn't be able to create a service when an invalid SubscriptionTargetConn is provided", func() {
			pgD := newDBResource(pool, network.ID, "postgres-4")
			defer purgeResource(pool, pgD.resource)
			badConfig := JobServiceConfig{
				LocalHostname:          "postgres-4",
				MaxPoolSize:            1,
				LocalConn:              pgD.externalDSN,
				SharedConn:             pgC.externalDSN,
				SubscriptionTargetConn: pgD.externalDSN,
			}
			_, err := NewJobService(badConfig)
			Expect(err).To(HaveOccurred(), "it shouldn't able to create the service")
		})
	})

	Context("adding failed_keys to the publication alongside stats", func() {
		It("should be able to add rsources_failed_keys table to the publication and subscription seamlessly", func() {
			pool, err := dockertest.NewPool("")
			Expect(err).NotTo(HaveOccurred())
			const networkId = "TestMultitenantSourcesHandlerMigration"
			network, _ := pool.Client.NetworkInfo(networkId)
			if network == nil {
				network, err = pool.Client.CreateNetwork(docker.CreateNetworkOptions{Name: networkId})
				Expect(err).NotTo(HaveOccurred())
			}
			for containerID := range network.Containers { // Remove any containers left from previous runs
				_ = pool.Client.RemoveContainer(docker.RemoveContainerOptions{ID: containerID, Force: true, RemoveVolumes: true})
			}

			pgA := newDBResource(pool, network.ID, "postgres-1", "wal_level=logical")
			pgB := newDBResource(pool, network.ID, "postgres-2", "wal_level=logical")
			pgC := newDBResource(pool, network.ID, "postgres-3")

			defer func() {
				if network != nil {
					_ = pool.Client.RemoveNetwork(network.ID)
				}
				if pgA.resource != nil {
					purgeResource(pool, pgA.resource, pgB.resource, pgC.resource)
				}
			}()

			configA := JobServiceConfig{
				LocalHostname:          "postgres-1",
				MaxPoolSize:            1,
				LocalConn:              pgA.externalDSN,
				SharedConn:             pgC.externalDSN,
				SubscriptionTargetConn: pgA.internalDSN,
			}

			configB := JobServiceConfig{
				LocalHostname:          "postgres-2",
				MaxPoolSize:            1,
				LocalConn:              pgB.externalDSN,
				SharedConn:             pgC.externalDSN,
				SubscriptionTargetConn: pgB.internalDSN,
			}

			// Setting up previous environment before adding failedkeys table to the publication
			// setup databases
			databaseA := getDB(configA.LocalConn, configA.MaxPoolSize)
			defer databaseA.Close()
			databaseB := getDB(configB.LocalConn, configB.MaxPoolSize)
			defer databaseB.Close()
			databaseC := getDB(configB.SharedConn, configB.MaxPoolSize) // shared
			defer databaseC.Close()

			// create tables
			err = setupStatsTable(context.Background(), databaseA, configA.LocalHostname)
			Expect(err).NotTo(HaveOccurred())
			err = setupStatsTable(context.Background(), databaseB, configB.LocalHostname)
			Expect(err).NotTo(HaveOccurred())
			err = setupStatsTable(context.Background(), databaseC, "shared")
			Expect(err).NotTo(HaveOccurred())

			// setup logical replication(only stats tables as previously done)
			publicationQuery := `CREATE PUBLICATION "rsources_stats_pub" FOR TABLE rsources_stats`
			_, err = databaseA.ExecContext(context.TODO(), publicationQuery)
			Expect(err).NotTo(HaveOccurred())
			_, err = databaseB.ExecContext(context.TODO(), publicationQuery)
			Expect(err).NotTo(HaveOccurred())

			normalizedHostnameA := replSlotDisallowedChars.ReplaceAllString(strings.ToLower(configA.LocalHostname), "_")
			normalizedHostnameB := replSlotDisallowedChars.ReplaceAllString(strings.ToLower(configB.LocalHostname), "_")
			subscriptionAName := fmt.Sprintf("%s_rsources_stats_sub", normalizedHostnameA)
			subscriptionBName := fmt.Sprintf("%s_rsources_stats_sub", normalizedHostnameB)
			subscriptionQuery := `CREATE SUBSCRIPTION "%s" CONNECTION '%s' PUBLICATION "rsources_stats_pub"`

			_, err = databaseC.ExecContext(context.Background(), fmt.Sprintf(subscriptionQuery, subscriptionAName, pgA.internalDSN))
			Expect(err).NotTo(HaveOccurred())
			_, err = databaseC.ExecContext(context.Background(), fmt.Sprintf(subscriptionQuery, subscriptionBName, pgB.internalDSN))
			Expect(err).NotTo(HaveOccurred())

			// Now setup the handlers
			serviceA := createService(configA)
			serviceB := createService(configB)
			jobRunId := newJobRunId()
			addFailedRecords(pgA.db, jobRunId, defaultJobTargetKey, serviceA, []json.RawMessage{
				json.RawMessage(`{"id": "1"}`),
				json.RawMessage(`{"id": "2"}`),
			})
			addFailedRecords(pgB.db, jobRunId, defaultJobTargetKey, serviceB, []json.RawMessage{
				json.RawMessage(`{"id": "2"}`),
				json.RawMessage(`{"id": "3"}`),
			})
			expected := FailedRecords{
				{
					JobRunID:      jobRunId,
					TaskRunID:     defaultJobTargetKey.TaskRunID,
					SourceID:      defaultJobTargetKey.SourceID,
					DestinationID: defaultJobTargetKey.DestinationID,
					RecordID:      json.RawMessage(`{"id": "1"}`),
				},
				{
					JobRunID:      jobRunId,
					TaskRunID:     defaultJobTargetKey.TaskRunID,
					SourceID:      defaultJobTargetKey.SourceID,
					DestinationID: defaultJobTargetKey.DestinationID,
					RecordID:      json.RawMessage(`{"id": "2"}`),
				},
				{
					JobRunID:      jobRunId,
					TaskRunID:     defaultJobTargetKey.TaskRunID,
					SourceID:      defaultJobTargetKey.SourceID,
					DestinationID: defaultJobTargetKey.DestinationID,
					RecordID:      json.RawMessage(`{"id": "2"}`),
				},
				{
					JobRunID:      jobRunId,
					TaskRunID:     defaultJobTargetKey.TaskRunID,
					SourceID:      defaultJobTargetKey.SourceID,
					DestinationID: defaultJobTargetKey.DestinationID,
					RecordID:      json.RawMessage(`{"id": "3"}`),
				},
			}
			Eventually(func() bool {
				failedKeysA, err := serviceA.GetFailedRecords(context.Background(), jobRunId, JobFilter{
					SourceID:  []string{"source_id"},
					TaskRunID: []string{"task_run_id"},
				})
				Expect(err).NotTo(HaveOccurred(), "it should be able to get failed keys from JobServiceA")
				failedKeysB, err := serviceB.GetFailedRecords(context.Background(), jobRunId, JobFilter{
					SourceID:  []string{"source_id"},
					TaskRunID: []string{"task_run_id"},
				})
				Expect(err).NotTo(HaveOccurred(), "it should be able to get failed keys from JobServiceB")
				return reflect.DeepEqual(failedKeysA, failedKeysB) && reflect.DeepEqual(failedKeysA, expected)
			}, "20s", "100ms").Should(BeTrue(), "Failed Records from both services should be the same")
		})
	})
})

func createService(config JobServiceConfig) JobService {
	service, err := NewJobService(config)
	Expect(err).NotTo(HaveOccurred(), "it should be able to create the service")
	return service
}

func addFailedRecords(db *sql.DB, jobRunId string, jobTargetKey JobTargetKey, sh JobService, records []json.RawMessage) {
	tx, err := db.Begin()
	Expect(err).NotTo(HaveOccurred(), "it should be able to begin the transaction")
	err = sh.AddFailedRecords(context.Background(), tx, jobRunId, jobTargetKey, records)
	Expect(err).NotTo(HaveOccurred(), "it should be able to add failed records")
	err = tx.Commit()
	Expect(err).NotTo(HaveOccurred(), "it should be able to commit the transaction")
}

func increment(db *sql.DB, jobRunId string, key JobTargetKey, stat Stats, sh JobService, wg *sync.WaitGroup) {
	tx, err := db.Begin()
	Expect(err).NotTo(HaveOccurred(), "it should be able to begin the transaction")
	err = sh.IncrementStats(context.Background(), tx, jobRunId, key, stat)
	Expect(err).NotTo(HaveOccurred(), "it should be able to increment stats")
	err = tx.Commit()
	Expect(err).NotTo(HaveOccurred(), "it should be able to commit the transaction")
	if wg != nil {
		wg.Done()
	}
}

func newJobRunId() string {
	return strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
}

func newDBResource(pool *dockertest.Pool, networkId, hostname string, params ...string) postgresResource {
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
		Expect(err).NotTo(HaveOccurred())
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
			return err
		}
		return db.Ping()
	}); err != nil {
		Expect(err).NotTo(HaveOccurred())
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
		_ = pool.Purge(resource)
	}
}

func getDB(conn string, maxOpenConns int) *sql.DB {
	db, err := sql.Open("postgres", conn)
	db.SetMaxOpenConns(maxOpenConns)
	Expect(err).NotTo(HaveOccurred())
	return db
}
