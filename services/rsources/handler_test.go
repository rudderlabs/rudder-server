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

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/rudderlabs/rudder-server/config"
	testlog "github.com/rudderlabs/rudder-server/testhelper/log"
	"github.com/rudderlabs/rudder-server/testhelper/rand"
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
				Log:           testlog.GinkgoLogger,
			}
			sh = createService(config)
		})

		AfterAll(func() {
			purgeResources(pool, resource.resource)
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
			expcetedRecords := JobFailedRecords{
				ID: jobRunId,
				Tasks: []TaskFailedRecords{{
					ID: "task_run_id",
					Sources: []SourceFailedRecords{{
						ID: "source_id",
						Destinations: []DestinationFailedRecords{{
							ID:      "destination_id",
							Records: []json.RawMessage{[]byte(`{"record-1": "id-1"}`), []byte(`{"record-2": "id-2"}`)},
						}},
					}},
				}},
			}
			Expect(failedRecords).To(Equal(expcetedRecords), "it should be able to get failed records")
		})

		It("shouldn't be able to get failed records when failed records collection is disabled", func() {
			handler := sh.(*sourcesHandler)
			previous := handler.config.SkipFailedRecordsCollection
			handler.config.SkipFailedRecordsCollection = true
			defer func() { handler.config.SkipFailedRecordsCollection = previous }()

			jobRunId := newJobRunId()
			addFailedRecords(resource.db, jobRunId, defaultJobTargetKey, sh, []json.RawMessage{
				[]byte(`{"record-1": "id-1"}`),
				[]byte(`{"record-2": "id-2"}`),
			})

			failedRecords, err := sh.GetFailedRecords(context.Background(), jobRunId, JobFilter{})
			Expect(err).To(HaveOccurred(), "it shouldn't be able to get failed records")
			Expect(failedRecords).To(Equal(JobFailedRecords{ID: jobRunId}), "it should return an empty failed records")
			Expect(err).To(Equal(ErrOperationNotSupported), "it should return an ErrOperationNotSupported error")
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
			err := sh.Delete(context.Background(), jobRunId, JobFilter{})
			Expect(err).NotTo(HaveOccurred(), "it should be able to delete stats, failed keys for the jobrunid")
			jobFilters := JobFilter{
				SourceID:  []string{"source_id"},
				TaskRunID: []string{"task_run_id"},
			}
			status, err := sh.GetStatus(context.Background(), jobRunId, jobFilters)
			Expect(err).To(HaveOccurred())
			Expect(status).To(Equal(JobStatus{}))
			Expect(errors.Is(err, ErrStatusNotFound)).To(BeTrue(), "it should return a StatusNotFoundError")
			failedRecords, err := sh.GetFailedRecords(context.Background(), jobRunId, jobFilters)
			Expect(err).NotTo(HaveOccurred())
			Expect(failedRecords).To(Equal(JobFailedRecords{ID: jobRunId}))
		})

		It("should be able to delete stats and failed keys partially", func() {
			otherJobTargetKey := defaultJobTargetKey
			otherJobTargetKey.SourceID = "other_source_id"
			jobRunId := newJobRunId()
			increment(resource.db, jobRunId, defaultJobTargetKey, stats, sh, nil)
			increment(resource.db, jobRunId, otherJobTargetKey, stats, sh, nil)

			addFailedRecords(resource.db, jobRunId, defaultJobTargetKey, sh, []json.RawMessage{
				[]byte(`{"record-1": "id-1"}`),
				[]byte(`{"record-2": "id-2"}`),
			})
			addFailedRecords(resource.db, jobRunId, otherJobTargetKey, sh, []json.RawMessage{
				[]byte(`{"record-1": "id-1"}`),
				[]byte(`{"record-2": "id-2"}`),
			})
			err := sh.Delete(context.Background(), jobRunId, JobFilter{SourceID: []string{"other_source_id"}})
			Expect(err).NotTo(HaveOccurred(), "it should be able to delete stats, failed keys for the jobrunid")

			jobFilters := JobFilter{
				SourceID:  []string{"other_source_id"},
				TaskRunID: []string{"task_run_id"},
			}
			status, err := sh.GetStatus(context.Background(), jobRunId, jobFilters)
			Expect(err).To(HaveOccurred())
			Expect(status).To(Equal(JobStatus{}))
			Expect(errors.Is(err, ErrStatusNotFound)).To(BeTrue(), "it should return a StatusNotFoundError")
			failedRecords, err := sh.GetFailedRecords(context.Background(), jobRunId, jobFilters)
			Expect(err).NotTo(HaveOccurred())
			Expect(failedRecords).To(Equal(JobFailedRecords{ID: jobRunId}))

			jobFilters.SourceID = []string{defaultJobTargetKey.SourceID}
			_, err = sh.GetStatus(context.Background(), jobRunId, jobFilters)
			Expect(err).ToNot(HaveOccurred())
			failedRecords, err = sh.GetFailedRecords(context.Background(), jobRunId, jobFilters)
			Expect(err).NotTo(HaveOccurred())
			Expect(failedRecords).ToNot(Equal(JobFailedRecords{ID: jobRunId}))
		})

		It("shouldn't be able to delete stats for an incomplete source", func() {
			jobRunId := newJobRunId()
			stats := Stats{
				In:     10,
				Out:    4,
				Failed: 5,
			}
			increment(resource.db, jobRunId, defaultJobTargetKey, stats, sh, nil)

			err := sh.Delete(context.Background(), jobRunId, JobFilter{SourceID: []string{defaultJobTargetKey.SourceID}})
			Expect(err).To(Equal(ErrSourceNotCompleted), "it shouldn't be able to delete stats for an incomplete source")
		})

		It("shouldn't be able to delete stats for an invalid job", func() {
			jobRunId := newJobRunId()
			stats := Stats{
				In:     10,
				Out:    4,
				Failed: 5,
			}
			increment(resource.db, jobRunId, defaultJobTargetKey, stats, sh, nil)

			err := sh.Delete(context.Background(), "invalidJobRunId", JobFilter{SourceID: []string{defaultJobTargetKey.SourceID}})
			Expect(err).To(Equal(ErrStatusNotFound), "it shouldn't be able to delete stats for an invalid jobrunid")
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
			expected := JobFailedRecords{
				ID: jobRunId,
				Tasks: []TaskFailedRecords{
					{
						ID: "task_run_id1",
						Sources: []SourceFailedRecords{
							{
								ID: "source_id1",
								Destinations: []DestinationFailedRecords{{
									ID: "destination_id",
									Records: []json.RawMessage{
										[]byte(`{"record-1": "id-1"}`),
										[]byte(`{"record-2": "id-2"}`),
										[]byte(`{"record-11": "id-112"}`),
										[]byte(`{"record-22": "id-222"}`),
									},
								}},
							},
							{
								ID: "source_id2",
								Destinations: []DestinationFailedRecords{{
									ID: "destination_id",
									Records: []json.RawMessage{
										[]byte(`{"record-12": "id-12"}`),
										[]byte(`{"record-21": "id-21"}`),
									},
								}},
							},
						},
					},
					{
						ID: "task_run_id2",
						Sources: []SourceFailedRecords{{
							ID: "source_id2",
							Destinations: []DestinationFailedRecords{{
								ID: "destination_id",
								Records: []json.RawMessage{
									[]byte(`{"record-11": "id-11"}`),
									[]byte(`{"record-32": "id-32"}`),
								},
							}},
						}},
					},
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
			ts := time.Now().Add(-time.Duration(defaultRetentionPeriodInHours+1) * time.Hour)
			stmt, err := resource.db.Prepare(`update "rsources_stats" set ts = $1`)
			Expect(err).NotTo(HaveOccurred())
			_, err = stmt.Exec(ts)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = stmt.Close() }()
			stmt2, err := resource.db.Prepare(`update "rsources_failed_keys" set ts = $1`)
			Expect(err).NotTo(HaveOccurred())
			_, err = stmt2.Exec(ts)
			defer func() { _ = stmt2.Close() }()
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
			networkId := randomString()
			network, _ = pool.Client.NetworkInfo(networkId)
			if network == nil {
				network, err = pool.Client.CreateNetwork(docker.CreateNetworkOptions{Name: networkId})
				Expect(err).NotTo(HaveOccurred())
			}
			for containerID := range network.Containers { // Remove any containers left from previous runs
				_ = pool.Client.RemoveContainer(docker.RemoveContainerOptions{ID: containerID, Force: true, RemoveVolumes: true})
			}
			postgres1Hostname := randomString() + "-1"
			postgres2Hostname := randomString() + "-2"
			postgres3Hostname := randomString() + "-3"
			pgA = newDBResource(pool, network.ID, postgres1Hostname, "wal_level=logical")
			pgB = newDBResource(pool, network.ID, postgres2Hostname, "wal_level=logical")
			pgC = newDBResource(pool, network.ID, postgres3Hostname)

			configA = JobServiceConfig{
				LocalHostname:          postgres1Hostname,
				MaxPoolSize:            1,
				LocalConn:              pgA.externalDSN,
				SharedConn:             pgC.externalDSN,
				SubscriptionTargetConn: pgA.internalDSN,
				Log:                    testlog.GinkgoLogger,
			}

			configB = JobServiceConfig{
				LocalHostname:          postgres2Hostname,
				MaxPoolSize:            1,
				LocalConn:              pgB.externalDSN,
				SharedConn:             pgC.externalDSN,
				SubscriptionTargetConn: pgB.internalDSN,
				Log:                    testlog.GinkgoLogger,
			}

			// Start 2 JobServices
			// 1. js1 with local=pgA, remote=pgC
			// 2. js2 with local=pgB, remote=pgC
			serviceA = createService(configA)
			serviceB = createService(configB)
		})

		AfterAll(func() {
			purgeResources(pool, pgA.resource, pgB.resource, pgC.resource)
			if network != nil {
				_ = pool.Client.RemoveNetwork(network.ID)
			}
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
				if err != nil {
					return false
				}
				totalStatsB, err := serviceB.GetStatus(context.Background(), jobRunId, JobFilter{
					SourceID:  []string{"source_id"},
					TaskRunID: []string{"task_run_id"},
				})
				if err != nil {
					return false
				}
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
			}, "30s", "100ms").Should(BeTrue(), "Status from both services should be same and representing the sum")
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
			expected := JobFailedRecords{
				ID: jobRunId,
				Tasks: []TaskFailedRecords{
					{
						ID: defaultJobTargetKey.TaskRunID,
						Sources: []SourceFailedRecords{
							{
								ID: defaultJobTargetKey.SourceID,
								Destinations: []DestinationFailedRecords{{
									ID: defaultJobTargetKey.DestinationID,
									Records: []json.RawMessage{
										[]byte(`{"id": "1"}`),
										[]byte(`{"id": "2"}`),
										[]byte(`{"id": "2"}`),
										[]byte(`{"id": "3"}`),
									},
								}},
							},
						},
					},
				},
			}

			var err error
			var failedKeysA, failedKeysB JobFailedRecords
			Eventually(func() bool {
				failedKeysA, err = serviceA.GetFailedRecords(context.Background(), jobRunId, JobFilter{
					SourceID:  []string{"source_id"},
					TaskRunID: []string{"task_run_id"},
				})
				if err != nil {
					err = fmt.Errorf("failed to get failed records from serviceA: %w", err)
					return false
				}
				failedKeysB, err = serviceB.GetFailedRecords(context.Background(), jobRunId, JobFilter{
					SourceID:  []string{"source_id"},
					TaskRunID: []string{"task_run_id"},
				})
				if err != nil {
					err = fmt.Errorf("failed to get failed records from serviceB: %w", err)
					return false
				}
				if !reflect.DeepEqual(failedKeysA, failedKeysB) {
					err = fmt.Errorf("failed keys from serviceA are different compared to failed keys from serviceB")
					return false
				}
				if len(failedKeysA.Tasks) != 1 || len(failedKeysA.Tasks[0].Sources) != 1 || len(failedKeysA.Tasks[0].Sources[0].Destinations) != 1 {
					err = fmt.Errorf("failed keys from serviceA don't contain 1 task with 1 source and 1 destination")
					return false
				}
				sort.Slice(failedKeysA.Tasks[0].Sources[0].Destinations[0].Records, func(i, j int) bool {
					return string(failedKeysA.Tasks[0].Sources[0].Destinations[0].Records[i]) < string(failedKeysA.Tasks[0].Sources[0].Destinations[0].Records[j])
				})
				if !reflect.DeepEqual(failedKeysA, expected) {
					err = fmt.Errorf("failed keys from serviceA don't match expectation")
					return false
				}
				return true
			}, "30s", "100ms").Should(BeTrue(), "Failed Records from both services should be the same", mustMarshal(failedKeysA), mustMarshal(failedKeysB), mustMarshal(expected), err)
		})

		It("should be able to create the same services again and not affect publications and subscriptions", func() {
			createService(configA)
			createService(configB)
		})

		It("shouldn't be able to create a service when wal_level=logical is not set on the local db", func() {
			postgres4Hostname := randomString() + "-4"
			pgD := newDBResource(pool, network.ID, postgres4Hostname)
			defer purgeResources(pool, pgD.resource)
			badConfig := JobServiceConfig{
				LocalHostname:          postgres4Hostname,
				MaxPoolSize:            1,
				LocalConn:              pgD.externalDSN,
				SharedConn:             pgC.externalDSN,
				SubscriptionTargetConn: pgD.internalDSN,
				Log:                    testlog.GinkgoLogger,
			}
			_, err := NewJobService(badConfig)
			Expect(err).To(HaveOccurred(), "it shouldn't able to create the service")
		})

		It("shouldn't be able to create a service when an invalid SubscriptionTargetConn is provided", func() {
			postgres4Hostname := randomString() + "-4"
			pgD := newDBResource(pool, network.ID, postgres4Hostname)
			defer purgeResources(pool, pgD.resource)
			badConfig := JobServiceConfig{
				LocalHostname:          postgres4Hostname,
				MaxPoolSize:            1,
				LocalConn:              pgD.externalDSN,
				SharedConn:             pgC.externalDSN,
				SubscriptionTargetConn: pgD.externalDSN,
				Log:                    testlog.GinkgoLogger,
			}
			_, err := NewJobService(badConfig)
			Expect(err).To(HaveOccurred(), "it shouldn't able to create the service")
		})
	})

	Context("monitoring lag when shared db is configured", Ordered, func() {
		var (
			pool     *dockertest.Pool
			network  *docker.Network
			pgA, pgB postgresResource
			configA  JobServiceConfig
			serviceA JobService
		)

		BeforeAll(func() {
			var err error
			pool, err = dockertest.NewPool("")
			Expect(err).NotTo(HaveOccurred())
			networkId := randomString()
			network, _ = pool.Client.NetworkInfo(networkId)
			if network == nil {
				network, err = pool.Client.CreateNetwork(docker.CreateNetworkOptions{Name: networkId})
				Expect(err).NotTo(HaveOccurred())
			}
			for containerID := range network.Containers { // Remove any containers left from previous runs
				_ = pool.Client.RemoveContainer(docker.RemoveContainerOptions{ID: containerID, Force: true, RemoveVolumes: true})
			}

			postgres1Hostname := randomString() + "-1"
			postgres2Hostname := randomString() + "-2"
			pgA = newDBResource(pool, network.ID, postgres1Hostname, "wal_level=logical")
			pgB = newDBResource(pool, network.ID, postgres2Hostname)

			configA = JobServiceConfig{
				LocalHostname:          postgres1Hostname,
				MaxPoolSize:            1,
				LocalConn:              pgA.externalDSN,
				SharedConn:             pgB.externalDSN,
				SubscriptionTargetConn: pgA.internalDSN,
				Log:                    testlog.GinkgoLogger,
			}
			serviceA = createService(configA)
		})

		AfterAll(func() {
			purgeResources(pool, pgA.resource, pgB.resource)
			if network != nil {
				_ = pool.Client.RemoveNetwork(network.ID)
			}
		})

		It("should be able to monitor lag when shared db is configured", func() {
			lagGauge := new(mockGauge)
			replicationSlotGauge := new(mockGauge)
			config.Set("Rsources.stats.monitoringInterval", 10*time.Millisecond)
			defer config.Reset()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go serviceA.Monitor(ctx, lagGauge, replicationSlotGauge)

			Eventually(func() bool {
				return replicationSlotGauge.value() == int64(1) && lagGauge.wasGauged()
			}, "30s", "100ms").Should(BeTrue(), "should have one replication slot and some non-nil lag")
		})

		It("unavailability of shared db is handled via -1 replication slot gauge", func() {
			pgB.resource.Close()

			lagGauge := new(mockGauge)
			replicationSlotGauge := new(mockGauge)
			// kill shared db
			config.Set("Rsources.stats.monitoringInterval", 10*time.Millisecond)
			defer config.Reset()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go serviceA.Monitor(ctx, lagGauge, replicationSlotGauge)
			Eventually(func() bool {
				return lagGauge.value() == float64(-1)
			}, "30s", "100ms").Should(BeTrue(), "should have -1 replication slot")
		})
	})

	Context("adding failed_keys to the publication alongside stats", Ordered, func() {
		It("should be able to add rsources_failed_keys table to the publication and subscription seamlessly", func() {
			pool, err := dockertest.NewPool("")
			Expect(err).NotTo(HaveOccurred())
			networkId := randomString()
			network, _ := pool.Client.NetworkInfo(networkId)
			if network == nil {
				network, err = pool.Client.CreateNetwork(docker.CreateNetworkOptions{Name: networkId})
				Expect(err).NotTo(HaveOccurred())
			}
			for containerID := range network.Containers { // Remove any containers left from previous runs
				_ = pool.Client.RemoveContainer(docker.RemoveContainerOptions{ID: containerID, Force: true, RemoveVolumes: true})
			}
			postgres1Hostname := randomString() + "-1"
			postgres2Hostname := randomString() + "-2"
			postgres3Hostname := randomString() + "-3"
			pgA := newDBResource(pool, network.ID, postgres1Hostname, "wal_level=logical")
			pgB := newDBResource(pool, network.ID, postgres2Hostname, "wal_level=logical")
			pgC := newDBResource(pool, network.ID, postgres3Hostname)

			defer func() {
				purgeResources(pool, pgA.resource, pgB.resource, pgC.resource)
				if network != nil {
					_ = pool.Client.RemoveNetwork(network.ID)
				}
			}()
			log := testlog.GinkgoLogger
			configA := JobServiceConfig{
				LocalHostname:          postgres1Hostname,
				MaxPoolSize:            1,
				LocalConn:              pgA.externalDSN,
				SharedConn:             pgC.externalDSN,
				SubscriptionTargetConn: pgA.internalDSN,
				Log:                    log,
			}

			configB := JobServiceConfig{
				LocalHostname:          postgres2Hostname,
				MaxPoolSize:            1,
				LocalConn:              pgB.externalDSN,
				SharedConn:             pgC.externalDSN,
				SubscriptionTargetConn: pgB.internalDSN,
				Log:                    log,
			}

			// Setting up previous environment before adding failedkeys table to the publication
			// setup databases
			databaseA := getDB(configA.LocalConn, configA.MaxPoolSize)
			defer func() { _ = databaseA.Close() }()
			databaseB := getDB(configB.LocalConn, configB.MaxPoolSize)
			defer func() { _ = databaseB.Close() }()
			databaseC := getDB(configB.SharedConn, configB.MaxPoolSize) // shared
			defer func() { _ = databaseC.Close() }()

			// create tables
			err = setupStatsTable(context.Background(), databaseA, configA.LocalHostname, log)
			Expect(err).NotTo(HaveOccurred())
			err = setupStatsTable(context.Background(), databaseB, configB.LocalHostname, log)
			Expect(err).NotTo(HaveOccurred())
			err = setupStatsTable(context.Background(), databaseC, "shared", log)
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
			expected := JobFailedRecords{
				ID: jobRunId,
				Tasks: []TaskFailedRecords{
					{
						ID: defaultJobTargetKey.TaskRunID,
						Sources: []SourceFailedRecords{
							{
								ID: defaultJobTargetKey.SourceID,
								Destinations: []DestinationFailedRecords{{
									ID: defaultJobTargetKey.DestinationID,
									Records: []json.RawMessage{
										[]byte(`{"id": "1"}`),
										[]byte(`{"id": "2"}`),
										[]byte(`{"id": "2"}`),
										[]byte(`{"id": "3"}`),
									},
								}},
							},
						},
					},
				},
			}

			var failedKeysA, failedKeysB JobFailedRecords
			Eventually(func() bool {
				failedKeysA, err = serviceA.GetFailedRecords(context.Background(), jobRunId, JobFilter{
					SourceID:  []string{"source_id"},
					TaskRunID: []string{"task_run_id"},
				})
				if err != nil {
					err = fmt.Errorf("failed to get failed records from serviceA: %w", err)
					return false
				}
				failedKeysB, err = serviceB.GetFailedRecords(context.Background(), jobRunId, JobFilter{
					SourceID:  []string{"source_id"},
					TaskRunID: []string{"task_run_id"},
				})
				if err != nil {
					err = fmt.Errorf("failed to get failed records from serviceB: %w", err)
					return false
				}
				if !reflect.DeepEqual(failedKeysA, failedKeysB) {
					err = fmt.Errorf("failed keys from serviceA are different compared to failed keys from serviceB")
					return false
				}
				if len(failedKeysA.Tasks) != 1 || len(failedKeysA.Tasks[0].Sources) != 1 || len(failedKeysA.Tasks[0].Sources[0].Destinations) != 1 {
					err = fmt.Errorf("failed keys from serviceA don't contain 1 task with 1 source and 1 destination")
					return false
				}
				sort.Slice(failedKeysA.Tasks[0].Sources[0].Destinations[0].Records, func(i, j int) bool {
					return string(failedKeysA.Tasks[0].Sources[0].Destinations[0].Records[i]) < string(failedKeysA.Tasks[0].Sources[0].Destinations[0].Records[j])
				})
				if !reflect.DeepEqual(failedKeysA, expected) {
					err = fmt.Errorf("failed keys from serviceA don't match expectation")
					return false
				}
				return true
			}, "30s", "100ms").Should(BeTrue(), "Failed Records from both services should be the same", mustMarshal(failedKeysA), mustMarshal(failedKeysB), mustMarshal(expected), err)
		})
	})
})

func mustMarshal[T any](v T) []byte {
	b, _ := json.Marshal(v)
	return b
}

// mock Gauges
type mockGauge struct {
	gauge  interface{}
	gauged bool
}

func (g *mockGauge) Gauge(value interface{}) {
	g.gauge = value
	g.gauged = true
}

func (g *mockGauge) value() interface{} {
	return g.gauge
}

func (g *mockGauge) wasGauged() bool {
	return g.gauged
}

func createService(config JobServiceConfig) JobService {
	service, err := NewJobService(config)
	Expect(err).ShouldNot(HaveOccurred(), "it should be able to create the service")
	return service
}

func addFailedRecords(db *sql.DB, jobRunId string, jobTargetKey JobTargetKey, sh JobService, records []json.RawMessage) {
	tx, err := db.Begin()
	Expect(err).ShouldNot(HaveOccurred(), "it should be able to begin the transaction")
	err = sh.AddFailedRecords(context.Background(), tx, jobRunId, jobTargetKey, records)
	Expect(err).ShouldNot(HaveOccurred(), "it should be able to add failed records")
	err = tx.Commit()
	Expect(err).ShouldNot(HaveOccurred(), "it should be able to commit the transaction")
}

func increment(db *sql.DB, jobRunId string, key JobTargetKey, stat Stats, sh JobService, wg *sync.WaitGroup) {
	tx, err := db.Begin()
	Expect(err).ShouldNot(HaveOccurred(), "it should be able to begin the transaction")
	err = sh.IncrementStats(context.Background(), tx, jobRunId, key, stat)
	Expect(err).ShouldNot(HaveOccurred(), "it should be able to increment stats")
	err = tx.Commit()
	Expect(err).ShouldNot(HaveOccurred(), "it should be able to commit the transaction")
	if wg != nil {
		wg.Done()
	}
}

func newJobRunId() string {
	return strings.ReplaceAll(uuid.New().String(), "-", "")
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
		Tag:        "15-alpine",
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
	var (
		db  *sql.DB
		dsn = fmt.Sprintf("host=localhost port=%s user=rudder password=password dbname=jobsdb sslmode=disable", port)
	)
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() (err error) {
		db, err = sql.Open("postgres", dsn)
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		Expect(err).NotTo(HaveOccurred(), dsn)
	}
	return postgresResource{
		db:          db,
		resource:    resource,
		internalDSN: internalDSN,
		externalDSN: externalDSN,
	}
}

func purgeResources(pool *dockertest.Pool, resources ...*dockertest.Resource) {
	for _, resource := range resources {
		if resource != nil {
			_ = pool.Purge(resource)
		}
	}
}

func getDB(conn string, maxOpenConns int) *sql.DB {
	db, err := sql.Open("postgres", conn)
	db.SetMaxOpenConns(maxOpenConns)
	Expect(err).NotTo(HaveOccurred())
	return db
}

func randomString() string {
	return strings.ToLower(rand.String(10))
}
