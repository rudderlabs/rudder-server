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
	"github.com/segmentio/ksuid"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	testlog "github.com/rudderlabs/rudder-server/testhelper/log"
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
	var noPaging PagingInfo
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
		BeforeEach(func() {
			var err error
			pool, err = dockertest.NewPool("")
			Expect(err).NotTo(HaveOccurred())
			resource = newDBResource(pool, "", "postgres")
			config := JobServiceConfig{
				LocalHostname:       "postgres",
				MaxPoolSize:         1,
				LocalConn:           resource.externalDSN,
				Log:                 testlog.GinkgoLogger,
				ShouldSetupSharedDB: true,
			}
			sh = createService(config)
		})

		AfterEach(func() {
			purgeResources(pool, resource.resource)
		})

		It("should be able to add and get failed records", func() {
			jobRunId := newJobRunId()
			addFailedRecords(resource.db, jobRunId, defaultJobTargetKey, sh, []FailedRecord{
				{Record: []byte(`{"record-1": "id-1"}`)},
				{Record: []byte(`{"record-2": "id-2"}`)},
			})
			jobFilters := JobFilter{
				SourceID:  []string{"source_id"},
				TaskRunID: []string{"task_run_id"},
			}

			failedRecords, err := sh.GetFailedRecords(context.Background(), jobRunId, jobFilters, noPaging)
			Expect(err).NotTo(HaveOccurred(), "it should be able to get failed records")
			expcetedRecords := JobFailedRecordsV2{
				ID: jobRunId,
				Tasks: []TaskFailedRecords[FailedRecord]{{
					ID: "task_run_id",
					Sources: []SourceFailedRecords[FailedRecord]{{
						ID: "source_id",
						Destinations: []DestinationFailedRecords[FailedRecord]{{
							ID: "destination_id",
							Records: []FailedRecord{
								{Record: []byte(`{"record-1": "id-1"}`)},
								{Record: []byte(`{"record-2": "id-2"}`)},
							},
						}},
					}},
				}},
			}
			Expect(failedRecords).To(Equal(expcetedRecords), "it should be able to get failed records")
		})

		It("should be able to add and get failed records using pagination", func() {
			jobRunId := newJobRunId()
			addFailedRecords(resource.db, jobRunId, defaultJobTargetKey, sh, []FailedRecord{
				{Record: []byte(`"id-1"`)},
				{Record: []byte(`"id-2"`)},
				{Record: []byte(`"id-3"`)},
				{Record: []byte(`"id-4"`)},
			})
			jobFilters := JobFilter{
				SourceID:  []string{defaultJobTargetKey.SourceID},
				TaskRunID: []string{defaultJobTargetKey.TaskRunID},
			}
			paging := PagingInfo{
				Size: 2,
			}
			for i := 0; i < 2; i++ {
				failedRecords, err := sh.GetFailedRecords(context.Background(), jobRunId, jobFilters, paging)
				Expect(err).NotTo(HaveOccurred(), "it should be able to get failed records")
				Expect(failedRecords.Paging).NotTo(BeNil(), "paging should not be nil")
				paging = *failedRecords.Paging
				expectedRecords := JobFailedRecordsV2{
					ID: jobRunId,
					Tasks: []TaskFailedRecords[FailedRecord]{{
						ID: "task_run_id",
						Sources: []SourceFailedRecords[FailedRecord]{{
							ID: defaultJobTargetKey.SourceID,
							Destinations: []DestinationFailedRecords[FailedRecord]{{
								ID: defaultJobTargetKey.DestinationID,
								Records: []FailedRecord{
									{Record: []byte(fmt.Sprintf(`"id-%d"`, (2*i)+1))},
									{Record: []byte(fmt.Sprintf(`"id-%d"`, (2*i)+2))},
								},
							}},
						}},
					}},
					Paging: failedRecords.Paging,
				}
				Expect(failedRecords).To(Equal(expectedRecords), "it should be able to get failed records")

			}
			failedRecords, err := sh.GetFailedRecords(context.Background(), jobRunId, jobFilters, paging)
			Expect(err).NotTo(HaveOccurred(), "it should be able to get failed records")
			Expect(failedRecords.Tasks).To(BeEmpty(), "last page should be empty")
			Expect(failedRecords.Paging).To(BeNil(), "last page should have no paging")
		})

		It("shouldn't be able to get failed records when failed records collection is disabled", func() {
			handler := sh.(*sourcesHandler)
			previous := handler.config.SkipFailedRecordsCollection
			handler.config.SkipFailedRecordsCollection = true
			defer func() { handler.config.SkipFailedRecordsCollection = previous }()

			jobRunId := newJobRunId()
			addFailedRecords(resource.db, jobRunId, defaultJobTargetKey, sh, []FailedRecord{
				{Record: []byte(`{"record-1": "id-1"}`)},
				{Record: []byte(`{"record-2": "id-2"}`)},
			})

			failedRecords, err := sh.GetFailedRecords(context.Background(), jobRunId, JobFilter{}, noPaging)
			Expect(err).To(HaveOccurred(), "it shouldn't be able to get failed records")
			Expect(failedRecords).To(Equal(JobFailedRecordsV2{ID: jobRunId}), "it should return an empty failed records")
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

		It("should be able to delete failed keys", func() {
			jobRunId := newJobRunId()
			increment(resource.db, jobRunId, defaultJobTargetKey, stats, sh, nil)
			addFailedRecords(resource.db, jobRunId, defaultJobTargetKey, sh, []FailedRecord{
				{Record: []byte(`{"record-1": "id-1"}`)},
				{Record: []byte(`{"record-2": "id-2"}`)},
			})
			err := sh.DeleteFailedRecords(context.Background(), jobRunId, JobFilter{})
			Expect(err).NotTo(HaveOccurred(), "it should be able to delete failed keys for the jobrunid")
			jobFilters := JobFilter{
				SourceID:  []string{"source_id"},
				TaskRunID: []string{"task_run_id"},
			}
			status, err := sh.GetStatus(context.Background(), jobRunId, jobFilters)
			Expect(err).To(BeNil())
			Expect(status).To(Equal(JobStatus{
				ID: jobRunId,
				TasksStatus: []TaskStatus{
					{
						ID: "task_run_id",
						SourcesStatus: []SourceStatus{
							{
								ID:        "source_id",
								Completed: true,
								Stats:     Stats{In: 0, Out: 0, Failed: 0},
								DestinationsStatus: []DestinationStatus{
									{
										ID:        "destination_id",
										Completed: true,
										Stats:     Stats{In: 10, Out: 4, Failed: 6},
									},
								},
							},
						},
					},
				},
			}))
			failedRecords, err := sh.GetFailedRecords(context.Background(), jobRunId, jobFilters, noPaging)
			Expect(err).NotTo(HaveOccurred())
			Expect(failedRecords).To(Equal(JobFailedRecordsV2{ID: jobRunId}))
		})

		It("should be able to delete stats only", func() {
			otherJobTargetKey := defaultJobTargetKey
			otherJobTargetKey.SourceID = "other_source_id"
			jobRunId := newJobRunId()
			increment(resource.db, jobRunId, defaultJobTargetKey, stats, sh, nil)
			increment(resource.db, jobRunId, otherJobTargetKey, stats, sh, nil)

			addFailedRecords(resource.db, jobRunId, defaultJobTargetKey, sh, []FailedRecord{
				{Record: []byte(`{"record-1": "id-1"}`)},
				{Record: []byte(`{"record-2": "id-2"}`)},
			})
			addFailedRecords(resource.db, jobRunId, otherJobTargetKey, sh, []FailedRecord{
				{Record: []byte(`{"record-1": "id-1"}`)},
				{Record: []byte(`{"record-2": "id-2"}`)},
			})
			err := sh.DeleteJobStatus(context.Background(), jobRunId, JobFilter{SourceID: []string{"other_source_id"}})
			Expect(err).NotTo(HaveOccurred(), "it should be able to delete stats for the jobrunid")

			jobFilters := JobFilter{
				SourceID:  []string{"other_source_id"},
				TaskRunID: []string{"task_run_id"},
			}
			status, err := sh.GetStatus(context.Background(), jobRunId, jobFilters)
			Expect(err).To(HaveOccurred())
			Expect(status).To(Equal(JobStatus{}))
			Expect(errors.Is(err, ErrStatusNotFound)).To(BeTrue(), "it should return a StatusNotFoundError")
			failedRecords, err := sh.GetFailedRecords(context.Background(), jobRunId, jobFilters, noPaging)
			Expect(err).NotTo(HaveOccurred())
			Expect(failedRecords).To(Equal(
				JobFailedRecordsV2{
					ID: jobRunId,
					Tasks: []TaskFailedRecords[FailedRecord]{
						{
							ID: "task_run_id",
							Sources: []SourceFailedRecords[FailedRecord]{
								{
									ID: "other_source_id",
									Destinations: []DestinationFailedRecords[FailedRecord]{
										{
											ID: "destination_id",
											Records: []FailedRecord{
												{Record: []byte(`{"record-1": "id-1"}`)},
												{Record: []byte(`{"record-2": "id-2"}`)},
											},
										},
									},
								},
							},
						},
					},
				},
			))

			jobFilters.SourceID = []string{defaultJobTargetKey.SourceID}
			_, err = sh.GetStatus(context.Background(), jobRunId, jobFilters)
			Expect(err).ToNot(HaveOccurred())
			failedRecords, err = sh.GetFailedRecords(context.Background(), jobRunId, jobFilters, noPaging)
			Expect(err).NotTo(HaveOccurred())
			Expect(failedRecords).ToNot(Equal(JobFailedRecordsV2{ID: jobRunId}))
		})

		It("should be able to delete stats and failed keys partially", func() {
			otherJobTargetKey := defaultJobTargetKey
			otherJobTargetKey.SourceID = "other_source_id"
			jobRunId := newJobRunId()
			increment(resource.db, jobRunId, defaultJobTargetKey, stats, sh, nil)
			increment(resource.db, jobRunId, otherJobTargetKey, stats, sh, nil)

			addFailedRecords(resource.db, jobRunId, defaultJobTargetKey, sh, []FailedRecord{
				{Record: []byte(`{"record-1": "id-1"}`)},
				{Record: []byte(`{"record-2": "id-2"}`)},
			})
			addFailedRecords(resource.db, jobRunId, otherJobTargetKey, sh, []FailedRecord{
				{Record: []byte(`{"record-1": "id-1"}`)},
				{Record: []byte(`{"record-2": "id-2"}`)},
			})
			err := sh.DeleteJobStatus(context.Background(), jobRunId, JobFilter{SourceID: []string{"other_source_id"}})
			Expect(err).NotTo(HaveOccurred(), "it should be able to delete stats for the jobrunid")

			err = sh.DeleteFailedRecords(context.Background(), jobRunId, JobFilter{SourceID: []string{"other_source_id"}})
			Expect(err).NotTo(HaveOccurred(), "it should be able to delete failed keys for the jobrunid")

			jobFilters := JobFilter{
				SourceID:  []string{"other_source_id"},
				TaskRunID: []string{"task_run_id"},
			}
			status, err := sh.GetStatus(context.Background(), jobRunId, jobFilters)
			Expect(err).To(HaveOccurred())
			Expect(status).To(Equal(JobStatus{}))
			Expect(errors.Is(err, ErrStatusNotFound)).To(BeTrue(), "it should return a StatusNotFoundError")
			failedRecords, err := sh.GetFailedRecords(context.Background(), jobRunId, jobFilters, noPaging)
			Expect(err).NotTo(HaveOccurred())
			Expect(failedRecords).To(Equal(JobFailedRecordsV2{ID: jobRunId}))

			jobFilters.SourceID = []string{defaultJobTargetKey.SourceID}
			_, err = sh.GetStatus(context.Background(), jobRunId, jobFilters)
			Expect(err).ToNot(HaveOccurred())
			failedRecords, err = sh.GetFailedRecords(context.Background(), jobRunId, jobFilters, noPaging)
			Expect(err).NotTo(HaveOccurred())
			Expect(failedRecords).ToNot(Equal(JobFailedRecordsV2{ID: jobRunId}))
		})

		It("shouldn't be able to delete stats for an incomplete source", func() {
			jobRunId := newJobRunId()
			stats := Stats{
				In:     10,
				Out:    4,
				Failed: 5,
			}
			increment(resource.db, jobRunId, defaultJobTargetKey, stats, sh, nil)

			err := sh.DeleteJobStatus(context.Background(), jobRunId, JobFilter{SourceID: []string{defaultJobTargetKey.SourceID}})
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

			err := sh.DeleteJobStatus(context.Background(), "invalidJobRunId", JobFilter{SourceID: []string{defaultJobTargetKey.SourceID}})
			Expect(err).To(Equal(ErrStatusNotFound), "it shouldn't be able to delete stats for an invalid jobrunid")
		})

		It("should be able to get failed records by filtering", func() {
			jobRunId := newJobRunId()

			addFailedRecords(resource.db, jobRunId, JobTargetKey{
				TaskRunID:     "task_run_id1",
				SourceID:      "source_id1",
				DestinationID: "destination_id",
			}, sh, []FailedRecord{
				{Record: []byte(`{"record-1": "id-1"}`)},
				{Record: []byte(`{"record-2": "id-2"}`)},
			})
			addFailedRecords(resource.db, jobRunId, JobTargetKey{
				TaskRunID:     "task_run_id1",
				SourceID:      "source_id1",
				DestinationID: "destination_id",
			}, sh, []FailedRecord{
				{Record: []byte(`{"record-11": "id-112"}`)},
				{Record: []byte(`{"record-22": "id-222"}`)},
			})
			addFailedRecords(resource.db, jobRunId, JobTargetKey{
				TaskRunID:     "task_run_id1",
				SourceID:      "source_id2",
				DestinationID: "destination_id",
			}, sh, []FailedRecord{
				{Record: []byte(`{"record-12": "id-12"}`)},
				{Record: []byte(`{"record-21": "id-21"}`)},
			})
			addFailedRecords(resource.db, jobRunId, JobTargetKey{
				TaskRunID:     "task_run_id2",
				SourceID:      "source_id2",
				DestinationID: "destination_id",
			}, sh, []FailedRecord{
				{Record: []byte(`{"record-11": "id-11"}`)},
				{Record: []byte(`{"record-32": "id-32"}`)},
			})
			addFailedRecords(resource.db, jobRunId, JobTargetKey{
				TaskRunID:     "task_run_id2",
				SourceID:      "source_id3",
				DestinationID: "destination_id",
			}, sh, []FailedRecord{
				{Record: []byte(`{"record-13": "id-13"}`)},
				{Record: []byte(`{"record-23": "id-23"}`)},
			})
			expected := JobFailedRecordsV2{
				ID: jobRunId,
				Tasks: []TaskFailedRecords[FailedRecord]{
					{
						ID: "task_run_id1",
						Sources: []SourceFailedRecords[FailedRecord]{
							{
								ID: "source_id1",
								Destinations: []DestinationFailedRecords[FailedRecord]{{
									ID: "destination_id",
									Records: []FailedRecord{
										{Record: []byte(`{"record-1": "id-1"}`)},
										{Record: []byte(`{"record-11": "id-112"}`)},
										{Record: []byte(`{"record-2": "id-2"}`)},
										{Record: []byte(`{"record-22": "id-222"}`)},
									},
								}},
							},
							{
								ID: "source_id2",
								Destinations: []DestinationFailedRecords[FailedRecord]{{
									ID: "destination_id",
									Records: []FailedRecord{
										{Record: []byte(`{"record-12": "id-12"}`)},
										{Record: []byte(`{"record-21": "id-21"}`)},
									},
								}},
							},
						},
					},
					{
						ID: "task_run_id2",
						Sources: []SourceFailedRecords[FailedRecord]{{
							ID: "source_id2",
							Destinations: []DestinationFailedRecords[FailedRecord]{{
								ID: "destination_id",
								Records: []FailedRecord{
									{Record: []byte(`{"record-11": "id-11"}`)},
									{Record: []byte(`{"record-32": "id-32"}`)},
								},
							}},
						}},
					},
				},
			}
			failedRecords, err := sh.GetFailedRecords(context.Background(), jobRunId, JobFilter{
				SourceID:  []string{"source_id1", "source_id2"},
				TaskRunID: []string{"task_run_id1", "task_run_id2"},
			}, noPaging)
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
			addFailedRecords(resource.db, jobRunId, defaultJobTargetKey, sh, []FailedRecord{
				{Record: []byte(`{"record-1": "id-1"}`)},
				{Record: []byte(`{"record-2": "id-2"}`)},
			})
			ts := time.Now().Add(-time.Duration(defaultRetentionPeriodInHours+1) * time.Hour)
			stmt, err := resource.db.Prepare(`update "rsources_stats" set ts = $1`)
			Expect(err).NotTo(HaveOccurred())
			_, err = stmt.Exec(ts)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = stmt.Close() }()
			stmt2, err := resource.db.Prepare(`update "rsources_failed_keys_v2_records" set ts = $1`)
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

					sqlStatement = `select count(*) from "rsources_failed_keys_v2" k join "rsources_failed_keys_v2_records" v on v.id = k.id where k.job_run_id = $1`
					var failedRecordCount int
					err = resource.db.QueryRow(sqlStatement, jobRunId).Scan(&failedRecordCount)
					Expect(err).NotTo(HaveOccurred())

					if statCount == 0 && failedRecordCount == 0 {
						return
					}
				}
			}
		})

		It("should be able to add failed concurrently", func() {
			jobRunId := newJobRunId()
			tx0, err := resource.db.Begin()
			Expect(err).ShouldNot(HaveOccurred(), "it should be able to begin the transaction")
			// Create a record in the rsources_failed_keys_v2 table without committing the transaction
			_, err = tx0.Exec("INSERT INTO rsources_failed_keys_v2 (id, job_run_id, task_run_id, source_id, destination_id) VALUES ($1, $2, $3, $4, $5)",
				ksuid.New().String(), jobRunId, defaultJobTargetKey.TaskRunID, defaultJobTargetKey.SourceID, defaultJobTargetKey.DestinationID)
			Expect(err).ShouldNot(HaveOccurred(), "it should be able to insert a record in the rsources_failed_keys_v2 table")
			// Start 2 goroutines both trying to add failed records for the same job target key
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
			defer cancel()
			g, ctx := errgroup.WithContext(ctx)
			g.Go(func() error {
				tx1, err := resource.db.BeginTx(ctx, nil)
				if err != nil {
					return fmt.Errorf("beginning tx1: %w", err)
				}
				if err := sh.AddFailedRecords(ctx, tx1, jobRunId, defaultJobTargetKey, []FailedRecord{
					{Record: []byte(`"record-1"`)},
					{Record: []byte(`"record-2"`)},
				}); err != nil {
					return fmt.Errorf("adding failed records during tx1: %w", err)
				}
				if err := tx1.Commit(); err != nil {
					return fmt.Errorf("committing tx1: %w", err)
				}
				return nil
			})
			g.Go(func() error {
				tx2, err := resource.db.Begin()
				if err != nil {
					return fmt.Errorf("beginning tx2: %w", err)
				}
				if err := sh.AddFailedRecords(ctx, tx2, jobRunId, defaultJobTargetKey, []FailedRecord{
					{Record: []byte(`"record-2"`)},
					{Record: []byte(`"record-3"`)},
				}); err != nil {
					return fmt.Errorf("adding failed records during tx2: %w", err)
				}
				if err := tx2.Commit(); err != nil {
					return fmt.Errorf("committing tx2: %w", err)
				}
				return nil
			})

			// wait for the goroutines to block on the query
			time.Sleep(1 * time.Second)

			// rollback tx0 so that the goroutines can continue
			Expect(tx0.Rollback()).ShouldNot(HaveOccurred(), "it should be able to rollback tx0")
			Expect(g.Wait()).ShouldNot(HaveOccurred(), "it should be able to add failed records concurrently")

			jobFilters := JobFilter{
				SourceID:  []string{"source_id"},
				TaskRunID: []string{"task_run_id"},
			}

			failedRecords, err := sh.GetFailedRecords(context.Background(), jobRunId, jobFilters, noPaging)
			Expect(err).NotTo(HaveOccurred(), "it should be able to get failed records")
			expcetedRecords := JobFailedRecordsV2{
				ID: jobRunId,
				Tasks: []TaskFailedRecords[FailedRecord]{{
					ID: "task_run_id",
					Sources: []SourceFailedRecords[FailedRecord]{{
						ID: "source_id",
						Destinations: []DestinationFailedRecords[FailedRecord]{{
							ID: "destination_id",
							Records: []FailedRecord{
								{Record: []byte(`"record-1"`)},
								{Record: []byte(`"record-2"`)},
								{Record: []byte(`"record-3"`)},
							},
						}},
					}},
				}},
			}
			Expect(failedRecords).To(Equal(expcetedRecords), "it should be able to get failed records")
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

		BeforeEach(func() {
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
				ShouldSetupSharedDB:    true,
			}

			configB = JobServiceConfig{
				LocalHostname:          postgres2Hostname,
				MaxPoolSize:            1,
				LocalConn:              pgB.externalDSN,
				SharedConn:             pgC.externalDSN,
				SubscriptionTargetConn: pgB.internalDSN,
				Log:                    testlog.GinkgoLogger,
				ShouldSetupSharedDB:    true,
			}

			// Start 2 JobServices
			// 1. js1 with local=pgA, remote=pgC
			// 2. js2 with local=pgB, remote=pgC
			serviceA = createService(configA)
			serviceB = createService(configB)
		})

		AfterEach(func() {
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
			addFailedRecords(pgA.db, jobRunId, defaultJobTargetKey, serviceA, []FailedRecord{
				{Record: json.RawMessage(`{"id": "1"}`)},
				{Record: json.RawMessage(`{"id": "2"}`)},
			})
			addFailedRecords(pgB.db, jobRunId, defaultJobTargetKey, serviceB, []FailedRecord{
				{Record: json.RawMessage(`{"id": "2"}`)},
				{Record: json.RawMessage(`{"id": "3"}`)},
			})
			expected := JobFailedRecordsV2{
				ID: jobRunId,
				Tasks: []TaskFailedRecords[FailedRecord]{
					{
						ID: defaultJobTargetKey.TaskRunID,
						Sources: []SourceFailedRecords[FailedRecord]{
							{
								ID: defaultJobTargetKey.SourceID,
								Destinations: []DestinationFailedRecords[FailedRecord]{{
									ID: defaultJobTargetKey.DestinationID,
									Records: []FailedRecord{
										{Record: []byte(`{"id": "1"}`)},
										{Record: []byte(`{"id": "2"}`)},
										{Record: []byte(`{"id": "2"}`)},
										{Record: []byte(`{"id": "3"}`)},
									},
								}},
							},
						},
					},
				},
			}

			var err error
			var failedKeysA, failedKeysB JobFailedRecordsV2
			Eventually(func() bool {
				failedKeysA, err = serviceA.GetFailedRecords(context.Background(), jobRunId, JobFilter{
					SourceID:  []string{"source_id"},
					TaskRunID: []string{"task_run_id"},
				}, noPaging)
				if err != nil {
					err = fmt.Errorf("failed to get failed records from serviceA: %w", err)
					return false
				}
				failedKeysB, err = serviceB.GetFailedRecords(context.Background(), jobRunId, JobFilter{
					SourceID:  []string{"source_id"},
					TaskRunID: []string{"task_run_id"},
				}, noPaging)
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
					return string(failedKeysA.Tasks[0].Sources[0].Destinations[0].Records[i].Record) < string(failedKeysA.Tasks[0].Sources[0].Destinations[0].Records[j].Record)
				})
				if !reflect.DeepEqual(failedKeysA, expected) {
					err = fmt.Errorf("failed keys from serviceA don't match expectation")
					return false
				}
				return true
			}, "30s", "100ms").Should(BeTrue(), "Failed Records from both services should be the same", string(mustMarshal(failedKeysA)), string(mustMarshal(failedKeysB)), string(mustMarshal(expected)), err)
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
				ShouldSetupSharedDB:    true,
			}
			sts, err := memstats.New()
			Expect(err).NotTo(HaveOccurred())

			_, err = NewJobService(badConfig, sts)
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
				ShouldSetupSharedDB:    true,
			}
			sts, err := memstats.New()
			Expect(err).NotTo(HaveOccurred())

			_, err = NewJobService(badConfig, sts)
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

		BeforeEach(func() {
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
				ShouldSetupSharedDB:    true,
			}
			serviceA = createService(configA)
		})

		AfterEach(func() {
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

	Context("migrating to failed_keys_v2", Ordered, func() {
		It("should be able to add rsources_failed_keys_v2 table to the publication and subscription seamlessly", func() {
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
				ShouldSetupSharedDB:    true,
			}

			configB := JobServiceConfig{
				LocalHostname:          postgres2Hostname,
				MaxPoolSize:            1,
				LocalConn:              pgB.externalDSN,
				SharedConn:             pgC.externalDSN,
				SubscriptionTargetConn: pgB.internalDSN,
				Log:                    log,
				ShouldSetupSharedDB:    true,
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
			err = setupFailedKeysTableV0(context.Background(), databaseA, configA.LocalHostname, log)
			Expect(err).NotTo(HaveOccurred())
			err = setupStatsTable(context.Background(), databaseB, configB.LocalHostname, log)
			Expect(err).NotTo(HaveOccurred())
			err = setupFailedKeysTableV0(context.Background(), databaseB, configB.LocalHostname, log)
			Expect(err).NotTo(HaveOccurred())
			err = setupStatsTable(context.Background(), databaseC, "shared", log)
			Expect(err).NotTo(HaveOccurred())
			err = setupFailedKeysTableV0(context.Background(), databaseC, "shared", log)
			Expect(err).NotTo(HaveOccurred())

			// setup logical replication(only stats tables as previously done)
			publicationQueryA := `CREATE PUBLICATION "rsources_stats_pub" FOR TABLE rsources_stats`
			publicationQueryB := `ALTER PUBLICATION "rsources_stats_pub" ADD TABLE rsources_failed_keys`
			_, err = databaseA.ExecContext(context.TODO(), publicationQueryA)
			Expect(err).NotTo(HaveOccurred())
			_, err = databaseA.ExecContext(context.TODO(), publicationQueryB)
			Expect(err).NotTo(HaveOccurred())
			_, err = databaseB.ExecContext(context.TODO(), publicationQueryA)
			Expect(err).NotTo(HaveOccurred())
			_, err = databaseB.ExecContext(context.TODO(), publicationQueryB)
			Expect(err).NotTo(HaveOccurred())

			subscriptionQuery := `CREATE SUBSCRIPTION "%s" CONNECTION '%s' PUBLICATION "rsources_stats_pub"`

			normalizedHostnameA := replSlotDisallowedChars.ReplaceAllString(strings.ToLower(configA.LocalHostname), "_")
			subscriptionAName := fmt.Sprintf("%s_rsources_stats_sub", normalizedHostnameA)
			_, err = databaseC.ExecContext(context.Background(), fmt.Sprintf(subscriptionQuery, subscriptionAName, pgA.internalDSN))
			Expect(err).NotTo(HaveOccurred())

			normalizedHostnameB := replSlotDisallowedChars.ReplaceAllString(strings.ToLower(configB.LocalHostname), "_")
			subscriptionBName := fmt.Sprintf("%s_rsources_stats_sub", normalizedHostnameB)
			_, err = databaseC.ExecContext(context.Background(), fmt.Sprintf(subscriptionQuery, subscriptionBName, pgB.internalDSN))
			Expect(err).NotTo(HaveOccurred())

			// Fill some data in the tables to verify migration
			_, err = databaseA.ExecContext(context.TODO(), `INSERT INTO rsources_stats (job_run_id, task_run_id, source_id, destination_id, in_count, out_count, failed_count) VALUES ('migrated-1','migrated-1','migrated-1','migrated-1',2,1,1)`)
			Expect(err).NotTo(HaveOccurred())
			_, err = databaseA.ExecContext(context.TODO(), `INSERT INTO rsources_failed_keys (job_run_id, task_run_id, source_id, destination_id, record_id) VALUES ('migrated-1','migrated-1','migrated-1','migrated-1','"migrated-1"')`)
			Expect(err).NotTo(HaveOccurred())
			_, err = databaseA.ExecContext(context.TODO(), `INSERT INTO rsources_failed_keys (job_run_id, task_run_id, source_id, destination_id, record_id) VALUES ('migrated-1','migrated-1','migrated-1','migrated-1','"migrated-1"')`)
			Expect(err).NotTo(HaveOccurred())

			// Now setup the handlers
			serviceA := createService(configA)
			serviceB := createService(configB)

			// make sure previous table is gone
			for _, db := range []*sql.DB{databaseA, databaseB, databaseA} {
				var exists bool
				err = db.QueryRow(`SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema') AND  tablename  = 'rsources_failed_keys')`).Scan(&exists)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeFalse())
			}

			var failedKeysA, failedKeysB, expected JobFailedRecordsV2

			// migrated data
			Eventually(func() bool {
				expected = JobFailedRecordsV2{
					ID: "migrated-1",
					Tasks: []TaskFailedRecords[FailedRecord]{
						{
							ID: "migrated-1",
							Sources: []SourceFailedRecords[FailedRecord]{
								{
									ID: "migrated-1",
									Destinations: []DestinationFailedRecords[FailedRecord]{{
										ID: "migrated-1",
										Records: []FailedRecord{
											{Record: []byte(`"migrated-1"`)},
										},
									}},
								},
							},
						},
					},
				}
				failedKeysA, err = serviceA.GetFailedRecords(context.Background(), "migrated-1", JobFilter{
					SourceID:  []string{"migrated-1"},
					TaskRunID: []string{"migrated-1"},
				}, noPaging)
				if err != nil {
					err = fmt.Errorf("failed to get failed records from serviceA: %w", err)
					return false
				}
				failedKeysB, err = serviceB.GetFailedRecords(context.Background(), "migrated-1", JobFilter{
					SourceID:  []string{"migrated-1"},
					TaskRunID: []string{"migrated-1"},
				}, noPaging)
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
					return string(failedKeysA.Tasks[0].Sources[0].Destinations[0].Records[i].Record) < string(failedKeysA.Tasks[0].Sources[0].Destinations[0].Records[j].Record)
				})
				if !reflect.DeepEqual(failedKeysA, expected) {
					err = fmt.Errorf("failed keys from serviceA don't match expectation")
					return false
				}
				return true
			}, "30s", "100ms").Should(BeTrue(), "Failed Records from both services should be the same", string(mustMarshal(failedKeysA)), string(mustMarshal(failedKeysB)), string(mustMarshal(expected)), err)

			jobRunId := newJobRunId()
			addFailedRecords(pgA.db, jobRunId, defaultJobTargetKey, serviceA, []FailedRecord{
				{Record: json.RawMessage(`{"id": "1"}`)},
				{Record: json.RawMessage(`{"id": "2"}`)},
			})
			addFailedRecords(pgB.db, jobRunId, defaultJobTargetKey, serviceB, []FailedRecord{
				{Record: json.RawMessage(`{"id": "2"}`)},
				{Record: json.RawMessage(`{"id": "3"}`)},
			})
			expected = JobFailedRecordsV2{
				ID: jobRunId,
				Tasks: []TaskFailedRecords[FailedRecord]{
					{
						ID: defaultJobTargetKey.TaskRunID,
						Sources: []SourceFailedRecords[FailedRecord]{
							{
								ID: defaultJobTargetKey.SourceID,
								Destinations: []DestinationFailedRecords[FailedRecord]{{
									ID: defaultJobTargetKey.DestinationID,
									Records: []FailedRecord{
										{Record: []byte(`{"id": "1"}`)},
										{Record: []byte(`{"id": "2"}`)},
										{Record: []byte(`{"id": "2"}`)},
										{Record: []byte(`{"id": "3"}`)},
									},
								}},
							},
						},
					},
				},
			}

			Eventually(func() bool {
				failedKeysA, err = serviceA.GetFailedRecords(context.Background(), jobRunId, JobFilter{
					SourceID:  []string{"source_id"},
					TaskRunID: []string{"task_run_id"},
				}, noPaging)
				if err != nil {
					err = fmt.Errorf("failed to get failed records from serviceA: %w", err)
					return false
				}
				failedKeysB, err = serviceB.GetFailedRecords(context.Background(), jobRunId, JobFilter{
					SourceID:  []string{"source_id"},
					TaskRunID: []string{"task_run_id"},
				}, noPaging)
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
					return string(failedKeysA.Tasks[0].Sources[0].Destinations[0].Records[i].Record) < string(failedKeysA.Tasks[0].Sources[0].Destinations[0].Records[j].Record)
				})
				if !reflect.DeepEqual(failedKeysA, expected) {
					err = fmt.Errorf("failed keys from serviceA don't match expectation")
					return false
				}
				return true
			}, "30s", "100ms").Should(BeTrue(), "Failed Records from both services should be the same", string(mustMarshal(failedKeysA)), string(mustMarshal(failedKeysB)), string(mustMarshal(expected)), err)
		})

		It("should be able to add a code column to rsources_failed_keys_v2_records table seamlessly, without affecting logical replication", func() {
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
				ShouldSetupSharedDB:    true,
			}

			configB := JobServiceConfig{
				LocalHostname:          postgres2Hostname,
				MaxPoolSize:            1,
				LocalConn:              pgB.externalDSN,
				SharedConn:             pgC.externalDSN,
				SubscriptionTargetConn: pgB.internalDSN,
				Log:                    log,
				ShouldSetupSharedDB:    true,
			}

			// Setting up previous environment before adding failedkeys table to the publication
			// setup databases
			databaseA := getDB(configA.LocalConn, configA.MaxPoolSize)
			defer func() { _ = databaseA.Close() }()
			databaseB := getDB(configB.LocalConn, configB.MaxPoolSize)
			defer func() { _ = databaseB.Close() }()
			databaseC := getDB(configB.SharedConn, configB.MaxPoolSize) // shared
			defer func() { _ = databaseC.Close() }()

			dropColumn := func(db *sql.DB) {
				_, err := db.Exec("alter table rsources_failed_keys_v2_records drop column code")
				Expect(err).NotTo(HaveOccurred())
			}
			// create tables

			err = setupStatsTable(context.Background(), databaseA, configA.LocalHostname, log)
			Expect(err).NotTo(HaveOccurred())
			err = setupFailedKeysTable(context.Background(), databaseA, configA.LocalHostname, log)
			Expect(err).NotTo(HaveOccurred())
			dropColumn(databaseA)

			err = setupStatsTable(context.Background(), databaseB, configB.LocalHostname, log)
			Expect(err).NotTo(HaveOccurred())
			err = setupFailedKeysTable(context.Background(), databaseB, configB.LocalHostname, log)
			Expect(err).NotTo(HaveOccurred())
			dropColumn(databaseB)

			err = setupStatsTable(context.Background(), databaseC, "shared", log)
			Expect(err).NotTo(HaveOccurred())
			err = setupFailedKeysTable(context.Background(), databaseC, "shared", log)
			Expect(err).NotTo(HaveOccurred())
			dropColumn(databaseC)

			// Fill some data in the tables to verify migration
			_, err = databaseA.ExecContext(context.TODO(), `INSERT INTO rsources_stats (job_run_id, task_run_id, source_id, destination_id, in_count, out_count, failed_count) VALUES ('migrated-1','migrated-1','migrated-1','migrated-1',2,1,1)`)
			Expect(err).NotTo(HaveOccurred())
			_, err = databaseA.ExecContext(context.TODO(), `INSERT INTO rsources_failed_keys_v2 (id, db_name, job_run_id, task_run_id, source_id, destination_id) VALUES ('1', 'db', 'migrated-1','migrated-1','migrated-1','migrated-1')`)
			Expect(err).NotTo(HaveOccurred())
			_, err = databaseA.ExecContext(context.TODO(), `INSERT INTO rsources_failed_keys_v2_records (id, record_id) VALUES ('1','"migrated-1"')`)
			Expect(err).NotTo(HaveOccurred())

			// Create services again to add the column
			serviceA := createService(configA)
			serviceB := createService(configB)

			var failedKeysA, failedKeysB, expected JobFailedRecordsV2

			// migrated data
			Eventually(func() bool {
				expected = JobFailedRecordsV2{
					ID: "migrated-1",
					Tasks: []TaskFailedRecords[FailedRecord]{
						{
							ID: "migrated-1",
							Sources: []SourceFailedRecords[FailedRecord]{
								{
									ID: "migrated-1",
									Destinations: []DestinationFailedRecords[FailedRecord]{{
										ID: "migrated-1",
										Records: []FailedRecord{
											{Record: []byte(`"migrated-1"`)},
										},
									}},
								},
							},
						},
					},
				}
				failedKeysA, err = serviceA.GetFailedRecords(context.Background(), "migrated-1", JobFilter{
					SourceID:  []string{"migrated-1"},
					TaskRunID: []string{"migrated-1"},
				}, noPaging)
				if err != nil {
					err = fmt.Errorf("failed to get failed records from serviceA: %w", err)
					return false
				}
				failedKeysB, err = serviceB.GetFailedRecords(context.Background(), "migrated-1", JobFilter{
					SourceID:  []string{"migrated-1"},
					TaskRunID: []string{"migrated-1"},
				}, noPaging)
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
					return string(failedKeysA.Tasks[0].Sources[0].Destinations[0].Records[i].Record) < string(failedKeysA.Tasks[0].Sources[0].Destinations[0].Records[j].Record)
				})
				if !reflect.DeepEqual(failedKeysA, expected) {
					err = fmt.Errorf("failed keys from serviceA don't match expectation")
					return false
				}
				return true
			}, "30s", "100ms").Should(BeTrue(), "Failed Records from both services should be the same", string(mustMarshal(failedKeysA)), string(mustMarshal(failedKeysB)), string(mustMarshal(expected)), err)

			jobRunId := newJobRunId()
			addFailedRecords(pgA.db, jobRunId, defaultJobTargetKey, serviceA, []FailedRecord{
				{Record: json.RawMessage(`{"id": "1"}`), Code: 1},
				{Record: json.RawMessage(`{"id": "2"}`), Code: 1},
			})
			addFailedRecords(pgB.db, jobRunId, defaultJobTargetKey, serviceB, []FailedRecord{
				{Record: json.RawMessage(`{"id": "2"}`), Code: 1},
				{Record: json.RawMessage(`{"id": "3"}`), Code: 1},
			})
			expected = JobFailedRecordsV2{
				ID: jobRunId,
				Tasks: []TaskFailedRecords[FailedRecord]{
					{
						ID: defaultJobTargetKey.TaskRunID,
						Sources: []SourceFailedRecords[FailedRecord]{
							{
								ID: defaultJobTargetKey.SourceID,
								Destinations: []DestinationFailedRecords[FailedRecord]{{
									ID: defaultJobTargetKey.DestinationID,
									Records: []FailedRecord{
										{Record: []byte(`{"id": "1"}`), Code: 1},
										{Record: []byte(`{"id": "2"}`), Code: 1},
										{Record: []byte(`{"id": "2"}`), Code: 1},
										{Record: []byte(`{"id": "3"}`), Code: 1},
									},
								}},
							},
						},
					},
				},
			}

			Eventually(func() bool {
				failedKeysA, err = serviceA.GetFailedRecords(context.Background(), jobRunId, JobFilter{
					SourceID:  []string{"source_id"},
					TaskRunID: []string{"task_run_id"},
				}, noPaging)
				if err != nil {
					err = fmt.Errorf("failed to get failed records from serviceA: %w", err)
					return false
				}
				failedKeysB, err = serviceB.GetFailedRecords(context.Background(), jobRunId, JobFilter{
					SourceID:  []string{"source_id"},
					TaskRunID: []string{"task_run_id"},
				}, noPaging)
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
					return string(failedKeysA.Tasks[0].Sources[0].Destinations[0].Records[i].Record) < string(failedKeysA.Tasks[0].Sources[0].Destinations[0].Records[j].Record)
				})
				if !reflect.DeepEqual(failedKeysA, expected) {
					err = fmt.Errorf("failed keys from serviceA don't match expectation")
					return false
				}
				return true
			}, "30s", "100ms").Should(BeTrue(), "Failed Records from both services should be the same", string(mustMarshal(failedKeysA)), string(mustMarshal(failedKeysB)), string(mustMarshal(expected)), err)
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
	sts, err := memstats.New()
	Expect(err).NotTo(HaveOccurred())

	service, err := NewJobService(config, sts)
	Expect(err).ShouldNot(HaveOccurred(), "it should be able to create the service")
	return service
}

func addFailedRecords(db *sql.DB, jobRunId string, jobTargetKey JobTargetKey, sh JobService, records []FailedRecord) {
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
	cmd := []string{"postgres"}
	if len(params) > 0 {
		cmd = append(cmd, "-c")
		cmd = append(cmd, params...)
	}
	username := rand.String(10)
	password := rand.String(10)
	database := rand.String(10)
	// pulls an image, creates a container based on it and runs it
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "15-alpine",
		NetworkID:  networkId,
		Hostname:   hostname,
		Env: []string{
			"POSTGRES_PASSWORD=" + password,
			"POSTGRES_DB=" + database,
			"POSTGRES_USER=" + username,
		},
		ExposedPorts: []string{"5432"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"5432/tcp": {
				{
					HostIP:   "127.0.0.1",
					HostPort: "",
				},
			},
		},
		Cmd: cmd,
	}, func(hc *docker.HostConfig) {
		hc.PublishAllPorts = false
	})
	if err != nil {
		Expect(err).NotTo(HaveOccurred())
	}

	port := resource.GetPort("5432/tcp")
	externalDSN := fmt.Sprintf("postgres://%[1]s:%[2]s@127.0.0.1:%[3]s/%[4]s?sslmode=disable", username, password, port, database)
	internalDSN := fmt.Sprintf("postgres://%[1]s:%[2]s@%[3]s:5432/%[4]s?sslmode=disable", username, password, hostname, database)
	var (
		db  *sql.DB
		dsn = fmt.Sprintf("host=localhost port=%[1]s user=%[2]s password=%[3]s dbname=%[4]s sslmode=disable options='-c idle_in_transaction_session_timeout=300000' ", port, username, password, database)
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
	return ksuid.New().String()
}
