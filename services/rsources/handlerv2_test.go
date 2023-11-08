package rsources

import (
	"context"
	"encoding/json"
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ory/dockertest/v3"

	testlog "github.com/rudderlabs/rudder-server/testhelper/log"
)

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

		It("should be able to delete failed keys", func() {
			jobRunId := newJobRunId()
			increment(resource.db, jobRunId, defaultJobTargetKey, stats, sh, nil)
			addFailedRecords(resource.db, jobRunId, defaultJobTargetKey, sh, []json.RawMessage{
				[]byte(`{"record-1": "id-1"}`),
				[]byte(`{"record-2": "id-2"}`),
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
			Expect(failedRecords).To(Equal(JobFailedRecords{ID: jobRunId}))
		})

		It("should be able to delete stats only", func() {
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
				JobFailedRecords{
					ID: jobRunId,
					Tasks: []TaskFailedRecords{
						{
							ID: "task_run_id",
							Sources: []SourceFailedRecords{
								{
									ID: "other_source_id",
									Destinations: []DestinationFailedRecords{
										{
											ID: "destination_id",
											Records: []json.RawMessage{
												[]byte(`{"record-1": "id-1"}`),
												[]byte(`{"record-2": "id-2"}`),
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
	})
})
