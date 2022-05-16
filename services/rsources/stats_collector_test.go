package rsources

import (
	"context"
	"encoding/json"
	"fmt"

	uuid "github.com/gofrs/uuid"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

var _ = Describe("Using StatsCollector", Serial, func() {
	var (
		jobs           []*jobsdb.JobT
		jobErrors      map[uuid.UUID]string
		jobStatuses    []*jobsdb.JobStatusT
		mockCtrl       *gomock.Controller
		js             *MockJobService
		statsCollector StatsCollector
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		js = NewMockJobService(mockCtrl)
		statsCollector = NewStatsCollector(js)
		jobs = []*jobsdb.JobT{}
		jobErrors = map[uuid.UUID]string{}
		jobStatuses = []*jobsdb.JobStatusT{}

	})

	When("there are rudder-sources jobs", func() {
		var params jobParams
		BeforeEach(func() {
			params = jobParams{
				JobRunID:      "jobRunId",
				TaskRunID:     "taskRunId",
				SourceID:      "sourceId",
				DestinationID: "destinationId",
			}
			jobs = append(jobs, generateJobs(10, params)...)
		})

		Context("all jobs are stored", func() {
			BeforeEach(func() {
				statsCollector.JobsStored(jobs)
			})

			It("can publish without error all statuses as In stats", func() {

				js.EXPECT().
					IncrementStats(
						gomock.Any(),
						gomock.Any(),
						params.JobRunID,
						JobTargetKey{
							TaskRunID:     params.TaskRunID,
							SourceID:      params.SourceID,
							DestinationID: params.DestinationID,
						},
						Stats{
							In: uint(len(jobs)),
						}).
					Times(1)

				err := statsCollector.Publish(context.TODO(), nil)
				Expect(err).To(BeNil())
			})

			Context("underlying service returns an error", func() {
				BeforeEach(func() {
					js.EXPECT().
						IncrementStats(
							gomock.Any(),
							gomock.Any(),
							params.JobRunID,
							JobTargetKey{
								TaskRunID:     params.TaskRunID,
								SourceID:      params.SourceID,
								DestinationID: params.DestinationID,
							},
							Stats{
								In: uint(len(jobs)),
							}).
						Times(1).
						Return(fmt.Errorf("error"))
				})
				It("fails during publish", func() {
					err := statsCollector.Publish(context.TODO(), nil)
					Expect(err).ToNot(BeNil())

				})
			})

		})

		Context("half of the jobs have errors", func() {
			BeforeEach(func() {
				for i, job := range jobs {
					if i < len(jobs)/2 {
						jobErrors[job.UUID] = "error"
					}
				}
			})
			Context("the rest of the jobs are stored", func() {
				BeforeEach(func() {
					statsCollector.JobsStoredWithErrors(jobs, jobErrors)
				})
				It("can publish without error all successful jobs as In stats", func() {

					js.EXPECT().
						IncrementStats(
							gomock.Any(),
							gomock.Any(),
							params.JobRunID,
							JobTargetKey{
								TaskRunID:     params.TaskRunID,
								SourceID:      params.SourceID,
								DestinationID: params.DestinationID,
							},
							Stats{
								In: uint(len(jobs) / 2),
							}).
						Times(1)

					err := statsCollector.Publish(context.TODO(), nil)
					Expect(err).To(BeNil())
				})

			})
		})

		Context("processing of jobs has started", func() {
			BeforeEach(func() {
				statsCollector.BeginProcessing(jobs)
			})

			Context("all jobs have succeeded", func() {
				BeforeEach(func() {
					for _, job := range jobs {
						jobStatuses = append(jobStatuses, newSucceededStatus(job.JobID))
					}
				})
				Context("it calls JobStatusesUpdated", func() {
					BeforeEach(func() {
						statsCollector.JobStatusesUpdated(jobStatuses)
					})

					It("can publish without error all statuses as Out stats", func() {
						js.EXPECT().
							IncrementStats(
								gomock.Any(),
								gomock.Any(),
								params.JobRunID,
								JobTargetKey{
									TaskRunID:     params.TaskRunID,
									SourceID:      params.SourceID,
									DestinationID: params.DestinationID,
								},
								Stats{
									Out: uint(len(jobs)),
								}).
							Times(1)
						err := statsCollector.Publish(context.TODO(), nil)
						Expect(err).To(BeNil())

					})
				})
			})

			Context("all jobs have failed", func() {
				BeforeEach(func() {
					for _, job := range jobs {
						jobStatuses = append(jobStatuses, newFailedStatus(job.JobID))
					}
				})
				Context("it calls JobStatusesUpdated", func() {
					BeforeEach(func() {
						statsCollector.JobStatusesUpdated(jobStatuses)
					})

					It("can publish without error all statuses but without actually updating stats", func() {
						err := statsCollector.Publish(context.TODO(), nil)
						Expect(err).To(BeNil())

					})
				})
			})

			Context("half jobs have failed and half jobs are aborted", func() {
				BeforeEach(func() {
					for i, job := range jobs {
						if i >= len(jobs)/2 {
							jobStatuses = append(jobStatuses, newFailedStatus(job.JobID))
						} else {
							jobStatuses = append(jobStatuses, newAbortedStatus(job.JobID))
						}
					}
				})
				Context("it calls JobStatusesUpdated", func() {
					BeforeEach(func() {
						statsCollector.JobStatusesUpdated(jobStatuses)
					})

					It("can publish without error all statuses but with updating half stats as Failed stats", func() {

						js.EXPECT().
							IncrementStats(
								gomock.Any(),
								gomock.Any(),
								params.JobRunID,
								JobTargetKey{
									TaskRunID:     params.TaskRunID,
									SourceID:      params.SourceID,
									DestinationID: params.DestinationID,
								},
								Stats{
									Failed: uint(len(jobs) / 2),
								}).
							Times(1)

						err := statsCollector.Publish(context.TODO(), nil)
						Expect(err).To(BeNil())

					})
				})
			})
		})

		Context("processing of jobs has not started", func() {
			It("should not allow for calling JobStatusesUpdated", func() {
				var err error
				defer func() {
					err = recover().(error)
					Expect(err).ToNot(BeNil())
				}()
				statsCollector.JobStatusesUpdated(jobStatuses)
				Expect(err).ToNot(BeNil())
			})
		})

	})

	When("there are no rudder-sources jobs", func() {
		var params jobParams
		BeforeEach(func() {
			params = jobParams{
				SourceID:      "sourceId",
				DestinationID: "destinationId",
			}
			jobs = append(jobs, generateJobs(10, params)...)
		})

		Context("all jobs are stored", func() {
			BeforeEach(func() {
				statsCollector.JobsStored(jobs)
			})
			It("doesn't publish any jobs", func() {
				// no js.EXPECT
				err := statsCollector.Publish(context.TODO(), nil)
				Expect(err).To(BeNil())
			})
		})

		Context("processing of jobs has started", func() {
			BeforeEach(func() {
				statsCollector.BeginProcessing(jobs)
			})
			Context("all jobs have succeeded", func() {
				BeforeEach(func() {
					for _, job := range jobs {
						jobStatuses = append(jobStatuses, newSucceededStatus(job.JobID))
					}
				})
				Context("it calls JobStatusesUpdated", func() {
					BeforeEach(func() {
						statsCollector.JobStatusesUpdated(jobStatuses)
					})

					It("doesn't publish any jobs", func() {
						// no js.EXPECT
						err := statsCollector.Publish(context.TODO(), nil)
						Expect(err).To(BeNil())
					})
				})
			})
		})
	})
})

type jobParams struct {
	JobRunID      string `json:"source_job_run_id"`
	TaskRunID     string `json:"source_task_run_id"`
	SourceID      string `json:"source_id"`
	DestinationID string `json:"destination_id"`
}

func generateJobs(num int, params jobParams) []*jobsdb.JobT {
	var jobs []*jobsdb.JobT
	for i := 0; i < num; i++ {
		jobs = append(jobs, newJob(int64(i), params))
	}
	return jobs
}

func newJob(id int64, params jobParams) *jobsdb.JobT {
	p, err := json.Marshal(params)
	Expect(err).To(BeNil())
	return &jobsdb.JobT{
		JobID:      id,
		UUID:       uuid.Must(uuid.NewV4()),
		Parameters: p,
	}
}
func newSucceededStatus(jobId int64) *jobsdb.JobStatusT {
	return &jobsdb.JobStatusT{
		JobID:    jobId,
		JobState: jobsdb.Succeeded.State,
	}
}
func newFailedStatus(jobId int64) *jobsdb.JobStatusT {
	return &jobsdb.JobStatusT{
		JobID:    jobId,
		JobState: jobsdb.Failed.State,
	}
}
func newAbortedStatus(jobId int64) *jobsdb.JobStatusT {
	return &jobsdb.JobStatusT{
		JobID:    jobId,
		JobState: jobsdb.Aborted.State,
	}
}
func newJobStatus(jobId int64, state string) *jobsdb.JobStatusT {
	return &jobsdb.JobStatusT{
		JobID:    jobId,
		JobState: state,
	}
}
