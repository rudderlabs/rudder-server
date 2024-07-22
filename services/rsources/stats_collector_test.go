package rsources

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

var _ = Describe("Using StatsCollector", Serial, func() {
	var (
		jobs                    []*jobsdb.JobT
		jobErrors               map[uuid.UUID]string
		jobStatuses             []*jobsdb.JobStatusT
		mockCtrl                *gomock.Controller
		js                      *MockJobService
		statsCollector          StatsCollector
		droppedJobsCollector    FailedJobsStatsCollector
		sourceOnlyStatCollector FailedJobsStatsCollector
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		js = NewMockJobService(mockCtrl)
		statsCollector = NewStatsCollector(js)
		droppedJobsCollector = NewDroppedJobsCollector(js)
		sourceOnlyStatCollector = NewDroppedJobsCollector(js, IgnoreDestinationID())
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
				RecordID:      "recordId",
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

		Context("jobs have no RecordId", func() {
			BeforeEach(func() {
				for i, job := range jobs {
					if i == 0 {
						// empty string
						params1 := params
						params1.RecordID = ""
						p, err := json.Marshal(params1)
						Expect(err).To(BeNil())
						job.Parameters = p
					} else if i >= len(jobs)/2 {
						// nil value
						params1 := params
						params1.RecordID = nil
						p, err := json.Marshal(params1)
						Expect(err).To(BeNil())
						job.Parameters = p
					} else {
						// non existent field
						params1 := jobParamsNoRecordId{
							JobRunID:      params.JobRunID,
							TaskRunID:     params.TaskRunID,
							SourceID:      params.SourceID,
							DestinationID: params.DestinationID,
						}
						p, err := json.Marshal(params1)
						Expect(err).To(BeNil())
						job.Parameters = p
					}
				}
			})
			Context("processing of jobs has started", func() {
				BeforeEach(func() {
					statsCollector.BeginProcessing(jobs)
				})

				Context("all jobs are aborted", func() {
					BeforeEach(func() {
						for _, job := range jobs {
							jobStatuses = append(jobStatuses, newAbortedStatus(job.JobID))
						}
					})
					Context("it calls CollectStats", func() {
						BeforeEach(func() {
							statsCollector.CollectStats(jobStatuses)
						})

						It("can publish without error all statuses but with updating all stats as Failed stats and without adding failed records", func() {
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
										Failed: uint(len(jobs)),
									}).
								Times(1)

							js.EXPECT().
								AddFailedRecords(
									gomock.Any(),
									gomock.Any(),
									params.JobRunID,
									JobTargetKey{
										TaskRunID:     params.TaskRunID,
										SourceID:      params.SourceID,
										DestinationID: params.DestinationID,
									},
									gomock.Any()).
								Times(0)

							err := statsCollector.Publish(context.TODO(), nil)
							Expect(err).To(BeNil())
						})
					})
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
				Context("it calls CollectStats", func() {
					BeforeEach(func() {
						statsCollector.CollectStats(jobStatuses)
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
				Context("it calls CollectStats", func() {
					BeforeEach(func() {
						statsCollector.CollectStats(jobStatuses)
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
				Context("it calls CollectStats and CollectFailedRecords", func() {
					BeforeEach(func() {
						statsCollector.CollectStats(jobStatuses)
						statsCollector.CollectFailedRecords(jobStatuses)
					})

					It("can publish without error all statuses but with updating half stats as Failed stats and adding failed records", func() {
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

						failedRecords := []FailedRecord{}
						for i := 0; i < len(jobs)/2; i++ {
							failedRecords = append(failedRecords, FailedRecord{Record: []byte(`"recordId"`)})
						}
						js.EXPECT().
							AddFailedRecords(
								gomock.Any(),
								gomock.Any(),
								params.JobRunID,
								JobTargetKey{
									TaskRunID:     params.TaskRunID,
									SourceID:      params.SourceID,
									DestinationID: params.DestinationID,
								},
								failedRecords).
							Times(1)

						err := statsCollector.Publish(context.TODO(), nil)
						Expect(err).To(BeNil())
					})
				})
			})
		})

		Context("processing of jobs has not started", func() {
			It("should not allow for calling CollectStats", func() {
				var err error
				defer func() {
					err = recover().(error)
					Expect(err).ToNot(BeNil())
				}()
				statsCollector.CollectStats(jobStatuses)
				Expect(err).ToNot(BeNil())
			})
		})

		Context("it calls failedRecordsCollector.JobsDropped", func() {
			BeforeEach(func() {
				droppedJobsCollector.JobsDropped(jobs)
			})

			It("publishes both in and out stats and doesn't add failed records", func() {
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
							In:     uint(len(jobs)),
							Failed: uint(len(jobs)),
						}).
					Times(1)

				err := droppedJobsCollector.Publish(context.TODO(), nil)
				Expect(err).To(BeNil())
			})
		})

		Context("it calls sourceOnlyStatCollector.JobsDroppedWith when only source information is of use", func() {
			BeforeEach(func() {
				sourceOnlyStatCollector.JobsDropped(jobs)
			})

			It("publishes stats with only source information and no failed records are captured", func() {
				js.EXPECT().
					IncrementStats(
						gomock.Any(),
						gomock.Any(),
						params.JobRunID,
						JobTargetKey{
							TaskRunID: params.TaskRunID,
							SourceID:  params.SourceID,
						},
						Stats{
							In:     uint(len(jobs)),
							Failed: uint(len(jobs)),
						}).
					Times(1)

				err := sourceOnlyStatCollector.Publish(context.TODO(), nil)
				Expect(err).To(BeNil())
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
				Context("it calls CollectStats", func() {
					BeforeEach(func() {
						statsCollector.CollectStats(jobStatuses)
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
	JobRunID      string      `json:"source_job_run_id"`
	TaskRunID     string      `json:"source_task_run_id"`
	SourceID      string      `json:"source_id"`
	DestinationID string      `json:"destination_id"`
	RecordID      interface{} `json:"record_id"`
}

type jobParamsNoRecordId struct {
	JobRunID      string `json:"source_job_run_id"`
	TaskRunID     string `json:"source_task_run_id"`
	SourceID      string `json:"source_id"`
	DestinationID string `json:"destination_id"`
}

func generateJobs(num int, params jobParams) []*jobsdb.JobT { // skipcq: CRT-P0003
	var jobs []*jobsdb.JobT
	for i := 0; i < num; i++ {
		jobs = append(jobs, newJob(int64(i), params))
	}
	return jobs
}

func newJob(id int64, params jobParams) *jobsdb.JobT { // skipcq: CRT-P0003
	p, err := json.Marshal(params)
	Expect(err).To(BeNil())
	return &jobsdb.JobT{
		JobID:      id,
		UUID:       uuid.New(),
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

func BenchmarkParamsParsing(b *testing.B) {
	jsonStr := []byte(`{
		"prop1": "prop1",
		"prop2": "prop2",
		"prop3": "prop3",
		"prop4": "prop4","source_job_run_id": "source_job_run_id",
		"source_task_run_id": "source_task_run_id",
		"source_id": "source_id",
		"destination_id": "destination_id",
		"prop5": "prop5",
		"prop6": "prop6"
	}`)

	b.Run("parse params using gjson.ForEach", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r := gjson.ParseBytes(jsonStr)
			var tk JobTargetKey
			found := 0
			r.ForEach(func(key, value gjson.Result) bool {
				if key.Str == "source_task_run_id" {
					tk.TaskRunID = value.Str
					found++
				} else if key.Str == "source_id" {
					tk.SourceID = value.Str
					found++
				} else if key.Str == "destination_id" {
					tk.DestinationID = value.Str
					found++
				}
				if found == 3 {
					return false
				}
				return true
			})
		}
	})

	b.Run("parse params using gjson.GetBytes 3 times", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = JobTargetKey{
				TaskRunID:     gjson.GetBytes(jsonStr, "source_task_run_id").Str,
				SourceID:      gjson.GetBytes(jsonStr, "source_id").Str,
				DestinationID: gjson.GetBytes(jsonStr, "destination_id").Str,
			}
		}
	})

	b.Run("parse params using jsoniter.Unmarshall", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var p JobTargetKey
			err := jsoniter.Unmarshal(jsonStr, &p)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
