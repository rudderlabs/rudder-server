package rsources

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

const errNotBeganProcessing = "cannot update job statuses without having previously called BeginProcessing"

type statsCollectorFixture struct {
	js                      *MockJobService
	statsCollector          StatsCollector
	droppedJobsCollector    FailedJobsStatsCollector
	sourceOnlyStatCollector FailedJobsStatsCollector
	statsStore              *memstats.Store
	syncSettings            SyncSettingDelegate
	statsTag                stats.Tags
	droppedStatsTag         stats.Tags
	srcOnlyStatsTag         stats.Tags
	params                  jobParams
	jobs                    []*jobsdb.JobT
	jobErrors               map[uuid.UUID]string
	jobStatuses             []*jobsdb.JobStatusT
}

func newStatsCollectorFixture(t *testing.T) *statsCollectorFixture {
	t.Helper()
	mockCtrl := gomock.NewController(t)
	statsStore, err := memstats.New()
	require.NoError(t, err)
	f := &statsCollectorFixture{
		js:              NewMockJobService(mockCtrl),
		statsStore:      statsStore,
		statsTag:        stats.Tags{"module": "stats"},
		droppedStatsTag: stats.Tags{"module": "dropped-jobs"},
		srcOnlyStatsTag: stats.Tags{"module": "src-only"},
		// A delegate that captures nothing: this suite is about the stats and the
		// record ids, not about the error text. The delegate seam itself is covered in
		// stats_collector_delegate_test.go.
		syncSettings: NewStaticSyncSettingDelegate("", nil),
		jobs:         []*jobsdb.JobT{},
		jobErrors:    map[uuid.UUID]string{},
		jobStatuses:  []*jobsdb.JobStatusT{},
	}
	f.statsCollector = NewStatsCollector(f.js, f.statsTag["module"], f.statsStore)
	f.droppedJobsCollector = NewDroppedJobsCollector(f.js, f.droppedStatsTag["module"], f.statsStore)
	f.sourceOnlyStatCollector = NewDroppedJobsCollector(f.js, f.srcOnlyStatsTag["module"], f.statsStore, IgnoreDestinationID())
	return f
}

func (f *statsCollectorFixture) targetKey() JobTargetKey {
	return JobTargetKey{
		TaskRunID:     f.params.TaskRunID,
		SourceID:      f.params.SourceID,
		DestinationID: f.params.DestinationID,
	}
}

func TestUsingStatsCollector(t *testing.T) {
	t.Run("when there are rudder-sources jobs", func(t *testing.T) {
		withRudderSourcesJobs := func(t *testing.T) *statsCollectorFixture {
			t.Helper()
			f := newStatsCollectorFixture(t)
			f.params = jobParams{
				JobRunID:      "jobRunId",
				TaskRunID:     "taskRunId",
				SourceID:      "sourceId",
				DestinationID: "destinationId",
				RecordID:      "recordId",
			}
			f.jobs = append(f.jobs, generateJobs(t, 10, f.params)...)
			return f
		}

		t.Run("all jobs are stored", func(t *testing.T) {
			withJobsStored := func(t *testing.T) *statsCollectorFixture {
				t.Helper()
				f := withRudderSourcesJobs(t)
				f.statsCollector.JobsStored(f.jobs)
				return f
			}

			t.Run("can publish without error all statuses as In stats", func(t *testing.T) {
				f := withJobsStored(t)
				f.js.EXPECT().
					IncrementStats(
						gomock.Any(),
						gomock.Any(),
						f.params.JobRunID,
						f.targetKey(),
						Stats{
							In: uint(len(f.jobs)),
						}).
					Times(1)

				err := f.statsCollector.Publish(t.Context(), nil)
				require.NoError(t, err)
				require.Len(t, f.statsStore.Get(rsourcesPublishTime, f.statsTag).Durations(), 1)
			})

			t.Run("underlying service returns an error", func(t *testing.T) {
				t.Run("fails during publish", func(t *testing.T) {
					f := withJobsStored(t)
					f.js.EXPECT().
						IncrementStats(
							gomock.Any(),
							gomock.Any(),
							f.params.JobRunID,
							f.targetKey(),
							Stats{
								In: uint(len(f.jobs)),
							}).
						Times(1).
						Return(fmt.Errorf("error"))

					err := f.statsCollector.Publish(t.Context(), nil)
					require.Error(t, err)
					require.Empty(t, f.statsStore.Get(rsourcesPublishTime, f.statsTag).Durations())
				})
			})
		})

		t.Run("half of the jobs have errors", func(t *testing.T) {
			t.Run("the rest of the jobs are stored", func(t *testing.T) {
				t.Run("can publish without error all successful jobs as In stats", func(t *testing.T) {
					f := withRudderSourcesJobs(t)
					for i, job := range f.jobs {
						if i < len(f.jobs)/2 {
							f.jobErrors[job.UUID] = "error"
						}
					}
					f.statsCollector.JobsStoredWithErrors(f.jobs, f.jobErrors)

					f.js.EXPECT().
						IncrementStats(
							gomock.Any(),
							gomock.Any(),
							f.params.JobRunID,
							f.targetKey(),
							Stats{
								In: uint(len(f.jobs) / 2),
							}).
						Times(1)

					err := f.statsCollector.Publish(t.Context(), nil)
					require.NoError(t, err)
					require.Len(t, f.statsStore.Get(rsourcesPublishTime, f.statsTag).Durations(), 1)
				})
			})
		})

		t.Run("jobs have no RecordId", func(t *testing.T) {
			t.Run("processing of jobs has started", func(t *testing.T) {
				t.Run("all jobs are aborted", func(t *testing.T) {
					t.Run("it calls CollectStats", func(t *testing.T) {
						t.Run("can publish without error all statuses but with updating all stats as Failed stats and without adding failed records", func(t *testing.T) {
							f := withRudderSourcesJobs(t)
							for i, job := range f.jobs {
								if i == 0 {
									// empty string
									params1 := f.params
									params1.RecordID = ""
									p, err := jsonrs.Marshal(params1)
									require.NoError(t, err)
									job.Parameters = p
								} else if i >= len(f.jobs)/2 {
									// nil value
									params1 := f.params
									params1.RecordID = nil
									p, err := jsonrs.Marshal(params1)
									require.NoError(t, err)
									job.Parameters = p
								} else {
									// non existent field
									params1 := jobParamsNoRecordId{
										JobRunID:      f.params.JobRunID,
										TaskRunID:     f.params.TaskRunID,
										SourceID:      f.params.SourceID,
										DestinationID: f.params.DestinationID,
									}
									p, err := jsonrs.Marshal(params1)
									require.NoError(t, err)
									job.Parameters = p
								}
							}
							f.statsCollector.BeginProcessing(f.jobs)
							for _, job := range f.jobs {
								f.jobStatuses = append(f.jobStatuses, newAbortedStatus(job.JobID))
							}
							f.statsCollector.CollectStats(f.jobStatuses)

							f.js.EXPECT().
								IncrementStats(
									gomock.Any(),
									gomock.Any(),
									f.params.JobRunID,
									f.targetKey(),
									Stats{
										Failed: uint(len(f.jobs)),
									}).
								Times(1)

							f.js.EXPECT().
								AddFailedRecords(
									gomock.Any(),
									gomock.Any(),
									f.params.JobRunID,
									f.targetKey(),
									gomock.Any()).
								Times(0)

							err := f.statsCollector.Publish(t.Context(), nil)
							require.NoError(t, err)
							require.Len(t, f.statsStore.Get(rsourcesPublishTime, f.statsTag).Durations(), 1)
						})
					})
				})
			})
		})

		t.Run("processing of jobs has started", func(t *testing.T) {
			withProcessingStarted := func(t *testing.T) *statsCollectorFixture {
				t.Helper()
				f := withRudderSourcesJobs(t)
				f.statsCollector.BeginProcessing(f.jobs)
				return f
			}

			t.Run("all jobs have succeeded", func(t *testing.T) {
				t.Run("it calls CollectStats", func(t *testing.T) {
					t.Run("can publish without error all statuses as Out stats", func(t *testing.T) {
						f := withProcessingStarted(t)
						for _, job := range f.jobs {
							f.jobStatuses = append(f.jobStatuses, newSucceededStatus(job.JobID))
						}
						f.statsCollector.CollectStats(f.jobStatuses)

						f.js.EXPECT().
							IncrementStats(
								gomock.Any(),
								gomock.Any(),
								f.params.JobRunID,
								f.targetKey(),
								Stats{
									Out: uint(len(f.jobs)),
								}).
							Times(1)
						err := f.statsCollector.Publish(t.Context(), nil)
						require.NoError(t, err)
						require.Len(t, f.statsStore.Get(rsourcesPublishTime, f.statsTag).Durations(), 1)
					})
				})
			})

			t.Run("all jobs have failed", func(t *testing.T) {
				t.Run("it calls CollectStats", func(t *testing.T) {
					t.Run("can publish without error all statuses but without actually updating stats", func(t *testing.T) {
						f := withProcessingStarted(t)
						for _, job := range f.jobs {
							f.jobStatuses = append(f.jobStatuses, newFailedStatus(job.JobID))
						}
						f.statsCollector.CollectStats(f.jobStatuses)

						err := f.statsCollector.Publish(t.Context(), nil)
						require.NoError(t, err)
						require.Len(t, f.statsStore.Get(rsourcesPublishTime, f.statsTag).Durations(), 1)
					})
				})
			})

			t.Run("half jobs have failed and half jobs are aborted", func(t *testing.T) {
				t.Run("it calls CollectStats and CollectFailedRecords", func(t *testing.T) {
					t.Run("can publish without error all statuses but with updating half stats as Failed stats and adding failed records", func(t *testing.T) {
						f := withProcessingStarted(t)
						for i, job := range f.jobs {
							if i >= len(f.jobs)/2 {
								f.jobStatuses = append(f.jobStatuses, newFailedStatus(job.JobID))
							} else {
								f.jobStatuses = append(f.jobStatuses, newAbortedStatus(job.JobID))
							}
						}
						f.statsCollector.CollectStats(f.jobStatuses)
						require.NoError(t, f.statsCollector.CollectFailedRecords(t.Context(), f.syncSettings, f.jobStatuses))

						f.js.EXPECT().
							IncrementStats(
								gomock.Any(),
								gomock.Any(),
								f.params.JobRunID,
								f.targetKey(),
								Stats{
									Failed: uint(len(f.jobs) / 2),
								}).
							Times(1)

						failedRecords := []FailedRecord{}
						for i := 0; i < len(f.jobs)/2; i++ {
							failedRecords = append(failedRecords, FailedRecord{Record: []byte(`"recordId"`)})
						}
						f.js.EXPECT().
							AddFailedRecords(
								gomock.Any(),
								gomock.Any(),
								f.params.JobRunID,
								f.targetKey(),
								failedRecords).
							Times(1)

						err := f.statsCollector.Publish(t.Context(), nil)
						require.NoError(t, err)
						require.Len(t, f.statsStore.Get(rsourcesPublishTime, f.statsTag).Durations(), 1)
					})
				})
			})
		})

		t.Run("processing of jobs has not started", func(t *testing.T) {
			t.Run("should not allow for calling CollectStats", func(t *testing.T) {
				f := withRudderSourcesJobs(t)
				require.PanicsWithError(t, errNotBeganProcessing, func() {
					f.statsCollector.CollectStats(f.jobStatuses)
				})
			})
		})

		t.Run("it calls statsCollector.JobsForked", func(t *testing.T) {
			t.Run("jobs carry multiple consumers", func(t *testing.T) {
				withMultipleConsumers := func(t *testing.T) *statsCollectorFixture {
					t.Helper()
					f := withRudderSourcesJobs(t)
					for _, job := range f.jobs {
						job.Consumers = []string{"destA", "destB", "destC"}
					}
					f.statsCollector.JobsForked(f.jobs)
					return f
				}

				t.Run("increments In by the number of consumers per job", func(t *testing.T) {
					f := withMultipleConsumers(t)
					f.js.EXPECT().
						IncrementStats(
							gomock.Any(),
							gomock.Any(),
							f.params.JobRunID,
							f.targetKey(),
							Stats{
								In: uint(len(f.jobs) * 3),
							}).
						Times(1)

					err := f.statsCollector.Publish(t.Context(), nil)
					require.NoError(t, err)
					require.Len(t, f.statsStore.Get(rsourcesPublishTime, f.statsTag).Durations(), 1)
				})

				t.Run("does not index jobs for a later CollectStats call", func(t *testing.T) {
					f := withMultipleConsumers(t)
					for _, job := range f.jobs {
						f.jobStatuses = append(f.jobStatuses, newSucceededStatus(job.JobID))
					}
					// BeginProcessing was never called, so CollectStats must panic.
					require.PanicsWithError(t, errNotBeganProcessing, func() {
						f.statsCollector.CollectStats(f.jobStatuses)
					})
				})
			})

			t.Run("jobs carry no consumers", func(t *testing.T) {
				t.Run("increments In by 1 per job", func(t *testing.T) {
					f := withRudderSourcesJobs(t)
					f.statsCollector.JobsForked(f.jobs)

					f.js.EXPECT().
						IncrementStats(
							gomock.Any(),
							gomock.Any(),
							f.params.JobRunID,
							f.targetKey(),
							Stats{
								In: uint(len(f.jobs)),
							}).
						Times(1)

					err := f.statsCollector.Publish(t.Context(), nil)
					require.NoError(t, err)
					require.Len(t, f.statsStore.Get(rsourcesPublishTime, f.statsTag).Durations(), 1)
				})
			})

			t.Run("combined with a subsequent JobsStored call for the same key", func(t *testing.T) {
				t.Run("sums both In contributions", func(t *testing.T) {
					f := withRudderSourcesJobs(t)
					for _, job := range f.jobs {
						job.Consumers = []string{"destA", "destB"}
					}
					f.statsCollector.JobsForked(f.jobs)
					f.statsCollector.JobsStored(f.jobs)

					f.js.EXPECT().
						IncrementStats(
							gomock.Any(),
							gomock.Any(),
							f.params.JobRunID,
							f.targetKey(),
							Stats{
								In: uint(len(f.jobs)*2 + len(f.jobs)),
							}).
						Times(1)

					err := f.statsCollector.Publish(t.Context(), nil)
					require.NoError(t, err)
					require.Len(t, f.statsStore.Get(rsourcesPublishTime, f.statsTag).Durations(), 1)
				})
			})
		})

		t.Run("it calls failedRecordsCollector.JobsDropped", func(t *testing.T) {
			t.Run("publishes both in and out stats and doesn't add failed records", func(t *testing.T) {
				f := withRudderSourcesJobs(t)
				f.droppedJobsCollector.JobsDropped(f.jobs)

				f.js.EXPECT().
					IncrementStats(
						gomock.Any(),
						gomock.Any(),
						f.params.JobRunID,
						f.targetKey(),
						Stats{
							In:     uint(len(f.jobs)),
							Failed: uint(len(f.jobs)),
						}).
					Times(1)

				err := f.droppedJobsCollector.Publish(t.Context(), nil)
				require.NoError(t, err)
				require.Len(t, f.statsStore.Get(rsourcesPublishTime, f.droppedStatsTag).Durations(), 1)
			})
		})

		t.Run("it calls sourceOnlyStatCollector.JobsDroppedWith when only source information is of use", func(t *testing.T) {
			t.Run("publishes stats with only source information and no failed records are captured", func(t *testing.T) {
				f := withRudderSourcesJobs(t)
				f.sourceOnlyStatCollector.JobsDropped(f.jobs)

				f.js.EXPECT().
					IncrementStats(
						gomock.Any(),
						gomock.Any(),
						f.params.JobRunID,
						JobTargetKey{
							TaskRunID: f.params.TaskRunID,
							SourceID:  f.params.SourceID,
						},
						Stats{
							In:     uint(len(f.jobs)),
							Failed: uint(len(f.jobs)),
						}).
					Times(1)

				err := f.sourceOnlyStatCollector.Publish(t.Context(), nil)
				require.NoError(t, err)
				require.Len(t, f.statsStore.Get(rsourcesPublishTime, f.srcOnlyStatsTag).Durations(), 1)
			})
		})
	})

	t.Run("when there are no rudder-sources jobs", func(t *testing.T) {
		withNonRudderSourcesJobs := func(t *testing.T) *statsCollectorFixture {
			t.Helper()
			f := newStatsCollectorFixture(t)
			f.params = jobParams{
				SourceID:      "sourceId",
				DestinationID: "destinationId",
			}
			f.jobs = append(f.jobs, generateJobs(t, 10, f.params)...)
			return f
		}

		t.Run("all jobs are stored", func(t *testing.T) {
			t.Run("doesn't publish any jobs", func(t *testing.T) {
				f := withNonRudderSourcesJobs(t)
				f.statsCollector.JobsStored(f.jobs)

				// no js.EXPECT
				err := f.statsCollector.Publish(t.Context(), nil)
				require.NoError(t, err)
				require.Len(t, f.statsStore.Get(rsourcesPublishTime, f.statsTag).Durations(), 1)
			})
		})

		t.Run("all jobs are forked", func(t *testing.T) {
			t.Run("doesn't publish any jobs", func(t *testing.T) {
				f := withNonRudderSourcesJobs(t)
				for _, job := range f.jobs {
					job.Consumers = []string{"destA", "destB"}
				}
				f.statsCollector.JobsForked(f.jobs)

				// no js.EXPECT
				err := f.statsCollector.Publish(t.Context(), nil)
				require.NoError(t, err)
				require.Len(t, f.statsStore.Get(rsourcesPublishTime, f.statsTag).Durations(), 1)
			})
		})

		t.Run("processing of jobs has started", func(t *testing.T) {
			t.Run("all jobs have succeeded", func(t *testing.T) {
				t.Run("it calls CollectStats", func(t *testing.T) {
					t.Run("doesn't publish any jobs", func(t *testing.T) {
						f := withNonRudderSourcesJobs(t)
						f.statsCollector.BeginProcessing(f.jobs)
						for _, job := range f.jobs {
							f.jobStatuses = append(f.jobStatuses, newSucceededStatus(job.JobID))
						}
						f.statsCollector.CollectStats(f.jobStatuses)

						// no js.EXPECT
						err := f.statsCollector.Publish(t.Context(), nil)
						require.NoError(t, err)
						require.Len(t, f.statsStore.Get(rsourcesPublishTime, f.statsTag).Durations(), 1)
					})
				})
			})
		})
	})
}

type jobParams struct {
	JobRunID      string `json:"source_job_run_id"`
	TaskRunID     string `json:"source_task_run_id"`
	SourceID      string `json:"source_id"`
	DestinationID string `json:"destination_id"`
	RecordID      any    `json:"record_id"`
}

type jobParamsNoRecordId struct {
	JobRunID      string `json:"source_job_run_id"`
	TaskRunID     string `json:"source_task_run_id"`
	SourceID      string `json:"source_id"`
	DestinationID string `json:"destination_id"`
}

func generateJobs(t testing.TB, num int, params jobParams) []*jobsdb.JobT { // skipcq: CRT-P0003
	t.Helper()
	var jobs []*jobsdb.JobT
	for i := range num {
		jobs = append(jobs, newJob(t, int64(i), params))
	}
	return jobs
}

func newJob(t testing.TB, id int64, params jobParams) *jobsdb.JobT { // skipcq: CRT-P0003
	t.Helper()
	p, err := jsonrs.Marshal(params)
	require.NoError(t, err)
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
				switch key.Str {
				case "source_task_run_id":
					tk.TaskRunID = value.Str
					found++
				case "source_id":
					tk.SourceID = value.Str
					found++
				case "destination_id":
					tk.DestinationID = value.Str
					found++
				}
				return found != 3
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

	b.Run("parse params using jsonrs.Unmarshall", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var p JobTargetKey
			err := jsonrs.Unmarshal(jsonStr, &p)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
