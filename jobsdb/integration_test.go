package jobsdb

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-server/jobsdb/prebackup"
	fileuploader "github.com/rudderlabs/rudder-server/services/fileuploader"
	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
)

const (
	defaultWorkspaceID = "workspaceId"
)

func genJobs(workspaceId, customVal string, jobCount, eventsPerJob int) []*JobT { // nolint: unparam
	js := make([]*JobT, jobCount)
	for i := range js {
		js[i] = &JobT{
			JobID:        int64(i) + 1,
			Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
			EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
			UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
			UUID:         uuid.New(),
			CustomVal:    customVal,
			EventCount:   eventsPerJob,
			WorkspaceId:  workspaceId,
		}
	}
	return js
}

func genJobStatuses(jobs []*JobT, state string) []*JobStatusT {
	statuses := make([]*JobStatusT, 0, len(jobs))
	for i := range jobs {
		job := jobs[i]
		statuses = append(statuses, &JobStatusT{
			JobID:         job.JobID,
			JobState:      state,
			AttemptNum:    1,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     "999",
			ErrorResponse: []byte(`\u0000{"status": "status"}`),
			Parameters:    []byte(``),
			WorkspaceId:   job.WorkspaceId,
		})
	}
	return statuses
}

func TestJobsDB(t *testing.T) {
	triggerAddNewDS := make(chan time.Time)
	c := config.New()
	_ = startPostgres(t, c)
	c.Set("jobsdb.maxDSSize", 10)
	jobDB := Handle{
		TriggerAddNewDS: func() <-chan time.Time {
			return triggerAddNewDS
		},
	}
	err := jobDB.Setup(c, ReadWrite, false, strings.ToLower(rand.String(5)), []prebackup.Handler{}, fileuploader.NewDefaultProvider())
	require.NoError(t, err)
	defer jobDB.TearDown()

	customVal := "MOCKDS"
	sampleTestJob := JobT{
		Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
		EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
		UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
		UUID:         uuid.New(),
		CustomVal:    customVal,
	}

	unprocessedJobEmpty, err := jobDB.GetUnprocessed(context.Background(), GetQueryParams{
		CustomValFilters: []string{customVal},
		JobsLimit:        1,
		ParameterFilters: []ParameterFilterT{},
	})
	require.NoError(t, err, "GetUnprocessed failed")
	unprocessedListEmpty := unprocessedJobEmpty.Jobs
	require.Equal(t, 0, len(unprocessedListEmpty))
	err = jobDB.Store(context.Background(), []*JobT{&sampleTestJob})
	require.NoError(t, err)

	unprocessedJob, err := jobDB.GetUnprocessed(context.Background(), GetQueryParams{
		CustomValFilters: []string{customVal},
		JobsLimit:        1,
		ParameterFilters: []ParameterFilterT{},
	})
	require.NoError(t, err, "GetUnprocessed failed")
	unprocessedList := unprocessedJob.Jobs
	require.Equal(t, 1, len(unprocessedList))

	status := JobStatusT{
		JobID:         unprocessedList[0].JobID,
		JobState:      "succeeded",
		AttemptNum:    1,
		ExecTime:      time.Now(),
		RetryTime:     time.Now(),
		ErrorCode:     "202",
		ErrorResponse: []byte(`{"success":"OK"}`),
		Parameters:    []byte(`{}`),
		WorkspaceId:   defaultWorkspaceID,
	}

	err = jobDB.UpdateJobStatus(context.Background(), []*JobStatusT{&status}, []string{customVal}, []ParameterFilterT{})
	require.NoError(t, err)

	uj, err := jobDB.GetUnprocessed(context.Background(), GetQueryParams{
		CustomValFilters: []string{customVal},
		JobsLimit:        1,
		ParameterFilters: []ParameterFilterT{},
	})
	require.NoError(t, err, "GetUnprocessed failed")
	unprocessedList = uj.Jobs
	require.Equal(t, 0, len(unprocessedList))

	t.Run("multi events per job", func(t *testing.T) {
		jobCountPerDS := 12
		eventsPerJob := 60

		dsCount := 3
		jobCount := dsCount * jobCountPerDS

		t.Logf("spread %d jobs into %d data sets", jobCount, dsCount)
		for i := 0; i < dsCount; i++ {
			require.NoError(t, jobDB.Store(context.Background(), genJobs(defaultWorkspaceID, customVal, jobCountPerDS, eventsPerJob)))
			triggerAddNewDS <- time.Now()
			triggerAddNewDS <- time.Now() // Second time, waits for the first loop to finish
		}

		t.Log("GetUnprocessed with job count limit")
		JobLimitJob, err := jobDB.GetUnprocessed(context.Background(), GetQueryParams{
			CustomValFilters: []string{customVal},
			JobsLimit:        100,
			ParameterFilters: []ParameterFilterT{},
		})
		require.NoError(t, err, "GetUnprocessed failed")
		JobLimitList := JobLimitJob.Jobs
		require.Equal(t, jobCount, len(JobLimitList))

		t.Log("GetUnprocessed with event count limit")
		eventLimitJob, err := jobDB.GetUnprocessed(context.Background(), GetQueryParams{
			CustomValFilters: []string{customVal},
			JobsLimit:        100,
			EventsLimit:      eventsPerJob * 20,
			ParameterFilters: []ParameterFilterT{},
		})
		require.NoError(t, err, "GetUnprocessed failed")
		eventLimitList := eventLimitJob.Jobs
		require.Equal(t, 20, len(eventLimitList))
		t.Log("GetUnprocessed jobs should have the expected event count")
		for _, j := range eventLimitList {
			require.Equal(t, eventsPerJob, j.EventCount)
		}

		t.Log("Repeat read")
		eventLimitListRepeat, err := jobDB.GetUnprocessed(context.Background(), GetQueryParams{
			CustomValFilters: []string{customVal},
			JobsLimit:        100,
			EventsLimit:      eventsPerJob * 20,
			ParameterFilters: []ParameterFilterT{},
		})
		require.NoError(t, err, "GetUnprocessed failed")
		require.Equal(t, 20, len(eventLimitListRepeat.Jobs))
		require.Equal(t, eventLimitList, eventLimitListRepeat.Jobs)

		statuses := make([]*JobStatusT, len(JobLimitList))

		n := time.Now().Add(time.Hour * -1)
		for i := range statuses {
			statuses[i] = &JobStatusT{
				JobID:         JobLimitList[i].JobID,
				JobState:      Failed.State,
				AttemptNum:    1,
				ExecTime:      n,
				RetryTime:     n,
				ErrorResponse: []byte(`{"success":"OK"}`),
				Parameters:    []byte(`{}`),
				WorkspaceId:   defaultWorkspaceID,
			}
		}
		t.Log("Mark some jobs as failed")
		err = jobDB.UpdateJobStatus(context.Background(), statuses, []string{customVal}, []ParameterFilterT{})
		require.NoError(t, err)

		t.Log("GetUnprocessed with job count limit")
		retryJobLimitList, err := jobDB.GetFailed(context.Background(), GetQueryParams{
			CustomValFilters: []string{customVal},
			JobsLimit:        100,
		})
		require.NoError(t, err, "GetToRetry failed")
		require.Equal(t, jobCount, len(retryJobLimitList.Jobs))

		t.Log("GetToRetry with event count limit")
		retryEventLimitList, err := jobDB.GetFailed(context.Background(), GetQueryParams{
			CustomValFilters: []string{customVal},
			JobsLimit:        100,
			EventsLimit:      eventsPerJob * 20,
		})
		require.NoError(t, err, "GetToRetry failed")
		require.Equal(t, 20, len(retryEventLimitList.Jobs))
		t.Log("GetToRetry jobs should have the expected event count")
		for _, j := range eventLimitList {
			require.Equal(t, eventsPerJob, j.EventCount)
		}
	})

	t.Run("DSoverflow", func(t *testing.T) {
		customVal := "MOCKDS"

		triggerAddNewDS := make(chan time.Time)

		c.Set("jobsdb.maxDSSize", 9)
		jobDB := Handle{
			TriggerAddNewDS: func() <-chan time.Time {
				return triggerAddNewDS
			},
			config: c,
		}

		err := jobDB.Setup(c, ReadWrite, true, strings.ToLower(rand.String(5)), []prebackup.Handler{}, fileuploader.NewDefaultProvider())
		require.NoError(t, err)
		defer jobDB.TearDown()

		jobCountPerDS := 10
		eventsPerJobDS1 := 60
		eventsPerJobDS2 := 20

		t.Log("First jobs table with jobs of 60 events, second with jobs of 20 events")
		require.NoError(t, jobDB.Store(context.Background(), genJobs(defaultWorkspaceID, customVal, jobCountPerDS, eventsPerJobDS1)))
		triggerAddNewDS <- time.Now()
		triggerAddNewDS <- time.Now() // Second time, waits for the first loop to finish

		require.NoError(t, jobDB.Store(context.Background(), genJobs(defaultWorkspaceID, customVal, jobCountPerDS, eventsPerJobDS2)))
		triggerAddNewDS <- time.Now()
		triggerAddNewDS <- time.Now() // Second time, waits for the first loop to finish

		t.Log("GetUnprocessed with event count limit")
		t.Log("Using event count that will cause spill-over, not exact for ds1, but remainder suitable for ds2")
		trickyEventCount := (eventsPerJobDS1 * (jobCountPerDS - 1)) + eventsPerJobDS2

		eventLimitList, err := jobDB.GetUnprocessed(context.Background(), GetQueryParams{
			CustomValFilters: []string{customVal},
			JobsLimit:        100,
			EventsLimit:      trickyEventCount,
			ParameterFilters: []ParameterFilterT{},
		})
		require.NoError(t, err, "GetUnprocessed failed")
		requireSequential(t, eventLimitList.Jobs)
		require.Equal(t, jobCountPerDS-1, len(eventLimitList.Jobs))

		t.Log("Prepare GetToRetry")
		{
			allJobs, err := jobDB.GetUnprocessed(context.Background(), GetQueryParams{
				CustomValFilters: []string{customVal},
				JobsLimit:        1000,
				ParameterFilters: []ParameterFilterT{},
			})
			require.NoError(t, err, "GetUnprocessed failed")

			statuses := make([]*JobStatusT, len(allJobs.Jobs))
			n := time.Now().Add(time.Hour * -1)
			for i := range statuses {
				statuses[i] = &JobStatusT{
					JobID:         allJobs.Jobs[i].JobID,
					JobState:      Failed.State,
					AttemptNum:    1,
					ExecTime:      n,
					RetryTime:     n,
					ErrorResponse: []byte(`{"success":"OK"}`),
					Parameters:    []byte(`{}`),
					WorkspaceId:   defaultWorkspaceID,
				}
			}
			t.Log("Mark all jobs as failed")
			err = jobDB.UpdateJobStatus(context.Background(), statuses, []string{customVal}, []ParameterFilterT{})
			require.NoError(t, err)
		}

		t.Log("Test spill over with GetToRetry")
		{
			eventLimitList, err := jobDB.GetFailed(context.Background(), GetQueryParams{
				CustomValFilters: []string{customVal},
				JobsLimit:        100,
				EventsLimit:      trickyEventCount,
				ParameterFilters: []ParameterFilterT{},
			})
			require.NoError(t, err, "GetToRetry failed")
			requireSequential(t, eventLimitList.Jobs)
			require.Equal(t, jobCountPerDS-1, len(eventLimitList.Jobs))
		}
	})

	t.Run("limit by total payload size", func(t *testing.T) {
		customVal := "MOCKDS"

		triggerAddNewDS := make(chan time.Time)

		c.Set("jobsdb.maxDSSize", 2)
		jobDB := Handle{
			TriggerAddNewDS: func() <-chan time.Time {
				return triggerAddNewDS
			},
			config: c,
		}

		err := jobDB.Setup(c, ReadWrite, true, strings.ToLower(rand.String(5)), []prebackup.Handler{}, fileuploader.NewDefaultProvider())
		require.NoError(t, err)
		defer jobDB.TearDown()

		jobs := genJobs(defaultWorkspaceID, customVal, 2, 1)
		require.NoError(t, jobDB.Store(context.Background(), jobs))
		payloadSize, err := getPayloadSize(t, &jobDB, jobs[0])
		require.NoError(t, err)
		triggerAddNewDS <- time.Now()
		triggerAddNewDS <- time.Now() // Second time, waits for the first loop to finish

		require.NoError(t, jobDB.Store(context.Background(), genJobs(defaultWorkspaceID, customVal, 2, 1)))
		triggerAddNewDS <- time.Now()
		triggerAddNewDS <- time.Now() // Second time, waits for the first loop to finish

		payloadLimit := 3 * payloadSize
		payloadLimitList, err := jobDB.GetUnprocessed(context.Background(), GetQueryParams{
			CustomValFilters: []string{customVal},
			JobsLimit:        100,
			PayloadSizeLimit: payloadLimit,
			ParameterFilters: []ParameterFilterT{},
		})
		require.NoError(t, err, "GetUnprocessed failed")

		requireSequential(t, payloadLimitList.Jobs)
		require.Equal(t, 3, len(payloadLimitList.Jobs))
	})

	t.Run("querying with an payload size limit should return at least one job even if limit is exceeded", func(t *testing.T) {
		customVal := "MOCKDS"
		triggerAddNewDS := make(chan time.Time)

		c.Set("jobsdb.maxDSSize", 1)
		jobDB := Handle{
			TriggerAddNewDS: func() <-chan time.Time {
				return triggerAddNewDS
			},
			config: c,
		}
		err := jobDB.Setup(c, ReadWrite, false, strings.ToLower(rand.String(5)), []prebackup.Handler{}, fileuploader.NewDefaultProvider())
		require.NoError(t, err)
		defer jobDB.TearDown()

		jobs := genJobs(defaultWorkspaceID, customVal, 2, 1)
		require.NoError(t, jobDB.Store(context.Background(), jobs))
		payloadSize, err := getPayloadSize(t, &jobDB, jobs[0])
		require.NoError(t, err)

		payloadLimit := payloadSize / 2
		payloadLimitList, err := jobDB.GetUnprocessed(context.Background(), GetQueryParams{
			CustomValFilters: []string{customVal},
			JobsLimit:        100,
			PayloadSizeLimit: payloadLimit,
			ParameterFilters: []ParameterFilterT{},
		})
		require.NoError(t, err, "GetUnprocessed failed")

		requireSequential(t, payloadLimitList.Jobs)
		require.Equal(t, 1, len(payloadLimitList.Jobs))
	})

	t.Run("querying with an event count limit should return at least one job even if limit is exceeded", func(t *testing.T) {
		customVal := "MOCKDS"
		triggerAddNewDS := make(chan time.Time)

		c.Set("jobsdb.maxDSSize", 1)
		jobDB := Handle{
			config: c,
			TriggerAddNewDS: func() <-chan time.Time {
				return triggerAddNewDS
			},
		}
		err := jobDB.Setup(c, ReadWrite, true, strings.ToLower(rand.String(5)), []prebackup.Handler{}, fileuploader.NewDefaultProvider())
		require.NoError(t, err)
		defer jobDB.TearDown()

		jobs := genJobs(defaultWorkspaceID, customVal, 2, 4)
		require.NoError(t, jobDB.Store(context.Background(), jobs))

		eventCountLimit := 1
		eventLimitList, err := jobDB.GetUnprocessed(context.Background(), GetQueryParams{
			CustomValFilters: []string{customVal},
			JobsLimit:        100,
			EventsLimit:      eventCountLimit,
			ParameterFilters: []ParameterFilterT{},
		})
		require.NoError(t, err, "GetUnprocessed failed")

		requireSequential(t, eventLimitList.Jobs)
		require.Equal(t, 1, len(eventLimitList.Jobs))
	})

	t.Run("should stay within event count limits", func(t *testing.T) {
		customVal := "MOCKDS"
		triggerAddNewDS := make(chan time.Time)

		c.Set("jobsdb.maxDSSize", 4)
		defer c.Set("jobsdb.maxDSSize", 100000)
		jobDB := Handle{
			config: c,
			TriggerAddNewDS: func() <-chan time.Time {
				return triggerAddNewDS
			},
		}

		err := jobDB.Setup(c, ReadWrite, true, strings.ToLower(rand.String(5)), []prebackup.Handler{}, fileuploader.NewDefaultProvider())
		require.NoError(t, err)
		defer jobDB.TearDown()

		var jobs []*JobT
		jobs = append(jobs, genJobs(defaultWorkspaceID, customVal, 1, 1)...)
		jobs = append(jobs, genJobs(defaultWorkspaceID, customVal, 1, 2)...)
		jobs = append(jobs, genJobs(defaultWorkspaceID, customVal, 1, 3)...)
		jobs = append(jobs, genJobs(defaultWorkspaceID, customVal, 1, 10)...)
		require.NoError(t, jobDB.Store(context.Background(), jobs))
		triggerAddNewDS <- time.Now()
		triggerAddNewDS <- time.Now() // Second time, waits for the first loop to finish

		require.NoError(t, jobDB.Store(context.Background(), jobs))
		triggerAddNewDS <- time.Now()
		triggerAddNewDS <- time.Now() // Second time, waits for the first loop to finish

		eventCountLimit := 10
		eventLimitList, err := jobDB.GetUnprocessed(context.Background(), GetQueryParams{
			CustomValFilters: []string{customVal},
			JobsLimit:        100,
			EventsLimit:      eventCountLimit,
			ParameterFilters: []ParameterFilterT{},
		})
		require.NoError(t, err, "GetUnprocessed failed")

		requireSequential(t, eventLimitList.Jobs)
		require.Equal(t, 3, len(eventLimitList.Jobs))
	})

	t.Run("should create a new dataset after maxDSRetentionPeriod", func(t *testing.T) {
		customVal := "MOCKDS"
		triggerAddNewDS := make(chan time.Time)

		jobDB := Handle{
			TriggerAddNewDS: func() <-chan time.Time {
				return triggerAddNewDS
			},
		}

		tablePrefix := strings.ToLower(rand.String(5))
		c.Set(fmt.Sprintf("JobsDB.%s.maxDSRetention", tablePrefix), "1s")
		err := jobDB.Setup(c, ReadWrite, true, tablePrefix, []prebackup.Handler{}, fileuploader.NewDefaultProvider())
		require.NoError(t, err)
		defer jobDB.TearDown()

		jobs := genJobs(defaultWorkspaceID, customVal, 1, 1)
		require.NoError(t, jobDB.Store(context.Background(), jobs))

		require.Equal(t, int64(1), jobDB.GetMaxDSIndex())
		time.Sleep(time.Second * 2)   // wait for some time to pass
		triggerAddNewDS <- time.Now() // trigger addNewDSLoop to run
		triggerAddNewDS <- time.Now() // Second time, waits for the first loop to finish

		require.Equal(t, int64(2), jobDB.GetMaxDSIndex())
	})

	t.Run("should migrate the datasets after maxDSRetentionPeriod (except the right most one)", func(t *testing.T) {
		customVal := "MOCKDS"
		triggerAddNewDS := make(chan time.Time)
		triggerMigrateDS := make(chan time.Time)

		jobDB := Handle{
			TriggerAddNewDS: func() <-chan time.Time {
				return triggerAddNewDS
			},
			TriggerMigrateDS: func() <-chan time.Time {
				return triggerMigrateDS
			},
			config: c,
		}

		tablePrefix := strings.ToLower(rand.String(5))
		c.Set(fmt.Sprintf("JobsDB.%s.maxDSRetention", tablePrefix), "1s")
		err := jobDB.Setup(c, ReadWrite, true, tablePrefix, []prebackup.Handler{}, fileuploader.NewDefaultProvider())
		require.NoError(t, err)
		defer jobDB.TearDown()

		jobs := genJobs(defaultWorkspaceID, customVal, 10, 1)
		require.NoError(t, jobDB.Store(context.Background(), jobs))

		require.EqualValues(t, 1, jobDB.GetMaxDSIndex())
		time.Sleep(time.Second * 2)   // wait for some time to pass
		triggerAddNewDS <- time.Now() // trigger addNewDSLoop to run
		triggerAddNewDS <- time.Now() // Second time, waits for the first loop to finish

		jobDBInspector := HandleInspector{Handle: &jobDB}
		require.EqualValues(t, 2, len(jobDBInspector.DSIndicesList()))
		require.EqualValues(t, 2, jobDB.GetMaxDSIndex())

		jobs = genJobs(defaultWorkspaceID, customVal, 10, 1)
		require.NoError(t, jobDB.Store(context.Background(), jobs)) // store in 2nd dataset

		jobsResult, err := jobDB.GetUnprocessed(context.Background(), GetQueryParams{
			CustomValFilters: []string{customVal},
			JobsLimit:        100,
			ParameterFilters: []ParameterFilterT{},
		})
		require.NoError(t, err, "GetUnprocessed failed")
		fetchedJobs := jobsResult.Jobs
		require.Equal(t, 20, len(fetchedJobs))

		status := JobStatusT{
			JobID:         fetchedJobs[0].JobID,
			JobState:      "succeeded",
			AttemptNum:    1,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     "202",
			ErrorResponse: []byte(`{"success":"OK"}`),
			Parameters:    []byte(`{}`),
		}

		err = jobDB.UpdateJobStatus(context.Background(), []*JobStatusT{&status}, []string{customVal}, []ParameterFilterT{})
		require.NoError(t, err)

		time.Sleep(time.Second * 2) // wait for some time to pass so that retention condition satisfies

		triggerMigrateDS <- time.Now() // trigger migrateDSLoop to run
		triggerMigrateDS <- time.Now() // Second time, waits for the first loop to finish

		dsIndicesList := jobDBInspector.DSIndicesList()
		require.EqualValues(t, "1_1", dsIndicesList[0])
		require.EqualValues(t, "2", dsIndicesList[1])
		require.EqualValues(t, 2, len(jobDBInspector.DSIndicesList()))
		require.EqualValues(t, 2, jobDB.GetMaxDSIndex())

		jobsResult, err = jobDB.GetUnprocessed(context.Background(), GetQueryParams{
			CustomValFilters: []string{customVal},
			JobsLimit:        100,
			ParameterFilters: []ParameterFilterT{},
		})
		require.NoError(t, err, "GetUnprocessed failed")
		require.EqualValues(t, 19, len(jobsResult.Jobs))
	})
	t.Run("should migrate small datasets that have been migrated at least once (except right most one)", func(t *testing.T) {
		customVal := "MOCKDS"
		triggerAddNewDS := make(chan time.Time)
		triggerMigrateDS := make(chan time.Time)
		trigger := func() {
			triggerAddNewDS <- time.Now()
			triggerAddNewDS <- time.Now() // Second time, waits for the first loop to finish
			triggerMigrateDS <- time.Now()
			triggerMigrateDS <- time.Now() // Second time, waits for the first loop to finish
		}

		c.Set("jobsdb.maxDSSize", 10)

		jobDB := Handle{
			TriggerAddNewDS: func() <-chan time.Time {
				return triggerAddNewDS
			},
			TriggerMigrateDS: func() <-chan time.Time {
				return triggerMigrateDS
			},
		}
		prefix := strings.ToLower(rand.String(5))
		c.Set("JobsDB.jobDoneMigrateThreshold", 0.7)
		c.Set("JobsDB.jobMinRowsMigrateThreshold", 0.6)
		err := jobDB.Setup(c, ReadWrite, true, prefix, []prebackup.Handler{}, fileuploader.NewDefaultProvider())
		require.NoError(t, err)
		defer jobDB.TearDown()

		getDSList := func() []dataSetT {
			jobDB.dsListLock.RLock()
			defer jobDB.dsListLock.RUnlock()
			return jobDB.getDSList()
		}

		jobs := genJobs(defaultWorkspaceID, customVal, 20, 1)
		require.NoError(t, jobDB.Store(context.Background(), jobs))
		trigger()
		jobs = genJobs(defaultWorkspaceID, customVal, 20, 1)
		require.NoError(t, jobDB.Store(context.Background(), jobs))
		trigger()
		jobs = genJobs(defaultWorkspaceID, customVal, 11, 1)
		require.NoError(t, jobDB.Store(context.Background(), jobs))
		trigger()
		jobs = genJobs(defaultWorkspaceID, customVal, 11, 1)
		require.NoError(t, jobDB.Store(context.Background(), jobs))
		trigger()
		dsList := getDSList()
		require.Lenf(t, dsList, 5, "dsList length is not 5, got %+v", dsList)
		require.Equal(t, prefix+"_jobs_1", dsList[0].JobTable)
		require.Equal(t, prefix+"_jobs_2", dsList[1].JobTable)
		require.Equal(t, prefix+"_jobs_3", dsList[2].JobTable)
		require.Equal(t, prefix+"_jobs_4", dsList[3].JobTable)
		require.Equal(t, prefix+"_jobs_5", dsList[4].JobTable)

		jobsResult, err := jobDB.GetUnprocessed(context.Background(), GetQueryParams{
			CustomValFilters: []string{customVal},
			JobsLimit:        100,
			ParameterFilters: []ParameterFilterT{},
		})
		require.NoError(t, err)

		// process some jobs
		for _, job := range jobsResult.Jobs[:15] {
			status := JobStatusT{
				JobID:         job.JobID,
				JobState:      "succeeded",
				AttemptNum:    1,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "202",
				ErrorResponse: []byte(`{"success":"OK"}`),
				Parameters:    []byte(`{}`),
			}
			err := jobDB.UpdateJobStatus(context.Background(), []*JobStatusT{&status}, []string{customVal}, []ParameterFilterT{})
			require.NoError(t, err)
		}

		trigger() // jobs_1 will be migrated to jobs_1_1 due to the completed threshold (15/20 > 0.7)

		dsList = getDSList()
		require.Lenf(t, dsList, 5, "dsList length is not 5, got %+v", dsList)
		require.Equal(t, prefix+"_jobs_1_1", dsList[0].JobTable)
		require.Equal(t, prefix+"_jobs_2", dsList[1].JobTable)
		require.Equal(t, prefix+"_jobs_3", dsList[2].JobTable)
		require.Equal(t, prefix+"_jobs_4", dsList[3].JobTable)
		require.Equal(t, prefix+"_jobs_5", dsList[4].JobTable)

		trigger() // jobs_1_1 will remain as is even though it is now a small table (5 < 10*0.6)
		dsList = getDSList()
		require.Lenf(t, dsList, 5, "dsList length is not 5, got %+v", dsList)
		require.Equal(t, prefix+"_jobs_1_1", dsList[0].JobTable)
		require.Equal(t, prefix+"_jobs_2", dsList[1].JobTable)
		require.Equal(t, prefix+"_jobs_3", dsList[2].JobTable)
		require.Equal(t, prefix+"_jobs_4", dsList[3].JobTable)
		require.Equal(t, prefix+"_jobs_5", dsList[4].JobTable)

		// process some jobs
		jobsResult, err = jobDB.GetUnprocessed(context.Background(), GetQueryParams{
			CustomValFilters: []string{customVal},
			JobsLimit:        100,
			ParameterFilters: []ParameterFilterT{},
		})
		require.NoError(t, err)
		for _, job := range jobsResult.Jobs[5:20] {
			status := JobStatusT{
				JobID:         job.JobID,
				JobState:      Succeeded.State,
				AttemptNum:    1,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "202",
				ErrorResponse: []byte(`{"success":"OK"}`),
				Parameters:    []byte(`{}`),
			}
			err := jobDB.UpdateJobStatus(context.Background(), []*JobStatusT{&status}, []string{customVal}, []ParameterFilterT{})
			require.NoError(t, err)
		}

		trigger() // both jobs_1_1 and jobs_2 would be migrated to jobs_2_1
		dsList = getDSList()
		require.Lenf(t, dsList, 4, "dsList length is not 4, got %+v", dsList)
		require.Equal(t, prefix+"_jobs_2_1", dsList[0].JobTable)
		require.Equal(t, prefix+"_jobs_3", dsList[1].JobTable)
		require.Equal(t, prefix+"_jobs_4", dsList[2].JobTable)
		require.Equal(t, prefix+"_jobs_5", dsList[3].JobTable)
	})

	t.Run(`migrates only moves non-terminal jobs to a new DS`, func(t *testing.T) {
		customVal := "MOCKDS"
		triggerAddNewDS := make(chan time.Time)
		triggerMigrateDS := make(chan time.Time)

		jobDB := Handle{
			TriggerAddNewDS: func() <-chan time.Time {
				return triggerAddNewDS
			},
			TriggerMigrateDS: func() <-chan time.Time {
				return triggerMigrateDS
			},
		}
		tablePrefix := strings.ToLower(rand.String(5))
		c.Set(fmt.Sprintf("JobsDB.%s.maxDSRetention", tablePrefix), "1s")
		err := jobDB.Setup(c, ReadWrite, true, tablePrefix, []prebackup.Handler{}, fileuploader.NewDefaultProvider())
		require.NoError(t, err)
		defer jobDB.TearDown()

		var (
			numTotalJobs       = 30
			numFailedJobs      = 10
			numUnprocessedJobs = 10
			numSucceededJobs   = 10
			jobs               = genJobs(defaultWorkspaceID, customVal, numTotalJobs, 1)
			// first #numFailedJobs jobs marked Failed - should be migrated
			failedStatuses = genJobStatuses(jobs[:numFailedJobs], Failed.State)
			// #numFailedJobs - #numFailedJobs+#numSucceededJobs jobs marked as succeeded - should not be migrated
			succeededStatuses = genJobStatuses(jobs[numFailedJobs:numFailedJobs+numSucceededJobs], Succeeded.State)
			// #numFailedJobs+#numSucceededJobs - #numTotalJobs jobs are unprocessed - should be migrated
		)
		require.NoError(t, jobDB.Store(context.Background(), jobs))

		require.NoError(
			t,
			jobDB.UpdateJobStatus(
				context.Background(),
				append(failedStatuses, succeededStatuses...),
				[]string{customVal},
				[]ParameterFilterT{},
			),
		)

		unprocessedBeforeMigration, err := jobDB.GetUnprocessed(context.Background(), GetQueryParams{JobsLimit: 100})
		require.NoError(t, err)
		failedBeforeMigration, err := jobDB.GetFailed(context.Background(), GetQueryParams{JobsLimit: 100})
		require.NoError(t, err)

		require.EqualValues(t, 1, jobDB.GetMaxDSIndex())
		time.Sleep(time.Second * 2)   // wait for some time to pass
		triggerAddNewDS <- time.Now() // trigger addNewDSLoop to run
		triggerAddNewDS <- time.Now() // Second time, waits for the first loop to finish

		jobDBInspector := HandleInspector{Handle: &jobDB}
		require.EqualValues(t, 2, len(jobDBInspector.DSIndicesList()))
		require.EqualValues(t, 2, jobDB.GetMaxDSIndex())

		time.Sleep(time.Second * 2) // wait for some time to pass so that retention condition satisfies

		triggerMigrateDS <- time.Now() // trigger migrateDSLoop to run
		triggerMigrateDS <- time.Now() // Second time, waits for the first loop to finish

		dsIndicesList := jobDBInspector.DSIndicesList()
		require.EqualValues(t, 2, len(jobDBInspector.DSIndicesList()))
		require.EqualValues(t, "1_1", dsIndicesList[0])
		require.EqualValues(t, "2", dsIndicesList[1])
		require.EqualValues(t, 2, jobDB.GetMaxDSIndex())

		// only non-terminal jobs should be migrated
		var numJobs int64
		require.NoError(
			t,
			jobDB.dbHandle.QueryRow(
				fmt.Sprintf(`SELECT COUNT(*) FROM %s`, tablePrefix+`_jobs_1_1`),
			).Scan(&numJobs),
		)
		require.Equal(t, numFailedJobs+numUnprocessedJobs, int(numJobs))

		// verify job statuses
		var numJobstatuses, maxJobStatusID, nextSeqVal int64
		require.NoError(
			t,
			jobDB.dbHandle.QueryRow(
				fmt.Sprintf(`SELECT COUNT(*), MAX(id), nextval('%[1]s_id_seq') FROM %[1]s`, tablePrefix+`_job_status_1_1`),
			).Scan(&numJobstatuses, &maxJobStatusID, &nextSeqVal),
		)
		require.Equal(t, numFailedJobs, int(numJobstatuses))
		require.Greater(t, nextSeqVal, maxJobStatusID)

		// verify that unprocessed jobs are migrated to new DS
		unprocessedResult, err := jobDB.GetUnprocessed(context.Background(), GetQueryParams{
			CustomValFilters: []string{customVal},
			JobsLimit:        100,
			ParameterFilters: []ParameterFilterT{},
		})
		require.NoError(t, err, "GetUnprocessed failed")
		require.Len(t, unprocessedResult.Jobs, numUnprocessedJobs)
		require.EqualValues(t, unprocessedBeforeMigration.Jobs, unprocessedResult.Jobs)

		// verifying that failed jobs are migrated to new DS
		failedResult, err := jobDB.GetFailed(context.Background(), GetQueryParams{
			CustomValFilters: []string{customVal},
			JobsLimit:        100,
			ParameterFilters: []ParameterFilterT{},
		})
		require.NoError(t, err, "GetToRetry failed")
		require.Len(t, failedResult.Jobs, numFailedJobs)
		require.EqualValues(t, failedBeforeMigration.Jobs, failedResult.Jobs)
	})
}

func TestMultiTenantLegacyGetAllJobs(t *testing.T) {
	triggerAddNewDS := make(chan time.Time)
	config.Reset()
	c := config.New()
	_ = startPostgres(t, c)
	c.Set("jobsdb.maxDSSize", 10)
	jobDB := Handle{
		config: c,
		TriggerAddNewDS: func() <-chan time.Time {
			return triggerAddNewDS
		},
	}

	customVal := "MTL"
	err := jobDB.Setup(c, ReadWrite, false, strings.ToLower(rand.String(5)), []prebackup.Handler{}, fileuploader.NewDefaultProvider())
	require.NoError(t, err)
	defer jobDB.TearDown()

	eventsPerJob := 10
	// Create 30 jobs
	jobs := genJobs(defaultWorkspaceID, customVal, 30, eventsPerJob)
	require.NoError(t, jobDB.Store(context.Background(), jobs))
	payloadSize, err := getPayloadSize(t, &jobDB, jobs[0])
	require.NoError(t, err)
	j, err := jobDB.GetUnprocessed(context.Background(), GetQueryParams{JobsLimit: 100}) // read to get Ids
	require.NoError(t, err, "failed to get unprocessed jobs")
	jobs = j.Jobs
	require.Equal(t, 30, len(jobs), "should get all 30 jobs")

	// Mark 1-10 as failed
	require.NoError(t, jobDB.UpdateJobStatus(context.Background(), genJobStatuses(jobs[0:10], Failed.State), []string{customVal}, []ParameterFilterT{}))

	// Mark 11-20 as waiting
	require.NoError(t, jobDB.UpdateJobStatus(context.Background(), genJobStatuses(jobs[10:20], Waiting.State), []string{customVal}, []ParameterFilterT{}))

	t.Run("GetAllJobs with large limits", func(t *testing.T) {
		params := GetQueryParams{JobsLimit: 30}
		allJobs, err := jobDB.GetToProcess(context.Background(), params, nil)
		require.NoError(t, err, "failed to get all jobs")
		require.Equal(t, 30, len(allJobs.Jobs), "should get all 30 jobs")
	})

	t.Run("GetAllJobs with only jobs limit", func(t *testing.T) {
		jobsLimit := 10
		params := GetQueryParams{JobsLimit: jobsLimit}
		allJobs, err := jobDB.GetToProcess(context.Background(), params, nil)
		require.NoError(t, err, "failed to get all jobs")
		require.Truef(t, len(allJobs.Jobs)-jobsLimit == 0, "should get %d jobs", jobsLimit)
	})

	t.Run("GetAllJobs with events limit", func(t *testing.T) {
		jobsLimit := 10
		params := GetQueryParams{JobsLimit: jobsLimit, EventsLimit: 3 * eventsPerJob}
		allJobs, err := jobDB.GetToProcess(context.Background(), params, nil)
		require.NoError(t, err, "failed to get all jobs")
		require.Equal(t, 3, len(allJobs.Jobs), "should get 3 jobs")
	})

	t.Run("GetAllJobs with events limit less than the events of the first job get one job", func(t *testing.T) {
		jobsLimit := 10
		params := GetQueryParams{JobsLimit: jobsLimit, EventsLimit: eventsPerJob - 1}
		allJobs, err := jobDB.GetToProcess(context.Background(), params, nil)
		require.NoError(t, err, "failed to get all jobs")
		require.Equal(t, 1, len(allJobs.Jobs), "should get 1 overflown job")
	})

	t.Run("GetAllJobs with payload limit", func(t *testing.T) {
		jobsLimit := 10
		params := GetQueryParams{JobsLimit: jobsLimit, PayloadSizeLimit: 3 * payloadSize}
		allJobs, err := jobDB.GetToProcess(context.Background(), params, nil)
		require.NoError(t, err, "failed to get all jobs")
		require.Equal(t, 3, len(allJobs.Jobs), "should get 3 jobs")
	})

	t.Run("GetAllJobs with payload limit less than the payload size should get one job", func(t *testing.T) {
		jobsLimit := 10
		params := GetQueryParams{JobsLimit: jobsLimit, PayloadSizeLimit: payloadSize - 1}
		allJobs, err := jobDB.GetToProcess(context.Background(), params, nil)
		require.NoError(t, err, "failed to get all jobs")
		require.Equal(t, 1, len(allJobs.Jobs), "should get 1 overflown job")
	})
}

func TestStoreAndUpdateStatusExceedingAnalyzeThreshold(t *testing.T) {
	t.Setenv("RSERVER_JOBS_DB_ANALYZE_THRESHOLD", "0")

	c := config.New()
	_ = startPostgres(t, c)
	c.Set("jobsdb.maxDSSize", 10)

	jobDB := Handle{}
	customVal := "MOCKDS"
	err := jobDB.Setup(c, ReadWrite, false, strings.ToLower(rand.String(5)), []prebackup.Handler{}, fileuploader.NewDefaultProvider())
	require.NoError(t, err)
	defer jobDB.TearDown()
	sampleTestJob := JobT{
		Parameters:   []byte(`{}`),
		EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient", "device_name":"FooBar\ufffd\u0000\ufffd\u000f\ufffd","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
		UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
		UUID:         uuid.New(),
		CustomVal:    customVal,
		WorkspaceId:  defaultWorkspaceID,
		EventCount:   1,
	}
	err = jobDB.Store(context.Background(), []*JobT{&sampleTestJob})
	require.NoError(t, err)
	unprocessedJob, err := jobDB.GetUnprocessed(context.Background(), GetQueryParams{
		CustomValFilters: []string{customVal},
		JobsLimit:        1,
		ParameterFilters: []ParameterFilterT{},
	})
	require.NoError(t, err, "should get unprocessed job")
	unprocessedList := unprocessedJob.Jobs
	require.Equal(t, 1, len(unprocessedList))
	j := unprocessedList[0]
	jobStatus := &JobStatusT{
		JobID:         j.JobID,
		JobState:      "succeeded",
		AttemptNum:    1,
		ExecTime:      time.Now(),
		RetryTime:     time.Now(),
		ErrorCode:     "202",
		ErrorResponse: []byte(`{"success":"OK"}`),
		Parameters:    []byte(`{}`),
		WorkspaceId:   defaultWorkspaceID,
	}
	err = jobDB.UpdateJobStatus(context.Background(), []*JobStatusT{jobStatus}, []string{customVal}, []ParameterFilterT{})
	require.NoError(t, err)
}

func TestCreateDS(t *testing.T) {
	c := config.New()
	postgresql := startPostgres(t, c)
	t.Run("CreateDS in case of negative job_indices in the previous", func(t *testing.T) {
		prefix := strings.ToLower(rand.String(5))
		// create -ve index table
		func() {
			_, err := postgresql.DB.Exec(fmt.Sprintf(`CREATE TABLE "%[1]s_jobs_-2" (
				job_id BIGSERIAL PRIMARY KEY,
			workspace_id TEXT NOT NULL DEFAULT '',
			uuid UUID NOT NULL,
			user_id TEXT NOT NULL,
			parameters JSONB NOT NULL,
			custom_val VARCHAR(64) NOT NULL,
			event_payload JSONB NOT NULL,
			event_count INTEGER NOT NULL DEFAULT 1,
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			expire_at TIMESTAMP NOT NULL DEFAULT NOW());`, prefix))
			require.NoError(t, err)
			_, err = postgresql.DB.Exec(fmt.Sprintf(`CREATE TABLE "%[1]s_job_status_-2" (
				id BIGSERIAL,
				job_id BIGINT REFERENCES "%[1]s_jobs_-2"(job_id),
				job_state VARCHAR(64),
				attempt SMALLINT,
				exec_time TIMESTAMP,
				retry_time TIMESTAMP,
				error_code VARCHAR(32),
				error_response JSONB DEFAULT '{}'::JSONB,
				parameters JSONB DEFAULT '{}'::JSONB,
				PRIMARY KEY (job_id, job_state, id));`, prefix))
			require.NoError(t, err)
			negativeJobID := -100
			_, err = postgresql.DB.Exec(fmt.Sprintf(`ALTER SEQUENCE "%[2]s_jobs_-2_job_id_seq" MINVALUE %[1]d START %[1]d RESTART %[1]d;`, negativeJobID, prefix))
			require.NoError(t, err)

			_, err = postgresql.DB.Exec(fmt.Sprintf(`INSERT INTO "%[1]s_jobs_-2" (uuid, user_id, custom_val, parameters, event_payload) values ('c2d29867-3d0b-d497-9191-18a9d8ee7869', 'someuserid', 'GW', '{}', '{}');`, prefix))
			require.NoError(t, err)
			_, err = postgresql.DB.Exec(fmt.Sprintf(`INSERT INTO "%[1]s_jobs_-2" (uuid, user_id, custom_val, parameters, event_payload) values ('c2d29867-3d0b-d497-9191-18a9d8ee7860', 'someuserid', 'GW', '{}', '{}');`, prefix))
			require.NoError(t, err)

			triggerAddNewDS := make(chan time.Time)
			c.Set("jobsdb.maxDSSize", 1)
			jobDB := Handle{
				dbHandle: postgresql.DB,
				config:   c,
				TriggerAddNewDS: func() <-chan time.Time {
					return triggerAddNewDS
				},
			}
			err = jobDB.Setup(c, ReadWrite, false, prefix, []prebackup.Handler{}, fileuploader.NewDefaultProvider())
			require.NoError(t, err)
			defer jobDB.TearDown()

			triggerAddNewDS <- time.Now()
			triggerAddNewDS <- time.Now() // Second time, waits for the first loop to finish

			tables, err := postgresql.DB.Query(fmt.Sprintf(`SELECT table_name FROM information_schema.tables WHERE table_name LIKE '%[1]s_jobs_`, prefix) + `%' order by table_name desc;`)
			require.NoError(t, err)
			defer func() { _ = tables.Close() }()
			var tableName string
			tableNames := make([]string, 0)
			for tables.Next() {
				err = tables.Scan(&tableName)
				require.NoError(t, err)
				tableNames = append(tableNames, tableName)
			}
			require.NoError(t, tables.Err())
			require.Equal(t, len(tableNames), 2, `should find two tables`)
			require.Equal(t, tableNames[0], prefix+"_jobs_-2")
			require.Equal(t, tableNames[1], prefix+"_jobs_-1")
			expectedNextVal := negativeJobID + 2

			nextVal := postgresql.DB.QueryRow(fmt.Sprintf(`select nextval('"%s_job_id_seq"');`, tableNames[1]))
			var nextValInt int
			err = nextVal.Scan(&nextValInt)
			require.NoError(t, err)
			require.Equal(t, nextValInt, expectedNextVal, `should have the correct nextval`)
		}()
	})
}

func requireSequential(t *testing.T, jobs []*JobT) {
	t.Helper()
	t.Log("job ids should be sequential")
	for i := 0; i < len(jobs)-1; i++ {
		require.Equal(t, jobs[i].JobID+1, jobs[i+1].JobID, "Gap detected: jobs[%d].id = %d +1 != jobs[%d].id = %d +1", i, jobs[i].JobID, i+1, jobs[i+1].JobID)
	}
}

func TestJobsDB_IncompatiblePayload(t *testing.T) {
	triggerAddNewDS := make(chan time.Time)
	c := config.New()
	_ = startPostgres(t, c)
	c.Set("jobsdb.maxDSSize", 10)
	jobDB := Handle{
		config: c,
		TriggerAddNewDS: func() <-chan time.Time {
			return triggerAddNewDS
		},
	}
	err := jobDB.Setup(c, ReadWrite, false, strings.ToLower(rand.String(5)), []prebackup.Handler{}, fileuploader.NewDefaultProvider())
	require.NoError(t, err)
	defer jobDB.TearDown()
	customVal := "MOCKDS"
	sampleTestJob := JobT{
		Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
		EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient", "device_name":"FooBar\ufffd\u0000\ufffd\u000f\ufffd","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
		UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
		UUID:         uuid.New(),
		CustomVal:    customVal,
		WorkspaceId:  defaultWorkspaceID,
		EventCount:   1,
	}
	errMap := jobDB.StoreEachBatchRetry(context.Background(), [][]*JobT{{&sampleTestJob}})
	for _, val := range errMap {
		require.Equal(t, "", val)
	}
	unprocessedJob, err := jobDB.GetUnprocessed(context.Background(), GetQueryParams{
		CustomValFilters: []string{customVal},
		JobsLimit:        1,
		ParameterFilters: []ParameterFilterT{},
	})
	require.NoError(t, err, "should not error")

	unprocessedList := unprocessedJob.Jobs
	require.Equal(t, 1, len(unprocessedList))

	t.Run("validate fetched event", func(t *testing.T) {
		require.Equal(t, "MOCKDS", unprocessedList[0].CustomVal)
		require.Equal(t, defaultWorkspaceID, unprocessedList[0].WorkspaceId)
	})
}

// BenchmarkJobsdb takes time... keep waiting
func BenchmarkJobsdb(b *testing.B) {
	// We are intentionally not using b.N, since we want to have a testbench for stress testing jobsdb's behaviour for longer periods of time (5-15 seconds)
	const (
		// totalJobs is the total number of jobs we want our workers to send to the jobsdb, regardless of concurrency
		totalJobs = 10000
		// maxDsSize is the maximum ds size, controls how often a new DS will be created
		maxDsSize = 500
		// pageSize is the batch size for appending and retrieving jobs within each worker
		pageSize = 10
	)
	b.Setenv("RSERVER_JOBS_DB_MAX_DSSIZE", fmt.Sprintf("%d", maxDsSize))
	_ = startPostgres(b, config.Default)
	concurrencies := []int{16, 64, 256, 512}

	for _, concurrency := range concurrencies {
		jobsDb1 := Handle{}
		b.Setenv("RSERVER_JOBS_DB_ENABLE_WRITER_QUEUE", "true")
		b.Setenv("RSERVER_JOBS_DB_ENABLE_READER_QUEUE", "true")
		err := jobsDb1.Setup(config.Default, ReadWrite, true, "batch_rt", []prebackup.Handler{}, fileuploader.NewDefaultProvider())
		require.NoError(b, err)

		b.Run(fmt.Sprintf("store and consume %d jobs using %d stream(s) with reader writer queues", totalJobs, concurrency), func(b *testing.B) {
			benchmarkJobsdbConcurrently(b, &jobsDb1, totalJobs, pageSize, concurrency)
		})
		jobsDb1.TearDown()

		jobsDb2 := Handle{}
		b.Setenv("RSERVER_JOBS_DB_ENABLE_WRITER_QUEUE", "false")
		b.Setenv("RSERVER_JOBS_DB_ENABLE_READER_QUEUE", "false")
		b.Setenv("RSERVER_JOBS_DB_GW_MAX_OPEN_CONNECTIONS", "64")
		err = jobsDb2.Setup(config.Default, ReadWrite, true, "batch_rt", []prebackup.Handler{}, fileuploader.NewDefaultProvider())
		require.NoError(b, err)

		b.Run(fmt.Sprintf("store and consume %d jobs using %d stream(s) without reader writer queues", totalJobs, concurrency), func(b *testing.B) {
			benchmarkJobsdbConcurrently(b, &jobsDb2, totalJobs, pageSize, concurrency)
		})
		jobsDb2.TearDown()
	}
}

func benchmarkJobsdbConcurrently(b *testing.B, jobsDB *Handle, totalJobs, pageSize, concurrency int) {
	b.StopTimer()
	var start, end sync.WaitGroup
	start.Add(1)
	workerJobs := totalJobs / concurrency
	end.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		num := i
		go func() {
			customVal := fmt.Sprintf("MOCKDS%d", num)
			expectedJobs := make([]JobT, workerJobs)
			for i := range expectedJobs {
				expectedJobs[i] = JobT{
					WorkspaceId:  "workspace",
					Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
					EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
					UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
					UUID:         uuid.New(),
					CustomVal:    customVal,
				}
			}

			g, _ := errgroup.WithContext(context.Background())
			g.Go(func() error {
				chunks := chunkJobs(expectedJobs, pageSize)
				start.Wait()
				for i := range chunks {
					err := jobsDB.Store(context.Background(), chunks[i])
					if err != nil {
						return err
					}
				}
				return nil
			})

			consumedJobs := make([]JobT, 0, len(expectedJobs))
			timeout := time.After(time.Second * time.Duration(len(expectedJobs)))
			g.Go(func() error {
				start.Wait()
				for {
					unprocessedJob, err := jobsDB.GetUnprocessed(context.Background(), GetQueryParams{
						CustomValFilters: []string{customVal},
						JobsLimit:        pageSize,
					})
					require.NoError(b, err, "failed to get unprocessed jobs")
					unprocessedList := unprocessedJob.Jobs
					status := make([]*JobStatusT, len(unprocessedList))
					for i, j := range unprocessedList {
						status[i] = &JobStatusT{
							JobID:         j.JobID,
							JobState:      "succeeded",
							AttemptNum:    1,
							ExecTime:      time.Now(),
							RetryTime:     time.Now(),
							ErrorCode:     "202",
							ErrorResponse: []byte(`{"success":"OK"}`),
							Parameters:    []byte(`{}`),
							WorkspaceId:   defaultWorkspaceID,
						}
					}

					err = jobsDB.UpdateJobStatus(context.Background(), status, []string{customVal}, []ParameterFilterT{})
					require.NoError(b, err)

					for _, j := range unprocessedList {
						consumedJobs = append(consumedJobs, *j)
					}
					select {
					case <-time.After(1 * time.Nanosecond):
						if len(consumedJobs) >= len(expectedJobs) {
							return nil
						}
					case <-timeout:
						return nil
					}
				}
			})

			err := g.Wait()
			require.NoError(b, err)
			require.Len(b, consumedJobs, len(expectedJobs))
			for i := range consumedJobs {
				expectedJob := expectedJobs[i]
				require.Equal(b, expectedJob.UUID, consumedJobs[i].UUID)
				require.Equal(b, expectedJob.UserID, consumedJobs[i].UserID)
				require.Equal(b, expectedJob.CustomVal, consumedJobs[i].CustomVal)
				require.JSONEq(b, string(expectedJob.Parameters), string(consumedJobs[i].Parameters))
				require.JSONEq(b, string(expectedJob.EventPayload), string(consumedJobs[i].EventPayload))
			}
			end.Done()
		}()
	}
	b.StartTimer()
	start.Done()
	end.Wait()
}

func chunkJobs(slice []JobT, chunkSize int) [][]*JobT {
	var chunks [][]*JobT
	for i := 0; i < len(slice); i += chunkSize {
		end := i + chunkSize
		if end > len(slice) {
			end = len(slice)
		}
		jslice := slice[i:end]
		var chunk []*JobT
		for i := range jslice {
			chunk = append(chunk, &jslice[i])
		}
		chunks = append(chunks, chunk)
	}

	return chunks
}

func BenchmarkLifecycle(b *testing.B) {
	c := config.New()
	_ = startPostgres(b, c)

	const writeConcurrency = 10
	const newJobs = 100

	dsSize := writeConcurrency * newJobs
	c.Set("jobsdb.maxDSSize", dsSize)
	triggerAddNewDS := make(chan time.Time)

	jobDB := &Handle{
		config: c,
		TriggerAddNewDS: func() <-chan time.Time {
			return triggerAddNewDS
		},
	}
	defer jobDB.TearDown()

	b.Run("Start, Work, Stop, Repeat", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			err := jobDB.Start()
			require.NoError(b, err)
			b.StopTimer()
			wg := sync.WaitGroup{}
			wg.Add(writeConcurrency)
			for j := 0; j < writeConcurrency; j++ {
				go func() {
					err = jobDB.Store(context.Background(), genJobs(defaultWorkspaceID, "", newJobs, 10))
					wg.Done()
				}()
			}
			wg.Wait()
			require.NoError(b, err, "expected no error while storing jobs")
			triggerAddNewDS <- time.Now()
			consume(b, jobDB, newJobs*writeConcurrency)
			b.StartTimer()
			jobDB.Stop()
		}
	})
}

func consume(t testing.TB, db *Handle, count int) {
	t.Helper()

	unprocessedList, err := db.GetUnprocessed(context.Background(), GetQueryParams{
		JobsLimit: count,
	})
	require.NoError(t, err, "failed to get unprocessed jobs")

	status := make([]*JobStatusT, len(unprocessedList.Jobs))
	for i, j := range unprocessedList.Jobs {
		status[i] = &JobStatusT{
			JobID:         j.JobID,
			JobState:      "succeeded",
			AttemptNum:    1,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     "202",
			ErrorResponse: []byte(`{"success":"OK"}`),
			Parameters:    []byte(`{}`),
			WorkspaceId:   defaultWorkspaceID,
		}
	}

	err = db.UpdateJobStatus(context.Background(), status, []string{}, []ParameterFilterT{})
	require.NoError(t, err)
}

func getPayloadSize(t *testing.T, jobsDB JobsDB, job *JobT) (int64, error) {
	var size int64
	var tables []string
	err := jobsDB.WithTx(func(tx *Tx) error {
		rows, err := tx.Query(fmt.Sprintf("SELECT tablename FROM pg_catalog.pg_tables where tablename like '%s_jobs_%%'", jobsDB.Identifier()))
		require.NoError(t, err)
		for rows.Next() {
			var table string
			err = rows.Scan(&table)
			require.NoError(t, err)
			tables = append(tables, table)
		}
		require.NoError(t, rows.Err())
		_ = rows.Close()

		for _, table := range tables {
			stmt, err := tx.Prepare(fmt.Sprintf("select pg_column_size(event_payload) from %s where uuid=$1", table))
			require.NoError(t, err)
			err = stmt.QueryRow(job.UUID).Scan(&size)
			_ = stmt.Close()
			if err == nil {
				break
			}
		}
		return err
	})

	return size, err
}
