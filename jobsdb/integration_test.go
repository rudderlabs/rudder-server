package jobsdb_test

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	uuid "github.com/gofrs/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/jobsdb/prebackup"
	"github.com/rudderlabs/rudder-server/services/archiver"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

const (
	defaultWorkspaceID = "workspaceId"
)

var (
	hold   bool
	DB_DSN = "root@tcp(127.0.0.1:3306)/service"
	db     *sql.DB
)

func TestMain(m *testing.M) {
	flag.BoolVar(&hold, "hold", false, "hold environment clean-up after test execution until Ctrl+C is provided")
	flag.Parse()

	// hack to make defer work, without being affected by the os.Exit in TestMain
	os.Exit(run(m))
}

func run(m *testing.M) int {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

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
	defer func() {
		if err := pool.Purge(resourcePostgres); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()

	DB_DSN = fmt.Sprintf("postgres://rudder:password@localhost:%s/%s?sslmode=disable", resourcePostgres.GetPort("5432/tcp"), database)
	fmt.Println("DB_DSN:", DB_DSN)
	os.Setenv("JOBS_DB_DB_NAME", database)
	os.Setenv("JOBS_DB_HOST", "localhost")
	os.Setenv("JOBS_DB_NAME", "jobsdb")
	os.Setenv("JOBS_DB_USER", "rudder")
	os.Setenv("JOBS_DB_PASSWORD", "password")
	os.Setenv("JOBS_DB_PORT", resourcePostgres.GetPort("5432/tcp"))

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		var err error
		db, err = sql.Open("postgres", DB_DSN)
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	code := m.Run()
	blockOnHold()

	return code
}

func blockOnHold() {
	if !hold {
		return
	}

	fmt.Println("Test on hold, before cleanup")
	fmt.Println("Press Ctrl+C to exit")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	<-c
}

func initJobsDB() {
	config.Load()
	logger.Init()
	admin.Init()
	jobsdb.Init()
	jobsdb.Init2()
	jobsdb.Init3()

	archiver.Init()
}

func genJobs(workspaceId, customVal string, jobCount, eventsPerJob int) []*jobsdb.JobT {
	js := make([]*jobsdb.JobT, jobCount)
	for i := range js {
		js[i] = &jobsdb.JobT{
			Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
			EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
			UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
			UUID:         uuid.Must(uuid.NewV4()),
			CustomVal:    customVal,
			EventCount:   eventsPerJob,
			WorkspaceId:  workspaceId,
		}
	}
	return js
}

func genJobStatuses(jobs []*jobsdb.JobT, state string) []*jobsdb.JobStatusT {
	statuses := []*jobsdb.JobStatusT{}
	for i := range jobs {
		job := jobs[i]
		statuses = append(statuses, &jobsdb.JobStatusT{
			JobID:         job.JobID,
			JobState:      state,
			AttemptNum:    1,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     "999",
			ErrorResponse: []byte(`{}`),
			Parameters:    []byte(`{}`),
			WorkspaceId:   job.WorkspaceId,
		})
	}
	return statuses
}

func TestJobsDB(t *testing.T) {
	initJobsDB()
	stats.Setup()

	dbRetention := time.Minute * 5
	migrationMode := ""

	triggerAddNewDS := make(chan time.Time, 0)
	maxDSSize := 10
	jobDB := jobsdb.HandleT{
		MaxDSSize: &maxDSSize,
		TriggerAddNewDS: func() <-chan time.Time {
			return triggerAddNewDS
		},
	}
	queryFilters := jobsdb.QueryFiltersT{
		CustomVal: true,
	}

	jobDB.Setup(jobsdb.ReadWrite, false, "batch_rt", dbRetention, migrationMode, true, queryFilters, []prebackup.Handler{})
	defer jobDB.TearDown()

	customVal := "MOCKDS"
	var sampleTestJob = jobsdb.JobT{
		Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
		EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
		UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
		UUID:         uuid.Must(uuid.NewV4()),
		CustomVal:    customVal,
	}

	unprocessedListEmpty := jobDB.GetUnprocessed(jobsdb.GetQueryParamsT{
		CustomValFilters: []string{customVal},
		JobsLimit:        1,
		ParameterFilters: []jobsdb.ParameterFilterT{},
	})

	require.Equal(t, 0, len(unprocessedListEmpty))
	err := jobDB.Store([]*jobsdb.JobT{&sampleTestJob})
	require.NoError(t, err)

	unprocessedList := jobDB.GetUnprocessed(jobsdb.GetQueryParamsT{
		CustomValFilters: []string{customVal},
		JobsLimit:        1,
		ParameterFilters: []jobsdb.ParameterFilterT{},
	})
	require.Equal(t, 1, len(unprocessedList))

	status := jobsdb.JobStatusT{
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

	err = jobDB.UpdateJobStatus([]*jobsdb.JobStatusT{&status}, []string{customVal}, []jobsdb.ParameterFilterT{})
	require.NoError(t, err)

	unprocessedList = jobDB.GetUnprocessed(jobsdb.GetQueryParamsT{
		CustomValFilters: []string{customVal},
		JobsLimit:        1,
		ParameterFilters: []jobsdb.ParameterFilterT{},
	})

	require.Equal(t, 0, len(unprocessedList))

	t.Run("multi events per job", func(t *testing.T) {
		jobCountPerDS := 12
		eventsPerJob := 60

		dsCount := 3
		jobCount := dsCount * jobCountPerDS

		t.Logf("spread %d jobs into %d data sets", jobCount, dsCount)
		for i := 0; i < dsCount; i++ {
			require.NoError(t, jobDB.Store(genJobs(defaultWorkspaceID, customVal, jobCountPerDS, eventsPerJob)))
			triggerAddNewDS <- time.Now()
			triggerAddNewDS <- time.Now() //Second time, waits for the first loop to finish
		}

		t.Log("GetUnprocessed with job count limit")
		JobLimitList := jobDB.GetUnprocessed(jobsdb.GetQueryParamsT{
			CustomValFilters: []string{customVal},
			JobsLimit:        100,
			ParameterFilters: []jobsdb.ParameterFilterT{},
		})
		require.Equal(t, jobCount, len(JobLimitList))

		t.Log("GetUnprocessed with event count limit")
		eventLimitList := jobDB.GetUnprocessed(jobsdb.GetQueryParamsT{
			CustomValFilters: []string{customVal},
			JobsLimit:        100,
			EventsLimit:      eventsPerJob * 20,
			ParameterFilters: []jobsdb.ParameterFilterT{},
		})
		require.Equal(t, 20, len(eventLimitList))
		t.Log("GetUnprocessed jobs should have the expected event count")
		for _, j := range eventLimitList {
			require.Equal(t, eventsPerJob, j.EventCount)
		}

		t.Log("Repeat read")
		eventLimitListRepeat := jobDB.GetUnprocessed(jobsdb.GetQueryParamsT{
			CustomValFilters: []string{customVal},
			JobsLimit:        100,
			EventsLimit:      eventsPerJob * 20,
			ParameterFilters: []jobsdb.ParameterFilterT{},
		})
		require.Equal(t, 20, len(eventLimitListRepeat))
		require.Equal(t, eventLimitList, eventLimitListRepeat)

		statuses := make([]*jobsdb.JobStatusT, len(JobLimitList))

		n := time.Now().Add(time.Hour * -1)
		for i := range statuses {
			statuses[i] = &jobsdb.JobStatusT{
				JobID:         JobLimitList[i].JobID,
				JobState:      jobsdb.Failed.State,
				AttemptNum:    1,
				ExecTime:      n,
				RetryTime:     n,
				ErrorResponse: []byte(`{"success":"OK"}`),
				Parameters:    []byte(`{}`),
				WorkspaceId:   defaultWorkspaceID,
			}
		}
		t.Log("Mark some jobs as failed")
		err = jobDB.UpdateJobStatus(statuses, []string{customVal}, []jobsdb.ParameterFilterT{})
		require.NoError(t, err)

		t.Log("GetUnprocessed with job count limit")
		retryJobLimitList := jobDB.GetToRetry(jobsdb.GetQueryParamsT{
			CustomValFilters: []string{customVal},
			JobsLimit:        100,
		})
		require.Equal(t, jobCount, len(retryJobLimitList))

		t.Log("GetToRetry with event count limit")
		retryEventLimitList := jobDB.GetToRetry(jobsdb.GetQueryParamsT{
			CustomValFilters: []string{customVal},
			JobsLimit:        100,
			EventsLimit:      eventsPerJob * 20,
		})
		require.Equal(t, 20, len(retryEventLimitList))
		t.Log("GetToRetry jobs should have the expected event count")
		for _, j := range eventLimitList {
			require.Equal(t, eventsPerJob, j.EventCount)
		}

	})

	t.Run("DSoverflow", func(t *testing.T) {
		customVal := "MOCKDS"

		triggerAddNewDS := make(chan time.Time)

		maxDSSize := 9
		jobDB := jobsdb.HandleT{
			MaxDSSize: &maxDSSize,
			TriggerAddNewDS: func() <-chan time.Time {
				return triggerAddNewDS
			},
		}

		jobDB.Setup(jobsdb.ReadWrite, true, "gw", dbRetention, migrationMode, true, queryFilters, []prebackup.Handler{})
		defer jobDB.TearDown()

		jobCountPerDS := 10
		eventsPerJob_ds1 := 60
		eventsPerJob_ds2 := 20

		t.Log("First jobs table with jobs of 60 events, second with jobs of 20 events")
		require.NoError(t, jobDB.Store(genJobs(defaultWorkspaceID, customVal, jobCountPerDS, eventsPerJob_ds1)))
		triggerAddNewDS <- time.Now()
		triggerAddNewDS <- time.Now() //Second time, waits for the first loop to finish

		require.NoError(t, jobDB.Store(genJobs(defaultWorkspaceID, customVal, jobCountPerDS, eventsPerJob_ds2)))
		triggerAddNewDS <- time.Now()
		triggerAddNewDS <- time.Now() //Second time, waits for the first loop to finish

		t.Log("GetUnprocessed with event count limit")
		t.Log("Using event count that will cause spill-over, not exact for ds1, but remainder suitable for ds2")
		trickyEventCount := (eventsPerJob_ds1 * (jobCountPerDS - 1)) + eventsPerJob_ds2

		eventLimitList := jobDB.GetUnprocessed(jobsdb.GetQueryParamsT{
			CustomValFilters: []string{customVal},
			JobsLimit:        100,
			EventsLimit:      trickyEventCount,
			ParameterFilters: []jobsdb.ParameterFilterT{},
		})
		requireSequential(t, eventLimitList)
		require.Equal(t, jobCountPerDS-1, len(eventLimitList))

		t.Log("Prepare GetToRetry")
		{
			allJobs := jobDB.GetUnprocessed(jobsdb.GetQueryParamsT{
				CustomValFilters: []string{customVal},
				JobsLimit:        1000,
				ParameterFilters: []jobsdb.ParameterFilterT{},
			})

			statuses := make([]*jobsdb.JobStatusT, len(allJobs))
			n := time.Now().Add(time.Hour * -1)
			for i := range statuses {
				statuses[i] = &jobsdb.JobStatusT{
					JobID:         allJobs[i].JobID,
					JobState:      jobsdb.Failed.State,
					AttemptNum:    1,
					ExecTime:      n,
					RetryTime:     n,
					ErrorResponse: []byte(`{"success":"OK"}`),
					Parameters:    []byte(`{}`),
					WorkspaceId:   defaultWorkspaceID,
				}
			}
			t.Log("Mark all jobs as failed")
			err = jobDB.UpdateJobStatus(statuses, []string{customVal}, []jobsdb.ParameterFilterT{})
			require.NoError(t, err)
		}

		t.Log("Test spill over with GetToRetry")
		{
			eventLimitList := jobDB.GetToRetry(jobsdb.GetQueryParamsT{
				CustomValFilters: []string{customVal},
				JobsLimit:        100,
				EventsLimit:      trickyEventCount,
				ParameterFilters: []jobsdb.ParameterFilterT{},
			})
			requireSequential(t, eventLimitList)
			require.Equal(t, jobCountPerDS-1, len(eventLimitList))
		}

	})

	t.Run("limit by total payload size", func(t *testing.T) {
		customVal := "MOCKDS"

		triggerAddNewDS := make(chan time.Time)

		maxDSSize := 2
		jobDB := jobsdb.HandleT{
			MaxDSSize: &maxDSSize,
			TriggerAddNewDS: func() <-chan time.Time {
				return triggerAddNewDS
			},
		}

		jobDB.Setup(jobsdb.ReadWrite, true, "gw", dbRetention, migrationMode, true, queryFilters)
		defer jobDB.TearDown()

		jobs := genJobs(defaultWorkspaceID, customVal, 2, 1)
		require.NoError(t, jobDB.Store(jobs))
		payloadSize, err := getPayloadSize(t, &jobDB, jobs[0])
		require.NoError(t, err)
		triggerAddNewDS <- time.Now()
		triggerAddNewDS <- time.Now() //Second time, waits for the first loop to finish

		require.NoError(t, jobDB.Store(genJobs(defaultWorkspaceID, customVal, 2, 1)))
		triggerAddNewDS <- time.Now()
		triggerAddNewDS <- time.Now() //Second time, waits for the first loop to finish

		payloadLimit := 3 * payloadSize
		payloadLimitList := jobDB.GetUnprocessed(jobsdb.GetQueryParamsT{
			CustomValFilters: []string{customVal},
			JobsLimit:        100,
			PayloadSizeLimit: payloadLimit,
			ParameterFilters: []jobsdb.ParameterFilterT{},
		})

		requireSequential(t, payloadLimitList)
		require.Equal(t, 3, len(payloadLimitList))
	})

	t.Run("querying with an payload size limit should return at least one job even if limit is exceeded", func(t *testing.T) {
		customVal := "MOCKDS"
		triggerAddNewDS := make(chan time.Time)

		maxDSSize := 1
		jobDB := jobsdb.HandleT{
			MaxDSSize: &maxDSSize,
			TriggerAddNewDS: func() <-chan time.Time {
				return triggerAddNewDS
			},
		}
		jobDB.Setup(jobsdb.ReadWrite, false, "gw", dbRetention, migrationMode, true, queryFilters)
		defer jobDB.TearDown()

		jobs := genJobs(defaultWorkspaceID, customVal, 2, 1)
		require.NoError(t, jobDB.Store(jobs))
		payloadSize, err := getPayloadSize(t, &jobDB, jobs[0])
		require.NoError(t, err)

		payloadLimit := payloadSize / 2
		payloadLimitList := jobDB.GetUnprocessed(jobsdb.GetQueryParamsT{
			CustomValFilters: []string{customVal},
			JobsLimit:        100,
			PayloadSizeLimit: payloadLimit,
			ParameterFilters: []jobsdb.ParameterFilterT{},
		})

		requireSequential(t, payloadLimitList)
		require.Equal(t, 1, len(payloadLimitList))
	})

	t.Run("querying with an event count limit should return at least one job even if limit is exceeded", func(t *testing.T) {
		customVal := "MOCKDS"
		triggerAddNewDS := make(chan time.Time)

		maxDSSize := 1
		jobDB := jobsdb.HandleT{
			MaxDSSize: &maxDSSize,
			TriggerAddNewDS: func() <-chan time.Time {
				return triggerAddNewDS
			},
		}
		jobDB.Setup(jobsdb.ReadWrite, true, "gw", dbRetention, migrationMode, true, queryFilters)
		defer jobDB.TearDown()

		jobs := genJobs(defaultWorkspaceID, customVal, 2, 4)
		require.NoError(t, jobDB.Store(jobs))

		eventCountLimit := 1
		eventLimitList := jobDB.GetUnprocessed(jobsdb.GetQueryParamsT{
			CustomValFilters: []string{customVal},
			JobsLimit:        100,
			EventsLimit:      eventCountLimit,
			ParameterFilters: []jobsdb.ParameterFilterT{},
		})

		requireSequential(t, eventLimitList)
		require.Equal(t, 1, len(eventLimitList))
	})

	t.Run("should stay within event count limits", func(t *testing.T) {
		customVal := "MOCKDS"
		triggerAddNewDS := make(chan time.Time)

		maxDSSize := 4
		jobDB := jobsdb.HandleT{
			MaxDSSize: &maxDSSize,
			TriggerAddNewDS: func() <-chan time.Time {
				return triggerAddNewDS
			},
		}

		jobDB.Setup(jobsdb.ReadWrite, true, "gw", dbRetention, migrationMode, true, queryFilters)
		defer jobDB.TearDown()

		jobs := []*jobsdb.JobT{}
		jobs = append(jobs, genJobs(defaultWorkspaceID, customVal, 1, 1)...)
		jobs = append(jobs, genJobs(defaultWorkspaceID, customVal, 1, 2)...)
		jobs = append(jobs, genJobs(defaultWorkspaceID, customVal, 1, 3)...)
		jobs = append(jobs, genJobs(defaultWorkspaceID, customVal, 1, 10)...)
		require.NoError(t, jobDB.Store(jobs))
		triggerAddNewDS <- time.Now()
		triggerAddNewDS <- time.Now() //Second time, waits for the first loop to finish

		require.NoError(t, jobDB.Store(jobs))
		triggerAddNewDS <- time.Now()
		triggerAddNewDS <- time.Now() //Second time, waits for the first loop to finish

		eventCountLimit := 10
		eventLimitList := jobDB.GetUnprocessed(jobsdb.GetQueryParamsT{
			CustomValFilters: []string{customVal},
			JobsLimit:        100,
			EventsLimit:      eventCountLimit,
			ParameterFilters: []jobsdb.ParameterFilterT{},
		})

		requireSequential(t, eventLimitList)
		require.Equal(t, 3, len(eventLimitList))
	})
}

func TestMultiTenantLegacyGetAllJobs(t *testing.T) {
	initJobsDB()
	stats.Setup()
	dbRetention := time.Minute * 5
	migrationMode := ""

	triggerAddNewDS := make(chan time.Time)
	maxDSSize := 10
	jobDB := jobsdb.HandleT{
		MaxDSSize: &maxDSSize,
		TriggerAddNewDS: func() <-chan time.Time {
			return triggerAddNewDS
		},
	}

	customVal := "MTL"
	queryFilters := jobsdb.QueryFiltersT{
		CustomVal: true,
	}
	jobDB.Setup(jobsdb.ReadWrite, false, strings.ToLower(customVal), dbRetention, migrationMode, true, queryFilters)
	defer jobDB.TearDown()

	mtl := jobsdb.MultiTenantLegacy{HandleT: &jobDB}

	eventsPerJob := 10
	// Create 30 jobs
	jobs := genJobs(defaultWorkspaceID, customVal, 30, eventsPerJob)
	require.NoError(t, jobDB.Store(jobs))
	payloadSize, err := getPayloadSize(t, &jobDB, jobs[0])
	require.NoError(t, err)
	jobs = jobDB.GetUnprocessed(jobsdb.GetQueryParamsT{JobsLimit: 100}) // read to get Ids
	require.Equal(t, 30, len(jobs), "should get all 30 jobs")

	// Mark 1-10 as failed
	require.NoError(t, jobDB.UpdateJobStatus(genJobStatuses(jobs[0:10], jobsdb.Failed.State), []string{customVal}, []jobsdb.ParameterFilterT{}))

	// Mark 11-20 as waiting
	require.NoError(t, jobDB.UpdateJobStatus(genJobStatuses(jobs[10:20], jobsdb.Waiting.State), []string{customVal}, []jobsdb.ParameterFilterT{}))

	t.Run("GetAllJobs with large limits", func(t *testing.T) {
		params := jobsdb.GetQueryParamsT{JobsLimit: 30}
		allJobs := mtl.GetAllJobs(map[string]int{defaultWorkspaceID: 30}, params, 0)
		require.Equal(t, 30, len(allJobs), "should get all 30 jobs")

	})

	t.Run("GetAllJobs with only jobs limit", func(t *testing.T) {
		jobsLimit := 10
		params := jobsdb.GetQueryParamsT{JobsLimit: 10}
		allJobs := mtl.GetAllJobs(map[string]int{defaultWorkspaceID: jobsLimit}, params, 0)
		require.Truef(t, len(allJobs)-jobsLimit == 0, "should get %d jobs", jobsLimit)
	})

	t.Run("GetAllJobs with events limit", func(t *testing.T) {
		jobsLimit := 10
		params := jobsdb.GetQueryParamsT{JobsLimit: 10, EventsLimit: 3 * eventsPerJob}
		allJobs := mtl.GetAllJobs(map[string]int{defaultWorkspaceID: jobsLimit}, params, 0)
		require.Equal(t, 3, len(allJobs), "should get 3 jobs")
	})

	t.Run("GetAllJobs with events limit less than the events of the first job get one job", func(t *testing.T) {
		jobsLimit := 10
		params := jobsdb.GetQueryParamsT{JobsLimit: jobsLimit, EventsLimit: eventsPerJob - 1}
		allJobs := mtl.GetAllJobs(map[string]int{defaultWorkspaceID: jobsLimit}, params, 0)
		require.Equal(t, 1, len(allJobs), "should get 1 overflown job")
	})

	t.Run("GetAllJobs with payload limit", func(t *testing.T) {
		jobsLimit := 10
		params := jobsdb.GetQueryParamsT{JobsLimit: jobsLimit, PayloadSizeLimit: 3 * payloadSize}
		allJobs := mtl.GetAllJobs(map[string]int{defaultWorkspaceID: jobsLimit}, params, 0)
		require.Equal(t, 3, len(allJobs), "should get 3 jobs")
	})

	t.Run("GetAllJobs with payload limit less than the payload size should get one job", func(t *testing.T) {
		jobsLimit := 10
		params := jobsdb.GetQueryParamsT{JobsLimit: jobsLimit, PayloadSizeLimit: payloadSize - 1}
		allJobs := mtl.GetAllJobs(map[string]int{defaultWorkspaceID: jobsLimit}, params, 0)
		require.Equal(t, 1, len(allJobs), "should get 1 overflown job")
	})

}

func TestMultiTenantGetAllJobs(t *testing.T) {
	initJobsDB()
	stats.Setup()
	dbRetention := time.Minute * 5
	migrationMode := ""

	triggerAddNewDS := make(chan time.Time)
	maxDSSize := 10
	jobDB := jobsdb.HandleT{
		MaxDSSize: &maxDSSize,
		TriggerAddNewDS: func() <-chan time.Time {
			return triggerAddNewDS
		},
	}

	customVal := "MT"
	queryFilters := jobsdb.QueryFiltersT{
		CustomVal: true,
	}
	jobDB.Setup(jobsdb.ReadWrite, false, strings.ToLower(customVal), dbRetention, migrationMode, true, queryFilters)
	defer jobDB.TearDown()

	mtl := jobsdb.MultiTenantHandleT{HandleT: &jobDB}

	eventsPerJob := 10

	const (
		workspaceA = "workspaceA"
		workspaceB = "workspaceB"
		workspaceC = "workspaceC"
	)
	// Create 30 jobs in each of 3 workspaces
	jobs := genJobs(workspaceA, customVal, 30, eventsPerJob)
	require.NoError(t, jobDB.Store(jobs))
	payloadSize, err := getPayloadSize(t, &jobDB, jobs[0])
	require.NoError(t, err)
	jobs = genJobs(workspaceB, customVal, 30, eventsPerJob)
	require.NoError(t, jobDB.Store(jobs))
	jobs = genJobs(workspaceC, customVal, 30, eventsPerJob)
	require.NoError(t, jobDB.Store(jobs))

	allJobs := jobDB.GetUnprocessed(jobsdb.GetQueryParamsT{JobsLimit: 90}) // read to get all Ids
	require.Equal(t, 90, len(allJobs), "should get all 90 jobs")
	workspaceAJobs := filterWorkspaceJobs(allJobs, workspaceA)
	workspaceBJobs := filterWorkspaceJobs(allJobs, workspaceB)
	workspaceCJobs := filterWorkspaceJobs(allJobs, workspaceC)

	// Mark 1-10 as failed for each workspace
	require.NoError(t, jobDB.UpdateJobStatus(genJobStatuses(workspaceAJobs[0:10], jobsdb.Failed.State), []string{customVal}, []jobsdb.ParameterFilterT{}))
	require.NoError(t, jobDB.UpdateJobStatus(genJobStatuses(workspaceBJobs[0:10], jobsdb.Failed.State), []string{customVal}, []jobsdb.ParameterFilterT{}))
	require.NoError(t, jobDB.UpdateJobStatus(genJobStatuses(workspaceCJobs[0:10], jobsdb.Failed.State), []string{customVal}, []jobsdb.ParameterFilterT{}))

	// Mark 11-20 as waiting for each workspace
	require.NoError(t, jobDB.UpdateJobStatus(genJobStatuses(workspaceAJobs[10:20], jobsdb.Waiting.State), []string{customVal}, []jobsdb.ParameterFilterT{}))
	require.NoError(t, jobDB.UpdateJobStatus(genJobStatuses(workspaceBJobs[10:20], jobsdb.Waiting.State), []string{customVal}, []jobsdb.ParameterFilterT{}))
	require.NoError(t, jobDB.UpdateJobStatus(genJobStatuses(workspaceCJobs[10:20], jobsdb.Waiting.State), []string{customVal}, []jobsdb.ParameterFilterT{}))

	t.Run("GetAllJobs with large limits", func(t *testing.T) {
		workspaceLimits := map[string]int{
			workspaceA: 30,
			workspaceB: 30,
			workspaceC: 30,
		}
		params := jobsdb.GetQueryParamsT{JobsLimit: 90}
		allJobs := mtl.GetAllJobs(workspaceLimits, params, 100)
		require.Equal(t, 90, len(allJobs), "should get all 90 jobs")

	})

	t.Run("GetAllJobs with only jobs limit", func(t *testing.T) {
		jobsLimit := 10
		workspaceLimits := map[string]int{
			workspaceA: jobsLimit,
			workspaceB: jobsLimit,
			workspaceC: 0,
		}
		params := jobsdb.GetQueryParamsT{JobsLimit: jobsLimit * 2}
		allJobs := mtl.GetAllJobs(workspaceLimits, params, 100)
		require.Truef(t, len(allJobs)-2*jobsLimit == 0, "should get %d jobs", 2*jobsLimit)
	})

	t.Run("GetAllJobs with payload limit", func(t *testing.T) {
		workspaceLimits := map[string]int{
			workspaceA: 30,
			workspaceB: 30,
			workspaceC: 30,
		}
		params := jobsdb.GetQueryParamsT{JobsLimit: 90, PayloadSizeLimit: 6 * payloadSize}
		allJobs := mtl.GetAllJobs(workspaceLimits, params, 100)
		require.Equal(t, 6+3, len(allJobs), "should get limit jobs +1 (overflow) per workspace")
	})

	t.Run("GetAllJobs with payload limit less than the payload size should get one job", func(t *testing.T) {
		workspaceLimits := map[string]int{
			workspaceA: 30,
			workspaceB: 30,
			workspaceC: 30,
		}
		params := jobsdb.GetQueryParamsT{JobsLimit: 90, PayloadSizeLimit: payloadSize - 1}
		allJobs := mtl.GetAllJobs(workspaceLimits, params, 100)
		require.Equal(t, 3, len(allJobs), "should get limit+1 jobs")
	})

}

func filterWorkspaceJobs(jobs []*jobsdb.JobT, workspaceId string) []*jobsdb.JobT {
	filtered := []*jobsdb.JobT{}
	for i := range jobs {
		job := jobs[i]
		if job.WorkspaceId == workspaceId {
			filtered = append(filtered, job)
		}
	}
	return filtered
}

func requireSequential(t *testing.T, jobs []*jobsdb.JobT) {
	t.Helper()
	t.Log("job ids should be sequential")
	for i := 0; i < len(jobs)-1; i++ {
		require.Equal(t, jobs[i].JobID+1, jobs[i+1].JobID, "Gap detected: jobs[%d].id = %d +1 != jobs[%d].id = %d +1", i, jobs[i].JobID, i+1, jobs[i+1].JobID)
	}
}

func TestJobsDB_IncompatiblePayload(t *testing.T) {
	initJobsDB()
	stats.Setup()

	dbRetention := time.Minute * 5
	migrationMode := ""

	triggerAddNewDS := make(chan time.Time, 0)
	maxDSSize := 10
	jobDB := jobsdb.HandleT{
		MaxDSSize: &maxDSSize,
		TriggerAddNewDS: func() <-chan time.Time {
			return triggerAddNewDS
		},
	}
	queryFilters := jobsdb.QueryFiltersT{
		CustomVal: true,
	}

	jobDB.Setup(jobsdb.ReadWrite, false, "gw", dbRetention, migrationMode, true, queryFilters, []prebackup.Handler{})
	defer jobDB.TearDown()
	customVal := "MOCKDS"
	var sampleTestJob = jobsdb.JobT{
		Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
		EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient", "device_name":"FooBar\ufffd\u0000\ufffd\u000f\ufffd","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
		UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
		UUID:         uuid.Must(uuid.NewV4()),
		CustomVal:    customVal,
		WorkspaceId:  defaultWorkspaceID,
		EventCount:   1,
	}
	err := jobDB.StoreWithRetryEach([]*jobsdb.JobT{&sampleTestJob})
	for _, val := range err {
		require.Equal(t, "", val)
	}
	unprocessedList := jobDB.GetUnprocessed(jobsdb.GetQueryParamsT{
		CustomValFilters: []string{customVal},
		JobsLimit:        1,
		ParameterFilters: []jobsdb.ParameterFilterT{},
	})
	require.Equal(t, 1, len(unprocessedList))

	t.Run("validate fetched event", func(t *testing.T) {
		require.Equal(t, "MOCKDS", unprocessedList[0].CustomVal)
		require.Equal(t, defaultWorkspaceID, unprocessedList[0].WorkspaceId)
	})
}

func BenchmarkJobsdb(b *testing.B) {
	initJobsDB()
	stats.Setup()

	dbRetention := time.Minute * 5
	migrationMode := ""
	jobDB := jobsdb.HandleT{}
	queryFilters := jobsdb.QueryFiltersT{
		CustomVal: true,
	}

	jobDB.Setup(jobsdb.ReadWrite, false, "batch_rt", dbRetention, migrationMode, true, queryFilters, []prebackup.Handler{})
	defer jobDB.TearDown()

	customVal := "MOCKDS"

	b.Run("store and consume", func(b *testing.B) {
		expectedJobs := make([]jobsdb.JobT, b.N)
		for i := range expectedJobs {
			expectedJobs[i] = jobsdb.JobT{
				Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
				EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
				UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
				UUID:         uuid.Must(uuid.NewV4()),
				CustomVal:    customVal,
			}
		}

		g, _ := errgroup.WithContext(context.Background())
		g.Go(func() error {
			for i := range expectedJobs {
				err := jobDB.Store([]*jobsdb.JobT{&expectedJobs[i]})
				if err != nil {
					return err
				}
			}
			return nil
		})

		consumedJobs := make([]jobsdb.JobT, 0, len(expectedJobs))
		timeout := time.After(time.Second * time.Duration(len(expectedJobs)))
		g.Go(func() error {
			for {
				unprocessedList := jobDB.GetUnprocessed(jobsdb.GetQueryParamsT{
					CustomValFilters: []string{customVal},
					JobsLimit:        1,
				})

				status := make([]*jobsdb.JobStatusT, len(unprocessedList))
				for i, j := range unprocessedList {
					status[i] = &jobsdb.JobStatusT{
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

				err := jobDB.UpdateJobStatus(status, []string{customVal}, []jobsdb.ParameterFilterT{})
				require.NoError(b, err)

				for _, j := range unprocessedList {
					consumedJobs = append(consumedJobs, *j)
				}
				select {
				case <-time.After(5 * time.Millisecond):
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
	})
}

func BenchmarkLifecycle(b *testing.B) {
	initJobsDB()
	stats.Setup()

	jobDB := jobsdb.NewForReadWrite("test")
	defer jobDB.Close()

	const writeConcurrency = 10
	const newJobs = 100

	dsSize := writeConcurrency * newJobs
	triggerAddNewDS := make(chan time.Time)

	jobDB.MaxDSSize = &dsSize
	jobDB.TriggerAddNewDS = func() <-chan time.Time {
		return triggerAddNewDS
	}

	b.Run("Start, Work, Stop, Repeat", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			jobDB.Start()
			b.StopTimer()

			wg := sync.WaitGroup{}
			wg.Add(writeConcurrency)
			for j := 0; j < writeConcurrency; j++ {
				go func() {
					jobDB.Store(genJobs(defaultWorkspaceID, "", newJobs, 10))
					wg.Done()
				}()
			}
			wg.Wait()
			triggerAddNewDS <- time.Now()
			consume(b, jobDB, newJobs*writeConcurrency)
			b.StartTimer()
			jobDB.Stop()
		}
	})
}

func consume(t testing.TB, db *jobsdb.HandleT, count int) {
	t.Helper()

	unprocessedList := db.GetUnprocessed(jobsdb.GetQueryParamsT{
		JobsLimit: count,
	})

	status := make([]*jobsdb.JobStatusT, len(unprocessedList))
	for i, j := range unprocessedList {
		status[i] = &jobsdb.JobStatusT{
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

	err := db.UpdateJobStatus(status, []string{}, []jobsdb.ParameterFilterT{})
	require.NoError(t, err)
}

func getPayloadSize(t *testing.T, jobsdb jobsdb.JobsDB, job *jobsdb.JobT) (int64, error) {
	var size int64
	var tables []string
	tx := jobsdb.BeginGlobalTransaction()
	rows, err := tx.Query(fmt.Sprintf("SELECT tablename FROM pg_catalog.pg_tables where tablename like '%s_jobs_%%'", jobsdb.GetIdentifier()))
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		require.NoError(t, err)
		tables = append(tables, table)
	}
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
	_ = tx.Rollback()
	return size, err
}
