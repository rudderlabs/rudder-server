package jobsdb_test

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/ory/dockertest"
	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/archiver"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
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

func TestJobsDB(t *testing.T) {
	initJobsDB()
	stats.Setup()

	dbRetention := time.Minute * 5
	migrationMode := ""
	jobDB := jobsdb.HandleT{}
	queryFilters := jobsdb.QueryFiltersT{
		CustomVal: true,
	}

	jobDB.Setup(jobsdb.ReadWrite, false, "batch_rt", dbRetention, migrationMode, true, queryFilters)
	defer jobDB.TearDown()

	customVal := "MOCKDS"
	var sampleTestJob = jobsdb.JobT{
		Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
		EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
		UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
		UUID:         uuid.NewV4(),
		CustomVal:    customVal,
	}

	unprocessedListEmpty := jobDB.GetUnprocessed(jobsdb.GetQueryParamsT{
		CustomValFilters: []string{customVal},
		Count:            1,
		ParameterFilters: []jobsdb.ParameterFilterT{},
	})

	require.Equal(t, 0, len(unprocessedListEmpty))
	err := jobDB.Store([]*jobsdb.JobT{&sampleTestJob})
	require.NoError(t, err)

	unprocessedList := jobDB.GetUnprocessed(jobsdb.GetQueryParamsT{
		CustomValFilters: []string{customVal},
		Count:            1,
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
	}

	err = jobDB.UpdateJobStatus([]*jobsdb.JobStatusT{&status}, []string{customVal}, []jobsdb.ParameterFilterT{})
	require.NoError(t, err)

	unprocessedList = jobDB.GetUnprocessed(jobsdb.GetQueryParamsT{
		CustomValFilters: []string{customVal},
		Count:            1,
		ParameterFilters: []jobsdb.ParameterFilterT{},
	})

	require.Equal(t, 0, len(unprocessedList))
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

	jobDB.Setup(jobsdb.ReadWrite, false, "batch_rt", dbRetention, migrationMode, true, queryFilters)
	defer jobDB.TearDown()

	customVal := "MOCKDS"

	b.Run("store and consume", func(b *testing.B) {
		expectedJobs := make([]jobsdb.JobT, b.N)
		for i := range expectedJobs {
			expectedJobs[i] = jobsdb.JobT{
				Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
				EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
				UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
				UUID:         uuid.NewV4(),
				CustomVal:    customVal,
			}
		}

		g, _ := errgroup.WithContext(context.Background())
		g.Go(func() error {
			for _, j := range expectedJobs {
				err := jobDB.Store([]*jobsdb.JobT{&j})
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
					Count:            1,
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
		for i, actualJob := range consumedJobs {
			expectedJob := expectedJobs[i]
			require.Equal(b, expectedJob.UUID, actualJob.UUID)
			require.Equal(b, expectedJob.UserID, actualJob.UserID)
			require.Equal(b, expectedJob.CustomVal, actualJob.CustomVal)
			require.JSONEq(b, string(expectedJob.Parameters), string(actualJob.Parameters))
			require.JSONEq(b, string(expectedJob.EventPayload), string(actualJob.EventPayload))
		}
	})
}
