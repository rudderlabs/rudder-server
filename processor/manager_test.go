package processor

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"github.com/gofrs/uuid"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/gomega"
	"github.com/ory/dockertest"
	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/config/backend-config"
	mocksTransformer "github.com/rudderlabs/rudder-server/mocks/processor/transformer"
	"github.com/rudderlabs/rudder-server/processor/stash"
	"github.com/rudderlabs/rudder-server/services/archiver"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/require"
	"log"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"
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
	stash.Init()
	admin.Init()
	jobsdb.Init()
	jobsdb.Init2()
	jobsdb.Init3()
	archiver.Init()
	Init()
	//dedup.Init()
	//misc.Init()
	//integrations.Init()

}

func genJobs(customVal string, jobCount, eventsPerJob int) []*jobsdb.JobT {
	js := make([]*jobsdb.JobT, jobCount)
	for i := range js {
		js[i] = &jobsdb.JobT{
			Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
			EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
			UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
			UUID:         uuid.Must(uuid.NewV4()),
			CustomVal:    customVal,
			EventCount:   eventsPerJob,
		}
	}
	return js
}

func TestProcessorManager(t *testing.T) {
	initJobsDB()
	stats.Setup()
	mockCtrl := gomock.NewController(t)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)
	mockBackendConfig.EXPECT().Subscribe(gomock.Any(), gomock.Any())
	mockBackendConfig.EXPECT().WaitForConfig(gomock.Any())
	mockTransformer := mocksTransformer.NewMockTransformer(mockCtrl)
	mockTransformer.EXPECT().Setup().Times(1)
	SetFeaturesRetryAttempts(0)
	enablePipelining = false

	dbRetention := time.Minute * 5
	migrationMode := ""
	triggerAddNewDS := make(chan time.Time, 0)
	maxDSSize := 10
	gwJobDB := jobsdb.HandleT{
		MaxDSSize: &maxDSSize,
		TriggerAddNewDS: func() <-chan time.Time {
			return triggerAddNewDS
		},
	}
	queryFilters := jobsdb.QueryFiltersT{
		CustomVal: true,
	}
	gwJobDB.Setup(jobsdb.Write, false, "gw", dbRetention, migrationMode, true, queryFilters)
	defer gwJobDB.TearDown()

	ctx:= context.Background()
	processor := NewProcessor(ctx)
	processor.transformer = mockTransformer
	processor.backendConfig = mockBackendConfig

	customVal := "MOCKDS"
	var sampleTestJob = jobsdb.JobT{
		Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
		EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
		UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
		UUID:         uuid.Must(uuid.NewV4()),
		CustomVal:    customVal,
	}

	unprocessedListEmpty := gwJobDB.GetUnprocessed(jobsdb.GetQueryParamsT{
		CustomValFilters: []string{customVal},
		JobCount:         1,
		ParameterFilters: []jobsdb.ParameterFilterT{},
	})
	require.Equal(t, 0, len(unprocessedListEmpty))
	err := gwJobDB.Store([]*jobsdb.JobT{&sampleTestJob})
	require.NoError(t, err)

	jobCountPerDS := 10
	eventsPerJob := 10
	err = gwJobDB.Store(genJobs(customVal, jobCountPerDS, eventsPerJob))
	require.NoError(t, err)
	unprocessedListEmpty = gwJobDB.GetUnprocessed(jobsdb.GetQueryParamsT{
		CustomValFilters: []string{customVal},
		JobCount:         20,
		ParameterFilters: []jobsdb.ParameterFilterT{},
	})

	require.Equal(t, 11, len(unprocessedListEmpty))
	processor.StartNew()
	unprocessedListEmpty = gwJobDB.GetUnprocessed(jobsdb.GetQueryParamsT{
		CustomValFilters: []string{customVal},
		JobCount:         20,
		ParameterFilters: []jobsdb.ParameterFilterT{},
	})

	require.Equal(t, 11, len(unprocessedListEmpty))

	t.Run("adding more jobs after the processor is running", func(t *testing.T) {
		processor.StartNew()
		err = gwJobDB.Store(genJobs(customVal, jobCountPerDS, eventsPerJob))
		require.NoError(t, err)
		Eventually(t, func() int {return len(gwJobDB.GetUnprocessed(jobsdb.GetQueryParamsT{
			CustomValFilters: []string{customVal},
			JobCount:         20,
			ParameterFilters: []jobsdb.ParameterFilterT{},
		}))}).Should(Equal(0))
	})
}
