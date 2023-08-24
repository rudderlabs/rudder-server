package processor

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	transformationdebugger "github.com/rudderlabs/rudder-server/services/debugger/transformation"

	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	. "github.com/onsi/gomega"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/enterprise/reporting"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transientsource"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	mocksTransformer "github.com/rudderlabs/rudder-server/mocks/processor/transformer"
	"github.com/rudderlabs/rudder-server/processor/stash"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

var (
	hold  bool
	db    *sql.DB
	dbDsn = "root@tcp(127.0.0.1:3306)/service"
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
		log.Printf("Could not connect to docker: %s", err)
		return 1
	}

	database := "jobsdb"
	// pulls an image, creates a container based on it and runs it
	resourcePostgres, err := pool.Run("postgres", "15-alpine", []string{
		"POSTGRES_PASSWORD=password",
		"POSTGRES_DB=" + database,
		"POSTGRES_USER=rudder",
	})
	if err != nil {
		log.Printf("Could not start resource: %s", err)
		return 1
	}
	defer func() {
		if err := pool.Purge(resourcePostgres); err != nil {
			log.Printf("Could not purge resource: %s", err)
		}
	}()

	dbDsn = fmt.Sprintf("postgres://rudder:password@localhost:%s/%s?sslmode=disable", resourcePostgres.GetPort("5432/tcp"), database)
	log.Println("DB_DSN:", dbDsn)
	_ = os.Setenv("JOBS_DB_DB_NAME", database)
	_ = os.Setenv("JOBS_DB_HOST", "localhost")
	_ = os.Setenv("JOBS_DB_NAME", "jobsdb")
	_ = os.Setenv("JOBS_DB_USER", "rudder")
	_ = os.Setenv("JOBS_DB_PASSWORD", "password")
	_ = os.Setenv("JOBS_DB_PORT", resourcePostgres.GetPort("5432/tcp"))

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		var err error
		db, err = sql.Open("postgres", dbDsn)
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		log.Printf("Could not connect to docker: %s", err)
		return 1
	}

	code := m.Run()
	blockOnHold()

	return code
}

func blockOnHold() {
	if !hold {
		return
	}

	log.Println("Test on hold, before cleanup")
	log.Println("Press Ctrl+C to exit")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	<-c
}

func initJobsDB() {
	config.Reset()
	logger.Reset()
	stash.Init()
	admin.Init()
}

func genJobs(customVal string, jobCount, eventsPerJob int) []*jobsdb.JobT {
	js := make([]*jobsdb.JobT, jobCount)
	for i := range js {
		js[i] = &jobsdb.JobT{
			Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":"123\u0000"}`),
			EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track\u0000","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
			UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
			UUID:         uuid.New(),
			CustomVal:    customVal,
			EventCount:   eventsPerJob,
		}
	}
	return js
}

func TestProcessorManager(t *testing.T) {
	initJobsDB()
	mockCtrl := gomock.NewController(t)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)
	mockTransformer := mocksTransformer.NewMockTransformer(mockCtrl)
	mockRsourcesService := rsources.NewMockJobService(mockCtrl)

	RegisterTestingT(t)
	config.Reset()
	c := config.New()
	c.Set("JobsDB.maxDSSize", 10)
	// tempDB is created to observe/manage the GW DB from the outside without touching the actual GW DB.
	tempDB := jobsdb.NewForWrite(
		"gw",
		jobsdb.WithConfig(c),
	)
	require.NoError(t, tempDB.Start())
	defer tempDB.TearDown()

	customVal := "GW"
	unprocessedListEmpty, err := tempDB.GetUnprocessed(context.Background(), jobsdb.GetQueryParams{
		CustomValFilters: []string{customVal},
		JobsLimit:        1,
		ParameterFilters: []jobsdb.ParameterFilterT{},
	})
	require.NoError(t, err, "GetUnprocessed failed")
	require.Equal(t, 0, len(unprocessedListEmpty.Jobs))

	jobCountPerDS := 10
	eventsPerJob := 10
	err = tempDB.Store(context.Background(), genJobs(customVal, jobCountPerDS, eventsPerJob))
	require.NoError(t, err)

	gwDB := jobsdb.NewForReadWrite("gw",
		jobsdb.WithConfig(c),
	)
	defer gwDB.Close()
	rtDB := jobsdb.NewForReadWrite("rt",
		jobsdb.WithConfig(c),
	)
	defer rtDB.Close()
	brtDB := jobsdb.NewForReadWrite("batch_rt",
		jobsdb.WithConfig(c),
	)
	defer brtDB.Close()
	readErrDB := jobsdb.NewForRead("proc_error",
		jobsdb.WithConfig(c),
	)
	defer readErrDB.Close()
	writeErrDB := jobsdb.NewForWrite("proc_error",
		jobsdb.WithConfig(c),
	)
	require.NoError(t, writeErrDB.Start())
	defer writeErrDB.TearDown()
	eschDB := jobsdb.NewForReadWrite("esch",
		jobsdb.WithConfig(c),
	)
	defer eschDB.Close()
	archDB := jobsdb.NewForReadWrite("archival",
		jobsdb.WithConfig(c),
	)
	defer archDB.Close()

	clearDb := false
	ctx := context.Background()

	processor := New(
		ctx,
		&clearDb,
		gwDB,
		rtDB,
		brtDB,
		readErrDB,
		writeErrDB,
		eschDB,
		archDB,
		&reporting.NOOP{},
		transientsource.NewEmptyService(),
		fileuploader.NewDefaultProvider(),
		mockRsourcesService,
		destinationdebugger.NewNoOpService(),
		transformationdebugger.NewNoOpService(),
		func(m *LifecycleManager) {
			m.Handle.config.enablePipelining = false
			m.Handle.config.featuresRetryMaxAttempts = 0
		})

	t.Run("jobs are already there in GW DB before processor starts", func(t *testing.T) {
		require.NoError(t, gwDB.Start())
		defer gwDB.Stop()
		require.NoError(t, rtDB.Start())
		defer rtDB.Stop()
		require.NoError(t, brtDB.Start())
		defer brtDB.Stop()
		require.NoError(t, readErrDB.Start())
		defer readErrDB.Stop()
		mockBackendConfig.EXPECT().Subscribe(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
				ch := make(chan pubsub.DataEvent, 1)
				ch <- pubsub.DataEvent{Data: map[string]backendconfig.ConfigT{sampleWorkspaceID: sampleBackendConfig}, Topic: string(topic)}
				close(ch)
				return ch
			},
		)
		mockBackendConfig.EXPECT().WaitForConfig(gomock.Any()).Times(1)
		processor.Handle.transformerFeatures = json.RawMessage(defaultTransformerFeatures)
		mockRsourcesService.EXPECT().IncrementStats(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), rsources.Stats{Out: 10}).Times(1)
		processor.BackendConfig = mockBackendConfig
		processor.Handle.transformer = mockTransformer
		require.NoError(t, processor.Start())
		defer processor.Stop()
		Eventually(func() int {
			res, err := tempDB.GetUnprocessed(context.Background(), jobsdb.GetQueryParams{
				CustomValFilters: []string{customVal},
				JobsLimit:        20,
				ParameterFilters: []jobsdb.ParameterFilterT{},
			})
			require.NoError(t, err)
			return len(res.Jobs)
		}, 10*time.Minute, 100*time.Millisecond).Should(Equal(0))
	})

	t.Run("adding more jobs after the processor is already running", func(t *testing.T) {
		require.NoError(t, gwDB.Start())
		defer gwDB.Stop()
		require.NoError(t, eschDB.Start())
		defer eschDB.Stop()
		require.NoError(t, archDB.Start())
		defer archDB.Stop()
		require.NoError(t, rtDB.Start())
		defer rtDB.Stop()
		require.NoError(t, brtDB.Start())
		defer brtDB.Stop()
		require.NoError(t, readErrDB.Start())
		defer readErrDB.Stop()
		mockBackendConfig.EXPECT().Subscribe(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
				ch := make(chan pubsub.DataEvent, 1)
				ch <- pubsub.DataEvent{Data: map[string]backendconfig.ConfigT{sampleWorkspaceID: sampleBackendConfig}, Topic: string(topic)}
				close(ch)
				return ch
			},
		)

		mockBackendConfig.EXPECT().WaitForConfig(gomock.Any()).Times(1)
		processor.Handle.transformerFeatures = json.RawMessage(defaultTransformerFeatures)
		mockRsourcesService.EXPECT().IncrementStats(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), rsources.Stats{Out: 10}).Times(1)

		require.NoError(t, processor.Start())
		err = tempDB.Store(context.Background(), genJobs(customVal, jobCountPerDS, eventsPerJob))
		require.NoError(t, err)
		unprocessedListEmpty, err = tempDB.GetUnprocessed(context.Background(), jobsdb.GetQueryParams{
			CustomValFilters: []string{customVal},
			JobsLimit:        20,
			ParameterFilters: []jobsdb.ParameterFilterT{},
		})
		require.NoError(t, err, "failed to get unprocessed jobs")
		Eventually(func() int {
			res, err := tempDB.GetUnprocessed(context.Background(), jobsdb.GetQueryParams{
				CustomValFilters: []string{customVal},
				JobsLimit:        20,
				ParameterFilters: []jobsdb.ParameterFilterT{},
			})
			require.NoError(t, err)
			return len(res.Jobs)
		}, time.Minute, 100*time.Millisecond).Should(Equal(0))
		processor.Stop()
	})
}
