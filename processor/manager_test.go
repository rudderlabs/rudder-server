package processor

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/enterprise/trackedusers"

	"github.com/google/uuid"
	. "github.com/onsi/gomega"
	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/admin"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/enterprise/reporting"
	"github.com/rudderlabs/rudder-server/internal/enricher"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	mocksTransformer "github.com/rudderlabs/rudder-server/mocks/processor/transformer"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	transformationdebugger "github.com/rudderlabs/rudder-server/services/debugger/transformation"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

func initJobsDB() {
	config.Reset()
	logger.Reset()
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
			WorkspaceId:  "workspaceID",
		}
	}
	return js
}

func TestProcessorManager(t *testing.T) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	resourcePostgres, err := postgres.Setup(pool, t)
	require.NoError(t, err)

	t.Log("DB_DSN:", resourcePostgres.DBDsn)
	t.Setenv("JOBS_DB_DB_NAME", resourcePostgres.Database)
	t.Setenv("JOBS_DB_HOST", resourcePostgres.Host)
	t.Setenv("JOBS_DB_NAME", resourcePostgres.Database)
	t.Setenv("JOBS_DB_USER", resourcePostgres.User)
	t.Setenv("JOBS_DB_PASSWORD", resourcePostgres.Password)
	t.Setenv("JOBS_DB_PORT", resourcePostgres.Port)

	initJobsDB()
	mockCtrl := gomock.NewController(t)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)
	mockTransformer := mocksTransformer.NewMockTransformer(mockCtrl)
	mockRsourcesService := rsources.NewMockJobService(mockCtrl)

	RegisterTestingT(t)
	config.Reset()
	c := config.New()
	c.Set("JobsDB.maxDSSize", 10)

	statStore, err := memstats.New()
	require.NoError(t, err)

	// tempDB is created to observe/manage the GW DB from the outside without touching the actual GW DB.
	tempDB := jobsdb.NewForWrite(
		"gw",
		jobsdb.WithConfig(c),
		jobsdb.WithStats(statStore),
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

	t.Log("now is a second after jobs' receivedAt time")
	now, err := time.Parse(time.RFC3339Nano, "2021-06-06T20:26:40.598+05:30")
	require.NoError(t, err, "parsing now")

	statsStore, err := memstats.New(memstats.WithNow(func() time.Time {
		return now
	}))
	require.NoError(t, err)

	jobCountPerDS := 10
	eventsPerJob := 10
	err = tempDB.Store(context.Background(), genJobs(customVal, jobCountPerDS, eventsPerJob))
	require.NoError(t, err)

	gwDB := jobsdb.NewForReadWrite("gw",
		jobsdb.WithConfig(c),
		jobsdb.WithStats(stats.NOP),
	)
	defer gwDB.Close()
	rtDB := jobsdb.NewForReadWrite("rt",
		jobsdb.WithConfig(c),
		jobsdb.WithStats(stats.NOP),
	)
	defer rtDB.Close()
	brtDB := jobsdb.NewForReadWrite("batch_rt",
		jobsdb.WithConfig(c),
		jobsdb.WithStats(stats.NOP),
	)
	defer brtDB.Close()
	readErrDB := jobsdb.NewForRead("proc_error",
		jobsdb.WithConfig(c),
		jobsdb.WithStats(stats.NOP),
	)
	defer readErrDB.Close()
	writeErrDB := jobsdb.NewForWrite("proc_error",
		jobsdb.WithConfig(c),
		jobsdb.WithStats(stats.NOP),
	)
	require.NoError(t, writeErrDB.Start())
	defer writeErrDB.TearDown()
	eschDB := jobsdb.NewForReadWrite("esch",
		jobsdb.WithConfig(c),
		jobsdb.WithStats(stats.NOP),
	)
	defer eschDB.Close()
	archDB := jobsdb.NewForReadWrite("archival",
		jobsdb.WithConfig(c),
		jobsdb.WithStats(stats.NOP),
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
		transformer.NewNoOpService(),
		destinationdebugger.NewNoOpService(),
		transformationdebugger.NewNoOpService(),
		[]enricher.PipelineEnricher{},
		trackedusers.NewNoopDataCollector(),
		WithStats(statsStore),
		func(m *LifecycleManager) {
			m.Handle.config.enablePipelining = false
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

		require.Equal(t,
			statsStore.GetByName("processor.event_pickup_lag_seconds"),
			[]memstats.Metric{{
				Name: "processor.event_pickup_lag_seconds",
				Tags: map[string]string{
					"sourceId":    "sourceID",
					"workspaceId": "workspaceID",
				},
				Durations: lo.Times(jobCountPerDS, func(i int) time.Duration {
					return time.Second
				}),
			}},
			"correctly capture the lag between job's receivedAt and now",
		)
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
