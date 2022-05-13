package cluster_test

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

	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/gofrs/uuid"
	"github.com/golang/mock/gomock"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/app/cluster"
	"github.com/rudderlabs/rudder-server/config"
	backendConfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/config/backend-config"
	mocksTransformer "github.com/rudderlabs/rudder-server/mocks/processor/transformer"
	mock_tenantstats "github.com/rudderlabs/rudder-server/mocks/services/multitenant"
	"github.com/rudderlabs/rudder-server/processor"
	"github.com/rudderlabs/rudder-server/processor/stash"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	routermanager "github.com/rudderlabs/rudder-server/router/manager"
	"github.com/rudderlabs/rudder-server/services/archiver"
	"github.com/rudderlabs/rudder-server/services/multitenant"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	utilTypes "github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/utils/types/servermode"
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

type reportingNOOP struct{}

func (*reportingNOOP) WaitForSetup(ctx context.Context, clientName string) {
}
func (*reportingNOOP) Report(metrics []*utilTypes.PUReportedMetric, txn *sql.Tx) {
}
func (*reportingNOOP) AddClient(ctx context.Context, c utilTypes.Config) {
}

const (
	WriteKeyEnabled           = "enabled-write-key"
	SourceIDEnabled           = "enabled-source"
	GADestinationID           = "did1"
	GADestinationDefinitionID = "gaid1"
)

var (
	workspaceID             = uuid.Must(uuid.NewV4()).String()
	gaDestinationDefinition = backendConfig.DestinationDefinitionT{ID: GADestinationDefinitionID, Name: "GA",
		DisplayName: "Google Analytics", Config: nil, ResponseRules: nil}
	sampleBackendConfig = backendConfig.ConfigT{
		Sources: []backendConfig.SourceT{
			{
				WorkspaceID: workspaceID,
				ID:          SourceIDEnabled,
				WriteKey:    WriteKeyEnabled,
				Enabled:     true,
				Destinations: []backendConfig.DestinationT{{ID: GADestinationID, Name: "ga dest",
					DestinationDefinition: gaDestinationDefinition, Enabled: true, IsProcessorEnabled: true}},
			},
		},
	}
)

func initJobsDB() {
	config.Load()
	logger.Init()
	stash.Init()
	admin.Init()
	jobsdb.Init()
	jobsdb.Init2()
	jobsdb.Init3()
	archiver.Init()
	stats.Setup()
	router.Init()
	router.InitRouterAdmin()
	batchrouter.Init()
	batchrouter.Init2()
	processor.Init()
	Init()
}

func TestDynamicClusterManager(t *testing.T) {
	initJobsDB()

	processor.SetFeaturesRetryAttempts(0)

	mockCtrl := gomock.NewController(t)
	mockMTI := mock_tenantstats.NewMockMultiTenantI(mockCtrl)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)
	mockTransformer := mocksTransformer.NewMockTransformer(mockCtrl)

	gwDB := jobsdb.NewForReadWrite("gw")
	defer gwDB.Close()
	rtDB := jobsdb.NewForReadWrite("rt")
	defer rtDB.Close()
	brtDB := jobsdb.NewForReadWrite("batch_rt")
	defer brtDB.Close()
	errDB := jobsdb.NewForReadWrite("proc_error")
	defer errDB.Close()

	clearDb := false
	ctx := context.Background()

	mtStat := &multitenant.MultitenantStatsT{
		RouterDBs: map[string]jobsdb.MultiTenantJobsDB{
			"rt":       &jobsdb.MultiTenantHandleT{HandleT: rtDB},
			"batch_rt": &jobsdb.MultiTenantLegacy{HandleT: brtDB},
		},
	}

	processor := processor.New(ctx, &clearDb, gwDB, rtDB, brtDB, errDB, mockMTI, &reportingNOOP{}, transientsource.NewEmptyService())
	processor.BackendConfig = mockBackendConfig
	processor.Transformer = mockTransformer
	mockBackendConfig.EXPECT().WaitForConfig(gomock.Any()).Times(1)
	mockBackendConfig.EXPECT().AccessToken().AnyTimes()
	mockTransformer.EXPECT().Setup().Times(1)

	tDb := &jobsdb.MultiTenantHandleT{HandleT: rtDB}
	rtFactory := &router.Factory{
		Reporting:        &reportingNOOP{},
		Multitenant:      mockMTI,
		BackendConfig:    mockBackendConfig,
		RouterDB:         tDb,
		ProcErrorDB:      errDB,
		TransientSources: transientsource.NewEmptyService(),
	}
	brtFactory := &batchrouter.Factory{
		Reporting:        &reportingNOOP{},
		Multitenant:      mockMTI,
		BackendConfig:    mockBackendConfig,
		RouterDB:         brtDB,
		ProcErrorDB:      errDB,
		TransientSources: transientsource.NewEmptyService(),
	}
	router := routermanager.New(rtFactory, brtFactory, mockBackendConfig)

	mockBackendConfig.EXPECT().Subscribe(gomock.Any(), gomock.Any()).Do(func(
		channel chan pubsub.DataEvent, topic backendConfig.Topic) {
		// on Subscribe, emulate a backend configuration event
		go func() { channel <- pubsub.DataEvent{Data: sampleBackendConfig, Topic: string(topic)} }()
	}).AnyTimes()
	mockMTI.EXPECT().UpdateWorkspaceLatencyMap(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	mockMTI.EXPECT().GetRouterPickupJobs(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any()).AnyTimes()

	provider := &mockModeProvider{modeCh: make(chan servermode.ChangeEvent)}
	dCM := &cluster.Dynamic{
		GatewayDB:     gwDB,
		RouterDB:      rtDB,
		BatchRouterDB: brtDB,
		ErrorDB:       errDB,

		Processor:       processor,
		Router:          router,
		Provider:        provider,
		MultiTenantStat: mtStat,
	}

	ctx, cancel := context.WithCancel(context.Background())

	wait := make(chan bool)
	go func() {
		err := dCM.Run(ctx)
		if err != nil {
			t.Logf("cluster runner stopped: %v", err)
		}
		close(wait)
	}()

	chACK := make(chan bool)
	provider.SendMode(servermode.NewChangeEvent(servermode.NormalMode, func(_ context.Context) error {
		close(chACK)
		return nil
	}))

	require.Eventually(t, func() bool {
		<-chACK
		return true
	}, time.Second, time.Millisecond)

	cancel()
	<-wait
}
