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

	"github.com/gofrs/uuid"
	"github.com/golang/mock/gomock"
	"github.com/ory/dockertest"
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
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	utilTypes "github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/utils/types/servermode"
	"github.com/stretchr/testify/require"
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
	workspaceID = uuid.Must(uuid.NewV4()).String()
	gaDestinationDefinition = backendConfig.DestinationDefinitionT{ID: GADestinationDefinitionID, Name: "GA",
		DisplayName: "Google Analytics", Config: nil, ResponseRules: nil}
	sampleBackendConfig = backendConfig.ConfigT{
		Sources: []backendConfig.SourceT{
			{
				WorkspaceID:  workspaceID,
				ID:           SourceIDEnabled,
				WriteKey:     WriteKeyEnabled,
				Enabled:      true,
				Destinations: []backendConfig.DestinationT{backendConfig.DestinationT{ID: GADestinationID, Name: "ga dest",
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
	router.Init2()
	batchrouter.Init()
	batchrouter.Init2()
	processor.Init()
	cluster.Init()
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

	processor := processor.New(ctx, &clearDb, gwDB, rtDB, brtDB, errDB)
	processor.BackendConfig = mockBackendConfig
	processor.Transformer = mockTransformer
	mockBackendConfig.EXPECT().Subscribe(gomock.Any(), gomock.Any()).Times(1)
	mockBackendConfig.EXPECT().WaitForConfig(gomock.Any()).Times(1)
	mockTransformer.EXPECT().Setup().Times(1)

	tDb := &jobsdb.MultiTenantHandleT{HandleT: rtDB}
	router := routermanager.New(ctx, brtDB, errDB, tDb)
	router.BackendConfig = mockBackendConfig
	router.ReportingI = &reportingNOOP{}
	router.MultitenantStats = mockMTI

	mockBackendConfig.EXPECT().Subscribe(gomock.Any(), gomock.Any()).Do(func(
		channel chan utils.DataEvent, topic backendConfig.Topic) {
		// on Subscribe, emulate a backend configuration event
		go func() { channel <- utils.DataEvent{Data: sampleBackendConfig, Topic: string(topic)} }()
	}).AnyTimes()
	mockMTI.EXPECT().UpdateWorkspaceLatencyMap(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	mockMTI.EXPECT().GetRouterPickupJobs(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any()).AnyTimes()

	provider := &mockModeProvider{ch: make(chan servermode.ModeAck)}
	dCM := &cluster.Dynamic{
		GatewayDB:     gwDB,
		RouterDB:      rtDB,
		BatchRouterDB: brtDB,
		ErrorDB:       errDB,

		Processor: processor,
		Router:    router,
		Provider:  provider,
	}
	dCM.Setup()

	ctx, cancel := context.WithCancel(context.Background())

	wait := make(chan bool)
	go func() {
		dCM.Run(ctx)
		close(wait)
	}()

	chACK := make(chan bool)
	provider.SendMode(servermode.WithACK(servermode.NormalMode, func() {
		close(chACK)
	}))

	require.Eventually(t, func() bool {
		<-chACK
		return true
	}, time.Second, time.Millisecond)

	cancel()
	<-wait
}
