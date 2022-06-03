package manager

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/gomega"
	"github.com/ory/dockertest/v3"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/config"
	backendConfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/config/backend-config"
	mock_tenantstats "github.com/rudderlabs/rudder-server/mocks/services/multitenant"
	"github.com/rudderlabs/rudder-server/processor/stash"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	"github.com/rudderlabs/rudder-server/services/archiver"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	testutils "github.com/rudderlabs/rudder-server/utils/tests"
	utilTypes "github.com/rudderlabs/rudder-server/utils/types"
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
	gcsDestinationDefinition = backendConfig.DestinationDefinitionT{ID: GADestinationDefinitionID, Name: "GCS",
		DisplayName: "Google Analytics", Config: nil, ResponseRules: nil}
	sampleBackendConfig = backendConfig.ConfigT{
		Sources: []backendConfig.SourceT{
			{
				WorkspaceID: workspaceID,
				ID:          SourceIDEnabled,
				WriteKey:    WriteKeyEnabled,
				Enabled:     true,
				Destinations: []backendConfig.DestinationT{backendConfig.DestinationT{ID: GADestinationID,
					Name: "GCS DEst", DestinationDefinition: gcsDestinationDefinition, Enabled: true, IsProcessorEnabled: true}},
			},
			{
				WorkspaceID: workspaceID,
				ID:          SourceIDEnabled,
				WriteKey:    WriteKeyEnabled,
				Enabled:     true,
				Destinations: []backendConfig.DestinationT{backendConfig.DestinationT{ID: GADestinationID, Name: "ga dest",
					DestinationDefinition: gaDestinationDefinition, Enabled: true, IsProcessorEnabled: true}},
			},
		},
	}
)

func initRouter() {
	config.Load()
	logger.Init()
	stash.Init()
	admin.Init()
	jobsdb.Init()
	jobsdb.Init2()
	jobsdb.Init3()
	archiver.Init()
	router.Init()
	router.InitRouterAdmin()
	batchrouter.Init()
	batchrouter.Init2()
}

func TestRouterManager(t *testing.T) {
	RegisterTestingT(t)
	initRouter()
	stats.Setup()
	pkgLogger = logger.NewLogger().Child("router")
	c := make(chan bool)
	once := sync.Once{}

	asyncHelper := testutils.AsyncTestHelper{}
	asyncHelper.Setup()
	mockCtrl := gomock.NewController(t)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)
	mockMTI := mock_tenantstats.NewMockMultiTenantI(mockCtrl)

	mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendConfig.TopicBackendConfig).Do(func(
		channel chan pubsub.DataEvent, topic backendConfig.Topic) {
		// on Subscribe, emulate a backend configuration event
		go func() { channel <- pubsub.DataEvent{Data: sampleBackendConfig, Topic: string(topic)} }()
	}).AnyTimes()
	mockBackendConfig.EXPECT().AccessToken().AnyTimes()
	mockMTI.EXPECT().UpdateWorkspaceLatencyMap(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	mockMTI.EXPECT().GetRouterPickupJobs(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any()).Do(
		func(destType string, noOfWorkers int, routerTimeOut time.Duration, jobQueryBatchSize int, timeGained float64) {
			once.Do(func() {
				close(c)
			})
		}).AnyTimes()

	rtDB := jobsdb.NewForReadWrite("rt")
	brtDB := jobsdb.NewForReadWrite("batch_rt")
	errDB := jobsdb.NewForReadWrite("proc_error")
	defer rtDB.Close()
	defer brtDB.Close()
	defer errDB.Close()
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
	r := New(rtFactory, brtFactory, mockBackendConfig)

	for i := 0; i < 5; i++ {
		rtDB.Start()
		brtDB.Start()
		errDB.Start()
		r.Start()
		<-c
		r.Stop()
		rtDB.Stop()
		brtDB.Stop()
		errDB.Stop()
		c = make(chan bool)
		once = sync.Once{}
	}
}
