package manager

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	"github.com/google/uuid"
	. "github.com/onsi/gomega"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/admin"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/enterprise/reporting"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	"github.com/rudderlabs/rudder-server/router/throttler"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	testutils "github.com/rudderlabs/rudder-server/utils/tests"
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
	resourcePostgres, err := pool.Run("postgres", "15-alpine", []string{
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

const (
	WriteKeyEnabled           = "enabled-write-key"
	SourceIDEnabled           = "enabled-source"
	GADestinationID           = "did1"
	GADestinationDefinitionID = "gaid1"
)

var (
	workspaceID             = uuid.New().String()
	gaDestinationDefinition = backendconfig.DestinationDefinitionT{
		ID: GADestinationDefinitionID, Name: "GA",
		DisplayName: "Google Analytics", Config: nil, ResponseRules: nil,
	}
	gcsDestinationDefinition = backendconfig.DestinationDefinitionT{
		ID: GADestinationDefinitionID, Name: "GCS",
		DisplayName: "Google Analytics", Config: nil, ResponseRules: nil,
	}
	sampleBackendConfig = backendconfig.ConfigT{
		WorkspaceID: workspaceID,
		Sources: []backendconfig.SourceT{
			{
				WorkspaceID: workspaceID,
				ID:          SourceIDEnabled,
				WriteKey:    WriteKeyEnabled,
				Enabled:     true,
				Destinations: []backendconfig.DestinationT{{
					ID:   GADestinationID,
					Name: "GCS DEst", DestinationDefinition: gcsDestinationDefinition, Enabled: true, IsProcessorEnabled: true,
				}},
			},
			{
				WorkspaceID: workspaceID,
				ID:          SourceIDEnabled,
				WriteKey:    WriteKeyEnabled,
				Enabled:     true,
				Destinations: []backendconfig.DestinationT{{
					ID: GADestinationID, Name: "ga dest",
					DestinationDefinition: gaDestinationDefinition, Enabled: true, IsProcessorEnabled: true,
				}},
			},
		},
	}
)

func initRouter() {
	config.Reset()
	logger.Reset()
	admin.Init()
}

func TestRouterManager(t *testing.T) {
	RegisterTestingT(t)
	initRouter()
	config.Set("Router.isolationMode", "none")
	defer config.Reset()

	asyncHelper := testutils.AsyncTestHelper{}
	asyncHelper.Setup()
	mockCtrl := gomock.NewController(t)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)
	mockRsourcesService := rsources.NewMockJobService(mockCtrl)

	mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicBackendConfig).DoAndReturn(func(
		ctx context.Context, topic backendconfig.Topic,
	) pubsub.DataChannel {
		// on Subscribe, emulate a backend configuration event

		ch := make(chan pubsub.DataEvent, 1)
		ch <- pubsub.DataEvent{Data: map[string]backendconfig.ConfigT{workspaceID: sampleBackendConfig}, Topic: string(topic)}
		go func() {
			<-ctx.Done()
			close(ch)
		}()
		return ch
	}).AnyTimes()
	mockBackendConfig.EXPECT().AccessToken().AnyTimes()

	rtDB := jobsdb.NewForReadWrite("rt")
	mockRtDB := &mockJobsDB{JobsDB: rtDB}
	brtDB := jobsdb.NewForReadWrite("batch_rt")
	errDB := jobsdb.NewForReadWrite("proc_error")
	defer rtDB.Close()
	defer brtDB.Close()
	defer errDB.Close()
	rtFactory := &router.Factory{
		Logger:                     logger.NOP,
		Reporting:                  &reporting.NOOP{},
		BackendConfig:              mockBackendConfig,
		RouterDB:                   mockRtDB,
		ProcErrorDB:                errDB,
		TransientSources:           transientsource.NewEmptyService(),
		RsourcesService:            mockRsourcesService,
		ThrottlerFactory:           throttler.NewNoOpThrottlerFactory(),
		TransformerFeaturesService: transformer.NewNoOpService(),
	}
	brtFactory := &batchrouter.Factory{
		Reporting:        &reporting.NOOP{},
		BackendConfig:    mockBackendConfig,
		RouterDB:         brtDB,
		ProcErrorDB:      errDB,
		TransientSources: transientsource.NewEmptyService(),
		RsourcesService:  mockRsourcesService,
	}
	r := New(rtFactory, brtFactory, mockBackendConfig, logger.NewLogger())

	for i := 0; i < 5; i++ {
		require.NoError(t, rtDB.Start())
		require.NoError(t, brtDB.Start())
		require.NoError(t, errDB.Start())
		require.NoError(t, r.Start())
		require.Eventually(t, func() bool {
			return mockRtDB.called.Load()
		}, 5*time.Second, 100*time.Millisecond)
		r.Stop()
		rtDB.Stop()
		brtDB.Stop()
		errDB.Stop()
		mockRtDB.called.Store(false)
	}
}

type mockJobsDB struct {
	called atomic.Bool
	jobsdb.JobsDB
}

func (m *mockJobsDB) GetToProcess(ctx context.Context, params jobsdb.GetQueryParams, more jobsdb.MoreToken) (*jobsdb.MoreJobsResult, error) {
	m.called.Store(true)
	return m.JobsDB.GetToProcess(ctx, params, more)
}
