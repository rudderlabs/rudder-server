package cluster_test

import (
	"context"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/enterprise/trackedusers"

	"go.uber.org/mock/gomock"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/app/cluster"
	arc "github.com/rudderlabs/rudder-server/archiver"
	backendConfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/enterprise/reporting"
	"github.com/rudderlabs/rudder-server/internal/enricher"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	mock_jobs_forwarder "github.com/rudderlabs/rudder-server/mocks/jobs-forwarder"
	mocksTransformer "github.com/rudderlabs/rudder-server/mocks/processor/transformer"
	"github.com/rudderlabs/rudder-server/processor"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	routermanager "github.com/rudderlabs/rudder-server/router/manager"
	"github.com/rudderlabs/rudder-server/router/throttler"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	transformationdebugger "github.com/rudderlabs/rudder-server/services/debugger/transformation"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/utils/types/servermode"
)

const (
	WriteKeyEnabled           = "enabled-write-key"
	SourceIDEnabled           = "enabled-source"
	GADestinationID           = "did1"
	GADestinationDefinitionID = "gaid1"
)

func initJobsDB() {
	config.Reset()
	logger.Reset()
	admin.Init()
	Init()
}

func TestDynamicClusterManager(t *testing.T) {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

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

	var (
		workspaceID             = uuid.New().String()
		gaDestinationDefinition = backendConfig.DestinationDefinitionT{
			ID: GADestinationDefinitionID, Name: "GA",
			DisplayName: "Google Analytics", Config: nil, ResponseRules: nil,
		}
		sampleBackendConfig = backendConfig.ConfigT{
			WorkspaceID: workspaceID,
			Sources: []backendConfig.SourceT{
				{
					WorkspaceID: workspaceID,
					ID:          SourceIDEnabled,
					WriteKey:    WriteKeyEnabled,
					Enabled:     true,
					Destinations: []backendConfig.DestinationT{{
						ID: GADestinationID, Name: "ga dest",
						DestinationDefinition: gaDestinationDefinition, Enabled: true, IsProcessorEnabled: true,
					}},
				},
			},
		}
	)

	mockCtrl := gomock.NewController(t)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)
	mockTransformer := mocksTransformer.NewMockTransformer(mockCtrl)
	mockRsourcesService := rsources.NewMockJobService(mockCtrl)

	gwDB := jobsdb.NewForReadWrite("gw", jobsdb.WithStats(stats.NOP))
	defer gwDB.TearDown()
	eschDB := jobsdb.NewForReadWrite("esch", jobsdb.WithStats(stats.NOP))
	defer eschDB.TearDown()
	archiveDB := jobsdb.NewForReadWrite("archive", jobsdb.WithStats(stats.NOP))
	defer archiveDB.TearDown()
	rtDB := jobsdb.NewForReadWrite("rt", jobsdb.WithStats(stats.NOP))
	defer rtDB.TearDown()
	brtDB := jobsdb.NewForReadWrite("batch_rt", jobsdb.WithStats(stats.NOP))
	defer brtDB.TearDown()

	archDB := jobsdb.NewForReadWrite("archival", jobsdb.WithStats(stats.NOP))
	defer archDB.TearDown()
	readErrDB := jobsdb.NewForRead("proc_error", jobsdb.WithStats(stats.NOP))
	defer readErrDB.TearDown()
	writeErrDB := jobsdb.NewForWrite("proc_error", jobsdb.WithStats(stats.NOP))
	require.NoError(t, writeErrDB.Start())
	defer writeErrDB.TearDown()

	clearDb := false
	ctx := context.Background()

	schemaForwarder := mock_jobs_forwarder.NewMockForwarder(gomock.NewController(t))

	processor := processor.New(
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
		rsources.NewNoOpService(),
		transformer.NewNoOpService(),
		destinationdebugger.NewNoOpService(),
		transformationdebugger.NewNoOpService(),
		[]enricher.PipelineEnricher{},
		trackedusers.NewNoopDataCollector(),
	)
	processor.BackendConfig = mockBackendConfig
	processor.Transformer = mockTransformer
	mockBackendConfig.EXPECT().WaitForConfig(gomock.Any()).Times(1)

	rtFactory := &router.Factory{
		Logger:                     logger.NOP,
		Reporting:                  &reporting.NOOP{},
		BackendConfig:              mockBackendConfig,
		RouterDB:                   rtDB,
		ProcErrorDB:                readErrDB,
		TransientSources:           transientsource.NewEmptyService(),
		RsourcesService:            mockRsourcesService,
		TransformerFeaturesService: transformer.NewNoOpService(),
		ThrottlerFactory:           throttler.NewNoOpThrottlerFactory(),
	}
	brtFactory := &batchrouter.Factory{
		Reporting:        &reporting.NOOP{},
		BackendConfig:    mockBackendConfig,
		RouterDB:         brtDB,
		ProcErrorDB:      readErrDB,
		TransientSources: transientsource.NewEmptyService(),
		RsourcesService:  mockRsourcesService,
	}
	router := routermanager.New(rtFactory, brtFactory, mockBackendConfig, logger.NewLogger())

	mockBackendConfig.EXPECT().Subscribe(gomock.Any(), gomock.Any()).DoAndReturn(func(
		ctx context.Context, topic backendConfig.Topic,
	) pubsub.DataChannel {
		ch := make(chan pubsub.DataEvent, 1)
		ch <- pubsub.DataEvent{Data: map[string]backendConfig.ConfigT{workspaceID: sampleBackendConfig}, Topic: string(topic)}

		go func() {
			<-ctx.Done()
			close(ch)
		}()

		return ch
	}).AnyTimes()
	schemaForwarder.EXPECT().Start().Return(nil).AnyTimes()
	schemaForwarder.EXPECT().Stop().AnyTimes()

	provider := &mockModeProvider{modeCh: make(chan servermode.ChangeEvent)}
	dCM := &cluster.Dynamic{
		GatewayDB:       gwDB,
		RouterDB:        rtDB,
		BatchRouterDB:   brtDB,
		ErrorDB:         readErrDB,
		EventSchemaDB:   eschDB,
		ArchivalDB:      archDB,
		SchemaForwarder: schemaForwarder,
		Archiver: arc.New(
			archiveDB,
			nil,
			config.Default,
			stats.Default,
		),

		Processor: processor,
		Router:    router,
		Provider:  provider,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		err := dCM.Run(ctx)
		if err != nil {
			t.Logf("cluster runner stopped: %v", err)
		}
	}()

	chACK := make(chan bool)
	provider.sendMode(servermode.NewChangeEvent(servermode.NormalMode, func(_ context.Context) error {
		return nil
	}))
	require.Eventually(t, func() bool {
		return dCM.Mode() == servermode.NormalMode
	}, 5*time.Second, time.Millisecond)

	provider.sendMode(servermode.NewChangeEvent(servermode.DegradedMode, func(_ context.Context) error {
		close(chACK)
		return nil
	}))

	require.Eventually(t, func() bool {
		if dCM.Mode() != servermode.DegradedMode {
			return false
		}
		select {
		case <-chACK:
			return true
		default:
			return false
		}
	}, 30*time.Second, time.Millisecond)
}
