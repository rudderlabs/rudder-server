package integration_tests

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"path"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/utils/httputil"

	"github.com/rudderlabs/rudder-server/admin"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/utils/pubsub"

	"github.com/google/uuid"

	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseclient "github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"

	"github.com/golang/mock/gomock"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/enterprise/reporting"
	mocksApp "github.com/rudderlabs/rudder-server/mocks/app"
	"github.com/rudderlabs/rudder-server/warehouse"
)

type testConfig struct {
	workspaceID           string
	sourceID              string
	destinationID         string
	destinationRevisionID string
	destinationType       string
	namespace             string
}

func TestUploads(t *testing.T) {
	admin.Init()

	t.Run("tracks", func(t *testing.T) {
		tc := &testConfig{
			workspaceID:           "test_workspace_id",
			sourceID:              "test_source_id",
			destinationID:         "test_destination_id",
			destinationRevisionID: "test_destination_id",
			namespace:             "test_namespace",
			destinationType:       warehouseutils.POSTGRES,
		}

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)

		pgResource, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)
		minioResource, err := resource.SetupMinio(pool, t)
		require.NoError(t, err)

		db := sqlmiddleware.New(pgResource.DB)

		ctx, stopServer := context.WithCancel(context.Background())
		defer stopServer()

		events := 100
		jobs := 1

		webPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		serverURL := fmt.Sprintf("http://localhost:%d", webPort)

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error {
			runWarehouseServer(t, gCtx, webPort, pgResource, minioResource, tc)
			return nil
		})
		g.Go(func() error {
			defer stopServer()
			requireHealthCheck(t, serverURL)
			pingWarehouse(t, gCtx, prepareStagingFile(t, gCtx, minioResource, events), events, serverURL, tc)
			requireStagingFilesCount(t, db, tc, "succeeded", events)
			requireLoadFilesCount(t, db, tc, events)
			requireTableUploadsCount(t, db, tc, model.ExportedData, events)
			requireUploadsCount(t, db, tc, model.ExportedData, jobs)
			requireWarehouseEventsCount(t, db, fmt.Sprintf("%s.%s", tc.namespace, "tracks"), events)
			return nil
		})
		require.NoError(t, g.Wait())
	})
	t.Run("destination revision", func(t *testing.T) {
		tc := &testConfig{
			workspaceID:           "test_workspace_id",
			sourceID:              "test_source_id",
			destinationID:         "test_destination_id",
			destinationRevisionID: "test_destination_revision_id",
			namespace:             "test_namespace",
			destinationType:       warehouseutils.POSTGRES,
		}

		bcserver := backendconfigtest.NewBuilder().
			WithWorkspaceConfig(
				backendconfigtest.NewConfigBuilder().
					WithSource(
						backendconfigtest.NewSourceBuilder().
							WithID("source-1").
							WithWriteKey("writekey-1").
							Build()).
					Build()).
			Build()
		defer bcserver.Close()

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)

		pgResource, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)
		minioResource, err := resource.SetupMinio(pool, t)
		require.NoError(t, err)

		db := sqlmiddleware.New(pgResource.DB)

		ctx, stopServer := context.WithCancel(context.Background())
		defer stopServer()

		events := 100
		jobs := 1

		webPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		serverURL := fmt.Sprintf("http://localhost:%d", webPort)

		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error {
			runWarehouseServer(t, gCtx, webPort, pgResource, minioResource, tc)
			return nil
		})
		g.Go(func() error {
			defer stopServer()
			requireHealthCheck(t, serverURL)
			pingWarehouse(t, gCtx, prepareStagingFile(t, gCtx, minioResource, events), events, serverURL, tc)
			requireStagingFilesCount(t, db, tc, "succeeded", events)
			requireLoadFilesCount(t, db, tc, events)
			requireTableUploadsCount(t, db, tc, model.ExportedData, events)
			requireUploadsCount(t, db, tc, model.ExportedData, jobs)
			requireWarehouseEventsCount(t, db, fmt.Sprintf("%s.%s", tc.namespace, "tracks"), events)
			return nil
		})
		require.NoError(t, g.Wait())
	})
	t.Run("staging file batch size", func(t *testing.T) {})
	t.Run("source job", func(t *testing.T) {})
	t.Run("reports", func(t *testing.T) {})
	t.Run("aborts", func(t *testing.T) {})
	t.Run("canAppend", func(t *testing.T) {})
	t.Run("JsonPaths", func(t *testing.T) {})
	t.Run("user and identifies", func(t *testing.T) {})
	t.Run("archiver", func(t *testing.T) {})
	t.Run("id resolution", func(t *testing.T) {})
	t.Run("retries", func(t *testing.T) {})
	t.Run("tunnelling", func(t *testing.T) {})
}

func runWarehouseServer(
	t testing.TB,
	ctx context.Context,
	webPort int,
	pgResource *resource.PostgresResource,
	minioResource *resource.MinioResource,
	tc *testConfig,
) {
	t.Helper()

	mockCtrl := gomock.NewController(t)

	mockApp := mocksApp.NewMockApp(mockCtrl)
	mockApp.EXPECT().Features().Return(&app.Features{
		Reporting: &reporting.Factory{},
	}).AnyTimes()

	bcConfigCh := make(chan pubsub.DataEvent)

	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)
	mockBackendConfig.EXPECT().WaitForConfig(gomock.Any()).DoAndReturn(func(ctx context.Context) error {
		return nil
	}).AnyTimes()
	mockBackendConfig.EXPECT().Identity().Return(nil)
	mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicBackendConfig).DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
		return bcConfigCh
	}).AnyTimes()

	conf := config.New()
	conf.Set("WAREHOUSE_JOBS_DB_HOST", pgResource.Host)
	conf.Set("WAREHOUSE_JOBS_DB_PORT", pgResource.Port)
	conf.Set("WAREHOUSE_JOBS_DB_USER", pgResource.User)
	conf.Set("WAREHOUSE_JOBS_DB_PASSWORD", pgResource.Password)
	conf.Set("WAREHOUSE_JOBS_DB_DB_NAME", pgResource.Database)
	conf.Set("Warehouse.mode", config.MasterSlaveMode)
	conf.Set("Warehouse.runningMode", "")
	conf.Set("Warehouse.webPort", webPort)
	conf.Set("Warehouse.jobs.maxAttemptsPerJob", 3)
	conf.Set("Warehouse.archiveUploadRelatedRecords", false)
	conf.Set("Warehouse.waitForWorkerSleep", "1s")
	conf.Set("Warehouse.uploadAllocatorSleep", "1s")
	conf.Set("Warehouse.uploadStatusTrackFrequency", "1s")
	conf.Set("Warehouse.mainLoopSleep", "1s")
	conf.Set("Warehouse.jobs.processingSleepInterval", "1s")
	conf.Set("PgNotifier.maxPollSleep", "1s")
	conf.Set("PgNotifier.trackBatchIntervalInS", "1s")
	conf.Set("PgNotifier.maxAttempt", 1)

	a := warehouse.New(mockApp, conf, logger.NOP, memstats.New(), mockBackendConfig, filemanager.New)
	err := a.Setup(ctx)
	require.NoError(t, err)

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(bcConfigCh)

		for {
			select {
			case <-gCtx.Done():
				return nil
			case <-time.After(time.Second):
				bcConfigCh <- pubsub.DataEvent{
					Data: map[string]backendconfig.ConfigT{
						tc.workspaceID: {
							WorkspaceID: tc.workspaceID,
							Sources: []backendconfig.SourceT{
								{
									ID:      tc.sourceID,
									Enabled: true,
									Destinations: []backendconfig.DestinationT{
										{
											ID:      tc.destinationID,
											Enabled: true,
											DestinationDefinition: backendconfig.DestinationDefinitionT{
												Name: tc.destinationType,
											},
											Config: map[string]interface{}{
												"host":             pgResource.Host,
												"database":         pgResource.Database,
												"user":             pgResource.User,
												"password":         pgResource.Password,
												"port":             pgResource.Port,
												"sslMode":          "disable",
												"namespace":        "test_namespace",
												"bucketProvider":   "MINIO",
												"bucketName":       minioResource.BucketName,
												"accessKeyID":      minioResource.AccessKeyID,
												"secretAccessKey":  minioResource.AccessKeySecret,
												"useSSL":           false,
												"endPoint":         minioResource.Endpoint,
												"syncFrequency":    "0",
												"useRudderStorage": false,
											},
											RevisionID: tc.destinationRevisionID,
										},
									},
								},
							},
						},
					},
					Topic: string(backendconfig.TopicBackendConfig),
				}
			}
		}
	})
	g.Go(func() error {
		return a.Run(gCtx)
	})
	require.NoError(t, g.Wait())
}

func prepareStagingFile(
	t testing.TB,
	ctx context.Context,
	minioResource *resource.MinioResource,
	events int,
) filemanager.UploadedFile {
	t.Helper()

	filePath := path.Join(t.TempDir(), "staging.json")
	gz, err := misc.CreateGZ(filePath)
	require.NoError(t, err)

	buf := bytes.NewBuffer(make([]byte, 0, 1024))
	for i := 0; i < events; i++ {
		buf.WriteString(fmt.Sprintf(`{"data":{"id":%q,"event":"tracks","user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","event":"string","user_id":"string","received_at":"datetime"}, "table": "tracks"}}`,
			uuid.New().String(),
			uuid.New().String(),
		))
		buf.WriteString("\n")
	}

	_, err = gz.Write(buf.Bytes())
	require.NoError(t, err)
	require.NoError(t, gz.Close())

	f, err := os.Open(filePath)
	require.NoError(t, err)

	minioConfig := map[string]any{
		"bucketName":      minioResource.BucketName,
		"accessKeyID":     minioResource.AccessKeyID,
		"secretAccessKey": minioResource.AccessKeySecret,
		"endPoint":        minioResource.Endpoint,
	}

	fm, err := filemanager.NewMinioManager(minioConfig, logger.NewLogger(), func() time.Duration {
		return time.Minute
	})
	require.NoError(t, err)

	uploadFile, err := fm.Upload(ctx, f)
	require.NoError(t, err)

	return uploadFile
}

func pingWarehouse(
	t testing.TB,
	ctx context.Context,
	uploadFile filemanager.UploadedFile,
	events int,
	url string,
	tc *testConfig,
) {
	t.Helper()

	wClient := warehouseclient.NewWarehouse(url)
	err := wClient.Process(ctx, warehouseclient.StagingFile{
		WorkspaceID:           tc.workspaceID,
		SourceID:              tc.sourceID,
		DestinationID:         tc.destinationID,
		Location:              uploadFile.ObjectName,
		TotalEvents:           events,
		FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
		LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
		UseRudderStorage:      false,
		DestinationRevisionID: tc.destinationID,
		Schema: map[string]map[string]interface{}{
			"tracks": {
				"id":          "string",
				"event":       "string",
				"user_id":     "string",
				"received_at": "datetime",
			},
		},
	})
	require.NoError(t, err)
}

func requireHealthCheck(
	t testing.TB,
	url string,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("%s/health", url))
		if err != nil {
			return false
		}
		defer func() {
			httputil.CloseResponse(resp)
		}()

		return resp.StatusCode == http.StatusOK
	},
		time.Second*25,
		time.Millisecond*100,
	)
}

func requireStagingFilesCount(
	t testing.TB,
	db *sqlmiddleware.DB,
	tc *testConfig,
	state string,
	expectedCount int,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		var eventsCount int
		require.NoError(t, db.QueryRow("SELECT COALESCE(sum(total_events), 0) FROM wh_staging_files WHERE source_id = $1 AND destination_id = $2 AND status = $3;",
			tc.sourceID,
			tc.destinationID,
			state,
		).Scan(&eventsCount))
		t.Logf("Staging files count: %d", eventsCount)
		return eventsCount == expectedCount
	},
		30*time.Second,
		1*time.Second,
		fmt.Sprintf("expected %s staging files count to be %d", state, expectedCount),
	)
}

func requireLoadFilesCount(
	t testing.TB,
	db *sqlmiddleware.DB,
	tc *testConfig,
	expectedCount int,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		var eventsCount int
		require.NoError(t, db.QueryRow("SELECT COALESCE(sum(total_events), 0) FROM wh_load_files WHERE source_id = $1 AND destination_id = $2;",
			tc.sourceID,
			tc.destinationID,
		).Scan(&eventsCount))
		t.Logf("Load files count: %d", eventsCount)
		return eventsCount == expectedCount
	},
		30*time.Second,
		1*time.Second,
		fmt.Sprintf("expected load files count to be %d", expectedCount),
	)
}

func requireTableUploadsCount(
	t testing.TB,
	db *sqlmiddleware.DB,
	tc *testConfig,
	state string,
	expectedCount int,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		var eventsCount int
		require.NoError(t, db.QueryRow("SELECT COALESCE(sum(total_events), 0) FROM wh_table_uploads WHERE status = $1 AND wh_upload_id IN (SELECT id FROM wh_uploads WHERE wh_uploads.source_id = $2 AND wh_uploads.destination_id = $3 AND wh_uploads.namespace = $4);",
			state,
			tc.sourceID,
			tc.destinationID,
			tc.namespace,
		).Scan(&eventsCount))
		t.Logf("Table uploads count: %d", eventsCount)
		return eventsCount == expectedCount
	},
		30*time.Second,
		1*time.Second,
		fmt.Sprintf("expected %s table uploads count to be %d", state, expectedCount),
	)
}

func requireUploadsCount(
	t testing.TB,
	db *sqlmiddleware.DB,
	tc *testConfig,
	state string,
	expectedCount int,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		var jobsCount int
		require.NoError(t, db.QueryRow("SELECT count(*) FROM wh_uploads WHERE source_id = $1 AND destination_id = $2 AND namespace = $3 AND status = $4;",
			tc.sourceID,
			tc.destinationID,
			tc.namespace,
			state,
		).Scan(&jobsCount))
		t.Logf("uploads count: %d", jobsCount)
		return jobsCount == expectedCount
	},
		30*time.Second,
		1*time.Second,
		fmt.Sprintf("expected %s uploads count to be %d", state, expectedCount),
	)
}

func requireWarehouseEventsCount(
	t testing.TB,
	db *sqlmiddleware.DB,
	tableName string,
	expectedCount int,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		var eventsCount int
		require.NoError(t, db.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s;", tableName)).Scan(&eventsCount))
		t.Logf("warehouse events count: %d", eventsCount)
		return eventsCount == expectedCount
	},
		30*time.Second,
		1*time.Second,
		fmt.Sprintf("expected warehouse events count to be %d", expectedCount),
	)
}
