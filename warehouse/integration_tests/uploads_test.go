package integration_tests

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseclient "github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"

	"github.com/golang/mock/gomock"
	"github.com/ory/dockertest/v3"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/rudderlabs/rudder-server/app"
	bcConfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/enterprise/reporting"
	mocksApp "github.com/rudderlabs/rudder-server/mocks/app"
	"github.com/rudderlabs/rudder-server/warehouse"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestUploads(t *testing.T) {
	sourceID := "test-source-id"
	destinationID := "test-destination-id"
	workspaceID := "test-workspace-id"
	namespace := "test-namespace"
	destinationType := warehouseutils.POSTGRES
	workspaceIdentifier := "test-workspace-identifier"

	ctx := context.Background()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	report := &reporting.Factory{}
	report.Setup(ctx, &bcConfig.NOOP{})

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockApp := mocksApp.NewMockApp(mockCtrl)
	mockApp.EXPECT().Features().Return(&app.Features{
		Reporting: report,
	}).AnyTimes()

	pgResource, err := resource.SetupPostgres(pool, t)
	require.NoError(t, err)
	minioResource, err := resource.SetupMinio(pool, t)
	require.NoError(t, err)

	webPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	ctx, stopServer := context.WithCancel(context.Background())

	c := config.New()
	c.Set("WAREHOUSE_JOBS_DB_HOST", pgResource.Host)
	c.Set("WAREHOUSE_JOBS_DB_PORT", pgResource.Port)
	c.Set("WAREHOUSE_JOBS_DB_USER", pgResource.User)
	c.Set("WAREHOUSE_JOBS_DB_PASSWORD", pgResource.Password)
	c.Set("WAREHOUSE_JOBS_DB_DB_NAME", pgResource.Database)
	c.Set("Warehouse.mode", config.MasterSlaveMode)
	c.Set("Warehouse.runningMode", "")
	c.Set("Warehouse.webPort", webPort)

	buf := bytes.NewBuffer(make([]byte, 0, 1024))
	for i := 0; i < 100; i++ {
		buf.WriteString(fmt.Sprintf(`{"data":{"id":%q,"event":"tracks","user_id":%q,"received_at":"2023-05-12T04:36:50.199Z"},"metadata":{"columns":{"id":"string","event":"string","user_id":"string","received_at":"datetime"}}}`,
			uuid.New().String(),
			uuid.New().String(),
		))
		buf.WriteString("\n")
	}

	filePath := path.Join(t.TempDir(), "staging.json")
	err = os.WriteFile(filePath, buf.Bytes(), os.ModePerm)
	require.NoError(t, err)

	f, err := os.Open(filePath)
	require.NoError(t, err)

	minioConfig := map[string]any{
		"bucketName":      minioResource.BucketName,
		"accessKeyID":     minioResource.AccessKeyID,
		"secretAccessKey": minioResource.AccessKeySecret,
		"endPoint":        minioResource.Endpoint,
	}
	fm, err := filemanager.NewMinioManager(minioConfig, logger.NOP, func() time.Duration {
		return time.Minute
	})
	require.NoError(t, err)
	uploadFile, err := fm.Upload(ctx, f)
	require.NoError(t, err)

	db := sqlmiddleware.New(pgResource.DB)

	whclient := warehouseclient.NewWarehouse(fmt.Sprintf("http://localhost:%d", webPort))
	err = whclient.Process(ctx, warehouseclient.StagingFile{
		WorkspaceID:           workspaceID,
		SourceID:              sourceID,
		DestinationID:         destinationID,
		Location:              uploadFile.Location,
		TotalEvents:           1,
		TotalBytes:            100,
		FirstEventAt:          time.Now().Format(misc.RFC3339Milli),
		LastEventAt:           time.Now().Add(time.Minute * 30).Format(misc.RFC3339Milli),
		UseRudderStorage:      false,
		DestinationRevisionID: destinationID,
		Schema: map[string]map[string]interface{}{
			"tracks": {
				"id":          "string",
				"event":       "string",
				"user_id":     "string",
				"received_at": "datetime",
			},
		},
	})
	require.Error(t, err)

	a := warehouse.New(mockApp, c, logger.NOP, memstats.New(), &bcConfig.NOOP{}, filemanager.New)
	err = a.Setup(ctx)
	require.NoError(t, err)

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return a.Run(gCtx)
	})
	g.Go(func() error {
		defer stopServer()
		requireStagingFilesCount(t, db, sourceID, destinationID, "succeeded", 1)
		requireLoadFilesCount(t, db, sourceID, destinationID, "succeeded", 1)
		requireTableUploadsCount(t, db, sourceID, destinationID, "succeeded", 1)
		requireUploadsCount(t, db, sourceID, destinationID, "succeeded", 1)
		requireWarehouseEventsCount(t, db, fmt.Sprintf("%s.%s", namespace, "tracks"), "succeeded", 1)
		return nil
	})
	require.NoError(t, g.Wait())
}

func requireStagingFilesCount(
	t *testing.T,
	db *sqlmiddleware.DB,
	sourceID string, destinationID string,
	state string,
	expectedCount int,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		var eventsCount int
		require.NoError(t, db.QueryRow("SELECT COALESCE(sum(total_events), 0) FROM wh_staging_files WHERE source_id = $1 AND destination_id = $2 AND status = $3;", sourceID, destinationID, state).Scan(&eventsCount))
		t.Logf("events count: %d", eventsCount)
		return eventsCount == expectedCount
	},
		120*time.Second,
		1*time.Second,
		fmt.Sprintf("expected %s job count to be %d", state, expectedCount),
	)
}

func requireLoadFilesCount(
	t *testing.T,
	db *sqlmiddleware.DB,
	sourceID string, destinationID string,
	state string,
	expectedCount int,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		var eventsCount int
		require.NoError(t, db.QueryRow("SELECT COALESCE(sum(total_events), 0) FROM wh_load_files WHERE source_id = $1 AND destination_id = $2 AND status = $3;", sourceID, destinationID, state).Scan(&eventsCount))
		t.Logf("events count: %d", eventsCount)
		return eventsCount == expectedCount
	},
		20*time.Second,
		1*time.Second,
		fmt.Sprintf("expected %s job count to be %d", state, expectedCount),
	)
}

func requireTableUploadsCount(
	t *testing.T,
	db *sqlmiddleware.DB,
	sourceID string, destinationID string,
	state string,
	expectedCount int,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		var eventsCount int
		require.NoError(t, db.QueryRow("SELECT COALESCE(sum(total_events), 0) FROM wh_table_uploads WHERE source_id = $1 AND destination_id = $2 AND status = $3;", sourceID, destinationID, state).Scan(&eventsCount))
		t.Logf("events count: %d", eventsCount)
		return eventsCount == expectedCount
	},
		20*time.Second,
		1*time.Second,
		fmt.Sprintf("expected %s job count to be %d", state, expectedCount),
	)
}

func requireUploadsCount(
	t *testing.T,
	db *sqlmiddleware.DB,
	sourceID string, destinationID string,
	state string,
	expectedCount int,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		var jobsCount int
		require.NoError(t, db.QueryRow("SELECT count(*) FROM wh_uploads WHERE source_id = $1 AND destination_id = $2 AND status = $3;", sourceID, destinationID, state).Scan(&jobsCount))
		t.Logf("jobs count: %d", jobsCount)
		return jobsCount == expectedCount
	},
		20*time.Second,
		1*time.Second,
		fmt.Sprintf("expected %s job count to be %d", state, expectedCount),
	)
}

func requireWarehouseEventsCount(
	t *testing.T,
	db *sqlmiddleware.DB,
	tableName string,
	state string,
	expectedCount int,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		var eventsCount int
		require.NoError(t, db.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s", tableName)).Scan(&eventsCount))
		t.Logf("events count: %d", eventsCount)
		return eventsCount == expectedCount
	},
		20*time.Second,
		1*time.Second,
		fmt.Sprintf("expected %s job count to be %d", state, expectedCount),
	)
}
