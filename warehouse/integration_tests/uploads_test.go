package integration_tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

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

func TestWarehouse(t *testing.T) {
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

	a := warehouse.New(mockApp, c, logger.NOP, memstats.New(), &bcConfig.NOOP{}, filemanager.New)
	err = a.Setup(ctx)
	require.NoError(t, err)

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return a.Run(gCtx)
	})
	g.Go(func() error {
		defer stopServer()
		return nil
	})
	require.NoError(t, g.Wait())
}

func requireStagingFilesCount(
	t *testing.T,
	db *sqlmiddleware.DB,
	stagingFile model.StagingFile,
	state string,
	expectedCount int,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		var eventsCount int
		require.NoError(t, db.QueryRow("SELECT COALESCE(sum(total_events), 0) FROM wh_staging_files WHERE source_id = $1 AND destination_id = $2 AND status = $3;", stagingFile.SourceID, stagingFile.DestinationID, state).Scan(&eventsCount))
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
	stagingFile model.StagingFile,
	state string,
	expectedCount int,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		var eventsCount int
		require.NoError(t, db.QueryRow("SELECT COALESCE(sum(total_events), 0) FROM wh_load_files WHERE source_id = $1 AND destination_id = $2 AND status = $3;", stagingFile.SourceID, stagingFile.DestinationID, state).Scan(&eventsCount))
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
	stagingFile model.StagingFile,
	state string,
	expectedCount int,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		var eventsCount int
		require.NoError(t, db.QueryRow("SELECT COALESCE(sum(total_events), 0) FROM wh_table_uploads WHERE source_id = $1 AND destination_id = $2 AND status = $3;", stagingFile.SourceID, stagingFile.DestinationID, state).Scan(&eventsCount))
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
	stagingFile model.StagingFile,
	state string,
	expectedCount int,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		var jobsCount int
		require.NoError(t, db.QueryRow("SELECT count(*) FROM wh_uploads WHERE source_id = $1 AND destination_id = $2 AND status = $3;", stagingFile.SourceID, stagingFile.DestinationID, state).Scan(&jobsCount))
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
