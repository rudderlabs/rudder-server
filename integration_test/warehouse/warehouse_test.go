package warehouse

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

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
	c.Set("Warehouse.mode", config.MasterMode)
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
}
