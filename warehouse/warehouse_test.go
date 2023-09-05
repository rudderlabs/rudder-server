package warehouse

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/mode"

	"github.com/golang/mock/gomock"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/app"
	bcConfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/enterprise/reporting"
	mocksApp "github.com/rudderlabs/rudder-server/mocks/app"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestApp(t *testing.T) {
	admin.Init()
	misc.Init()

	const (
		workspaceID              = "test_workspace_id"
		sourceID                 = "test_source_id"
		destinationID            = "test_destination_id"
		unsupportedWorkspaceID   = "unsupported_test_workspace_id"
		unsupportedSourceID      = "unsupported_test_source_id"
		unsupportedDestinationID = "unsupported_test_destination_id"
	)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	report := &reporting.Factory{}
	report.Setup(&bcConfig.NOOP{})

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockApp := mocksApp.NewMockApp(mockCtrl)
	mockApp.EXPECT().Features().Return(&app.Features{
		Reporting: report,
	}).AnyTimes()

	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)
	mockBackendConfig.EXPECT().WaitForConfig(gomock.Any()).DoAndReturn(func(ctx context.Context) error {
		return nil
	}).AnyTimes()
	mockBackendConfig.EXPECT().Identity().Return(nil)
	mockBackendConfig.EXPECT().Subscribe(gomock.Any(), bcConfig.TopicBackendConfig).DoAndReturn(func(ctx context.Context, topic bcConfig.Topic) pubsub.DataChannel {
		ch := make(chan pubsub.DataEvent, 1)
		ch <- pubsub.DataEvent{
			Data: map[string]bcConfig.ConfigT{
				workspaceID: {
					WorkspaceID: workspaceID,
					Sources: []bcConfig.SourceT{
						{
							ID:      sourceID,
							Enabled: true,
							Destinations: []bcConfig.DestinationT{
								{
									ID:      destinationID,
									Enabled: true,
									DestinationDefinition: bcConfig.DestinationDefinitionT{
										Name: whutils.POSTGRES,
									},
								},
								{
									ID:      destinationID,
									Enabled: true,
									DestinationDefinition: bcConfig.DestinationDefinitionT{
										Name: whutils.POSTGRES,
									},
								},
							},
						},
					},
				},
				unsupportedWorkspaceID: {
					WorkspaceID: unsupportedWorkspaceID,
					Sources: []bcConfig.SourceT{
						{
							ID:      unsupportedSourceID,
							Enabled: true,
							Destinations: []bcConfig.DestinationT{
								{
									ID:      unsupportedDestinationID,
									Enabled: true,
									DestinationDefinition: bcConfig.DestinationDefinitionT{
										Name: "unknown_destination_type",
									},
								},
							},
						},
					},
				},
			},
			Topic: string(bcConfig.TopicBackendConfig),
		}
		close(ch)
		return ch
	}).AnyTimes()

	t.Run("Graceful shutdown", func(t *testing.T) {
		testCases := []struct {
			name          string
			warehouseMode string
		}{
			{
				name:          "master",
				warehouseMode: config.MasterMode,
			},
			{
				name:          "master and slave",
				warehouseMode: config.MasterSlaveMode,
			},
			{
				name:          "slave",
				warehouseMode: config.SlaveMode,
			},
		}

		for _, tc := range testCases {
			tc := tc

			subTestCases := []struct {
				name        string
				runningMode string
			}{
				{
					name:        "running mode",
					runningMode: "",
				},
				{
					name:        "degraded mode",
					runningMode: mode.DegradedMode,
				},
			}

			for _, subTC := range subTestCases {
				subTC := subTC

				t.Run(tc.name+" with "+subTC.name, func(t *testing.T) {
					pgResource, err := resource.SetupPostgres(pool, t)
					require.NoError(t, err)

					webPort, err := kithelper.GetFreePort()
					require.NoError(t, err)

					ctx, stopServer := context.WithCancel(context.Background())

					port, err := strconv.Atoi(pgResource.Port)
					require.NoError(t, err)

					c := config.New()
					c.Set("WAREHOUSE_JOBS_DB_HOST", pgResource.Host)
					c.Set("WAREHOUSE_JOBS_DB_PORT", port)
					c.Set("WAREHOUSE_JOBS_DB_USER", pgResource.User)
					c.Set("WAREHOUSE_JOBS_DB_PASSWORD", pgResource.Password)
					c.Set("WAREHOUSE_JOBS_DB_DB_NAME", pgResource.Database)
					c.Set("Warehouse.mode", tc.warehouseMode)
					c.Set("Warehouse.runningMode", subTC.runningMode)
					c.Set("Warehouse.webPort", webPort)

					a := NewApp(mockApp, c, logger.NOP, stats.Default, &bcConfig.NOOP{}, filemanager.New)
					err = a.Setup(ctx)
					require.NoError(t, err)

					g, gCtx := errgroup.WithContext(ctx)
					g.Go(func() error {
						return a.Start(gCtx)
					})
					g.Go(func() error {
						defer stopServer()

						serverURL := fmt.Sprintf("http://localhost:%d", webPort)

						require.Eventually(t, func() bool {
							resp, err := http.Get(fmt.Sprintf("%s/health", serverURL))
							if err != nil {
								return false
							}
							defer func() {
								httputil.CloseResponse(resp)
							}()

							return resp.StatusCode == http.StatusOK
						},
							time.Second*10,
							time.Second,
						)
						return nil
					})
					require.NoError(t, g.Wait())
				})
			}
		}
	})
	t.Run("incompatible postgres", func(t *testing.T) {
		pgResource, err := resource.SetupPostgres(pool, t, postgres.WithTag("9-alpine"))
		require.NoError(t, err)

		port, err := strconv.Atoi(pgResource.Port)
		require.NoError(t, err)

		c := config.New()
		c.Set("WAREHOUSE_JOBS_DB_HOST", pgResource.Host)
		c.Set("WAREHOUSE_JOBS_DB_PORT", port)
		c.Set("WAREHOUSE_JOBS_DB_USER", pgResource.User)
		c.Set("WAREHOUSE_JOBS_DB_PASSWORD", pgResource.Password)
		c.Set("WAREHOUSE_JOBS_DB_DB_NAME", pgResource.Database)

		a := NewApp(mockApp, c, logger.NOP, stats.Default, &bcConfig.NOOP{}, filemanager.New)
		err = a.Setup(context.Background())
		require.EqualError(t, err, "setting up database: warehouse Service needs postgres version >= 10. Exiting")
	})
	t.Run("postgres down", func(t *testing.T) {
		c := config.New()
		c.Set("WAREHOUSE_JOBS_DB_HOST", "localhost")
		c.Set("WAREHOUSE_JOBS_DB_PORT", "5432")
		c.Set("WAREHOUSE_JOBS_DB_USER", "ubuntu")
		c.Set("WAREHOUSE_JOBS_DB_PASSWORD", "ubuntu")
		c.Set("WAREHOUSE_JOBS_DB_DB_NAME", "ubuntu")

		a := NewApp(mockApp, c, logger.NOP, stats.Default, &bcConfig.NOOP{}, filemanager.New)
		err = a.Setup(context.Background())
		require.ErrorContains(t, err, "setting up database: could not check compatibility:")
	})
	t.Run("without env vars", func(t *testing.T) {
		pgResource, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)

		port, err := strconv.Atoi(pgResource.Port)
		require.NoError(t, err)

		c := config.New()
		c.Set("DB.host", pgResource.Host)
		c.Set("DB.port", port)
		c.Set("DB.user", pgResource.User)
		c.Set("DB.password", pgResource.Password)
		c.Set("DB.name", pgResource.Database)

		a := NewApp(mockApp, c, logger.NOP, stats.Default, &bcConfig.NOOP{}, filemanager.New)
		err = a.Setup(context.Background())
		require.NoError(t, err)
	})
	t.Run("monitor routers", func(t *testing.T) {
		pgResource, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)

		webPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		ctx := context.Background()

		port, err := strconv.Atoi(pgResource.Port)
		require.NoError(t, err)

		c := config.New()
		c.Set("WAREHOUSE_JOBS_DB_HOST", pgResource.Host)
		c.Set("WAREHOUSE_JOBS_DB_PORT", port)
		c.Set("WAREHOUSE_JOBS_DB_USER", pgResource.User)
		c.Set("WAREHOUSE_JOBS_DB_PASSWORD", pgResource.Password)
		c.Set("WAREHOUSE_JOBS_DB_DB_NAME", pgResource.Database)
		c.Set("Warehouse.mode", config.MasterMode)
		c.Set("Warehouse.webPort", webPort)

		a := NewApp(mockApp, c, logger.NOP, stats.Default, mockBackendConfig, filemanager.New)
		require.NoError(t, a.Setup(ctx))
		require.NoError(t, a.monitorDestRouters(ctx))
	})
}
