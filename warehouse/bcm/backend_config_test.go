package bcm

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestBackendConfigManager(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pgResource, err := postgres.Setup(pool, t)
	require.NoError(t, err)

	workspaceID := "test-workspace-id"
	sourceID := "test-source-id"
	destinationID := "test-destination-id"
	namespace := "test-namespace"

	unknownSourceID := "unknown-source-id"
	unknownDestinationID := "unknown-destination-id"
	unknownDestinationName := "unknown-destination-name"

	ctrl := gomock.NewController(t)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(ctrl)
	mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicBackendConfig).DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
		ch := make(chan pubsub.DataEvent, 1)
		ch <- pubsub.DataEvent{
			Data: map[string]backendconfig.ConfigT{
				workspaceID: {
					WorkspaceID: workspaceID,
					Sources: []backendconfig.SourceT{
						{
							ID:      sourceID,
							Enabled: true,
							Destinations: []backendconfig.DestinationT{
								{
									ID:      destinationID,
									Enabled: true,
									DestinationDefinition: backendconfig.DestinationDefinitionT{
										Name: warehouseutils.RS,
									},
									Config: map[string]interface{}{
										"namespace": namespace,
									},
								},
							},
						},
						{
							ID:      unknownSourceID,
							Enabled: true,
							Destinations: []backendconfig.DestinationT{
								{
									ID:      unknownDestinationID,
									Enabled: true,
									DestinationDefinition: backendconfig.DestinationDefinitionT{
										Name: unknownDestinationName,
									},
									Config: map[string]interface{}{},
								},
							},
						},
					},
				},
			},
			Topic: string(backendconfig.TopicBackendConfig),
		}
		close(ch)
		return ch
	}).AnyTimes()

	svc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"publicKey": "public_key", "privateKey": "private_key"}`))
	}))
	defer svc.Close()

	c := config.New()
	c.Set("CONFIG_BACKEND_URL", svc.URL)
	c.Set("CP_INTERNAL_API_USERNAME", "username")
	c.Set("CP_INTERNAL_API_PASSWORD", "password")

	db := sqlquerywrapper.New(
		pgResource.DB,
		sqlquerywrapper.WithLogger(logger.NOP),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tenantManager := multitenant.New(config.New(), mockBackendConfig)

	t.Run("Subscriptions", func(t *testing.T) {
		bcm := New(c, db, tenantManager, logger.NOP, stats.NOP)

		require.False(t, bcm.IsInitialized())
		require.Equal(t, bcm.Connections(), map[string]map[string]model.Warehouse{})

		csm, ok := bcm.ConnectionSourcesMap(destinationID)
		require.False(t, ok)

		require.Nil(t, csm)
		require.Nil(t, bcm.SourceIDsByWorkspace())
		require.Equal(t, bcm.WarehousesBySourceID(sourceID), []model.Warehouse{})
		require.Equal(t, bcm.WarehousesByDestID(destinationID), []model.Warehouse{})

		subscriptionsCh := bcm.Subscribe(ctx)

		go func() {
			bcm.Start(ctx)
		}()

		expectedWarehouse := model.Warehouse{
			WorkspaceID: workspaceID,
			Source: backendconfig.SourceT{
				ID:      sourceID,
				Enabled: true,
				Destinations: []backendconfig.DestinationT{
					{
						ID:      destinationID,
						Enabled: true,
						DestinationDefinition: backendconfig.DestinationDefinitionT{
							Name: warehouseutils.RS,
						},
						Config: map[string]interface{}{
							"namespace": namespace,
						},
					},
				},
			},
			Destination: backendconfig.DestinationT{
				ID:      destinationID,
				Enabled: true,
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: warehouseutils.RS,
				},
				Config: map[string]interface{}{
					"namespace": namespace,
				},
			},
			Namespace:  "test_namespace",
			Type:       warehouseutils.RS,
			Identifier: "RS:test-source-id:test-destination-id",
		}

		require.Equal(t, <-subscriptionsCh, []model.Warehouse{expectedWarehouse})
		require.True(t, bcm.IsInitialized())
		require.Equal(t, bcm.Connections(), map[string]map[string]model.Warehouse{
			destinationID: {
				sourceID: expectedWarehouse,
			},
		})

		csm, ok = bcm.ConnectionSourcesMap(destinationID)
		require.True(t, ok)
		require.Equal(t, csm, map[string]model.Warehouse{
			sourceID: expectedWarehouse,
		})

		require.Equal(t, bcm.SourceIDsByWorkspace(), map[string][]string{
			workspaceID: {sourceID, unknownSourceID},
		})
		require.Equal(t, bcm.WarehousesBySourceID(sourceID), []model.Warehouse{expectedWarehouse})
		require.Equal(t, bcm.WarehousesByDestID(destinationID), []model.Warehouse{expectedWarehouse})
		require.Equal(t, bcm.WarehousesBySourceID(unknownSourceID), []model.Warehouse{})
		require.Equal(t, bcm.WarehousesByDestID(unknownDestinationID), []model.Warehouse{})
	})

	t.Run("Tunnelling", func(t *testing.T) {
		bcm := New(c, db, tenantManager, logger.NOP, stats.NOP)

		testCases := []struct {
			name     string
			input    backendconfig.DestinationT
			expected backendconfig.DestinationT
		}{
			{
				name: "tunnelling disabled",
				input: backendconfig.DestinationT{
					ID:      destinationID,
					Enabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: warehouseutils.RS,
					},
					Config: map[string]interface{}{
						"namespace": namespace,
					},
				},
				expected: backendconfig.DestinationT{
					ID:      destinationID,
					Enabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: warehouseutils.RS,
					},
					Config: map[string]interface{}{
						"namespace": namespace,
					},
				},
			},
			{
				name: "tunnelling enabled",
				input: backendconfig.DestinationT{
					ID:      destinationID,
					Enabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: warehouseutils.RS,
					},
					Config: map[string]interface{}{
						"namespace": namespace,
						"useSSH":    true,
					},
				},
				expected: backendconfig.DestinationT{
					ID:      destinationID,
					Enabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name: warehouseutils.RS,
					},
					Config: map[string]interface{}{
						"namespace":     namespace,
						"useSSH":        true,
						"sshPrivateKey": "private_key",
					},
				},
			},
		}

		for _, tc := range testCases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				require.Equal(t, bcm.attachSSHTunnellingInfo(ctx, tc.input), tc.expected)
			})
		}
	})

	t.Run("Many subscribers", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		numSubscribers := 1000
		bcm := New(c, db, tenantManager, logger.NOP, stats.NOP)
		subscriptionsChs := make([]<-chan []model.Warehouse, numSubscribers)

		for i := 0; i < numSubscribers; i++ {
			subscriptionsChs[i] = bcm.Subscribe(ctx)
		}

		go func() {
			bcm.Start(ctx)
		}()

		for i := 0; i < numSubscribers; i++ {
			require.Len(t, <-subscriptionsChs[i], 1)
		}

		cancel()

		for i := 0; i < numSubscribers; i++ {
			w, ok := <-subscriptionsChs[i]
			require.Nil(t, w)
			require.False(t, ok)
		}
	})
}

func TestBackendConfigManager_Namespace(t *testing.T) {
	testcases := []struct {
		name              string
		config            map[string]interface{}
		source            backendconfig.SourceT
		destination       backendconfig.DestinationT
		expectedNamespace string
		setConfig         bool
	}{
		{
			name:   "clickhouse with database configured in config",
			source: backendconfig.SourceT{},
			destination: backendconfig.DestinationT{
				Config: map[string]interface{}{
					"database": "test_db",
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: warehouseutils.CLICKHOUSE,
				},
			},
			expectedNamespace: "test_db",
			setConfig:         false,
		},
		{
			name:   "clickhouse without database configured in config",
			source: backendconfig.SourceT{},
			destination: backendconfig.DestinationT{
				Config: map[string]interface{}{},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: warehouseutils.CLICKHOUSE,
				},
			},
			expectedNamespace: "rudder",
			setConfig:         false,
		},
		{
			name:   "namespace only contains letters",
			source: backendconfig.SourceT{},
			destination: backendconfig.DestinationT{
				Config: map[string]interface{}{
					"namespace": "      test_namespace        ",
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: "test-destinationType-1",
				},
			},
			expectedNamespace: "test_namespace",
			setConfig:         false,
		},
		{
			name:   "namespace only contains special characters",
			source: backendconfig.SourceT{},
			destination: backendconfig.DestinationT{
				Config: map[string]interface{}{
					"namespace": "##",
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: "test-destinationType-1",
				},
			},
			expectedNamespace: "stringempty",
			setConfig:         false,
		},
		{
			name:   "namespace contains special characters and letters",
			source: backendconfig.SourceT{},
			destination: backendconfig.DestinationT{
				Config: map[string]interface{}{
					"namespace": "##evrnvrv$vtr&^",
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: "test-destinationType-1",
				},
			},
			expectedNamespace: "evrnvrv_vtr",
			setConfig:         false,
		},
		{
			name:   "empty namespace but config is set",
			source: backendconfig.SourceT{},
			destination: backendconfig.DestinationT{
				Config: map[string]interface{}{},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: "test-destinationType-1",
				},
			},
			expectedNamespace: "config_result",
			setConfig:         true,
		},
		{
			name: "empty namespace with picking from cache",
			source: backendconfig.SourceT{
				Name: "test-source",
				ID:   "test-sourceID",
			},
			destination: backendconfig.DestinationT{
				Config: map[string]interface{}{},
				ID:     "test-destinationID",
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: "test-destinationType-1",
				},
			},
			expectedNamespace: "test-namespace",
			setConfig:         false,
		},
		{
			name: "destination config without namespace configured and custom dataset prefix is not configured",
			source: backendconfig.SourceT{
				Name: "test-source",
				ID:   "random-sourceID",
			},
			destination: backendconfig.DestinationT{
				Config: map[string]interface{}{},
				ID:     "random-destinationID",
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: "test-destinationType-1",
				},
			},
			expectedNamespace: "test_source",
			setConfig:         false,
		},
		{
			name: "destination config without namespace configured and custom dataset prefix configured",
			source: backendconfig.SourceT{
				Name: "test-source",
				ID:   "test-sourceID",
			},
			destination: backendconfig.DestinationT{
				Config: map[string]interface{}{},
				ID:     "test-destinationID",
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: "test-destinationType-1",
				},
			},
			expectedNamespace: "config_result_test_source",
			setConfig:         true,
		},
	}

	for _, tc := range testcases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			pgResource, err := postgres.Setup(pool, t)
			require.NoError(t, err)

			db := sqlquerywrapper.New(
				pgResource.DB,
				sqlquerywrapper.WithLogger(logger.NOP),
			)

			err = (&migrator.Migrator{
				Handle:          db.DB,
				MigrationsTable: "wh_schema_migrations",
			}).Migrate("warehouse")
			require.NoError(t, err)

			c := config.New()

			if tc.setConfig {
				c.Set(fmt.Sprintf("Warehouse.%s.customDatasetPrefix", warehouseutils.WHDestNameMap[tc.destination.DestinationDefinition.Name]), "config_result")
			}

			sqlStatement, err := os.ReadFile("testdata/sql/namespace_test.sql")
			require.NoError(t, err)

			_, err = pgResource.DB.Exec(string(sqlStatement))
			require.NoError(t, err)

			tenantManager := multitenant.New(
				config.New(),
				backendconfig.DefaultBackendConfig,
			)

			bcm := New(c, db, tenantManager, logger.NOP, stats.NOP)

			namespace := bcm.namespace(context.Background(), tc.source, tc.destination)
			require.Equal(t, tc.expectedNamespace, namespace)
		})
	}
}
