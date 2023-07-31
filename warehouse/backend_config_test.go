package warehouse

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestBackendConfigManager(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pgResource, err := resource.SetupPostgres(pool, t)
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

	bcm := newBackendConfigManager(c, db, mockBackendConfig, logger.NOP)

	t.Run("Subscriptions", func(t *testing.T) {
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
		manyBCM := newBackendConfigManager(c, db, mockBackendConfig, logger.NOP)
		subscriptionsChs := make([]<-chan []model.Warehouse, numSubscribers)

		for i := 0; i < numSubscribers; i++ {
			subscriptionsChs[i] = manyBCM.Subscribe(ctx)
		}

		go func() {
			manyBCM.Start(ctx)
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
