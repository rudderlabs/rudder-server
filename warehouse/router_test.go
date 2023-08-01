package warehouse

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/services/controlplane"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestRouter(t *testing.T) {
	pgnotifier.Init()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pkgLogger = logger.NOP

	t.Run("UploadsToProcess", func(t *testing.T) {
		pgResource, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)

		t.Log("db:", pgResource.DBDsn)

		err = (&migrator.Migrator{
			Handle:          pgResource.DB,
			MigrationsTable: "wh_schema_migrations",
		}).Migrate("warehouse")
		require.NoError(t, err)

		db := sqlmiddleware.New(pgResource.DB)

		now := time.Date(2021, 1, 1, 0, 0, 3, 0, time.UTC)
		repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
			return now
		}))
		repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
			return now
		}))

		destType := "RS"
		ctx := context.Background()

		uploads := []model.Upload{
			{
				WorkspaceID:     "workspace_id",
				Namespace:       "namespace",
				SourceID:        "source_id",
				DestinationID:   "destination_id",
				DestinationType: destType,
			},
		}

		for i := range uploads {
			stagingID, err := repoStaging.Insert(ctx, &model.StagingFileWithSchema{})
			require.NoError(t, err)

			_, err = repoUpload.CreateWithStagingFiles(ctx, uploads[i], []*model.StagingFile{{
				ID:            stagingID,
				SourceID:      uploads[i].SourceID,
				DestinationID: uploads[i].DestinationID,
			}})
			require.NoError(t, err)
		}

		workspaceID := "test-workspace-id"
		sourceID := "test-source-id"
		destinationID := "test-destination-id"
		namespace := "test-namespace"
		workspaceIdentifier := "test-workspace-identifier"

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
						},
					},
				},
				Topic: string(backendconfig.TopicBackendConfig),
			}
			close(ch)
			return ch
		}).AnyTimes()

		tenantManager := &multitenant.Manager{
			BackendConfig: mockBackendConfig,
		}

		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer s.Close()

		conf := config.New()

		cp := controlplane.NewClient(s.URL, &identity.Namespace{},
			controlplane.WithHTTPClient(s.Client()),
		)
		bcm := newBackendConfigManager(conf, db, tenantManager, logger.NOP)

		notifier, err = pgnotifier.New(workspaceIdentifier, pgResource.DBDsn)
		require.NoError(t, err)

		r, err := NewRouter(ctx, destType, conf, logger.NOP, stats.Default, db, notifier, tenantManager, cp, bcm)
		require.NoError(t, err)

		r.warehouses = []model.Warehouse{
			{
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
			},
		}

		ups, err := r.uploadsToProcess(ctx, 1, []string{})
		require.NoError(t, err)
		require.Equal(t, 1, len(ups))
	})
}
