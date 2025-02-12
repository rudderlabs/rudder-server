package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/services/notifier"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/warehouse/bcm"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/mode"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
	"github.com/rudderlabs/rudder-server/warehouse/source"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestHTTPApi(t *testing.T) {
	const (
		workspaceID              = "test_workspace_id"
		sourceID                 = "test_source_id"
		destinationID            = "test_destination_id"
		degradedWorkspaceID      = "degraded_test_workspace_id"
		degradedSourceID         = "degraded_test_source_id"
		degradedDestinationID    = "degraded_test_destination_id"
		unusedWorkspaceID        = "unused_test_workspace_id"
		unusedSourceID           = "unused_test_source_id"
		unusedDestinationID      = "unused_test_destination_id"
		unsupportedWorkspaceID   = "unsupported_test_workspace_id"
		unsupportedSourceID      = "unsupported_test_source_id"
		unsupportedDestinationID = "unsupported_test_destination_id"
		workspaceIdentifier      = "test_workspace-identifier"
		namespace                = "test_namespace"
		destinationType          = "test_destination_type"
		sourceTaskRunID          = "test_source_task_run_id"
		sourceJobID              = "test_source_job_id"
		sourceJobRunID           = "test_source_job_run_id"
	)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pgResource, err := postgres.Setup(pool, t)
	require.NoError(t, err)

	t.Log("db:", pgResource.DBDsn)

	err = (&migrator.Migrator{
		Handle:          pgResource.DB,
		MigrationsTable: "wh_schema_migrations",
	}).Migrate("warehouse")
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(ctrl)
	mockBackendConfig.EXPECT().WaitForConfig(gomock.Any()).DoAndReturn(func(ctx context.Context) error {
		return nil
	}).AnyTimes()
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
										Name: warehouseutils.POSTGRES,
									},
								},
							},
						},
					},
				},
				degradedWorkspaceID: {
					WorkspaceID: degradedWorkspaceID,
					Sources: []backendconfig.SourceT{
						{
							ID:      degradedSourceID,
							Enabled: true,
							Destinations: []backendconfig.DestinationT{
								{
									ID:      degradedDestinationID,
									Enabled: true,
									DestinationDefinition: backendconfig.DestinationDefinitionT{
										Name: warehouseutils.POSTGRES,
									},
								},
							},
						},
					},
				},
				unsupportedWorkspaceID: {
					WorkspaceID: unsupportedWorkspaceID,
					Sources: []backendconfig.SourceT{
						{
							ID:      unsupportedSourceID,
							Enabled: true,
							Destinations: []backendconfig.DestinationT{
								{
									ID:      unsupportedDestinationID,
									Enabled: true,
									DestinationDefinition: backendconfig.DestinationDefinitionT{
										Name: "unknown_destination_type",
									},
								},
							},
						},
					},
				},
				unusedWorkspaceID: {
					WorkspaceID: unusedWorkspaceID,
					Sources: []backendconfig.SourceT{
						{
							ID:      unusedSourceID,
							Enabled: true,
							Destinations: []backendconfig.DestinationT{
								{
									ID:      unusedDestinationID,
									Enabled: true,
									DestinationDefinition: backendconfig.DestinationDefinitionT{
										Name: warehouseutils.POSTGRES,
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

	c := config.New()
	c.Set("Warehouse.degradedWorkspaceIDs", []string{degradedWorkspaceID})

	db := sqlmiddleware.New(pgResource.DB)

	tenantManager := multitenant.New(c, mockBackendConfig)

	bcManager := bcm.New(config.New(), db, tenantManager, logger.NOP, stats.NOP)

	triggerStore := &sync.Map{}

	ctx, stopTest := context.WithCancel(context.Background())

	n := notifier.New(config.New(), logger.NOP, stats.NOP, workspaceIdentifier)
	err = n.Setup(ctx, pgResource.DBDsn)
	require.NoError(t, err)

	sourcesManager := source.New(
		c,
		logger.NOP,
		db,
		n,
	)

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		tenantManager.Run(gCtx)
		return nil
	})
	g.Go(func() error {
		bcManager.Start(gCtx)
		return nil
	})
	g.Go(func() error {
		return sourcesManager.Run(gCtx)
	})

	setupCh := make(chan struct{})
	go func() {
		require.NoError(t, g.Wait())
		close(setupCh)
	}()

	now := time.Now().Truncate(time.Second).UTC()
	stagingRepo := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
		return now
	}))
	uploadsRepo := repo.NewUploads(db, repo.WithNow(func() time.Time {
		return now
	}))
	tableUploadsRepo := repo.NewTableUploads(db, repo.WithNow(func() time.Time {
		return now
	}))

	stagingFile := model.StagingFile{
		WorkspaceID:           workspaceID,
		Location:              "s3://bucket/path/to/file",
		SourceID:              sourceID,
		DestinationID:         destinationID,
		Status:                warehouseutils.StagingFileWaitingState,
		Error:                 fmt.Errorf("dummy error"),
		FirstEventAt:          now.Add(time.Second),
		UseRudderStorage:      true,
		DestinationRevisionID: "destination_revision_id",
		TotalEvents:           100,
		SourceTaskRunID:       sourceTaskRunID,
		SourceJobID:           sourceJobID,
		SourceJobRunID:        sourceJobRunID,
		TimeWindow:            time.Date(1993, 8, 1, 3, 0, 0, 0, time.UTC),
	}.WithSchema([]byte(`{"type": "object"}`))

	failedStagingID, err := stagingRepo.Insert(ctx, &stagingFile)
	require.NoError(t, err)
	pendingStagingID, err := stagingRepo.Insert(ctx, &stagingFile)
	require.NoError(t, err)

	_, err = uploadsRepo.CreateWithStagingFiles(ctx, model.Upload{
		WorkspaceID:     workspaceID,
		Namespace:       namespace,
		SourceID:        sourceID,
		DestinationID:   destinationID,
		DestinationType: destinationType,
		Status:          model.Aborted,
		SourceJobRunID:  sourceJobRunID,
		SourceTaskRunID: sourceTaskRunID,
	}, []*model.StagingFile{{
		ID:              failedStagingID,
		SourceID:        sourceID,
		DestinationID:   destinationID,
		SourceJobRunID:  sourceJobRunID,
		SourceTaskRunID: sourceTaskRunID,
	}})
	require.NoError(t, err)
	uploadID, err := uploadsRepo.CreateWithStagingFiles(ctx, model.Upload{
		WorkspaceID:     workspaceID,
		Namespace:       namespace,
		SourceID:        sourceID,
		DestinationID:   destinationID,
		DestinationType: destinationType,
		Status:          model.Waiting,
		SourceJobRunID:  sourceJobRunID,
		SourceTaskRunID: sourceTaskRunID,
	}, []*model.StagingFile{{
		ID:              pendingStagingID,
		SourceID:        sourceID,
		DestinationID:   destinationID,
		SourceJobRunID:  sourceJobRunID,
		SourceTaskRunID: sourceTaskRunID,
	}})
	require.NoError(t, err)

	err = tableUploadsRepo.Insert(ctx, uploadID, []string{
		"test_table_1",
		"test_table_2",
		"test_table_3",
		"test_table_4",
		"test_table_5",

		"rudder_discards",
		"rudder_identity_mappings",
		"rudder_identity_merge_rules",
	})
	require.NoError(t, err)

	for pendingStagingFiles := 0; pendingStagingFiles < 5; pendingStagingFiles++ {
		_, err = stagingRepo.Insert(ctx, &stagingFile)
		require.NoError(t, err)
	}

	schemaRepo := repo.NewWHSchemas(db)
	_, err = schemaRepo.Insert(ctx, &model.WHSchema{
		SourceID:        sourceID,
		Namespace:       namespace,
		DestinationID:   destinationID,
		DestinationType: destinationType,
		Schema: model.Schema{
			"test_table": {
				"test_column": "test_data_type",
			},
		},
		CreatedAt: now,
		UpdatedAt: now,
		ExpiresAt: now.Add(1 * time.Hour),
	})
	require.NoError(t, err)

	t.Run("health handler", func(t *testing.T) {
		testCases := []struct {
			name        string
			mode        string
			runningMode string
			response    map[string]string
		}{
			{
				name: "embedded",
				mode: config.EmbeddedMode,
				response: map[string]string{
					"acceptingEvents": "TRUE",
					"db":              "UP",
					"notifier":        "UP",
					"server":          "UP",
					"warehouseMode":   "EMBEDDED",
				},
			},
			{
				name: "master",
				mode: config.MasterMode,
				response: map[string]string{
					"acceptingEvents": "TRUE",
					"db":              "UP",
					"notifier":        "UP",
					"server":          "UP",
					"warehouseMode":   "MASTER",
				},
			},
			{
				name:        "degraded master",
				mode:        config.MasterMode,
				runningMode: "degraded",
				response: map[string]string{
					"acceptingEvents": "TRUE",
					"db":              "UP",
					"notifier":        "",
					"server":          "UP",
					"warehouseMode":   "MASTER",
				},
			},
			{
				name: "master and slave",
				mode: config.MasterSlaveMode,
				response: map[string]string{
					"acceptingEvents": "TRUE",
					"db":              "UP",
					"notifier":        "UP",
					"server":          "UP",
					"warehouseMode":   "MASTER_AND_SLAVE",
				},
			},
			{
				name: "embedded master",
				mode: config.EmbeddedMasterMode,
				response: map[string]string{
					"acceptingEvents": "TRUE",
					"db":              "UP",
					"notifier":        "UP",
					"server":          "UP",
					"warehouseMode":   "EMBEDDED_MASTER",
				},
			},
			{
				name: "slave",
				mode: config.SlaveMode,
				response: map[string]string{
					"acceptingEvents": "TRUE",
					"db":              "",
					"notifier":        "UP",
					"server":          "UP",
					"warehouseMode":   "SLAVE",
				},
			},
			{
				name: "off",
				mode: config.OffMode,
				response: map[string]string{
					"acceptingEvents": "TRUE",
					"db":              "",
					"notifier":        "UP",
					"server":          "UP",
					"warehouseMode":   "OFF",
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				req := httptest.NewRequest(http.MethodGet, "/health", nil)
				resp := httptest.NewRecorder()

				c := config.New()
				c.Set("Warehouse.runningMode", tc.runningMode)

				a := NewApi(tc.mode, c, logger.NOP, stats.NOP, mockBackendConfig, db, n, tenantManager, bcManager, sourcesManager, triggerStore)
				a.healthHandler(resp, req)

				var healthBody map[string]string
				err = json.NewDecoder(resp.Body).Decode(&healthBody)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.Code)

				require.EqualValues(t, healthBody, tc.response)
			})
		}
	})

	t.Run("pending events handler", func(t *testing.T) {
		t.Run("invalid payload", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/warehouse/pending-events", bytes.NewReader([]byte(`"Invalid payload"`)))
			resp := httptest.NewRecorder()

			a := NewApi(config.MasterMode, config.New(), logger.NOP, stats.NOP, mockBackendConfig, db, n, tenantManager, bcManager, sourcesManager, triggerStore)
			a.pendingEventsHandler(resp, req)
			require.Equal(t, http.StatusBadRequest, resp.Code)

			b, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, "invalid JSON in request body\n", string(b))
		})

		t.Run("empty source id or task run id", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/warehouse/pending-events", bytes.NewReader([]byte(`
				{
				  "source_id": "",
				  "task_run_id": ""
				}
			`)))
			resp := httptest.NewRecorder()

			a := NewApi(config.MasterMode, config.New(), logger.NOP, stats.NOP, mockBackendConfig, db, n, tenantManager, bcManager, sourcesManager, triggerStore)
			a.pendingEventsHandler(resp, req)
			require.Equal(t, http.StatusBadRequest, resp.Code)

			b, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, "empty source or task run id\n", string(b))
		})

		t.Run("workspace not found", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/warehouse/pending-events", bytes.NewReader([]byte(`
				{
				  "source_id": "unknown_source_id",
				  "task_run_id": "unknown_task_run_id"
				}
			`)))
			resp := httptest.NewRecorder()

			a := NewApi(config.MasterMode, config.New(), logger.NOP, stats.NOP, mockBackendConfig, db, n, tenantManager, bcManager, sourcesManager, triggerStore)
			a.pendingEventsHandler(resp, req)
			require.Equal(t, http.StatusBadRequest, resp.Code)

			b, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, "workspace from source not found\n", string(b))
		})

		t.Run("degraded workspace", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/warehouse/pending-events", bytes.NewReader([]byte(`
				{
				  "source_id": "degraded_test_source_id",
				  "task_run_id": "degraded_task_run_id"
				}
			`)))
			resp := httptest.NewRecorder()

			a := NewApi(config.MasterMode, config.New(), logger.NOP, stats.NOP, mockBackendConfig, db, n, tenantManager, bcManager, sourcesManager, triggerStore)
			a.pendingEventsHandler(resp, req)
			require.Equal(t, http.StatusServiceUnavailable, resp.Code)

			b, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, "workspace is degraded\n", string(b))
		})

		t.Run("pending events available with without trigger uploads", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/warehouse/pending-events?triggerUpload=false", bytes.NewReader([]byte(`
				{
				  "source_id": "test_source_id",
				  "task_run_id": "test_source_task_run_id"
				}
			`)))
			resp := httptest.NewRecorder()

			a := NewApi(config.MasterMode, config.New(), logger.NOP, stats.NOP, mockBackendConfig, db, n, tenantManager, bcManager, sourcesManager, triggerStore)
			a.pendingEventsHandler(resp, req)
			require.Equal(t, http.StatusOK, resp.Code)

			var pendingEventsResponse pendingEventsResponse
			err := json.NewDecoder(resp.Body).Decode(&pendingEventsResponse)
			require.NoError(t, err)

			require.EqualValues(t, pendingEventsResponse.PendingEvents, true)
			require.EqualValues(t, pendingEventsResponse.PendingUploadCount, 1)
			require.EqualValues(t, pendingEventsResponse.PendingStagingFilesCount, 5)
			require.EqualValues(t, pendingEventsResponse.AbortedEvents, true)

			_, isTriggered := triggerStore.Load("POSTGRES:test_source_id:test_destination_id")
			require.False(t, isTriggered)
		})

		t.Run("pending events available with trigger uploads", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/warehouse/pending-events?triggerUpload=true", bytes.NewReader([]byte(`
				{
				  "source_id": "test_source_id",
				  "task_run_id": "test_source_task_run_id"
				}
			`)))
			resp := httptest.NewRecorder()

			a := NewApi(config.MasterMode, config.New(), logger.NOP, stats.NOP, mockBackendConfig, db, n, tenantManager, bcManager, sourcesManager, triggerStore)
			a.pendingEventsHandler(resp, req)
			require.Equal(t, http.StatusOK, resp.Code)

			var pendingEventsResponse pendingEventsResponse
			err := json.NewDecoder(resp.Body).Decode(&pendingEventsResponse)
			require.NoError(t, err)

			defer func() {
				triggerStore.Delete("POSTGRES:test_source_id:test_destination_id")
			}()

			require.EqualValues(t, pendingEventsResponse.PendingEvents, true)
			require.EqualValues(t, pendingEventsResponse.PendingUploadCount, 1)
			require.EqualValues(t, pendingEventsResponse.PendingStagingFilesCount, 5)
			require.EqualValues(t, pendingEventsResponse.AbortedEvents, true)

			_, isTriggered := triggerStore.Load("POSTGRES:test_source_id:test_destination_id")
			require.True(t, isTriggered)
		})

		t.Run("no pending events available", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/warehouse/pending-events?triggerUpload=true", bytes.NewReader([]byte(`
				{
				  "source_id": "unused_test_source_id",
				  "task_run_id": "unused_test_source_task_run_id"
				}
			`)))
			resp := httptest.NewRecorder()

			a := NewApi(config.MasterMode, config.New(), logger.NOP, stats.NOP, mockBackendConfig, db, n, tenantManager, bcManager, sourcesManager, triggerStore)
			a.pendingEventsHandler(resp, req)
			require.Equal(t, http.StatusOK, resp.Code)

			var pendingEventsResponse pendingEventsResponse
			err := json.NewDecoder(resp.Body).Decode(&pendingEventsResponse)
			require.NoError(t, err)
			require.EqualValues(t, pendingEventsResponse.PendingEvents, false)
			require.EqualValues(t, pendingEventsResponse.PendingUploadCount, 0)
			require.EqualValues(t, pendingEventsResponse.PendingStagingFilesCount, 0)
			require.EqualValues(t, pendingEventsResponse.AbortedEvents, false)

			_, isTriggered := triggerStore.Load("POSTGRES:test_source_id:test_destination_id")
			require.False(t, isTriggered)
		})
	})

	t.Run("fetch tables handler", func(t *testing.T) {
		t.Run("invalid payload", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/internal/v1/warehouse/fetch-tables", bytes.NewReader([]byte(`"Invalid payload"`)))
			resp := httptest.NewRecorder()

			a := NewApi(config.MasterMode, config.New(), logger.NOP, stats.NOP, mockBackendConfig, db, n, tenantManager, bcManager, sourcesManager, triggerStore)
			a.fetchTablesHandler(resp, req)
			require.Equal(t, http.StatusBadRequest, resp.Code)

			b, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, "invalid JSON in request body\n", string(b))
		})

		t.Run("empty connections", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/internal/v1/warehouse/fetch-tables", bytes.NewReader([]byte(`
				{
				  "connections": []
				}
			`)))
			resp := httptest.NewRecorder()

			a := NewApi(config.MasterMode, config.New(), logger.NOP, stats.NOP, mockBackendConfig, db, n, tenantManager, bcManager, sourcesManager, triggerStore)
			a.fetchTablesHandler(resp, req)
			require.Equal(t, http.StatusInternalServerError, resp.Code)

			b, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, "can't fetch tables\n", string(b))
		})

		t.Run("succeed", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/internal/v1/warehouse/fetch-tables", bytes.NewReader([]byte(`
				{
				  "connections": [
					{
					  "source_id": "test_source_id",
					  "destination_id": "test_destination_id"
					}
				  ]
				}
			`)))
			resp := httptest.NewRecorder()

			a := NewApi(config.MasterMode, config.New(), logger.NOP, stats.NOP, mockBackendConfig, db, n, tenantManager, bcManager, sourcesManager, triggerStore)
			a.fetchTablesHandler(resp, req)
			require.Equal(t, http.StatusOK, resp.Code)

			var ftr fetchTablesResponse
			err = json.NewDecoder(resp.Body).Decode(&ftr)
			require.NoError(t, err)
			require.EqualValues(t, ftr.ConnectionsTables, []warehouseutils.FetchTableInfo{
				{
					SourceID:      sourceID,
					DestinationID: destinationID,
					Namespace:     namespace,
					Tables:        []string{"test_table"},
				},
			})
		})
	})

	t.Run("trigger uploads handler", func(t *testing.T) {
		t.Run("invalid payload", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/warehouse/trigger-upload", bytes.NewReader([]byte(`"Invalid payload"`)))
			resp := httptest.NewRecorder()

			a := NewApi(config.MasterMode, config.New(), logger.NOP, stats.NOP, mockBackendConfig, db, n, tenantManager, bcManager, sourcesManager, triggerStore)
			a.triggerUploadHandler(resp, req)
			require.Equal(t, http.StatusBadRequest, resp.Code)

			b, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, "invalid JSON in request body\n", string(b))
		})

		t.Run("workspace not found", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/warehouse/trigger-upload", bytes.NewReader([]byte(`
				{
				  "source_id": "unknown_source_id",
				  "destination_id": "unknown_destination_id"
				}
			`)))
			resp := httptest.NewRecorder()

			a := NewApi(config.MasterMode, config.New(), logger.NOP, stats.NOP, mockBackendConfig, db, n, tenantManager, bcManager, sourcesManager, triggerStore)
			a.triggerUploadHandler(resp, req)
			require.Equal(t, http.StatusBadRequest, resp.Code)

			b, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, "workspace from source not found\n", string(b))
		})

		t.Run("degraded workspaces", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/warehouse/trigger-upload", bytes.NewReader([]byte(`
				{
				  "source_id": "degraded_test_source_id",
				  "destination_id": "degraded_test_destination_id"
				}
			`)))
			resp := httptest.NewRecorder()

			a := NewApi(config.MasterMode, config.New(), logger.NOP, stats.NOP, mockBackendConfig, db, n, tenantManager, bcManager, sourcesManager, triggerStore)
			a.triggerUploadHandler(resp, req)

			require.Equal(t, http.StatusServiceUnavailable, resp.Code)
			b, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, "workspace is degraded\n", string(b))
		})

		t.Run("no warehouses", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/warehouse/trigger-upload", bytes.NewReader([]byte(`
				{
				  "source_id": "unsupported_test_source_id",
				  "destination_id": "unsupported_test_destination_id"
				}
			`)))
			resp := httptest.NewRecorder()

			a := NewApi(config.MasterMode, config.New(), logger.NOP, stats.NOP, mockBackendConfig, db, n, tenantManager, bcManager, sourcesManager, triggerStore)
			a.triggerUploadHandler(resp, req)

			require.Equal(t, http.StatusBadRequest, resp.Code)

			_, isTriggered := triggerStore.Load("POSTGRES:unsupported_test_source_id:unsupported_test_destination_id")
			require.False(t, isTriggered)
		})

		t.Run("without destination id", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/warehouse/trigger-upload", bytes.NewReader([]byte(`
				{
				  "source_id": "test_source_id",
				  "destination_id": ""
				}
			`)))
			resp := httptest.NewRecorder()

			a := NewApi(config.MasterMode, config.New(), logger.NOP, stats.NOP, mockBackendConfig, db, n, tenantManager, bcManager, sourcesManager, triggerStore)
			a.triggerUploadHandler(resp, req)

			require.Equal(t, http.StatusOK, resp.Code)

			defer func() {
				triggerStore.Delete("POSTGRES:test_source_id:test_destination_id")
			}()

			_, isTriggered := triggerStore.Load("POSTGRES:test_source_id:test_destination_id")
			require.True(t, isTriggered)
		})

		t.Run("with destination id", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/warehouse/trigger-upload", bytes.NewReader([]byte(`
				{
				  "source_id": "test_source_id",
				  "destination_id": "test_destination_id"
				}
			`)))
			resp := httptest.NewRecorder()

			a := NewApi(config.MasterMode, config.New(), logger.NOP, stats.NOP, mockBackendConfig, db, n, tenantManager, bcManager, sourcesManager, triggerStore)
			a.triggerUploadHandler(resp, req)

			require.Equal(t, http.StatusOK, resp.Code)

			defer func() {
				triggerStore.Delete("POSTGRES:test_source_id:test_destination_id")
			}()

			_, isTriggered := triggerStore.Load("POSTGRES:test_source_id:test_destination_id")
			require.True(t, isTriggered)
		})
	})

	t.Run("endpoints", func(t *testing.T) {
		t.Run("normal mode", func(t *testing.T) {
			webPort, err := kithelper.GetFreePort()
			require.NoError(t, err)

			c := config.New()
			c.Set("Warehouse.webPort", webPort)

			srvCtx, stopServer := context.WithCancel(ctx)

			a := NewApi(config.MasterMode, c, logger.NOP, stats.NOP, mockBackendConfig, db, n, tenantManager, bcManager, sourcesManager, triggerStore)

			serverSetupCh := make(chan struct{})
			go func() {
				require.NoError(t, a.Start(srvCtx))

				close(serverSetupCh)
			}()

			serverURL := fmt.Sprintf("http://localhost:%d", webPort)

			t.Run("health", func(t *testing.T) {
				health.WaitUntilReady(ctx, t, fmt.Sprintf("%s/health", serverURL), time.Second*10, time.Millisecond*100, t.Name())
			})

			t.Run("process", func(t *testing.T) {
				pendingEventsURL := fmt.Sprintf("%s/v1/process", serverURL)
				req, err := http.NewRequest(http.MethodPost, pendingEventsURL, bytes.NewReader([]byte(`
				{
				  "WorkspaceID": "test_workspace_id",
				  "Schema": {
					"test_table": {
					  "test_column": "test_data_type"
					}
				  },
				  "BatchDestination": {
					"Source": {
					  "ID": "test_source_id"
					},
					"Destination": {
					  "ID": "test_destination_id"
					}
				  },
				  "Location": "rudder-warehouse-staging-logs/279L3gEKqwruBoKGsXZtSVX7vIy/2022-11-08/1667913810.279L3gEKqwruBoKGsXZtSVX7vIy.7a6e7785-7a75-4345-8d3c-d7a1ce49a43f.json.gz",
				  "FirstEventAt": "2022-11-08T13:23:07Z",
				  "LastEventAt": "2022-11-08T13:23:07Z",
				  "TotalEvents": 2,
				  "TotalBytes": 2000,
				  "UseRudderStorage": false,
				  "DestinationRevisionID": "2H1cLBvL3v0prRBNzpe8D34XTzU",
				  "SourceTaskRunID": "test_source_task_run_id",
				  "SourceJobID": "test_source_job_id",
				  "SourceJobRunID": "test_source_job_run_id",
				  "TimeWindow": "0001-01-01T00:40:00Z"
				}
			`)))
				require.NoError(t, err)
				req.Header.Set("Content-Type", "application/json")

				resp, err := (&http.Client{}).Do(req)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.StatusCode)

				defer func() {
					httputil.CloseResponse(resp)
				}()
			})

			t.Run("pending events", func(t *testing.T) {
				pendingEventsURL := fmt.Sprintf("%s/v1/warehouse/pending-events?triggerUpload=true", serverURL)
				req, err := http.NewRequest(http.MethodPost, pendingEventsURL, bytes.NewReader([]byte(`
				{
				  "source_id": "test_source_id",
				  "task_run_id": "test_source_task_run_id"
				}
			`)))
				require.NoError(t, err)
				req.Header.Set("Content-Type", "application/json")

				resp, err := (&http.Client{}).Do(req)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.StatusCode)

				defer func() {
					httputil.CloseResponse(resp)
				}()
			})

			t.Run("trigger upload", func(t *testing.T) {
				triggerUploadURL := fmt.Sprintf("%s/v1/warehouse/trigger-upload", serverURL)
				req, err := http.NewRequest(http.MethodPost, triggerUploadURL, bytes.NewReader([]byte(`
				{
				  "source_id": "test_source_id",
				  "destination_id": "test_destination_id"
				}
			`)))
				require.NoError(t, err)
				req.Header.Set("Content-Type", "application/json")

				resp, err := (&http.Client{}).Do(req)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.StatusCode)

				defer func() {
					httputil.CloseResponse(resp)
				}()
			})

			t.Run("fetch tables", func(t *testing.T) {
				for _, u := range []string{
					fmt.Sprintf("%s/v1/warehouse/fetch-tables", serverURL),
					fmt.Sprintf("%s/internal/v1/warehouse/fetch-tables", serverURL),
				} {
					req, err := http.NewRequest(http.MethodGet, u, bytes.NewReader([]byte(`
				{
				  "connections": [
					{
					  "source_id": "test_source_id",
					  "destination_id": "test_destination_id"
					}
				  ]
				}
			`)))
					require.NoError(t, err)
					req.Header.Set("Content-Type", "application/json")

					resp, err := (&http.Client{}).Do(req)
					require.NoError(t, err)
					require.Equal(t, http.StatusOK, resp.StatusCode)

					t.Cleanup(func() {
						httputil.CloseResponse(resp)
					})
				}
			})

			t.Run("jobs", func(t *testing.T) {
				jobsURL := fmt.Sprintf("%s/v1/warehouse/jobs", serverURL)
				req, err := http.NewRequest(http.MethodPost, jobsURL, bytes.NewReader([]byte(`
				{
				  "source_id": "test_source_id",
				  "destination_id": "test_destination_id",
				  "job_run_id": "test_source_job_run_id",
				  "task_run_id": "test_source_task_run_id",
				  "async_job_type": "deletebyjobrunid"
				}
			`)))
				require.NoError(t, err)
				req.Header.Set("Content-Type", "application/json")

				resp, err := (&http.Client{}).Do(req)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.StatusCode)

				defer func() {
					httputil.CloseResponse(resp)
				}()
			})

			t.Run("jobs status", func(t *testing.T) {
				qp := url.Values{}
				qp.Add("task_run_id", sourceTaskRunID)
				qp.Add("job_run_id", sourceJobRunID)
				qp.Add("source_id", sourceID)
				qp.Add("destination_id", destinationID)
				qp.Add("workspace_id", workspaceID)

				jobsStatusURL := fmt.Sprintf("%s/v1/warehouse/jobs/status?"+qp.Encode(), serverURL)
				req, err := http.NewRequest(http.MethodGet, jobsStatusURL, nil)
				require.NoError(t, err)
				req.Header.Set("Content-Type", "application/json")

				resp, err := (&http.Client{}).Do(req)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.StatusCode)

				defer func() {
					httputil.CloseResponse(resp)
				}()
			})

			stopServer()

			<-serverSetupCh
		})

		t.Run("degraded mode", func(t *testing.T) {
			webPort, err := kithelper.GetFreePort()
			require.NoError(t, err)

			c := config.New()
			c.Set("Warehouse.webPort", webPort)
			c.Set("Warehouse.runningMode", mode.DegradedMode)

			srvCtx, stopServer := context.WithCancel(ctx)

			a := NewApi(config.MasterMode, c, logger.NOP, stats.NOP, mockBackendConfig, db, n, tenantManager, bcManager, sourcesManager, triggerStore)

			serverSetupCh := make(chan struct{})
			go func() {
				require.NoError(t, a.Start(srvCtx))

				close(serverSetupCh)
			}()

			serverURL := fmt.Sprintf("http://localhost:%d", webPort)

			t.Run("health endpoint should work", func(t *testing.T) {
				health.WaitUntilReady(ctx, t, fmt.Sprintf("%s/health", serverURL), time.Second*10, time.Millisecond*100, t.Name())
			})

			t.Run("other endpoints should fail", func(t *testing.T) {
				testCases := []struct {
					name   string
					url    string
					method string
					body   io.Reader
				}{
					{
						name:   "process",
						url:    fmt.Sprintf("%s/v1/process", serverURL),
						method: http.MethodPost,
						body:   bytes.NewReader([]byte(`{}`)),
					},
					{
						name:   "pending events",
						url:    fmt.Sprintf("%s/v1/warehouse/pending-events", serverURL),
						method: http.MethodPost,
						body:   bytes.NewReader([]byte(`{}`)),
					},
					{
						name:   "trigger upload",
						url:    fmt.Sprintf("%s/v1/warehouse/trigger-upload", serverURL),
						method: http.MethodPost,
						body:   bytes.NewReader([]byte(`{}`)),
					},
					{
						name:   "jobs",
						url:    fmt.Sprintf("%s/v1/warehouse/jobs", serverURL),
						method: http.MethodPost,
						body:   bytes.NewReader([]byte(`{}`)),
					},
					{
						name:   "jobs status",
						url:    fmt.Sprintf("%s/v1/warehouse/jobs/status", serverURL),
						method: http.MethodGet,
						body:   nil,
					},
					{
						name:   "fetch tables",
						url:    fmt.Sprintf("%s/v1/warehouse/fetch-tables", serverURL),
						method: http.MethodGet,
						body:   nil,
					},
					{
						name:   "internal fetch tables",
						url:    fmt.Sprintf("%s/internal/v1/warehouse/fetch-tables", serverURL),
						method: http.MethodGet,
						body:   nil,
					},
				}

				for _, tc := range testCases {
					t.Run(tc.name, func(t *testing.T) {
						req, err := http.NewRequest(tc.method, tc.url, tc.body)
						require.NoError(t, err)

						resp, err := (&http.Client{}).Do(req)
						require.NoError(t, err)
						require.Equal(t, http.StatusNotFound, resp.StatusCode)

						defer func() {
							httputil.CloseResponse(resp)
						}()
					})
				}
			})

			stopServer()

			<-serverSetupCh
		})
	})

	stopTest()

	<-setupCh
}
