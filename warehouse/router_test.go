package warehouse

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/enterprise/reporting"
	mocksApp "github.com/rudderlabs/rudder-server/mocks/app"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/services/controlplane"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	"github.com/golang/mock/gomock"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
)

func TestRouter(t *testing.T) {
	pgnotifier.Init()
	Init()
	Init4()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	report := &reporting.Factory{}
	report.Setup(&backendconfig.NOOP{})

	mockCtrl := gomock.NewController(t)
	mockApp := mocksApp.NewMockApp(mockCtrl)
	mockApp.EXPECT().Features().Return(&app.Features{
		Reporting: report,
	}).AnyTimes()

	application = mockApp
	pkgLogger = logger.NOP

	sourceID := "test-source-id"
	destinationID := "test-destination-id"
	namespace := "test-namespace"
	workspaceID := "test-workspace-id"
	destinationType := "test-destination-type"
	workspaceIdentifier := "test-workspace-identifier"

	createStagingFiles := func(t *testing.T, ctx context.Context, repoStaging *repo.StagingFiles, workspaceID, sourceID, destinationID string) []*model.StagingFileWithSchema {
		var stagingFiles []*model.StagingFileWithSchema
		for i := 0; i < 10; i++ {
			stagingFile := &model.StagingFileWithSchema{
				StagingFile: model.StagingFile{
					WorkspaceID:   workspaceID,
					SourceID:      sourceID,
					DestinationID: destinationID,
				},
			}
			stagingID, err := repoStaging.Insert(ctx, stagingFile)
			require.NoError(t, err)

			stagingFile.ID = stagingID
			stagingFiles = append(stagingFiles, stagingFile)
		}
		return stagingFiles
	}

	t.Run("Graceful shutdown", func(t *testing.T) {
		pgResource, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)

		t.Log("db:", pgResource.DBDsn)

		err = (&migrator.Migrator{
			Handle:          pgResource.DB,
			MigrationsTable: "wh_schema_migrations",
		}).Migrate("warehouse")
		require.NoError(t, err)

		db := sqlmiddleware.New(pgResource.DB)
		wrappedDBHandle = db

		notifier, err = pgnotifier.New(workspaceIdentifier, pgResource.DBDsn)
		require.NoError(t, err)

		tenantManager := &multitenant.Manager{
			BackendConfig: mocksBackendConfig.NewMockBackendConfig(gomock.NewController(t)),
		}

		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer s.Close()

		cp := controlplane.NewClient(s.URL, &identity.Namespace{},
			controlplane.WithHTTPClient(s.Client()),
		)
		bcm := newBackendConfigManager(config.Default, db, tenantManager, logger.NOP)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		r, err := NewRouter(
			ctx,
			destinationType,
			config.Default,
			logger.NOP,
			stats.Default,
			db,
			notifier,
			tenantManager,
			cp,
			bcm,
		)
		require.NoError(t, err)

		cancel()
		require.NoError(t, r.Shutdown())
	})

	t.Run("CreateJobs", func(t *testing.T) {
		pgResource, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)

		t.Log("db:", pgResource.DBDsn)

		err = (&migrator.Migrator{
			Handle:          pgResource.DB,
			MigrationsTable: "wh_schema_migrations",
		}).Migrate("warehouse")
		require.NoError(t, err)

		db := sqlmiddleware.New(pgResource.DB)
		wrappedDBHandle = db

		now := time.Date(2021, 1, 1, 0, 0, 3, 0, time.UTC)
		repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
			return now
		}))
		repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
			return now
		}))

		ctx := context.Background()
		warehouse := model.Warehouse{
			WorkspaceID: workspaceID,
			Source: backendconfig.SourceT{
				ID: sourceID,
			},
			Destination: backendconfig.DestinationT{
				ID: destinationID,
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: destinationType,
				},
				Config: map[string]interface{}{
					"namespace": namespace,
				},
			},
			Namespace:  "test_namespace",
			Identifier: "RS:test-source-id:test-destination-id-create-jobs",
		}

		r := Router{}
		r.dbHandle = db
		r.warehouseDBHandle = NewWarehouseDB(db)
		r.uploadRepo = repoUpload
		r.stagingRepo = repoStaging
		r.stats = memstats.New()
		r.config.stagingFilesBatchSize = 100
		r.config.warehouseSyncFreqIgnore = true
		r.config.enableJitterForSyncs = true
		r.destType = destinationType
		r.logger = logger.NOP

		t.Run("no staging files", func(t *testing.T) {
			err = r.createJobs(ctx, warehouse)
			require.NoError(t, err)

			upload, err := repoUpload.Get(ctx, 1)
			require.Equal(t, err, model.ErrUploadNotFound)
			require.Equal(t, upload, model.Upload{})
		})

		t.Run("with staging files", func(t *testing.T) {
			stagingFiles := createStagingFiles(t, ctx, repoStaging, workspaceID, sourceID, destinationID)

			t.Run("without existing upload", func(t *testing.T) {
				err = r.createJobs(ctx, warehouse)
				require.NoError(t, err)

				count, err := repoUpload.Count(ctx)
				require.NoError(t, err)
				require.Equal(t, count, int64(1))

				upload, err := repoUpload.Get(ctx, 1)
				require.NoError(t, err)
				require.Equal(t, upload.ID, int64(1))
				require.Equal(t, upload.StagingFileStartID, stagingFiles[0].ID)
				require.Equal(t, upload.StagingFileEndID, stagingFiles[len(stagingFiles)-1].ID)
				require.Equal(t, upload.WorkspaceID, workspaceID)
				require.Equal(t, upload.SourceID, sourceID)
				require.Equal(t, upload.DestinationID, destinationID)
				require.Equal(t, upload.DestinationType, destinationType)
				require.Equal(t, upload.Namespace, "test_namespace")
				require.Equal(t, upload.Status, model.Waiting)
				require.Equal(t, upload.Priority, 100)
			})

			t.Run("with existing upload", func(t *testing.T) {
				err = r.createJobs(ctx, warehouse)
				require.NoError(t, err)

				count, err := repoUpload.Count(ctx)
				require.NoError(t, err)
				require.Equal(t, count, int64(1))
			})

			t.Run("merge existing upload", func(t *testing.T) {
				setLastProcessedMarker(warehouse, now.Add(-time.Hour))

				stagingFiles := append(stagingFiles, createStagingFiles(t, ctx, repoStaging, workspaceID, sourceID, destinationID)...)

				err = r.createJobs(ctx, warehouse)
				require.NoError(t, err)

				count, err := repoUpload.Count(ctx)
				require.NoError(t, err)
				require.Equal(t, count, int64(1))

				upload, err := repoUpload.Get(ctx, 2)
				require.NoError(t, err)
				require.Equal(t, upload.ID, int64(2))
				require.Equal(t, upload.StagingFileStartID, stagingFiles[0].ID)
				require.Equal(t, upload.StagingFileEndID, stagingFiles[len(stagingFiles)-1].ID)
				require.Equal(t, upload.WorkspaceID, workspaceID)
				require.Equal(t, upload.SourceID, sourceID)
				require.Equal(t, upload.DestinationID, destinationID)
				require.Equal(t, upload.DestinationType, destinationType)
				require.Equal(t, upload.Namespace, "test_namespace")
				require.Equal(t, upload.Status, model.Waiting)
				require.Equal(t, upload.Priority, 100)
			})

			t.Run("upload triggered", func(t *testing.T) {
				setLastProcessedMarker(warehouse, now.Add(-time.Hour))
				triggerUpload(warehouse)

				stagingFiles := append(stagingFiles, createStagingFiles(t, ctx, repoStaging, workspaceID, sourceID, destinationID)...)

				err = r.createJobs(ctx, warehouse)
				require.NoError(t, err)

				count, err := repoUpload.Count(ctx)
				require.NoError(t, err)
				require.Equal(t, count, int64(1))

				upload, err := repoUpload.Get(ctx, 3)
				require.NoError(t, err)
				require.Equal(t, upload.ID, int64(3))
				require.Equal(t, upload.StagingFileStartID, stagingFiles[0].ID)
				require.Equal(t, upload.StagingFileEndID, stagingFiles[len(stagingFiles)-1].ID)
				require.Equal(t, upload.WorkspaceID, workspaceID)
				require.Equal(t, upload.SourceID, sourceID)
				require.Equal(t, upload.DestinationID, destinationID)
				require.Equal(t, upload.DestinationType, destinationType)
				require.Equal(t, upload.Namespace, "test_namespace")
				require.Equal(t, upload.Status, model.Waiting)
				require.Equal(t, upload.Priority, 50)
			})
		})
	})

	t.Run("Scheduler", func(t *testing.T) {
		pgResource, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)

		t.Log("db:", pgResource.DBDsn)

		err = (&migrator.Migrator{
			Handle:          pgResource.DB,
			MigrationsTable: "wh_schema_migrations",
		}).Migrate("warehouse")
		require.NoError(t, err)

		db := sqlmiddleware.New(pgResource.DB)
		wrappedDBHandle = db

		now := time.Date(2021, 1, 1, 0, 0, 3, 0, time.UTC)
		repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
			return now
		}))
		repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
			return now
		}))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		warehouse := model.Warehouse{
			WorkspaceID: workspaceID,
			Source: backendconfig.SourceT{
				ID: sourceID,
			},
			Destination: backendconfig.DestinationT{
				ID: destinationID,
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: destinationType,
				},
				Config: map[string]interface{}{
					"namespace": namespace,
				},
			},
			Namespace:  "test_namespace",
			Identifier: "RS:test-source-id:test-destination-id-scheduler",
		}

		r := Router{}
		r.dbHandle = db
		r.warehouseDBHandle = NewWarehouseDB(db)
		r.uploadRepo = repoUpload
		r.stagingRepo = repoStaging
		r.stats = memstats.New()
		r.config.stagingFilesBatchSize = 100
		r.config.warehouseSyncFreqIgnore = true
		r.config.enableJitterForSyncs = true
		r.config.mainLoopSleep = time.Millisecond * 100
		r.config.maxParallelJobCreation = 100
		r.destType = destinationType
		r.logger = logger.NOP
		r.warehouses = []model.Warehouse{warehouse}

		r.Enable()

		stagingFiles := createStagingFiles(t, ctx, repoStaging, workspaceID, sourceID, destinationID)

		go func() {
			r.mainLoop(ctx)
		}()

		require.Eventually(t, func() bool {
			count, err := repoUpload.Count(ctx)
			require.NoError(t, err)

			return count != 0
		},
			time.Second*10,
			time.Millisecond*100,
		)

		upload, err := repoUpload.Get(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, upload.ID, int64(1))
		require.Equal(t, upload.StagingFileStartID, stagingFiles[0].ID)
		require.Equal(t, upload.StagingFileEndID, stagingFiles[len(stagingFiles)-1].ID)
		require.Equal(t, upload.WorkspaceID, workspaceID)
		require.Equal(t, upload.SourceID, sourceID)
		require.Equal(t, upload.DestinationID, destinationID)
		require.Equal(t, upload.DestinationType, destinationType)
		require.Equal(t, upload.Namespace, "test_namespace")
		require.Equal(t, upload.Status, model.Waiting)
		require.Equal(t, upload.Priority, 100)
	})

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
		wrappedDBHandle = db

		now := time.Date(2021, 1, 1, 0, 0, 3, 0, time.UTC)
		repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
			return now
		}))
		repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
			return now
		}))

		ctx := context.Background()
		warehouse := model.Warehouse{
			WorkspaceID: workspaceID,
			Source: backendconfig.SourceT{
				ID: sourceID,
			},
			Destination: backendconfig.DestinationT{
				ID: destinationID,
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: destinationType,
				},
				Config: map[string]interface{}{
					"namespace": namespace,
				},
			},
			Namespace:  "test_namespace",
			Identifier: "RS:test-source-id:test-destination-id-uploads-to-process",
		}

		ctrl := gomock.NewController(t)

		r := Router{}
		r.dbHandle = db
		r.warehouseDBHandle = NewWarehouseDB(db)
		r.uploadRepo = repoUpload
		r.stagingRepo = repoStaging
		r.stats = memstats.New()
		r.conf = config.Default
		r.config.allowMultipleSourcesForJobsPickup = true
		r.config.stagingFilesBatchSize = 100
		r.config.warehouseSyncFreqIgnore = true
		r.destType = destinationType
		r.logger = logger.NOP
		r.tenantManager = &multitenant.Manager{
			BackendConfig: mocksBackendConfig.NewMockBackendConfig(ctrl),
		}
		r.warehouses = []model.Warehouse{warehouse}
		r.uploadJobFactory = UploadJobFactory{
			stats:    r.stats,
			dbHandle: r.dbHandle,
		}

		t.Run("no uploads", func(t *testing.T) {
			ujs, err := r.uploadsToProcess(ctx, 1, []string{})
			require.NoError(t, err)
			require.Empty(t, ujs)
		})

		createUpload := func(ctx context.Context, destType string) {
			var stagingFiles []*model.StagingFile
			for _, sf := range createStagingFiles(t, ctx, repoStaging, workspaceID, sourceID, destinationID) {
				stagingFiles = append(stagingFiles, &sf.StagingFile)
			}

			_, err = repoUpload.CreateWithStagingFiles(ctx,
				model.Upload{
					WorkspaceID:     workspaceID,
					SourceID:        sourceID,
					DestinationID:   destinationID,
					DestinationType: destType,
				},
				stagingFiles,
			)
			require.NoError(t, err)
		}

		t.Run("with uploads", func(t *testing.T) {
			t.Run("unsupported destination type", func(t *testing.T) {
				r.destType = destinationType

				createUpload(ctx, r.destType)

				ujs, err := r.uploadsToProcess(ctx, 1, []string{})
				require.Errorf(t, err, errors.New("provider of type test-destination-type is not configured for WarehouseManager").Error())
				require.Empty(t, ujs)
			})

			t.Run("supported destination type", func(t *testing.T) {
				r.destType = warehouseutils.RS

				createUpload(ctx, r.destType)

				ujs, err := r.uploadsToProcess(ctx, 1, []string{})
				require.NoError(t, err)
				require.Len(t, ujs, 1)
			})

			t.Run("warehouse model does not exists", func(t *testing.T) {
				r.warehouses = []model.Warehouse{}
				r.destType = warehouseutils.RS

				createUpload(ctx, r.destType)

				ujs, err := r.uploadsToProcess(ctx, 1, []string{})
				require.NoError(t, err)
				require.Len(t, ujs, 0)
			})
		})
	})

	t.Run("Processor", func(t *testing.T) {
		pgResource, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)

		t.Log("db:", pgResource.DBDsn)

		err = (&migrator.Migrator{
			Handle:          pgResource.DB,
			MigrationsTable: "wh_schema_migrations",
		}).Migrate("warehouse")
		require.NoError(t, err)

		db := sqlmiddleware.New(pgResource.DB)
		wrappedDBHandle = db

		now := time.Date(2021, 1, 1, 0, 0, 3, 0, time.UTC)
		repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
			return now
		}))
		repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
			return now
		}))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		warehouse := model.Warehouse{
			WorkspaceID: workspaceID,
			Source: backendconfig.SourceT{
				ID: sourceID,
			},
			Destination: backendconfig.DestinationT{
				ID: destinationID,
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: destinationType,
				},
				Config: map[string]interface{}{
					"namespace": namespace,
				},
			},
			Namespace:  "test_namespace",
			Identifier: "RS:test-source-id:test-destination-id-processor",
		}

		ctrl := gomock.NewController(t)

		r := Router{}
		r.dbHandle = db
		r.warehouseDBHandle = NewWarehouseDB(db)
		r.uploadRepo = repoUpload
		r.stagingRepo = repoStaging
		r.stats = memstats.New()
		r.conf = config.Default
		r.config.allowMultipleSourcesForJobsPickup = true
		r.config.stagingFilesBatchSize = 100
		r.config.warehouseSyncFreqIgnore = true
		r.config.noOfWorkers = 10
		r.config.waitForWorkerSleep = time.Millisecond * 100
		r.config.uploadAllocatorSleep = time.Millisecond * 100
		r.destType = warehouseutils.RS
		r.logger = logger.NOP
		r.tenantManager = &multitenant.Manager{
			BackendConfig: mocksBackendConfig.NewMockBackendConfig(ctrl),
		}
		r.bcManager = newBackendConfigManager(r.conf, r.dbHandle, r.tenantManager, r.logger)
		r.warehouses = []model.Warehouse{warehouse}
		r.uploadJobFactory = UploadJobFactory{
			stats:    r.stats,
			dbHandle: r.dbHandle,
		}
		r.workerChannelMap = map[string]chan *UploadJob{
			r.workerIdentifier(warehouse): make(chan *UploadJob, 1),
		}
		r.inProgressMap = make(map[WorkerIdentifierT][]JobID)

		close(r.bcManager.initialConfigFetched)

		createUpload := func(ctx context.Context) {
			var stagingFiles []*model.StagingFile
			for _, sf := range createStagingFiles(t, ctx, repoStaging, workspaceID, sourceID, destinationID) {
				stagingFiles = append(stagingFiles, &sf.StagingFile)
			}

			_, err = repoUpload.CreateWithStagingFiles(ctx,
				model.Upload{
					WorkspaceID:     workspaceID,
					SourceID:        sourceID,
					DestinationID:   destinationID,
					DestinationType: warehouseutils.RS,
				},
				stagingFiles,
			)
			require.NoError(t, err)
		}

		createUpload(ctx)

		require.Empty(t, r.getInProgressNamespaces())

		go func() {
			r.runUploadJobAllocator(ctx)
		}()

		t.Run("upload job allocator", func(t *testing.T) {
			uploadJob := <-r.workerChannelMap[r.workerIdentifier(warehouse)]
			require.Equal(t, uploadJob.upload.ID, int64(1))
			require.Equal(t, uploadJob.upload.WorkspaceID, workspaceID)
			require.Equal(t, uploadJob.upload.SourceID, sourceID)
			require.Equal(t, uploadJob.upload.DestinationID, destinationID)
			require.Equal(t, r.getInProgressNamespaces(), []string{r.workerIdentifier(warehouse)})
		})

		t.Run("checking in progress namespace", func(t *testing.T) {
			idx, exists := r.checkInProgressMap(int64(1), r.workerIdentifier(warehouse))
			require.True(t, exists)
			require.Equal(t, idx, 0)
		})

		t.Run("removing from in progress namespace", func(t *testing.T) {
			r.removeDestInProgress(warehouse, int64(1))
			uploadJob := <-r.workerChannelMap[r.workerIdentifier(warehouse)]
			require.Equal(t, uploadJob.upload.ID, int64(1))
			require.Equal(t, uploadJob.upload.WorkspaceID, workspaceID)
			require.Equal(t, uploadJob.upload.SourceID, sourceID)
			require.Equal(t, uploadJob.upload.DestinationID, destinationID)
			require.Equal(t, r.getInProgressNamespaces(), []string{r.workerIdentifier(warehouse)})
		})

		t.Run("checking if worker channel map is closed", func(t *testing.T) {
			cancel()

			for _, workerChannel := range r.workerChannelMap {
				_, ok := <-workerChannel
				require.False(t, ok)
			}
		})
	})

	t.Run("Backend config subscriber", func(t *testing.T) {
		pgResource, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)

		t.Log("db:", pgResource.DBDsn)

		err = (&migrator.Migrator{
			Handle:          pgResource.DB,
			MigrationsTable: "wh_schema_migrations",
		}).Migrate("warehouse")
		require.NoError(t, err)

		db := sqlmiddleware.New(pgResource.DB)
		wrappedDBHandle = db

		now := time.Date(2021, 1, 1, 0, 0, 3, 0, time.UTC)
		repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
			return now
		}))
		repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
			return now
		}))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		warehouse := model.Warehouse{
			WorkspaceID: workspaceID,
			Type:        warehouseutils.RS,
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
			Identifier: "RS:test-source-id:test-destination-id",
		}

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

		r := Router{}
		r.dbHandle = db
		r.warehouseDBHandle = NewWarehouseDB(db)
		r.uploadRepo = repoUpload
		r.stagingRepo = repoStaging
		r.stats = memstats.New()
		r.conf = config.Default
		r.logger = logger.NOP
		r.destType = warehouseutils.RS
		r.config.maxConcurrentUploadJobs = 1
		r.tenantManager = &multitenant.Manager{
			BackendConfig: mockBackendConfig,
		}
		r.bcManager = newBackendConfigManager(r.conf, r.dbHandle, r.tenantManager, r.logger)

		go func() {
			r.bcManager.Start(ctx)
		}()
		go func() {
			r.backendConfigSubscriber(ctx)
		}()

		<-r.bcManager.initialConfigFetched

		require.Eventually(t, func() bool {
			r.configSubscriberLock.RLock()
			defer r.configSubscriberLock.RUnlock()
			r.workerChannelMapLock.RLock()
			defer r.workerChannelMapLock.RUnlock()

			if len(r.warehouses) == 0 || len(r.workerChannelMap) == 0 || len(r.workspaceBySourceIDs) == 0 {
				return false
			}

			require.Len(t, r.warehouses, 1)
			require.Len(t, r.workspaceBySourceIDs, 1)
			require.Len(t, r.workerChannelMap, 1)
			require.Equal(t, r.warehouses, []model.Warehouse{warehouse})

			return true
		},
			time.Second*5,
			time.Millisecond*100,
		)
	})
}
