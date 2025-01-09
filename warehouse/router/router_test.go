package router

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/enterprise/reporting"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/services/controlplane"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/services/notifier"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/bcm"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestRouter(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	sourceID := "test-source-id"
	destinationID := "test-destination-id"
	namespace := "test-namespace"
	workspaceID := "test-workspace-id"
	destinationType := warehouseutils.RS
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
		pgResource, err := postgres.Setup(pool, t)
		require.NoError(t, err)

		t.Log("db:", pgResource.DBDsn)

		err = (&migrator.Migrator{
			Handle:          pgResource.DB,
			MigrationsTable: "wh_schema_migrations",
		}).Migrate("warehouse")
		require.NoError(t, err)

		db := sqlmiddleware.New(pgResource.DB)

		ctx := context.Background()

		n := notifier.New(config.New(), logger.NOP, stats.NOP, workspaceIdentifier)
		err = n.Setup(ctx, pgResource.DBDsn)
		require.NoError(t, err)

		ctrl := gomock.NewController(t)

		createUploadAlways := &atomic.Bool{}
		triggerStore := &sync.Map{}
		tenantManager := multitenant.New(config.New(), mocksBackendConfig.NewMockBackendConfig(ctrl))

		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer s.Close()

		cp := controlplane.NewClient(s.URL, &identity.Namespace{},
			controlplane.WithHTTPClient(s.Client()),
		)
		backendConfigManager := bcm.New(config.New(), db, tenantManager, logger.NOP, stats.NOP)

		ctx, cancel := context.WithTimeout(ctx, time.Second*10)
		defer cancel()
		ef := encoding.NewFactory(config.New())

		r := New(
			&reporting.NOOP{},
			destinationType,
			config.New(),
			logger.NOP,
			stats.NOP,
			db,
			n,
			tenantManager,
			cp,
			backendConfigManager,
			ef,
			triggerStore,
			createUploadAlways,
		)
		_ = r.Start(ctx)
	})

	t.Run("CreateJobs", func(t *testing.T) {
		pgResource, err := postgres.Setup(pool, t)
		require.NoError(t, err)

		t.Log("db:", pgResource.DBDsn)

		err = (&migrator.Migrator{
			Handle:          pgResource.DB,
			MigrationsTable: "wh_schema_migrations",
		}).Migrate("warehouse")
		require.NoError(t, err)

		db := sqlmiddleware.New(pgResource.DB)

		now := timeutil.Now()

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
		r.now = func() time.Time {
			return now
		}
		r.db = db
		r.uploadRepo = repoUpload
		r.stagingRepo = repoStaging
		r.statsFactory = stats.NOP
		r.conf = config.New()
		r.config.uploadFreqInS = config.SingleValueLoader(int64(1800))
		r.config.stagingFilesBatchSize = config.SingleValueLoader(100)
		r.config.warehouseSyncFreqIgnore = config.SingleValueLoader(true)
		r.config.enableJitterForSyncs = config.SingleValueLoader(true)
		r.destType = destinationType
		r.logger = logger.NOP
		r.triggerStore = &sync.Map{}
		r.inProgressMap = make(map[workerIdentifierMapKey][]jobID)
		r.createJobMarkerMap = make(map[string]time.Time)
		r.createUploadAlways = &atomic.Bool{}
		r.scheduledTimesCache = make(map[string][]int)

		t.Run("no staging files", func(t *testing.T) {
			err = r.createJobs(ctx, warehouse)
			require.NoError(t, err)

			upload, err := repoUpload.Get(ctx, 1)
			require.ErrorIs(t, err, model.ErrUploadNotFound)
			require.Zero(t, upload)
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
				r.updateCreateJobMarker(warehouse, now.Add(-time.Hour))

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
				r.updateCreateJobMarker(warehouse, now.Add(-time.Hour))
				r.triggerStore.Store(warehouse.Identifier, struct{}{})

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

	t.Run("handlePriorityForWaitingUploads", func(t *testing.T) {
		pgResource, err := postgres.Setup(pool, t)
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
		r.now = time.Now
		r.db = db
		r.uploadRepo = repoUpload
		r.stagingRepo = repoStaging
		r.statsFactory = stats.NOP
		r.conf = config.New()
		r.config.stagingFilesBatchSize = config.SingleValueLoader(100)
		r.config.warehouseSyncFreqIgnore = config.SingleValueLoader(true)
		r.config.enableJitterForSyncs = config.SingleValueLoader(true)
		r.destType = destinationType
		r.inProgressMap = make(map[workerIdentifierMapKey][]jobID)
		r.triggerStore = &sync.Map{}
		r.logger = logger.NOP
		r.createUploadAlways = &atomic.Bool{}
		r.scheduledTimesCache = make(map[string][]int)

		priority := 50

		createJob := func(t *testing.T, priority int) {
			t.Helper()

			stagingFiles := createStagingFiles(t, ctx, repoStaging, workspaceID, sourceID, destinationID)

			err = r.createUploadJobsFromStagingFiles(
				ctx,
				warehouse,
				lo.Map(stagingFiles, func(item *model.StagingFileWithSchema, index int) *model.StagingFile {
					return &item.StagingFile
				}),
				priority,
				r.now(),
			)
			require.NoError(t, err)
		}

		t.Run("no uploads", func(t *testing.T) {
			priority, err := r.handlePriorityForWaitingUploads(ctx, warehouse)
			require.NoError(t, err)
			require.Equal(t, priority, defaultUploadPriority)
		})

		t.Run("context cancelled", func(t *testing.T) {
			ctx, cancel := context.WithCancel(ctx)
			cancel()

			priority, err := r.handlePriorityForWaitingUploads(ctx, warehouse)
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, priority, 0)
		})

		t.Run("with waiting uploads and in progress", func(t *testing.T) {
			createJob(t, priority)

			r.setDestInProgress(warehouse, -1)
			defer r.removeDestInProgress(warehouse, -1)

			jobPriority, err := r.handlePriorityForWaitingUploads(ctx, warehouse)
			require.NoError(t, err)
			require.Equal(t, jobPriority, priority)

			_, err = r.uploadRepo.Get(ctx, 1)
			require.ErrorIs(t, err, model.ErrUploadNotFound)
		})

		t.Run("with waiting uploads and no in progress", func(t *testing.T) {
			createJob(t, priority)

			jobPriority, err := r.handlePriorityForWaitingUploads(ctx, warehouse)
			require.NoError(t, err)
			require.Equal(t, jobPriority, priority)

			_, err = r.uploadRepo.Get(ctx, 2)
			require.ErrorIs(t, err, model.ErrUploadNotFound)
		})

		t.Run("no waiting uploads", func(t *testing.T) {
			createJob(t, priority)

			_, err := db.ExecContext(ctx, `UPDATE `+warehouseutils.WarehouseUploadsTable+` SET status = 'executing' WHERE id = $1`, 3)
			require.NoError(t, err)

			jobPriority, err := r.handlePriorityForWaitingUploads(ctx, warehouse)
			require.NoError(t, err)
			require.Equal(t, jobPriority, defaultUploadPriority)

			_, err = r.uploadRepo.Get(ctx, 3)
			require.NoError(t, err)
		})
	})

	t.Run("Scheduler", func(t *testing.T) {
		pgResource, err := postgres.Setup(pool, t)
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

		statsStore, err := memstats.New()
		require.NoError(t, err)

		r := Router{}
		r.db = db
		r.statsFactory = statsStore
		r.uploadRepo = repoUpload
		r.stagingRepo = repoStaging
		r.conf = config.New()
		r.config.uploadFreqInS = config.SingleValueLoader(int64(1800))
		r.config.stagingFilesBatchSize = config.SingleValueLoader(100)
		r.config.warehouseSyncFreqIgnore = config.SingleValueLoader(true)
		r.config.enableJitterForSyncs = config.SingleValueLoader(true)
		r.config.enableJitterForSyncs = config.SingleValueLoader(true)
		r.config.mainLoopSleep = config.SingleValueLoader(time.Millisecond * 100)
		r.config.maxParallelJobCreation = config.SingleValueLoader(100)
		r.destType = destinationType
		r.logger = logger.NOP
		r.now = func() time.Time {
			return now.Add(time.Second)
		}
		r.warehouses = []model.Warehouse{warehouse}
		r.stats.schedulerWarehouseLengthStat = r.statsFactory.NewTaggedStat("wh_scheduler.warehouse_length", stats.GaugeType, stats.Tags{
			"destinationType": r.destType,
		})
		r.stats.schedulerTotalSchedulingTimeStat = r.statsFactory.NewTaggedStat("wh_scheduler.total_scheduling_time", stats.TimerType, stats.Tags{
			"destinationType": r.destType,
		})
		r.createJobMarkerMap = make(map[string]time.Time)
		r.triggerStore = &sync.Map{}
		r.createUploadAlways = &atomic.Bool{}
		r.scheduledTimesCache = make(map[string][]int)

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

		require.Equal(t, statsStore.Get("wh_scheduler.warehouse_length", stats.Tags{
			"destinationType": destinationType,
		}).LastValue(), float64(1))
	})

	t.Run("UploadsToProcess", func(t *testing.T) {
		pgResource, err := postgres.Setup(pool, t)
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
		r.db = db
		r.uploadRepo = repoUpload
		r.stagingRepo = repoStaging
		r.statsFactory = stats.NOP
		r.conf = config.New()
		r.config.allowMultipleSourcesForJobsPickup = true
		r.config.stagingFilesBatchSize = config.SingleValueLoader(100)
		r.config.warehouseSyncFreqIgnore = config.SingleValueLoader(true)
		r.destType = destinationType
		r.logger = logger.NOP
		r.tenantManager = multitenant.New(config.New(), mocksBackendConfig.NewMockBackendConfig(ctrl))
		r.warehouses = []model.Warehouse{warehouse}
		r.uploadJobFactory = UploadJobFactory{
			reporting:    &reporting.NOOP{},
			conf:         config.New(),
			logger:       logger.NOP,
			statsFactory: r.statsFactory,
			db:           r.db,
		}
		r.stats.processingPendingJobsStat = r.statsFactory.NewTaggedStat("wh_processing_pending_jobs", stats.GaugeType, stats.Tags{
			"destType": r.destType,
		})
		r.stats.processingAvailableWorkersStat = r.statsFactory.NewTaggedStat("wh_processing_available_workers", stats.GaugeType, stats.Tags{
			"destType": r.destType,
		})
		r.stats.processingPickupLagStat = r.statsFactory.NewTaggedStat("wh_processing_pickup_lag", stats.TimerType, stats.Tags{
			"destType": r.destType,
		})
		r.stats.processingPickupWaitTimeStat = r.statsFactory.NewTaggedStat("wh_processing_pickup_wait_time", stats.TimerType, stats.Tags{
			"destType": r.destType,
		})
		r.createJobMarkerMap = make(map[string]time.Time)
		r.triggerStore = &sync.Map{}
		r.createUploadAlways = &atomic.Bool{}
		r.scheduledTimesCache = make(map[string][]int)

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
			t.Run("unknown destination type", func(t *testing.T) {
				createUpload(ctx, "test-destination-type")

				ujs, err := r.uploadsToProcess(ctx, 1, []string{})
				require.NoError(t, err)
				require.Len(t, ujs, 0)
			})

			t.Run("known destination type", func(t *testing.T) {
				createUpload(ctx, destinationType)

				ujs, err := r.uploadsToProcess(ctx, 1, []string{})
				require.NoError(t, err)
				require.Len(t, ujs, 1)
			})

			t.Run("empty warehouses", func(t *testing.T) {
				r.warehouses = []model.Warehouse{}

				createUpload(ctx, destinationType)

				ujs, err := r.uploadsToProcess(ctx, 1, []string{})
				require.NoError(t, err)
				require.Len(t, ujs, 0)
			})
		})
	})

	t.Run("Processor", func(t *testing.T) {
		pgResource, err := postgres.Setup(pool, t)
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
		r.db = db
		r.uploadRepo = repoUpload
		r.stagingRepo = repoStaging
		r.statsFactory = stats.NOP
		r.conf = config.New()
		r.config.allowMultipleSourcesForJobsPickup = true
		r.config.stagingFilesBatchSize = config.SingleValueLoader(100)
		r.config.warehouseSyncFreqIgnore = config.SingleValueLoader(true)
		r.config.noOfWorkers = config.SingleValueLoader(10)
		r.config.waitForWorkerSleep = time.Millisecond * 100
		r.config.uploadAllocatorSleep = time.Millisecond * 100
		r.destType = warehouseutils.RS
		r.logger = logger.NOP
		r.tenantManager = multitenant.New(config.New(), mocksBackendConfig.NewMockBackendConfig(ctrl))
		r.bcManager = bcm.New(r.conf, r.db, r.tenantManager, r.logger, stats.NOP)
		r.warehouses = []model.Warehouse{warehouse}
		r.uploadJobFactory = UploadJobFactory{
			reporting:    &reporting.NOOP{},
			conf:         config.New(),
			logger:       logger.NOP,
			statsFactory: r.statsFactory,
			db:           r.db,
		}
		r.workerChannelMap = map[string]chan *UploadJob{
			r.workerIdentifier(warehouse): make(chan *UploadJob, 1),
		}
		r.inProgressMap = make(map[workerIdentifierMapKey][]jobID)
		r.stats.processingPendingJobsStat = r.statsFactory.NewTaggedStat("wh_processing_pending_jobs", stats.GaugeType, stats.Tags{
			"destType": r.destType,
		})
		r.stats.processingAvailableWorkersStat = r.statsFactory.NewTaggedStat("wh_processing_available_workers", stats.GaugeType, stats.Tags{
			"destType": r.destType,
		})
		r.stats.processingPickupLagStat = r.statsFactory.NewTaggedStat("wh_processing_pickup_lag", stats.TimerType, stats.Tags{
			"destType": r.destType,
		})
		r.stats.processingPickupWaitTimeStat = r.statsFactory.NewTaggedStat("wh_processing_pickup_wait_time", stats.TimerType, stats.Tags{
			"destType": r.destType,
		})
		r.createJobMarkerMap = make(map[string]time.Time)
		r.triggerStore = &sync.Map{}
		r.createUploadAlways = &atomic.Bool{}
		r.scheduledTimesCache = make(map[string][]int)

		close(r.bcManager.InitialConfigFetched)

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
			_ = r.runUploadJobAllocator(ctx)
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

	t.Run("Preemptable", func(t *testing.T) {
		t.Run("Processor with workers < 1", func(t *testing.T) {
			pgResource, err := postgres.Setup(pool, t)
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
			r.db = db
			r.uploadRepo = repoUpload
			r.stagingRepo = repoStaging
			r.statsFactory = stats.NOP
			r.conf = config.New()
			r.config.allowMultipleSourcesForJobsPickup = true
			r.config.stagingFilesBatchSize = config.SingleValueLoader(100)
			r.config.warehouseSyncFreqIgnore = config.SingleValueLoader(true)
			r.config.noOfWorkers = config.SingleValueLoader(0)
			r.config.waitForWorkerSleep = time.Second * 5
			r.config.uploadAllocatorSleep = time.Millisecond * 100
			r.destType = warehouseutils.RS
			r.logger = logger.NOP
			r.tenantManager = multitenant.New(config.New(), mocksBackendConfig.NewMockBackendConfig(ctrl))
			r.bcManager = bcm.New(r.conf, r.db, r.tenantManager, r.logger, stats.NOP)
			r.warehouses = []model.Warehouse{warehouse}
			r.uploadJobFactory = UploadJobFactory{
				reporting:    &reporting.NOOP{},
				conf:         config.New(),
				logger:       logger.NOP,
				statsFactory: r.statsFactory,
				db:           r.db,
			}
			r.workerChannelMap = map[string]chan *UploadJob{
				r.workerIdentifier(warehouse): make(chan *UploadJob, 1),
			}
			r.inProgressMap = make(map[workerIdentifierMapKey][]jobID)
			r.stats.processingPendingJobsStat = r.statsFactory.NewTaggedStat("wh_processing_pending_jobs", stats.GaugeType, stats.Tags{
				"destType": r.destType,
			})
			r.stats.processingAvailableWorkersStat = r.statsFactory.NewTaggedStat("wh_processing_available_workers", stats.GaugeType, stats.Tags{
				"destType": r.destType,
			})
			r.stats.processingPickupLagStat = r.statsFactory.NewTaggedStat("wh_processing_pickup_lag", stats.TimerType, stats.Tags{
				"destType": r.destType,
			})
			r.stats.processingPickupWaitTimeStat = r.statsFactory.NewTaggedStat("wh_processing_pickup_wait_time", stats.TimerType, stats.Tags{
				"destType": r.destType,
			})
			r.createJobMarkerMap = make(map[string]time.Time)
			r.triggerStore = &sync.Map{}
			r.createUploadAlways = &atomic.Bool{}
			r.scheduledTimesCache = make(map[string][]int)

			close(r.bcManager.InitialConfigFetched)

			closeCh := make(chan struct{})

			ctx, cancel := context.WithCancel(context.Background())

			go func() {
				_ = r.runUploadJobAllocator(ctx)

				close(closeCh)
			}()

			<-r.bcManager.InitialConfigFetched

			time.AfterFunc(100*time.Millisecond, func() {
				cancel()

				_, ok := <-closeCh
				require.False(t, ok)
			})
		})

		t.Run("Scheduler is not enabled", func(t *testing.T) {
			pgResource, err := postgres.Setup(pool, t)
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

			r := Router{}
			r.db = db
			r.statsFactory = stats.NOP
			r.uploadRepo = repoUpload
			r.stagingRepo = repoStaging
			r.conf = config.New()

			r.config.stagingFilesBatchSize = config.SingleValueLoader(100)
			r.config.warehouseSyncFreqIgnore = config.SingleValueLoader(true)
			r.config.enableJitterForSyncs = config.SingleValueLoader(true)
			r.config.mainLoopSleep = config.SingleValueLoader(time.Second * 5)
			r.config.maxParallelJobCreation = config.SingleValueLoader(100)
			r.destType = destinationType
			r.logger = logger.NOP
			r.now = func() time.Time {
				return now.Add(time.Second)
			}
			r.stats.schedulerWarehouseLengthStat = r.statsFactory.NewTaggedStat("wh_scheduler.warehouse_length", stats.GaugeType, stats.Tags{
				"destinationType": r.destType,
			})
			r.stats.schedulerTotalSchedulingTimeStat = r.statsFactory.NewTaggedStat("wh_scheduler.total_scheduling_time", stats.TimerType, stats.Tags{
				"destinationType": r.destType,
			})
			r.createJobMarkerMap = make(map[string]time.Time)
			r.triggerStore = &sync.Map{}
			r.createUploadAlways = &atomic.Bool{}
			r.scheduledTimesCache = make(map[string][]int)

			closeCh := make(chan struct{})

			ctx, cancel := context.WithCancel(context.Background())

			go func() {
				r.mainLoop(ctx)

				close(closeCh)
			}()

			time.AfterFunc(100*time.Millisecond, func() {
				cancel()

				_, ok := <-closeCh
				require.False(t, ok)
			})
		})
	})

	t.Run("Processor stats", func(t *testing.T) {
		testcases := []struct {
			name            string
			destType        string
			skipIdentifiers []string
			pendingJobs     int
			pickupLag       time.Duration
			pickupWaitTime  time.Duration
			wantErr         error
		}{
			{
				name:     "no pending jobs",
				destType: "unknown-destination",
			},
			{
				name:            "in progress namespace",
				destType:        warehouseutils.POSTGRES,
				skipIdentifiers: []string{"test-destinationID_test-namespace"},
			},
			{
				name:           "some pending jobs",
				destType:       warehouseutils.POSTGRES,
				pendingJobs:    3,
				pickupLag:      time.Duration(3983) * time.Second,
				pickupWaitTime: time.Duration(8229) * time.Second,
			},
			{
				name:     "invalid metadata",
				destType: "test-destinationType-1",
				wantErr:  fmt.Errorf("count pending jobs: pq: invalid input syntax for type timestamp with time zone: \"\""),
			},
			{
				name:           "no next retry time",
				destType:       "test-destinationType-2",
				pendingJobs:    1,
				pickupLag:      time.Duration(0) * time.Second,
				pickupWaitTime: time.Duration(0) * time.Second,
			},
		}

		for _, tc := range testcases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

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

				sqlStatement, err := os.ReadFile("testdata/sql/processing_stats_test.sql")
				require.NoError(t, err)

				_, err = pgResource.DB.Exec(string(sqlStatement))
				require.NoError(t, err)

				ctx := context.Background()

				jobStats, err := repo.NewUploads(sqlmiddleware.New(pgResource.DB), repo.WithNow(func() time.Time {
					// nowSQL := "'2022-12-06 22:00:00'"
					return time.Date(2022, 12, 6, 22, 0, 0, 0, time.UTC)
				})).UploadJobsStats(ctx, tc.destType, repo.ProcessOptions{
					SkipIdentifiers: tc.skipIdentifiers,
					SkipWorkspaces:  nil,
				})
				if tc.wantErr != nil {
					require.EqualError(t, err, tc.wantErr.Error())
					return
				}
				require.NoError(t, err)

				require.EqualValues(t, jobStats.PendingJobs, tc.pendingJobs)
				require.EqualValues(t, jobStats.PickupLag, tc.pickupLag)
				require.EqualValues(t, jobStats.PickupWaitTime, tc.pickupWaitTime)
			})
		}
	})

	t.Run("Backend config subscriber", func(t *testing.T) {
		pgResource, err := postgres.Setup(pool, t)
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

		ctx, cancel := context.WithCancel(context.Background())
		g := &errgroup.Group{}
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
		r.db = db
		r.uploadRepo = repoUpload
		r.stagingRepo = repoStaging
		r.statsFactory = stats.NOP
		r.conf = config.New()
		r.logger = logger.NOP
		r.destType = warehouseutils.RS
		r.config.maxConcurrentUploadJobs = 1
		r.tenantManager = multitenant.New(config.New(), mockBackendConfig)
		r.bcManager = bcm.New(r.conf, r.db, r.tenantManager, r.logger, stats.NOP)
		r.createJobMarkerMap = make(map[string]time.Time)
		r.triggerStore = &sync.Map{}
		r.createUploadAlways = &atomic.Bool{}
		r.scheduledTimesCache = make(map[string][]int)
		r.backgroundGroup = g

		go func() {
			r.bcManager.Start(ctx)
		}()
		go func() {
			r.backendConfigSubscriber(ctx)
		}()

		<-r.bcManager.InitialConfigFetched

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
