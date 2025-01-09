package router

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestRouter_CronTrack(t *testing.T) {
	t.Run("source / destination disabled", func(t *testing.T) {
		ctx := context.Background()

		statsStore, err := memstats.New()
		require.NoError(t, err)

		warehouse := model.Warehouse{
			WorkspaceID: "test-workspaceID",
			Source: backendconfig.SourceT{
				ID:      "test-sourceID",
				Name:    "test-sourceName",
				Enabled: false,
			},
			Destination: backendconfig.DestinationT{
				ID:      "test-destinationID",
				Name:    "test-destinationName",
				Enabled: false,
				Config: map[string]any{
					"syncFrequency": "30",
				},
			},
		}

		r := Router{
			conf:         config.New(),
			destType:     whutils.POSTGRES,
			now:          timeutil.Now,
			statsFactory: statsStore,
			logger:       logger.NOP,
		}

		require.NoError(t, r.trackSync(ctx, &warehouse))
		require.Nil(t, statsStore.Get("warehouse_track_upload_missing", stats.Tags{
			"module":        moduleName,
			"workspaceId":   warehouse.WorkspaceID,
			"destType":      r.destType,
			"sourceId":      warehouse.Source.ID,
			"destinationId": warehouse.Destination.ID,
		}))
	})
	t.Run("exclusion window", func(t *testing.T) {
		ctx := context.Background()

		statsStore, err := memstats.New()
		require.NoError(t, err)

		warehouse := model.Warehouse{
			WorkspaceID: "test-workspaceID",
			Source: backendconfig.SourceT{
				ID:      "test-sourceID",
				Name:    "test-sourceName",
				Enabled: true,
			},
			Destination: backendconfig.DestinationT{
				ID:      "test-destinationID",
				Name:    "test-destinationName",
				Enabled: true,
				Config: map[string]any{
					"syncFrequency": "30",
					"excludeWindow": map[string]any{
						"excludeWindowStartTime": "05:09",
						"excludeWindowEndTime":   "09:07",
					},
				},
			},
		}

		r := Router{
			conf:     config.New(),
			destType: whutils.POSTGRES,
			now: func() time.Time {
				return time.Date(2023, 1, 1, 6, 19, 0, 0, time.UTC)
			},
			statsFactory: statsStore,
			logger:       logger.NOP,
		}

		require.NoError(t, r.trackSync(ctx, &warehouse))
		require.Nil(t, statsStore.Get("warehouse_track_upload_missing", stats.Tags{
			"module":        moduleName,
			"workspaceId":   warehouse.WorkspaceID,
			"destType":      r.destType,
			"sourceId":      warehouse.Source.ID,
			"destinationId": warehouse.Destination.ID,
		}))
	})
	t.Run("no staging files present", func(t *testing.T) {
		db, ctx := setupDB(t), context.Background()

		statsStore, err := memstats.New()
		require.NoError(t, err)

		warehouse := model.Warehouse{
			WorkspaceID: "test-workspaceID",
			Source: backendconfig.SourceT{
				ID:      "test-sourceID",
				Name:    "test-sourceName",
				Enabled: true,
			},
			Destination: backendconfig.DestinationT{
				ID:      "test-destinationID",
				Name:    "test-destinationName",
				Enabled: true,
				Config: map[string]any{
					"syncFrequency": "30",
				},
			},
		}

		r := Router{
			conf:         config.New(),
			destType:     whutils.POSTGRES,
			now:          timeutil.Now,
			statsFactory: statsStore,
			db:           db,
			logger:       logger.NOP,
		}
		r.config.uploadBufferTimeInMin = config.SingleValueLoader(30 * time.Minute)

		require.NoError(t, r.trackSync(ctx, &warehouse))
		require.Nil(t, statsStore.Get("warehouse_track_upload_missing", stats.Tags{
			"module":        moduleName,
			"workspaceId":   warehouse.WorkspaceID,
			"destType":      r.destType,
			"sourceId":      warehouse.Source.ID,
			"destinationId": warehouse.Destination.ID,
		}))
	})
	t.Run("staging files without missing uploads", func(t *testing.T) {
		testCases := []struct {
			name   string
			status string
		}{
			{name: "ExportedData", status: model.ExportedData},
			{name: "Aborted", status: model.Aborted},
			{name: "Failed", status: model.ExportingDataFailed},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				db, ctx := setupDB(t), context.Background()

				now := time.Date(2023, 1, 1, 6, 19, 0, 0, time.UTC)
				nowSQL := fmt.Sprintf("'%s'::timestamp", now.Format(time.DateTime))

				repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
					return now.Add(-time.Hour*1 - time.Minute*30)
				}))
				repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
					return now.Add(-time.Hour * 1)
				}))

				stagingID, err := repoStaging.Insert(ctx, &model.StagingFileWithSchema{
					StagingFile: model.StagingFile{
						WorkspaceID:   "test-workspaceID",
						SourceID:      "test-sourceID",
						DestinationID: "test-destinationID",
					},
				})
				require.NoError(t, err)

				_, err = repoUpload.CreateWithStagingFiles(ctx, model.Upload{
					WorkspaceID:     "test-workspaceID",
					Namespace:       "namespace",
					SourceID:        "test-sourceID",
					DestinationID:   "test-destinationID",
					DestinationType: whutils.POSTGRES,
					Status:          tc.status,
				}, []*model.StagingFile{{
					ID:            stagingID,
					WorkspaceID:   "test-workspaceID",
					SourceID:      "test-sourceID",
					DestinationID: "test-destinationID",
				}})
				require.NoError(t, err)

				statsStore, err := memstats.New()
				require.NoError(t, err)

				warehouse := model.Warehouse{
					WorkspaceID: "test-workspaceID",
					Source: backendconfig.SourceT{
						ID:      "test-sourceID",
						Name:    "test-sourceName",
						Enabled: true,
					},
					Destination: backendconfig.DestinationT{
						ID:      "test-destinationID",
						Name:    whutils.POSTGRES,
						Enabled: true,
						Config: map[string]any{
							"syncFrequency": "30",
						},
					},
				}

				r := Router{
					conf:     config.New(),
					destType: whutils.POSTGRES,
					now: func() time.Time {
						return now
					},
					nowSQL:       nowSQL,
					statsFactory: statsStore,
					db:           db,
					logger:       logger.NOP,
				}
				r.config.uploadBufferTimeInMin = config.SingleValueLoader(30 * time.Minute)

				require.NoError(t, r.trackSync(ctx, &warehouse))

				uploadMissingStat := statsStore.Get("warehouse_track_upload_missing", stats.Tags{
					"module":        moduleName,
					"workspaceId":   warehouse.WorkspaceID,
					"destType":      r.destType,
					"sourceId":      warehouse.Source.ID,
					"destinationId": warehouse.Destination.ID,
				})
				require.NotNil(t, uploadMissingStat)
				require.EqualValues(t, 0, uploadMissingStat.LastValue())
			})
		}
	})
	t.Run("staging files with missing uploads", func(t *testing.T) {
		db, ctx := setupDB(t), context.Background()

		now := time.Date(2023, 1, 1, 6, 19, 0, 0, time.UTC)
		nowSQL := fmt.Sprintf("'%s'::timestamp", now.Format(time.DateTime))

		repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
			return now.Add(-time.Hour*1 - time.Minute*30)
		}))

		_, err := repoStaging.Insert(ctx, &model.StagingFileWithSchema{
			StagingFile: model.StagingFile{
				WorkspaceID:   "test-workspaceID",
				SourceID:      "test-sourceID",
				DestinationID: "test-destinationID",
			},
		})
		require.NoError(t, err)

		statsStore, err := memstats.New()
		require.NoError(t, err)

		warehouse := model.Warehouse{
			WorkspaceID: "test-workspaceID",
			Source: backendconfig.SourceT{
				ID:      "test-sourceID",
				Name:    "test-sourceName",
				Enabled: true,
			},
			Destination: backendconfig.DestinationT{
				ID:      "test-destinationID",
				Name:    whutils.POSTGRES,
				Enabled: true,
				Config: map[string]any{
					"syncFrequency": "30",
				},
			},
		}

		r := Router{
			conf:     config.New(),
			destType: whutils.POSTGRES,
			now: func() time.Time {
				return now
			},
			nowSQL:       nowSQL,
			statsFactory: statsStore,
			db:           db,
			logger:       logger.NOP,
		}
		r.config.uploadBufferTimeInMin = config.SingleValueLoader(30 * time.Minute)

		require.NoError(t, r.trackSync(ctx, &warehouse))

		uploadMissingStat := statsStore.Get("warehouse_track_upload_missing", stats.Tags{
			"module":        moduleName,
			"workspaceId":   warehouse.WorkspaceID,
			"destType":      r.destType,
			"sourceId":      warehouse.Source.ID,
			"destinationId": warehouse.Destination.ID,
		})
		require.NotNil(t, uploadMissingStat)
		require.EqualValues(t, 1, uploadMissingStat.LastValue())
	})
	t.Run("context cancelled", func(t *testing.T) {
		db, ctx := setupDB(t), context.Background()

		ctx, cancel := context.WithCancel(ctx)
		cancel()

		statsStore, err := memstats.New()
		require.NoError(t, err)

		warehouse := model.Warehouse{
			WorkspaceID: "test-workspaceID",
			Source: backendconfig.SourceT{
				ID:      "test-sourceID",
				Name:    "test-sourceName",
				Enabled: true,
			},
			Destination: backendconfig.DestinationT{
				ID:      "test-destinationID",
				Name:    "test-destinationName",
				Enabled: true,
				Config: map[string]any{
					"syncFrequency": "30",
				},
			},
		}

		r := Router{
			conf:         config.New(),
			destType:     whutils.POSTGRES,
			now:          time.Now,
			statsFactory: statsStore,
			db:           db,
			logger:       logger.NOP,
			warehouses:   []model.Warehouse{warehouse},
		}
		r.config.uploadBufferTimeInMin = config.SingleValueLoader(30 * time.Minute)
		r.config.cronTrackerRetries = config.SingleValueLoader(int64(5))
		r.stats.cronTrackerExecTimestamp = statsStore.NewTaggedStat("warehouse_cron_tracker_timestamp_seconds", stats.GaugeType, stats.Tags{"module": moduleName, "destType": r.destType})

		require.NoError(t, r.cronTracker(ctx))
		require.Nil(t, statsStore.Get("warehouse_track_upload_missing", stats.Tags{
			"module":        moduleName,
			"workspaceId":   warehouse.WorkspaceID,
			"destType":      r.destType,
			"sourceId":      warehouse.Source.ID,
			"destinationId": warehouse.Destination.ID,
		}))
	})
}

func setupDB(t testing.TB) *sqlmiddleware.DB {
	t.Helper()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pgResource, err := postgres.Setup(pool, t)
	require.NoError(t, err)

	require.NoError(t, (&migrator.Migrator{
		Handle:          pgResource.DB,
		MigrationsTable: "wh_schema_migrations",
	}).Migrate("warehouse"))

	return sqlmiddleware.New(pgResource.DB)
}
