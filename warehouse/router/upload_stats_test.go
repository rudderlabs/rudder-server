package router

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
)

func TestUploadJob_Stats(t *testing.T) {
	db := setupUploadTest(t, "testdata/sql/stats_test.sql")

	t.Run("Generate upload success metrics", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		ujf := &UploadJobFactory{
			conf:         config.New(),
			logger:       logger.NOP,
			statsFactory: statsStore,
			db:           sqlmiddleware.New(db),
		}
		job := ujf.NewUploadJob(context.Background(), &model.UploadJob{
			Upload: model.Upload{
				ID:                 1,
				StagingFileStartID: 1,
				StagingFileEndID:   4,
				SourceID:           "test-sourceID",
				DestinationID:      "test-destinationID",
			},
			Warehouse: model.Warehouse{
				Type: "POSTGRES",
			},
			StagingFiles: []*model.StagingFile{
				{ID: 1},
				{ID: 2},
				{ID: 3},
				{ID: 4},
			},
		}, nil)

		job.generateUploadSuccessMetrics()

		tags := stats.Tags{
			"module":      moduleName,
			"workspaceId": job.warehouse.WorkspaceID,
			"warehouseID": warehouseTagName(job.warehouse.Destination.ID, job.warehouse.Source.Name, job.warehouse.Destination.Name, job.warehouse.Source.ID),
			"destID":      job.warehouse.Destination.ID,
			"destType":    job.warehouse.Destination.DestinationDefinition.Name,
			"sourceID":    job.warehouse.Source.ID,
			"sourceType":  job.warehouse.Source.SourceDefinition.Name,
		}
		require.EqualValues(t, 4, statsStore.Get("total_rows_synced", tags).LastValue())
		require.EqualValues(t, 4, statsStore.Get("num_staged_events", tags).LastValue())
		require.EqualValues(t, 1, statsStore.Get("upload_success", tags).LastValue())
	})

	t.Run("Generate upload aborted metrics", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		ujf := &UploadJobFactory{
			conf:         config.New(),
			logger:       logger.NOP,
			statsFactory: statsStore,
			db:           sqlmiddleware.New(db),
		}
		job := ujf.NewUploadJob(context.Background(), &model.UploadJob{
			Upload: model.Upload{
				ID:                 1,
				StagingFileStartID: 1,
				StagingFileEndID:   4,
				SourceID:           "test-sourceID",
				DestinationID:      "test-destinationID",
			},
			Warehouse: model.Warehouse{
				Type: "POSTGRES",
			},
		}, nil)

		job.generateUploadAbortedMetrics()

		tags := stats.Tags{
			"module":      moduleName,
			"workspaceId": job.warehouse.WorkspaceID,
			"warehouseID": warehouseTagName(job.warehouse.Destination.ID, job.warehouse.Source.Name, job.warehouse.Destination.Name, job.warehouse.Source.ID),
			"destID":      job.warehouse.Destination.ID,
			"destType":    job.warehouse.Destination.DestinationDefinition.Name,
			"sourceID":    job.warehouse.Source.ID,
			"sourceType":  job.warehouse.Source.SourceDefinition.Name,
		}
		require.EqualValues(t, 4, statsStore.Get("total_rows_synced", tags).LastValue())
		require.EqualValues(t, 4, statsStore.Get("num_staged_events", tags).LastValue())
	})

	t.Run("Record table load", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		ujf := &UploadJobFactory{
			conf:         config.New(),
			logger:       logger.NOP,
			statsFactory: statsStore,
			db:           sqlmiddleware.New(db),
		}
		job := ujf.NewUploadJob(context.Background(), &model.UploadJob{
			Upload: model.Upload{
				ID:                 1,
				StagingFileStartID: 1,
				StagingFileEndID:   4,
				WorkspaceID:        "workspaceID",
			},
			Warehouse: model.Warehouse{
				Type: "POSTGRES",
			},
		}, nil)

		job.recordTableLoad("tracks", 4)

		tags := stats.Tags{
			"module":      moduleName,
			"workspaceId": job.warehouse.WorkspaceID,
			"warehouseID": warehouseTagName(job.warehouse.Destination.ID, job.warehouse.Source.Name, job.warehouse.Destination.Name, job.warehouse.Source.ID),
			"destID":      job.warehouse.Destination.ID,
			"destType":    job.warehouse.Destination.DestinationDefinition.Name,
			"sourceID":    job.warehouse.Source.ID,
			"sourceType":  job.warehouse.Source.SourceDefinition.Name,
			"tableName":   "tracks",
		}
		require.EqualValues(t, 4, statsStore.Get("event_delivery", tags).LastValue())
		require.EqualValues(t, 4, statsStore.Get("rows_synced", tags).LastValue())
	})

	t.Run("Record load files generation time", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		ujf := &UploadJobFactory{
			conf:         config.New(),
			logger:       logger.NOP,
			statsFactory: statsStore,
			db:           sqlmiddleware.New(db),
		}
		job := ujf.NewUploadJob(context.Background(), &model.UploadJob{
			Upload: model.Upload{
				ID:                 1,
				StagingFileStartID: 1,
				StagingFileEndID:   4,
			},
			Warehouse: model.Warehouse{
				Type: "POSTGRES",
			},
		}, nil)

		err = job.recordLoadFileGenerationTimeStat(1, 4)
		require.NoError(t, err)

		tags := stats.Tags{
			"module":      moduleName,
			"workspaceId": job.warehouse.WorkspaceID,
			"warehouseID": warehouseTagName(job.warehouse.Destination.ID, job.warehouse.Source.Name, job.warehouse.Destination.Name, job.warehouse.Source.ID),
			"destID":      job.warehouse.Destination.ID,
			"destType":    job.warehouse.Destination.DestinationDefinition.Name,
			"sourceID":    job.warehouse.Source.ID,
			"sourceType":  job.warehouse.Source.SourceDefinition.Name,
		}
		require.EqualValues(t, 3*time.Second, statsStore.Get("load_file_generation_time", tags).LastDuration())
	})
}

func TestUploadJob_MatchRows(t *testing.T) {
	var (
		sourceID        = "test-sourceID"
		destinationID   = "test-destinationID"
		destinationName = "test-destinationName"
		namespace       = "test-namespace"
		destinationType = "POSTGRES"
	)

	db := setupUploadTest(t, "testdata/sql/upload_test.sql")

	t.Run("Total rows in load files", func(t *testing.T) {
		ujf := &UploadJobFactory{
			conf:         config.New(),
			logger:       logger.NOP,
			statsFactory: stats.NOP,
			db:           sqlmiddleware.New(db),
		}
		job := ujf.NewUploadJob(context.Background(), &model.UploadJob{
			Upload: model.Upload{
				ID:            1,
				DestinationID: destinationID,
				SourceID:      sourceID,
				Namespace:     namespace,
			},
			Warehouse: model.Warehouse{
				Type: destinationType,
				Destination: backendconfig.DestinationT{
					ID:   destinationID,
					Name: destinationName,
				},
				Source: backendconfig.SourceT{
					ID:   sourceID,
					Name: destinationName,
				},
			},
		}, nil)

		count := job.getTotalRowsInLoadFiles(context.Background())
		require.EqualValues(t, 4, count)
	})

	t.Run("Total rows in staging files", func(t *testing.T) {
		ujf := &UploadJobFactory{
			conf:         config.New(),
			logger:       logger.NOP,
			statsFactory: stats.NOP,
			db:           sqlmiddleware.New(db),
		}
		job := ujf.NewUploadJob(context.Background(), &model.UploadJob{
			Upload: model.Upload{
				ID:            1,
				DestinationID: destinationID,
				SourceID:      sourceID,
				Namespace:     namespace,
			},
			Warehouse: model.Warehouse{
				Type: destinationType,
				Destination: backendconfig.DestinationT{
					ID:   destinationID,
					Name: destinationName,
				},
				Source: backendconfig.SourceT{
					ID:   sourceID,
					Name: destinationName,
				},
			},
		}, nil)

		count, err := repo.NewStagingFiles(sqlmiddleware.New(db), config.New()).TotalEventsForUploadID(context.Background(), job.upload.ID)
		require.NoError(t, err)
		require.EqualValues(t, 4, count)
	})

	t.Run("Get uploads timings", func(t *testing.T) {
		ujf := &UploadJobFactory{
			conf:         config.New(),
			logger:       logger.NOP,
			statsFactory: stats.NOP,
			db:           sqlmiddleware.New(db),
		}
		job := ujf.NewUploadJob(context.Background(), &model.UploadJob{
			Upload: model.Upload{
				ID:                 1,
				DestinationID:      destinationID,
				SourceID:           sourceID,
				StagingFileStartID: 1,
				StagingFileEndID:   5,
				Namespace:          namespace,
			},
			Warehouse: model.Warehouse{
				Type: destinationType,
				Destination: backendconfig.DestinationT{
					ID:   destinationID,
					Name: destinationName,
				},
				Source: backendconfig.SourceT{
					ID:   sourceID,
					Name: destinationName,
				},
			},
			StagingFiles: []*model.StagingFile{
				{ID: 1},
				{ID: 2},
				{ID: 3},
				{ID: 4},
				{ID: 5},
			},
		}, nil)

		exportedData, err := time.Parse(time.RFC3339, "2020-04-21T15:26:34.344356Z")
		require.NoError(t, err)

		exportingData, err := time.Parse(time.RFC3339, "2020-04-21T15:16:19.687716Z")
		require.NoError(t, err)

		timings, err := repo.NewUploads(job.db).UploadTimings(context.Background(), job.upload.ID)
		require.NoError(t, err)
		require.EqualValues(t, timings, model.Timings{
			{
				"exported_data":  exportedData,
				"exporting_data": exportingData,
			},
		})
	})

	t.Run("Staging files and load files events match", func(t *testing.T) {
		testCases := []struct {
			name          string
			uploadID      int64
			mismatchCount int
		}{
			{
				name:          "In case of no mismatch",
				uploadID:      1,
				mismatchCount: 0,
			},
			{
				name:          "In case of mismatch",
				uploadID:      2,
				mismatchCount: 3,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				statsStore, err := memstats.New()
				require.NoError(t, err)

				ujf := &UploadJobFactory{
					conf:         config.New(),
					logger:       logger.NOP,
					statsFactory: statsStore,
					db:           sqlmiddleware.New(db),
				}
				job := ujf.NewUploadJob(context.Background(), &model.UploadJob{
					Upload: model.Upload{
						ID:            tc.uploadID,
						DestinationID: destinationID,
						SourceID:      sourceID,
						Namespace:     namespace,
					},
					Warehouse: model.Warehouse{
						Type: destinationType,
						Destination: backendconfig.DestinationT{
							ID:   destinationID,
							Name: destinationName,
						},
						Source: backendconfig.SourceT{
							ID:   sourceID,
							Name: destinationName,
						},
					},
				}, nil)

				err = job.matchRowsInStagingAndLoadFiles(context.Background())
				require.NoError(t, err)

				require.EqualValues(t, tc.mismatchCount, statsStore.Get("warehouse_staging_load_file_events_count_mismatched", stats.Tags{
					"module":         moduleName,
					"workspaceId":    job.warehouse.WorkspaceID,
					"warehouseID":    warehouseTagName(job.warehouse.Destination.ID, job.warehouse.Source.Name, job.warehouse.Destination.Name, job.warehouse.Source.ID),
					"destID":         job.warehouse.Destination.ID,
					"destType":       job.warehouse.Destination.DestinationDefinition.Name,
					"sourceID":       job.warehouse.Source.ID,
					"sourceType":     job.warehouse.Source.SourceDefinition.Name,
					"sourceCategory": job.warehouse.Source.SourceDefinition.Category,
				}).LastValue())
			})
		}
	})
}

func setupUploadTest(t testing.TB, migrationsPath string) *sql.DB {
	t.Helper()

	sqlStatement, err := os.ReadFile(migrationsPath)
	require.NoError(t, err)

	db := setupDB(t)

	_, err = db.Exec(string(sqlStatement))
	require.NoError(t, err)

	return db.DB
}
