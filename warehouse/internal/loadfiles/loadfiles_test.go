package loadfiles_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-go-kit/logger"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/internal/loadfiles"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type mockControlPlaneClient struct {
	revisions map[string]backendconfig.DestinationT
}

func (m *mockControlPlaneClient) DestinationHistory(_ context.Context, revisionID string) (backendconfig.DestinationT, error) {
	dest, ok := m.revisions[revisionID]
	if !ok {
		return backendconfig.DestinationT{}, fmt.Errorf("revision %q not found", revisionID)
	}

	return dest, nil
}

func getStagingFiles() []*model.StagingFile {
	var stagingFiles []*model.StagingFile
	for i := range 10 {
		stagingFiles = append(stagingFiles, &model.StagingFile{
			ID:                    int64(i),
			WorkspaceID:           "workspace_id",
			Location:              fmt.Sprintf("s3://bucket/path/to/file/%d", i),
			SourceID:              "source_id",
			DestinationID:         "destination_id",
			Status:                "",
			UseRudderStorage:      true,
			DestinationRevisionID: "revision_id",
		})
	}

	return stagingFiles
}

func TestCreateLoadFiles_DestinationHistory(t *testing.T) {
	t.Parallel()

	notifer := &mockNotifier{
		t:      t,
		tables: []string{"track", "indentify"},
	}
	stageRepo := &mockStageFilesRepo{}
	loadRepo := &mockLoadFilesRepo{}
	controlPlane := &mockControlPlaneClient{}

	lf := loadfiles.LoadFileGenerator{
		Logger:    logger.NOP,
		Notifier:  notifer,
		StageRepo: stageRepo,
		LoadRepo:  loadRepo,
		Conf:      config.New(),

		ControlPlaneClient: controlPlane,
	}

	ctx := context.Background()
	now := time.Now()

	t.Log("staging file destination_id different from upload destination_id")

	stagingFile := &model.StagingFile{
		ID:                    1,
		WorkspaceID:           "workspace_id",
		Location:              "s3://bucket/path/to/file/1",
		SourceID:              "source_id",
		DestinationID:         "destination_id",
		Status:                "",
		FirstEventAt:          now.Add(-time.Hour),
		LastEventAt:           now.Add(-2 * time.Hour),
		UseRudderStorage:      true,
		DestinationRevisionID: "older_revision_id",
	}

	controlPlane.revisions = map[string]backendconfig.DestinationT{
		"older_revision_id": {
			ID: "destination_id_1",
			Config: map[string]any{
				"token": "old_token",
			},
		},
	}

	job := model.UploadJob{
		Warehouse: model.Warehouse{
			WorkspaceID: "",
			Source:      backendconfig.SourceT{},
			Destination: backendconfig.DestinationT{
				ID:         "destination_id",
				RevisionID: "revision_id",
			},
			Namespace:  "",
			Type:       "",
			Identifier: "",
		},
		Upload: model.Upload{
			ID:               100,
			DestinationID:    "destination_id",
			DestinationType:  warehouseutils.SNOWFLAKE,
			SourceID:         "source_id",
			UseRudderStorage: true,
		},
		StagingFiles: []*model.StagingFile{
			stagingFile,
		},
	}

	startID, endID, err := lf.CreateLoadFiles(ctx, &job)
	require.NoError(t, err)
	require.Equal(t, int64(1), startID)
	require.Equal(t, int64(2), endID)

	loadFiles, err := loadRepo.Get(ctx, job.Upload.ID)
	require.Equal(t, 2, len(loadFiles))
	require.NoError(t, err)

	var tableNames []string
	for _, loadFile := range loadFiles {
		require.Contains(t, loadFile.Location, loadFile.TableName)
		require.Equal(t, job.Upload.ID, loadFile.UploadID)
		require.Equal(t, stagingFile.SourceID, loadFile.SourceID)
		require.Equal(t, stagingFile.DestinationID, loadFile.DestinationID)
		require.Equal(t, job.Warehouse.Destination.RevisionID, loadFile.DestinationRevisionID)
		require.Equal(t, stagingFile.UseRudderStorage, loadFile.UseRudderStorage)

		tableNames = append(tableNames, loadFile.TableName)
	}
	require.ElementsMatch(t, notifer.tables, tableNames)
	require.Equal(t, warehouseutils.StagingFileSucceededState, stageRepo.store[stagingFile.ID].Status)

	require.Equal(t,
		controlPlane.revisions[stagingFile.DestinationRevisionID].Config,
		notifer.requestsV2[0].StagingDestinationConfig,
	)

	t.Run("invalid revision ID", func(t *testing.T) {
		stagingFile.DestinationRevisionID = "invalid_revision_id"

		startID, endID, err := lf.CreateLoadFiles(ctx, &job)
		require.ErrorContains(t, err, "populating destination revision ID: revision \"invalid_revision_id\" not found")
		require.Zero(t, startID)
		require.Zero(t, endID)
	})
}

func TestGetLoadFilePrefix(t *testing.T) {
	testCases := []struct {
		name      string
		warehouse model.Warehouse
		expected  string
	}{
		{
			name: "s3 datalake",
			warehouse: model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]any{},
				},
				Type: warehouseutils.S3Datalake,
			},
			expected: "2022/08/06/14",
		},
		{
			name: "s3 datalake with glue",
			warehouse: model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"region":  "test-region",
						"useGlue": true,
					},
				},
				Type: warehouseutils.S3Datalake,
			},
			expected: "2022/08/06/14",
		},
		{
			name: "s3 datalake with glue and layout",
			warehouse: model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"region":           "test-region",
						"useGlue":          true,
						"timeWindowLayout": "dt=2006-01-02",
					},
				},
				Type: warehouseutils.S3Datalake,
			},
			expected: "dt=2022-08-06",
		},
		{
			name: "azure datalake",
			warehouse: model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"tableSuffix": "key=val",
					},
				},
				Type: warehouseutils.AzureDatalake,
			},
			expected: "2022/08/06/14",
		},
		{
			name: "gcs datalake",
			warehouse: model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]any{},
				},
				Type: warehouseutils.GCSDatalake,
			},
			expected: "2022/08/06/14",
		},
		{
			name: "gcs datalake with suffix",
			warehouse: model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"tableSuffix": "key=val",
					},
				},
				Type: warehouseutils.GCSDatalake,
			},
			expected: "key=val/2022/08/06/14",
		},
		{
			name: "gcs datalake with layout",
			warehouse: model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"timeWindowLayout": "year=2006/month=01/day=02/hour=15",
					},
				},
				Type: warehouseutils.GCSDatalake,
			},
			expected: "year=2022/month=08/day=06/hour=14",
		},
		{
			name: "gcs datalake with suffix and layout",
			warehouse: model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"tableSuffix":      "key=val",
						"timeWindowLayout": "year=2006/month=01/day=02/hour=15",
					},
				},
				Type: warehouseutils.GCSDatalake,
			},
			expected: "key=val/year=2022/month=08/day=06/hour=14",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			timeWindow := time.Date(2022, time.Month(8), 6, 14, 10, 30, 0, time.UTC)
			lf := loadfiles.LoadFileGenerator{}
			lf.Conf = config.New()
			require.Equal(t, lf.GetLoadFilePrefix(timeWindow, tc.warehouse), tc.expected)
		})
	}
}

func TestGroupStagingFiles(t *testing.T) {
	t.Run("size based grouping", func(t *testing.T) {
		testCases := []struct {
			name          string
			files         []*model.StagingFile
			batchSizes    []int
			skipSizeCheck bool
		}{
			{
				name:       "empty files",
				files:      []*model.StagingFile{},
				batchSizes: []int{},
			},
			{
				name: "single file under limit",
				files: []*model.StagingFile{
					{
						ID: 1,
						BytesPerTable: map[string]int64{
							"table1": 50 * 1024 * 1024, // 50MB
						},
					},
				},
				batchSizes: []int{1},
			},
			{
				name: "all files over limit",
				files: []*model.StagingFile{
					{
						ID: 1,
						BytesPerTable: map[string]int64{
							"table1": 150 * 1024 * 1024, // 150MB
						},
					},
					{
						ID: 2,
						BytesPerTable: map[string]int64{
							"table1": 150 * 1024 * 1024, // 150MB
						},
					},
				},
				batchSizes:    []int{1, 1},
				skipSizeCheck: true,
			},
			{
				name: "multiple files under limit",
				files: []*model.StagingFile{
					{
						ID: 1,
						BytesPerTable: map[string]int64{
							"table1": 30 * 1024 * 1024, // 30MB
						},
					},
					{
						ID: 2,
						BytesPerTable: map[string]int64{
							"table1": 40 * 1024 * 1024, // 40MB
						},
					},
				},
				batchSizes: []int{2},
			},
			{
				name: "optimal grouping case",
				files: []*model.StagingFile{
					{
						ID: 1,
						BytesPerTable: map[string]int64{
							"table1": 50 * 1024 * 1024, // 50MB
						},
					},
					{
						ID: 2,
						BytesPerTable: map[string]int64{
							"table1": 100 * 1024 * 1024, // 100MB
						},
					},
					{
						ID: 3,
						BytesPerTable: map[string]int64{
							"table1": 50 * 1024 * 1024, // 50MB
						},
					},
				},
				batchSizes: []int{1, 2}, // [100MB], [50MB, 50MB]
			},
			{
				name: "sorting logic",
				files: []*model.StagingFile{
					{
						ID: 1,
						BytesPerTable: map[string]int64{
							"table1": 20 * 1024 * 1024,
							"table2": 71 * 1024 * 1024,
						},
					},
					{
						ID: 2,
						BytesPerTable: map[string]int64{
							"table1": 50 * 1024 * 1024,
							"table2": 1 * 1024 * 1024,
						},
					},
					{
						ID: 3,
						BytesPerTable: map[string]int64{
							"table1": 70 * 1024 * 1024,
							"table2": 1 * 1024 * 1024,
						},
					},
					{
						ID: 4,
						BytesPerTable: map[string]int64{
							"table1": 40 * 1024 * 1024,
							"table2": 1 * 1024 * 1024,
						},
					},
				},
				batchSizes: []int{2, 2},
			},
			{
				name: "multiple tables different sizes",
				files: []*model.StagingFile{
					{
						ID: 1,
						BytesPerTable: map[string]int64{
							"table1": 60 * 1024 * 1024, // 60MB
							"table2": 30 * 1024 * 1024, // 30MB
						},
					},
					{
						ID: 2,
						BytesPerTable: map[string]int64{
							"table1": 5 * 1024 * 1024,  // 5MB
							"table2": 90 * 1024 * 1024, // 90MB
						},
					},
				},
				batchSizes: []int{1, 1}, // Split due to table2 exceeding limit when combined
			},
			{
				name: "multiple tables under limit",
				files: []*model.StagingFile{
					{
						ID: 1,
						BytesPerTable: map[string]int64{
							"table1": 40 * 1024 * 1024, // 40MB
							"table2": 30 * 1024 * 1024, // 30MB
						},
					},
					{
						ID: 2,
						BytesPerTable: map[string]int64{
							"table1": 30 * 1024 * 1024, // 30MB
							"table2": 20 * 1024 * 1024, // 20MB
						},
					},
				},
				batchSizes: []int{2},
			},
			{
				name: "one table exceeds limit",
				files: []*model.StagingFile{
					{
						ID: 1,
						BytesPerTable: map[string]int64{
							"table1": 110 * 1024 * 1024,
							"table2": 3 * 1024 * 1024,
						},
					},
					{
						ID: 2,
						BytesPerTable: map[string]int64{
							"table2": 20 * 1024 * 1024,
						},
					},
				},
				// Ideally we should have only 1 batch here
				// but we are not handling this case
				batchSizes:    []int{1, 1},
				skipSizeCheck: true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				lf := loadfiles.LoadFileGenerator{
					Conf:   config.New(),
					Logger: logger.NOP,
				}
				maxSizeMB := 100

				batches := lf.GroupStagingFiles(tc.files, maxSizeMB)
				require.Equal(t, len(tc.batchSizes), len(batches), "number of batches mismatch")

				actualBatchSizes := make([]int, len(batches))
				for i, batch := range batches {
					actualBatchSizes[i] = len(batch)
				}
				require.ElementsMatch(t, tc.batchSizes, actualBatchSizes, "batch sizes mismatch")

				if !tc.skipSizeCheck {
					// Verify that no table in any batch exceeds the size limit
					maxSizeBytes := int64(maxSizeMB * 1024 * 1024) // 100MB
					for _, batch := range batches {
						tableSizes := make(map[string]int64)
						for _, file := range batch {
							for table, size := range file.BytesPerTable {
								tableSizes[table] += size
							}
						}
						for _, size := range tableSizes {
							require.LessOrEqual(t, size, maxSizeBytes, "table size exceeds limit in a batch")
						}
					}
				}
			})
		}
	})

	t.Run("key based grouping", func(t *testing.T) {
		testCases := []struct {
			name       string
			files      []*model.StagingFile
			batchSizes []int
		}{
			{
				name: "group by UseRudderStorage",
				files: []*model.StagingFile{
					{
						ID:               1,
						UseRudderStorage: true,
					},
					{
						ID:               2,
						UseRudderStorage: false,
					},
					{
						ID:               3,
						UseRudderStorage: true,
					},
				},
				batchSizes: []int{2, 1},
			},
			{
				name: "group by DestinationRevisionID",
				files: []*model.StagingFile{
					{
						ID:                    1,
						DestinationRevisionID: "rev1",
					},
					{
						ID:                    2,
						DestinationRevisionID: "rev2",
					},
					{
						ID:                    3,
						DestinationRevisionID: "rev1",
					},
				},
				batchSizes: []int{2, 1},
			},
			{
				name: "group by TimeWindow",
				files: []*model.StagingFile{
					{
						ID:         1,
						TimeWindow: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
					},
					{
						ID:         2,
						TimeWindow: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
					},
					{
						ID:         3,
						TimeWindow: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
					},
				},
				batchSizes: []int{2, 1},
			},
			{
				name: "mixed keys",
				files: []*model.StagingFile{
					{
						ID:                    1,
						UseRudderStorage:      true,
						DestinationRevisionID: "rev1",
						TimeWindow:            time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
					},
					{
						ID:                    2,
						UseRudderStorage:      true,
						DestinationRevisionID: "rev1",
						TimeWindow:            time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
					},
					{
						ID:                    3,
						UseRudderStorage:      false,
						DestinationRevisionID: "rev2",
						TimeWindow:            time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
					},
				},
				batchSizes: []int{1, 1, 1},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				lf := loadfiles.LoadFileGenerator{
					Conf:   config.New(),
					Logger: logger.NOP,
				}

				batches := lf.GroupStagingFiles(tc.files, 100)
				require.Equal(t, len(tc.batchSizes), len(batches), "number of batches mismatch")

				// Get actual batch sizes
				actualBatchSizes := make([]int, len(batches))
				for i, batch := range batches {
					actualBatchSizes[i] = len(batch)
				}
				require.ElementsMatch(t, tc.batchSizes, actualBatchSizes, "batch sizes mismatch")

				// Verify that files in each batch have the same grouping attributes
				for _, batch := range batches {
					firstFile := batch[0]
					for _, file := range batch[1:] {
						require.Equal(t, firstFile.UseRudderStorage, file.UseRudderStorage, "UseRudderStorage mismatch in batch")
						require.Equal(t, firstFile.DestinationRevisionID, file.DestinationRevisionID, "DestinationRevisionID mismatch in batch")
						require.Equal(t, firstFile.TimeWindow, file.TimeWindow, "TimeWindow mismatch in batch")
					}
				}
			})
		}
	})
}

func TestV2CreateLoadFiles(t *testing.T) {
	notifier := &mockNotifier{
		t:      t,
		tables: []string{"track", "identify"},
	}
	stageRepo := &mockStageFilesRepo{}
	loadRepo := &mockLoadFilesRepo{}
	controlPlane := &mockControlPlaneClient{}

	lf := loadfiles.LoadFileGenerator{
		Logger:    logger.NOP,
		Notifier:  notifier,
		StageRepo: stageRepo,
		LoadRepo:  loadRepo,
		Conf:      config.New(),

		ControlPlaneClient: controlPlane,
	}

	ctx := context.Background()

	stagingFiles := getStagingFiles()
	for _, file := range stagingFiles {
		file.BytesPerTable = map[string]int64{
			"track":    100,
			"identify": 200,
		}
	}

	job := model.UploadJob{
		Warehouse: model.Warehouse{
			WorkspaceID: "",
			Source:      backendconfig.SourceT{},
			Destination: backendconfig.DestinationT{
				ID:         "destination_id",
				RevisionID: "revision_id",
			},
			Namespace:  "",
			Type:       warehouseutils.SNOWFLAKE,
			Identifier: "",
		},
		Upload: model.Upload{
			DestinationID:    "destination_id",
			DestinationType:  "RS",
			SourceID:         "source_id",
			UseRudderStorage: true,
		},
		StagingFiles: stagingFiles,
	}
	startID, endID, err := lf.CreateLoadFiles(ctx, &job)
	require.NoError(t, err)
	require.Equal(t, int64(1), startID)
	require.Equal(t, int64(2), endID)
	require.Len(t, loadRepo.store, len(notifier.tables))
	require.Len(t, stageRepo.store, len(stagingFiles))
}

func TestV2CreateLoadFiles_Failure(t *testing.T) {
	t.Parallel()

	tables := []string{"track", "identify"}

	warehouse := model.Warehouse{
		WorkspaceID: "",
		Source:      backendconfig.SourceT{},
		Destination: backendconfig.DestinationT{
			ID:         "destination_id",
			RevisionID: "revision_id",
		},
		Namespace:  "",
		Type:       warehouseutils.SNOWFLAKE,
		Identifier: "",
	}

	upload := model.Upload{
		DestinationID:    "destination_id",
		DestinationType:  "RS",
		SourceID:         "source_id",
		UseRudderStorage: true,
	}

	ctx := context.Background()

	t.Run("worker partial failure", func(t *testing.T) {
		notifier := &mockNotifier{
			t:      t,
			tables: tables,
		}
		stageRepo := &mockStageFilesRepo{}
		loadRepo := &mockLoadFilesRepo{}
		controlPlane := &mockControlPlaneClient{}

		lf := loadfiles.LoadFileGenerator{
			Logger:    logger.NOP,
			Notifier:  notifier,
			StageRepo: stageRepo,
			LoadRepo:  loadRepo,
			Conf:      config.New(),

			ControlPlaneClient: controlPlane,
		}

		stagingFiles := getStagingFiles()
		for _, file := range stagingFiles {
			file.BytesPerTable = map[string]int64{
				"track":    100,
				"identify": 200,
			}
		}

		timeWindow := time.Now().Add(time.Hour)
		// Setting time window so that these 2 files are grouped together in a single upload_v2 job
		stagingFiles[0].TimeWindow = timeWindow
		stagingFiles[1].TimeWindow = timeWindow

		// Batch 1 should fail, batch 2 should succeed
		stagingFiles[0].Location = "abort"

		startID, endID, err := lf.CreateLoadFiles(ctx, &model.UploadJob{
			Warehouse:    warehouse,
			Upload:       upload,
			StagingFiles: stagingFiles,
		})
		require.NoError(t, err)

		require.Len(t, loadRepo.store, len(tables))
		require.Equal(t, loadRepo.store[0].ID, startID)
		require.Equal(t, loadRepo.store[len(loadRepo.store)-1].ID, endID)
		require.Equal(t, loadRepo.store[0].TotalRows, 8)
	})

	t.Run("worker failures for all", func(t *testing.T) {
		notifier := &mockNotifier{
			t:      t,
			tables: tables,
		}
		stageRepo := &mockStageFilesRepo{}
		loadRepo := &mockLoadFilesRepo{}
		controlPlane := &mockControlPlaneClient{}

		lf := loadfiles.LoadFileGenerator{
			Logger:    logger.NOP,
			Notifier:  notifier,
			StageRepo: stageRepo,
			LoadRepo:  loadRepo,
			Conf:      config.New(),

			ControlPlaneClient: controlPlane,
		}

		stagingFiles := getStagingFiles()
		for _, file := range stagingFiles {
			file.BytesPerTable = map[string]int64{
				"track":    100,
				"identify": 200,
			}
			file.Location = "abort"
		}

		startID, endID, err := lf.CreateLoadFiles(ctx, &model.UploadJob{
			Warehouse:    warehouse,
			Upload:       upload,
			StagingFiles: stagingFiles,
		})
		require.EqualError(t, err, "no load files generated")
		require.Zero(t, startID)
		require.Zero(t, endID)
		require.Equal(t, warehouseutils.StagingFileFailedState, stageRepo.store[stagingFiles[0].ID].Status)
	})
}
