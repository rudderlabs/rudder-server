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
	for i := 0; i < 10; i++ {
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

func TestCreateLoadFiles(t *testing.T) {
	t.Parallel()
	notifier := &mockNotifier{
		t:      t,
		tables: []string{"track", "indentify"},
	}
	stageRepo := &mockStageFilesRepo{}
	loadRepo := &mockLoadFilesRepo{}
	controlPlane := &mockControlPlaneClient{}

	lf := loadfiles.LoadFileGenerator{
		Logger:    logger.NOP,
		Notifier:  notifier,
		StageRepo: stageRepo,
		LoadRepo:  loadRepo,

		ControlPlaneClient: controlPlane,
	}

	ctx := context.Background()

	stagingFiles := getStagingFiles()

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
	require.Equal(t, int64(20), endID)

	require.Len(t, loadRepo.store, len(stagingFiles)*len(notifier.tables))
	require.Len(t, stageRepo.store, len(stagingFiles))

	for _, stagingFile := range stagingFiles {
		loadFiles, err := loadRepo.GetByStagingFiles(ctx, []int64{stagingFile.ID})
		require.Equal(t, 2, len(loadFiles))
		require.NoError(t, err)

		var tableNames []string
		for _, loadFile := range loadFiles {
			require.Equal(t, stagingFile.ID, loadFile.StagingFileID)
			require.Contains(t, loadFile.Location, fmt.Sprintf("s3://bucket/path/to/file/%d", stagingFile.ID))
			require.Equal(t, stagingFile.SourceID, loadFile.SourceID)
			require.Equal(t, stagingFile.DestinationID, loadFile.DestinationID)
			require.Equal(t, stagingFile.DestinationRevisionID, loadFile.DestinationRevisionID)
			require.Equal(t, stagingFile.UseRudderStorage, loadFile.UseRudderStorage)

			tableNames = append(tableNames, loadFile.TableName)
		}
		require.ElementsMatch(t, notifier.tables, tableNames)
		require.Equal(t, warehouseutils.StagingFileSucceededState, stageRepo.store[stagingFile.ID].Status)
	}

	t.Run("skip already processed", func(t *testing.T) {
		t.Log("all staging file are successfully processed, except one")
		for _, stagingFile := range stagingFiles {
			stagingFile.Status = warehouseutils.StagingFileSucceededState
		}
		stagingFiles[0].Status = warehouseutils.StagingFileFailedState

		startID, endID, err := lf.CreateLoadFiles(ctx, &job)
		require.NoError(t, err)
		require.Equal(t, int64(21), startID)
		require.Equal(t, int64(22), endID)

		require.Len(t, loadRepo.store, len(stagingFiles)*len(notifier.tables))
	})

	t.Run("force recreate", func(t *testing.T) {
		for _, stagingFile := range stagingFiles {
			stagingFile.Status = warehouseutils.StagingFileSucceededState
		}

		startID, endID, err := lf.ForceCreateLoadFiles(ctx, &job)
		require.NoError(t, err)
		require.Equal(t, int64(23), startID)
		require.Equal(t, int64(42), endID)

		require.Len(t, loadRepo.store, len(stagingFiles)*len(notifier.tables))
		require.Len(t, stageRepo.store, len(stagingFiles))
	})
}

func TestCreateLoadFiles_Failure(t *testing.T) {
	t.Parallel()

	tables := []string{"track", "indentify"}

	warehouse := model.Warehouse{
		WorkspaceID: "",
		Source:      backendconfig.SourceT{},
		Destination: backendconfig.DestinationT{
			ID:         "destination_id",
			RevisionID: "revision_id",
		},
		Namespace:  "",
		Type:       "",
		Identifier: "",
	}

	upload := model.Upload{
		DestinationID:    "destination_id",
		DestinationType:  warehouseutils.SNOWFLAKE,
		SourceID:         "source_id",
		UseRudderStorage: true,
	}

	ctx := context.Background()

	t.Run("worker partial failure", func(t *testing.T) {
		notifer := &mockNotifier{
			t:      t,
			tables: tables,
		}
		stageRepo := &mockStageFilesRepo{}
		loadRepo := &mockLoadFilesRepo{}
		controlPlane := &mockControlPlaneClient{}

		lf := loadfiles.LoadFileGenerator{
			Logger:    logger.NOP,
			Notifier:  notifer,
			StageRepo: stageRepo,
			LoadRepo:  loadRepo,

			ControlPlaneClient: controlPlane,
		}

		stagingFiles := getStagingFiles()

		t.Log("empty location should cause worker failure")
		stagingFiles[0].Location = ""

		startID, endID, err := lf.CreateLoadFiles(ctx, &model.UploadJob{
			Warehouse:    warehouse,
			Upload:       upload,
			StagingFiles: stagingFiles,
		})
		require.NoError(t, err)
		require.Equal(t, int64(1), startID)

		require.Len(t,
			loadRepo.store,
			len(tables)*(len(stagingFiles)-1),
		)

		require.Equal(t, loadRepo.store[0].ID, startID)
		require.Equal(t, loadRepo.store[len(loadRepo.store)-1].ID, endID)

		require.Equal(t, warehouseutils.StagingFileFailedState, stageRepo.store[stagingFiles[0].ID].Status)
		require.EqualError(t, stageRepo.store[stagingFiles[0].ID].Error, "staging file location is empty")
	})
	t.Run("worker failure for all", func(t *testing.T) {
		notifer := &mockNotifier{
			t:      t,
			tables: tables,
		}
		stageRepo := &mockStageFilesRepo{}
		loadRepo := &mockLoadFilesRepo{}
		controlPlane := &mockControlPlaneClient{}

		lf := loadfiles.LoadFileGenerator{
			Logger:    logger.NOP,
			Notifier:  notifer,
			StageRepo: stageRepo,
			LoadRepo:  loadRepo,

			ControlPlaneClient: controlPlane,
		}

		stagingFiles := getStagingFiles()

		t.Log("empty location should cause worker failure")
		for i := range stagingFiles {
			stagingFiles[i].Location = ""
		}

		startID, endID, err := lf.CreateLoadFiles(ctx, &model.UploadJob{
			Warehouse:    warehouse,
			Upload:       upload,
			StagingFiles: stagingFiles,
		})
		require.EqualError(t, err, "no load files generated. Sample error: staging file location is empty")
		require.Zero(t, startID)
		require.Zero(t, endID)
	})
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
			Config: map[string]interface{}{
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

	loadFiles, err := loadRepo.GetByStagingFiles(ctx, []int64{stagingFile.ID})
	require.Equal(t, 2, len(loadFiles))
	require.NoError(t, err)

	var tableNames []string
	for _, loadFile := range loadFiles {
		require.Equal(t, stagingFile.ID, loadFile.StagingFileID)
		require.Contains(t, loadFile.Location, "s3://bucket/path/to/file/1")
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
		notifer.requests[0].StagingDestinationConfig,
	)

	t.Run("invalid revision ID", func(t *testing.T) {
		stagingFile.DestinationRevisionID = "invalid_revision_id"

		startID, endID, err := lf.CreateLoadFiles(ctx, &job)
		require.EqualError(t, err, "populating destination revision ID: revision \"invalid_revision_id\" not found")
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
					Config: map[string]interface{}{},
				},
				Type: warehouseutils.S3Datalake,
			},
			expected: "2022/08/06/14",
		},
		{
			name: "s3 datalake with glue",
			warehouse: model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
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
					Config: map[string]interface{}{
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
					Config: map[string]interface{}{
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
					Config: map[string]interface{}{},
				},
				Type: warehouseutils.GCSDatalake,
			},
			expected: "2022/08/06/14",
		},
		{
			name: "gcs datalake with suffix",
			warehouse: model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
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
					Config: map[string]interface{}{
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
					Config: map[string]interface{}{
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
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			timeWindow := time.Date(2022, time.Month(8), 6, 14, 10, 30, 0, time.UTC)
			lf := loadfiles.LoadFileGenerator{}
			lf.Conf = config.New()
			require.Equal(t, lf.GetLoadFilePrefix(timeWindow, tc.warehouse), tc.expected)
		})
	}
}
