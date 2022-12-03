package loadfiles_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/warehouse/internal/loadfiles"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

type mockControlPlaneClient struct {
	revisions map[string]backendconfig.DestinationT
}

func (m *mockControlPlaneClient) DestinationHistory(ctx context.Context, revisionID string) (backendconfig.DestinationT, error) {
	dest, ok := m.revisions[revisionID]
	if !ok {
		return backendconfig.DestinationT{}, fmt.Errorf("revision %q not found", revisionID)
	}

	return dest, nil
}

func Test_CreateLoadFiles(t *testing.T) {
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

	var stagingFiles []*model.StagingFile
	for i := 0; i < 10; i++ {
		stagingFiles = append(stagingFiles, &model.StagingFile{
			ID:                    int64(i),
			WorkspaceID:           "workspace_id",
			Location:              fmt.Sprintf("s3://bucket/path/to/file/%d", i),
			SourceID:              "source_id",
			DestinationID:         "destination_id",
			Status:                "",
			FirstEventAt:          now.Add(-time.Hour),
			LastEventAt:           now.Add(-2 * time.Hour),
			UseRudderStorage:      true,
			DestinationRevisionID: "revision_id",
		})
	}

	job := model.UploadJob{
		Warehouse: warehouseutils.Warehouse{
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
			DestinationType:  "RS",
			SourceID:         "source_id",
			UseRudderStorage: true,
		},
		StagingFiles: stagingFiles,
	}

	startID, endID, err := lf.CreateLoadFiles(ctx, job, false)
	require.NoError(t, err)
	require.Equal(t, int64(1), startID)
	require.Equal(t, int64(20), endID)

	require.Len(t, loadRepo.store, len(stagingFiles)*len(notifer.tables))
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
		require.ElementsMatch(t, notifer.tables, tableNames)
		require.Equal(t, warehouseutils.StagingFileSucceededState, stageRepo.store[stagingFile.ID].Status)
	}
}

func Test_CreateLoadFiles_DestinationHistory(t *testing.T) {
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
		Warehouse: warehouseutils.Warehouse{
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
			DestinationType:  "RS",
			SourceID:         "source_id",
			UseRudderStorage: true,
		},
		StagingFiles: []*model.StagingFile{
			stagingFile,
		},
	}

	startID, endID, err := lf.CreateLoadFiles(ctx, job, false)
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

		startID, endID, err := lf.CreateLoadFiles(ctx, job, false)
		require.EqualError(t, err, "populating destination revision ID: revision \"invalid_revision_id\" not found")
		require.Zero(t, startID)
		require.Zero(t, endID)
	})
}
