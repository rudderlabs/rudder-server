package repo_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/samber/lo"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"

	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
)

func createUpload(t *testing.T, ctx context.Context, db *sqlmiddleware.DB) int64 {
	t.Helper()
	stagingFilesRepo := repo.NewStagingFiles(db, config.New())
	stagingFile := model.StagingFileWithSchema{
		StagingFile: model.StagingFile{
			ID: 1,
		},
	}
	_, err := stagingFilesRepo.Insert(ctx, &stagingFile)
	require.NoError(t, err)
	stagingFiles := []*model.StagingFile{&stagingFile.StagingFile}
	uploadRepo := repo.NewUploads(db)
	upload := model.Upload{}
	uploadID, err := uploadRepo.CreateWithStagingFiles(ctx, upload, stagingFiles)
	require.NoError(t, err)
	return uploadID
}

func Test_LoadFiles(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Second).UTC()
	db := setupDB(t)

	conf := config.New()

	r := repo.NewLoadFiles(db, conf, repo.WithNow(func() time.Time {
		return now
	}))

	var upload1LoadFiles []model.LoadFile

	uploadID1 := createUpload(t, ctx, db)
	uploadID2 := createUpload(t, ctx, db)
	uploads := []int64{uploadID1, uploadID2}
	t.Run("insert", func(t *testing.T) {
		var loadFiles []model.LoadFile

		for i := 0; i < 10; i++ {
			loadFile := model.LoadFile{
				TableName:             "table_name__" + strconv.Itoa(i%3),
				Location:              "s3://bucket/path/to/file",
				TotalRows:             10,
				ContentLength:         1000,
				UploadID:              &uploads[i%2],
				DestinationRevisionID: "revision_id",
				UseRudderStorage:      true,
				SourceID:              "source_id",
				DestinationID:         "destination_id",
				DestinationType:       "RS",
			}

			loadFiles = append(loadFiles, loadFile)
			if i%2 == 0 {
				upload1LoadFiles = append(upload1LoadFiles, loadFile)
			}
		}
		err := r.Insert(ctx, loadFiles)
		require.NoError(t, err)

		for i := range loadFiles {
			loadFiles[i].ID = int64(i + 1)
			loadFiles[i].CreatedAt = now
		}
		for i := range upload1LoadFiles {
			upload1LoadFiles[i].ID = int64(2*i + 1)
			upload1LoadFiles[i].CreatedAt = now
		}
	})

	t.Run("get", func(t *testing.T) {
		loadFiles, err := r.Get(ctx, uploadID1)
		require.Len(t, loadFiles, len(upload1LoadFiles))
		require.NoError(t, err)

		for i := range loadFiles {
			require.Equal(t, upload1LoadFiles[i], loadFiles[i])
		}
	})

	t.Run("delete", func(t *testing.T) {
		err := r.Delete(ctx, uploadID2)
		require.NoError(t, err)

		loadFiles, err := r.Get(ctx, uploadID2)
		require.Len(t, loadFiles, 0)
		require.NoError(t, err)
	})
}

func TestLoadFiles_GetByID(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	db := setupDB(t)

	r := repo.NewLoadFiles(db, config.New(), repo.WithNow(func() time.Time {
		return now
	}))

	uploadID := createUpload(t, ctx, db)
	loadFiles := lo.RepeatBy(10, func(i int) model.LoadFile {
		file := model.LoadFile{
			TableName:             "table_name",
			Location:              "s3://bucket/path/to/file",
			TotalRows:             10,
			ContentLength:         1000,
			DestinationRevisionID: "revision_id",
			UseRudderStorage:      true,
			SourceID:              "source_id",
			DestinationID:         "destination_id",
			DestinationType:       "RS",
		}
		// Not adding uploadID for first file to test NULL value
		if i != 0 {
			file.UploadID = &uploadID
		}
		return file
	})
	require.NoError(t, r.Insert(ctx, loadFiles))

	for i := range loadFiles {
		loadFiles[i].ID = int64(i + 1)
		loadFiles[i].CreatedAt = now
	}

	t.Run("found", func(t *testing.T) {
		for _, loadFile := range loadFiles {
			gotLoadFile, err := r.GetByID(ctx, loadFile.ID)
			require.NoError(t, err)
			require.EqualValues(t, loadFile, *gotLoadFile)
		}
	})
	t.Run("not found", func(t *testing.T) {
		loadFile, err := r.GetByID(ctx, -1)
		require.ErrorIs(t, err, model.ErrLoadFileNotFound)
		require.Nil(t, loadFile)
	})
	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		loadFile, err := r.GetByID(ctx, -1)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, loadFile)
	})
}

func TestLoadFiles_TotalExportedEvents(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Second).UTC()
	db := setupDB(t)
	conf := config.New()

	r := repo.NewLoadFiles(db, conf, repo.WithNow(func() time.Time {
		return now
	}))

	stagingFilesCount := 960
	loadFilesCount := 25
	retriesCount := 3

	loadFiles := make([]model.LoadFile, 0, stagingFilesCount*loadFilesCount*retriesCount)
	totalEvents := 0
	uploadID := createUpload(t, ctx, db)
	var skipTables []string
	skipTablesTotalEvents := 0

	for i := 0; i < stagingFilesCount; i++ {
		for j := 0; j < loadFilesCount; j++ {
			for k := 0; k < retriesCount; k++ {
				tableName := "table_name_" + strconv.Itoa(i+1) + "_" + strconv.Itoa(j+1)
				rows := (i + 1) + (j + 1) + (k + 1)
				loadFiles = append(loadFiles, model.LoadFile{
					TableName:             tableName,
					Location:              "s3://bucket/path/to/file",
					TotalRows:             rows,
					ContentLength:         1000,
					UploadID:              &uploadID,
					DestinationRevisionID: "revision_id",
					UseRudderStorage:      true,
					SourceID:              "source_id",
					DestinationID:         "destination_id",
					DestinationType:       "RS",
				})
				if i != 0 || j != 0 {
					// Skip every table except the first one
					skipTables = append(skipTables, tableName)
				} else {
					skipTablesTotalEvents += rows
				}
				totalEvents += rows
			}
		}
	}

	err := r.Insert(ctx, loadFiles)
	require.NoError(t, err)

	t.Run("without skip tables", func(t *testing.T) {
		exportedEvents, err := r.TotalExportedEvents(ctx, uploadID, nil)
		require.NoError(t, err)
		require.Equal(t, int64(totalEvents), exportedEvents)
	})
	t.Run("with skip tables", func(t *testing.T) {
		exportedEvents, err := r.TotalExportedEvents(ctx, uploadID, skipTables)
		require.NoError(t, err)
		require.Equal(t, int64(skipTablesTotalEvents), exportedEvents)
	})
	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		exportedEvents, err := r.TotalExportedEvents(ctx, uploadID, nil)
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, exportedEvents)
	})
}

func TestLoadFiles_DistinctTableName(t *testing.T) {
	sourceID := "source_id"
	destinationID := "destination_id"

	ctx := context.Background()
	now := time.Now().Truncate(time.Second).UTC()
	db := setupDB(t)

	r := repo.NewLoadFiles(db, config.New(), repo.WithNow(func() time.Time {
		return now
	}))

	stagingFilesCount := 960
	loadFilesCount := 25

	loadFiles := make([]model.LoadFile, 0, stagingFilesCount*loadFilesCount)

	for i := 0; i < stagingFilesCount; i++ {
		for j := 0; j < loadFilesCount; j++ {
			loadFiles = append(loadFiles, model.LoadFile{
				TableName:             "table_name_" + strconv.Itoa(j+1),
				Location:              "s3://bucket/path/to/file",
				TotalRows:             (i + 1) + (j + 1),
				ContentLength:         1000,
				DestinationRevisionID: "revision_id",
				UseRudderStorage:      true,
				SourceID:              sourceID,
				DestinationID:         destinationID,
				DestinationType:       "RS",
			})
		}
	}

	err := r.Insert(ctx, loadFiles)
	require.NoError(t, err)

	t.Run("no staging files", func(t *testing.T) {
		tables, err := r.DistinctTableName(ctx, sourceID, destinationID, -1, -1)
		require.NoError(t, err)
		require.Zero(t, tables)
	})
	t.Run("some staging files", func(t *testing.T) {
		tables, err := r.DistinctTableName(ctx, sourceID, destinationID, 1, int64(len(loadFiles)))
		require.NoError(t, err)
		require.Len(t, tables, loadFilesCount)
	})
	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		tables, err := r.DistinctTableName(ctx, sourceID, destinationID, -1, -1)
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, tables)
	})
}
