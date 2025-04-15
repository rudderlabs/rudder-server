package repo_test

import (
	"context"
	"fmt"
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
	stagingFilesRepo := repo.NewStagingFiles(db)
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

	r := repo.NewLoadFiles(db, config.New(), repo.WithNow(func() time.Time {
		return now
	}))

	var expectedLoadFiles []model.LoadFile
	var stagingIDs []int64
	uploadID := int64(-1)

	t.Run("insert", func(t *testing.T) {
		var loadFiles []model.LoadFile

		for i := 0; i < 10; i++ {
			loadFile := model.LoadFile{
				TableName:             "table_name",
				Location:              "s3://bucket/path/to/file",
				TotalRows:             10,
				ContentLength:         1000,
				StagingFileID:         int64(i + 1),
				DestinationRevisionID: "revision_id",
				UseRudderStorage:      true,
				SourceID:              "source_id",
				DestinationID:         "destination_id",
				DestinationType:       "RS",
			}

			stagingIDs = append(stagingIDs, loadFile.StagingFileID)
			loadFiles = append(loadFiles, loadFile)
		}
		err := r.Insert(ctx, loadFiles)
		require.NoError(t, err)

		for i := range loadFiles {
			loadFiles[i].ID = int64(i + 1)
			loadFiles[i].CreatedAt = now
		}

		expectedLoadFiles = loadFiles
	})

	t.Run("get", func(t *testing.T) {
		loadFiles, err := r.Get(ctx, uploadID, stagingIDs)
		require.Len(t, loadFiles, len(expectedLoadFiles))
		require.NoError(t, err)

		for i := range loadFiles {
			require.Equal(t, expectedLoadFiles[i], loadFiles[i])
		}
	})

	t.Run("delete", func(t *testing.T) {
		err := r.Delete(ctx, uploadID, stagingIDs[1:])
		require.NoError(t, err)

		loadFiles, err := r.Get(ctx, uploadID, stagingIDs)
		require.Len(t, loadFiles, 1)
		require.NoError(t, err)

		for i := range loadFiles {
			require.Equal(t, expectedLoadFiles[i], loadFiles[i])
		}
	})

	t.Run("get latest for stagingID", func(t *testing.T) {
		stagingID := int64(42)

		var lastLoadFile model.LoadFile
		var loadFiles []model.LoadFile
		for i := 0; i < 10; i++ {
			loadFile := model.LoadFile{
				TableName:             "table_name",
				Location:              fmt.Sprintf("s3://bucket/path/to/file/%d", i),
				TotalRows:             10,
				ContentLength:         1000,
				StagingFileID:         stagingID,
				DestinationRevisionID: "revision_id",
				UseRudderStorage:      true,
				SourceID:              "source_id",
				DestinationID:         "destination_id",
				DestinationType:       "RS",
			}
			loadFiles = append(loadFiles, loadFile)
			lastLoadFile = loadFile
		}
		err := r.Insert(ctx, loadFiles)
		require.NoError(t, err)

		gotLoadFiles, err := r.Get(ctx, uploadID, []int64{stagingID})
		require.NoError(t, err)

		require.Len(t, gotLoadFiles, 1)
		lastLoadFile.ID = gotLoadFiles[0].ID
		lastLoadFile.CreatedAt = gotLoadFiles[0].CreatedAt
		require.Equal(t, lastLoadFile, gotLoadFiles[0])
	})
}

func Test_LoadFiles_WithUploadID(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Second).UTC()
	db := setupDB(t)

	conf := config.New()
	conf.Set("Warehouse.loadFiles.queryWithUploadID.enable", true)

	r := repo.NewLoadFiles(db, conf, repo.WithNow(func() time.Time {
		return now
	}))

	var upload1LoadFiles []model.LoadFile
	var stagingIDs []int64

	uploadID1 := createUpload(t, ctx, db)
	uploadID2 := createUpload(t, ctx, db)
	uploads := []int64{uploadID1, uploadID2}
	t.Run("insert", func(t *testing.T) {
		var loadFiles []model.LoadFile

		for i := 0; i < 10; i++ {
			loadFile := model.LoadFile{
				TableName:             "table_name__" + strconv.Itoa(i),
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

			stagingIDs = append(stagingIDs, loadFile.StagingFileID)
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
		loadFiles, err := r.Get(ctx, uploadID1, []int64{})
		require.Len(t, loadFiles, len(upload1LoadFiles))
		require.NoError(t, err)

		for i := range loadFiles {
			require.Equal(t, upload1LoadFiles[i], loadFiles[i])
		}
	})

	t.Run("delete", func(t *testing.T) {
		err := r.Delete(ctx, uploadID2, []int64{})
		require.NoError(t, err)

		loadFiles, err := r.Get(ctx, uploadID2, []int64{})
		require.Len(t, loadFiles, 0)
		require.NoError(t, err)
	})

	t.Run("get latest for stagingID", func(t *testing.T) {
		var lastLoadFile model.LoadFile
		var loadFiles []model.LoadFile
		for i := 0; i < 10; i++ {
			loadFile := model.LoadFile{
				TableName:             "table_name",
				Location:              fmt.Sprintf("s3://bucket/path/to/file/%d", i),
				TotalRows:             10,
				ContentLength:         1000,
				DestinationRevisionID: "revision_id",
				UploadID:              &uploadID2,
				UseRudderStorage:      true,
				SourceID:              "source_id",
				DestinationID:         "destination_id",
				DestinationType:       "RS",
			}
			loadFiles = append(loadFiles, loadFile)
			lastLoadFile = loadFile
		}
		err := r.Insert(ctx, loadFiles)
		require.NoError(t, err)

		gotLoadFiles, err := r.Get(ctx, uploadID2, []int64{})
		require.NoError(t, err)

		require.Len(t, gotLoadFiles, 1)
		lastLoadFile.ID = gotLoadFiles[0].ID
		lastLoadFile.CreatedAt = gotLoadFiles[0].CreatedAt
		require.Equal(t, lastLoadFile, gotLoadFiles[0])
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
			StagingFileID:         int64(i + 1),
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

	r := repo.NewLoadFiles(db, config.New(), repo.WithNow(func() time.Time {
		return now
	}))

	stagingFilesCount := 960
	loadFilesCount := 25
	retriesCount := 3

	loadFiles := make([]model.LoadFile, 0, stagingFilesCount*loadFilesCount*retriesCount)
	stagingFileIDs := make([]int64, 0, stagingFilesCount)

	for i := 0; i < stagingFilesCount; i++ {
		for j := 0; j < loadFilesCount; j++ {
			for k := 0; k < retriesCount; k++ {
				loadFiles = append(loadFiles, model.LoadFile{
					TableName:             "table_name_" + strconv.Itoa(j+1),
					Location:              "s3://bucket/path/to/file",
					TotalRows:             (i + 1) + (j + 1) + (k + 1),
					ContentLength:         1000,
					StagingFileID:         int64(i + 1),
					DestinationRevisionID: "revision_id",
					UseRudderStorage:      true,
					SourceID:              "source_id",
					DestinationID:         "destination_id",
					DestinationType:       "RS",
				})
			}
		}
		stagingFileIDs = append(stagingFileIDs, int64(i+1))
	}

	err := r.Insert(ctx, loadFiles)
	require.NoError(t, err)
	uploadID := int64(-1)

	t.Run("no staging files", func(t *testing.T) {
		exportedEvents, err := r.TotalExportedEvents(ctx, uploadID, []int64{-1}, []string{})
		require.NoError(t, err)
		require.Zero(t, exportedEvents)
	})
	t.Run("without skip tables", func(t *testing.T) {
		exportedEvents, err := r.TotalExportedEvents(ctx, uploadID, stagingFileIDs, nil)
		require.NoError(t, err)

		actualEvents := lo.SumBy(stagingFileIDs, func(item int64) int64 {
			sum := 0
			for j := 0; j < loadFilesCount; j++ {
				sum += int(item) + (j + 1) + retriesCount
			}
			return int64(sum)
		})
		require.Equal(t, actualEvents, exportedEvents)
	})
	t.Run("with skip tables", func(t *testing.T) {
		excludeIDS := []int64{1, 3, 5, 7, 9}

		skipTable := lo.Map(excludeIDS, func(item int64, index int) string {
			return "table_name_" + strconv.Itoa(int(item))
		})

		exportedEvents, err := r.TotalExportedEvents(ctx, uploadID, stagingFileIDs, skipTable) // 11916000
		require.NoError(t, err)

		actualEvents := lo.SumBy(stagingFileIDs, func(item int64) int64 {
			sum := 0
			for j := 0; j < loadFilesCount; j++ {
				if lo.Contains(excludeIDS, int64(j+1)) {
					continue
				}
				sum += int(item) + (j + 1) + retriesCount
			}
			return int64(sum)
		})
		require.Equal(t, actualEvents, exportedEvents)
	})
	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		exportedEvents, err := r.TotalExportedEvents(ctx, uploadID, stagingFileIDs, nil)
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, exportedEvents)
	})
}

func TestLoadFiles_TotalExportedEvents_WithUploadID(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Second).UTC()
	db := setupDB(t)
	conf := config.New()
	conf.Set("Warehouse.loadFiles.queryWithUploadID.enable", true)

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

	for i := 0; i < stagingFilesCount; i++ {
		for j := 0; j < loadFilesCount; j++ {
			for k := 0; k < retriesCount; k++ {
				tableName := "table_name_" + strconv.Itoa(i+1) + "_" + strconv.Itoa(j+1)
				loadFiles = append(loadFiles, model.LoadFile{
					TableName:             tableName,
					Location:              "s3://bucket/path/to/file",
					TotalRows:             (i + 1) + (j + 1) + (k + 1),
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
				}
			}
			totalEvents += (i + 1) + (j + 1) + retriesCount
		}
	}

	err := r.Insert(ctx, loadFiles)
	require.NoError(t, err)

	t.Run("no staging files", func(t *testing.T) {
		exportedEvents, err := r.TotalExportedEvents(ctx, int64(-1), []int64{-1}, []string{})
		require.NoError(t, err)
		require.Zero(t, exportedEvents)
	})
	t.Run("without skip tables", func(t *testing.T) {
		exportedEvents, err := r.TotalExportedEvents(ctx, uploadID, []int64{}, nil)
		require.NoError(t, err)
		require.Equal(t, int64(totalEvents), exportedEvents)
	})
	t.Run("with skip tables", func(t *testing.T) {
		exportedEvents, err := r.TotalExportedEvents(ctx, uploadID, []int64{}, skipTables)
		require.NoError(t, err)
		require.Equal(t, int64(retriesCount+2), exportedEvents)
	})
	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		exportedEvents, err := r.TotalExportedEvents(ctx, uploadID, []int64{}, nil)
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
				StagingFileID:         int64(i + 1),
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
