//go:build !warehouse_integration

package repo_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
)

func Test_LoadFiles(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Second).UTC()
	db := setupDB(t)

	r := repo.NewLoadFiles(db, repo.WithNow(func() time.Time {
		return now
	}))

	var expectedLoadFiles []model.LoadFile
	var stagingIDs []int64

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
		}

		expectedLoadFiles = loadFiles
	})

	t.Run("get", func(t *testing.T) {
		loadFiles, err := r.GetByStagingFiles(ctx, stagingIDs)
		require.Len(t, loadFiles, len(expectedLoadFiles))
		require.NoError(t, err)

		for i := range loadFiles {
			require.Equal(t, expectedLoadFiles[i], loadFiles[i])
		}
	})

	t.Run("delete", func(t *testing.T) {
		err := r.DeleteByStagingFiles(ctx, stagingIDs[1:])
		require.NoError(t, err)

		loadFiles, err := r.GetByStagingFiles(ctx, stagingIDs)
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

		gotLoadFiles, err := r.GetByStagingFiles(ctx, []int64{stagingID})
		require.NoError(t, err)

		require.Len(t, gotLoadFiles, 1)
		lastLoadFile.ID = gotLoadFiles[0].ID
		require.Equal(t, lastLoadFile, gotLoadFiles[0])
	})
}
