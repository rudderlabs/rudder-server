package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"
)

func TestBackupFileMigrationIntegration(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	prepareFileManager := func(t testing.TB) filemanager.FileManager {
		minioContainer, err := minio.Setup(pool, t)
		require.NoError(t, err)

		t.Setenv("JOBS_BACKUP_STORAGE_PROVIDER", "MINIO")
		t.Setenv("MINIO_ENDPOINT", minioContainer.Endpoint)
		t.Setenv("JOBS_BACKUP_BUCKET", minioContainer.BucketName)
		t.Setenv("MINIO_SECRET_ACCESS_KEY", minioContainer.AccessKeySecret)
		t.Setenv("MINIO_ACCESS_KEY_ID", minioContainer.AccessKeyID)

		fm, err := filemanager.NewMinioManager(minioContainer.ToFileManagerConfig(""), logger.NewLogger(), func() time.Duration {
			return time.Minute
		})
		require.NoError(t, err)
		return fm
	}

	t.Run("test successful migration - batch size 5", func(t *testing.T) {
		fm := prepareFileManager(t)
		f, err := os.Open("testdata/gw_jobs_52306.1311260446.1311289420.1703893976586.1703894226969.workspace.gz")
		require.NoError(t, err)
		_, err = fm.Upload(context.Background(), f, "dummy", "dummy-v0-rudderstack-10")
		require.NoError(t, err)

		require.NoError(t, app.Run([]string{
			"migration-tool",
			"migrate",
			"-startTime", "2023-12-29T23:52:55.726942+00:00",
			"-endTime", "2023-12-29T23:53:59.809574+00:00",
			"-uploadBatchSize", "5",
			"-backupFileNamePrefix", "dummy",
		}))

		itr := filemanager.IterateFilesWithPrefix(context.Background(), "", "", 100, fm)
		listOfTransformedFiles := make([]string, 0)
		for itr.Next() {
			listOfTransformedFiles = append(listOfTransformedFiles, itr.Get().Key)
		}
		require.Len(t, listOfTransformedFiles, 9) // 8 converted files and one original file
		require.ElementsMatch(t, listOfTransformedFiles, []string{
			"dummy/dummy-v0-rudderstack-10/gw_jobs_52306.1311260446.1311289420.1703893976586.1703894226969.workspace.gz",
			"dummy/source_id_1/gw/2023-12-29/23/dummy-v0-rudderstack-10/1703893984_1703894012_workspace.json.gz",
			"dummy/source_id_2/gw/2023-12-29/23/dummy-v0-rudderstack-10/1703893979_1703893986_workspace.json.gz",
			"dummy/source_id_2/gw/2023-12-29/23/dummy-v0-rudderstack-10/1703893988_1703893993_workspace.json.gz",
			"dummy/source_id_2/gw/2023-12-29/23/dummy-v0-rudderstack-10/1703893994_1703894000_workspace.json.gz",
			"dummy/source_id_2/gw/2023-12-29/23/dummy-v0-rudderstack-10/1703894001_1703894008_workspace.json.gz",
			"dummy/source_id_2/gw/2023-12-29/23/dummy-v0-rudderstack-10/1703894009_1703894014_workspace.json.gz",
			"dummy/source_id_3/gw/2023-12-29/23/dummy-v0-rudderstack-10/1703893981_1703893995_workspace.json.gz",
			"dummy/source_id_3/gw/2023-12-29/23/dummy-v0-rudderstack-10/1703893997_1703894004_workspace.json.gz",
		})
	})

	t.Run("test successful migration - batch size 12", func(t *testing.T) {
		fm := prepareFileManager(t)
		f, err := os.Open("testdata/gw_jobs_52306.1311260446.1311289420.1703893976586.1703894226969.workspace.gz")
		require.NoError(t, err)
		_, err = fm.Upload(context.Background(), f, "dummy", "dummy-v0-rudderstack-10")
		require.NoError(t, err)

		require.NoError(t, app.Run([]string{
			"migration-tool",
			"migrate",
			"-startTime", "2023-12-29T23:52:55.726942+00:00",
			"-endTime", "2023-12-29T23:53:59.809574+00:00",
			"-uploadBatchSize", "12",
			"-backupFileNamePrefix", "dummy",
		}))

		itr := filemanager.IterateFilesWithPrefix(context.Background(), "", "", 100, fm)
		listOfTransformedFiles := make([]string, 0)
		for itr.Next() {
			listOfTransformedFiles = append(listOfTransformedFiles, itr.Get().Key)
		}
		require.Len(t, listOfTransformedFiles, 6) // 5 converted files and 1 original file
		require.ElementsMatch(t, listOfTransformedFiles, []string{
			"dummy/dummy-v0-rudderstack-10/gw_jobs_52306.1311260446.1311289420.1703893976586.1703894226969.workspace.gz",
			"dummy/source_id_2/gw/2023-12-29/23/dummy-v0-rudderstack-10/1703893979_1703893996_workspace.json.gz",
			"dummy/source_id_2/gw/2023-12-29/23/dummy-v0-rudderstack-10/1703893998_1703894013_workspace.json.gz",
			"dummy/source_id_2/gw/2023-12-29/23/dummy-v0-rudderstack-10/1703894014_1703894014_workspace.json.gz",
			"dummy/source_id_3/gw/2023-12-29/23/dummy-v0-rudderstack-10/1703893981_1703894004_workspace.json.gz",
			"dummy/source_id_1/gw/2023-12-29/23/dummy-v0-rudderstack-10/1703893984_1703894012_workspace.json.gz",
		})
	})

	t.Run("no migration - out of time range", func(t *testing.T) {
		fm := prepareFileManager(t)
		f, err := os.Open("testdata/gw_jobs_52306.1311260446.1311289420.1703893976586.1703894226969.workspace.gz")
		require.NoError(t, err)
		_, err = fm.Upload(context.Background(), f, "dummy", "dummy-v0-rudderstack-10")
		require.NoError(t, err)

		require.NoError(t, app.Run([]string{
			"migration-tool",
			"migrate",
			"-startTime", "2023-11-29T23:52:55.726942+00:00",
			"-endTime", "2023-11-29T23:53:59.809574+00:00",
			"-uploadBatchSize", "12",
			"-backupFileNamePrefix", "dummy",
		}))

		itr := filemanager.IterateFilesWithPrefix(context.Background(), "", "", 100, fm)
		listOfTransformedFiles := make([]string, 0)
		for itr.Next() {
			listOfTransformedFiles = append(listOfTransformedFiles, itr.Get().Key)
		}
		require.ElementsMatch(t, listOfTransformedFiles, []string{
			"dummy/dummy-v0-rudderstack-10/gw_jobs_52306.1311260446.1311289420.1703893976586.1703894226969.workspace.gz",
		})
	})

	t.Run("no migration - no file with prefix", func(t *testing.T) {
		fm := prepareFileManager(t)
		f, err := os.Open("testdata/gw_jobs_52306.1311260446.1311289420.1703893976586.1703894226969.workspace.gz")
		require.NoError(t, err)
		_, err = fm.Upload(context.Background(), f, "dummy", "dummy-v0-rudderstack-10")
		require.NoError(t, err)

		require.NoError(t, app.Run([]string{
			"migration-tool",
			"migrate",
			"-startTime", "2023-11-29T23:52:55.726942+00:00",
			"-endTime", "2023-11-29T23:53:59.809574+00:00",
			"-uploadBatchSize", "12",
			"-backupFileNamePrefix", "randomPrefix",
		}))

		itr := filemanager.IterateFilesWithPrefix(context.Background(), "", "", 100, fm)
		listOfTransformedFiles := make([]string, 0)
		for itr.Next() {
			listOfTransformedFiles = append(listOfTransformedFiles, itr.Get().Key)
		}
		require.ElementsMatch(t, listOfTransformedFiles, []string{
			"dummy/dummy-v0-rudderstack-10/gw_jobs_52306.1311260446.1311289420.1703893976586.1703894226969.workspace.gz",
		})
	})

	t.Run("failed to migrate file - parsing fail", func(t *testing.T) {
		fm := prepareFileManager(t)
		f, err := os.Open("testdata/gw_jobs_52307.1311260446.1311289420.1703893976586.1703894226969.workspace.gz")
		require.NoError(t, err)
		_, err = fm.Upload(context.Background(), f, "dummy", "dummy-v0-rudderstack-10")
		require.NoError(t, err)

		require.Error(t, app.Run([]string{
			"migration-tool",
			"migrate",
			"-startTime", "2023-12-29T23:52:55.726942+00:00",
			"-endTime", "2023-12-29T23:53:59.809574+00:00",
			"-uploadBatchSize", "12",
			"-backupFileNamePrefix", "dummy",
		}))

		itr := filemanager.IterateFilesWithPrefix(context.Background(), "", "", 100, fm)
		listOfTransformedFiles := make([]string, 0)
		for itr.Next() {
			listOfTransformedFiles = append(listOfTransformedFiles, itr.Get().Key)
		}
		require.ElementsMatch(t, listOfTransformedFiles, []string{
			"dummy/dummy-v0-rudderstack-10/gw_jobs_52307.1311260446.1311289420.1703893976586.1703894226969.workspace.gz",
		})
	})
}
