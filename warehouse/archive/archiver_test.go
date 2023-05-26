package archive_test

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"

	"github.com/golang/mock/gomock"
	"github.com/minio/minio-go"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/mock_stats"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/warehouse/archive"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

func TestArchiver(t *testing.T) {
	testcases := []struct {
		name                 string
		degradedWorkspaceIDs []string
		workspaceID          string
		archived             bool
		status               string
	}{
		{
			name:        "should archive uploads",
			workspaceID: "1",
			archived:    true,
		},
		{
			name:        "skip archive not exported uploads",
			workspaceID: "1",
			status:      model.Waiting,
		},
		{
			name:                 "skip archive degraded uploads",
			degradedWorkspaceIDs: []string{"1"},
			workspaceID:          "1",
		},
		{
			name:        "skip archive uploads for empty workspace",
			workspaceID: "",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.yml"}))

			t.Cleanup(func() {
				c.Stop(context.Background())
			})
			c.Start(context.Background())

			minioPort := c.Port("minio", 9000)
			postgresPort := c.Port("postgres", 5432)

			prefix := "test-prefix"
			host := "localhost"
			database := "rudderdb"
			user := "rudder"
			password := "rudder-password"
			bucketName := "testbucket"
			accessKeyID := "MYACCESSKEY"
			secretAccessKey := "MYSECRETKEY"
			region := "us-east-1"

			minioEndpoint := fmt.Sprintf("localhost:%d", minioPort)

			t.Setenv("JOBS_BACKUP_STORAGE_PROVIDER", "MINIO")
			t.Setenv("JOBS_BACKUP_BUCKET", bucketName)
			t.Setenv("JOBS_BACKUP_PREFIX", prefix)
			t.Setenv("MINIO_ACCESS_KEY_ID", accessKeyID)
			t.Setenv("MINIO_SECRET_ACCESS_KEY", secretAccessKey)
			t.Setenv("MINIO_ENDPOINT", minioEndpoint)
			t.Setenv("MINIO_SSL", "false")
			t.Setenv("RUDDER_TMPDIR", t.TempDir())
			t.Setenv("RSERVER_WAREHOUSE_UPLOADS_ARCHIVAL_TIME_IN_DAYS", "0")

			dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
				user,
				password,
				host,
				strconv.Itoa(postgresPort),
				database,
			)
			db, err := sql.Open("postgres", dsn)
			require.NoError(t, err)
			require.Eventually(t, func() bool { return db.Ping() == nil }, 5*time.Second, 100*time.Millisecond)

			err = (&migrator.Migrator{
				Handle:          db,
				MigrationsTable: "wh_schema_migrations",
			}).Migrate("warehouse")
			require.NoError(t, err)

			sqlStatement, err := os.ReadFile("testdata/dump.sql")
			require.NoError(t, err)

			_, err = db.Exec(string(sqlStatement))
			require.NoError(t, err)

			minioClient, err := minio.New(minioEndpoint, accessKeyID, secretAccessKey, false)
			require.NoError(t, err)

			err = minioClient.MakeBucket(bucketName, region)
			require.NoError(t, err)

			ctrl := gomock.NewController(t)
			mockStats := mock_stats.NewMockStats(ctrl)
			mockMeasurement := mock_stats.NewMockMeasurement(ctrl)

			if tc.archived {
				mockStats.EXPECT().NewTaggedStat(gomock.Any(), gomock.Any(), gomock.Any()).Times(4).Return(mockMeasurement)
				mockMeasurement.EXPECT().Count(1).Times(4)
			}

			now := time.Now().Truncate(time.Second)

			if tc.status == "" {
				tc.status = model.ExportedData
			}

			_, err = db.Exec(`
				UPDATE wh_uploads SET workspace_id = $1, status = $2
			`, tc.workspaceID, tc.status)
			require.NoError(t, err)

			_, err = db.Exec(`
				UPDATE wh_staging_files
				SET
					workspace_id = $1,
					first_event_at = $2,
					last_event_at = $2,
					created_at = $2,
					updated_at = $2
			`, tc.workspaceID, now)
			require.NoError(t, err)

			archiver := archive.Archiver{
				DB:          db,
				Stats:       mockStats,
				Logger:      logger.NOP,
				FileManager: filemanager.DefaultFileManagerFactory,
				Multitenant: &multitenant.Manager{
					DegradedWorkspaceIDs: tc.degradedWorkspaceIDs,
				},
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			err = archiver.Do(ctx)
			require.NoError(t, err)

			for _, table := range []string{
				warehouseutils.WarehouseLoadFilesTable,
				warehouseutils.WarehouseStagingFilesTable,
			} {
				var count int
				err := db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %q`, table)).Scan(&count)

				require.NoError(t, err)
				if tc.archived {
					require.Equal(t, 0, count, "%q rows should be deleted", table)
				} else {
					require.Equal(t, 4, count, "%q rows not deleted", table)
				}
			}

			contents := minioContents(t, minioClient, prefix)

			var expectedContents map[string]string
			jsonTestData(t, "testdata/storage.json", &expectedContents)

			// fix time-sensitive fields:
			for name, file := range expectedContents {
				expectedContents[name] = strings.ReplaceAll(file, "{{.Now}}", now.Local().Format("2006-01-02T15:04:05.999999999"))
			}

			unixSuffix := regexp.MustCompile(`\d+\.json\.gz$`)
			for name, file := range contents {
				newFile := unixSuffix.ReplaceAllString(name, "unix_time.json.gz")
				delete(contents, name)
				contents[newFile] = file
			}
			if tc.archived {
				require.Equal(t, expectedContents, contents)
			} else {
				require.Empty(t, contents)
			}
		})
	}
}

func minioContents(t require.TestingT, minioClient *minio.Client, prefix string) map[string]string {
	contents := make(map[string]string)

	doneCh := make(chan struct{})
	defer close(doneCh)

	for objInfo := range minioClient.ListObjectsV2("testbucket", prefix, true, nil) {
		o, err := minioClient.GetObject("testbucket", objInfo.Key, minio.GetObjectOptions{})
		require.NoError(t, err)

		g, err := gzip.NewReader(o)
		require.NoError(t, err)

		b, err := io.ReadAll(g)
		require.NoError(t, err)

		contents[objInfo.Key] = string(b)
	}

	return contents
}

func jsonTestData(t require.TestingT, file string, value any) {
	f, err := os.Open(file)
	require.NoError(t, err)

	defer func() { _ = f.Close() }()

	err = json.NewDecoder(f).Decode(value)
	require.NoError(t, err)
}
